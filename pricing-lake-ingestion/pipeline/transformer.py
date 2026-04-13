import io
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from storage import s3
from catalog import glue
from utils.logger import get_logger

log = get_logger(__name__)

# Region normalization: cloud-native name → normalized slug
REGION_MAP = {
    # AWS
    "US East (N. Virginia)": "us-east-1",
    "US East (Ohio)": "us-east-2",
    "US West (Oregon)": "us-west-2",
    "EU (Ireland)": "eu-west-1",
    "EU (Frankfurt)": "eu-central-1",
    "Asia Pacific (Tokyo)": "ap-northeast-1",
    "Asia Pacific (Singapore)": "ap-southeast-1",
    "South America (Sao Paulo)": "sa-east-1",
    # Azure
    "eastus": "us-east-1",
    "eastus2": "us-east-2",
    "westus2": "us-west-2",
    "westeurope": "eu-west-1",
    "northeurope": "eu-north-1",
    "brazilsouth": "sa-east-1",
    "japaneast": "ap-northeast-1",
    "southeastasia": "ap-southeast-1",
    # GCP (already normalized)
    "us-east1": "us-east-1",
    "us-central1": "us-central-1",
    "europe-west1": "eu-west-1",
    "asia-east1": "ap-east-1",
    "southamerica-east1": "sa-east-1",
}

UNIT_MAP = {
    "Hrs": "hour", "Hr": "hour", "hours": "hour",
    "GB-Mo": "GB-month", "GB/Month": "GB-month", "GiB-Mo": "GB-month",
    "GB": "GB", "GiB": "GB",
    "Requests": "request", "1M Requests": "1M-requests",
    "vCPU-Hours": "vcpu-hour",
}

GEOGRAPHY_MAP = {
    "us-": "americas", "sa-": "americas",
    "eu-": "europe", "ap-": "apac",
    "me-": "middle-east", "af-": "africa", "ca-": "americas",
}

PARQUET_SCHEMA = pa.schema([
    ("provider", pa.string()),
    ("sku", pa.string()),
    ("service", pa.string()),
    ("service_family", pa.string()),
    ("description", pa.string()),
    ("region", pa.string()),
    ("region_orig", pa.string()),
    ("geography", pa.string()),
    ("price_usd", pa.float64()),
    ("price_orig", pa.float64()),
    ("currency_orig", pa.string()),
    ("unit", pa.string()),
    ("unit_orig", pa.string()),
    ("price_type", pa.string()),
    ("effective_date", pa.string()),
    ("collected_date", pa.string()),
    ("source_file", pa.string()),
])


def transform(provider: str, date: str, bucket: str) -> str:
    """
    Read consolidated JSON from Camada 2, apply canonical schema,
    write Parquet to Camada 3, register Glue partition.
    Returns s3_path of parquet file.
    """
    source_key = s3.consolidated_key(date, provider)
    raw_bytes = s3.download(bucket, source_key)
    data = json.loads(raw_bytes)
    source_file = f"s3://{bucket}/{source_key}"

    log.info(f"Transforming provider={provider} date={date}")
    records = _extract_records(provider, data)
    rows = [_map_record(provider, date, source_file, r) for r in records]
    rows = [r for r in rows if r is not None]

    log.info(f"Mapped rows={len(rows)} provider={provider}")
    df = pd.DataFrame(rows)

    # Enforce schema column order and types
    for col in [f.name for f in PARQUET_SCHEMA]:
        if col not in df.columns:
            df[col] = None

    df = df[[f.name for f in PARQUET_SCHEMA]]

    buf = io.BytesIO()
    table = pa.Table.from_pandas(df, schema=PARQUET_SCHEMA)
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    dest_key = s3.parquet_key(date, provider)
    path = s3.upload(bucket, dest_key, buf.read(), content_type="application/octet-stream")
    log.info(f"Parquet written provider={provider} → {path}")

    glue.register_partition(bucket, date, provider)
    return path


def _extract_records(provider: str, data) -> list[dict]:
    """Extract flat list of pricing records from consolidated JSON."""
    if provider == "aws":
        # AWS: list of service JSONs, each with 'products' and 'terms'
        records = []
        if isinstance(data, list):
            for svc in data:
                records.extend(_flatten_aws(svc))
        else:
            records.extend(_flatten_aws(data))
        return records

    if provider == "azure":
        return data.get("items", []) if isinstance(data, dict) else data

    if provider == "gcp":
        skus = []
        if isinstance(data, list):
            for svc in data:
                skus.extend(svc.get("skus", []))
        else:
            skus = data.get("skus", [])
        return skus

    if provider == "oracle":
        products = []
        if isinstance(data, list):
            for grp in data:
                products.extend(grp.get("products", []))
        else:
            products = data.get("products", [])
        return products

    return []


def _flatten_aws(svc: dict) -> list[dict]:
    """Flatten AWS service JSON into individual pricing records."""
    products = svc.get("products", {})
    terms = svc.get("terms", {}).get("OnDemand", {})
    records = []
    for sku, product in products.items():
        term = terms.get(sku, {})
        price_dimensions = next(iter(next(iter(term.values()), {}).get("priceDimensions", {}).values()), {}) if term else {}
        records.append({**product.get("attributes", {}), "sku": sku, "_price_dim": price_dimensions})
    return records


def _map_record(provider: str, date: str, source_file: str, r: dict) -> dict | None:
    try:
        if provider == "aws":
            return _map_aws(r, date, source_file)
        if provider == "azure":
            return _map_azure(r, date, source_file)
        if provider == "gcp":
            return _map_gcp(r, date, source_file)
        if provider == "oracle":
            return _map_oracle(r, date, source_file)
    except Exception as e:
        log.warning(f"Failed to map record provider={provider} error={e}")
    return None


def _map_aws(r: dict, date: str, source_file: str) -> dict:
    pd_dim = r.get("_price_dim", {})
    price_str = next(iter(pd_dim.get("pricePerUnit", {}).values()), "0")
    price = float(price_str)
    unit_orig = pd_dim.get("unit", "")
    region_orig = r.get("location", "")
    return {
        "provider": "aws",
        "sku": r.get("sku", ""),
        "service": r.get("servicecode", ""),
        "service_family": r.get("serviceFamily", ""),
        "description": r.get("usagetype", ""),
        "region": _normalize_region(region_orig),
        "region_orig": region_orig,
        "geography": _infer_geography(_normalize_region(region_orig)),
        "price_usd": price,
        "price_orig": price,
        "currency_orig": "USD",
        "unit": _normalize_unit(unit_orig),
        "unit_orig": unit_orig,
        "price_type": "on-demand",
        "effective_date": date,
        "collected_date": date,
        "source_file": source_file,
    }


def _map_azure(r: dict, date: str, source_file: str) -> dict:
    region_orig = r.get("armRegionName", "")
    unit_orig = r.get("unitOfMeasure", "")
    price = float(r.get("retailPrice", 0))
    price_type_raw = r.get("type", "Consumption").lower()
    price_type = "reserved" if "reservation" in price_type_raw else "on-demand"
    return {
        "provider": "azure",
        "sku": r.get("skuId", ""),
        "service": r.get("serviceName", ""),
        "service_family": r.get("serviceFamily", ""),
        "description": r.get("skuName", ""),
        "region": _normalize_region(region_orig),
        "region_orig": region_orig,
        "geography": _infer_geography(_normalize_region(region_orig)),
        "price_usd": price,
        "price_orig": price,
        "currency_orig": r.get("currencyCode", "USD"),
        "unit": _normalize_unit(unit_orig),
        "unit_orig": unit_orig,
        "price_type": price_type,
        "effective_date": r.get("effectiveStartDate", date)[:10],
        "collected_date": date,
        "source_file": source_file,
    }


def _map_gcp(r: dict, date: str, source_file: str) -> dict:
    pricing_info = (r.get("pricingInfo") or [{}])[0]
    pricing_expr = pricing_info.get("pricingExpression", {})
    tiered = (pricing_expr.get("tieredRates") or [{}])[0]
    money = tiered.get("unitPrice", {})
    unit_orig = pricing_expr.get("usageUnitDescription", "")
    regions = r.get("serviceRegions", ["global"])
    region_orig = regions[0] if regions else "global"
    price = float(money.get("units", 0)) + float(money.get("nanos", 0)) / 1e9
    cat = r.get("category", {})
    return {
        "provider": "gcp",
        "sku": r.get("skuId", ""),
        "service": cat.get("serviceDisplayName", ""),
        "service_family": cat.get("resourceFamily", ""),
        "description": r.get("description", ""),
        "region": _normalize_region(region_orig),
        "region_orig": region_orig,
        "geography": _infer_geography(_normalize_region(region_orig)),
        "price_usd": price,
        "price_orig": price,
        "currency_orig": money.get("currencyCode", "USD"),
        "unit": _normalize_unit(unit_orig),
        "unit_orig": unit_orig,
        "price_type": "on-demand",
        "effective_date": date,
        "collected_date": date,
        "source_file": source_file,
    }


def _map_oracle(r: dict, date: str, source_file: str) -> dict:
    currency = r.get("currencyCodeLocalizations", [{}])[0]
    price = float(currency.get("prices", [{}])[0].get("value", 0))
    unit_orig = r.get("billingUnit", "")
    return {
        "provider": "oracle",
        "sku": r.get("partNumber", ""),
        "service": r.get("serviceName", ""),
        "service_family": r.get("serviceCategory", ""),
        "description": r.get("displayName", ""),
        "region": "global",
        "region_orig": "global",
        "geography": "global",
        "price_usd": price,
        "price_orig": price,
        "currency_orig": currency.get("currencyCode", "USD"),
        "unit": _normalize_unit(unit_orig),
        "unit_orig": unit_orig,
        "price_type": "on-demand",
        "effective_date": date,
        "collected_date": date,
        "source_file": source_file,
    }


def _normalize_region(region_orig: str) -> str:
    return REGION_MAP.get(region_orig, region_orig.lower().replace(" ", "-"))


def _normalize_unit(unit_orig: str) -> str:
    return UNIT_MAP.get(unit_orig, unit_orig.lower())


def _infer_geography(region: str) -> str:
    for prefix, geo in GEOGRAPHY_MAP.items():
        if region.startswith(prefix):
            return geo
    return "global"
