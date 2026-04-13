import boto3
from datetime import datetime, timezone
from utils.logger import get_logger

log = get_logger(__name__)
_cw = boto3.client("cloudwatch")
NAMESPACE = "pricing-lake/custom"


def put_metric(name: str, value: float, unit: str = "Count", dimensions: dict = None) -> None:
    dims = [{"Name": k, "Value": v} for k, v in (dimensions or {}).items()]
    try:
        _cw.put_metric_data(
            Namespace=NAMESPACE,
            MetricData=[{
                "MetricName": name,
                "Value": value,
                "Unit": unit,
                "Timestamp": datetime.now(timezone.utc),
                "Dimensions": dims,
            }],
        )
    except Exception as e:
        log.warning(f"Failed to publish metric {name}: {e}")
