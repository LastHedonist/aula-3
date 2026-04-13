import argparse
import sys
from datetime import date as dt
from scrapers.aws import AWSScraper
from scrapers.azure import AzureScraper
from scrapers.gcp import GCPScraper
from scrapers.oracle import OracleScraper
from pipeline.consolidator import consolidate
from pipeline.transformer import transform
from utils.logger import get_logger
from utils.metrics import put_metric

log = get_logger("main")

BUCKET = "pricing-lake"
PROVIDERS = ["gcp", "aws", "azure", "oracle"]
SCRAPER_MAP = {
    "aws": AWSScraper,
    "azure": AzureScraper,
    "gcp": GCPScraper,
    "oracle": OracleScraper,
}


def main(collection_date: str) -> None:
    log.info(f"Pipeline started date={collection_date}")
    failed_providers = []

    # ── FASE 1: Download Raw (Camada 1) ──────────────────────────────────
    log.info("=== FASE 1: Download Raw ===")
    for provider in PROVIDERS:
        scraper = SCRAPER_MAP[provider](bucket=BUCKET, date=collection_date)
        result = scraper.run()
        if result.success:
            put_metric("ScraperSuccess", 1, dimensions={"Provider": provider})
        else:
            log.error(f"Scraper failed provider={provider} error={result.error}")
            put_metric("ScraperFailure", 1, dimensions={"Provider": provider})
            failed_providers.append(provider)

    successful_providers = [p for p in PROVIDERS if p not in failed_providers]

    # ── FASE 2: Consolidação (Camada 2) ───────────────────────────────────
    log.info("=== FASE 2: Consolidação ===")
    consolidated_providers = []
    for provider in successful_providers:
        try:
            consolidate(provider, collection_date, BUCKET)
            consolidated_providers.append(provider)
        except Exception as e:
            log.error(f"Consolidation failed provider={provider} error={e}")
            failed_providers.append(provider)

    # ── FASE 3: Transformação Parquet (Camada 3) ──────────────────────────
    log.info("=== FASE 3: Transformação Parquet ===")
    parquet_providers = []
    for provider in consolidated_providers:
        try:
            transform(provider, collection_date, BUCKET)
            parquet_providers.append(provider)
            put_metric("ParquetSuccess", 1, dimensions={"Provider": provider})
        except Exception as e:
            log.error(f"Transform failed provider={provider} error={e}")
            put_metric("ParquetFailure", 1, dimensions={"Provider": provider})
            failed_providers.append(provider)

    # ── Relatório final ───────────────────────────────────────────────────
    put_metric("CloudsFailedCount", len(set(failed_providers)))
    put_metric("CloudsSuccessCount", len(parquet_providers))

    log.info(
        f"Pipeline completed date={collection_date} "
        f"success={parquet_providers} "
        f"failed={list(set(failed_providers))}"
    )

    if set(failed_providers) == set(PROVIDERS):
        log.error("All providers failed — exiting with error")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=str(dt.today()), help="Collection date YYYY-MM-DD")
    args = parser.parse_args()
    main(args.date)
