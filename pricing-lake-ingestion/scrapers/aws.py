import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from scrapers.base import BaseScraper, ScraperResult
from storage import s3
from utils.retry import with_retry

AWS_INDEX_URL = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/index.json"
MAX_WORKERS = 3


class AWSScraper(BaseScraper):
    @property
    def provider(self) -> str:
        return "aws"

    def fetch(self) -> list[dict]:
        # Not used — run() is overridden to stream uploads
        return []

    def run(self) -> ScraperResult:
        """Fetch and upload each service immediately to avoid accumulating GBs in memory."""
        self.log.info(f"Starting scraper provider={self.provider} date={self.date}")
        try:
            index = self._fetch_index()
            offers = index.get("offers", {})
            self.log.info(f"AWS services in index: {len(offers)}")

            uploaded = []
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {
                    executor.submit(self._fetch_and_upload, name, meta): name
                    for name, meta in offers.items()
                }
                for future in as_completed(futures):
                    service_name = futures[future]
                    try:
                        path = future.result()
                        if path:
                            uploaded.append(path)
                    except Exception as e:
                        self.log.warning(f"AWS service failed service={service_name} error={e}")

            self.log.info(f"Completed provider={self.provider} files={len(uploaded)}")
            return ScraperResult(provider=self.provider, date=self.date, success=True, files_uploaded=uploaded)
        except Exception as e:
            self.log.error(f"Scraper failed provider={self.provider} error={e}")
            return ScraperResult(provider=self.provider, date=self.date, success=False, error=str(e))

    @with_retry
    def _fetch_index(self) -> dict:
        resp = requests.get(AWS_INDEX_URL, timeout=60)
        resp.raise_for_status()
        return resp.json()

    @with_retry
    def _fetch_and_upload(self, service_name: str, meta: dict) -> str | None:
        url = meta.get("currentVersionUrl")
        if not url:
            return None

        if url.startswith("/"):
            url = f"https://pricing.us-east-1.amazonaws.com{url}"

        if not url.endswith(".json"):
            url = url.replace("/index.csv", "/index.json")

        resp = requests.get(url, timeout=120, stream=True)
        resp.raise_for_status()
        content = resp.content

        key = s3.raw_prefix(self.date, self.provider) + f"{service_name}.json"
        return s3.upload(self.bucket, key, content)
