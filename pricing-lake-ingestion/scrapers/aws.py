import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from scrapers.base import BaseScraper
from utils.retry import with_retry

AWS_INDEX_URL = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/index.json"
MAX_WORKERS = 3


class AWSScraper(BaseScraper):
    @property
    def provider(self) -> str:
        return "aws"

    def fetch(self) -> list[dict]:
        index = self._fetch_index()
        offers = index.get("offers", {})
        self.log.info(f"AWS services in index: {len(offers)}")

        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(self._fetch_service, name, meta): name
                for name, meta in offers.items()
            }
            for future in as_completed(futures):
                service_name = futures[future]
                try:
                    file_dict = future.result()
                    if file_dict:
                        results.append(file_dict)
                except Exception as e:
                    self.log.warning(f"AWS service failed service={service_name} error={e}")

        self.log.info(f"AWS files prepared: {len(results)}")
        return results

    @with_retry
    def _fetch_index(self) -> dict:
        resp = requests.get(AWS_INDEX_URL, timeout=60)
        resp.raise_for_status()
        return resp.json()

    @with_retry
    def _fetch_service(self, service_name: str, meta: dict) -> dict | None:
        url = meta.get("currentVersionUrl")
        if not url:
            return None

        # AWS index URLs are relative paths
        if url.startswith("/"):
            url = f"https://pricing.us-east-1.amazonaws.com{url}"

        # Prefer JSON format
        if not url.endswith(".json"):
            url = url.replace("/index.csv", "/index.json")

        resp = requests.get(url, timeout=120, stream=True)
        resp.raise_for_status()
        content = resp.content
        return {"filename": f"{service_name}.json", "content": content}
