import json
import requests
from scrapers.base import BaseScraper
from utils.retry import with_retry

AZURE_API_URL = "https://prices.azure.com/api/retail/prices"
AZURE_API_VERSION = "2023-01-01-preview"


class AzureScraper(BaseScraper):
    @property
    def provider(self) -> str:
        return "azure"

    def fetch(self) -> list[dict]:
        all_items = []
        page = 1
        url = f"{AZURE_API_URL}?api-version={AZURE_API_VERSION}"

        while url:
            self.log.info(f"Azure fetching page={page}")
            data = self._fetch_page(url)
            items = data.get("Items", [])
            all_items.extend(items)

            # Save each page as a separate raw file
            content = json.dumps({"page": page, "items": items}, ensure_ascii=False).encode()
            self._page_files = getattr(self, "_page_files", [])
            self._page_files.append({"filename": f"page-{page:04d}.json", "content": content})

            url = data.get("NextPageLink")
            page += 1

        self.log.info(f"Azure total items={len(all_items)} pages={page - 1}")
        return self._page_files

    @with_retry
    def _fetch_page(self, url: str) -> dict:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        return resp.json()
