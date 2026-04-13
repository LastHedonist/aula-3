import json
import requests
from scrapers.base import BaseScraper
from utils.retry import with_retry

# Oracle Cloud public price list endpoint
ORACLE_PRICE_LIST_URL = "https://apexapps.oracle.com/pls/apex/cetools/api/v1/products/"


class OracleScraper(BaseScraper):
    @property
    def provider(self) -> str:
        return "oracle"

    def fetch(self) -> list[dict]:
        data = self._fetch_price_list()
        items = data.get("items", [])
        self.log.info(f"Oracle items fetched: {len(items)}")

        # Group by product family
        families: dict[str, list] = {}
        for item in items:
            family = item.get("serviceCategory", "uncategorized")
            family_key = family.lower().replace(" ", "-").replace("/", "-")
            families.setdefault(family_key, []).append(item)

        files = []
        for family, products in families.items():
            content = json.dumps({"family": family, "products": products}, ensure_ascii=False).encode()
            files.append({"filename": f"{family}.json", "content": content})

        self.log.info(f"Oracle files prepared: {len(files)}")
        return files

    @with_retry
    def _fetch_price_list(self) -> dict:
        resp = requests.get(
            ORACLE_PRICE_LIST_URL,
            params={"limit": 2000},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()
