import json
import requests
from scrapers.base import BaseScraper
from utils.retry import with_retry

GCP_API_URL = "https://cloudbilling.googleapis.com/v1/services"
GCP_API_KEY = None  # Public pricing API — no auth required for list prices


class GCPScraper(BaseScraper):
    @property
    def provider(self) -> str:
        return "gcp"

    def fetch(self) -> list[dict]:
        services = self._list_services()
        self.log.info(f"GCP services found: {len(services)}")

        files = []
        for svc in services:
            service_id = svc["serviceId"]
            skus = self._fetch_skus(svc["name"])
            if not skus:
                continue
            content = json.dumps({
                "serviceId": service_id,
                "displayName": svc.get("displayName", ""),
                "skus": skus,
            }, ensure_ascii=False).encode()
            files.append({"filename": f"{service_id}.json", "content": content})

        self.log.info(f"GCP files prepared: {len(files)}")
        return files

    @with_retry
    def _list_services(self) -> list[dict]:
        resp = requests.get(GCP_API_URL, timeout=30)
        resp.raise_for_status()
        return resp.json().get("services", [])

    @with_retry
    def _fetch_skus(self, service_name: str) -> list[dict]:
        skus = []
        url = f"https://cloudbilling.googleapis.com/v1/{service_name}/skus"
        while url:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            skus.extend(data.get("skus", []))
            page_token = data.get("nextPageToken")
            url = f"https://cloudbilling.googleapis.com/v1/{service_name}/skus?pageToken={page_token}" if page_token else None
        return skus
