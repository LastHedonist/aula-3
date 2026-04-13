from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from storage import s3
from utils.logger import get_logger


@dataclass
class ScraperResult:
    provider: str
    date: str
    success: bool
    files_uploaded: list[str] = field(default_factory=list)
    skus_count: int = 0
    error: str = None


class BaseScraper(ABC):
    def __init__(self, bucket: str, date: str):
        self.bucket = bucket
        self.date = date
        self.log = get_logger(f"scraper.{self.provider}")

    @property
    @abstractmethod
    def provider(self) -> str:
        """Cloud provider identifier: aws | azure | gcp | oracle"""

    @abstractmethod
    def fetch(self) -> list[dict]:
        """
        Download raw data from cloud pricing API.
        Returns list of dicts: [{"filename": str, "content": bytes}, ...]
        """

    def upload_raw(self, files: list[dict]) -> list[str]:
        """Upload raw files to S3 Camada 1. Idempotent."""
        uploaded = []
        for f in files:
            key = s3.raw_prefix(self.date, self.provider) + f["filename"]
            path = s3.upload(self.bucket, key, f["content"])
            uploaded.append(path)
        return uploaded

    def run(self) -> ScraperResult:
        """Orchestrate fetch + upload_raw with error handling."""
        self.log.info(f"Starting scraper provider={self.provider} date={self.date}")
        try:
            files = self.fetch()
            uploaded = self.upload_raw(files)
            self.log.info(f"Completed provider={self.provider} files={len(uploaded)}")
            return ScraperResult(
                provider=self.provider,
                date=self.date,
                success=True,
                files_uploaded=uploaded,
            )
        except Exception as e:
            self.log.error(f"Scraper failed provider={self.provider} error={e}")
            return ScraperResult(
                provider=self.provider,
                date=self.date,
                success=False,
                error=str(e),
            )
