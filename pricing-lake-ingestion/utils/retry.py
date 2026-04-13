from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import requests


def with_retry(func):
    """Decorator: 3 attempts, exponential backoff 30s → 60s → 120s."""
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=30, min=30, max=120),
        retry=retry_if_exception_type((requests.HTTPError, requests.ConnectionError, requests.Timeout)),
        reraise=True,
    )(func)
