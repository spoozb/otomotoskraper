import random
import time

import httpx
import structlog

log = structlog.get_logger()

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]


class OtomotoClient:
    def __init__(
        self,
        delay_range: tuple[float, float] = (2.0, 5.0),
        proxy: str | None = None,
        max_retries: int = 3,
    ):
        self._user_agents = _USER_AGENTS
        self._delay_range = delay_range
        self._proxy = proxy
        self._max_retries = max_retries
        self._client: httpx.Client | None = None

    def _random_ua(self) -> str:
        return random.choice(self._user_agents)

    def _build_headers(self) -> dict[str, str]:
        return {
            "User-Agent": self._random_ua(),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.7,en;q=0.3",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://www.otomoto.pl/",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

    def _wait(self) -> None:
        delay = random.uniform(*self._delay_range)
        time.sleep(delay)

    def _new_session(self) -> httpx.Client:
        kwargs: dict = {
            "headers": self._build_headers(),
            "follow_redirects": True,
            "timeout": 30.0,
        }
        if self._proxy:
            kwargs["proxy"] = self._proxy
        return httpx.Client(**kwargs)

    def start_session(self) -> None:
        """Start a new HTTP session with fresh cookies and UA."""
        if self._client:
            self._client.close()
        self._client = self._new_session()
        log.info("session_started")

    def close(self) -> None:
        if self._client:
            self._client.close()
            self._client = None

    def fetch(self, url: str) -> str | None:
        """Fetch a URL with retry logic and anti-detection delays.

        Returns the response text, or None if all retries fail.
        """
        if not self._client:
            self.start_session()

        backoff = 4.0
        for attempt in range(1, self._max_retries + 1):
            try:
                self._wait()
                resp = self._client.get(url)

                if resp.status_code == 200:
                    log.info("fetch_ok", url=url, status=200)
                    return resp.text

                if resp.status_code in (429, 503):
                    log.warning(
                        "rate_limited",
                        url=url,
                        status=resp.status_code,
                        attempt=attempt,
                        backoff=backoff,
                    )
                    time.sleep(backoff)
                    backoff *= 2
                    continue

                if resp.status_code == 403:
                    log.warning("blocked", url=url, attempt=attempt)
                    time.sleep(random.uniform(30, 60))
                    self._client.close()
                    self._client = self._new_session()
                    continue

                log.warning(
                    "unexpected_status",
                    url=url,
                    status=resp.status_code,
                    attempt=attempt,
                )

            except httpx.RequestError as e:
                log.warning("request_error", url=url, error=str(e), attempt=attempt)
                time.sleep(backoff)
                backoff *= 2

        log.error("fetch_failed", url=url, retries=self._max_retries)
        return None
