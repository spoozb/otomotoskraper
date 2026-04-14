from dataclasses import dataclass, field
from datetime import datetime, timezone
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse

import structlog

from otomotoskrap.client import OtomotoClient
from otomotoskrap.models import Listing, QueryConfig, Settings
from otomotoskrap.parser import parse_listings, parse_pagination

log = structlog.get_logger()


@dataclass
class ScrapeResult:
    listings: list[Listing] = field(default_factory=list)
    pages_scraped: int = 0
    pages_failed: int = 0


def _build_page_url(base_url: str, page: int) -> str:
    """Add or update ?page=N parameter in the URL."""
    parsed = urlparse(base_url)
    params = parse_qs(parsed.query)
    params["page"] = [str(page)]
    new_query = urlencode(params, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def scrape_query(
    query: QueryConfig,
    settings: Settings,
    client: OtomotoClient | None = None,
) -> ScrapeResult:
    """Scrape all pages for a single query configuration."""
    own_client = client is None
    if own_client:
        client = OtomotoClient(
            delay_range=tuple(settings.delay_range),
            proxy=settings.proxy,
            max_retries=settings.max_retries,
        )
        client.start_session()

    result = ScrapeResult()
    now = datetime.now(tz=timezone.utc)
    total_pages: int | None = None

    try:
        for page_num in range(1, query.max_pages + 1):
            url = _build_page_url(query.url, page_num)
            log.info("scraping_page", query=query.name, page=page_num, url=url)

            html = client.fetch(url)
            if html is None:
                result.pages_failed += 1
                log.warning("page_failed", query=query.name, page=page_num)
                break

            raw_listings = parse_listings(html)
            if not raw_listings:
                log.info("no_listings_found", query=query.name, page=page_num)
                break

            for raw in raw_listings:
                raw["scraped_at"] = now
                raw["query_name"] = query.name
                result.listings.append(Listing.model_validate(raw))

            result.pages_scraped += 1

            if total_pages is None:
                pagination = parse_pagination(html)
                if pagination:
                    total_pages = min(pagination["total_pages"], query.max_pages)
                    log.info(
                        "pagination_detected",
                        query=query.name,
                        total_count=pagination["total_count"],
                        total_pages=total_pages,
                    )

            if total_pages is not None and page_num >= total_pages:
                break

    finally:
        if own_client:
            client.close()

    log.info(
        "query_complete",
        query=query.name,
        listings=len(result.listings),
        pages_scraped=result.pages_scraped,
        pages_failed=result.pages_failed,
    )
    return result
