from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from otomotoskrap.models import Listing, QueryConfig, Settings
from otomotoskrap.scraper import scrape_query, ScrapeResult


class TestScrapeQuery:
    def _make_page_html(self, listings: list[dict], total_count: int, offset: int) -> str:
        """Build minimal HTML with __NEXT_DATA__ containing given listings."""
        import json

        edges = []
        for listing in listings:
            edges.append(
                {
                    "__typename": "AdvertEdge",
                    "node": {
                        "__typename": "Advert",
                        "id": listing["id"],
                        "title": listing["title"],
                        "url": f"https://www.otomoto.pl/osobowe/oferta/test-{listing['id']}.html",
                        "createdAt": "2026-04-14T12:00:00Z",
                        "shortDescription": "",
                        "parameters": [
                            {"key": "make", "displayValue": "BMW", "value": "bmw"},
                            {"key": "model", "displayValue": "Seria 3", "value": "seria-3"},
                            {"key": "year", "displayValue": "2020", "value": "2020"},
                            {"key": "mileage", "displayValue": "50000 km", "value": "50000"},
                            {"key": "fuel_type", "displayValue": "Benzyna", "value": "petrol"},
                            {"key": "gearbox", "displayValue": "Manualna", "value": "manual"},
                        ],
                        "location": {
                            "city": {"name": "Warszawa"},
                            "region": {"name": "Mazowieckie"},
                        },
                        "price": {
                            "amount": {"value": "50000", "currencyCode": "PLN"}
                        },
                        "seller": {"__typename": "PrivateSeller"},
                    },
                }
            )

        advert_search = json.dumps(
            {
                "advertSearch": {
                    "totalCount": total_count,
                    "pageInfo": {"pageSize": 32, "currentOffset": offset},
                    "edges": edges,
                }
            }
        )

        next_data = json.dumps(
            {
                "props": {
                    "pageProps": {
                        "urqlState": {"-123": {"hasNext": False, "data": advert_search}}
                    }
                }
            }
        )
        return f'<html><script id="__NEXT_DATA__" type="application/json">{next_data}</script></html>'

    def test_scrape_single_page(self):
        query = QueryConfig(name="test", url="https://www.otomoto.pl/osobowe/bmw", max_pages=5)
        settings = Settings(delay_range=[0, 0])

        page_html = self._make_page_html(
            [{"id": "1", "title": "BMW 1"}, {"id": "2", "title": "BMW 2"}],
            total_count=2,
            offset=0,
        )

        mock_client = MagicMock()
        mock_client.fetch.return_value = page_html

        result = scrape_query(query, settings, client=mock_client)
        assert isinstance(result, ScrapeResult)
        assert len(result.listings) == 2
        assert result.pages_scraped == 1
        assert result.pages_failed == 0

    def test_scrape_paginates(self):
        query = QueryConfig(name="test", url="https://www.otomoto.pl/osobowe/bmw", max_pages=5)
        settings = Settings(delay_range=[0, 0])

        page1 = self._make_page_html(
            [{"id": str(i), "title": f"BMW {i}"} for i in range(32)],
            total_count=40,
            offset=0,
        )
        page2 = self._make_page_html(
            [{"id": str(i), "title": f"BMW {i}"} for i in range(32, 40)],
            total_count=40,
            offset=32,
        )

        mock_client = MagicMock()
        mock_client.fetch.side_effect = [page1, page2]

        result = scrape_query(query, settings, client=mock_client)
        assert len(result.listings) == 40
        assert result.pages_scraped == 2
        assert mock_client.fetch.call_count == 2

    def test_scrape_respects_max_pages(self):
        query = QueryConfig(name="test", url="https://www.otomoto.pl/osobowe/bmw", max_pages=1)
        settings = Settings(delay_range=[0, 0])

        page_html = self._make_page_html(
            [{"id": str(i), "title": f"BMW {i}"} for i in range(32)],
            total_count=100,
            offset=0,
        )

        mock_client = MagicMock()
        mock_client.fetch.return_value = page_html

        result = scrape_query(query, settings, client=mock_client)
        assert result.pages_scraped == 1
        assert mock_client.fetch.call_count == 1

    def test_scrape_handles_failed_page(self):
        query = QueryConfig(name="test", url="https://www.otomoto.pl/osobowe/bmw", max_pages=5)
        settings = Settings(delay_range=[0, 0])

        mock_client = MagicMock()
        mock_client.fetch.return_value = None  # fetch failed

        result = scrape_query(query, settings, client=mock_client)
        assert len(result.listings) == 0
        assert result.pages_failed == 1

    def test_scrape_adds_query_name_and_timestamp(self):
        query = QueryConfig(name="my_query", url="https://www.otomoto.pl/osobowe/bmw", max_pages=5)
        settings = Settings(delay_range=[0, 0])

        page_html = self._make_page_html(
            [{"id": "1", "title": "BMW 1"}],
            total_count=1,
            offset=0,
        )

        mock_client = MagicMock()
        mock_client.fetch.return_value = page_html

        result = scrape_query(query, settings, client=mock_client)
        assert result.listings[0].query_name == "my_query"
        assert result.listings[0].scraped_at is not None
