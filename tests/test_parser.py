# tests/test_parser.py
import pytest

from otomotoskrap.parser import parse_listings, parse_pagination


class TestParseListings:
    def test_extracts_listings_from_html(self, sample_html):
        listings = parse_listings(sample_html)
        assert len(listings) == 2

    def test_first_listing_fields(self, sample_html):
        listings = parse_listings(sample_html)
        first = listings[0]
        assert first["listing_id"] == "6147167659"
        assert first["title"] == "BMW Seria 3 320e"
        assert first["url"] == "https://www.otomoto.pl/osobowe/oferta/bmw-seria-3-ID6I0T7t.html"
        assert first["brand"] == "BMW"
        assert first["model"] == "Seria 3"
        assert first["year"] == 2021
        assert first["price"] == 49200.0
        assert first["currency"] == "PLN"
        assert first["mileage_km"] == 85000
        assert first["fuel_type"] == "Hybryda Plug-in"
        assert first["transmission"] == "Automatyczna"
        assert first["engine_capacity_cm3"] == 1998
        assert first["engine_power_hp"] == 204
        assert first["location_city"] == "Tarnowskie G\u00f3ry"
        assert first["location_region"] == "\u015al\u0105skie"
        assert first["seller_type"] == "private"

    def test_dealer_seller_type(self, sample_html):
        listings = parse_listings(sample_html)
        second = listings[1]
        assert second["seller_type"] == "dealer"
        assert second["listing_id"] == "6147142768"

    def test_empty_html_returns_empty_list(self):
        listings = parse_listings("<html><body></body></html>")
        assert listings == []

    def test_missing_optional_params(self, sample_html):
        listings = parse_listings(sample_html)
        first = listings[0]
        assert first.get("body_type") is None
        assert first.get("color") is None
        assert first.get("is_new") is None


class TestParsePagination:
    def test_extracts_pagination_info(self, sample_html):
        info = parse_pagination(sample_html)
        assert info["total_count"] == 150
        assert info["page_size"] == 32
        assert info["current_offset"] == 0

    def test_calculates_total_pages(self, sample_html):
        info = parse_pagination(sample_html)
        assert info["total_pages"] == 5  # ceil(150 / 32)

    def test_empty_html_returns_none(self):
        info = parse_pagination("<html><body></body></html>")
        assert info is None
