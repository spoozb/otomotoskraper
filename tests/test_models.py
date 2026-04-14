from datetime import datetime, timezone

import pytest

from otomotoskrap.models import AppConfig, Listing, QueryConfig, Settings


class TestListing:
    def test_valid_listing(self):
        listing = Listing(
            listing_id="6147167659",
            url="https://www.otomoto.pl/osobowe/oferta/bmw-seria-3-ID6I0T7t.html",
            title="BMW Seria 3 320e",
            brand="BMW",
            model="Seria 3",
            year=2021,
            price=49200.0,
            currency="PLN",
            mileage_km=85000,
            fuel_type="Hybryda Plug-in",
            transmission="Automatyczna",
            engine_capacity_cm3=1998,
            engine_power_hp=204,
            location_city="Tarnowskie Gory",
            location_region="Slaskie",
            seller_type="private",
            scraped_at=datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc),
            query_name="bmw_3_series",
        )
        assert listing.listing_id == "6147167659"
        assert listing.price == 49200.0
        assert listing.seller_type == "private"

    def test_optional_fields_default_none(self):
        listing = Listing(
            listing_id="123",
            url="https://example.com",
            title="Test",
            brand="BMW",
            model="3",
            year=2020,
            price=10000.0,
            currency="PLN",
            mileage_km=50000,
            fuel_type="Benzyna",
            transmission="Manualna",
            location_city="Warszawa",
            location_region="Mazowieckie",
            seller_type="dealer",
            scraped_at=datetime.now(tz=timezone.utc),
            query_name="test",
        )
        assert listing.engine_capacity_cm3 is None
        assert listing.engine_power_hp is None
        assert listing.body_type is None
        assert listing.color is None
        assert listing.is_new is None

    def test_listing_csv_headers(self):
        headers = Listing.csv_headers()
        assert "listing_id" in headers
        assert "price" in headers
        assert "scraped_at" in headers
        assert len(headers) == 20

    def test_listing_to_csv_row(self):
        dt = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
        listing = Listing(
            listing_id="1",
            url="https://example.com",
            title="Test",
            brand="BMW",
            model="3",
            year=2020,
            price=10000.0,
            currency="PLN",
            mileage_km=50000,
            fuel_type="Benzyna",
            transmission="Manualna",
            location_city="Warszawa",
            location_region="Mazowieckie",
            seller_type="private",
            scraped_at=dt,
            query_name="test",
        )
        row = listing.to_csv_row()
        assert row[0] == "1"
        assert row[4] == "3"
        assert len(row) == 20


class TestConfig:
    def test_valid_query_config(self):
        q = QueryConfig(
            name="bmw_3_series",
            url="https://www.otomoto.pl/osobowe/bmw/seria-3",
            max_pages=50,
        )
        assert q.name == "bmw_3_series"
        assert q.max_pages == 50

    def test_query_max_pages_default(self):
        q = QueryConfig(
            name="test",
            url="https://www.otomoto.pl/osobowe",
        )
        assert q.max_pages == 50

    def test_settings_defaults(self):
        s = Settings()
        assert s.delay_range == [2, 5]
        assert s.max_retries == 3
        assert s.output_dir == "./data"
        assert s.proxy is None

    def test_app_config(self):
        config = AppConfig(
            queries=[
                QueryConfig(name="test", url="https://www.otomoto.pl/osobowe"),
            ],
            settings=Settings(),
        )
        assert len(config.queries) == 1
        assert config.settings.max_retries == 3
