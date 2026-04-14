import csv
import json
from datetime import datetime, timezone

import pytest

from otomotoskrap.models import Listing
from otomotoskrap.storage import write_raw_json, append_csv


def _make_listing(**overrides) -> Listing:
    defaults = dict(
        listing_id="1",
        url="https://example.com/1",
        title="BMW 320i",
        brand="BMW",
        model="Seria 3",
        year=2020,
        price=50000.0,
        currency="PLN",
        mileage_km=80000,
        fuel_type="Benzyna",
        transmission="Manualna",
        location_city="Warszawa",
        location_region="Mazowieckie",
        seller_type="private",
        scraped_at=datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc),
        query_name="test_query",
    )
    defaults.update(overrides)
    return Listing(**defaults)


class TestWriteRawJson:
    def test_creates_dated_directory(self, tmp_path):
        listings = [_make_listing()]
        write_raw_json(listings, "bmw_3", str(tmp_path))
        expected_dir = tmp_path / "raw" / "2026-04-14"
        assert expected_dir.exists()

    def test_writes_json_file(self, tmp_path):
        listings = [_make_listing(), _make_listing(listing_id="2")]
        write_raw_json(listings, "bmw_3", str(tmp_path))
        json_file = tmp_path / "raw" / "2026-04-14" / "bmw_3.json"
        assert json_file.exists()
        data = json.loads(json_file.read_text())
        assert len(data) == 2
        assert data[0]["listing_id"] == "1"

    def test_empty_listings_writes_empty_array(self, tmp_path):
        write_raw_json([], "empty", str(tmp_path))
        json_file = tmp_path / "raw" / "2026-04-14" / "empty.json"
        data = json.loads(json_file.read_text())
        assert data == []


class TestAppendCsv:
    def test_creates_csv_with_headers(self, tmp_path):
        listings = [_make_listing()]
        append_csv(listings, str(tmp_path))
        csv_file = tmp_path / "consolidated" / "all_listings.csv"
        assert csv_file.exists()
        with open(csv_file) as f:
            reader = csv.reader(f)
            headers = next(reader)
            assert headers[0] == "listing_id"
            assert "price" in headers

    def test_appends_rows(self, tmp_path):
        listings1 = [_make_listing(listing_id="1")]
        listings2 = [_make_listing(listing_id="2")]
        append_csv(listings1, str(tmp_path))
        append_csv(listings2, str(tmp_path))
        csv_file = tmp_path / "consolidated" / "all_listings.csv"
        with open(csv_file) as f:
            reader = csv.reader(f)
            headers = next(reader)
            rows = list(reader)
            assert len(rows) == 2

    def test_dedup_same_listing_same_day(self, tmp_path):
        dt = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
        listings = [
            _make_listing(listing_id="1", scraped_at=dt),
            _make_listing(listing_id="1", scraped_at=dt),
        ]
        append_csv(listings, str(tmp_path))
        csv_file = tmp_path / "consolidated" / "all_listings.csv"
        with open(csv_file) as f:
            reader = csv.reader(f)
            next(reader)  # skip header
            rows = list(reader)
            assert len(rows) == 1

    def test_allows_same_listing_different_days(self, tmp_path):
        day1 = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)
        day2 = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
        append_csv([_make_listing(listing_id="1", scraped_at=day1)], str(tmp_path))
        append_csv([_make_listing(listing_id="1", scraped_at=day2)], str(tmp_path))
        csv_file = tmp_path / "consolidated" / "all_listings.csv"
        with open(csv_file) as f:
            reader = csv.reader(f)
            next(reader)
            rows = list(reader)
            assert len(rows) == 2
