import csv
import json
from datetime import date
from pathlib import Path

import structlog

from otomotoskrap.models import Listing

log = structlog.get_logger()


def write_raw_json(listings: list[Listing], query_name: str, output_dir: str) -> Path:
    """Write listings as raw JSON to a daily-partitioned file.

    The partition date is derived from the first listing's scraped_at when
    listings are provided; otherwise falls back to today.
    """
    if listings:
        today = listings[0].scraped_at.date().isoformat()
    else:
        today = date.today().isoformat()

    raw_dir = Path(output_dir) / "raw" / today
    raw_dir.mkdir(parents=True, exist_ok=True)

    json_path = raw_dir / f"{query_name}.json"
    data = [listing.model_dump(mode="json") for listing in listings]
    json_path.write_text(json.dumps(data, ensure_ascii=False, indent=2))

    log.info("raw_json_written", path=str(json_path), count=len(listings))
    return json_path


def _load_existing_keys(csv_path: Path) -> set[tuple[str, str]]:
    """Load existing (listing_id, date) pairs from CSV for dedup."""
    keys: set[tuple[str, str]] = set()
    if not csv_path.exists():
        return keys

    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            lid = row.get("listing_id", "")
            scraped = row.get("scraped_at", "")
            scraped_date = scraped[:10] if scraped else ""
            keys.add((lid, scraped_date))

    return keys


def append_csv(listings: list[Listing], output_dir: str) -> Path:
    """Append listings to the consolidated CSV, deduplicating by listing_id + date."""
    csv_dir = Path(output_dir) / "consolidated"
    csv_dir.mkdir(parents=True, exist_ok=True)
    csv_path = csv_dir / "all_listings.csv"

    existing_keys = _load_existing_keys(csv_path)
    is_new_file = not csv_path.exists()

    new_listings = []
    for listing in listings:
        scraped_date = listing.scraped_at.date().isoformat()
        key = (listing.listing_id, scraped_date)
        if key not in existing_keys:
            new_listings.append(listing)
            existing_keys.add(key)

    with open(csv_path, "a", newline="") as f:
        writer = csv.writer(f)
        if is_new_file:
            writer.writerow(Listing.csv_headers())
        for listing in new_listings:
            writer.writerow(listing.to_csv_row())

    log.info(
        "csv_appended",
        path=str(csv_path),
        new=len(new_listings),
        skipped=len(listings) - len(new_listings),
    )
    return csv_path
