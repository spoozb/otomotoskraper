# Otomoto.pl Price Tracker — Design Spec

## Purpose

Build a Python CLI tool that scrapes car listings from otomoto.pl to create a live dataset for price prediction, market trend tracking, and general data analysis. The tool runs daily, captures configurable search categories, and stores data as JSON/CSV files.

## Site Analysis

- **otomoto.pl** is a Polish car marketplace (cars, motorcycles, parts)
- Pages are server-side rendered with **JSON-LD structured data** (Schema.org) embedded in HTML
- ~30 listings per search results page
- `robots.txt` blocks `/api/` and `/ajax/` but does **not** block listing/search pages
- No Cloudflare or CAPTCHAs detected; New Relic session tracking present
- Pagination via URL query parameters

## Architecture

```
config.yaml → CLI → HTTP Client → Fetch Pages → Parse JSON-LD → Normalize → Write JSON/CSV
                   (anti-detection)  (pagination)                          (daily partitioned)
```

### Components

| Module | Responsibility |
|--------|---------------|
| `cli.py` | Click-based CLI entry point (`run`, `stats`, `--dry-run`) |
| `config.py` | Load and validate YAML config (search queries + settings) |
| `client.py` | httpx client with UA rotation, delays, realistic headers, cookie persistence |
| `scraper.py` | Page fetching, pagination loop, retry logic, orchestration |
| `parser.py` | Extract JSON-LD blocks from HTML, parse into listing records |
| `models.py` | Pydantic models for listings and config validation |
| `storage.py` | Write raw JSON per query/day, append to consolidated CSV with dedup |

## Data Model

Each listing record:

| Field | Type | Source |
|-------|------|--------|
| `listing_id` | str | URL/JSON-LD — otomoto's unique listing ID |
| `url` | str | Constructed from listing page |
| `title` | str | JSON-LD `name` field |
| `brand` | str | JSON-LD `brand` |
| `model` | str | JSON-LD or title parsing |
| `year` | int | JSON-LD or HTML attributes |
| `price` | float | JSON-LD `offers.price` |
| `currency` | str | JSON-LD `offers.priceCurrency` (always PLN) |
| `mileage_km` | int | JSON-LD `mileageFromOdometer` |
| `fuel_type` | str | JSON-LD or HTML — Benzyna/Diesel/Elektryczny/LPG/Hybryda |
| `body_type` | str | HTML params — SUV/Sedan/Kombi/Hatchback/etc. |
| `transmission` | str | HTML — Manualna/Automatyczna |
| `engine_capacity_cm3` | int | HTML listing details |
| `engine_power_hp` | int | HTML listing details |
| `color` | str | HTML listing details |
| `location_city` | str | HTML — seller's city |
| `location_region` | str | HTML — seller's voivodeship |
| `seller_type` | str | HTML — "dealer" or "private" |
| `is_new` | bool | HTML — new vs used flag |
| `scraped_at` | datetime | Runtime — UTC timestamp |
| `query_name` | str | Config — which query produced this listing |

## Storage Layout

```
data/
├── raw/                          # One JSON file per query per day
│   └── 2026-04-14/
│       ├── bmw_3_series.json
│       └── electric_cars.json
└── consolidated/
    └── all_listings.csv          # Appended daily, deduped by listing_id + scraped_at date
```

- **Raw JSON**: full listing records as-is, for debugging and reprocessing
- **Consolidated CSV**: analysis-ready flat file, appended daily
- Same listing appearing on different days creates separate rows (enables time-series price tracking)

## Config Format

```yaml
queries:
  - name: bmw_3_series
    url: "https://www.otomoto.pl/osobowe/bmw/seria-3"
    max_pages: 50

  - name: electric_cars
    url: "https://www.otomoto.pl/osobowe?search[filter_enum_fuel_type][0]=electric"
    max_pages: 100

  - name: suv_under_100k
    url: "https://www.otomoto.pl/osobowe?search[filter_enum_body_type][0]=suv&search[filter_float_price%3Ato]=100000"
    max_pages: 50

settings:
  delay_range: [2, 5]        # seconds between requests (uniform random)
  max_retries: 3
  output_dir: "./data"
  proxy: null                 # optional: "http://proxy:port" or list for rotation
```

## Anti-Detection Strategy

Conservative approach suited for daily scraping:

### Request Headers
- Rotate User-Agent from a pool of ~15 real browser strings (Chrome/Firefox/Safari, Windows/Mac/Linux)
- Realistic headers: `Accept`, `Accept-Language: pl-PL,pl;q=0.9,en;q=0.8`, `Accept-Encoding`, `Referer` (otomoto.pl)
- Maintain session cookies across a scraping run via httpx `Client`

### Request Pacing
- Random delay between requests: configurable, default 2-5 seconds (uniform random + jitter)
- Exponential backoff on errors: 4s → 8s → 16s on 429/503 responses
- Configurable max requests per minute

### Session Management
- Fresh session per scraping run (new cookies, new User-Agent)
- First request hits homepage to establish cookies before search pages
- Optionally rotate UA every N requests within a session

### Proxy Support
- Configurable proxy URL or list of proxies in config
- Round-robin rotation when multiple proxies provided
- Default: no proxy (sufficient for daily single-IP scraping)

### Error Handling
- Detect blocks: 403 status, CAPTCHA-like HTML patterns, empty responses
- On block: log warning, extended wait (30-60s), retry with new UA
- After 3 failed retries per page: skip, continue, log the gap
- End-of-run summary: pages scraped, pages failed, listings collected

## CLI Interface

```bash
# Run all configured queries
python -m otomotoskrap run

# Run specific queries only
python -m otomotoskrap run --query bmw_3_series --query electric_cars

# Dry run — validate config, show what would be scraped
python -m otomotoskrap run --dry-run

# Show dataset stats (row counts, date range, coverage)
python -m otomotoskrap stats
```

## Project Structure

```
otomotoskrap/
├── pyproject.toml
├── config.yaml
├── .gitignore
├── src/
│   └── otomotoskrap/
│       ├── __init__.py
│       ├── cli.py
│       ├── config.py
│       ├── client.py
│       ├── scraper.py
│       ├── parser.py
│       ├── models.py
│       └── storage.py
├── data/                     # gitignored
├── tests/
│   ├── test_parser.py
│   ├── test_storage.py
│   └── fixtures/             # Sample HTML pages
└── logs/
```

## Dependencies

- `httpx` — async HTTP client with session/cookie support
- `beautifulsoup4` + `lxml` — HTML parsing to locate JSON-LD script tags
- `pydantic` — data models and config validation
- `click` — CLI framework
- `pyyaml` — config file parsing
- `structlog` — structured logging

## Scheduling

Cron job for daily execution:
```cron
0 6 * * * cd /path/to/otomotoskrap && python -m otomotoskrap run >> logs/cron.log 2>&1
```

## Verification Plan

1. **Unit tests**: Test JSON-LD parser with saved HTML fixtures, test CSV dedup logic, test config validation
2. **Integration test**: Run `--dry-run` to validate config + URL reachability
3. **Manual test**: Run a single query with `max_pages: 2`, verify JSON output contains expected fields
4. **Data quality check**: After first full run, load CSV in pandas, verify no null prices, check field distributions
