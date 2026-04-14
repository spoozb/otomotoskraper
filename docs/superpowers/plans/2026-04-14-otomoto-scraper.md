# Otomoto.pl Price Tracker — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Python CLI tool that scrapes car listings from otomoto.pl search pages, extracting structured data from the Next.js GraphQL cache (`__NEXT_DATA__`), and stores results as daily JSON + consolidated CSV.

**Architecture:** Each search page is a Next.js SSR page embedding a GraphQL cache (`urqlState`) with fully structured listing data — id, price, parameters, location, seller type. The parser extracts this JSON from `<script id="__NEXT_DATA__">`, avoiding fragile HTML scraping. Pagination uses `?page=N` with offset-based GraphQL paging (32 per page, `totalCount` for bounds).

**Tech Stack:** Python 3.11+, httpx (HTTP client), BeautifulSoup4+lxml (HTML script extraction), Pydantic (models/validation), Click (CLI), PyYAML (config), structlog (logging)

---

## File Structure

| File | Responsibility |
|------|---------------|
| `pyproject.toml` | Project metadata, dependencies, scripts |
| `config.yaml` | Default search queries and settings |
| `.gitignore` | Ignore data/, logs/, venv/, etc. |
| `src/otomotoskrap/__init__.py` | Package init with version |
| `src/otomotoskrap/models.py` | Pydantic: `Listing`, `QueryConfig`, `Settings`, `AppConfig` |
| `src/otomotoskrap/config.py` | Load + validate YAML config |
| `src/otomotoskrap/parser.py` | Extract `__NEXT_DATA__` JSON, parse listings + pagination |
| `src/otomotoskrap/client.py` | httpx client with UA rotation, delays, headers, cookies |
| `src/otomotoskrap/storage.py` | Write raw JSON per query/day, append CSV with dedup |
| `src/otomotoskrap/scraper.py` | Orchestrate: per-query pagination loop, wire client→parser→storage |
| `src/otomotoskrap/cli.py` | Click CLI: `run`, `stats`, `--dry-run`, `--query` filter |
| `tests/conftest.py` | Shared fixtures: sample HTML, temp dirs, sample config |
| `tests/test_models.py` | Model validation tests |
| `tests/test_config.py` | Config loading tests |
| `tests/test_parser.py` | Parser tests against real HTML fixture |
| `tests/test_client.py` | Client header/UA tests |
| `tests/test_storage.py` | JSON/CSV write + dedup tests |
| `tests/test_scraper.py` | Scraper orchestration tests (mocked HTTP) |
| `tests/test_cli.py` | CLI integration tests |
| `tests/fixtures/search_page.html` | Real otomoto.pl search page (fetched during setup) |

---

## Task 1: Project Scaffolding

**Files:**
- Create: `pyproject.toml`
- Create: `.gitignore`
- Create: `src/otomotoskrap/__init__.py`
- Create: `config.yaml`

- [ ] **Step 1: Create pyproject.toml**

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "otomotoskrap"
version = "0.1.0"
description = "Otomoto.pl car listing scraper for price tracking"
requires-python = ">=3.11"
dependencies = [
    "httpx>=0.27",
    "beautifulsoup4>=4.12",
    "lxml>=5.0",
    "pydantic>=2.0",
    "click>=8.1",
    "pyyaml>=6.0",
    "structlog>=24.0",
]

[project.scripts]
otomotoskrap = "otomotoskrap.cli:cli"

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-httpx>=0.30",
]

[tool.hatch.build.targets.wheel]
packages = ["src/otomotoskrap"]

[tool.pytest.ini_options]
testpaths = ["tests"]
```

- [ ] **Step 2: Create .gitignore**

```gitignore
data/
logs/
__pycache__/
*.pyc
.venv/
*.egg-info/
dist/
.pytest_cache/
```

- [ ] **Step 3: Create package init**

```python
# src/otomotoskrap/__init__.py
__version__ = "0.1.0"
```

- [ ] **Step 4: Create default config.yaml**

```yaml
queries:
  - name: bmw_3_series
    url: "https://www.otomoto.pl/osobowe/bmw/seria-3"
    max_pages: 50

  - name: electric_cars
    url: "https://www.otomoto.pl/osobowe?search%5Bfilter_enum_fuel_type%5D%5B0%5D=electric"
    max_pages: 100

settings:
  delay_range: [2, 5]
  max_retries: 3
  output_dir: "./data"
  proxy: null
```

- [ ] **Step 5: Create directory structure and install**

```bash
mkdir -p src/otomotoskrap tests/fixtures data/raw data/consolidated logs
touch src/otomotoskrap/__init__.py tests/__init__.py tests/conftest.py
```

- [ ] **Step 6: Initialize git and install**

```bash
git init
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

- [ ] **Step 7: Verify installation**

Run: `python -c "import otomotoskrap; print(otomotoskrap.__version__)"`
Expected: `0.1.0`

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml .gitignore config.yaml src/ tests/
git commit -m "chore: scaffold project structure with dependencies"
```

---

## Task 2: Data Models

**Files:**
- Create: `src/otomotoskrap/models.py`
- Create: `tests/test_models.py`

- [ ] **Step 1: Write failing tests for Listing model**

```python
# tests/test_models.py
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_models.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'otomotoskrap.models'`

- [ ] **Step 3: Implement models**

```python
# src/otomotoskrap/models.py
from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class Listing(BaseModel):
    listing_id: str
    url: str
    title: str
    brand: str
    model: str
    year: int
    price: float
    currency: str
    mileage_km: int
    fuel_type: str
    body_type: Optional[str] = None
    transmission: str
    engine_capacity_cm3: Optional[int] = None
    engine_power_hp: Optional[int] = None
    color: Optional[str] = None
    location_city: str
    location_region: str
    seller_type: str
    is_new: Optional[bool] = None
    scraped_at: datetime
    query_name: str

    @classmethod
    def csv_headers(cls) -> list[str]:
        return list(cls.model_fields.keys())

    def to_csv_row(self) -> list[str]:
        values = []
        for field_name in self.model_fields:
            val = getattr(self, field_name)
            if val is None:
                values.append("")
            elif isinstance(val, datetime):
                values.append(val.isoformat())
            elif isinstance(val, bool):
                values.append(str(val).lower())
            else:
                values.append(str(val))
        return values


class QueryConfig(BaseModel):
    name: str
    url: str
    max_pages: int = 50


class Settings(BaseModel):
    delay_range: list[float] = [2, 5]
    max_retries: int = 3
    output_dir: str = "./data"
    proxy: Optional[str] = None


class AppConfig(BaseModel):
    queries: list[QueryConfig]
    settings: Settings = Settings()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_models.py -v`
Expected: All 8 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/otomotoskrap/models.py tests/test_models.py
git commit -m "feat: add Pydantic data models for listings and config"
```

---

## Task 3: Config Loading

**Files:**
- Create: `src/otomotoskrap/config.py`
- Create: `tests/test_config.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_config.py
import pytest
import yaml

from otomotoskrap.config import load_config
from otomotoskrap.models import AppConfig


def test_load_valid_config(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        yaml.dump(
            {
                "queries": [
                    {
                        "name": "bmw",
                        "url": "https://www.otomoto.pl/osobowe/bmw",
                        "max_pages": 10,
                    }
                ],
                "settings": {
                    "delay_range": [1, 3],
                    "max_retries": 2,
                    "output_dir": "./out",
                },
            }
        )
    )
    config = load_config(str(config_file))
    assert isinstance(config, AppConfig)
    assert config.queries[0].name == "bmw"
    assert config.settings.delay_range == [1, 3]


def test_load_config_defaults(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        yaml.dump(
            {
                "queries": [
                    {"name": "test", "url": "https://www.otomoto.pl/osobowe"},
                ],
            }
        )
    )
    config = load_config(str(config_file))
    assert config.settings.max_retries == 3
    assert config.settings.output_dir == "./data"


def test_load_config_missing_file():
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/config.yaml")


def test_load_config_invalid_yaml(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("queries: not_a_list")
    with pytest.raises(Exception):
        load_config(str(config_file))
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_config.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'otomotoskrap.config'`

- [ ] **Step 3: Implement config loading**

```python
# src/otomotoskrap/config.py
from pathlib import Path

import yaml

from otomotoskrap.models import AppConfig


def load_config(path: str) -> AppConfig:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(config_path) as f:
        raw = yaml.safe_load(f)

    return AppConfig.model_validate(raw)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_config.py -v`
Expected: All 4 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/otomotoskrap/config.py tests/test_config.py
git commit -m "feat: add YAML config loading with validation"
```

---

## Task 4: Test Fixtures + Parser

**Files:**
- Create: `tests/fixtures/search_page.html`
- Create: `tests/conftest.py`
- Create: `src/otomotoskrap/parser.py`
- Create: `tests/test_parser.py`

### Real site analysis (completed during planning)

The otomoto.pl search page is a Next.js app. All listing data lives in a `<script id="__NEXT_DATA__">` tag containing a JSON blob. The relevant path is:

```
__NEXT_DATA__
  .props.pageProps.urqlState
    .{hash_key}       ← numeric key, varies per page
      .data.advertSearch
        .totalCount   ← total listings matching query
        .pageInfo     ← { pageSize: 32, currentOffset: 0 }
        .edges[]      ← array of listing objects
          .node
            .id, .title, .url, .createdAt
            .parameters[] ← [{ key: "make", value: "bmw", displayValue: "BMW" }, ...]
            .location.city.name, .location.region.name
            .price.amount.value, .price.amount.currencyCode
            .seller.__typename ← "PrivateSeller" or "ProfessionalSeller"
```

Available parameter keys: `make`, `model`, `version`, `year`, `mileage`, `fuel_type`, `gearbox`, `engine_capacity`, `engine_power`, `country_origin`.

Fields NOT available from search results (would require individual listing pages): `body_type`, `color`, `is_new`.

- [ ] **Step 1: Create a minimal but realistic HTML fixture**

```python
# Run this script once to create the fixture:
# python -c "
# import httpx
# resp = httpx.get('https://www.otomoto.pl/osobowe/bmw/seria-3?page=1',
#     headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
#              'Accept-Language': 'pl-PL,pl;q=0.9'})
# with open('tests/fixtures/search_page.html', 'w') as f:
#     f.write(resp.text)
# "
```

If fetching a live page isn't practical, create this minimal fixture that mirrors the real structure:

Save to `tests/fixtures/search_page.html`:

```html
<!DOCTYPE html>
<html>
<head>
<script id="listing-json-ld" data-testid="listing-json-ld" type="application/ld+json">
{"@context":"https://schema.org","@type":"Webpage","mainEntity":{"@type":"OfferCatalog","name":"Otomoto","itemListElement":[]}}
</script>
</head>
<body>
<div id="__next"></div>
<script id="__NEXT_DATA__" type="application/json">
{
  "props": {
    "pageProps": {
      "urqlState": {
        "-1234567890": {
          "hasNext": false,
          "data": "{\"advertSearch\":{\"__typename\":\"AdvertSearchOutput\",\"totalCount\":150,\"pageInfo\":{\"__typename\":\"Pager\",\"pageSize\":32,\"currentOffset\":0},\"edges\":[{\"__typename\":\"AdvertEdge\",\"node\":{\"__typename\":\"Advert\",\"id\":\"6147167659\",\"title\":\"BMW Seria 3 320e\",\"createdAt\":\"2026-04-14T17:44:02Z\",\"shortDescription\":\"BMW 320e | Plug-in Hybrid\",\"url\":\"https://www.otomoto.pl/osobowe/oferta/bmw-seria-3-ID6I0T7t.html\",\"parameters\":[{\"__typename\":\"AdvertParameter\",\"key\":\"make\",\"displayValue\":\"BMW\",\"value\":\"bmw\"},{\"__typename\":\"AdvertParameter\",\"key\":\"model\",\"displayValue\":\"Seria 3\",\"value\":\"seria-3\"},{\"__typename\":\"AdvertParameter\",\"key\":\"year\",\"displayValue\":\"2021\",\"value\":\"2021\"},{\"__typename\":\"AdvertParameter\",\"key\":\"mileage\",\"displayValue\":\"85000 km\",\"value\":\"85000\"},{\"__typename\":\"AdvertParameter\",\"key\":\"fuel_type\",\"displayValue\":\"Hybryda Plug-in\",\"value\":\"plugin-hybrid\"},{\"__typename\":\"AdvertParameter\",\"key\":\"gearbox\",\"displayValue\":\"Automatyczna\",\"value\":\"automatic\"},{\"__typename\":\"AdvertParameter\",\"key\":\"engine_capacity\",\"displayValue\":\"1998 cm3\",\"value\":\"1998\"},{\"__typename\":\"AdvertParameter\",\"key\":\"engine_power\",\"displayValue\":\"204 KM\",\"value\":\"204\"}],\"location\":{\"__typename\":\"Location\",\"city\":{\"__typename\":\"AdministrativeLevel\",\"name\":\"Tarnowskie G\\u00f3ry\"},\"region\":{\"__typename\":\"AdministrativeLevel\",\"name\":\"\\u015al\\u0105skie\"}},\"price\":{\"__typename\":\"Price\",\"amount\":{\"__typename\":\"Money\",\"units\":49200,\"nanos\":0,\"value\":\"49200\",\"currencyCode\":\"PLN\"}},\"seller\":{\"__typename\":\"PrivateSeller\"}}},{\"__typename\":\"AdvertEdge\",\"node\":{\"__typename\":\"Advert\",\"id\":\"6147142768\",\"title\":\"BMW Seria 3 330i\",\"createdAt\":\"2026-04-14T16:30:00Z\",\"shortDescription\":\"BMW 330i M Sport\",\"url\":\"https://www.otomoto.pl/osobowe/oferta/bmw-seria-3-ID6I0ME0.html\",\"parameters\":[{\"__typename\":\"AdvertParameter\",\"key\":\"make\",\"displayValue\":\"BMW\",\"value\":\"bmw\"},{\"__typename\":\"AdvertParameter\",\"key\":\"model\",\"displayValue\":\"Seria 3\",\"value\":\"seria-3\"},{\"__typename\":\"AdvertParameter\",\"key\":\"year\",\"displayValue\":\"2024\",\"value\":\"2024\"},{\"__typename\":\"AdvertParameter\",\"key\":\"mileage\",\"displayValue\":\"15000 km\",\"value\":\"15000\"},{\"__typename\":\"AdvertParameter\",\"key\":\"fuel_type\",\"displayValue\":\"Benzyna\",\"value\":\"petrol\"},{\"__typename\":\"AdvertParameter\",\"key\":\"gearbox\",\"displayValue\":\"Automatyczna\",\"value\":\"automatic\"},{\"__typename\":\"AdvertParameter\",\"key\":\"engine_capacity\",\"displayValue\":\"1998 cm3\",\"value\":\"1998\"},{\"__typename\":\"AdvertParameter\",\"key\":\"engine_power\",\"displayValue\":\"245 KM\",\"value\":\"245\"}],\"location\":{\"__typename\":\"Location\",\"city\":{\"__typename\":\"AdministrativeLevel\",\"name\":\"Warszawa\"},\"region\":{\"__typename\":\"AdministrativeLevel\",\"name\":\"Mazowieckie\"}},\"price\":{\"__typename\":\"Price\",\"amount\":{\"__typename\":\"Money\",\"units\":189900,\"nanos\":0,\"value\":\"189900\",\"currencyCode\":\"PLN\"}},\"seller\":{\"__typename\":\"ProfessionalSeller\"}}}]}}"
        }
      }
    }
  },
  "page": "/osobowe/[[...listing]]",
  "query": {"page": "1", "listing": ["bmw", "seria-3"]}
}
</script>
</body>
</html>
```

- [ ] **Step 2: Create shared conftest.py**

```python
# tests/conftest.py
from pathlib import Path

import pytest


@pytest.fixture
def fixtures_dir():
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_html(fixtures_dir):
    return (fixtures_dir / "search_page.html").read_text()
```

- [ ] **Step 3: Write failing parser tests**

```python
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
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `pytest tests/test_parser.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'otomotoskrap.parser'`

- [ ] **Step 5: Implement parser**

```python
# src/otomotoskrap/parser.py
import json
import math
from typing import Any, Optional

from bs4 import BeautifulSoup


def _extract_next_data(html: str) -> Optional[dict]:
    """Extract the __NEXT_DATA__ JSON from the page."""
    soup = BeautifulSoup(html, "lxml")
    script = soup.find("script", id="__NEXT_DATA__")
    if not script or not script.string:
        return None
    return json.loads(script.string)


def _find_advert_search(next_data: dict) -> Optional[dict]:
    """Navigate urqlState to find the advertSearch data."""
    urql_state = (
        next_data.get("props", {}).get("pageProps", {}).get("urqlState", {})
    )
    for _key, entry in urql_state.items():
        if not isinstance(entry, dict) or "data" not in entry:
            continue
        data = entry["data"]
        if isinstance(data, str):
            data = json.loads(data)
        if isinstance(data, dict) and "advertSearch" in data:
            return data["advertSearch"]
    return None


def _get_param(parameters: list[dict], key: str) -> Optional[str]:
    """Get a parameter value by key from the parameters list."""
    for p in parameters:
        if p.get("key") == key:
            return p.get("value")
    return None


def _get_display(parameters: list[dict], key: str) -> Optional[str]:
    """Get a parameter displayValue by key from the parameters list."""
    for p in parameters:
        if p.get("key") == key:
            return p.get("displayValue")
    return None


def _parse_int(value: Optional[str]) -> Optional[int]:
    """Safely parse a string to int, returning None on failure."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _seller_type(seller: dict) -> str:
    """Map seller __typename to simple seller type string."""
    type_name = seller.get("__typename", "")
    if type_name == "PrivateSeller":
        return "private"
    return "dealer"


def _parse_node(node: dict) -> dict[str, Any]:
    """Parse a single advert node into a flat listing dict."""
    params = node.get("parameters", [])
    location = node.get("location", {})
    price_data = node.get("price", {}).get("amount", {})
    seller = node.get("seller", {})

    return {
        "listing_id": node["id"],
        "url": node["url"],
        "title": node["title"],
        "brand": _get_display(params, "make"),
        "model": _get_display(params, "model"),
        "year": _parse_int(_get_param(params, "year")),
        "price": float(price_data.get("value", 0)),
        "currency": price_data.get("currencyCode", "PLN"),
        "mileage_km": _parse_int(_get_param(params, "mileage")),
        "fuel_type": _get_display(params, "fuel_type"),
        "body_type": None,
        "transmission": _get_display(params, "gearbox"),
        "engine_capacity_cm3": _parse_int(_get_param(params, "engine_capacity")),
        "engine_power_hp": _parse_int(_get_param(params, "engine_power")),
        "color": None,
        "location_city": location.get("city", {}).get("name"),
        "location_region": location.get("region", {}).get("name"),
        "seller_type": _seller_type(seller),
        "is_new": None,
    }


def parse_listings(html: str) -> list[dict[str, Any]]:
    """Parse all listings from an otomoto.pl search results page."""
    next_data = _extract_next_data(html)
    if not next_data:
        return []

    advert_search = _find_advert_search(next_data)
    if not advert_search:
        return []

    listings = []
    for edge in advert_search.get("edges", []):
        node = edge.get("node")
        if node:
            listings.append(_parse_node(node))

    return listings


def parse_pagination(html: str) -> Optional[dict[str, int]]:
    """Extract pagination info from an otomoto.pl search results page."""
    next_data = _extract_next_data(html)
    if not next_data:
        return None

    advert_search = _find_advert_search(next_data)
    if not advert_search:
        return None

    page_info = advert_search.get("pageInfo", {})
    total_count = advert_search.get("totalCount", 0)
    page_size = page_info.get("pageSize", 32)

    return {
        "total_count": total_count,
        "page_size": page_size,
        "current_offset": page_info.get("currentOffset", 0),
        "total_pages": math.ceil(total_count / page_size) if page_size > 0 else 0,
    }
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `pytest tests/test_parser.py -v`
Expected: All 8 tests PASS

- [ ] **Step 7: Commit**

```bash
git add src/otomotoskrap/parser.py tests/test_parser.py tests/conftest.py tests/fixtures/
git commit -m "feat: add parser for __NEXT_DATA__ GraphQL cache extraction"
```

---

## Task 5: HTTP Client

**Files:**
- Create: `src/otomotoskrap/client.py`
- Create: `tests/test_client.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_client.py
import time

import pytest

from otomotoskrap.client import OtomotoClient


class TestUserAgentRotation:
    def test_has_user_agents(self):
        client = OtomotoClient()
        assert len(client._user_agents) >= 10

    def test_random_ua_returns_string(self):
        client = OtomotoClient()
        ua = client._random_ua()
        assert isinstance(ua, str)
        assert "Mozilla" in ua

    def test_different_uas_over_calls(self):
        client = OtomotoClient()
        uas = {client._random_ua() for _ in range(50)}
        assert len(uas) > 1


class TestHeaders:
    def test_default_headers(self):
        client = OtomotoClient()
        headers = client._build_headers()
        assert "User-Agent" in headers
        assert headers["Accept-Language"].startswith("pl-PL")
        assert "Accept" in headers
        assert "Referer" in headers

    def test_referer_is_otomoto(self):
        client = OtomotoClient()
        headers = client._build_headers()
        assert "otomoto.pl" in headers["Referer"]


class TestDelay:
    def test_delay_within_range(self):
        client = OtomotoClient(delay_range=(0.01, 0.02))
        start = time.monotonic()
        client._wait()
        elapsed = time.monotonic() - start
        assert 0.01 <= elapsed < 0.1


class TestProxyConfig:
    def test_no_proxy_by_default(self):
        client = OtomotoClient()
        assert client._proxy is None

    def test_single_proxy(self):
        client = OtomotoClient(proxy="http://proxy:8080")
        assert client._proxy == "http://proxy:8080"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_client.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement HTTP client**

```python
# src/otomotoskrap/client.py
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_client.py -v`
Expected: All 7 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/otomotoskrap/client.py tests/test_client.py
git commit -m "feat: add HTTP client with UA rotation, delays, and retry logic"
```

---

## Task 6: Storage

**Files:**
- Create: `src/otomotoskrap/storage.py`
- Create: `tests/test_storage.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_storage.py
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_storage.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement storage**

```python
# src/otomotoskrap/storage.py
import csv
import json
from datetime import date
from pathlib import Path

import structlog

from otomotoskrap.models import Listing

log = structlog.get_logger()


def write_raw_json(listings: list[Listing], query_name: str, output_dir: str) -> Path:
    """Write listings as raw JSON to a daily-partitioned file."""
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_storage.py -v`
Expected: All 6 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/otomotoskrap/storage.py tests/test_storage.py
git commit -m "feat: add storage with daily JSON partitions and CSV dedup"
```

---

## Task 7: Scraper Orchestration

**Files:**
- Create: `src/otomotoskrap/scraper.py`
- Create: `tests/test_scraper.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_scraper.py
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_scraper.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement scraper**

```python
# src/otomotoskrap/scraper.py
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
                continue

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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_scraper.py -v`
Expected: All 5 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/otomotoskrap/scraper.py tests/test_scraper.py
git commit -m "feat: add scraper with pagination, retry, and orchestration"
```

---

## Task 8: CLI

**Files:**
- Create: `src/otomotoskrap/cli.py`
- Create: `tests/test_cli.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_cli.py
import json
from pathlib import Path

import yaml
from click.testing import CliRunner

from otomotoskrap.cli import cli


def _write_config(tmp_path, queries=None):
    """Write a test config file and return its path."""
    config_path = tmp_path / "config.yaml"
    config_data = {
        "queries": queries
        or [
            {
                "name": "test_bmw",
                "url": "https://www.otomoto.pl/osobowe/bmw",
                "max_pages": 1,
            }
        ],
        "settings": {
            "delay_range": [0, 0],
            "output_dir": str(tmp_path / "data"),
        },
    }
    config_path.write_text(yaml.dump(config_data))
    return str(config_path)


class TestDryRun:
    def test_dry_run_shows_queries(self, tmp_path):
        config = _write_config(tmp_path)
        runner = CliRunner()
        result = runner.invoke(cli, ["run", "--config", config, "--dry-run"])
        assert result.exit_code == 0
        assert "test_bmw" in result.output
        assert "Dry run" in result.output or "dry run" in result.output.lower()

    def test_dry_run_with_query_filter(self, tmp_path):
        config = _write_config(
            tmp_path,
            queries=[
                {"name": "bmw", "url": "https://www.otomoto.pl/osobowe/bmw", "max_pages": 1},
                {"name": "audi", "url": "https://www.otomoto.pl/osobowe/audi", "max_pages": 1},
            ],
        )
        runner = CliRunner()
        result = runner.invoke(cli, ["run", "--config", config, "--dry-run", "--query", "bmw"])
        assert result.exit_code == 0
        assert "bmw" in result.output


class TestStats:
    def test_stats_no_data(self, tmp_path):
        config = _write_config(tmp_path)
        runner = CliRunner()
        result = runner.invoke(cli, ["stats", "--config", config])
        assert result.exit_code == 0
        assert "No data" in result.output or "0" in result.output

    def test_stats_with_data(self, tmp_path):
        config = _write_config(tmp_path)
        # Create a CSV with some data
        data_dir = tmp_path / "data" / "consolidated"
        data_dir.mkdir(parents=True)
        csv_file = data_dir / "all_listings.csv"
        csv_file.write_text(
            "listing_id,url,title,brand,model,year,price,currency,mileage_km,"
            "fuel_type,body_type,transmission,engine_capacity_cm3,engine_power_hp,"
            "color,location_city,location_region,seller_type,is_new,scraped_at,query_name\n"
            '1,https://x.com,BMW 320i,BMW,Seria 3,2020,50000.0,PLN,80000,'
            'Benzyna,,Manualna,1998,150,,Warszawa,Mazowieckie,private,,2026-04-14T12:00:00+00:00,test\n'
        )
        runner = CliRunner()
        result = runner.invoke(cli, ["stats", "--config", config])
        assert result.exit_code == 0
        assert "1" in result.output  # at least shows the count
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_cli.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement CLI**

```python
# src/otomotoskrap/cli.py
import csv
from pathlib import Path

import click
import structlog

from otomotoskrap.client import OtomotoClient
from otomotoskrap.config import load_config
from otomotoskrap.scraper import scrape_query
from otomotoskrap.storage import append_csv, write_raw_json

structlog.configure(
    processors=[
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(0),
)

log = structlog.get_logger()


@click.group()
def cli():
    """Otomoto.pl car listing scraper."""
    pass


@cli.command()
@click.option("--config", default="config.yaml", help="Path to config file.")
@click.option("--dry-run", is_flag=True, help="Validate config and show what would be scraped.")
@click.option("--query", multiple=True, help="Run only these queries (by name).")
def run(config: str, dry_run: bool, query: tuple[str, ...]):
    """Run scraper for configured queries."""
    app_config = load_config(config)
    queries = app_config.queries

    if query:
        query_set = set(query)
        queries = [q for q in queries if q.name in query_set]
        missing = query_set - {q.name for q in queries}
        if missing:
            click.echo(f"Warning: unknown queries: {', '.join(missing)}")

    if dry_run:
        click.echo("Dry run - would scrape the following queries:")
        for q in queries:
            click.echo(f"  {q.name}: {q.url} (max {q.max_pages} pages)")
        click.echo(f"\nSettings: delay={app_config.settings.delay_range}s, "
                    f"retries={app_config.settings.max_retries}, "
                    f"output={app_config.settings.output_dir}")
        return

    client = OtomotoClient(
        delay_range=tuple(app_config.settings.delay_range),
        proxy=app_config.settings.proxy,
        max_retries=app_config.settings.max_retries,
    )
    client.start_session()

    total_listings = 0
    total_pages = 0
    total_failed = 0

    try:
        for q in queries:
            click.echo(f"\nScraping: {q.name}")
            result = scrape_query(q, app_config.settings, client=client)

            if result.listings:
                write_raw_json(result.listings, q.name, app_config.settings.output_dir)
                append_csv(result.listings, app_config.settings.output_dir)

            total_listings += len(result.listings)
            total_pages += result.pages_scraped
            total_failed += result.pages_failed

            click.echo(
                f"  {q.name}: {len(result.listings)} listings, "
                f"{result.pages_scraped} pages OK, {result.pages_failed} failed"
            )
    finally:
        client.close()

    click.echo(f"\nDone: {total_listings} listings from {total_pages} pages "
               f"({total_failed} pages failed)")


@cli.command()
@click.option("--config", default="config.yaml", help="Path to config file.")
def stats(config: str):
    """Show dataset statistics."""
    app_config = load_config(config)
    csv_path = Path(app_config.settings.output_dir) / "consolidated" / "all_listings.csv"

    if not csv_path.exists():
        click.echo("No data found. Run the scraper first.")
        return

    with open(csv_path) as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        click.echo("No data found. CSV is empty.")
        return

    dates = sorted({r["scraped_at"][:10] for r in rows if r.get("scraped_at")})
    queries = sorted({r["query_name"] for r in rows if r.get("query_name")})
    unique_ids = len({r["listing_id"] for r in rows})

    click.echo(f"Total rows: {len(rows)}")
    click.echo(f"Unique listings: {unique_ids}")
    click.echo(f"Date range: {dates[0]} to {dates[-1]}" if dates else "No dates")
    click.echo(f"Queries: {', '.join(queries)}" if queries else "No queries")
    click.echo(f"Scrape days: {len(dates)}")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_cli.py -v`
Expected: All 4 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/otomotoskrap/cli.py tests/test_cli.py
git commit -m "feat: add Click CLI with run, stats, and dry-run commands"
```

---

## Task 9: Integration Wiring + Manual Verification

**Files:**
- Modify: `src/otomotoskrap/__init__.py`

- [ ] **Step 1: Run the full test suite**

Run: `pytest tests/ -v`
Expected: All tests PASS (models: 8, config: 4, parser: 8, client: 7, storage: 6, scraper: 5, cli: 4 = 42 tests)

- [ ] **Step 2: Test dry-run with real config**

Run: `python -m otomotoskrap run --dry-run`
Expected: Output showing configured queries and settings, no HTTP requests made.

- [ ] **Step 3: Test single-page live scrape**

Create a test config with `max_pages: 1`:

```bash
python -c "
import yaml
config = {
    'queries': [{'name': 'bmw_test', 'url': 'https://www.otomoto.pl/osobowe/bmw/seria-3', 'max_pages': 1}],
    'settings': {'delay_range': [2, 5], 'output_dir': './data'}
}
with open('config_test.yaml', 'w') as f:
    yaml.dump(config, f)
"
```

Run: `python -m otomotoskrap run --config config_test.yaml`

Expected:
- Terminal shows: scraping page 1, pagination detected with total count, query_complete with listing count
- `data/raw/2026-04-14/bmw_test.json` exists with ~32 listing records
- `data/consolidated/all_listings.csv` exists with header + ~32 rows

- [ ] **Step 4: Verify JSON output**

Run: `python -c "import json; data = json.load(open('data/raw/$(date +%Y-%m-%d)/bmw_test.json')); print(f'Listings: {len(data)}'); print(json.dumps(data[0], indent=2, ensure_ascii=False))"`

Expected: Listing with all fields populated (listing_id, title, brand, model, year, price, mileage_km, fuel_type, transmission, location_city, location_region, seller_type).

- [ ] **Step 5: Verify CSV output**

Run: `python -c "import csv; rows = list(csv.DictReader(open('data/consolidated/all_listings.csv'))); print(f'Rows: {len(rows)}'); print(f'Fields: {list(rows[0].keys())}')" `

Expected: Rows with 20+ fields including all expected columns.

- [ ] **Step 6: Test stats command**

Run: `python -m otomotoskrap stats --config config_test.yaml`

Expected: Output showing total rows, unique listings, date range, queries.

- [ ] **Step 7: Clean up test config and commit**

```bash
rm config_test.yaml
git add -A
git commit -m "feat: complete integration wiring and verify end-to-end"
```

---

## Notes

### Fields not available from search results
`body_type`, `color`, and `is_new` are not present in the search results page data (neither in `__NEXT_DATA__` nor in the HTML). These would require visiting individual listing pages — left as `None` for now. Future enhancement: optionally follow listing URLs to enrich records.

### Pagination strategy
Otomoto uses offset-based pagination: `?page=1` → offset 0, `?page=2` → offset 32, etc. The `totalCount` from the first page determines bounds. The scraper stops when either `page >= total_pages` or `page >= max_pages` from config.

### Anti-detection is conservative
The default 2-5 second delay between requests means a full 50-page scrape takes ~4 minutes per query. This is by design for daily batch use. Aggressive scraping (lower delays, higher concurrency) is not needed and would risk blocks.
