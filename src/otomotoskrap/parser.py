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
