from datetime import datetime
from typing import ClassVar, Optional

from pydantic import BaseModel


_LISTING_CSV_FIELDS: list[str] = [
    "listing_id",
    "url",
    "title",
    "brand",
    "model",
    "year",
    "price",
    "currency",
    "mileage_km",
    "fuel_type",
    "body_type",
    "transmission",
    "engine_capacity_cm3",
    "engine_power_hp",
    "color",
    "location_city",
    "location_region",
    "seller_type",
    "is_new",
    "scraped_at",
]


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
        return _LISTING_CSV_FIELDS

    def to_csv_row(self) -> list[str]:
        values = []
        for field_name in _LISTING_CSV_FIELDS:
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
