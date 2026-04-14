"""
Pydantic models for validation.
"""
from pydantic import BaseModel, EmailStr, field_validator
from typing import Optional


class CampaignCreate(BaseModel):
    name: str
    niches: list[str]
    countries: list[str]
    cities: list[str]

    @field_validator("niches", "countries", "cities", mode="before")
    @classmethod
    def split_if_string(cls, v):
        if isinstance(v, str):
            return [x.strip() for x in v.split(",") if x.strip()]
        return v


class URLRecord(BaseModel):
    campaign_id: int
    url: str
    domain: str
    niche: Optional[str] = None
    city: Optional[str] = None
    country: Optional[str] = None


class EmailRecord(BaseModel):
    email: str
    domain: str
    source_url: str
    source_domain: str
    campaign_id: int
    niche: Optional[str] = None
    city: Optional[str] = None
    country: Optional[str] = None
    is_generic: int = 0
