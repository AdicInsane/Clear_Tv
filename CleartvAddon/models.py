"""
models.py - Stremio addon protocol schemas.

Field names follow the Stremio addon SDK exactly; anything left as None is
stripped before the response is sent (see routes.py).
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class CatalogExtra(BaseModel):
    name: str
    isRequired: bool = False
    options: list[str] | None = None
    optionsLimit: int | None = None


class CatalogDef(BaseModel):
    type: str
    id: str
    name: str
    extra: list[CatalogExtra] | None = None


class BehaviorHints(BaseModel):
    """Manifest-level hints."""

    configurable: bool = True
    configurationRequired: bool = False


class Manifest(BaseModel):
    id: str
    version: str
    name: str
    description: str
    resources: list[str]
    types: list[str]
    catalogs: list[CatalogDef]
    idPrefixes: list[str] | None = None
    logo: str | None = None
    background: str | None = None
    behaviorHints: BehaviorHints | None = None


class MetaPreview(BaseModel):
    """A single tile in a catalog listing."""

    id: str
    type: str
    name: str
    poster: str | None = None
    posterShape: str | None = None
    background: str | None = None
    logo: str | None = None
    description: str | None = None
    genres: list[str] | None = None
    releaseInfo: str | None = None


class MetaDetail(MetaPreview):
    """The detail page for one channel."""

    country: str | None = None
    website: str | None = None


class CatalogResponse(BaseModel):
    metas: list[MetaPreview] = Field(default_factory=list)


class MetaResponse(BaseModel):
    meta: MetaDetail | None = None


class StreamBehaviorHints(BaseModel):
    # Live content must not resume from a saved position.
    notWebReady: bool | None = None
    bingeGroup: str | None = None


class Stream(BaseModel):
    url: str
    name: str | None = None
    title: str | None = None
    description: str | None = None
    behaviorHints: StreamBehaviorHints | None = None


class StreamResponse(BaseModel):
    streams: list[Stream] = Field(default_factory=list)
