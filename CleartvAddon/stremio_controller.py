"""
stremio_controller.py - builds every Stremio protocol response.

All data comes from the in-memory channel snapshot (channel_store), so these
functions are pure and fast — no network I/O on the request path. The background
refresh loop in main.py is the only thing that talks to the leaf node.
"""

from __future__ import annotations

import logging

from channel_store import Channel, store
from config import (
    ADDON_BACKGROUND,
    ADDON_DESCRIPTION,
    ADDON_ID,
    ADDON_LOGO,
    ADDON_NAME,
    ADDON_VERSION,
    CATALOG_ID,
    CATALOG_NAME,
    ID_PREFIX,
    PAGE_SIZE,
    STREMIO_TYPE,
)
from models import (
    BehaviorHints,
    CatalogDef,
    CatalogExtra,
    CatalogResponse,
    Manifest,
    MetaDetail,
    MetaPreview,
    MetaResponse,
    Stream,
    StreamBehaviorHints,
    StreamResponse,
)
from user_config import DEFAULT, SORT_NAME, UserConfig

logger = logging.getLogger("cleartv.controller")


# ── helpers ───────────────────────────────────────────────────────────────────

def strip_prefix(stremio_id: str) -> str:
    """'cleartv:sky-sports-main' → 'sky-sports-main'."""
    prefix = f"{ID_PREFIX}:"
    return stremio_id[len(prefix):] if stremio_id.startswith(prefix) else stremio_id


def _visible(cfg: UserConfig) -> list[Channel]:
    """Channels this user should see, in their preferred order."""
    channels = [
        c for c in store.all(include_buffering=cfg.show_buffering) if cfg.allows(c.categories)
    ]
    if cfg.sort == SORT_NAME:
        channels.sort(key=lambda c: c.name.lower())
    return channels


def _display_name(channel: Channel, cfg: UserConfig) -> str:
    """Catalog tile label, optionally suffixed with the resolved quality."""
    if cfg.show_quality and channel.quality:
        return f"{channel.name} · {channel.quality}"
    return channel.name


def _preview(channel: Channel, cfg: UserConfig) -> MetaPreview:
    return MetaPreview(
        id=channel.stremio_id,
        type=STREMIO_TYPE,
        name=_display_name(channel, cfg),
        poster=channel.poster or None,
        posterShape="landscape",
        background=channel.poster or None,
        description=channel.description or None,
        genres=list(channel.categories) or None,
        releaseInfo="Buffering" if channel.buffering else "Live",
    )


# ── manifest ──────────────────────────────────────────────────────────────────

async def get_manifest(cfg: UserConfig = DEFAULT) -> Manifest:
    """
    The addon manifest.

    Genre options are filled from the live snapshot so the category dropdown in
    Stremio always matches what the leaf node is actually serving. Hidden
    categories are removed so a configured install never offers an empty filter.
    """
    genres = [c for c in store.categories() if c.lower() not in cfg.hidden_categories]

    extra = [CatalogExtra(name="search")]
    if genres:
        extra.append(CatalogExtra(name="genre", options=genres, optionsLimit=1))
    extra.append(CatalogExtra(name="skip"))

    return Manifest(
        id=ADDON_ID,
        version=ADDON_VERSION,
        name=ADDON_NAME,
        description=ADDON_DESCRIPTION,
        resources=["catalog", "meta", "stream"],
        types=[STREMIO_TYPE],
        catalogs=[
            CatalogDef(
                type=STREMIO_TYPE,
                id=CATALOG_ID,
                name=CATALOG_NAME,
                extra=extra,
            )
        ],
        idPrefixes=[f"{ID_PREFIX}:"],
        logo=ADDON_LOGO or None,
        background=ADDON_BACKGROUND or None,
        behaviorHints=BehaviorHints(configurable=True, configurationRequired=False),
    )


# ── catalog ───────────────────────────────────────────────────────────────────

async def get_catalog(
    catalog_id: str,
    *,
    skip: int = 0,
    genre: str | None = None,
    search: str | None = None,
    cfg: UserConfig = DEFAULT,
) -> CatalogResponse:
    """One page of channel tiles, optionally filtered by genre or search text."""
    if catalog_id != CATALOG_ID:
        return CatalogResponse(metas=[])

    channels = _visible(cfg)

    if genre:
        wanted = genre.strip().lower()
        channels = [c for c in channels if any(cat.lower() == wanted for cat in c.categories)]

    if search:
        needle = search.strip().lower()
        if needle:
            channels = [
                c
                for c in channels
                if needle in c.name.lower()
                or needle in c.slug.lower()
                or any(needle in cat.lower() for cat in c.categories)
            ]

    skip = max(0, skip)
    page = channels[skip: skip + PAGE_SIZE]
    return CatalogResponse(metas=[_preview(c, cfg) for c in page])


# ── meta ──────────────────────────────────────────────────────────────────────

async def get_meta(stremio_id: str, *, cfg: UserConfig = DEFAULT) -> MetaResponse:
    """Detail page for one channel."""
    channel = store.get(strip_prefix(stremio_id))
    if channel is None:
        return MetaResponse(meta=None)

    lines: list[str] = []
    if channel.description:
        lines.append(channel.description)
    if channel.categories:
        lines.append("Category: " + ", ".join(channel.categories))
    if channel.quality:
        lines.append(f"Quality: {channel.quality}")
    if channel.buffering:
        lines.append("Buffering — playback starts in a few seconds.")

    detail = MetaDetail(
        id=channel.stremio_id,
        type=STREMIO_TYPE,
        name=_display_name(channel, cfg),
        poster=channel.poster or None,
        posterShape="landscape",
        background=channel.poster or None,
        logo=channel.poster or None,
        description="\n".join(lines) or None,
        genres=list(channel.categories) or None,
        releaseInfo="Buffering" if channel.buffering else "Live",
    )
    return MetaResponse(meta=detail)


# ── stream ────────────────────────────────────────────────────────────────────

async def get_stream(stremio_id: str, *, cfg: UserConfig = DEFAULT) -> StreamResponse:
    """
    The playable stream for a channel.

    The leaf node's playlist URL is handed to Stremio directly — this addon is
    not in the media path, so there is nothing to proxy, rewrite, or rate-limit
    per stream. No quality gating: whatever the leaf serves is what plays.
    """
    slug = strip_prefix(stremio_id)
    channel = store.get(slug)
    if channel is None:
        logger.info("stream: unknown channel %r", slug)
        return StreamResponse(streams=[])

    if not channel.playlist_url:
        logger.warning("stream: channel %r has no playlist URL", slug)
        return StreamResponse(streams=[])

    label_bits = [channel.quality or "Live"]
    if channel.buffering:
        label_bits.append("buffering")
    if channel.source:
        label_bits.append(channel.source)

    stream = Stream(
        url=channel.playlist_url,
        name=ADDON_NAME,
        title=f"{channel.name}\n{' · '.join(label_bits)}",
        behaviorHints=StreamBehaviorHints(
            notWebReady=False,
            # Keeps Stremio from treating consecutive plays of the same channel
            # as separate items in the "continue watching" row.
            bingeGroup=f"{ID_PREFIX}-{slug}",
        ),
    )
    return StreamResponse(streams=[stream])
