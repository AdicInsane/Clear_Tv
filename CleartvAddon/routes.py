"""
routes.py - Stremio addon protocol endpoints.

Every resource exists twice: a bare form and a /{user_config}/ form. Stremio
calls the configured form once a user installs from the configure page.

    GET /manifest.json
    GET /catalog/tv/{id}.json
    GET /catalog/tv/{id}/{extra}.json
    GET /meta/tv/{id}.json
    GET /stream/tv/{id}.json

Route order matters: the literal paths are declared before the /{user_config}/
ones so a real protocol path is never swallowed as a config blob.
"""

from __future__ import annotations

import logging
from urllib.parse import unquote_plus

from fastapi import APIRouter, Path, Query
from fastapi.responses import HTMLResponse, ORJSONResponse, RedirectResponse, Response

import stremio_controller as ctrl
import user_config
from channel_store import store
from config import ADDON_LOGO, ADDON_NAME, ADDON_VERSION, CATALOG_ID

logger = logging.getLogger("cleartv.routes")

router = APIRouter(default_response_class=ORJSONResponse)

# Stremio may re-request the manifest often; a short cache keeps it cheap without
# holding a stale genre list for long.
_MANIFEST_HEADERS = {"Cache-Control": "public, max-age=60"}
_CATALOG_HEADERS = {"Cache-Control": "public, max-age=30"}
_STREAM_HEADERS = {"Cache-Control": "no-store"}


def _parse_extra(extra: str) -> dict:
    """
    Parse Stremio's extra path segment: 'genre=Sports&skip=100'.

    Unknown keys are ignored. A non-numeric skip becomes 0 rather than a 422,
    because a malformed client request should degrade to page one.
    """
    out: dict = {"skip": 0, "genre": None, "search": None}
    for part in extra.split("&"):
        if "=" not in part:
            continue
        key, _, value = part.partition("=")
        value = unquote_plus(value).strip()
        key = key.strip().lower()
        if key == "skip":
            try:
                out["skip"] = max(0, int(value))
            except ValueError:
                pass
        elif key == "genre":
            out["genre"] = value or None
        elif key == "search":
            out["search"] = value or None
    return out


def _json(model, headers: dict | None = None) -> ORJSONResponse:
    return ORJSONResponse(model.model_dump(exclude_none=True), headers=headers)


# ── manifest ──────────────────────────────────────────────────────────────────

@router.get("/manifest.json")
async def manifest():
    return _json(await ctrl.get_manifest(), _MANIFEST_HEADERS)


# ── catalog ───────────────────────────────────────────────────────────────────

@router.get("/catalog/{type}/{catalog_id}.json")
async def catalog(
    type: str = Path(...),
    catalog_id: str = Path(...),
    skip: int = Query(0, ge=0),
    genre: str | None = Query(None),
    search: str | None = Query(None),
):
    data = await ctrl.get_catalog(catalog_id, skip=skip, genre=genre, search=search)
    return _json(data, _CATALOG_HEADERS)


@router.get("/catalog/{type}/{catalog_id}/{extra}.json")
async def catalog_extra(
    type: str = Path(...),
    catalog_id: str = Path(...),
    extra: str = Path(...),
):
    parsed = _parse_extra(extra)
    data = await ctrl.get_catalog(catalog_id, **parsed)
    return _json(data, _CATALOG_HEADERS)


# ── meta ──────────────────────────────────────────────────────────────────────

@router.get("/meta/{type}/{stremio_id}.json")
async def meta(type: str = Path(...), stremio_id: str = Path(...)):
    data = await ctrl.get_meta(unquote_plus(stremio_id))
    return _json(data, _CATALOG_HEADERS)


# ── stream ────────────────────────────────────────────────────────────────────

@router.get("/stream/{type}/{stremio_id}.json")
async def stream(type: str = Path(...), stremio_id: str = Path(...)):
    data = await ctrl.get_stream(unquote_plus(stremio_id))
    return _json(data, _STREAM_HEADERS)


# ── configure page + its data endpoints ───────────────────────────────────────

@router.get("/configure", response_class=HTMLResponse, include_in_schema=False)
async def configure():
    from configure import configure_html

    return HTMLResponse(configure_html())


@router.get("/categories.json", include_in_schema=False)
async def categories():
    """Category list for the configure page's checkbox grid."""
    return ORJSONResponse(
        {"categories": store.categories(), "channels": len(store.all())},
        headers={"Cache-Control": "public, max-age=30"},
    )


@router.get("/channels.json", include_in_schema=False)
async def channels():
    """Flat channel list — useful for checking what the leaf is serving."""
    return ORJSONResponse(
        {
            "count": len(store.all()),
            "channels": [
                {
                    "id": c.stremio_id,
                    "slug": c.slug,
                    "name": c.name,
                    "categories": list(c.categories),
                    "quality": c.quality or None,
                    "buffering": c.buffering,
                    "source": c.source or None,
                }
                for c in store.all()
            ],
        },
        headers={"Cache-Control": "no-store"},
    )


# ── static ────────────────────────────────────────────────────────────────────

@router.get("/logo.png", include_in_schema=False)
async def logo():
    import os

    path = os.path.join(os.path.dirname(__file__), "assets", "logo.png")
    if os.path.exists(path):
        with open(path, "rb") as fh:
            return Response(
                content=fh.read(),
                media_type="image/png",
                headers={"Cache-Control": "public, max-age=86400"},
            )
    if ADDON_LOGO and not ADDON_LOGO.endswith("/logo.png"):
        return RedirectResponse(ADDON_LOGO, status_code=302)
    return Response(status_code=404)


@router.get("/favicon.ico", include_in_schema=False)
async def favicon():
    import os

    path = os.path.join(os.path.dirname(__file__), "assets", "favicon.ico")
    if os.path.exists(path):
        with open(path, "rb") as fh:
            return Response(
                content=fh.read(),
                media_type="image/x-icon",
                headers={"Cache-Control": "public, max-age=86400"},
            )
    return Response(status_code=404)


# ── configured (per-user) variants ────────────────────────────────────────────
# Declared last so they cannot shadow the literal routes above.

@router.get("/{cfg}/manifest.json")
async def manifest_configured(cfg: str = Path(...)):
    return _json(await ctrl.get_manifest(user_config.decode(cfg)), _MANIFEST_HEADERS)


@router.get("/{cfg}/catalog/{type}/{catalog_id}.json")
async def catalog_configured(
    cfg: str = Path(...),
    type: str = Path(...),
    catalog_id: str = Path(...),
    skip: int = Query(0, ge=0),
    genre: str | None = Query(None),
    search: str | None = Query(None),
):
    data = await ctrl.get_catalog(
        catalog_id, skip=skip, genre=genre, search=search, cfg=user_config.decode(cfg)
    )
    return _json(data, _CATALOG_HEADERS)


@router.get("/{cfg}/catalog/{type}/{catalog_id}/{extra}.json")
async def catalog_extra_configured(
    cfg: str = Path(...),
    type: str = Path(...),
    catalog_id: str = Path(...),
    extra: str = Path(...),
):
    parsed = _parse_extra(extra)
    data = await ctrl.get_catalog(catalog_id, **parsed, cfg=user_config.decode(cfg))
    return _json(data, _CATALOG_HEADERS)


@router.get("/{cfg}/meta/{type}/{stremio_id}.json")
async def meta_configured(
    cfg: str = Path(...), type: str = Path(...), stremio_id: str = Path(...)
):
    data = await ctrl.get_meta(unquote_plus(stremio_id), cfg=user_config.decode(cfg))
    return _json(data, _CATALOG_HEADERS)


@router.get("/{cfg}/stream/{type}/{stremio_id}.json")
async def stream_configured(
    cfg: str = Path(...), type: str = Path(...), stremio_id: str = Path(...)
):
    data = await ctrl.get_stream(unquote_plus(stremio_id), cfg=user_config.decode(cfg))
    return _json(data, _STREAM_HEADERS)


@router.get("/{cfg}/configure", include_in_schema=False)
async def configure_configured(cfg: str = Path(...)):
    """
    Stremio links an installed addon's settings to /{config}/configure. Redirect
    to the plain page with the config in the fragment so the form pre-fills from
    the user's existing selections.
    """
    return RedirectResponse(f"/configure#{cfg}", status_code=302)
