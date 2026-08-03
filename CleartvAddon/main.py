"""
main.py - ClearTV addon entry point.

Startup does three things: validate the environment, open the pooled leaf
client, and load the channel list once before accepting traffic. After that a
single background loop keeps the snapshot fresh; every request is served from
memory.

Run with:  python main.py     (or via the systemd unit in deploy/)
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, ORJSONResponse, PlainTextResponse

import config

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s  %(levelname)-8s  %(name)s - %(message)s",
)
logger = logging.getLogger("cleartv.main")


async def _refresh_loop() -> None:
    """
    Keep the channel snapshot fresh.

    A failed refresh retries on the shorter REFRESH_RETRY_INTERVAL — the leaf
    node restarting should cost seconds of staleness, not a full cycle.
    """
    from channel_store import store

    while True:
        ok = await store.refresh()
        delay = config.REFRESH_INTERVAL if ok else config.REFRESH_RETRY_INTERVAL
        await asyncio.sleep(delay)


async def _bucket_cleanup_loop() -> None:
    """Drop idle rate-limit buckets so the dict cannot grow without bound."""
    import time

    while True:
        await asyncio.sleep(300)
        cutoff = time.monotonic() - 300
        for ip in [ip for ip, (_, last) in _buckets.items() if last < cutoff]:
            _buckets.pop(ip, None)


@asynccontextmanager
async def lifespan(app: FastAPI):
    import leaf_client
    from channel_store import store

    # Validated here rather than only under __main__, so an ASGI launch
    # (uvicorn main:app / the systemd unit) fails just as loudly on a bad env.
    config.validate()

    logger.info(
        "%s v%s starting — leaf: %s",
        config.ADDON_NAME,
        config.ADDON_VERSION,
        config.LEAF_BASE_URL,
    )

    await leaf_client.init_client()

    # Load once up front so the first request sees a populated catalog rather
    # than an empty one. A failure here is not fatal: the loop keeps retrying
    # and /health reports the problem.
    if not await store.refresh():
        logger.warning("initial channel load failed — retrying in the background")

    tasks = [
        asyncio.create_task(_refresh_loop()),
        asyncio.create_task(_bucket_cleanup_loop()),
    ]

    yield

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await leaf_client.close_client()
    logger.info("%s shut down.", config.ADDON_NAME)


app = FastAPI(
    title=config.ADDON_NAME,
    description=config.ADDON_DESCRIPTION,
    version=config.ADDON_VERSION,
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
    openapi_url=None,
    docs_url=None,
    redoc_url=None,
    redirect_slashes=False,
)

# Stremio calls the addon from arbitrary origins, so CORS must be permissive.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "HEAD", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=500)


# ── Per-IP rate limit ─────────────────────────────────────────────────────────
# The addon serves only small JSON documents (it is never in the media path), so
# one generous bucket per IP is enough to stop a broken client from hammering
# the leaf node through us.

_buckets: dict[str, tuple[float, float]] = {}  # ip → (tokens, last_refill_mono)


def _rate_ok(ip: str) -> bool:
    import time

    if ip in config.RATE_LIMIT_BYPASS_IPS:
        return True

    now = time.monotonic()
    tokens, last = _buckets.get(ip, (float(config.RATE_LIMIT_MAX), now))
    tokens = min(
        float(config.RATE_LIMIT_MAX),
        tokens + ((now - last) / config.RATE_LIMIT_WINDOW) * config.RATE_LIMIT_MAX,
    )
    if tokens < 1:
        _buckets[ip] = (tokens, now)
        return False
    _buckets[ip] = (tokens - 1, now)
    return True


async def _bucket_cleanup_loop() -> None:
    """Drop idle rate-limit buckets so the dict cannot grow without bound."""
    import time

    while True:
        await asyncio.sleep(300)
        cutoff = time.monotonic() - 600
        for ip in [ip for ip, (_, last) in _buckets.items() if last < cutoff]:
            _buckets.pop(ip, None)


def _client_ip(request: Request) -> str:
    """Real client IP behind Cloudflare / nginx."""
    cf = request.headers.get("CF-Connecting-IP")
    if cf:
        return cf.strip()
    xff = request.headers.get("X-Forwarded-For", "")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


@app.middleware("http")
async def rate_limit(request: Request, call_next):
    if not config.RATE_LIMIT_ENABLED or request.url.path in ("/health", "/"):
        return await call_next(request)

    if not _rate_ok(_client_ip(request)):
        return JSONResponse(
            status_code=429,
            content={"error": "Too many requests"},
            headers={"Retry-After": "10"},
        )
    return await call_next(request)


@app.exception_handler(Exception)
async def unhandled_error(request: Request, exc: Exception):
    logger.exception("unhandled error on %s: %s", request.url.path, exc)
    return JSONResponse(status_code=500, content={"error": "Internal server error"})


from routes import router  # noqa: E402  (imported after app config by design)

app.include_router(router)


@app.get("/", include_in_schema=False)
async def root():
    return PlainTextResponse(f"{config.ADDON_NAME} v{config.ADDON_VERSION} — /configure to install")


@app.get("/health", include_in_schema=False)
async def health():
    from channel_store import store

    snapshot = store.status()
    return {
        "status": "ok" if not snapshot["stale"] else "degraded",
        "addon": config.ADDON_NAME,
        "version": config.ADDON_VERSION,
        "leaf": config.LEAF_BASE_URL,
        "snapshot": snapshot,
    }


if __name__ == "__main__":
    try:
        config.validate()
    except config.ConfigError as exc:
        raise SystemExit(f"ClearTV: {exc}")

    uvicorn.run(
        "main:app",
        host=config.HOST,
        port=config.PORT,
        reload=os.getenv("RELOAD", "").lower() in ("1", "true"),
        log_level=config.LOG_LEVEL.lower(),
    )
