"""
worker_client.py — Thin HTTP client for master→worker communication.

Used by master to route start/stop commands to worker VPS nodes.
"""

from __future__ import annotations

import logging
from typing import Optional

import httpx

logger = logging.getLogger("leaf.worker_client")


async def start_on_worker(
    worker_url: str,
    slug:       str,
    stream_url: str,
    headers:    dict,
    proxies:    list[str],
    standalone: bool = False,
) -> bool:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{worker_url}/internal/start",
                json={
                    "slug":       slug,
                    "stream_url": stream_url,
                    "headers":    headers,
                    "proxies":    proxies,
                    "standalone": standalone,
                },
            )
        return resp.status_code == 200
    except Exception as exc:
        logger.warning("start_on_worker(%s, %s) failed: %s", worker_url, slug, exc)
        return False


async def stop_on_worker(worker_url: str, slug: str) -> bool:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.delete(f"{worker_url}/internal/stop/{slug}")
        return resp.status_code == 200
    except Exception as exc:
        logger.warning("stop_on_worker(%s, %s) failed: %s", worker_url, slug, exc)
        return False


async def status_of_worker(worker_url: str) -> Optional[dict]:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(f"{worker_url}/internal/status")
        if resp.status_code == 200:
            return resp.json()
        return None
    except Exception as exc:
        logger.warning("status_of_worker(%s) failed: %s", worker_url, exc)
        return None
