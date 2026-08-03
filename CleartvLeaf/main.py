"""
main.py - ClearTV leaf entry point.

Startup order matters:
  1. validate the environment (refuses to run without an admin password)
  2. clear stale account claims from the previous run — nothing is streaming yet,
     so any claim still on disk is a leftover that would shrink the usable pool
  3. load the lineup and re-claim an account for each live channel
  4. start the background loops and bind the server

Run with:  python main.py
"""

from __future__ import annotations

import asyncio
import logging

from aiohttp import web

import config

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s  %(levelname)-8s  %(name)s - %(message)s",
)
logger = logging.getLogger("leaf.main")


async def run() -> None:
    import accounts
    import admin
    import relay
    import server
    from channels import store

    config.validate()

    for warning in config.warnings():
        logger.warning("%s", warning)

    logger.info("ClearTV leaf starting — public URL %s", config.PUBLIC_URL)

    accounts.reset_claims()
    store.load()
    await admin.restore()

    tasks = [
        asyncio.create_task(accounts.refresh_loop()),
        asyncio.create_task(relay.sweep_loop()),
    ]

    app = server.build_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, config.HOST, config.PORT, backlog=512)
    await site.start()

    stats = store.stats()
    logger.info(
        "listening on http://%s:%d — %d channel(s) live, admin at %s/admin",
        config.HOST,
        config.PORT,
        stats["enabled"],
        config.PUBLIC_URL,
    )

    try:
        # Sleep forever; the server runs in the background from here.
        await asyncio.Event().wait()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await relay.close_client()
        await runner.cleanup()
        logger.info("ClearTV leaf stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except config.ConfigError as exc:
        raise SystemExit(f"ClearTV leaf: {exc}")
    except KeyboardInterrupt:
        pass
