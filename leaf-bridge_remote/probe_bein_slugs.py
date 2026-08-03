"""Probe which slug format cinearena uses for bein channels."""
import asyncio, sys
sys.path.insert(0, ".")
from dotenv import load_dotenv
load_dotenv()
from cinearena_recorder import scrape_channel

CANDIDATES = [
    "bein1", "bein2", "bein3",
    "bein-1", "bein-2", "bein-3",
    "bein_1", "bein_2", "bein_3",
    "beinsports1", "beinsports2", "beinsports3",
    "bein-sports-1", "bein-sports-2", "bein-sports-3",
]

async def main():
    for ch in CANDIDATES:
        r = await scrape_channel(ch)
        if r:
            print(f"  FOUND: {ch} -> {r['stream_url'][:70]}")
        else:
            print(f"  miss:  {ch}")

asyncio.run(main())
