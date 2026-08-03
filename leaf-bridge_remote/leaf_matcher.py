"""
leaf_matcher.py - Match SKTech (Leaf) events to streamed.pk live matches.

Reuses the exact same matching logic as sportsaddon:
  - match_name_matcher.py fuzzy logic (team names + sport compat)
  - groq_matcher.py LLM fallback for difficult name pairs
  - MongoDB Atlas for shared Groq cache across all nodes

The Leaf events use different sport category names than streamed.pk,
so we map them here before passing to the fuzzy matcher.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import os
import time
from difflib import SequenceMatcher
import re
import unicodedata

import httpx

from config import ADDON_API_BASE, ADDON_ADMIN_TOKEN, GROQ_API_KEY, MONGODB_URI

# Groq key may be comma-separated list (same as sportsaddon) — use first one
_GROQ_KEY = GROQ_API_KEY.split(",")[0].strip() if GROQ_API_KEY else ""

logger = logging.getLogger("leaf.matcher")

# ── Sport category mapping: SKTech → streamed.pk ─────────────────────────────
# SKTech uses these category strings in its events.txt
_SKTECH_TO_STREAMED: dict[str, str] = {
    "cricket":          "cricket",
    "football":         "football",
    "soccer":           "football",
    "basketball":       "basketball",
    "tennis":           "tennis",
    "hockey":           "hockey",
    "ice hockey":       "hockey",
    "rugby":            "rugby",
    "golf":             "golf",
    "motorsport":       "motor-sports",
    "motor sport":      "motor-sports",
    "formula 1":        "motor-sports",
    "f1":               "motor-sports",
    "boxing":           "fight",
    "mma":              "fight",
    "ufc":              "fight",
    "combat":           "fight",
    "baseball":         "baseball",
    "afl":              "afl",
    "darts":            "darts",
    "billiards":        "billiards",
    "snooker":          "billiards",
}

# ── Noise words / normalisation (mirrors match_name_matcher.py) ───────────────
_NOISE = re.compile(
    r"\b(vs?|at|the|fc|afc|cfc|cf|sc|ac|bfc|utd|united|city|town|"
    r"real|club|sporting|athletic|atletico|inter|dynamo|lokomotiv|"
    r"live|stream|hd|sd|watch|online)\b",
    re.IGNORECASE,
)
_PUNCT  = re.compile(r"[^\w\s]")
_SPACES = re.compile(r"\s+")


def _normalise(text: str) -> str:
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.lower()
    text = _PUNCT.sub(" ", text)
    text = _NOISE.sub(" ", text)
    text = _SPACES.sub(" ", text).strip()
    return text


def _ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def _team_in_title(team: str, title_norm: str) -> bool:
    tn = _normalise(team)
    if not tn:
        return False
    if tn in title_norm:
        return True
    tokens = title_norm.split()
    team_tokens = tn.split()
    n = len(team_tokens)
    for i in range(len(tokens) - n + 1):
        window = " ".join(tokens[i: i + n])
        if _ratio(tn, window) >= 0.82:
            return True
    return _ratio(tn, title_norm) >= 0.82


def _map_sktech_sport(sktech_category: str) -> str:
    """Convert a SKTech category string to a streamed.pk category ID."""
    return _SKTECH_TO_STREAMED.get(sktech_category.lower().strip(), sktech_category.lower())


def _sports_compatible(sktech_cat: str, streamed_cat: str) -> bool:
    """Return True if the SKTech category maps to the streamed.pk category."""
    mapped = _map_sktech_sport(sktech_cat)
    return mapped == streamed_cat or mapped in streamed_cat or streamed_cat in mapped


def fuzzy_match_event(leaf_event: dict, streamed_matches: list[dict]) -> dict | None:
    """
    Try to find a confident fuzzy match for a Leaf event in the streamed.pk
    live match list. Returns the matched streamed match dict or None.

    Matching gates (same philosophy as match_name_matcher.py):
      1. Sport category must be compatible
      2. Both team names must appear in the streamed title (if known)
      3. Overall title similarity >= 0.72
    """
    team_a = leaf_event.get("team_a", "")
    team_b = leaf_event.get("team_b", "")
    leaf_cat = leaf_event.get("category", "")

    # Build a synthetic title from the Leaf event for whole-title comparison
    if team_a and team_b:
        leaf_title = f"{team_a} vs {team_b}"
    else:
        leaf_title = leaf_event.get("event_name", "")

    leaf_norm = _normalise(leaf_title)
    team_a_norm = _normalise(team_a)
    team_b_norm = _normalise(team_b)
    have_teams = bool(team_a_norm or team_b_norm)

    for sm in streamed_matches:
        sm_cat   = sm.get("category", "")
        sm_title = sm.get("title", "")
        sm_norm  = _normalise(sm_title)

        teams    = sm.get("teams") or {}
        home     = (teams.get("home") or {}).get("name", "")
        away     = (teams.get("away") or {}).get("name", "")

        # Gate 1: sport compatibility
        if not _sports_compatible(leaf_cat, sm_cat):
            continue

        if have_teams:
            # Gate 2: both team names must appear
            a_found = _team_in_title(team_a, sm_norm) if team_a_norm else True
            b_found = _team_in_title(team_b, sm_norm) if team_b_norm else True

            # Also try the reverse: streamed team names in leaf title
            if not a_found and home:
                a_found = _team_in_title(home, leaf_norm)
            if not b_found and away:
                b_found = _team_in_title(away, leaf_norm)

            if not (a_found and b_found):
                continue

            # Gate 3: whole-title similarity
            if _ratio(leaf_norm, sm_norm) < 0.72:
                continue
        else:
            # No team names — pure title similarity
            if _ratio(leaf_norm, sm_norm) < 0.88:
                continue

        logger.info(
            "Fuzzy match: '%s' → '%s' (sport: %s→%s)",
            leaf_title, sm_title, leaf_cat, sm_cat,
        )
        return sm

    return None


# ── Groq LLM matching (same pattern as sportsaddon/groq_matcher.py) ───────────
# We maintain a separate in-memory map for Leaf events to avoid polluting
# the sportsaddon Groq cache with Leaf-specific entries.
_leaf_groq_map: dict[str, str] = {}   # leaf_event_key → streamed_match_id
_leaf_groq_expires: float = 0.0
_GROQ_TTL = 300  # 5 minutes


async def groq_match_event(
    leaf_event: dict,
    streamed_matches: list[dict],
) -> dict | None:
    """
    Use Groq LLM to match a Leaf event to a streamed.pk match when fuzzy
    matching fails. Returns the matched streamed match dict or None.
    """
    if not _GROQ_KEY:
        return None

    team_a = leaf_event.get("team_a", "")
    team_b = leaf_event.get("team_b", "")
    event_name = leaf_event.get("event_name", "")

    if team_a and team_b:
        leaf_title = f"{team_a} vs {team_b}"
    else:
        leaf_title = event_name

    if not leaf_title.strip():
        return None

    # Check in-memory cache first
    cache_key = leaf_title.lower().strip()
    if cache_key in _leaf_groq_map:
        matched_id = _leaf_groq_map[cache_key]
        if matched_id == "":
            return None  # cached as no-match
        for sm in streamed_matches:
            if sm.get("id") == matched_id:
                return sm
        return None

    # Build candidate list for Groq
    candidates = []
    leaf_cat_mapped = _map_sktech_sport(leaf_event.get("category", ""))
    for sm in streamed_matches:
        if sm.get("category", "") == leaf_cat_mapped:
            candidates.append({"id": sm["id"], "title": sm.get("title", "")})

    if not candidates:
        _leaf_groq_map[cache_key] = ""
        return None

    # If there is only 1 candidate and it's a different sport/competition, skip Groq
    # (Groq will hallucinate a match when there's only one option to pick from)
    if len(candidates) == 1:
        only = candidates[0]
        # Only auto-match if the single candidate title has actual team overlap
        only_title_norm = _normalise(only.get("title", ""))
        leaf_norm = _normalise(leaf_title)
        if _ratio(leaf_norm, only_title_norm) < 0.45:
            _leaf_groq_map[cache_key] = ""
            return None

    # Build prompt
    candidates_text = "\n".join(
        f"  - id={c['id']}: {c['title']}" for c in candidates[:30]
    )
    prompt = (
        f"You are a sports event matcher. Given a Leaf/SKTech event title and a list of "
        f"streamed.pk match titles, find the EXACT same match.\n\n"
        f"Leaf event: \"{leaf_title}\" (sport: {leaf_event.get('category', 'unknown')})\n\n"
        f"Candidates:\n{candidates_text}\n\n"
        f"Reply with ONLY the id of the matching candidate, or 'none' if no confident match. "
        f"No explanation."
    )

    try:
        from groq import AsyncGroq
        client = AsyncGroq(api_key=_GROQ_KEY)
        response = await client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=50,
            temperature=0,
        )
        answer = response.choices[0].message.content.strip().lower()

        if answer == "none" or not answer:
            _leaf_groq_map[cache_key] = ""
            return None

        # Find the match by EXACT ID only — never use partial/substring match
        # (substring match causes false positives when Groq returns a partial ID)
        answer_clean = answer.strip().strip('"').strip("'")
        for sm in streamed_matches:
            if sm.get("id", "") == answer_clean:
                logger.info("Groq matched Leaf '%s' → streamed '%s'", leaf_title, sm.get("title"))
                _leaf_groq_map[cache_key] = sm["id"]
                return sm

        _leaf_groq_map[cache_key] = ""
        return None

    except Exception as exc:
        logger.warning("Groq match error for '%s': %s", leaf_title, exc)
        return None


async def fetch_streamed_live_matches() -> list[dict]:
    """
    Fetch the current live match list from our own premium addon API.
    Returns a list of match dicts with id, title, category, teams fields.
    """
    if not ADDON_ADMIN_TOKEN or not ADDON_API_BASE:
        logger.warning("ADDON_ADMIN_TOKEN or ADDON_API_BASE not set — cannot fetch live matches")
        return []

    url = f"{ADDON_API_BASE}/{ADDON_ADMIN_TOKEN}/catalog/sport/sports_live.json"
    try:
        async with httpx.AsyncClient(verify=False, timeout=10) as client:
            resp = await client.get(url)
        if resp.status_code != 200:
            logger.warning("Live match fetch HTTP %d", resp.status_code)
            return []
        metas = resp.json().get("metas", [])
        # Convert Stremio meta format back to match-like dicts for the matcher
        matches = []
        for m in metas:
            mid = m.get("id", "")
            if mid.startswith("streamed:"):
                mid = mid[len("streamed:"):]
            # Extract category from genres
            genres = m.get("genres") or []
            category = genres[0].lower().replace(" ", "-") if genres else ""
            # Extract team names from description
            desc = m.get("description") or ""
            team_a = team_b = ""
            if " vs " in desc:
                parts = desc.split(" vs ", 1)
                team_a = parts[0].strip().split("\n")[0]
                team_b = parts[1].strip().split("\n")[0]
            matches.append({
                "id":       mid,
                "title":    m.get("name", ""),
                "category": category,
                "teams": {
                    "home": {"name": team_a},
                    "away": {"name": team_b},
                },
            })
        logger.info("Fetched %d live streamed.pk matches", len(matches))
        return matches
    except Exception as exc:
        logger.warning("fetch_streamed_live_matches error: %s", exc)
        return []


async def match_leaf_events(
    leaf_events: list[dict],
    streamed_matches: list[dict],
) -> list[tuple[dict, dict]]:
    """
    Match a list of Leaf events against streamed.pk live matches.

    Returns a list of (leaf_event, streamed_match) pairs where we are
    confident they refer to the same real-world event.

    Uses fuzzy matching first, then Groq LLM for anything unmatched.
    """
    results: list[tuple[dict, dict]] = []
    unmatched: list[dict] = []

    for event in leaf_events:
        if not event.get("slug"):
            continue  # no slug = can't fetch stream list

        sm = fuzzy_match_event(event, streamed_matches)
        if sm:
            results.append((event, sm))
        else:
            unmatched.append(event)

    # Run Groq on unmatched events concurrently
    if unmatched and GROQ_API_KEY:
        groq_tasks = [
            groq_match_event(ev, streamed_matches)
            for ev in unmatched
        ]
        groq_results = await asyncio.gather(*groq_tasks, return_exceptions=True)
        for ev, gr in zip(unmatched, groq_results):
            if isinstance(gr, dict):
                results.append((ev, gr))

    logger.info(
        "match_leaf_events: %d events → %d matched, %d unmatched",
        len(leaf_events), len(results), len(leaf_events) - len(results),
    )
    return results
