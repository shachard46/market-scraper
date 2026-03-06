"""
Polymarket CLOB API client.

Fetches market data and orderbook information from the public CLOB API
to compute best bid/offer, midpoint, and spread.

For active markets, uses the Gamma API (which supports active/closed filtering);
the CLOB API returns primarily closed/historical markets.
"""

import json
import sys
from datetime import datetime
from typing import Any

import requests

BASE_URL = "https://clob.polymarket.com"
GAMMA_URL = "https://gamma-api.polymarket.com"
GAMMA_PAGE_LIMIT = 100
GAMMA_FETCH_PROGRESS_INTERVAL = 500  # Print every N markets fetched
DEFAULT_OUTCOMES = ["Yes", "No"]
DEFAULT_PRICES = ["0.5", "0.5"]


def _parse_json_field(value: str | list | None, default: list[str]) -> list:
    """Parse JSON string or return list. Returns default on decode error."""
    if value is None:
        return default
    if isinstance(value, list):
        return value
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return default


def _build_tokens(token_ids: list, outcomes: list[str], prices: list[str]) -> list[dict[str, Any]]:
    """Build token list for yes/no outcomes from parallel lists."""
    tokens = []
    for tid, outcome, price in zip(token_ids[:2], outcomes[:2], prices[:2]):
        tokens.append({
            "token_id": str(tid) if tid else None,
            "outcome": str(outcome),
            "price": float(price) if price is not None else 0.5,
            "winner": False,
        })
    while len(tokens) < 2 and len(token_ids) > len(tokens):
        idx = len(tokens)
        tokens.append({
            "token_id": str(token_ids[idx]),
            "outcome": "Yes" if idx == 0 else "No",
            "price": 0.5,
            "winner": False,
        })
    return tokens


_KEYS_IN_EXPLICIT_COLUMNS = {
    "conditionId", "condition_id", "question", "slug", "clobTokenIds", "clob_token_ids",
    "outcomes", "outcomePrices", "active", "closed", "archived",
    "negRisk", "neg_risk", "orderPriceMinTickSize", "orderMinSize",
    "volume", "liquidity", "volumeNum", "liquidityNum",
    "startDate", "startDateIso", "category", "tags", "marketType", "description",
}


def _parse_iso_datetime(value: str | None) -> datetime | None:
    """Parse ISO datetime string to datetime, or return None."""
    if not value:
        return None
    try:
        s = value.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return None


def _extract_enriched_fields(gamma_market: dict[str, Any]) -> dict[str, Any]:
    """
    Extract enriched fields from Gamma API market for DB storage.

    Returns dict with: volume, liquidity, start_date, category, tags,
    market_type, description, extra_info (JSON string of remaining fields).
    """
    vol = gamma_market.get("volumeNum") or gamma_market.get("volume")
    volume = float(vol) if vol is not None else None

    liq = gamma_market.get("liquidityNum") or gamma_market.get("liquidity")
    liquidity = float(liq) if liq is not None else None

    start_date = _parse_iso_datetime(
        gamma_market.get("startDate") or gamma_market.get("startDateIso")
    )

    category = gamma_market.get("category")
    if category is not None:
        category = str(category)

    tags_raw = gamma_market.get("tags")
    if tags_raw is not None:
        tags = json.dumps(tags_raw) if not isinstance(tags_raw, str) else tags_raw
    else:
        tags = None

    market_type = gamma_market.get("marketType")
    if market_type is not None:
        market_type = str(market_type)

    description = gamma_market.get("description")
    if description is not None:
        description = str(description)

    extra = {k: v for k, v in gamma_market.items() if k not in _KEYS_IN_EXPLICIT_COLUMNS}
    extra_info = json.dumps(extra) if extra else None

    return {
        "volume": volume,
        "liquidity": liquidity,
        "start_date": start_date,
        "category": category,
        "tags": tags,
        "market_type": market_type,
        "description": description,
        "extra_info": extra_info,
    }


def _gamma_to_clob_format(m: dict[str, Any]) -> dict[str, Any]:
    """Transform Gamma API market to CLOB-like format for the scanner."""
    token_ids = _parse_json_field(
        m.get("clobTokenIds") or m.get("clob_token_ids"),
        [],
    )
    outcomes = _parse_json_field(m.get("outcomes"), DEFAULT_OUTCOMES)
    prices = _parse_json_field(m.get("outcomePrices"), DEFAULT_PRICES)
    tokens = _build_tokens(token_ids, outcomes, prices)

    tags = [m["groupItemTitle"]] if m.get("groupItemTitle") else []
    return {
        "condition_id": m.get("conditionId") or m.get("condition_id"),
        "question": m.get("question", ""),
        "market_slug": m.get("slug", ""),
        "tokens": tokens,
        "active": m.get("active", True),
        "closed": m.get("closed", False),
        "archived": m.get("archived", False),
        "neg_risk": m.get("negRisk", m.get("neg_risk", False)),
        "minimum_tick_size": m.get("orderPriceMinTickSize") or m.get("orderMinSize"),
        "tags": tags,
    }


def _fetch_gamma_markets() -> list[dict[str, Any]]:
    """Fetch active markets from Gamma API (supports active/closed filtering)."""
    print("Fetching active markets...", file=sys.stderr)
    all_markets: list[dict[str, Any]] = []
    offset = 0
    while True:
        resp = requests.get(
            f"{GAMMA_URL}/markets",
            params={
                "active": "true",
                "closed": "false",
                "limit": GAMMA_PAGE_LIMIT,
                "offset": offset,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        markets = data if isinstance(data, list) else data.get("data", [])
        if not markets:
            break
        all_markets.extend(markets)
        n = len(all_markets)
        if n % GAMMA_FETCH_PROGRESS_INTERVAL == 0 or len(markets) < GAMMA_PAGE_LIMIT:
            print(f"  Fetched {n} markets...", file=sys.stderr)
        if len(markets) < GAMMA_PAGE_LIMIT:
            break
        offset += GAMMA_PAGE_LIMIT
    return all_markets


def _is_slug(identifier: str) -> bool:
    """Return True if identifier looks like a slug (no 0x, no long hex)."""
    id_ = identifier.strip()
    if id_.startswith("0x") and len(id_) > 10:
        return False
    return "-" in id_ or id_.replace("-", "").replace("_", "").isalnum()


def fetch_market(identifier: str) -> dict[str, Any] | None:
    """
    Fetch a single market from Gamma API by condition_id or slug.

    Args:
        identifier: Either a slug (e.g. "bitboy-convicted") or condition_id (0x...).

    Returns:
        Raw Gamma API market object, or None if not found.
    """
    id_ = identifier.strip()
    try:
        if _is_slug(id_):
            resp = requests.get(
                f"{GAMMA_URL}/markets/slug/{id_}",
                timeout=15,
            )
        else:
            resp = requests.get(
                f"{GAMMA_URL}/markets",
                params={"condition_id": id_},
                timeout=15,
            )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data[0] if data else None
        return data
    except (requests.RequestException, ValueError, KeyError, IndexError):
        return None


def fetch_markets(active_only: bool = True) -> list[dict[str, Any]]:
    """
    Fetch markets for scanning.

    When active_only is True (default), uses the Gamma API which correctly
    returns active tradable markets. The CLOB /markets endpoint returns
    primarily closed/historical markets.

    When active_only is False, uses the CLOB API to fetch all markets.

    Args:
        active_only: If True (default), fetch only active markets via Gamma API.
            If False, fetch all markets from CLOB API.

    Returns:
        List of market objects in CLOB-like format (condition_id, question,
        tokens, etc.). Empty list on error.
    """
    try:
        if active_only:
            gamma_markets = _fetch_gamma_markets()
            return [_gamma_to_clob_format(m) for m in gamma_markets]
        print("Fetching markets from CLOB...", file=sys.stderr)
        resp = requests.get(f"{BASE_URL}/markets", timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and "data" in data:
            markets = data["data"]
        elif isinstance(data, list):
            markets = data
        else:
            markets = []
        print(f"  Fetched {len(markets)} markets.", file=sys.stderr)
        return markets
    except (requests.RequestException, ValueError) as e:
        raise RuntimeError(f"Failed to fetch markets: {e}") from e


def fetch_orderbook(token_id: str) -> dict[str, Any] | None:
    """
    Fetch the orderbook for a given token ID.

    Args:
        token_id: CLOB token ID (e.g., yes or no outcome token).

    Returns:
        Dict with bids, asks, tick_size, etc., or None on error.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/book",
            params={"token_id": token_id},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()
    except (requests.RequestException, ValueError):
        return None


def fetch_last_trade_price(token_id: str) -> float | None:
    """
    Fetch the last trade price for a token (best available price to buy).

    Args:
        token_id: CLOB token ID.

    Returns:
        Price as float, or None if unavailable.
    """
    try:
        resp = requests.get(
            f"{BASE_URL}/price",
            params={"token_id": token_id, "side": "BUY"},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        price = data.get("price")
        if price is not None:
            return float(price)
        return None
    except (requests.RequestException, ValueError, TypeError):
        return None


def compute_bbo_from_orderbook(book: dict[str, Any] | None) -> tuple[float | None, float | None]:
    """
    Extract best bid and best ask from an orderbook response.

    Args:
        book: Orderbook dict with bids and asks arrays. Each level has "price" and "size".

    Returns:
        (best_bid, best_ask) as floats, or (None, None) if missing/empty.
    """
    if not book:
        return (None, None)
    bids = book.get("bids") or []
    asks = book.get("asks") or []
    best_bid = float(bids[0]["price"]) if bids else None
    best_ask = float(asks[0]["price"]) if asks else None
    return (best_bid, best_ask)


def compute_midpoint_and_spread(best_bid: float | None, best_ask: float | None) -> tuple[float | None, float | None]:
    """
    Compute midpoint and spread from best bid and best ask.

    Midpoint = (best_bid + best_ask) / 2
    Spread = best_ask - best_bid

    Args:
        best_bid: Highest bid price.
        best_ask: Lowest ask price.

    Returns:
        (midpoint, spread) as floats, or (None, None) if either BBO is missing.
    """
    if best_bid is None or best_ask is None:
        return (None, None)
    midpoint = (best_bid + best_ask) / 2
    spread = best_ask - best_bid
    return (midpoint, spread)
