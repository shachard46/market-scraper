"""
Scan logic for polling Polymarket CLOB and persisting via SQLAlchemy.

Fetches markets, computes BBO/midpoint/spread per token, INSERTs into
market_change, then UPSERTs into markets using SQLAlchemy's SQLite insert.
"""

import json
import sys
from typing import Any

from sqlalchemy.dialects.sqlite import insert

PROGRESS_INTERVAL = 50

from . import api, db
from .db import Market, MarketChange


def _derive_status(m: dict[str, Any]) -> str:
    """Build status string from active, closed, archived."""
    if m.get("archived"):
        return "archived"
    if m.get("closed"):
        return "closed"
    if m.get("active"):
        return "active"
    return "unknown"


def _get_token_ids(m: dict[str, Any]) -> tuple[str | None, str | None]:
    """Extract yes and no token IDs from market tokens array."""
    tokens = m.get("tokens") or []
    if len(tokens) < 2:
        return (None, None)
    first = tokens[0]
    second = tokens[1]
    if first.get("outcome", "").lower() == "yes" or second.get("outcome", "").lower() == "no":
        return (first.get("token_id"), second.get("token_id"))
    if second.get("outcome", "").lower() == "yes" or first.get("outcome", "").lower() == "no":
        return (second.get("token_id"), first.get("token_id"))
    return (first.get("token_id"), second.get("token_id"))


def _get_outcome(m: dict[str, Any]) -> str | None:
    """Return winning outcome string if market is resolved, else None."""
    tokens = m.get("tokens") or []
    for t in tokens:
        if t.get("winner"):
            return t.get("outcome")
    return None


def _get_market_category(m: dict[str, Any]) -> str:
    """Extract primary category from tags."""
    tags = m.get("tags")
    if tags and tags[0]:
        return str(tags[0])
    return "uncategorized"


def _get_token_price_from_market(m: dict[str, Any], yes_token_id: str | None) -> float | None:
    """Get price from tokens[].price (Gamma outcomePrices). Prefer yes token."""
    tokens = m.get("tokens") or []
    if yes_token_id:
        for t in tokens:
            if str(t.get("token_id", "")) == str(yes_token_id):
                p = t.get("price")
                if p is not None:
                    try:
                        return float(p)
                    except (TypeError, ValueError):
                        pass
                break
    for t in tokens:
        p = t.get("price")
        if p is not None:
            try:
                return float(p)
            except (TypeError, ValueError):
                pass
    return None


def _get_last_trade_price(
    m: dict[str, Any],
    yes_token_id: str | None,
    yes_book: dict[str, Any] | None = None,
    use_api: bool = True,
) -> float | None:
    """
    Get last trade price from multiple sources in priority order.

    1. orderbook.last_trade_price (from batch response)
    2. tokens[].price (Gamma outcomePrices)
    3. API fetch (only when use_api=True, e.g. scan_single_market)
    """
    if yes_book:
        p = api.last_trade_price_from_book(yes_book)
        if p is not None:
            return p
    p = _get_token_price_from_market(m, yes_token_id)
    if p is not None:
        return p
    if use_api and yes_token_id:
        return api.fetch_last_trade_price(yes_token_id)
    return None


def _persist_market(
    session,
    m: dict[str, Any],
    enriched: dict[str, Any] | None,
    orderbooks: dict[str, dict[str, Any]] | None = None,
) -> bool:
    """
    Persist a single market to DB: insert MarketChange, upsert Market.

    Args:
        session: SQLAlchemy session.
        m: Market in CLOB-like format (condition_id, tokens, question, etc.).
        enriched: Optional dict with volume, liquidity, start_date, category,
            tags, market_type, description, extra_info. If None, enriched cols stay NULL.
        orderbooks: Optional pre-fetched map token_id -> orderbook. If None, fetch per token.

    Returns:
        True if persisted, False if skipped (missing condition_id or tokens).
    """
    condition_id = m.get("condition_id")
    if not condition_id:
        return False

    yes_token_id, no_token_id = _get_token_ids(m)
    if not yes_token_id or not no_token_id:
        return False

    if orderbooks is not None:
        yes_book = orderbooks.get(yes_token_id)
        no_book = orderbooks.get(no_token_id)
    else:
        yes_book = api.fetch_orderbook(yes_token_id)
        no_book = api.fetch_orderbook(no_token_id)
    best_bid_yes, best_ask_yes = api.compute_bbo_from_orderbook(yes_book)
    best_bid_no, best_ask_no = api.compute_bbo_from_orderbook(no_book)

    mid, spread = api.compute_midpoint_and_spread(best_bid_yes, best_ask_yes)
    if mid is None:
        mid, spread = api.compute_midpoint_and_spread(best_bid_no, best_ask_no)

    yes_price = mid
    no_price = (1.0 - mid) if mid is not None else None
    use_api_for_price = orderbooks is None
    if yes_price is None:
        last = _get_last_trade_price(m, yes_token_id, yes_book=yes_book, use_api=use_api_for_price)
        yes_price = last if last is not None else 0.0
    if no_price is None:
        no_price = 1.0 - yes_price if yes_price is not None else 0.0

    last_trade_price = _get_last_trade_price(
        m, yes_token_id, yes_book=yes_book, use_api=use_api_for_price
    )

    change = MarketChange(
        market_id=condition_id,
        yes_price=yes_price,
        no_price=no_price,
        volume=0.0,
        midpoint=mid,
        spread=spread,
    )
    session.add(change)
    session.flush()
    change_id = change.change_id

    clob_token_ids = json.dumps(
        [t.get("token_id") for t in (m.get("tokens") or []) if t.get("token_id")]
    )
    status = _derive_status(m)
    question = m.get("question") or ""
    slug = m.get("market_slug") or ""
    minimum_tick_size = m.get("minimum_tick_size")
    if minimum_tick_size is not None:
        try:
            minimum_tick_size = float(minimum_tick_size)
        except (TypeError, ValueError):
            minimum_tick_size = None
    neg_risk = bool(m.get("neg_risk"))
    outcome = _get_outcome(m)
    market_category = _get_market_category(m)

    values: dict[str, Any] = {
        "market_id": condition_id,
        "clob_token_ids": clob_token_ids,
        "status": status,
        "question": question,
        "slug": slug,
        "yes_token_id": yes_token_id,
        "no_token_id": no_token_id,
        "last_trade_price": last_trade_price,
        "minimum_tick_size": minimum_tick_size,
        "neg_risk": neg_risk,
        "change_id": change_id,
        "outcome": outcome,
        "market_category": market_category,
    }
    set_cols = [
        "clob_token_ids", "status", "question", "slug",
        "yes_token_id", "no_token_id", "last_trade_price", "minimum_tick_size",
        "neg_risk", "change_id", "outcome", "market_category",
    ]

    if enriched:
        values["volume"] = enriched.get("volume")
        values["liquidity"] = enriched.get("liquidity")
        values["start_date"] = enriched.get("start_date")
        values["category"] = enriched.get("category")
        values["tags"] = enriched.get("tags")
        values["market_type"] = enriched.get("market_type")
        values["description"] = enriched.get("description")
        values["extra_info"] = enriched.get("extra_info")
        set_cols.extend(["volume", "liquidity", "start_date", "category",
                        "tags", "market_type", "description", "extra_info"])

    stmt = insert(Market).values(**values)
    stmt = stmt.on_conflict_do_update(
        index_elements=["market_id"],
        set_={c: stmt.excluded[c] for c in set_cols},
    )
    session.execute(stmt)
    return True


def scan_single_market(identifier: str) -> bool:
    """
    Fetch and persist a single market by condition_id or slug.

    Uses Gamma API for full market details and populates enriched columns
    (volume, liquidity, description, tags, extra_info, etc.).

    Returns:
        True if market was found and persisted, False otherwise.
    """
    gamma_market = api.fetch_market(identifier)
    if not gamma_market:
        return False
    m = api._gamma_to_clob_format(gamma_market)
    enriched = api._extract_enriched_fields(gamma_market)
    with db.get_session() as session:
        return _persist_market(session, m, enriched)


def scan_once(
    limit: int | None = None,
    active_only: bool = True,
    batch_only: bool = False,
) -> int:
    """
    Perform a single scan: fetch markets, compute BBO, persist to DB.

    For each market:
        1. Fetch orderbooks for yes/no tokens (batch or per-market).
        2. Compute midpoint and spread from BBO.
        3. INSERT row into market_change (returns change_id).
        4. UPSERT row into markets using SQLite INSERT ... ON CONFLICT DO UPDATE.

    Args:
        limit: Optional max number of markets to process (for testing).
        active_only: If True (default), fetch only active markets. If False, fetch all.
        batch_only: If True, use only batch API calls (POST /books) - no per-market
            fetch_orderbook or fetch_last_trade_price. Faster but may miss orderbooks
            for tokens that fail in the batch.

    Returns:
        Number of markets processed successfully.
    """
    markets = api.fetch_markets(active_only=active_only)
    if limit is not None and limit > 0:
        markets = markets[:limit]
    total = len(markets)
    print(f"Scanning {total} markets...", file=sys.stderr)

    orderbooks: dict[str, dict[str, Any]] | None = None
    if batch_only:
        token_ids: list[str] = []
        for m in markets:
            yes_id, no_id = _get_token_ids(m)
            if yes_id:
                token_ids.append(yes_id)
            if no_id and no_id != yes_id:
                token_ids.append(no_id)
        token_ids = list(dict.fromkeys(token_ids))  # preserve order, dedupe
        print(f"  Fetching orderbooks for {len(token_ids)} tokens (batch)...", file=sys.stderr)
        orderbooks = api.fetch_orderbooks_batch(token_ids)

    processed = 0
    with db.get_session() as session:
        for m in markets:
            if _persist_market(session, m, enriched=None, orderbooks=orderbooks):
                processed += 1
                if processed % PROGRESS_INTERVAL == 0:
                    print(f"  Processed {processed}/{total} markets.", file=sys.stderr)

    return processed


def sync_closed_markets(limit: int = 500) -> int:
    """
    Sync markets that have closed since the last scan.

    Fetches recently closed markets from the Gamma API (closed=true, active=false),
    then for each market that exists in our DB with status 'active', updates the
    record to status='closed' and sets the outcome from the API response.

    Args:
        limit: Max closed markets to fetch from API (default 500). Uses pagination.

    Returns:
        Number of DB records updated from active to closed.
    """
    gamma_markets = api.fetch_closed_gamma_markets(limit=limit)
    updated = 0

    with db.get_session() as session:
        for gamma_m in gamma_markets:
            condition_id = gamma_m.get("conditionId") or gamma_m.get("condition_id")
            if not condition_id:
                continue

            market = session.get(db.Market, condition_id)
            if market is None or market.status != "active":
                continue

            m = api._gamma_to_clob_format(gamma_m)
            outcome = _get_outcome(m)

            market.status = "closed"
            market.outcome = outcome
            updated += 1
            if updated % PROGRESS_INTERVAL == 0:
                print(f"  Synced {updated} closed markets.", file=sys.stderr)

    if updated > 0:
        print(f"Synced {updated} markets from active to closed.", file=sys.stderr)
    return updated
