"""
Query tools for the agent to retrieve market data from the SQLite database.

Exposes get_all_markets, get_market, get_market_trends, get_category_markets,
get_closed_markets, get_open_markets, and query_market_field.
"""

import json
from datetime import datetime

from sqlalchemy import and_, func, or_, select

from . import db
from .db import Market, MarketChange

# Enriched columns: excluded from list endpoints, included only in get_market
EXTRA_FIELDS = frozenset({
    "volume", "liquidity", "start_date", "category", "tags",
    "market_type", "description", "extra_info",
})

# Valid field names for query_market_field (must match Market model columns)
_MARKET_QUERYABLE_FIELDS = frozenset(
    c.key for c in Market.__table__.columns
)


def _serialize_value(v) -> str | float | int | bool | None:
    """Convert value for JSON serialization (e.g., datetime -> ISO string)."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.isoformat()
    return v


def _model_to_dict(
    obj,
    exclude: set[str] | None = None,
    parse_extra_info: bool = False,
) -> dict:
    """Convert ORM model instance to dict for JSON serialization."""
    if obj is None:
        return {}
    exclude = exclude or set()
    result = {}
    for c in obj.__table__.columns:
        if c.key in exclude:
            continue
        v = getattr(obj, c.key)
        if parse_extra_info and c.key == "extra_info" and v is not None:
            try:
                v = json.loads(v)
            except (json.JSONDecodeError, TypeError):
                pass
        result[c.key] = _serialize_value(v)
    return result


def _build_market_dict_with_change(
    market,
    change,
    exclude: set[str] | None = None,
    parse_extra_info: bool = False,
) -> dict:
    """Build market dict with nested latest_change from MarketChange row."""
    result = _model_to_dict(market, exclude=exclude, parse_extra_info=parse_extra_info)
    if change is None:
        result["latest_change"] = None
    else:
        result["latest_change"] = {
            "datetime": _serialize_value(change.datetime),
            "yes_price": change.yes_price,
            "no_price": change.no_price,
            "volume": change.volume,
            "midpoint": change.midpoint,
            "spread": change.spread,
        }
    return result


def _markets_with_change_stmt():
    """Base select: Market LEFT JOIN MarketChange on change_id."""
    return (
        select(Market, MarketChange)
        .select_from(Market)
        .outerjoin(MarketChange, Market.change_id == MarketChange.change_id)
    )


def _open_market_filter(stmt):
    """Apply filter for open/active markets (outcome IS NULL, status = 'active')."""
    return stmt.where(Market.outcome.is_(None)).where(Market.status == "active")


def _fetch_markets_with_change(
    session,
    stmt,
    *,
    exclude: frozenset[str] | set[str] | None = None,
    parse_extra_info: bool = False,
) -> list[dict]:
    """Execute stmt (Market, MarketChange rows) and return list of market dicts with latest_change."""
    exclude = exclude if exclude is not None else EXTRA_FIELDS
    rows = session.execute(stmt).all()
    return [
        _build_market_dict_with_change(m, c, exclude=exclude, parse_extra_info=parse_extra_info)
        for m, c in rows
    ]


def _fetch_single_market_with_change(
    session,
    stmt,
    *,
    include_extra: bool = True,
) -> dict | None:
    """Execute stmt (single Market, MarketChange row) and return market dict or None."""
    row = session.execute(stmt).first()
    if row is None:
        return None
    m, c = row
    return _build_market_dict_with_change(
        m, c,
        exclude=None if include_extra else EXTRA_FIELDS,
        parse_extra_info=include_extra,
    )


def get_all_markets(limit: int = 50) -> list[dict]:
    """
    Return all available markets from the database.

    Args:
        limit: Maximum number of markets to return. Default 50.

    Returns:
        List of market dicts with market_id, question, slug, status, etc.
    """
    with db.get_session() as session:
        stmt = _markets_with_change_stmt().order_by(Market.change_id.desc()).limit(limit)
        return _fetch_markets_with_change(session, stmt)


def get_market(market_id: str, include_extra: bool = True) -> dict | None:
    """
    Return full market by market_id including enriched fields.

    Args:
        market_id: Polymarket condition_id.
        include_extra: If True (default), include volume, liquidity, description,
            tags, extra_info, etc. These are only populated when the market
            was scanned via scan --market.

    Returns:
        Market dict with all columns, or None if not found.
    """
    with db.get_session() as session:
        stmt = _markets_with_change_stmt().where(Market.market_id == market_id)
        return _fetch_single_market_with_change(session, stmt, include_extra=include_extra)


def get_market_trends(market_id: str, limit: int = 50) -> list[dict]:
    """
    Return price/volume/midpoint/spread history for a specific market.

    Queries MarketChange filtered by market_id, ordered by datetime descending.

    Args:
        market_id: Polymarket condition_id (market identifier).
        limit: Maximum number of change records to return. Default 50.

    Returns:
        List of change dicts with datetime, yes_price, no_price, volume,
        midpoint, spread.
    """
    with db.get_session() as session:
        stmt = (
            select(MarketChange)
            .where(MarketChange.market_id == market_id)
            .order_by(MarketChange.datetime.desc())
            .limit(limit)
        )
        rows = session.scalars(stmt).all()
    return [_model_to_dict(r) for r in rows]


def _like_escape(value: str) -> str:
    """Escape % and _ for safe use in SQL LIKE patterns."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def search_markets(
    keywords: list[str],
    limit: int | None = 50,
    match_all: bool = False,
) -> list[dict]:
    """
    Return markets where question or description contains the keywords (case-insensitive).

    Args:
        keywords: One or more search terms.
        limit: Maximum number of markets to return. Default 50. None = no limit (all matches).
        match_all: If True, all keywords must match (AND). If False (default), any keyword
            can match (OR).

    Returns:
        List of market dicts with latest_change.
    """
    if not keywords:
        return []
    with db.get_session() as session:
        stmt = _markets_with_change_stmt()
        keyword_conditions = []
        for kw in keywords:
            pattern = f"%{_like_escape(kw)}%".lower()
            kw_match = or_(
                func.lower(Market.question).like(pattern),
                (Market.description.isnot(None))
                & func.lower(Market.description).like(pattern),
            )
            keyword_conditions.append(kw_match)
        if match_all:
            stmt = stmt.where(and_(*keyword_conditions))
        else:
            stmt = stmt.where(or_(*keyword_conditions))
        stmt = stmt.order_by(Market.change_id.desc())
        if limit is not None:
            stmt = stmt.limit(limit)
        return _fetch_markets_with_change(session, stmt)


def get_category_markets(category_names: list[str], limit: int = 50) -> list[dict]:
    """
    Return markets filtered by market_category.

    Args:
        category_names: One or more values of market_category (e.g., Politics, Sports, All).
        limit: Maximum number of markets to return. Default 50.

    Returns:
        List of market dicts matching any of the categories.
    """
    with db.get_session() as session:
        stmt = (
            _markets_with_change_stmt()
            .where(Market.market_category.in_(category_names))
            .order_by(Market.change_id.desc())
            .limit(limit)
        )
        return _fetch_markets_with_change(session, stmt)


def get_closed_markets(limit: int = 50) -> list[dict]:
    """
    Return markets that are closed (resolved).

    Filters by outcome IS NOT NULL or status in ('closed', 'archived').

    Args:
        limit: Maximum number of markets to return. Default 50.

    Returns:
        List of market dicts for closed/resolved markets.
    """
    with db.get_session() as session:
        stmt = (
            _markets_with_change_stmt()
            .where(
                (Market.outcome.isnot(None)) |
                (Market.status.in_(["closed", "archived"]))
            )
            .order_by(Market.change_id.desc())
            .limit(limit)
        )
        return _fetch_markets_with_change(session, stmt)


def get_open_markets(limit: int = 50) -> list[dict]:
    """
    Return markets that are open (accepting orders).

    Filters by outcome IS NULL and status indicates open (e.g., 'active').

    Args:
        limit: Maximum number of markets to return. Default 50.

    Returns:
        List of market dicts for open/active markets.
    """
    with db.get_session() as session:
        stmt = _open_market_filter(_markets_with_change_stmt()).order_by(
            Market.change_id.desc()
        ).limit(limit)
        return _fetch_markets_with_change(session, stmt)


def get_stale_open_markets(limit: int = 200) -> list[dict]:
    """
    Return open markets ordered by staleness (least recently updated first).

    For sample_refresh: picks markets with lowest change_id (updated longest ago)
    so each gets a turn before any gets a second refresh.

    Args:
        limit: Maximum number of markets to return. Default 200.

    Returns:
        List of market dicts for open/active markets, oldest-refreshed first.
    """
    with db.get_session() as session:
        stmt = _open_market_filter(_markets_with_change_stmt()).order_by(
            Market.change_id.asc()
        ).limit(limit)
        return _fetch_markets_with_change(session, stmt)


def query_market_field(market_id: str, field_name: str) -> str | None:
    """
    Return a single field value for a market by market_id.

    The field_name is validated against the Market model's actual columns
    to prevent attribute errors.

    Args:
        market_id: Polymarket condition_id.
        field_name: Name of the Market column (e.g., 'question', 'status',
                    'slug', 'outcome', 'market_category').

    Returns:
        The column value as a string representation, or None if market
        not found or field is null.

    Raises:
        ValueError: If field_name is not a valid Market column.
    """
    if field_name not in _MARKET_QUERYABLE_FIELDS:
        raise ValueError(
            f"Invalid field_name '{field_name}'. "
            f"Must be one of: {sorted(_MARKET_QUERYABLE_FIELDS)}"
        )

    with db.get_session() as session:
        stmt = select(Market).where(Market.market_id == market_id)
        market = session.scalar(stmt)
        if market is None:
            return None
        value = getattr(market, field_name)
        return str(value) if value is not None else None
