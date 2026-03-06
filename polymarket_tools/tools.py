"""
Query tools for the agent to retrieve market data from the SQLite database.

Exposes get_all_markets, get_market_trends, get_category_markets,
get_closed_markets, get_open_markets, and query_market_field.
"""

from datetime import datetime

from sqlalchemy import select

from . import db
from .db import Market, MarketChange


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


def _model_to_dict(obj, exclude: set[str] | None = None) -> dict:
    """Convert ORM model instance to dict for JSON serialization."""
    if obj is None:
        return {}
    exclude = exclude or set()
    return {
        c.key: _serialize_value(getattr(obj, c.key))
        for c in obj.__table__.columns
        if c.key not in exclude
    }


def get_all_markets(limit: int = 50) -> list[dict]:
    """
    Return all available markets from the database.

    Args:
        limit: Maximum number of markets to return. Default 50.

    Returns:
        List of market dicts with market_id, question, slug, status, etc.
    """
    with db.get_session() as session:
        stmt = (
            select(Market)
            .order_by(Market.change_id.desc())
            .limit(limit)
        )
        rows = session.scalars(stmt).all()
    return [_model_to_dict(r) for r in rows]


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


def get_category_markets(category_name: str, limit: int = 50) -> list[dict]:
    """
    Return markets filtered by market_category.

    Args:
        category_name: Value of market_category (e.g., Politics, Sports, All).
        limit: Maximum number of markets to return. Default 50.

    Returns:
        List of market dicts matching the category.
    """
    with db.get_session() as session:
        stmt = (
            select(Market)
            .where(Market.market_category == category_name)
            .order_by(Market.change_id.desc())
            .limit(limit)
        )
        rows = session.scalars(stmt).all()
    return [_model_to_dict(r) for r in rows]


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
            select(Market)
            .where(
                (Market.outcome.isnot(None)) |
                (Market.status.in_(["closed", "archived"]))
            )
            .order_by(Market.change_id.desc())
            .limit(limit)
        )
        rows = session.scalars(stmt).all()
    return [_model_to_dict(r) for r in rows]


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
        stmt = (
            select(Market)
            .where(Market.outcome.is_(None))
            .where(Market.status == "active")
            .order_by(Market.change_id.desc())
            .limit(limit)
        )
        rows = session.scalars(stmt).all()
    return [_model_to_dict(r) for r in rows]


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
