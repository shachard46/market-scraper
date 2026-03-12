"""
Database schema and connection management using SQLAlchemy.

Defines Market and MarketChange ORM models and provides a session factory
for the polymarket.db SQLite database.
"""

import os
from contextlib import contextmanager
from pathlib import Path
from datetime import datetime as dt
from typing import Generator

from sqlalchemy import DateTime, create_engine, event, ForeignKey, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session, sessionmaker

# Default DB path; override via POLYMARKET_DB_PATH env var
_DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "polymarket.db"


def _get_db_path() -> Path:
    """Return the database file path from env or default."""
    env_path = os.environ.get("POLYMARKET_DB_PATH")
    if env_path:
        return Path(env_path)
    return _DEFAULT_DB_PATH


def _get_engine_url() -> str:
    """Build SQLAlchemy engine URL for SQLite."""
    path = _get_db_path()
    return f"sqlite:///{path}"


class Base(DeclarativeBase):
    """Declarative base for ORM models."""
    pass


class Market(Base):
    """
    ORM model for the `markets` table.

    Reflects the current state of open/closed markets. Updated via UPSERT
    on each scanner run. State columns (volume, liquidity, last_trade_price)
    live in market_change; join via change_id for current values.
    """
    __tablename__ = "markets"

    market_id: Mapped[str] = mapped_column(primary_key=True)
    clob_token_ids: Mapped[str] = mapped_column(nullable=False)  # JSON list of strings
    status: Mapped[str] = mapped_column(nullable=False)
    question: Mapped[str] = mapped_column(nullable=False)
    slug: Mapped[str] = mapped_column(nullable=False)
    yes_token_id: Mapped[str] = mapped_column(nullable=False)
    no_token_id: Mapped[str] = mapped_column(nullable=False)
    minimum_tick_size: Mapped[float | None] = mapped_column(nullable=True)
    neg_risk: Mapped[bool] = mapped_column(nullable=False)
    change_id: Mapped[int | None] = mapped_column(
        ForeignKey("market_change.change_id"),
        nullable=True,
    )
    outcome: Mapped[str | None] = mapped_column(nullable=True)
    market_category: Mapped[str] = mapped_column(nullable=False)
    # Enriched columns: populated only when scraped via single-market fetch
    start_date: Mapped[dt | None] = mapped_column(DateTime, nullable=True)
    category: Mapped[str | None] = mapped_column(nullable=True)
    tags: Mapped[str | None] = mapped_column(nullable=True)  # JSON array
    market_type: Mapped[str | None] = mapped_column(nullable=True)
    description: Mapped[str | None] = mapped_column(nullable=True)
    extra_info: Mapped[str | None] = mapped_column(nullable=True)  # JSON


class MarketChange(Base):
    """
    ORM model for the `market_change` table.

    Log of price/volume/liquidity/midpoint/spread snapshots. INSERT-only on each scan.
    State columns (volume, liquidity, last_trade_price) are stored here per snapshot.
    """
    __tablename__ = "market_change"

    change_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    datetime: Mapped[dt] = mapped_column(DateTime, default=dt.utcnow)
    # market_id references markets.market_id; no FK to avoid circular dependency on create
    market_id: Mapped[str] = mapped_column(nullable=False)
    yes_price: Mapped[float] = mapped_column(nullable=False)
    no_price: Mapped[float] = mapped_column(nullable=False)
    volume: Mapped[float | None] = mapped_column(nullable=True)  # From Gamma enriched; 0.0 if none
    liquidity: Mapped[float | None] = mapped_column(nullable=True)  # From Gamma enriched
    last_trade_price: Mapped[float | None] = mapped_column(nullable=True)  # From orderbook/API
    midpoint: Mapped[float | None] = mapped_column(nullable=True)
    spread: Mapped[float | None] = mapped_column(nullable=True)


_engine = None
_SessionLocal: sessionmaker[Session] | None = None


def _get_engine():
    """Lazy-init engine."""
    global _engine
    if _engine is None:
        url = _get_engine_url()
        # timeout: seconds to wait when DB is locked (e.g. market-ranker reading)
        _engine = create_engine(url, echo=False, connect_args={"timeout": 30})

        @event.listens_for(_engine, "connect")
        def _set_sqlite_pragma(dbapi_conn, *_):
            """Enable WAL and busy_timeout for better concurrent access."""
            cursor = dbapi_conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA busy_timeout=30000")
            cursor.close()
    return _engine


def get_session_factory() -> sessionmaker[Session]:
    """Return the session factory (sessionmaker)."""
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(
            bind=_get_engine(),
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,
        )
    return _SessionLocal


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """
    Yield a SQLAlchemy session. Commits on success, rolls back on error.

    Usage:
        with get_session() as session:
            session.query(Market).all()
    """
    factory = get_session_factory()
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


_ENRICHED_COLUMNS = [
    ("start_date", "TIMESTAMP"),
    ("category", "TEXT"),
    ("tags", "TEXT"),
    ("market_type", "TEXT"),
    ("description", "TEXT"),
    ("extra_info", "TEXT"),
]


def _migrate_add_enriched_columns(engine) -> None:
    """Add enriched columns to markets table if they do not exist (SQLite migration)."""
    with engine.connect() as conn:
        result = conn.execute(text("PRAGMA table_info(markets)"))
        existing = {row[1] for row in result}
    for col_name, col_type in _ENRICHED_COLUMNS:
        if col_name not in existing:
            with engine.connect() as conn:
                conn.execute(text(f"ALTER TABLE markets ADD COLUMN {col_name} {col_type}"))
                conn.commit()


def setup_db() -> Path:
    """
    Create the database file and tables if they do not exist.
    Migrates existing tables to add new enriched columns if missing.

    Creates:
        - market_change: log of price/volume changes (INSERT only)
        - markets: current state of markets (UPSERT target)

    Returns:
        Path to the database file.
    """
    path = _get_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    engine = _get_engine()
    Base.metadata.create_all(engine)
    if path.exists():
        _migrate_add_enriched_columns(engine)
        from .migrate_state_to_market_change import run_migration

        run_migration()
    return path
