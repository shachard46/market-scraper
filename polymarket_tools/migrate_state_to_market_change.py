"""
Migration: Move market state columns (volume, liquidity, last_trade_price)
from markets to market_change.

Run automatically via db.setup_db(). Can also be run standalone for existing DBs:
    python -m polymarket_tools.migrate_state_to_market_change
"""

import sqlite3
import sys
from pathlib import Path

# Add parent for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from polymarket_tools.db import _get_db_path


def _sqlite_version(conn: sqlite3.Connection) -> tuple[int, int, int]:
    """Return (major, minor, patch) of SQLite version."""
    row = conn.execute("SELECT sqlite_version()").fetchone()
    if not row:
        return (0, 0, 0)
    parts = row[0].split(".")
    return (
        int(parts[0]) if len(parts) > 0 else 0,
        int(parts[1]) if len(parts) > 1 else 0,
        int(parts[2]) if len(parts) > 2 else 0,
    )


def _market_change_has_column(conn: sqlite3.Connection, col: str) -> bool:
    """Check if market_change table has the given column."""
    result = conn.execute("PRAGMA table_info(market_change)")
    return col in {row[1] for row in result}


def _markets_has_column(conn: sqlite3.Connection, col: str) -> bool:
    """Check if markets table has the given column."""
    result = conn.execute("PRAGMA table_info(markets)")
    return col in {row[1] for row in result}


def _drop_columns_via_recreate(conn: sqlite3.Connection) -> None:
    """Recreate markets table without volume, liquidity, last_trade_price."""
    conn.execute("""
        CREATE TABLE markets_new (
            market_id TEXT NOT NULL PRIMARY KEY,
            clob_token_ids TEXT NOT NULL,
            status TEXT NOT NULL,
            question TEXT NOT NULL,
            slug TEXT NOT NULL,
            yes_token_id TEXT NOT NULL,
            no_token_id TEXT NOT NULL,
            minimum_tick_size REAL,
            neg_risk INTEGER NOT NULL,
            change_id INTEGER,
            outcome TEXT,
            market_category TEXT NOT NULL,
            start_date TIMESTAMP,
            category TEXT,
            tags TEXT,
            market_type TEXT,
            description TEXT,
            extra_info TEXT,
            FOREIGN KEY (change_id) REFERENCES market_change(change_id)
        )
    """)
    conn.execute("""
        INSERT INTO markets_new (
            market_id, clob_token_ids, status, question, slug,
            yes_token_id, no_token_id, minimum_tick_size, neg_risk,
            change_id, outcome, market_category,
            start_date, category, tags, market_type, description, extra_info
        )
        SELECT
            market_id, clob_token_ids, status, question, slug,
            yes_token_id, no_token_id, minimum_tick_size, neg_risk,
            change_id, outcome, market_category,
            start_date, category, tags, market_type, description, extra_info
        FROM markets
    """)
    conn.execute("DROP TABLE markets")
    conn.execute("ALTER TABLE markets_new RENAME TO markets")


def run_migration() -> bool:
    """
    Migrate state columns from markets to market_change.

    - Adds liquidity, last_trade_price to market_change
    - Makes market_change.volume nullable (ALTER to add new; SQLite keeps old)
    - Backfills market_change from markets where change_id matches
    - Drops volume, liquidity, last_trade_price from markets

    Returns True if migration was applied, False if already migrated.
    """
    path = _get_db_path()
    if not path.exists():
        return False

    with sqlite3.connect(str(path)) as conn:
        conn.execute("BEGIN")
        try:
            # Check if market_change already has liquidity (migration done)
            if _market_change_has_column(conn, "liquidity"):
                conn.rollback()
                return False

            # 1. Add liquidity and last_trade_price to market_change
            conn.execute("ALTER TABLE market_change ADD COLUMN liquidity REAL")
            conn.execute(
                "ALTER TABLE market_change ADD COLUMN last_trade_price REAL"
            )

            # 2. Backfill: copy from markets to market_change where change_id matches
            # Only if markets still has these columns (pre-migration)
            if _markets_has_column(conn, "volume"):
                conn.execute("""
                    UPDATE market_change
                    SET
                        volume = COALESCE(
                            (SELECT m.volume FROM markets m
                             WHERE m.change_id = market_change.change_id),
                            market_change.volume
                        ),
                        liquidity = (
                            SELECT m.liquidity FROM markets m
                            WHERE m.change_id = market_change.change_id
                        ),
                        last_trade_price = (
                            SELECT m.last_trade_price FROM markets m
                            WHERE m.change_id = market_change.change_id
                        )
                    WHERE change_id IN (SELECT change_id FROM markets WHERE change_id IS NOT NULL)
                """)

            # 3. Drop columns from markets
            version = _sqlite_version(conn)
            if version >= (3, 35, 0):
                conn.execute("ALTER TABLE markets DROP COLUMN last_trade_price")
                conn.execute("ALTER TABLE markets DROP COLUMN volume")
                conn.execute("ALTER TABLE markets DROP COLUMN liquidity")
            else:
                _drop_columns_via_recreate(conn)

            conn.commit()
        except Exception:
            conn.rollback()
            raise

    return True


def main() -> int:
    """CLI entry: run migration and exit."""
    try:
        applied = run_migration()
        if applied:
            print("Migration applied: state columns moved to market_change")
        else:
            print("Migration already applied or DB does not exist")
        return 0
    except Exception as e:
        print(f"Migration failed: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
