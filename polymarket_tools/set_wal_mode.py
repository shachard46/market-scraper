"""
Set SQLite journal mode to WAL for better concurrent read/write performance.

Reduces "database is locked" errors when sample_refresh and market-ranker
(or other readers) access the DB simultaneously.

Usage:
    python -m polymarket_tools set_wal
    python -m polymarket_tools.set_wal_mode
"""

import sqlite3
import sys
from pathlib import Path

# Add parent for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from polymarket_tools.db import _get_db_path


def set_wal_mode() -> bool:
    """
    Set journal_mode to WAL for the Polymarket database.
    Returns True on success, False if DB does not exist.
    """
    path = _get_db_path()
    if not path.exists():
        print(f"Database not found: {path}", file=sys.stderr)
        return False

    with sqlite3.connect(str(path)) as conn:
        cur = conn.execute("PRAGMA journal_mode=WAL")
        mode = cur.fetchone()[0]
        if mode.upper() != "WAL":
            print(f"Failed to set WAL mode; current mode: {mode}", file=sys.stderr)
            return False

    return True


def main() -> int:
    """CLI entry."""
    try:
        if set_wal_mode():
            print("Journal mode set to WAL")
            return 0
        return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
