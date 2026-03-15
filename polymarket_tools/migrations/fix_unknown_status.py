"""
Migration: Set status = 'active' for markets with outcome IS NULL.

Markets with no outcome are open/unresolved and should have status 'active'.
This fixes rows that were incorrectly set to 'unknown' (e.g. from
refresh_sample_open_markets before status fields were preserved).

Run:
    python -m polymarket_tools.migrations.fix_unknown_status
"""

import sqlite3
import sys
from pathlib import Path

# Ensure project root is in path (when run as __main__)
_root = Path(__file__).resolve().parent.parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from polymarket_tools.db import _get_db_path


def run_migration() -> int:
    """
    Set status = 'active' for markets where outcome IS NULL.
    Unresolved markets should be active.

    Returns:
        Number of rows updated.
    """
    path = _get_db_path()
    if not path.exists():
        return 0

    with sqlite3.connect(str(path)) as conn:
        cursor = conn.execute(
            "UPDATE markets SET status = 'active' WHERE outcome IS NULL"
        )
        updated = cursor.rowcount
        conn.commit()
    return updated


def main() -> int:
    """CLI entry."""
    try:
        updated = run_migration()
        if updated > 0:
            print(f"Updated {updated} markets to status='active'.")
        else:
            print("No markets needed updating.")
        return 0
    except Exception as e:
        print(f"Migration failed: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
