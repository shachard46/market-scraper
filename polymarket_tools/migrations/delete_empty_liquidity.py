"""
Delete market_change rows where liquidity IS NULL.

These rows are created when scan/batch_only runs without enriched data
(volume/liquidity come from Gamma API, not the CLOB batch response).
For markets that reference a deleted row, sets change_id to NULL.

Run:
    python -m polymarket_tools.migrations.delete_empty_liquidity
"""

import sqlite3
import sys
from pathlib import Path

# Ensure project root is in path (when run as __main__)
_root = Path(__file__).resolve().parent.parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from polymarket_tools.db import _get_db_path


def run_cleanup() -> tuple[int, int]:
    """
    Delete market_change rows where liquidity IS NULL.
    Set markets.change_id = NULL for any market that referenced a deleted row.

    Returns:
        (rows_deleted, markets_cleared) - counts for reporting.
    """
    path = _get_db_path()
    if not path.exists():
        return (0, 0)

    with sqlite3.connect(str(path)) as conn:
        conn.execute("BEGIN")
        try:
            # change_ids we're about to delete
            cursor = conn.execute(
                "SELECT change_id FROM market_change WHERE liquidity IS NULL"
            )
            to_delete = {row[0] for row in cursor}

            if not to_delete:
                conn.rollback()
                return (0, 0)

            # Clear change_id in markets that point to these rows
            placeholders = ",".join("?" * len(to_delete))
            cleared = conn.execute(
                f"UPDATE markets SET change_id = NULL WHERE change_id IN ({placeholders})",
                list(to_delete),
            ).rowcount

            # Delete the rows
            deleted = conn.execute(
                f"DELETE FROM market_change WHERE change_id IN ({placeholders})",
                list(to_delete),
            ).rowcount

            conn.commit()
            return (deleted, cleared)
        except Exception:
            conn.rollback()
            raise


def main() -> int:
    """CLI entry."""
    try:
        deleted, cleared = run_cleanup()
        if deleted > 0:
            print(f"Deleted {deleted} market_change rows with NULL liquidity.")
            if cleared > 0:
                print(f"Cleared change_id for {cleared} markets (re-scan to populate).")
        else:
            print("No rows with NULL liquidity found.")
        return 0
    except Exception as e:
        print(f"Cleanup failed: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
