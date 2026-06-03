"""
Prune old market_change rows, keeping the last 4 days of history.

Rules:
  - Keep ALL rows for closed markets (status = 'closed' in markets table).
  - Keep ALL rows for markets whose last recorded change is older than 4 days
    (stale/inactive markets — they haven't been scanned recently, so don't
    throw away what little history they have).
  - For all other markets (active, recently updated): delete rows older than
    4 days, but ALWAYS preserve the single most-recent row per market so the
    markets.change_id FK is never left dangling.

Run:
    python -m polymarket_tools.migrations.prune_market_changes
    python -m polymarket_tools.migrations.prune_market_changes --dry-run
    python -m polymarket_tools.migrations.prune_market_changes --days 7
"""

import argparse
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

_root = Path(__file__).resolve().parent.parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from polymarket_tools.db import _get_db_path

_CUTOFF_DAYS = 4


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def run_prune(days: int = _CUTOFF_DAYS, dry_run: bool = False) -> dict:
    """
    Prune stale market_change rows.

    Args:
        days: History window to keep (default 4). Rows older than this are
              candidates for deletion.
        dry_run: If True, compute what would be deleted but make no changes.

    Returns:
        Dict with keys:
            deleted       – number of rows deleted (0 if dry_run)
            would_delete  – number of rows that would be deleted (dry_run)
            markets_cleared – markets whose change_id was nulled out
            cutoff        – the datetime threshold used
    """
    path = _get_db_path()
    if not path.exists():
        return {"deleted": 0, "would_delete": 0, "markets_cleared": 0, "cutoff": None}

    cutoff: datetime = _utcnow() - timedelta(days=days)
    # SQLite stores datetimes as text; use ISO format without timezone offset
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

    with sqlite3.connect(str(path)) as conn:
        conn.row_factory = sqlite3.Row

        # ── 1. markets to skip entirely ──────────────────────────────────────
        # a) Closed markets
        closed_ids: set[str] = {
            row["market_id"]
            for row in conn.execute(
                "SELECT market_id FROM markets WHERE status = 'closed'"
            )
        }

        # b) Markets whose last change is older than the cutoff (stale/not scanned recently)
        stale_ids: set[str] = {
            row["market_id"]
            for row in conn.execute(
                """
                SELECT market_id
                FROM market_change
                GROUP BY market_id
                HAVING MAX(datetime) < ?
                """,
                (cutoff_str,),
            )
        }

        skip_ids: set[str] = closed_ids | stale_ids

        # ── 2. last change_id per market (always keep these) ─────────────────
        last_change_per_market: dict[str, int] = {
            row["market_id"]: row["last_id"]
            for row in conn.execute(
                "SELECT market_id, MAX(change_id) AS last_id FROM market_change GROUP BY market_id"
            )
        }
        protected_ids: set[int] = set(last_change_per_market.values())

        # ── 3. find rows to delete ────────────────────────────────────────────
        candidates = conn.execute(
            """
            SELECT change_id, market_id
            FROM market_change
            WHERE datetime < ?
            """,
            (cutoff_str,),
        ).fetchall()

        to_delete: list[int] = [
            row["change_id"]
            for row in candidates
            if row["market_id"] not in skip_ids
            and row["change_id"] not in protected_ids
        ]

        result = {
            "deleted": 0,
            "would_delete": len(to_delete),
            "markets_cleared": 0,
            "cutoff": cutoff,
            "skipped_closed": len(closed_ids),
            "skipped_stale": len(stale_ids),
        }

        if dry_run or not to_delete:
            return result

        # ── 4. apply deletions in a transaction ───────────────────────────────
        conn.execute("BEGIN")
        try:
            chunk_size = 500
            total_deleted = 0
            for i in range(0, len(to_delete), chunk_size):
                chunk = to_delete[i : i + chunk_size]
                placeholders = ",".join("?" * len(chunk))
                total_deleted += conn.execute(
                    f"DELETE FROM market_change WHERE change_id IN ({placeholders})",
                    chunk,
                ).rowcount

            # Clear markets.change_id for any market whose current pointer was deleted
            deleted_set = set(to_delete)
            orphaned_markets: list[str] = [
                mid
                for mid, cid in last_change_per_market.items()
                if cid in deleted_set
            ]
            markets_cleared = 0
            if orphaned_markets:
                placeholders = ",".join("?" * len(orphaned_markets))
                markets_cleared = conn.execute(
                    f"UPDATE markets SET change_id = NULL WHERE market_id IN ({placeholders})",
                    orphaned_markets,
                ).rowcount

            conn.commit()
            result["deleted"] = total_deleted
            result["markets_cleared"] = markets_cleared
        except Exception:
            conn.rollback()
            raise

        return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Prune old market_change rows, keeping the last N days + latest per market."
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=_CUTOFF_DAYS,
        dest="days",
        help=f"Days of history to keep (default: {_CUTOFF_DAYS})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without making changes.",
    )
    args = parser.parse_args()

    try:
        r = run_prune(days=args.days, dry_run=args.dry_run)
    except Exception as e:
        print(f"Prune failed: {e}", file=sys.stderr)
        return 1

    cutoff = r["cutoff"]
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S UTC") if cutoff else "N/A"
    prefix = "[DRY RUN] " if args.dry_run else ""

    if args.dry_run:
        count = r["would_delete"]
        print(f"{prefix}Would delete {count} market_change row(s) older than {cutoff_str}.")
    else:
        count = r["deleted"]
        if count:
            print(f"Deleted {count} market_change row(s) older than {cutoff_str}.")
            if r["markets_cleared"]:
                print(
                    f"Cleared change_id for {r['markets_cleared']} market(s) "
                    "(will repopulate on next scan)."
                )
        else:
            print("Nothing to delete.")

    print(
        f"Skipped: {r['skipped_closed']} closed market(s), "
        f"{r['skipped_stale']} stale market(s) (last update > {args.days}d ago)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
