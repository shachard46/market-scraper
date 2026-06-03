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
    python -m polymarket_tools.migrations.prune_market_changes --days-back 7
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
from polymarket_tools.log import log

_CUTOFF_DAYS = 4
_PROGRESS_INTERVAL = 50_000  # Log every N candidate rows while scanning
_DELETE_LOG_EVERY_CHUNKS = 10  # Log delete progress every N chunks (chunk_size=500)


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def _format_count(n: int) -> str:
    return f"{n:,}"


def _progress_bar(current: int, total: int, width: int = 40) -> str:
    if total <= 0:
        return "[" + "=" * width + "] 100%"
    filled = int(width * current / total)
    bar = "=" * filled + "-" * (width - filled)
    pct = 100 * current / total
    return f"[{bar}] {pct:5.1f}% ({_format_count(current)}/{_format_count(total)})"


def _log_progress(current: int, total: int, label: str) -> None:
    """Log when crossing ~10% thresholds to avoid spam."""
    if total <= 0:
        return
    step = max(1, total // 10)
    if current == total or current % step == 0:
        log(f"  {label}: {_progress_bar(current, total)}")


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
        log(f"Database not found: {path}")
        return {"deleted": 0, "would_delete": 0, "markets_cleared": 0, "cutoff": None}

    mode = "dry-run" if dry_run else "prune"
    log(f"Starting market_change {mode} (keep last {days} day(s))")
    log(f"  database: {path}")

    cutoff: datetime = _utcnow() - timedelta(days=days)
    # SQLite stores datetimes as text; use ISO format without timezone offset
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    log(f"  cutoff:   {cutoff_str} UTC (rows older than this may be deleted)")

    with sqlite3.connect(str(path)) as conn:
        conn.row_factory = sqlite3.Row

        total_rows = conn.execute("SELECT COUNT(*) FROM market_change").fetchone()[0]
        log(f"  market_change rows in DB: {_format_count(total_rows)}")

        # ── 1. markets to skip entirely ──────────────────────────────────────
        log("Phase 1/4: Loading markets to skip (closed + stale)...")
        closed_ids: set[str] = {
            row["market_id"]
            for row in conn.execute(
                "SELECT market_id FROM markets WHERE status = 'closed'"
            )
        }
        log(f"  closed markets: {_format_count(len(closed_ids))}")

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
        log(f"  stale markets (last update > {days}d ago): {_format_count(len(stale_ids))}")

        skip_ids: set[str] = closed_ids | stale_ids
        log(f"  total skip set: {_format_count(len(skip_ids))} market(s)")

        # ── 2. last change_id per market (always keep these) ─────────────────
        log("Phase 2/4: Resolving latest change per market...")
        last_change_per_market: dict[str, int] = {
            row["market_id"]: row["last_id"]
            for row in conn.execute(
                "SELECT market_id, MAX(change_id) AS last_id FROM market_change GROUP BY market_id"
            )
        }
        protected_ids: set[int] = set(last_change_per_market.values())
        log(f"  markets tracked: {_format_count(len(last_change_per_market))}")
        log(f"  protected rows (latest per market): {_format_count(len(protected_ids))}")

        # ── 3. find rows to delete ────────────────────────────────────────────
        log("Phase 3/4: Scanning candidate rows older than cutoff...")
        cursor = conn.execute(
            """
            SELECT change_id, market_id
            FROM market_change
            WHERE datetime < ?
            """,
            (cutoff_str,),
        )

        to_delete: list[int] = []
        scanned = 0
        for row in cursor:
            scanned += 1
            if row["market_id"] not in skip_ids and row["change_id"] not in protected_ids:
                to_delete.append(row["change_id"])
            if scanned % _PROGRESS_INTERVAL == 0:
                log(
                    f"  scanned {_format_count(scanned)} candidates, "
                    f"queued {_format_count(len(to_delete))} for deletion..."
                )

        log(
            f"  scan complete: {_format_count(scanned)} candidate(s), "
            f"{_format_count(len(to_delete))} to delete"
        )

        result = {
            "deleted": 0,
            "would_delete": len(to_delete),
            "markets_cleared": 0,
            "cutoff": cutoff,
            "skipped_closed": len(closed_ids),
            "skipped_stale": len(stale_ids),
            "candidates_scanned": scanned,
            "total_rows": total_rows,
        }

        if dry_run or not to_delete:
            if dry_run:
                log("Dry-run complete — no rows deleted.")
            elif not to_delete:
                log("Nothing to delete.")
            return result

        # ── 4. apply deletions in a transaction ───────────────────────────────
        log(f"Phase 4/4: Deleting {_format_count(len(to_delete))} row(s)...")
        conn.execute("BEGIN")
        try:
            chunk_size = 500
            num_chunks = (len(to_delete) + chunk_size - 1) // chunk_size
            total_deleted = 0
            for chunk_idx, i in enumerate(range(0, len(to_delete), chunk_size), start=1):
                chunk = to_delete[i : i + chunk_size]
                placeholders = ",".join("?" * len(chunk))
                total_deleted += conn.execute(
                    f"DELETE FROM market_change WHERE change_id IN ({placeholders})",
                    chunk,
                ).rowcount
                if chunk_idx % _DELETE_LOG_EVERY_CHUNKS == 0 or chunk_idx == num_chunks:
                    _log_progress(chunk_idx, num_chunks, "delete chunks")

            log(f"  deleted {_format_count(total_deleted)} row(s)")

            deleted_set = set(to_delete)
            orphaned_markets: list[str] = [
                mid
                for mid, cid in last_change_per_market.items()
                if cid in deleted_set
            ]
            markets_cleared = 0
            if orphaned_markets:
                log(f"  clearing change_id for {_format_count(len(orphaned_markets))} market(s)...")
                placeholders = ",".join("?" * len(orphaned_markets))
                markets_cleared = conn.execute(
                    f"UPDATE markets SET change_id = NULL WHERE market_id IN ({placeholders})",
                    orphaned_markets,
                ).rowcount

            conn.commit()
            log("Commit complete.")
            result["deleted"] = total_deleted
            result["markets_cleared"] = markets_cleared
        except Exception:
            conn.rollback()
            log("Rolled back transaction after error.")
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
        log(f"Prune failed: {e}")
        print(f"Prune failed: {e}", file=sys.stderr)
        return 1

    cutoff = r["cutoff"]
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S UTC") if cutoff else "N/A"
    prefix = "[DRY RUN] " if args.dry_run else ""

    if args.dry_run:
        count = r["would_delete"]
        summary = (
            f"{prefix}Would delete {_format_count(count)} of "
            f"{_format_count(r.get('candidates_scanned', 0))} candidate row(s) "
            f"(older than {cutoff_str})."
        )
    else:
        count = r["deleted"]
        if count:
            summary = f"Deleted {_format_count(count)} market_change row(s) older than {cutoff_str}."
            if r["markets_cleared"]:
                summary += (
                    f" Cleared change_id for {_format_count(r['markets_cleared'])} market(s) "
                    "(will repopulate on next scan)."
                )
        else:
            summary = "Nothing to delete."

    skip_summary = (
        f"Skipped {_format_count(r['skipped_closed'])} closed and "
        f"{_format_count(r['skipped_stale'])} stale market(s) "
        f"(last update > {args.days}d ago)."
    )
    log(f"Summary: {summary}")
    log(skip_summary)
    print(summary)
    print(skip_summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
