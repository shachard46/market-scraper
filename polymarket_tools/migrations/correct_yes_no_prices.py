"""
Correct historical YES/NO mistakes in polymarket.db.

Phase 1 (Gamma): If ``markets`` has yes/no token IDs reversed vs Gamma for the same
condition_id, updates ``markets`` and swaps ``yes_price`` / ``no_price`` on all
``market_change`` rows for that market. For binary markets, ``last_trade_price``
stored under the old (wrong) YES token is complemented to ``1.0 - p``; ``midpoint``
is complemented when present.

Phase 2 (heuristic): For markets left aligned after phase 1, if ``last_trade_price``
matches ``no_price`` much better than ``yes_price``, swaps price columns (legacy
NO-book midpoint bug). Sets ``midpoint`` to the new yes side (old ``no_price``).

**Before running:** copy your DB, e.g. ``cp polymarket.db polymarket.db.bak``

**Idempotency:** Safe to re-run after a successful pass (should no-op).

Run::

    python -m polymarket_tools.migrations.correct_yes_no_prices
    python -m polymarket_tools.migrations.correct_yes_no_prices --dry-run --limit 50
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

_root = Path(__file__).resolve().parent.parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from sqlalchemy import select, text

from polymarket_tools.api import _gamma_to_clob_format, _parse_json_field, fetch_market
from polymarket_tools.db import Market, get_session
from polymarket_tools.scanner import _get_token_ids


def _norm_tid(t: str | None) -> str:
    return str(t).strip() if t else ""


def _gamma_token_count(raw: dict) -> int:
    ids = _parse_json_field(
        raw.get("clobTokenIds") or raw.get("clob_token_ids"),
        [],
    )
    return len(ids) if isinstance(ids, list) else 0


def run_correct(
    *,
    dry_run: bool = False,
    verbose: bool = False,
    limit: int | None = None,
    epsilon: float = 0.04,
    sleep_s: float = 0.15,
) -> dict[str, int]:
    """
    Run phase 1 and phase 2. Returns counts of actions taken.

    Args:
        dry_run: If True, do not commit DB changes.
        limit: Max markets to process (phase 1 loop); None = all.
        epsilon: Tolerance for phase 2 last-trade heuristic.
        sleep_s: Delay between Gamma HTTP calls to reduce rate-limit risk.
    """
    stats = {
        "phase1_skipped_no_gamma": 0,
        "phase1_skipped_multi_outcome": 0,
        "phase1_skipped_mismatch": 0,
        "phase1_aligned": 0,
        "phase1_token_swap": 0,
        "phase1_mc_rows_updated": 0,
        "phase2_rows_swapped": 0,
    }
    phase1_swapped_markets: set[str] = set()

    with get_session() as session:
        stmt = select(Market.market_id, Market.yes_token_id, Market.no_token_id).order_by(
            Market.market_id
        )
        if limit is not None and limit > 0:
            stmt = stmt.limit(limit)
        rows = session.execute(stmt).all()

    for market_id, db_yes, db_no in rows:
        mid = str(market_id)
        raw = fetch_market(mid)
        if sleep_s > 0:
            time.sleep(sleep_s)

        if not raw:
            stats["phase1_skipped_no_gamma"] += 1
            continue

        if _gamma_token_count(raw) > 2:
            stats["phase1_skipped_multi_outcome"] += 1
            continue

        clob = _gamma_to_clob_format(raw)
        gy, gn = _get_token_ids(clob)
        if not gy or not gn:
            stats["phase1_skipped_mismatch"] += 1
            continue

        a, b = _norm_tid(db_yes), _norm_tid(db_no)
        gya, gnb = _norm_tid(gy), _norm_tid(gn)

        if gya == a and gnb == b:
            stats["phase1_aligned"] += 1
            continue

        if gya == b and gnb == a:
            phase1_swapped_markets.add(mid)
            stats["phase1_token_swap"] += 1
            clob_json = json.dumps([gya, gnb])

            if dry_run:
                if verbose:
                    print(
                        f"[dry-run] phase1 token swap market_id={mid} "
                        f"db_yes={a[:16]}... -> gamma_yes={gya[:16]}..."
                    )
                continue

            with get_session() as sess:
                sess.execute(
                    text(
                        """
                        UPDATE markets
                        SET yes_token_id = :gy,
                            no_token_id = :gn,
                            clob_token_ids = :cjson
                        WHERE market_id = :mid
                        """
                    ),
                    {"gy": gya, "gn": gnb, "cjson": clob_json, "mid": mid},
                )
                r = sess.execute(
                    text(
                        """
                        UPDATE market_change
                        SET yes_price = no_price,
                            no_price = yes_price,
                            last_trade_price = CASE
                                WHEN last_trade_price IS NOT NULL
                                THEN 1.0 - last_trade_price
                                ELSE NULL
                            END,
                            midpoint = CASE
                                WHEN midpoint IS NOT NULL THEN 1.0 - midpoint
                                ELSE NULL
                            END
                        WHERE market_id = :mid
                        """
                    ),
                    {"mid": mid},
                )
                stats["phase1_mc_rows_updated"] += int(r.rowcount or 0)
            continue

        stats["phase1_skipped_mismatch"] += 1
        print(
            f"[warn] market_id={mid} Gamma tokens do not match DB (not a simple swap): "
            f"db=({a[:12]}...,{b[:12]}...) gamma=({gya[:12]}...,{gnb[:12]}...)",
            file=sys.stderr,
        )

    # Phase 2: heuristic on markets not token-swapped in phase 1
    with get_session() as session:
        mc_rows = session.execute(
            text(
                """
                SELECT change_id, market_id, yes_price, no_price, last_trade_price
                FROM market_change
                WHERE last_trade_price IS NOT NULL
                ORDER BY change_id
                """
            )
        ).all()

    for change_id, m_id, yp, np, lt in mc_rows:
        mid = str(m_id)
        if mid in phase1_swapped_markets:
            continue
        if lt is None:
            continue
        try:
            lt_f = float(lt)
            y_f = float(yp)
            n_f = float(np)
        except (TypeError, ValueError):
            continue

        if abs(y_f + n_f - 1.0) > 0.02:
            continue

        if abs(lt_f - y_f) <= epsilon:
            continue
        if abs(lt_f - n_f) > epsilon:
            continue

        if dry_run:
            stats["phase2_rows_swapped"] += 1
            if verbose:
                print(
                    f"[dry-run] phase2 swap change_id={change_id} market_id={mid} "
                    f"last={lt_f} yes={y_f} no={n_f}"
                )
            continue

        with get_session() as sess:
            r = sess.execute(
                text(
                    """
                    UPDATE market_change
                    SET yes_price = no_price,
                        no_price = yes_price,
                        midpoint = no_price
                    WHERE change_id = :cid
                    """
                ),
                {"cid": change_id},
            )
            if r.rowcount:
                stats["phase2_rows_swapped"] += 1

    return stats


def main() -> None:
    p = argparse.ArgumentParser(
        description="Correct YES/NO token IDs and prices (see module docstring)."
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Log actions only; do not write to the database.",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Process at most N markets in phase 1 (Gamma loop).",
    )
    p.add_argument(
        "--epsilon",
        type=float,
        default=0.04,
        help="Phase 2: last_trade must be within this of no_price and farther from yes_price.",
    )
    p.add_argument(
        "--sleep",
        type=float,
        default=0.15,
        metavar="SEC",
        help="Seconds to sleep between Gamma requests (0 to disable).",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="With --dry-run, print each phase1/phase2 row; default is counts only.",
    )
    args = p.parse_args()

    from polymarket_tools.db import _get_db_path

    db_path = _get_db_path()
    print(
        f"Database: {db_path}\n"
        "IMPORTANT: Back up your database first, e.g.\n"
        f"  cp {db_path} {db_path}.bak\n",
        file=sys.stderr,
    )

    stats = run_correct(
        dry_run=args.dry_run,
        verbose=args.verbose,
        limit=args.limit,
        epsilon=args.epsilon,
        sleep_s=args.sleep,
    )
    print("Done.", stats)


if __name__ == "__main__":
    main()
