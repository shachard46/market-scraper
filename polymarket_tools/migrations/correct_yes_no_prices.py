"""
Correct historical YES/NO mistakes in polymarket.db.

Phase 1 (Gamma): Bulk-fetches all active and closed markets from Gamma in one pass
(paginated, ~170 requests instead of ~8,000), builds a condition_id → market map,
then for each ``markets`` row checks whether yes/no token IDs are reversed vs Gamma.
If reversed, updates ``markets`` and swaps ``yes_price`` / ``no_price`` on all
``market_change`` rows for that market. For binary markets, ``last_trade_price``
stored under the old (wrong) YES token is complemented to ``1.0 - p``; ``midpoint``
is complemented when present.

Phase 2 (heuristic): For markets left aligned after phase 1, if ``last_trade_price``
matches ``no_price`` much better than ``yes_price``, swaps price columns (legacy
NO-book midpoint bug). Sets ``midpoint`` to the new yes side (old ``no_price``).
Gamma prices veto the swap when they already confirm ``yes_price`` is correct —
this handles markets where the CLOB orderbook reports a No-side trade as the
YES token's ``last_trade_price`` (e.g. xi-jinping-out-before-2027).

Phase 2b: Gamma-authoritative pass over ALL rows — if ``yes_price`` is clearly closer
to Gamma's NO token price than its YES token price, swap columns. This supersedes the
old NULL-LTP-only check and also corrects rows that were previously mis-swapped by the
LTP heuristic (e.g. markets where the CLOB book reports a No-side trade as the YES
token's ``last_trade_price``).

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
from pathlib import Path

_root = Path(__file__).resolve().parent.parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from sqlalchemy import select, text

from polymarket_tools.api import (
    _fetch_gamma_markets,
    _gamma_to_clob_format,
    _parse_json_field,
    fetch_closed_gamma_markets,
)


def _gamma_prices_for_db_tokens(
    raw: dict,
    db_yes: str,
    db_no: str,
) -> tuple[float | None, float | None]:
    """Map Gamma outcomePrices to DB yes/no token IDs (binary markets only)."""
    tids = _parse_json_field(
        raw.get("clobTokenIds") or raw.get("clob_token_ids"),
        [],
    )
    prices = _parse_json_field(raw.get("outcomePrices"), [])
    if not isinstance(tids, list) or len(tids) < 2:
        return None, None
    by_tid: dict[str, float] = {}
    for i in range(min(2, len(tids), len(prices))):
        try:
            by_tid[str(tids[i])] = float(prices[i])
        except (TypeError, ValueError):
            continue
    return by_tid.get(str(db_yes)), by_tid.get(str(db_no))
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


def _build_gamma_map(closed_limit: int = 99_999) -> dict[str, dict]:
    """
    Bulk-fetch all active and closed markets from Gamma, returning a dict
    keyed by condition_id. Two paginated calls replace ~8,000 per-market calls.
    """
    print("Fetching active markets from Gamma...", file=sys.stderr)
    active = _fetch_gamma_markets()
    print(f"  Got {len(active)} active markets.", file=sys.stderr)

    print("Fetching closed markets from Gamma...", file=sys.stderr)
    closed = fetch_closed_gamma_markets(limit=closed_limit)
    print(f"  Got {len(closed)} closed markets.", file=sys.stderr)

    gamma_map: dict[str, dict] = {}
    for raw in active + closed:
        cid = raw.get("conditionId") or raw.get("condition_id")
        if cid:
            gamma_map[str(cid)] = raw
    print(f"  Gamma map: {len(gamma_map)} unique markets.", file=sys.stderr)
    return gamma_map


def run_correct(
    *,
    dry_run: bool = False,
    verbose: bool = False,
    limit: int | None = None,
    epsilon: float = 0.04,
) -> dict[str, int]:
    """
    Run phase 1 and phase 2. Returns counts of actions taken.

    Args:
        dry_run: If True, do not commit DB changes.
        limit: Max markets to process from the DB in phase 1; None = all.
        epsilon: Tolerance for phase 2 last-trade heuristic.
    """
    stats = {
        "phase1_skipped_no_gamma": 0,
        "phase1_skipped_multi_outcome": 0,
        "phase1_skipped_mismatch": 0,
        "phase1_aligned": 0,
        "phase1_token_swap": 0,
        "phase1_mc_rows_updated": 0,
        "phase2_rows_swapped": 0,
        "phase2_gamma_vetoed": 0,
        "phase2b_rows_swapped": 0,
    }
    phase1_swapped_markets: set[str] = set()

    gamma_map = _build_gamma_map()

    with get_session() as session:
        stmt = select(Market.market_id, Market.yes_token_id, Market.no_token_id).order_by(
            Market.market_id
        )
        if limit is not None and limit > 0:
            stmt = stmt.limit(limit)
        rows = session.execute(stmt).all()

    print(f"Checking {len(rows)} DB markets against Gamma...", file=sys.stderr)
    for market_id, db_yes, db_no in rows:
        mid = str(market_id)
        raw = gamma_map.get(mid)

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

    # Phase 2: heuristic on markets not token-swapped in phase 1.
    # JOIN markets to get token IDs so we can veto the swap via Gamma prices (same
    # guard as scanner._finalize_binary_yes_no_prices: if Gamma already confirms
    # yes_price is on the correct side the LTP is a No-side trade, not an inversion).
    with get_session() as session:
        mc_rows = session.execute(
            text(
                """
                SELECT mc.change_id, mc.market_id, mc.yes_price, mc.no_price,
                       mc.last_trade_price, m.yes_token_id, m.no_token_id
                FROM market_change mc
                JOIN markets m ON m.market_id = mc.market_id
                WHERE mc.last_trade_price IS NOT NULL
                ORDER BY mc.change_id
                """
            )
        ).all()

    for change_id, m_id, yp, np, lt, db_yes, db_no in mc_rows:
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

        # Veto: if Gamma prices are available and confirm yes_price is already on the
        # correct side, the LTP is a No-side trade price — do not swap.
        raw = gamma_map.get(mid)
        if raw:
            gy, gn = _gamma_prices_for_db_tokens(raw, str(db_yes), str(db_no))
            if (
                gy is not None
                and gn is not None
                and abs(gy + gn - 1.0) <= 0.08
                and abs(y_f - gy) <= abs(y_f - gn)
            ):
                stats["phase2_gamma_vetoed"] += 1
                if verbose:
                    print(
                        f"[phase2 skip] change_id={change_id} market_id={mid} "
                        f"last={lt_f} yes={y_f} no={n_f} "
                        f"gamma_yes={gy} gamma_no={gn} — Gamma confirms current yes_price"
                    )
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

    # Phase 2b: Gamma-authoritative pass — covers ALL rows (NULL and non-NULL LTP).
    # If yes_price is clearly closer to Gamma's NO token price than its YES token price,
    # the columns are inverted regardless of what last_trade_price says.
    # This corrects rows that were previously mis-swapped by the old LTP heuristic
    # (e.g. markets where the CLOB book reports a No-side trade as the YES token LTP).
    epsilon_b = max(epsilon, 0.055)
    with get_session() as session:
        mc_b = session.execute(
            text(
                """
                SELECT mc.change_id, mc.market_id, mc.yes_price, mc.no_price,
                       m.yes_token_id, m.no_token_id
                FROM market_change mc
                JOIN markets m ON m.market_id = mc.market_id
                ORDER BY mc.change_id
                """
            )
        ).all()

    for change_id, m_id, yp, np_, db_yes, db_no in mc_b:
        mid = str(m_id)
        if mid in phase1_swapped_markets:
            continue
        raw = gamma_map.get(mid)
        if not raw:
            continue
        try:
            y_f = float(yp)
            n_f = float(np_)
        except (TypeError, ValueError):
            continue
        if abs(y_f + n_f - 1.0) > 0.03:
            continue
        gy, gn = _gamma_prices_for_db_tokens(raw, str(db_yes), str(db_no))
        if gy is None or gn is None or abs(gy + gn - 1.0) > 0.08:
            continue
        if abs(y_f - gn) + 0.02 >= abs(y_f - gy) or abs(y_f - gn) > epsilon_b * 2:
            continue

        if dry_run:
            stats["phase2b_rows_swapped"] += 1
            if verbose:
                print(
                    f"[dry-run] phase2b swap change_id={change_id} market_id={mid} "
                    f"yes_col={y_f} no_col={n_f} gamma_yes={gy} gamma_no={gn}"
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
                stats["phase2b_rows_swapped"] += 1

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
    )
    print("Done.", stats)


if __name__ == "__main__":
    main()
