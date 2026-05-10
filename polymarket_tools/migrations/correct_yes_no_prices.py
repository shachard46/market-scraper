"""
Correct historical YES/NO mistakes in polymarket.db.

Operational contract:
    1. Back up the database before running in apply mode.
    2. Pause scanner/writer processes during token alignment to avoid semantic
       skew between ``markets`` and newly inserted ``market_change`` rows.
    3. Treat dry-run output as a validation report, not proof that every
       historical row is clean; ambiguous rows are reported separately.

The repair is intentionally narrow:
    - Token IDs are aligned against Gamma's binary YES/NO mapping.
    - Price columns are swapped only when current Gamma prices clearly indicate
      that DB ``yes_price`` is closer to Gamma NO than Gamma YES.
    - ``last_trade_price`` is never sufficient by itself to mutate prices. It is
      reported as ``suspicious_not_repaired`` when it resembles the old failure
      mode so operators can review residual historical debt.

Re-running is expected to no-op after a complete successful run. Partial runs or
manual DB edits can still produce mismatch warnings; use ``--phase`` to rerun a
specific phase after fixing upstream data issues.

Run::

    python -m polymarket_tools.migrations.correct_yes_no_prices --dry-run
    python -m polymarket_tools.migrations.correct_yes_no_prices --validate
    python -m polymarket_tools.migrations.correct_yes_no_prices --phase tokens
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
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
from polymarket_tools.db import Market, get_session
from polymarket_tools.scanner import _binary_market_view


@dataclass(frozen=True)
class GammaBinaryMarket:
    market_id: str
    yes_token_id: str
    no_token_id: str
    yes_price: float | None
    no_price: float | None
    raw: dict


def _norm_tid(t: str | None) -> str:
    return str(t).strip() if t else ""


def _gamma_token_count(raw: dict) -> int:
    ids = _parse_json_field(
        raw.get("clobTokenIds") or raw.get("clob_token_ids"),
        [],
    )
    return len(ids) if isinstance(ids, list) else 0


def load_gamma_binary_markets(closed_limit: int = 99_999) -> tuple[dict[str, GammaBinaryMarket], dict[str, int]]:
    """
    Bulk-fetch active and closed Gamma markets and keep only explicit binary markets.
    """
    stats = {
        "gamma_markets_loaded": 0,
        "gamma_skipped_no_condition_id": 0,
        "gamma_skipped_multi_outcome": 0,
        "gamma_skipped_non_binary": 0,
    }
    print("Fetching active markets from Gamma...", file=sys.stderr)
    active = _fetch_gamma_markets()
    print(f"  Got {len(active)} active markets.", file=sys.stderr)

    print("Fetching closed markets from Gamma...", file=sys.stderr)
    closed = fetch_closed_gamma_markets(limit=closed_limit)
    print(f"  Got {len(closed)} closed markets.", file=sys.stderr)

    gamma_map: dict[str, GammaBinaryMarket] = {}
    for raw in active + closed:
        cid = raw.get("conditionId") or raw.get("condition_id")
        if not cid:
            stats["gamma_skipped_no_condition_id"] += 1
            continue
        if _gamma_token_count(raw) > 2:
            stats["gamma_skipped_multi_outcome"] += 1
            continue
        clob = _gamma_to_clob_format(raw)
        binary = _binary_market_view(clob)
        if binary is None:
            stats["gamma_skipped_non_binary"] += 1
            continue
        gamma_map[str(cid)] = GammaBinaryMarket(
            market_id=str(cid),
            yes_token_id=binary.yes_token_id,
            no_token_id=binary.no_token_id,
            yes_price=binary.gamma_yes_price,
            no_price=binary.gamma_no_price,
            raw=raw,
        )
        stats["gamma_markets_loaded"] += 1
    print(f"  Gamma map: {len(gamma_map)} unique markets.", file=sys.stderr)
    return gamma_map, stats


def classify_token_alignment(
    db_yes: str | None,
    db_no: str | None,
    gamma: GammaBinaryMarket,
) -> str:
    """Return aligned, reversed, or mismatch for DB token columns vs Gamma."""
    a, b = _norm_tid(db_yes), _norm_tid(db_no)
    gy, gn = _norm_tid(gamma.yes_token_id), _norm_tid(gamma.no_token_id)
    if gy == a and gn == b:
        return "aligned"
    if gy == b and gn == a:
        return "reversed"
    return "mismatch"


def _to_float(value) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _complement_is_sane(yes_price: float, no_price: float, tolerance: float = 0.03) -> bool:
    return abs(yes_price + no_price - 1.0) <= tolerance


def _gamma_prices_are_sane(gamma: GammaBinaryMarket) -> bool:
    if gamma.yes_price is None or gamma.no_price is None:
        return False
    return abs(gamma.yes_price + gamma.no_price - 1.0) <= 0.08


def repair_price_swap_when_gamma_clear(
    yes_price: float,
    no_price: float,
    gamma: GammaBinaryMarket,
    *,
    epsilon: float = 0.055,
) -> bool:
    """Return True when DB yes/no price columns are clearly inverted vs Gamma."""
    if not _complement_is_sane(yes_price, no_price) or not _gamma_prices_are_sane(gamma):
        return False
    assert gamma.yes_price is not None and gamma.no_price is not None
    return (
        abs(yes_price - gamma.no_price) + 0.02 < abs(yes_price - gamma.yes_price)
        and abs(yes_price - gamma.no_price) <= epsilon * 2
    )


def _ltp_suspicious_not_proof(
    yes_price: float,
    no_price: float,
    last_trade_price: float | None,
    *,
    epsilon: float,
) -> bool:
    """Detect the old LTP-near-NO pattern without treating it as repair proof."""
    if last_trade_price is None:
        return False
    if not _complement_is_sane(yes_price, no_price, tolerance=0.02):
        return False
    return (
        abs(last_trade_price - yes_price) > epsilon
        and abs(last_trade_price - no_price) <= epsilon
    )


def _count_suspicious_ltp_after_token_swap(
    session,
    market_id: str,
    gamma: GammaBinaryMarket,
    *,
    tolerance: float = 0.20,
) -> int:
    """Count rows where complemented LTP would sharply disagree with Gamma YES."""
    if gamma.yes_price is None:
        return 0
    row = session.execute(
        text(
            """
            SELECT COUNT(*)
            FROM market_change
            WHERE market_id = :mid
              AND last_trade_price IS NOT NULL
              AND ABS((1.0 - last_trade_price) - :gy) > :tolerance
            """
        ),
        {"mid": market_id, "gy": gamma.yes_price, "tolerance": tolerance},
    ).first()
    return int(row[0] if row else 0)


def repair_token_swap(session, market_id: str, gamma: GammaBinaryMarket) -> int:
    """
    Align market token columns and all historical price rows for a reversed market.

    last_trade_price is complemented under the historical assumption that the
    stored observation followed the old YES column. Suspicious complements are
    counted in dry-run/reporting before mutation.
    """
    clob_json = json.dumps([gamma.yes_token_id, gamma.no_token_id])
    session.execute(
        text(
            """
            UPDATE markets
            SET yes_token_id = :gy,
                no_token_id = :gn,
                clob_token_ids = :cjson
            WHERE market_id = :mid
            """
        ),
        {
            "gy": gamma.yes_token_id,
            "gn": gamma.no_token_id,
            "cjson": clob_json,
            "mid": market_id,
        },
    )
    result = session.execute(
        text(
            """
            UPDATE market_change
            SET yes_price = no_price,
                no_price = yes_price,
                last_trade_price = CASE
                    WHEN last_trade_price IS NOT NULL THEN 1.0 - last_trade_price
                    ELSE NULL
                END,
                midpoint = CASE
                    WHEN midpoint IS NOT NULL THEN 1.0 - midpoint
                    ELSE NULL
                END
            WHERE market_id = :mid
            """
        ),
        {"mid": market_id},
    )
    return int(result.rowcount or 0)


def _verification_stats(epsilon: float = 0.03) -> dict[str, int]:
    with get_session() as session:
        row = session.execute(
            text(
                """
                SELECT COUNT(*)
                FROM market_change
                WHERE ABS(yes_price + no_price - 1.0) > :epsilon
                """
            ),
            {"epsilon": epsilon},
        ).first()
    return {"verify_non_complement_rows": int(row[0] if row else 0)}


def run_correct(
    *,
    dry_run: bool = False,
    verbose: bool = False,
    limit: int | None = None,
    epsilon: float = 0.04,
    phase: str = "all",
) -> dict[str, int]:
    """
    Validate and optionally repair token/price inversions. Returns counters.

    Args:
        dry_run: If True, do not commit DB changes.
        limit: Max markets to process from the DB in phase 1; None = all.
        epsilon: Tolerance for suspicious LTP reporting and Gamma price repair.
        phase: all, tokens, or prices.
    """
    stats = {
        "token_skipped_no_gamma": 0,
        "token_aligned": 0,
        "token_reversed": 0,
        "token_mismatch": 0,
        "token_swap_markets": 0,
        "token_swap_mc_rows_updated": 0,
        "token_swap_ltp_suspicious": 0,
        "price_rows_examined": 0,
        "gamma_clear_swap": 0,
        "price_rows_swapped": 0,
        "suspicious_not_repaired": 0,
        "skipped_ambiguous": 0,
    }
    token_swapped_markets: set[str] = set()

    gamma_map, gamma_stats = load_gamma_binary_markets()
    stats.update(gamma_stats)

    with get_session() as session:
        stmt = select(Market.market_id, Market.yes_token_id, Market.no_token_id).order_by(
            Market.market_id
        )
        if limit is not None and limit > 0:
            stmt = stmt.limit(limit)
        rows = session.execute(stmt).all()

    print(f"Checking {len(rows)} DB markets against Gamma...", file=sys.stderr)
    if phase in ("all", "tokens"):
        for market_id, db_yes, db_no in rows:
            mid = str(market_id)
            gamma = gamma_map.get(mid)
            if gamma is None:
                stats["token_skipped_no_gamma"] += 1
                continue

            alignment = classify_token_alignment(str(db_yes), str(db_no), gamma)
            if alignment == "aligned":
                stats["token_aligned"] += 1
                continue
            if alignment == "mismatch":
                stats["token_mismatch"] += 1
                print(
                    f"[warn] market_id={mid} Gamma tokens do not match DB (not a simple swap): "
                    f"db=({_norm_tid(db_yes)[:12]}...,{_norm_tid(db_no)[:12]}...) "
                    f"gamma=({gamma.yes_token_id[:12]}...,{gamma.no_token_id[:12]}...)",
                    file=sys.stderr,
                )
                continue

            token_swapped_markets.add(mid)
            stats["token_reversed"] += 1
            if verbose or dry_run:
                print(
                    f"[{'dry-run' if dry_run else 'apply'}] token swap market_id={mid} "
                    f"db_yes={_norm_tid(db_yes)[:16]}... -> gamma_yes={gamma.yes_token_id[:16]}...",
                    file=sys.stderr,
                )
            with get_session() as sess:
                stats["token_swap_ltp_suspicious"] += _count_suspicious_ltp_after_token_swap(
                    sess, mid, gamma
                )
                if dry_run:
                    continue
                updated = repair_token_swap(sess, mid, gamma)
                stats["token_swap_markets"] += 1
                stats["token_swap_mc_rows_updated"] += updated

    if phase in ("all", "prices"):
        with get_session() as session:
            price_rows = session.execute(
                text(
                    """
                    SELECT mc.change_id, mc.market_id, mc.yes_price, mc.no_price,
                           mc.last_trade_price, mc.datetime
                    FROM market_change mc
                    JOIN markets m ON m.market_id = mc.market_id
                    ORDER BY mc.market_id, mc.change_id
                    """
                )
            ).all()

        repairs_by_market: dict[str, list[int]] = {}
        epsilon_b = max(epsilon, 0.055)
        for change_id, market_id, yp, np_, lt, _dt in price_rows:
            mid = str(market_id)
            if mid in token_swapped_markets:
                continue
            gamma = gamma_map.get(mid)
            y_f, n_f = _to_float(yp), _to_float(np_)
            lt_f = _to_float(lt)
            if y_f is None or n_f is None:
                stats["skipped_ambiguous"] += 1
                continue
            stats["price_rows_examined"] += 1

            if gamma is None or not _gamma_prices_are_sane(gamma):
                if _ltp_suspicious_not_proof(y_f, n_f, lt_f, epsilon=epsilon):
                    stats["suspicious_not_repaired"] += 1
                else:
                    stats["skipped_ambiguous"] += 1
                continue

            if repair_price_swap_when_gamma_clear(y_f, n_f, gamma, epsilon=epsilon_b):
                stats["gamma_clear_swap"] += 1
                repairs_by_market.setdefault(mid, []).append(int(change_id))
                if verbose or dry_run:
                    print(
                        f"[{'dry-run' if dry_run else 'apply'}] gamma-clear price swap "
                        f"change_id={change_id} market_id={mid} "
                        f"yes_col={y_f} no_col={n_f} "
                        f"gamma_yes={gamma.yes_price} gamma_no={gamma.no_price}",
                        file=sys.stderr,
                    )
                continue

            if _ltp_suspicious_not_proof(y_f, n_f, lt_f, epsilon=epsilon):
                stats["suspicious_not_repaired"] += 1

        if not dry_run:
            for _market_id, change_ids in repairs_by_market.items():
                with get_session() as sess:
                    for change_id in change_ids:
                        result = sess.execute(
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
                        if result.rowcount:
                            stats["price_rows_swapped"] += 1

    stats.update(_verification_stats())
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
        "--validate",
        action="store_true",
        help="Alias for --dry-run; report possible flips without mutation.",
    )
    p.add_argument(
        "--phase",
        choices=("all", "tokens", "prices"),
        default="all",
        help="Run only token alignment, only price repair, or both.",
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
        help="Tolerance for suspicious LTP reporting and Gamma-clear price repair.",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Print each token or price repair candidate; default is counts only.",
    )
    args = p.parse_args()

    from polymarket_tools.db import _get_db_path

    db_path = _get_db_path()
    print(
        f"Database: {db_path}\n"
        "IMPORTANT: Back up your database first, e.g.\n"
        f"  cp {db_path} {db_path}.bak\n"
        "For apply mode, pause scanner/writer processes while token alignment runs.\n",
        file=sys.stderr,
    )

    stats = run_correct(
        dry_run=args.dry_run or args.validate,
        verbose=args.verbose,
        limit=args.limit,
        epsilon=args.epsilon,
        phase=args.phase,
    )
    print("Done.", stats)


if __name__ == "__main__":
    main()
