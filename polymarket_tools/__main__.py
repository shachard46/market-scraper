"""
CLI entry point for Polymarket tools.

Commands:
    setup                Initialize the SQLite database
    scan                 Run a single scan (for cron/scheduled runs)
    sync_closed_markets  Sync markets that have closed since last scan
    poll                 Run background polling loop
    get_all_markets      List all markets
    get_market_trends    Get price/volume history for a market
    get_category_markets  List markets by category
    get_closed_markets   List closed/resolved markets
    get_open_markets     List open/active markets
    get_market          Get full market details (including enriched fields)
    query_market_field  Get a single field value for a market
"""

import argparse
import asyncio
import json
import sys
from typing import NoReturn

from . import db, scanner, tools


def _cmd_setup(_: argparse.Namespace) -> int:
    """Initialize the database."""
    path = db.setup_db()
    print(f"Database initialized at {path}", file=sys.stderr)
    return 0


def _cmd_scan(args: argparse.Namespace) -> int:
    """Run a single scan."""
    market_id = getattr(args, "market", None)
    if market_id:
        try:
            ok = scanner.scan_single_market(market_id)
            if ok:
                print(f"Scanned market: {market_id}", file=sys.stderr)
                return 0
            print(f"Market not found: {market_id}", file=sys.stderr)
            return 1
        except Exception as e:
            print(f"Scan failed: {e}", file=sys.stderr)
            return 1
    limit = getattr(args, "limit", None)
    active_only = getattr(args, "active_only", True)
    batch_only = getattr(args, "batch_only", False)
    try:
        n = scanner.scan_once(limit=limit, active_only=active_only, batch_only=batch_only)
        print(f"Scanned {n} markets", file=sys.stderr)
        return 0
    except Exception as e:
        print(f"Scan failed: {e}", file=sys.stderr)
        return 1


def _cmd_sync_closed_markets(args: argparse.Namespace) -> int:
    """Sync markets that have closed since the last scan."""
    limit = getattr(args, "limit", 500)
    try:
        n = scanner.sync_closed_markets(limit=limit)
        print(f"Synced {n} closed markets", file=sys.stderr)
        return 0
    except Exception as e:
        print(f"Sync failed: {e}", file=sys.stderr)
        return 1


def _cmd_poll(args: argparse.Namespace) -> NoReturn:
    """Run the polling loop indefinitely."""
    interval = getattr(args, "interval", 5) * 60  # seconds
    print(f"Starting poll loop (interval={interval}s)", file=sys.stderr)

    async def loop() -> None:
        while True:
            try:
                n = scanner.scan_once(
                    limit=getattr(args, "scan_limit", None),
                    active_only=getattr(args, "active_only", True),
                    batch_only=getattr(args, "batch_only", False),
                )
                print(f"[poll] Scanned {n} markets", file=sys.stderr)
            except Exception as e:
                print(f"[poll] Error: {e}", file=sys.stderr)
            await asyncio.sleep(interval)

    asyncio.run(loop())


def _cmd_get_all_markets(args: argparse.Namespace) -> int:
    """List all markets."""
    limit = getattr(args, "limit", 50)
    data = tools.get_all_markets(limit=limit)
    print(json.dumps(data, indent=2))
    return 0


def _cmd_get_market_trends(args: argparse.Namespace) -> int:
    """Get trends for a market."""
    market_id = getattr(args, "market_id", None)
    if not market_id:
        print("Error: market_id required", file=sys.stderr)
        return 1
    limit = getattr(args, "limit", 50)
    data = tools.get_market_trends(market_id=market_id, limit=limit)
    print(json.dumps(data, indent=2))
    return 0


def _cmd_get_category_markets(args: argparse.Namespace) -> int:
    """List markets by category."""
    category = getattr(args, "category", None)
    if not category:
        print("Error: category required", file=sys.stderr)
        return 1
    limit = getattr(args, "limit", 50)
    data = tools.get_category_markets(category_name=category, limit=limit)
    print(json.dumps(data, indent=2))
    return 0


def _cmd_get_closed_markets(args: argparse.Namespace) -> int:
    """List closed/resolved markets."""
    limit = getattr(args, "limit", 50)
    data = tools.get_closed_markets(limit=limit)
    print(json.dumps(data, indent=2))
    return 0


def _cmd_get_open_markets(args: argparse.Namespace) -> int:
    """List open/active markets."""
    limit = getattr(args, "limit", 50)
    data = tools.get_open_markets(limit=limit)
    print(json.dumps(data, indent=2))
    return 0


def _cmd_get_market(args: argparse.Namespace) -> int:
    """Get full market details including enriched fields."""
    market_id = getattr(args, "market_id", None)
    if not market_id:
        print("Error: market_id required", file=sys.stderr)
        return 1
    data = tools.get_market(market_id=market_id)
    if data is None:
        print(f"Market not found: {market_id}", file=sys.stderr)
        return 1
    print(json.dumps(data, indent=2))
    return 0


def _cmd_query_market_field(args: argparse.Namespace) -> int:
    """Get a single field value for a market."""
    market_id = getattr(args, "market_id", None)
    field_name = getattr(args, "field_name", None)
    if not market_id or not field_name:
        print("Error: market_id and field_name required", file=sys.stderr)
        return 1
    try:
        value = tools.query_market_field(market_id=market_id, field_name=field_name)
        print(json.dumps({"market_id": market_id, "field": field_name, "value": value}))
        return 0
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def main() -> int:
    """Parse args and dispatch to command handler."""
    parser = argparse.ArgumentParser(prog="polymarket_tools", description="Polymarket CLOB scanner and query tools")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("setup").set_defaults(handler=_cmd_setup)
    scan_p = sub.add_parser("scan")
    scan_p.add_argument("--limit", type=int, default=None, help="Max markets to scan (default: all)")
    scan_p.add_argument("--all", action="store_false", dest="active_only", help="Include closed/archived markets")
    scan_p.add_argument("--market", type=str, default=None, help="Scan single market by condition_id or slug")
    scan_p.add_argument("--batch-only", action="store_true", dest="batch_only", help="Use only batch API calls (no per-market orderbook fetches)")
    scan_p.set_defaults(handler=_cmd_scan)

    sync_closed_p = sub.add_parser("sync_closed_markets")
    sync_closed_p.add_argument("--limit", type=int, default=500, help="Max closed markets to fetch from API (default: 500)")
    sync_closed_p.set_defaults(handler=_cmd_sync_closed_markets)

    poll_p = sub.add_parser("poll")
    poll_p.add_argument("--interval", type=int, default=5, help="Poll interval in minutes")
    poll_p.add_argument("--limit", type=int, default=None, dest="scan_limit", help="Max markets per scan (default: all)")
    poll_p.add_argument("--all", action="store_false", dest="active_only", help="Include closed/archived markets")
    poll_p.add_argument("--batch-only", action="store_true", dest="batch_only", help="Use only batch API calls (no per-market orderbook fetches)")
    poll_p.set_defaults(handler=_cmd_poll)

    m_p = sub.add_parser("get_all_markets")
    m_p.add_argument("--limit", type=int, default=50)
    m_p.set_defaults(handler=_cmd_get_all_markets)

    t_p = sub.add_parser("get_market_trends")
    t_p.add_argument("market_id", help="Polymarket condition_id")
    t_p.add_argument("--limit", type=int, default=50)
    t_p.set_defaults(handler=_cmd_get_market_trends)

    c_p = sub.add_parser("get_category_markets")
    c_p.add_argument("category", help="Market category (e.g., from tags)")
    c_p.add_argument("--limit", type=int, default=50)
    c_p.set_defaults(handler=_cmd_get_category_markets)

    closed_p = sub.add_parser("get_closed_markets")
    closed_p.add_argument("--limit", type=int, default=50)
    closed_p.set_defaults(handler=_cmd_get_closed_markets)

    open_p = sub.add_parser("get_open_markets")
    open_p.add_argument("--limit", type=int, default=50)
    open_p.set_defaults(handler=_cmd_get_open_markets)

    gm_p = sub.add_parser("get_market")
    gm_p.add_argument("market_id", help="Polymarket condition_id")
    gm_p.set_defaults(handler=_cmd_get_market)

    qf_p = sub.add_parser("query_market_field")
    qf_p.add_argument("market_id", help="Polymarket condition_id")
    qf_p.add_argument("field_name", help="Market field (e.g., question, status, slug)")
    qf_p.set_defaults(handler=_cmd_query_market_field)

    args = parser.parse_args()
    handler = args.handler
    if handler is _cmd_poll:
        _cmd_poll(args)
        return 0  # unreachable
    return handler(args)


if __name__ == "__main__":
    sys.exit(main())
