"""Timestamped logging for scraper operations."""

import sys
from datetime import datetime, timezone


def log(msg: str) -> None:
    """Print message to stderr with UTC timestamp prefix."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{ts}] {msg}", file=sys.stderr)
