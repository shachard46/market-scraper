# Polymarket OpenClaw Tool

Python-based OpenClaw skill that periodically scans the Polymarket CLOB API, stores market data in SQLite, and exposes query tools for agents.

## Quick Start

```bash
pip install -e .
python -m polymarket_tools setup
python -m polymarket_tools scan --limit 10   # Optional: limit for faster first run
python -m polymarket_tools get_all_markets
```

## Commands

| Command | Description |
|---------|-------------|
| `setup` | Initialize SQLite database |
| `scan [--limit N]` | Run a single scan |
| `poll [--interval M] [--limit N]` | Run background polling loop |
| `get_all_markets [--limit N]` | List markets |
| `get_market_trends <market_id> [--limit N]` | Price/volume history |
| `get_category_markets <category> [--limit N]` | Markets by category |

## Configuration

- `POLYMARKET_DB_PATH`: Override database location (default: `./polymarket.db`)

## OpenClaw Integration

Copy or symlink `skills/polymarket-scraper/` to `~/.openclaw/workspace/skills/polymarket-scraper/` and refresh skills. See [skills/polymarket-scraper/SKILL.md](skills/polymarket-scraper/SKILL.md) for full documentation.
