---
name: polymarket-scraper
description: Maintains a local SQLite replica of Polymarket state using SQLAlchemy. Use when the user asks about Polymarket markets, prices, trends, closed/open markets, or prediction market data. The agent queries the local database—data is kept up to date by a background scanner.
---

# Polymarket Scraper Skill

This skill maintains a local, up-to-date SQLite replica of Polymarket CLOB market state. Use it when the user asks about Polymarket prediction markets, prices, trends, categories, closed markets, or open markets.

## Background Task

The background polling loop runs automatically (e.g., via cron, systemd, or OpenClaw's exec with `background: true`). **The agent does not need to manually trigger data fetches.** Query the database using the tools below.

## Available Tools

Invoke these via the shell (e.g., `python -m polymarket_tools <command> [args]`). Run from the project root (`market-scarper/`) or ensure `polymarket_tools` is on `PYTHONPATH`.

### get_all_markets

List all available markets.

```bash
python -m polymarket_tools get_all_markets [--limit N]
```

- `--limit`: Max markets to return (default 50).

**When to use:** User asks for "all Polymarket markets," "list markets," or "what markets are available."

---

### get_market_trends

Return price/volume/midpoint/spread history for a specific market.

```bash
python -m polymarket_tools get_market_trends <market_id> [--limit N]
```

- `market_id`: Polymarket condition_id (e.g., `0x5eed579ff6763914d78a966c83473ba2485ac8910d0a0914eef6d9fcb33085de`).
- `--limit`: Max change records (default 50).

**When to use:** User asks about price trends, historical prices, or "how has market X changed over time."

---

### get_category_markets

List markets filtered by category (from Polymarket tags).

```bash
python -m polymarket_tools get_category_markets <category_name> [--limit N]
```

- `category_name`: Value of `market_category` (e.g., `Politics`, `Sports`, `All`).
- `--limit`: Max markets to return (default 50).

**When to use:** User asks for "Politics markets," "sports prediction markets," or "markets in category X."

---

### get_closed_markets

List markets that are closed or resolved.

```bash
python -m polymarket_tools get_closed_markets [--limit N]
```

- `--limit`: Max markets to return (default 50).

**When to use:** User asks for "closed markets," "resolved markets," or "markets that have finished."

---

### get_open_markets

List markets that are open and accepting orders.

```bash
python -m polymarket_tools get_open_markets [--limit N]
```

- `--limit`: Max markets to return (default 50).

**When to use:** User asks for "open markets," "active markets," or "markets I can trade on."

---

### get_market

Return full market details including enriched fields (volume, liquidity, description, tags, extra_info, etc.). These fields are only populated when the market was scanned via `scan --market`.

```bash
python -m polymarket_tools get_market <market_id>
```

- `market_id`: Polymarket condition_id (e.g., `0xb48621f7eba07b0a3eeabc6afb09ae42490239903997b9d412b0f69aeb040c8b`).

**When to use:** User asks for full details about a specific market, including volume, liquidity, description, resolution source, or other enriched metadata. List commands (get_all_markets, get_open_markets, etc.) do not return these fields.

---

### query_market_field

Return a single field value for a market (e.g., question, status, slug).

```bash
python -m polymarket_tools query_market_field <market_id> <field_name>
```

- `market_id`: Polymarket condition_id.
- `field_name`: One of `market_id`, `clob_token_ids`, `status`, `question`, `slug`, `yes_token_id`, `no_token_id`, `last_trade_price`, `minimum_tick_size`, `neg_risk`, `change_id`, `outcome`, `market_category`, or enriched fields (`volume`, `liquidity`, `start_date`, `category`, `tags`, `market_type`, `description`, `extra_info`) when the market was scanned with `scan --market`.

**When to use:** User needs one specific piece of information about a market (e.g., "what is the question for market X?" or "what is the status of market Y?").

---

## Setup and Installation

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
   Or with pyproject:
   ```bash
   pip install -e .
   ```
   Required: `requests`, `sqlalchemy`.

2. **Initialize the database (once):**
   ```bash
   python -m polymarket_tools setup
   ```

3. **Start the background scanner** (choose one):
   - Long-lived process: `python -m polymarket_tools poll --interval 5` (polls every 5 minutes).
   - Cron: `*/5 * * * * cd /path/to/market-scarper && python -m polymarket_tools scan && python -m polymarket_tools sync_closed_markets`
   - OpenClaw schedule: use `ScheduleConfig` to run `python -m polymarket_tools scan` and `python -m polymarket_tools sync_closed_markets` periodically.

### scan

Run a single scan of Polymarket markets and persist to the database. **By default scans only active markets** (via Gamma API). Use `--all` to include closed/archived markets (via CLOB API). Use `--market` to scan a single market and populate enriched fields (volume, liquidity, description, tags, etc.).

```bash
python -m polymarket_tools scan [--limit N] [--all] [--batch-only] [--market ID_OR_SLUG]
```

- `--limit`: Max markets to scan (default: all).
- `--all`: Include closed/archived markets. If omitted, only active tradable markets are scanned.
- `--batch-only`: Use only batch API calls (POST /books) — no per-market orderbook or last-trade-price fetches. Faster and fewer API calls; tokens missing from the batch response will have no BBO/spread.
- `--market`: Scan a single market by condition_id or slug. Populates enriched columns (volume, liquidity, start_date, category, tags, market_type, description, extra_info). Mutually exclusive with bulk scan.

**When to use:** Cron jobs, one-off syncs, or OpenClaw scheduled runs. Use `--market` to enrich specific markets with full Gamma API data. Use `--all` only when you need historical/closed market data.

### sync_closed_markets

Sync markets that have closed since the last scan. Queries the Gamma API for recently closed markets (closed=true, active=false) with pagination, then updates any DB records that are still marked `active` to `closed` and sets the resolved outcome.

```bash
python -m polymarket_tools sync_closed_markets [--limit N]
```

- `--limit`: Max closed markets to fetch from API (default: 500). Uses pagination to retrieve the most recent closed markets.

**When to use:** Run after `scan` (or in the same cron/poll cycle) to keep the local DB in sync when markets close between scans. Ensures `get_closed_markets` and `get_open_markets` reflect accurate status.

### poll

Run a background loop that periodically scans markets.

```bash
python -m polymarket_tools poll [--interval M] [--limit N] [--all]
```

- `--interval`: Poll interval in minutes (default: 5).
- `--limit`: Max markets per scan (default: all).
- `--all`: Include closed/archived markets per scan.
- `--batch-only`: Use only batch API calls per scan (no per-market orderbook fetches).

**When to use:** Long-lived background process for continuous updates.

4. **Mount the skill** (if using OpenClaw workspace):
   - Copy this skill directory to `~/.openclaw/workspace/skills/polymarket-scraper/`
   - Or symlink: `ln -s /path/to/market-scarper/skills/polymarket-scraper ~/.openclaw/workspace/skills/polymarket-scraper`
   - Refresh skills or restart the OpenClaw gateway.

## Database

- Uses **SQLAlchemy** with SQLite and Declarative Base ORM.
- Tables: `markets` (current state), `market_change` (historical log).
- Default path: `./polymarket.db`. Override with `POLYMARKET_DB_PATH`:
  ```bash
  export POLYMARKET_DB_PATH=/path/to/polymarket.db
  python -m polymarket_tools get_all_markets
  ```

## Example Agent Workflow

1. User: "What Polymarket markets are open?"
   - Run: `python -m polymarket_tools get_open_markets --limit 20`

2. User: "Show me the price history for market 0xabc123..."
   - Run: `python -m polymarket_tools get_market_trends 0xabc123... --limit 30`

3. User: "What is the question for market 0xdef456...?"
   - Run: `python -m polymarket_tools query_market_field 0xdef456... question`

4. User: "List closed politics markets."
   - Run: `python -m polymarket_tools get_closed_markets --limit 50`, then filter by category Politics, or run `python -m polymarket_tools get_category_markets Politics` and filter client-side for closed status.
