---
name: polymarket-scraper
description: Maintains a local SQLite replica of Polymarket state using SQLAlchemy. Use when the user asks about Polymarket markets, prices, trends, closed/open markets, keyword search, or prediction market data. The agent queries the local database—data is kept up to date by a background scanner.
---

# Polymarket Scraper Skill

This skill maintains a local, up-to-date SQLite replica of Polymarket CLOB market state. Use it when the user asks about Polymarket prediction markets, prices, trends, categories, closed markets, or open markets.

## Background Task

The background polling loop runs automatically (e.g., via cron, systemd, or OpenClaw's exec with `background: true`). **The agent does not need to manually trigger data fetches.** Query the database using the tools below.

## Available Tools

Invoke these via the shell (e.g., `python -m polymarket_tools <command> [args]`). Run from the project root (`market-scarper/`) or ensure `polymarket_tools` is on `PYTHONPATH`.

**Response format:** All market list commands (get_all_markets, get_category_markets, search_markets, get_open_markets, get_closed_markets) and get_market return each market with a nested `latest_change` object: `{ "datetime", "yes_price", "no_price", "volume", "liquidity", "last_trade_price", "midpoint", "spread" }` from the most recent `market_change` row. Volume and liquidity are stored in market_change and populated when the market was scanned via `scan --market` or sample_refresh.

### get_all_markets

List all available markets. Each market includes `latest_change` (datetime, yes_price, no_price, volume, liquidity, last_trade_price, midpoint, spread).

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

List markets filtered by category (from Polymarket tags). Supports multiple categories. Each market includes `latest_change`.

```bash
python -m polymarket_tools get_category_markets <category> [category ...] [--limit N]
```

- `category`: One or more values of `market_category` (e.g., `Politics`, `Sports`, `All`). Markets matching any category are returned.
- `--limit`: Max markets to return (default 50).

**When to use:** User asks for "Politics markets," "sports prediction markets," "markets in Politics or Sports," or "markets in category X."

---

### search_markets

Search markets by keyword in question and description. Case-insensitive. By default, any keyword matches (OR). Use `--and` to require all keywords (AND). Each market includes `latest_change`.

```bash
python -m polymarket_tools search_markets <keyword> [keyword ...] [--limit N] [--all] [--and]
```

- `keyword`: One or more search terms (e.g., `oil`, `price`). Default: any keyword matches (OR).
- `--limit`: Max markets to return (default 50).
- `--all`: Return all matching markets (no limit). Use when you don't know the DB size.
- `--and`: Require all keywords to match (AND). Omit for OR (any keyword matches).

**When to use:** User asks for "oil markets," "markets about X," "search for markets with Y," or "find markets mentioning Z." Use `--and` when the user wants markets containing all terms (e.g., "oil AND price"). Note: `description` is only populated for markets scanned via `scan --market`; most markets are searched via `question` only.

---

### get_closed_markets

List markets that are closed or resolved. Each market includes `latest_change`.

```bash
python -m polymarket_tools get_closed_markets [--limit N]
```

- `--limit`: Max markets to return (default 50).

**When to use:** User asks for "closed markets," "resolved markets," or "markets that have finished."

---

### get_open_markets

List markets that are open and accepting orders. Each market includes `latest_change`.

```bash
python -m polymarket_tools get_open_markets [--limit N]
```

- `--limit`: Max markets to return (default 50).

**When to use:** User asks for "open markets," "active markets," or "markets I can trade on."

---

### get_market

Return full market details including enriched fields (description, tags, extra_info, etc.) and `latest_change`. Volume and liquidity are in `latest_change` (from market_change); enriched metadata is populated when the market was scanned via `scan --market`.

```bash
python -m polymarket_tools get_market <market_id>
```

- `market_id`: Polymarket condition_id (e.g., `0xb48621f7eba07b0a3eeabc6afb09ae42490239903997b9d412b0f69aeb040c8b`).

**When to use:** User asks for full details about a specific market, including volume, liquidity, description, resolution source, or other enriched metadata. List commands (get_all_markets, get_open_markets, etc.) do not return enriched fields but do include `latest_change`.

---

### query_market_field

Return a single field value for a market (e.g., question, status, slug).

```bash
python -m polymarket_tools query_market_field <market_id> <field_name>
```

- `market_id`: Polymarket condition_id.
- `field_name`: One of `market_id`, `clob_token_ids`, `status`, `question`, `slug`, `yes_token_id`, `no_token_id`, `minimum_tick_size`, `neg_risk`, `change_id`, `outcome`, `market_category`, enriched fields (`start_date`, `category`, `tags`, `market_type`, `description`, `extra_info`), or state fields (`volume`, `liquidity`, `last_trade_price`) which are read from the latest market_change row.

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
   - Gentle refresh: `python -m polymarket_tools sample_refresh` (200 markets/min, batch orderbooks only).
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

### sample_refresh

Periodically refresh a sample of open markets from the DB, ordered by staleness (oldest-refreshed first). Each cycle uses one batch orderbook fetch (`POST /books`) plus per-market Gamma fetches to populate enriched columns (`volume`, `liquidity`, `description`, `tags`, `extra_info`, etc.). Run an initial `scan --batch-only` first to populate the DB.

```bash
python -m polymarket_tools sample_refresh [--limit N] [--interval S]
```

- `--limit`: Markets to refresh per cycle (default: 200). Picks oldest-refreshed first (change_id ASC). Higher values increase per-cycle Gamma API load.
- `--interval`: Seconds between cycles (default: 60).

**When to use:** Long-lived process to gradually fill the DB with up-to-date prices and enriched metadata without a full bulk rescan. Tune `--limit`/`--interval` conservatively (for example, `--limit 100 --interval 60`) when API budget is tight. Each cycle refreshes the stalest open markets; over repeated cycles all open markets are revisited.

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
- Tables: `markets` (current state, identity metadata), `market_change` (historical log of price/volume/liquidity snapshots). State columns (volume, liquidity, last_trade_price) live in market_change; join via change_id for current values.
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

5. User: "Find oil markets" or "Search for markets about oil."
   - Run: `python -m polymarket_tools search_markets oil --limit 20`
