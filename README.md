# polymarket_bot

Research collector for Polymarket 5-minute crypto Up/Down markets.

## First-stage scope

Symbols: `BTC`, `ETH`, `SOL`, `XRP`.

The collector stores four market-data sources:

1. **Polymarket CLOB** — order-book snapshots, price-level changes, last-trade updates and other public market-channel messages for both Up/Down tokens.
2. **Polymarket RTDS Binance prices** — Polymarket's Binance-derived crypto price stream.
3. **Polymarket RTDS Chainlink prices** — Polymarket's Chainlink crypto price stream.
4. **Binance direct WebSocket** — `bookTicker` and `aggTrade` for the same symbols.

It also stores clock/connection diagnostics used to evaluate latency quality.

## Timestamp fields

Every record contains:

- `source_ts_ms`: source/exchange timestamp when one is available.
- `recv_wall_ns`: local wall-clock timestamp recorded by the collector.
- `recv_mono_ns`: local monotonic timestamp for reliable within-run intervals.
- `payload`: raw source message plus the WebSocket receive timestamp captured immediately after `recv()`.

`clock_probe` records query Polymarket and Binance server clocks every minute and save request RTT plus an estimated clock offset. Polymarket WebSocket heartbeat RTT is also recorded.

These fields let later analysis separate source-time price movement from collector/network delay. GitHub-hosted runners are suitable for the first research stage, but sub-100 ms conclusions should not be treated as execution-grade evidence because runner location and network path are not controlled.

## GitHub Actions collection

`.github/workflows/collect-market-data.yml` runs every three hours. A scheduled run collects for 3h45m, so adjacent runs intentionally overlap by roughly 45 minutes. This favors continuity over uniqueness; later analysis should de-duplicate overlap.

Each run uploads gzip-compressed JSONL files as a GitHub Actions artifact and keeps them for 7 days.

Manual runs are also supported from the Actions tab.

## Local test

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m unittest discover -s tests -v
python -m src.collector --duration-seconds 300
```

## Data format

Files rotate every 15 minutes:

```text
data/YYYYMMDD/market-data-YYYYMMDDTHHMMSSZ.jsonl.gz
```

The files are append-only raw research data. No trading code is included in this stage.
