# polymarket_bot

Research collector for Polymarket 5-minute crypto Up/Down markets.

## First-stage scope

Symbols: `BTC`, `ETH`, `SOL`, `XRP`.

The collector stores four market-data sources:

1. **Polymarket CLOB** — Top-5 order-book snapshots, best bid/ask changes, trades, and tick-size changes for both Up/Down tokens.
2. **Polymarket RTDS Binance prices** — Polymarket's Binance-derived crypto reference-price stream.
3. **Polymarket RTDS Chainlink prices** — Polymarket's Chainlink crypto reference-price stream.
4. **Binance direct WebSocket** — best-price changes from `bookTicker` plus all `aggTrade` events for the same symbols, using Binance's public market-data-only endpoint.

The collector intentionally normalizes high-volume feeds instead of storing every redundant raw update. This keeps the data small enough for the first GitHub Actions research stage while retaining the fields needed for 250 ms–10 s reversal studies.

## Timestamp fields

Every stored record contains:

- `source_ts_ms`: source/exchange timestamp when the source supplies one.
- `recv_wall_ns`: local wall-clock receive timestamp recorded by the collector.
- `recv_mono_ns`: local monotonic timestamp for reliable within-run intervals.

`clock_probe` queries Polymarket and Binance server clocks every minute and records request RTT plus an estimated clock offset. Polymarket WebSocket heartbeat RTT is also recorded.

These fields let later analysis separate source-time price movement from collector/network delay. GitHub-hosted runners are suitable for the first research stage, but sub-100 ms conclusions are not execution-grade evidence because runner region and network path are uncontrolled.

## GitHub Actions collection

`.github/workflows/collect-market-data.yml` runs every three hours. A scheduled run collects for 3h45m, so adjacent runs intentionally overlap by roughly 45 minutes. Duplicate overlap is expected and should be de-duplicated during analysis.

A push to `main` runs a 120-second smoke collection. The workflow validates that all required live sources produced records before it reports success.

Each run uploads gzip-compressed JSONL files as a GitHub Actions artifact with one-day retention to limit artifact-storage usage. Manual runs are supported from the Actions tab.

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

The files contain normalized append-only research records. No trading code is included in this stage.
