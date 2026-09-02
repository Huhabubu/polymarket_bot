from __future__ import annotations

import argparse
import asyncio
import gzip
import json
import logging
import os
import signal
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiohttp
import websockets

LOG = logging.getLogger("collector")
SYMBOLS = ("BTC", "ETH", "SOL", "XRP")
MARKET_SECONDS = 300
GAMMA = "https://gamma-api.polymarket.com/events/slug/{slug}"
CLOB_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
RTDS_WS = "wss://ws-live-data.polymarket.com"
BINANCE_WS = "wss://data-stream.binance.vision:443/stream?streams="
BINANCE_TIME = "https://data-api.binance.vision/api/v3/time"


def aligned_epoch(ts: float | None = None) -> int:
    ts = time.time() if ts is None else ts
    return int(ts // MARKET_SECONDS) * MARKET_SECONDS


def event_slug(symbol: str, epoch: int) -> str:
    return f"{symbol.lower()}-updown-5m-{epoch}"


def _jsonish(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


@dataclass(frozen=True)
class TokenMeta:
    symbol: str
    epoch_seconds: int
    slug: str
    event_id: str | None
    condition_id: str | None
    token_id: str
    outcome: str
    start_date: str | None
    end_date: str | None

    def as_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


def parse_event_tokens(symbol: str, epoch: int, event: dict[str, Any]) -> list[TokenMeta]:
    markets = event.get("markets") or []
    if not markets:
        return []
    market = markets[0]
    ids = _jsonish(market.get("clobTokenIds")) or []
    outcomes = _jsonish(market.get("outcomes")) or []
    if not isinstance(ids, list) or not isinstance(outcomes, list) or len(ids) != len(outcomes):
        return []
    return [
        TokenMeta(
            symbol=symbol,
            epoch_seconds=epoch,
            slug=event.get("slug") or market.get("slug") or event_slug(symbol, epoch),
            event_id=str(event.get("id")) if event.get("id") is not None else None,
            condition_id=event.get("conditionId") or market.get("conditionId"),
            token_id=str(token_id),
            outcome=str(outcome),
            start_date=event.get("startDate") or market.get("startDate"),
            end_date=event.get("endDate") or market.get("endDate"),
        )
        for token_id, outcome in zip(ids, outcomes)
    ]


def extract_source_ts_ms(source: str, message: Any) -> int | None:
    if not isinstance(message, dict):
        return None
    if source == "polymarket_clob":
        value = message.get("timestamp")
    elif source.startswith("polymarket_rtds"):
        payload = message.get("payload")
        payload_ts = payload.get("timestamp") if isinstance(payload, dict) else None
        value = payload_ts if payload_ts is not None else message.get("timestamp")
    elif source == "binance_direct":
        payload = message.get("data") if isinstance(message.get("data"), dict) else message
        value = payload.get("T") or payload.get("E")
    else:
        value = None
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _top_five(levels: list[dict[str, Any]], reverse: bool) -> list[dict[str, Any]]:
    try:
        return sorted(levels, key=lambda x: float(x["price"]), reverse=reverse)[:5]
    except (KeyError, TypeError, ValueError):
        return levels[:5]


class CompactMarketState:
    def __init__(self) -> None:
        self.polymarket_best: dict[str, tuple[str | None, str | None]] = {}
        self.binance_best: dict[str, tuple[str | None, str | None]] = {}

    def compact_clob(self, message: dict[str, Any]) -> list[dict[str, Any]]:
        event_type = message.get("event_type") or message.get("type")
        market = message.get("market")
        if event_type == "book":
            asset_id = str(message.get("asset_id"))
            bids = _top_five(message.get("bids") or [], reverse=True)
            asks = _top_five(message.get("asks") or [], reverse=False)
            best = (
                bids[0].get("price") if bids else None,
                asks[0].get("price") if asks else None,
            )
            self.polymarket_best[asset_id] = best
            return [
                {
                    "event_type": "book_top5",
                    "market": market,
                    "asset_id": asset_id,
                    "bids": bids,
                    "asks": asks,
                    "tick_size": message.get("tick_size"),
                    "last_trade_price": message.get("last_trade_price"),
                }
            ]
        if event_type == "price_change":
            out: list[dict[str, Any]] = []
            for change in message.get("price_changes") or []:
                asset_id = str(change.get("asset_id"))
                best = (change.get("best_bid"), change.get("best_ask"))
                if best == self.polymarket_best.get(asset_id):
                    continue
                self.polymarket_best[asset_id] = best
                out.append(
                    {
                        "event_type": "best_bid_ask",
                        "market": market,
                        "asset_id": asset_id,
                        "best_bid": best[0],
                        "best_ask": best[1],
                    }
                )
            return out
        if event_type == "last_trade_price":
            return [
                {
                    "event_type": "last_trade_price",
                    "market": market,
                    "asset_id": message.get("asset_id"),
                    "price": message.get("price"),
                    "size": message.get("size"),
                    "side": message.get("side"),
                    "fee_rate_bps": message.get("fee_rate_bps"),
                    "transaction_hash": message.get("transaction_hash"),
                }
            ]
        if event_type == "tick_size_change":
            return [
                {
                    "event_type": "tick_size_change",
                    "market": market,
                    "asset_id": message.get("asset_id"),
                    "old_tick_size": message.get("old_tick_size"),
                    "new_tick_size": message.get("new_tick_size"),
                }
            ]
        return []

    def compact_binance(self, message: dict[str, Any]) -> dict[str, Any] | None:
        data = message.get("data") if isinstance(message.get("data"), dict) else message
        stream = message.get("stream")
        event_type = data.get("e")
        if event_type == "aggTrade" or (stream and stream.endswith("@aggTrade")):
            return {
                "event_type": "aggTrade",
                "symbol": data.get("s"),
                "price": data.get("p"),
                "quantity": data.get("q"),
                "trade_id": data.get("a"),
                "buyer_is_maker": data.get("m"),
                "event_ts_ms": data.get("E"),
                "trade_ts_ms": data.get("T"),
            }
        if stream and stream.endswith("@bookTicker"):
            symbol = str(data.get("s") or stream.split("@", 1)[0]).upper()
            best = (data.get("b"), data.get("a"))
            if best == self.binance_best.get(symbol):
                return None
            self.binance_best[symbol] = best
            return {
                "event_type": "bookTicker",
                "symbol": symbol,
                "best_bid": best[0],
                "best_ask": best[1],
                "best_bid_qty": data.get("B"),
                "best_ask_qty": data.get("A"),
                "update_id": data.get("u"),
            }
        if event_type == "serverShutdown":
            return {"event_type": "serverShutdown", "event_ts_ms": data.get("E")}
        return None


class Recorder:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.q: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue(maxsize=200_000)
        self.bucket: int | None = None
        self.fp = None

    async def put(
        self,
        source: str,
        message: Any,
        *,
        source_ts_ms: int | None = None,
        kind: str = "data",
        recv_wall_ns: int | None = None,
        recv_mono_ns: int | None = None,
    ) -> None:
        wall = time.time_ns() if recv_wall_ns is None else recv_wall_ns
        mono = time.monotonic_ns() if recv_mono_ns is None else recv_mono_ns
        await self.q.put(
            {
                "schema_version": 2,
                "kind": kind,
                "source": source,
                "source_ts_ms": source_ts_ms,
                "recv_wall_ns": wall,
                "recv_mono_ns": mono,
                "github_run_id": os.getenv("GITHUB_RUN_ID"),
                "github_run_attempt": os.getenv("GITHUB_RUN_ATTEMPT"),
                "runner_name": os.getenv("RUNNER_NAME"),
                "runner_os": os.getenv("RUNNER_OS"),
                "runner_arch": os.getenv("RUNNER_ARCH"),
                "payload": message,
            }
        )

    def _rotate(self, wall_ns: int) -> None:
        bucket = wall_ns // 1_000_000_000 // 900
        if bucket == self.bucket and self.fp:
            return
        if self.fp:
            self.fp.close()
        start = bucket * 900
        day = time.strftime("%Y%m%d", time.gmtime(start))
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(start))
        folder = self.root / day
        folder.mkdir(parents=True, exist_ok=True)
        self.fp = gzip.open(
            folder / f"market-data-{stamp}.jsonl.gz",
            "at",
            encoding="utf-8",
            compresslevel=6,
        )
        self.bucket = bucket

    async def run(self) -> None:
        while True:
            row = await self.q.get()
            try:
                if row is None:
                    break
                self._rotate(row["recv_wall_ns"])
                self.fp.write(json.dumps(row, separators=(",", ":"), ensure_ascii=False) + "\n")
            finally:
                self.q.task_done()
        if self.fp:
            self.fp.close()

    async def close(self) -> None:
        await self.q.join()
        await self.q.put(None)


async def discover(
    session: aiohttp.ClientSession,
    symbols: tuple[str, ...],
    token_cache: dict[str, list[TokenMeta]],
    desired_subscriptions: asyncio.Queue[set[str]],
    recorder: Recorder,
    stop: asyncio.Event,
) -> None:
    last_desired: set[str] = set()
    while not stop.is_set():
        current = aligned_epoch()
        desired: set[str] = set()
        for epoch in (current, current + MARKET_SECONDS):
            for symbol in symbols:
                slug = event_slug(symbol, epoch)
                tokens = token_cache.get(slug)
                if tokens is None:
                    try:
                        async with session.get(GAMMA.format(slug=slug), timeout=5) as response:
                            if response.status == 404:
                                continue
                            response.raise_for_status()
                            event = await response.json()
                    except (aiohttp.ClientError, asyncio.TimeoutError):
                        continue
                    tokens = parse_event_tokens(symbol, epoch, event)
                    if not tokens:
                        continue
                    token_cache[slug] = tokens
                    await recorder.put(
                        "market_discovery",
                        {"slug": slug, "tokens": [token.as_dict() for token in tokens]},
                        source_ts_ms=epoch * 1000,
                        kind="metadata",
                    )
                desired.update(token.token_id for token in tokens)
        if desired and desired != last_desired:
            await desired_subscriptions.put(desired)
            last_desired = desired
        try:
            await asyncio.wait_for(stop.wait(), 10)
        except asyncio.TimeoutError:
            pass


async def text_heartbeat(
    ws: Any,
    seconds: float,
    state: dict[str, int],
    stop: asyncio.Event,
) -> None:
    while not stop.is_set():
        state["sent"] = time.monotonic_ns()
        await ws.send("PING")
        try:
            await asyncio.wait_for(stop.wait(), seconds)
        except asyncio.TimeoutError:
            pass


async def clob_loop(
    desired_subscriptions: asyncio.Queue[set[str]],
    market_state: CompactMarketState,
    recorder: Recorder,
    stop: asyncio.Event,
) -> None:
    backoff = 1
    desired: set[str] = set()
    while not stop.is_set():
        if not desired:
            desired = await desired_subscriptions.get()
            desired_subscriptions.task_done()
        while not desired_subscriptions.empty():
            desired = desired_subscriptions.get_nowait()
            desired_subscriptions.task_done()
        try:
            async with websockets.connect(CLOB_WS, ping_interval=None, max_queue=100_000) as ws:
                subscribed = set(desired)
                await ws.send(json.dumps({"assets_ids": sorted(subscribed), "type": "market"}))
                state: dict[str, int] = {}
                heartbeat = asyncio.create_task(text_heartbeat(ws, 10, state, stop))

                async def update_subscriptions() -> None:
                    nonlocal desired, subscribed
                    while not stop.is_set():
                        desired = await desired_subscriptions.get()
                        try:
                            add = sorted(desired - subscribed)
                            remove = sorted(subscribed - desired)
                            if add:
                                await ws.send(json.dumps({"operation": "subscribe", "assets_ids": add}))
                            if remove:
                                await ws.send(json.dumps({"operation": "unsubscribe", "assets_ids": remove}))
                            subscribed = set(desired)
                        finally:
                            desired_subscriptions.task_done()

                sub_task = asyncio.create_task(update_subscriptions())
                backoff = 1
                try:
                    async for raw in ws:
                        wall, mono = time.time_ns(), time.monotonic_ns()
                        if raw == "PONG":
                            sent = state.get("sent")
                            await recorder.put(
                                "diagnostic",
                                {
                                    "stream": "polymarket_clob",
                                    "event": "heartbeat_rtt",
                                    "rtt_ms": (mono - sent) / 1e6 if sent else None,
                                },
                                kind="diagnostic",
                                recv_wall_ns=wall,
                                recv_mono_ns=mono,
                            )
                            continue
                        try:
                            decoded = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        for msg in decoded if isinstance(decoded, list) else [decoded]:
                            if not isinstance(msg, dict):
                                continue
                            source_ts = extract_source_ts_ms("polymarket_clob", msg)
                            for compact in market_state.compact_clob(msg):
                                await recorder.put(
                                    "polymarket_clob",
                                    compact,
                                    source_ts_ms=source_ts,
                                    recv_wall_ns=wall,
                                    recv_mono_ns=mono,
                                )
                finally:
                    heartbeat.cancel()
                    sub_task.cancel()
                    await asyncio.gather(heartbeat, sub_task, return_exceptions=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await recorder.put(
                "diagnostic",
                {"stream": "polymarket_clob", "event": "disconnect", "error": repr(exc)},
                kind="diagnostic",
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 20)


def _rtds_source(message: dict[str, Any]) -> str:
    topic = message.get("topic")
    if topic in {"crypto_prices_chainlink", "prices.crypto.chainlink"}:
        return "polymarket_rtds_chainlink"
    if topic in {"crypto_prices", "prices.crypto.binance"}:
        return "polymarket_rtds_binance"
    return "polymarket_rtds_other"


def _wanted_rtds(message: dict[str, Any], symbols: tuple[str, ...]) -> bool:
    payload = message.get("payload")
    if not isinstance(payload, dict):
        return True
    symbol = payload.get("symbol")
    if not symbol:
        return True
    allowed = {f"{item.lower()}usdt" for item in symbols} | {f"{item.lower()}/usd" for item in symbols}
    return str(symbol).lower() in allowed


async def rtds_loop(symbols: tuple[str, ...], recorder: Recorder, stop: asyncio.Event) -> None:
    request = {
        "action": "subscribe",
        "subscriptions": [
            {"topic": "crypto_prices", "type": "update"},
            {"topic": "crypto_prices_chainlink", "type": "update"},
        ],
    }
    backoff = 1
    while not stop.is_set():
        try:
            async with websockets.connect(RTDS_WS, ping_interval=None, max_queue=100_000) as ws:
                await ws.send(json.dumps(request))
                state: dict[str, int] = {}
                heartbeat = asyncio.create_task(text_heartbeat(ws, 5, state, stop))
                backoff = 1
                try:
                    async for raw in ws:
                        wall, mono = time.time_ns(), time.monotonic_ns()
                        if raw == "PONG":
                            sent = state.get("sent")
                            await recorder.put(
                                "diagnostic",
                                {
                                    "stream": "polymarket_rtds",
                                    "event": "heartbeat_rtt",
                                    "rtt_ms": (mono - sent) / 1e6 if sent else None,
                                },
                                kind="diagnostic",
                                recv_wall_ns=wall,
                                recv_mono_ns=mono,
                            )
                            continue
                        try:
                            decoded = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        for msg in decoded if isinstance(decoded, list) else [decoded]:
                            if not isinstance(msg, dict) or not _wanted_rtds(msg, symbols):
                                continue
                            source = _rtds_source(msg)
                            await recorder.put(
                                source,
                                msg,
                                source_ts_ms=extract_source_ts_ms(source, msg),
                                recv_wall_ns=wall,
                                recv_mono_ns=mono,
                            )
                finally:
                    heartbeat.cancel()
                    await asyncio.gather(heartbeat, return_exceptions=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await recorder.put(
                "diagnostic",
                {"stream": "polymarket_rtds", "event": "disconnect", "error": repr(exc)},
                kind="diagnostic",
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 20)


async def binance_loop(
    symbols: tuple[str, ...],
    market_state: CompactMarketState,
    recorder: Recorder,
    stop: asyncio.Event,
) -> None:
    streams = [
        stream
        for symbol in symbols
        for stream in (
            f"{symbol.lower()}usdt@bookTicker",
            f"{symbol.lower()}usdt@aggTrade",
        )
    ]
    url = BINANCE_WS + "/".join(streams)
    backoff = 1
    while not stop.is_set():
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_queue=100_000) as ws:
                backoff = 1
                async for raw in ws:
                    wall, mono = time.time_ns(), time.monotonic_ns()
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    compact = market_state.compact_binance(msg)
                    if compact is None:
                        continue
                    kind = "diagnostic" if compact.get("event_type") == "serverShutdown" else "data"
                    await recorder.put(
                        "binance_direct",
                        compact,
                        source_ts_ms=extract_source_ts_ms("binance_direct", msg),
                        kind=kind,
                        recv_wall_ns=wall,
                        recv_mono_ns=mono,
                    )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await recorder.put(
                "diagnostic",
                {"stream": "binance_direct", "event": "disconnect", "error": repr(exc)},
                kind="diagnostic",
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 20)


async def clock_probe(
    session: aiohttp.ClientSession,
    recorder: Recorder,
    stop: asyncio.Event,
) -> None:
    endpoints = (
        ("polymarket", "https://clob.polymarket.com/time"),
        ("binance", BINANCE_TIME),
    )
    while not stop.is_set():
        for name, url in endpoints:
            w0, m0 = time.time_ns(), time.monotonic_ns()
            try:
                async with session.get(url, timeout=5) as response:
                    response.raise_for_status()
                    body = await response.json(content_type=None)
                w1, m1 = time.time_ns(), time.monotonic_ns()
                server_ms = float(body) * 1000 if name == "polymarket" else float(body["serverTime"])
                await recorder.put(
                    "clock_probe",
                    {
                        "server": name,
                        "server_ts_ms": server_ms,
                        "rtt_ms": (m1 - m0) / 1e6,
                        "estimated_server_minus_local_ms": server_ms - ((w0 + w1) / 2 / 1e6),
                    },
                    source_ts_ms=int(server_ms),
                    kind="diagnostic",
                    recv_wall_ns=w1,
                    recv_mono_ns=m1,
                )
            except Exception as exc:
                await recorder.put(
                    "clock_probe",
                    {"server": name, "error": repr(exc)},
                    kind="diagnostic",
                )
        try:
            await asyncio.wait_for(stop.wait(), 60)
        except asyncio.TimeoutError:
            pass


async def main_async(symbols: tuple[str, ...], output: Path, duration: int) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    recorder = Recorder(output)
    writer = asyncio.create_task(recorder.run())
    desired_subscriptions: asyncio.Queue[set[str]] = asyncio.Queue(maxsize=10)
    token_cache: dict[str, list[TokenMeta]] = {}
    market_state = CompactMarketState()

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
        tasks = [
            asyncio.create_task(
                discover(
                    session,
                    symbols,
                    token_cache,
                    desired_subscriptions,
                    recorder,
                    stop,
                )
            ),
            asyncio.create_task(clob_loop(desired_subscriptions, market_state, recorder, stop)),
            asyncio.create_task(rtds_loop(symbols, recorder, stop)),
            asyncio.create_task(binance_loop(symbols, market_state, recorder, stop)),
            asyncio.create_task(clock_probe(session, recorder, stop)),
        ]
        await recorder.put(
            "diagnostic",
            {"event": "collector_started", "symbols": symbols, "duration_seconds": duration},
            kind="diagnostic",
        )
        if duration > 0:
            try:
                await asyncio.wait_for(stop.wait(), duration)
            except asyncio.TimeoutError:
                stop.set()
        else:
            await stop.wait()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    await recorder.put("diagnostic", {"event": "collector_stopped"}, kind="diagnostic")
    await recorder.close()
    await writer


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", default=os.getenv("SYMBOLS", ",".join(SYMBOLS)))
    parser.add_argument("--output-dir", default=os.getenv("OUTPUT_DIR", "data"))
    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=int(os.getenv("COLLECT_DURATION_SECONDS", "0")),
    )
    args = parser.parse_args()
    symbols = tuple(item.strip().upper() for item in args.symbols.split(",") if item.strip())
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(main_async(symbols, Path(args.output_dir), args.duration_seconds))


if __name__ == "__main__":
    main()
