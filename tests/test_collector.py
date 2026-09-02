import unittest

from src.collector import CompactMarketState, extract_source_ts_ms, parse_event_tokens


class CollectorParsingTests(unittest.TestCase):
    def test_parse_event_tokens_handles_stringified_arrays(self):
        event = {
            "id": "123",
            "slug": "btc-updown-5m-1788325200",
            "startDate": "2026-09-02T05:00:00Z",
            "endDate": "2026-09-02T05:05:00Z",
            "markets": [
                {
                    "conditionId": "0xabc",
                    "clobTokenIds": '["11","22"]',
                    "outcomes": '["Up","Down"]',
                }
            ],
        }
        tokens = parse_event_tokens("BTC", 1788325200, event)
        self.assertEqual([token.token_id for token in tokens], ["11", "22"])
        self.assertEqual([token.outcome for token in tokens], ["Up", "Down"])
        self.assertEqual(tokens[0].condition_id, "0xabc")

    def test_extract_polymarket_timestamp(self):
        self.assertEqual(
            extract_source_ts_ms("polymarket_clob", {"timestamp": "1788325200123"}),
            1788325200123,
        )

    def test_extract_rtds_payload_timestamp(self):
        event = {"topic": "crypto_prices", "payload": {"timestamp": 1788325200456}}
        self.assertEqual(extract_source_ts_ms("polymarket_rtds_binance", event), 1788325200456)

    def test_extract_rtds_snapshot_falls_back_to_envelope_timestamp(self):
        event = {"topic": "crypto_prices", "timestamp": 1788325200789, "payload": {"data": []}}
        self.assertEqual(extract_source_ts_ms("polymarket_rtds_binance", event), 1788325200789)

    def test_extract_binance_prefers_transaction_time(self):
        message = {"stream": "btcusdt@aggTrade", "data": {"E": 2000, "T": 1997}}
        self.assertEqual(extract_source_ts_ms("binance_direct", message), 1997)


class CompactMarketStateTests(unittest.TestCase):
    def test_clob_keeps_top_five_and_deduplicates_best_quote(self):
        state = CompactMarketState()
        book = {
            "event_type": "book",
            "market": "0xmarket",
            "asset_id": "1",
            "bids": [{"price": "0.40", "size": "2"}, {"price": "0.50", "size": "1"}],
            "asks": [{"price": "0.70", "size": "2"}, {"price": "0.60", "size": "1"}],
        }
        compact = state.compact_clob(book)
        self.assertEqual(compact[0]["bids"][0]["price"], "0.50")
        self.assertEqual(compact[0]["asks"][0]["price"], "0.60")

        unchanged = {
            "event_type": "price_change",
            "market": "0xmarket",
            "price_changes": [{"asset_id": "1", "best_bid": "0.50", "best_ask": "0.60"}],
        }
        self.assertEqual(state.compact_clob(unchanged), [])

        changed = {
            "event_type": "price_change",
            "market": "0xmarket",
            "price_changes": [{"asset_id": "1", "best_bid": "0.51", "best_ask": "0.60"}],
        }
        self.assertEqual(state.compact_clob(changed)[0]["best_bid"], "0.51")

    def test_binance_book_ticker_deduplicates_price_only(self):
        state = CompactMarketState()
        message = {
            "stream": "btcusdt@bookTicker",
            "data": {"s": "BTCUSDT", "b": "100", "B": "2", "a": "101", "A": "3", "u": 1},
        }
        self.assertIsNotNone(state.compact_binance(message))
        message["data"]["B"] = "9"
        self.assertIsNone(state.compact_binance(message))
        message["data"]["b"] = "100.1"
        self.assertEqual(state.compact_binance(message)["best_bid"], "100.1")


if __name__ == "__main__":
    unittest.main()
