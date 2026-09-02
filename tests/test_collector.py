import unittest

from src.collector import extract_source_ts_ms, parse_event_tokens


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

    def test_extract_binance_prefers_transaction_time(self):
        message = {"stream": "btcusdt@aggTrade", "data": {"E": 2000, "T": 1997}}
        self.assertEqual(extract_source_ts_ms("binance_direct", message), 1997)


if __name__ == "__main__":
    unittest.main()
