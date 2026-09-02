import unittest

from src.watchdog import recent_healthy_run


NOW = 1_788_330_000.0
MAX_AGE = 11_700


def run(*, created_at: str, status: str, conclusion=None, event="schedule"):
    return {
        "id": 1,
        "created_at": created_at,
        "status": status,
        "conclusion": conclusion,
        "event": event,
    }


class WatchdogDecisionTests(unittest.TestCase):
    def test_recent_in_progress_run_is_healthy(self):
        item = run(created_at="2026-09-02T06:00:00Z", status="in_progress")
        self.assertIsNotNone(
            recent_healthy_run([item], now_epoch=NOW, max_start_age_seconds=MAX_AGE)
        )

    def test_recent_successful_run_is_healthy(self):
        item = run(created_at="2026-09-02T06:00:00Z", status="completed", conclusion="success")
        self.assertIsNotNone(
            recent_healthy_run([item], now_epoch=NOW, max_start_age_seconds=MAX_AGE)
        )

    def test_recent_failed_run_requires_recovery(self):
        item = run(created_at="2026-09-02T06:00:00Z", status="completed", conclusion="failure")
        self.assertIsNone(
            recent_healthy_run([item], now_epoch=NOW, max_start_age_seconds=MAX_AGE)
        )

    def test_old_in_progress_run_does_not_block_recovery(self):
        item = run(created_at="2026-09-02T02:00:00Z", status="in_progress")
        self.assertIsNone(
            recent_healthy_run([item], now_epoch=NOW, max_start_age_seconds=MAX_AGE)
        )

    def test_unrelated_event_is_ignored(self):
        item = run(created_at="2026-09-02T06:00:00Z", status="in_progress", event="pull_request")
        self.assertIsNone(
            recent_healthy_run([item], now_epoch=NOW, max_start_age_seconds=MAX_AGE)
        )


if __name__ == "__main__":
    unittest.main()
