from __future__ import annotations

import json
import os
import time
import urllib.request
from datetime import datetime
from typing import Any

WORKFLOW_FILE = "collect-market-data.yml"
DEFAULT_MAX_START_AGE_SECONDS = 3 * 3600 + 15 * 60
ALLOWED_EVENTS = {"schedule", "workflow_dispatch", "push"}
HEALTHY_ACTIVE_STATUSES = {"queued", "in_progress"}


def github_time_to_epoch(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def run_is_healthy(run: dict[str, Any]) -> bool:
    status = run.get("status")
    if status in HEALTHY_ACTIVE_STATUSES:
        return True
    return status == "completed" and run.get("conclusion") == "success"


def recent_healthy_run(
    runs: list[dict[str, Any]],
    *,
    now_epoch: float,
    max_start_age_seconds: int,
) -> dict[str, Any] | None:
    cutoff = now_epoch - max_start_age_seconds
    candidates = []
    for run in runs:
        if run.get("event") not in ALLOWED_EVENTS:
            continue
        created_at = run.get("created_at")
        if not created_at or not run_is_healthy(run):
            continue
        created_epoch = github_time_to_epoch(created_at)
        if created_epoch >= cutoff:
            candidates.append((created_epoch, run))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


def _request_json(method: str, url: str, token: str, body: dict[str, Any] | None = None) -> Any:
    payload = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=payload,
        method=method,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        raw = response.read()
        return json.loads(raw) if raw else None


def main() -> None:
    repository = os.environ["GITHUB_REPOSITORY"]
    token = os.environ["GH_TOKEN"]
    api_url = os.getenv("GITHUB_API_URL", "https://api.github.com").rstrip("/")
    ref = os.getenv("COLLECTOR_REF", "main")
    duration = os.getenv("COLLECTOR_DURATION_SECONDS", "13500")
    max_age = int(os.getenv("COLLECTOR_MAX_START_AGE_SECONDS", str(DEFAULT_MAX_START_AGE_SECONDS)))

    runs_url = f"{api_url}/repos/{repository}/actions/workflows/{WORKFLOW_FILE}/runs?per_page=30"
    response = _request_json("GET", runs_url, token)
    runs = response.get("workflow_runs", []) if isinstance(response, dict) else []
    healthy = recent_healthy_run(runs, now_epoch=time.time(), max_start_age_seconds=max_age)

    if healthy is not None:
        print(
            "collector is fresh:",
            {
                "id": healthy.get("id"),
                "event": healthy.get("event"),
                "status": healthy.get("status"),
                "conclusion": healthy.get("conclusion"),
                "created_at": healthy.get("created_at"),
            },
        )
        return

    dispatch_url = f"{api_url}/repos/{repository}/actions/workflows/{WORKFLOW_FILE}/dispatches"
    _request_json(
        "POST",
        dispatch_url,
        token,
        {"ref": ref, "inputs": {"duration_seconds": duration}},
    )
    print(f"collector stale or unhealthy; dispatched {duration}s run on {ref}")


if __name__ == "__main__":
    main()
