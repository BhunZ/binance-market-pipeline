"""Whether the streaming branch is doing its job, judged by what it produced.

**Outputs, not process state.** `systemctl is-active` answers a question nobody needs: a service
can be running and producing nothing, and that is the failure worth catching. A crash-looping unit
reports `activating` rather than `failed` and never trips a status check at all. So this reads the
two heartbeats and asks whether the counters inside them moved.

**Counters, not timestamps.** A timestamp alone proves a process is alive. The counter proves it is
working. The receiver writes its heartbeat every few hundred messages, so a stale file already
means the stream is quiet — but the aggregator writes on every flush cycle whether or not a window
closed, and there a fresh file with an unchanged count is exactly the shape of a stalled consumer
group.

Comparing counters needs the previous reading, which is kept in a small state file. A missing
state file is not a failure: the first run after a reboot simply has nothing to compare against and
says so.

Alerting is a webhook if one is configured and the log otherwise, and the exit code is non-zero on
any failure so a systemd timer records it either way. The payload carries both `content` and `text`
so the same URL works for Discord and Slack without a per-provider branch.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

WS_HEARTBEAT = os.getenv("BMP_WS_HEARTBEAT", "/tmp/bmp_ws_heartbeat")
AGG_HEARTBEAT = os.getenv("BMP_AGG_HEARTBEAT", "/tmp/bmp_agg_heartbeat")
STATE_PATH = os.getenv("BMP_HEALTH_STATE", "/tmp/bmp_health_state.json")

#: The receiver rewrites its heartbeat every few hundred messages, which at the observed rate is
#: every handful of seconds. Two minutes is far past any plausible gap and still well inside the
#: window where a dead stream is worth knowing about.
WS_STALE_S = 120.0

#: The aggregator writes on its flush cycle, so this only has to clear a few of those.
AGG_STALE_S = 180.0


@dataclass
class Check:
    name: str
    ok: bool
    detail: str

    def line(self) -> str:
        return f"[{'ok' if self.ok else 'FAIL'}] {self.name}: {self.detail}"


@dataclass
class Report:
    checks: list[Check] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return all(c.ok for c in self.checks)

    def summary(self) -> str:
        failed = [c for c in self.checks if not c.ok]
        if not failed:
            return "streaming branch healthy: " + "; ".join(c.detail for c in self.checks)
        return "streaming branch degraded — " + "; ".join(c.line() for c in failed)


def read_heartbeat(path: str) -> tuple[float, int] | None:
    """`<unix seconds> <counter>`, or None if it cannot be read as that."""
    try:
        with open(path) as fh:
            written, counter = fh.read().split()
        return float(written), int(counter)
    except (OSError, ValueError):
        return None


def read_state(path: str = STATE_PATH) -> dict:
    try:
        with open(path) as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return {}


def write_state(state: dict, path: str = STATE_PATH) -> None:
    try:
        with open(path, "w") as fh:
            json.dump(state, fh)
    except OSError:
        logger.debug("could not write health state", exc_info=True)


def check_heartbeat(name: str, path: str, stale_s: float, previous: int | None,
                    now: float, require_progress: bool) -> tuple[Check, int | None]:
    """One heartbeat, against its own freshness and its own previous counter."""
    reading = read_heartbeat(path)
    if reading is None:
        return Check(name, False, f"no readable heartbeat at {path}"), None

    written, counter = reading
    age = now - written

    if age > stale_s:
        return Check(name, False, f"heartbeat {age:.0f}s old, limit {stale_s:.0f}s"), counter

    if require_progress and previous is not None and counter <= previous:
        return Check(name, False,
                     f"alive but stalled — counter still {counter} since the last check"), counter

    moved = "" if previous is None else f", +{counter - previous} since the last check"
    return Check(name, True, f"fresh ({age:.0f}s), counter {counter}{moved}"), counter


def evaluate(now: float | None = None, state: dict | None = None) -> tuple[Report, dict]:
    now = now if now is not None else time.time()
    state = read_state() if state is None else state
    report = Report()

    # The receiver's counter is only written alongside a fresh timestamp, so freshness already
    # implies progress and checking the counter again would fail on a genuinely quiet market.
    check, ws_counter = check_heartbeat("stream receiving", WS_HEARTBEAT, WS_STALE_S,
                                        state.get("ws"), now, require_progress=False)
    report.checks.append(check)

    check, agg_counter = check_heartbeat("stream aggregating", AGG_HEARTBEAT, AGG_STALE_S,
                                         state.get("agg"), now, require_progress=True)
    report.checks.append(check)

    return report, {"ws": ws_counter, "agg": agg_counter, "at": now}


def alert(message: str, url: str | None = None) -> bool:
    """Post to the configured webhook. Returns whether anything was sent."""
    url = url if url is not None else os.getenv("BMP_ALERT_WEBHOOK", "")
    if not url:
        return False

    payload = json.dumps({"content": message, "text": message}).encode()
    request = urllib.request.Request(url, data=payload,
                                     headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=10):
            return True
    except (urllib.error.URLError, OSError):
        # An unreachable webhook must not mask the health result it was carrying.
        logger.warning("could not reach the alert webhook", exc_info=True)
        return False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Health of the streaming branch")
    parser.add_argument("--quiet", action="store_true",
                        help="print only on failure, for a timer that should stay silent")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    report, state = evaluate()
    write_state(state)

    if report.ok:
        if not args.quiet:
            for check in report.checks:
                print(check.line())
        return 0

    for check in report.checks:
        print(check.line(), file=sys.stderr)
    alert(report.summary())
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
