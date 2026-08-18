"""The health check, and the two ways a streaming service lies about being fine.

**A crash-looping unit is not `failed`.** It is `activating`, forever, and any check that asks
systemd for a status sees nothing wrong. The aggregator restarted thirty-four times in a row
without a single alert, because nothing was watching what it produced.

**A live process is not a working one.** The aggregator writes its heartbeat on every flush cycle
whether or not a window closed, so a stalled consumer group leaves a perfectly fresh file with a
counter that never moves. Freshness alone would pass it.
"""

import json

import pytest

from bmp import health


@pytest.fixture
def beat(tmp_path):
    def write(name, written_at, counter):
        p = tmp_path / name
        p.write_text(f"{written_at:.0f} {counter}\n")
        return str(p)
    return write


# --- reading a heartbeat ----------------------------------------------------------------

def test_a_heartbeat_is_a_timestamp_and_a_counter(beat):
    assert health.read_heartbeat(beat("hb", 1_700_000_000, 42)) == (1_700_000_000.0, 42)


@pytest.mark.parametrize("body", ["", "not-a-number 5", "1700000000", "1700000000 5 extra"])
def test_an_unreadable_heartbeat_is_a_failure_not_an_exception(tmp_path, body):
    """The health check runs unattended on a timer. A crash there is a check that stopped
    checking, which is worse than the condition it was watching for."""
    p = tmp_path / "hb"
    p.write_text(body)
    assert health.read_heartbeat(str(p)) is None


def test_a_missing_heartbeat_reads_as_missing_rather_than_raising(tmp_path):
    assert health.read_heartbeat(str(tmp_path / "nothing")) is None


# --- freshness --------------------------------------------------------------------------

def test_a_fresh_heartbeat_passes(beat):
    now = 1_700_000_000.0
    check, counter = health.check_heartbeat("x", beat("hb", now - 5, 100), 120, None, now, False)
    assert check.ok and counter == 100


def test_a_stale_heartbeat_fails_and_says_by_how_much(beat):
    now = 1_700_000_000.0
    check, _ = health.check_heartbeat("x", beat("hb", now - 400, 100), 120, None, now, False)
    assert not check.ok
    assert "400s old" in check.detail


def test_a_missing_heartbeat_names_the_path_it_looked_for(tmp_path):
    """Whoever reads the alert at night should not have to go and find out where it looks."""
    path = str(tmp_path / "absent")
    check, _ = health.check_heartbeat("x", path, 120, None, 1.0, False)
    assert not check.ok and path in check.detail


# --- progress ---------------------------------------------------------------------------

def test_a_fresh_heartbeat_whose_counter_never_moves_is_a_failure(beat):
    """The regression test for a service that is alive and building nothing."""
    now = 1_700_000_000.0
    check, _ = health.check_heartbeat("agg", beat("hb", now - 5, 500), 180, 500, now, True)
    assert not check.ok
    assert "stalled" in check.detail


def test_a_counter_that_moved_passes_and_reports_the_delta(beat):
    now = 1_700_000_000.0
    check, _ = health.check_heartbeat("agg", beat("hb", now - 5, 530), 180, 500, now, True)
    assert check.ok and "+30" in check.detail


def test_the_first_run_has_nothing_to_compare_and_does_not_fail_for_it(beat):
    """A missing state file means a reboot, not an outage."""
    now = 1_700_000_000.0
    check, _ = health.check_heartbeat("agg", beat("hb", now - 5, 7), 180, None, now, True)
    assert check.ok


def test_the_receiver_is_not_held_to_a_moving_counter(beat, tmp_path, monkeypatch):
    """It rewrites the file only alongside a fresh timestamp, so freshness already implies
    progress — and requiring the counter to move would report a quiet market as an outage."""
    now = 1_700_000_000.0
    monkeypatch.setattr(health, "WS_HEARTBEAT", beat("ws", now - 5, 900))
    monkeypatch.setattr(health, "AGG_HEARTBEAT", beat("agg", now - 5, 61))

    report, _ = health.evaluate(now=now, state={"ws": 900, "agg": 60})

    assert report.ok, report.summary()


# --- the report -------------------------------------------------------------------------

def test_a_healthy_report_is_ok_and_a_degraded_one_is_not(beat, monkeypatch):
    now = 1_700_000_000.0
    monkeypatch.setattr(health, "WS_HEARTBEAT", beat("ws", now - 5, 900))
    monkeypatch.setattr(health, "AGG_HEARTBEAT", beat("agg", now - 900, 60))

    report, state = health.evaluate(now=now, state={})

    assert not report.ok
    assert "degraded" in report.summary()
    assert state["agg"] == 60


def test_the_summary_of_a_failure_names_only_what_failed(beat, monkeypatch):
    now = 1_700_000_000.0
    monkeypatch.setattr(health, "WS_HEARTBEAT", beat("ws", now - 5, 900))
    monkeypatch.setattr(health, "AGG_HEARTBEAT", beat("agg", now - 900, 60))

    summary = health.evaluate(now=now, state={})[0].summary()

    assert "aggregating" in summary
    assert "[ok]" not in summary


def test_state_survives_a_round_trip(tmp_path):
    path = str(tmp_path / "state.json")
    health.write_state({"ws": 1, "agg": 2}, path)
    assert health.read_state(path) == {"ws": 1, "agg": 2}


def test_corrupt_state_reads_as_empty_rather_than_raising(tmp_path):
    p = tmp_path / "state.json"
    p.write_text("{not json")
    assert health.read_state(str(p)) == {}


# --- alerting ---------------------------------------------------------------------------

def test_no_webhook_configured_is_not_an_error(monkeypatch):
    """The check has to be useful on a box with no outbound integration at all."""
    monkeypatch.delenv("BMP_ALERT_WEBHOOK", raising=False)
    assert health.alert("anything") is False


def test_an_unreachable_webhook_does_not_take_the_check_down(monkeypatch):
    """The alert is the messenger. Losing it must not lose the result it was carrying."""
    assert health.alert("anything", url="http://127.0.0.1:1/nowhere") is False


def test_the_payload_carries_both_field_names(monkeypatch):
    """Discord reads `content` and Slack reads `text`. Sending both means one URL works for
    either without asking which provider it is."""
    captured = {}

    class FakeResponse:
        def __enter__(self): return self
        def __exit__(self, *a): return False

    def fake_urlopen(request, timeout=None):
        captured["body"] = json.loads(request.data)
        return FakeResponse()

    monkeypatch.setattr(health.urllib.request, "urlopen", fake_urlopen)

    assert health.alert("stream down", url="https://example.invalid/hook") is True
    assert captured["body"] == {"content": "stream down", "text": "stream down"}
