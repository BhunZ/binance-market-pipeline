"""Retry classification, and why the distinction matters more than the retrying.

A 429 means slow down and try again. A 400 means the request was wrong and will be wrong every
time. Retrying the second kind burns the rate budget the first kind needs, and turns an error
that could have been read in one line into one that takes four attempts and a backoff to
surface.

The failure this guards against is not a crash. It is an exhausted quota that looks like an
empty result — a task that returns "no candles today" when what actually happened is that
Binance stopped answering.
"""

import pytest
import requests

from bmp import binance


class _Response:
    def __init__(self, status: int, payload=None, headers=None, text=""):
        self.status_code = status
        self._payload = payload
        self.headers = headers or {}
        self.text = text

    def json(self):
        return self._payload


@pytest.fixture(autouse=True)
def _no_sleeping(monkeypatch):
    """Tests must not actually wait out a backoff."""
    monkeypatch.setattr("time.sleep", lambda *_: None)


def _responses(monkeypatch, *sequence):
    calls = {"n": 0}

    def fake_get(url, params=None, timeout=None):
        r = sequence[min(calls["n"], len(sequence) - 1)]
        calls["n"] += 1
        if isinstance(r, Exception):
            raise r
        return r

    monkeypatch.setattr(requests, "get", fake_get)
    return calls


def test_a_successful_call_returns_the_payload(monkeypatch):
    _responses(monkeypatch, _Response(200, {"serverTime": 1}))
    assert binance.get("/api/v3/time") == {"serverTime": 1}


def test_a_bad_request_fails_immediately(monkeypatch):
    """The body of a 400 is the only thing that explains a malformed request. Retrying it three
    times delays that explanation and tells you nothing new."""
    calls = _responses(monkeypatch, _Response(400, text="Invalid symbol."))

    with pytest.raises(binance.BinanceError, match="Invalid symbol"):
        binance.get("/api/v3/klines", {"symbol": "NOPE"})

    assert calls["n"] == 1, "a 400 was retried"


def test_rate_limiting_is_retried_and_then_succeeds(monkeypatch):
    calls = _responses(monkeypatch, _Response(429, headers={"Retry-After": "1"}),
                       _Response(200, ["ok"]))
    assert binance.get("/api/v3/klines") == ["ok"]
    assert calls["n"] == 2


def test_a_server_error_is_retried(monkeypatch):
    _responses(monkeypatch, _Response(503), _Response(200, ["ok"]))
    assert binance.get("/api/v3/klines") == ["ok"]


def test_a_connection_error_is_retried(monkeypatch):
    _responses(monkeypatch, requests.ConnectionError("dns"), _Response(200, ["ok"]))
    assert binance.get("/api/v3/klines") == ["ok"]


def test_persistent_failure_raises_rather_than_returning_empty(monkeypatch):
    """The one that matters. If exhaustion returned `[]`, a throttled run would write an empty
    partition and the completeness gate would blame the market instead of the quota."""
    _responses(monkeypatch, _Response(429, headers={"Retry-After": "0"}))

    with pytest.raises(binance.BinanceError):
        binance.get("/api/v3/klines")


def test_the_retry_after_header_is_honoured(monkeypatch):
    """Guessing a shorter wait than the server asked for is how a soft throttle becomes a ban."""
    waited = []
    monkeypatch.setattr("time.sleep", lambda s: waited.append(s))
    _responses(monkeypatch, _Response(429, headers={"Retry-After": "7"}), _Response(200, ["ok"]))

    binance.get("/api/v3/klines")

    assert 7 in waited


def test_pacing_is_shared_across_callers(monkeypatch):
    """The rate limit belongs to the source address, not to whichever object holds the client.
    Two callers in one process must draw on one budget."""
    assert binance._pace.__module__ == "bmp.binance"
    assert "_last_call" in binance.__dict__
