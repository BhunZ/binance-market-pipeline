"""Kafka connection settings, in one place because three programs need the same ones.

Aiven authenticates with a client certificate rather than a username and password, so the
connection needs three files and no secret in the environment. They live outside the repository
and are read by path.
"""

from __future__ import annotations

import os
from pathlib import Path

TOPIC_TRADES = "trades.raw"

#: Free-tier ceiling, measured rather than assumed: asking for 3 returns
#: `POLICY_VIOLATION: maximum 2 partitions per user topic allowed`. Two is plenty at ~82
#: messages a second, but anyone rebuilding this will hit the same wall.
TOPIC_PARTITIONS = 2
TOPIC_REPLICATION = 2

CERT_DIR = Path(os.getenv("KAFKA_CERT_DIR", Path.home() / "certs"))


def is_configured() -> bool:
    """True when a broker and all three certificate files are present.

    Everything that uses Kafka checks this first and degrades rather than crashing, so the
    repository stays runnable by someone who has no cluster.
    """
    if not os.getenv("KAFKA_BOOTSTRAP"):
        return False
    return all((CERT_DIR / f).exists() for f in ("ca.pem", "service.cert", "service.key"))


def missing() -> list[str]:
    out = []
    if not os.getenv("KAFKA_BOOTSTRAP"):
        out.append("KAFKA_BOOTSTRAP")
    out += [str(CERT_DIR / f) for f in ("ca.pem", "service.cert", "service.key")
            if not (CERT_DIR / f).exists()]
    return out


def base_conf() -> dict:
    return {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
        "security.protocol": "SSL",
        "ssl.ca.location": str(CERT_DIR / "ca.pem"),
        "ssl.certificate.location": str(CERT_DIR / "service.cert"),
        "ssl.key.location": str(CERT_DIR / "service.key"),
    }


def producer_conf() -> dict:
    return {
        **base_conf(),
        # Batch for 20 ms before sending. At ~82 messages a second that is a handful per request
        # instead of one request per trade, which is the difference between a steady connection
        # and a chatty one on a shared free-tier broker.
        "linger.ms": 20,
        "compression.type": "lz4",
        # Wait for both replicas. The whole reason the raw feed goes through a broker is so it
        # survives a mistake in the aggregation; acking on one replica would give that up for
        # latency this pipeline does not need.
        "acks": "all",
        "enable.idempotence": True,
        # A queue that fills means the broker is unreachable. Blocking is right: dropping trades
        # silently is the one failure this design exists to prevent.
        "queue.buffering.max.messages": 200_000,
    }


def consumer_conf(group_id: str) -> dict:
    return {
        **base_conf(),
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        # Offsets are committed by hand, after a window is safely on object storage. Automatic
        # commits would advance the offset for trades that were read and then lost when the
        # process died before writing them.
        "enable.auto.commit": False,
        "session.timeout.ms": 45_000,
        "max.poll.interval.ms": 300_000,
    }
