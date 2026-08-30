"""Bounded end-to-end check of the topology described by docker-compose.yml.

Compose answers "which services run"; this answers "are they actually working".
Keeping the assertions here rather than in the CI workflow means the workflow
stays a caller and the success criteria stay reviewable code.
"""

import json
import logging
import subprocess
import sys
import time
import uuid
from collections.abc import Callable

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from pipeline import ensure_topic
from pipeline.config import KAFKA_SERVER

# Every check is bounded, so a broken topology fails rather than hangs.
CONSUME_TIMEOUT_MS = 30_000
PRODUCE_TIMEOUT_SECONDS = 30
COMMAND_TIMEOUT_SECONDS = 120

logger = logging.getLogger("smoke-test")

SMOKE_TOPIC = "smoke_test"

# The address the broker advertises to other containers, which is not the one
# this process uses.
INTERNAL_BOOTSTRAP = "kafka:29092"


def _consume_until(marker: str, timeout_ms: int = CONSUME_TIMEOUT_MS) -> dict | None:
    """Read the smoke topic from the host, returning the event carrying marker.

    Returns None rather than raising when the broker is unreachable: losing the
    broker between producing and consuming is a failed check, not a crash.
    """
    try:
        consumer = KafkaConsumer(
            SMOKE_TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            # A fresh group each run, so a previous run's committed offsets
            # cannot hide the message this run is looking for.
            group_id=f"smoke-{uuid.uuid4()}",
            auto_offset_reset="earliest",
            consumer_timeout_ms=timeout_ms,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )
    except (KafkaError, OSError) as error:
        logger.warning(f"smoke consumer could not connect: {error}")
        return None

    try:
        for message in consumer:
            if message.value.get("marker") == marker:
                return message.value
    except (KafkaError, OSError) as error:
        logger.warning(f"smoke consumer stopped reading: {error}")
        return None
    finally:
        consumer.close()
    return None


def host_listener_round_trips() -> tuple[bool, str]:
    """An event produced from the host comes back to a host consumer."""
    marker = str(uuid.uuid4())
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )
    except (KafkaError, OSError) as error:
        return False, f"could not connect to {KAFKA_SERVER}: {error}"

    try:
        # flush() waits for records to settle but does not re-raise each
        # future's delivery error. Keeping the future and calling get() turns a
        # wrong advertised listener into an immediate, named failure.
        producer.send(SMOKE_TOPIC, {"marker": marker, "origin": "host"}).get(
            timeout=PRODUCE_TIMEOUT_SECONDS
        )
    except (KafkaError, OSError) as error:
        return False, f"produce to {KAFKA_SERVER} failed: {error}"
    finally:
        producer.close()

    if _consume_until(marker) is None:
        return False, f"event produced to {KAFKA_SERVER} did not come back"
    return True, f"produced and consumed via {KAFKA_SERVER}"


def internal_listener_reaches_the_same_broker() -> tuple[bool, str]:
    """An event produced inside the network is readable from the host.

    This is the release's governing idea reduced to an assertion: two addresses,
    one broker. If the advertised listeners are wrong, this is what fails.
    """
    marker = str(uuid.uuid4())
    payload = json.dumps({"marker": marker, "origin": "container"})
    command = [
        "docker",
        "compose",
        "exec",
        "-T",
        "kafka",
        "/opt/kafka/bin/kafka-console-producer.sh",
        "--bootstrap-server",
        INTERNAL_BOOTSTRAP,
        "--topic",
        SMOKE_TOPIC,
    ]

    try:
        result = subprocess.run(
            command,
            check=False,
            input=f"{payload}\n",
            capture_output=True,
            text=True,
            timeout=COMMAND_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return False, f"producing via {INTERNAL_BOOTSTRAP} did not finish in time"
    except FileNotFoundError:
        return False, "docker is not available on PATH"

    if result.returncode != 0:
        return (
            False,
            f"producing via {INTERNAL_BOOTSTRAP} failed: {result.stderr.strip()}",
        )

    if _consume_until(marker) is None:
        return False, (
            f"event produced via {INTERNAL_BOOTSTRAP} was not readable at {KAFKA_SERVER}"
        )
    return True, f"{INTERNAL_BOOTSTRAP} and {KAFKA_SERVER} are the same broker"


CHECKS: list[tuple[str, Callable[[], tuple[bool, str]]]] = [
    ("host listener", host_listener_round_trips),
    ("internal listener", internal_listener_reaches_the_same_broker),
]


def main() -> int:
    started = time.monotonic()
    try:
        ensure_topic(SMOKE_TOPIC, 1, KAFKA_SERVER)
    except (KafkaError, OSError) as error:
        # No broker at all. Report it as a failed run rather than a traceback,
        # for the same reason every check is bounded.
        print(f"FAIL  setup: could not reach {KAFKA_SERVER}: {error}")
        print("\n1 of 1 checks failed", file=sys.stderr)
        return 1

    failures = 0
    for name, check in CHECKS:
        passed, detail = check()
        print(f"{'PASS' if passed else 'FAIL'}  {name}: {detail}")
        if not passed:
            failures += 1

    elapsed = time.monotonic() - started
    if failures:
        print(f"\n{failures} of {len(CHECKS)} checks failed", file=sys.stderr)
        return 1

    print(f"\nall {len(CHECKS)} checks passed in {elapsed:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
