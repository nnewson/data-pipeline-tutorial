"""Bounded end-to-end check of the topology described by docker-compose.yml.

Compose answers "which services run"; this answers "are they actually working".
Keeping the assertions here rather than in the CI workflow means the workflow
stays a caller and the success criteria stay reviewable code.
"""

import json
import logging
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from websockets.exceptions import InvalidStatus, WebSocketException
from websockets.sync.client import connect as websocket_connect

from pipeline import (
    api,
    cassandra_store,
    coordination,
    ensure_topic,
    flink_cluster,
    get_partition,
    job_keeper,
    jobs_queue,
    redis_store,
    wait_for_topic,
    windows,
    windows_writer,
)
from pipeline.config import KAFKA_PARTITIONS, KAFKA_SERVER
from pipeline.flink_job import Settings as FlinkSettings
from pipeline.schema import SCHEMA_FILE
from pipeline.topology_runner import (
    COMMAND_TIMEOUT_SECONDS,
    Topology,
)

# Every check is bounded, so a broken topology fails rather than hangs.
# The Procfile runs three coordinators competing for one leadership.
COORDINATORS = 3

# Long enough for a watch to fire and four workers to re-register.
LIVE_CONFIG_TIMEOUT_SECONDS = 30

CONSUME_TIMEOUT_MS = 30_000
PRODUCE_TIMEOUT_SECONDS = 30

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

    # Ask for the topic's partitions before iterating. That forces the client's
    # own metadata fetch to complete before the group-join assignor runs, which
    # is what otherwise logs "No partition metadata" on a freshly created topic.
    # Broker-side existence is not the same as this client having seen it.
    try:
        consumer.partitions_for_topic(SMOKE_TOPIC)
    except (KafkaError, OSError) as error:
        logger.warning(f"smoke consumer could not fetch metadata: {error}")
        consumer.close()
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


def routing_uses_every_partition() -> tuple[bool, str]:
    """Each username lands on the partition the routing rule names for it.

    Asserted against a real broker rather than against get_partition alone,
    because the rule is only useful if the broker agrees with it.
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )
    except (KafkaError, OSError) as error:
        return False, f"could not connect to {KAFKA_SERVER}: {error}"

    # One username per partition, chosen by walking the alphabet.
    names: dict[int, str] = {}
    for letter in "abcdefghijklmnopqrstuvwxyz":
        names.setdefault(get_partition(letter, KAFKA_PARTITIONS), f"{letter}-user")

    try:
        for expected, username in sorted(names.items()):
            metadata = producer.send(
                SMOKE_TOPIC,
                {"marker": "routing", "user_id": username},
                partition=get_partition(username, KAFKA_PARTITIONS),
            ).get(timeout=PRODUCE_TIMEOUT_SECONDS)
            if metadata.partition != expected:
                return False, (
                    f"{username} landed on partition {metadata.partition}, "
                    f"expected {expected}"
                )
    except (KafkaError, OSError) as error:
        return False, f"routed produce failed: {error}"
    finally:
        producer.close()

    if len(names) != KAFKA_PARTITIONS:
        return False, (
            f"routing rule only reaches {len(names)} of {KAFKA_PARTITIONS} partitions"
        )
    return True, f"all {KAFKA_PARTITIONS} partitions addressed by the routing rule"


TOPOLOGY_SETTLE_SECONDS = 60
TOPOLOGY_PROGRESS_SECONDS = 10


def _delete_topic(name: str) -> None:
    """Remove a per-run topic so repeated smoke runs do not accumulate them."""
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_SERVER)
    except (KafkaError, OSError) as error:
        logger.warning(f"could not connect to delete {name}: {error}")
        return
    try:
        admin.delete_topics([name])
    except (KafkaError, OSError) as error:
        logger.warning(f"could not delete {name}: {error}")
    finally:
        admin.close()


def _group_command(group: str, extra: list[str]) -> tuple[str | None, str]:
    """Run kafka-consumer-groups.sh, returning stdout or a reason it failed.

    kafka-python 3 does not expose consumer-group description on its admin
    client, so this goes through Kafka's own CLI.
    """
    command = [
        "docker",
        "compose",
        "exec",
        "-T",
        "kafka",
        "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--describe",
        "--group",
        group,
        *extra,
    ]
    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=COMMAND_TIMEOUT_SECONDS,
        )
    except FileNotFoundError:
        return None, "docker is not available on PATH"
    except subprocess.TimeoutExpired:
        return None, f"describing group {group} timed out"

    if result.returncode != 0:
        return None, f"describing group {group} failed: {result.stderr.strip()}"
    return result.stdout, ""


def _group_members(group: str) -> tuple[set[str] | None, str]:
    """Every member of the group, including ones holding no partition.

    Plain --describe lists assignments, so an idle extra consumer would be
    invisible. --members --verbose lists the members themselves.
    """
    output, error = _group_command(group, ["--members", "--verbose"])
    if output is None:
        return None, error

    members = set()
    for line in output.splitlines():
        fields = line.split()
        if len(fields) >= 2 and fields[0] == group:
            members.add(fields[1])
    return members, ""


def _group_offsets(group: str) -> tuple[dict[int, int] | None, str]:
    """Committed offset per partition for the group."""
    output, error = _group_command(group, [])
    if output is None:
        return None, error

    offsets: dict[int, int] = {}
    for line in output.splitlines():
        fields = line.split()
        if len(fields) < 4 or fields[0] != group:
            continue
        try:
            offsets[int(fields[2])] = int(fields[3])
        except ValueError:
            continue
    return offsets, ""


def _clear_redis_prefix(prefix: str) -> None:
    """Remove a run's Redis keys. Best effort: never fail the check on cleanup."""
    try:
        client = redis_store.connect()
    except (OSError, redis_store.redis.RedisError) as error:
        logger.warning(f"could not connect to clear {prefix}: {error}")
        return
    try:
        redis_store.clear(client, prefix)
    except redis_store.redis.RedisError as error:
        logger.warning(f"could not clear {prefix}: {error}")
    finally:
        client.close()


def _redis_state(prefix: str) -> tuple[dict[str, int] | None, dict[str, str], str]:
    """Counters and last-page values under a prefix, or why they are unavailable."""
    try:
        client = redis_store.connect()
    except (OSError, redis_store.redis.RedisError) as error:
        return None, {}, f"could not connect to Redis: {error}"
    try:
        return (
            redis_store.page_counts(client, prefix),
            redis_store.last_pages(client, prefix),
            "",
        )
    except redis_store.redis.RedisError as error:
        return None, {}, f"reading Redis failed: {error}"
    finally:
        client.close()


def _create_keyspace(keyspace: str) -> str:
    """Give the run a keyspace of its own. Returns an error string, or empty.

    The same schema file the reader applies, pointed at a different keyspace —
    so the check exercises the real `create-schema` path rather than a
    hand-written table definition that could drift from it.
    """
    try:
        cluster, session = cassandra_store.connect()
    except Exception as error:  # noqa: BLE001 - reported, not swallowed
        return f"could not connect to Cassandra: {error}"
    try:
        cassandra_store.apply_schema(session, keyspace, SCHEMA_FILE.read_text())
    except Exception as error:  # noqa: BLE001 - reported, not swallowed
        return f"could not create keyspace {keyspace}: {error}"
    finally:
        cluster.shutdown()
    return ""


def _drop_keyspace(keyspace: str) -> None:
    """Remove a run's keyspace. Best effort: never fail the check on cleanup."""
    try:
        cluster, session = cassandra_store.connect()
    except Exception as error:  # noqa: BLE001 - cleanup is best effort
        logger.warning(f"could not connect to drop {keyspace}: {error}")
        return
    try:
        cassandra_store.drop_keyspace(session, keyspace)
    except Exception as error:  # noqa: BLE001 - cleanup is best effort
        logger.warning(f"could not drop {keyspace}: {error}")
    finally:
        cluster.shutdown()


def _cassandra_rows(keyspace: str) -> tuple[list | None, str]:
    """Rows stored under the run's keyspace, or why they are unavailable."""
    try:
        cluster, session = cassandra_store.connect(keyspace=keyspace)
    except Exception as error:  # noqa: BLE001 - reported, not swallowed
        return None, f"could not read keyspace {keyspace}: {error}"
    try:
        rows = list(
            session.execute(
                f"SELECT user_id, event_time, event_id, page "
                f"FROM {cassandra_store.TABLE} LIMIT 5"
            )
        )
    except Exception as error:  # noqa: BLE001 - reported, not swallowed
        return None, f"reading keyspace {keyspace} failed: {error}"
    finally:
        cluster.shutdown()
    return rows, ""


def _delete_queue(queue: str) -> None:
    """Remove a run's queue. Best effort: never fail the check on cleanup."""
    try:
        connection = jobs_queue.connect()
    except Exception as error:  # noqa: BLE001 - cleanup is best effort
        logger.warning(f"could not connect to delete {queue}: {error}")
        return
    try:
        connection.channel().queue_delete(queue=queue)
    except Exception as error:  # noqa: BLE001 - cleanup is best effort
        logger.warning(f"could not delete {queue}: {error}")
    finally:
        jobs_queue.close_quietly(connection)


def _queue_consumers(queue: str) -> tuple[int | None, str]:
    """How many workers are consuming the queue.

    Shares `jobs_queue.queue_state`, so the inspection cannot drift from the one
    `uv run jobs` uses — including its passive declare, which asks about the
    queue rather than creating it. Declaring here would hide a broken
    declaration path in the publisher or the workers.
    """
    try:
        _waiting, consumers = jobs_queue.queue_state(queue)
    except Exception as error:  # noqa: BLE001 - reported, not swallowed
        return None, f"could not inspect queue {queue}: {error}"
    return consumers, ""


def _job_state(prefix: str) -> tuple[int | None, dict[str, int], str]:
    """Completed executions recorded by the workers.

    Queue depth cannot answer this: acknowledged messages are gone, so the
    workers record completions in Redis instead.
    """
    try:
        client = redis_store.connect()
    except (OSError, redis_store.redis.RedisError) as error:
        return None, {}, f"could not connect to Redis: {error}"
    try:
        completed, runs = redis_store.job_summary(client, prefix)
    except redis_store.redis.RedisError as error:
        return None, {}, f"reading job state failed: {error}"
    finally:
        client.close()
    return completed, runs, ""


def _leader_state(root: str) -> tuple[dict | None, str]:
    """The acknowledged leader, or why it cannot be read."""
    try:
        client = coordination.connect()
    except Exception as error:  # noqa: BLE001 - reported, not swallowed
        return None, f"could not connect to ZooKeeper: {error}"
    try:
        return coordination.read_leader(client, coordination.Paths(root=root)), ""
    except Exception as error:  # noqa: BLE001 - reported, not swallowed
        return None, f"reading leadership failed: {error}"
    finally:
        client.stop()
        client.close()


def _coordination_state(root: str) -> tuple[dict | None, str]:
    """Leader, contenders, registrations and snapshot, in one connection."""
    paths = coordination.Paths(root=root)
    try:
        client = coordination.connect()
    except Exception as error:  # noqa: BLE001 - reported, not swallowed
        return None, f"could not connect to ZooKeeper: {error}"
    try:
        snapshot, version = coordination.read_snapshot(client, paths)
        return {
            "leader": coordination.read_leader(client, paths),
            "contenders": len(client.get_children(paths.election))
            if client.exists(paths.election)
            else 0,
            "workers": coordination.registrations(client, paths, "worker"),
            "consumers": coordination.registrations(client, paths, "consumer"),
            "coordinators": coordination.registrations(client, paths, "coordinator"),
            "snapshot": snapshot,
            "snapshot_version": version,
        }, ""
    except Exception as error:  # noqa: BLE001 - reported, not swallowed
        return None, f"reading coordination state failed: {error}"
    finally:
        client.stop()
        client.close()


def _delete_zookeeper_root(root: str) -> None:
    """Remove a run's subtree. Best effort: never fail the check on cleanup."""
    try:
        client = coordination.connect()
    except Exception as error:  # noqa: BLE001 - cleanup is best effort
        logger.warning(f"could not connect to delete {root}: {error}")
        return
    try:
        if client.exists(root):
            client.delete(root, recursive=True)
    except Exception as error:  # noqa: BLE001 - cleanup is best effort
        logger.warning(f"could not delete {root}: {error}")
    finally:
        client.stop()
        client.close()


@dataclass(frozen=True)
class Sentinels:
    """Known values the API must read back exactly.

    The API check tests the read path only; the other predicates already prove
    the writes. Sentinels rather than whatever the producer happens to have
    written: two reads of a moving value disagree, which is the trap removed
    from this test's output once already.
    """

    user: str
    page: str
    count: int
    event_id: str

    @classmethod
    def for_run(cls, run_id: str) -> "Sentinels":
        return cls(
            user=f"sentinel-{run_id}",
            page=f"/sentinel-{run_id}",
            count=7,
            event_id=f"sentinel-event-{run_id}",
        )


def _free_port() -> int:
    """A port nothing is bound to at this moment.

    Released before the API binds it, so something else could take it in
    between. A small window, accepted here and said rather than hidden.
    """
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def _write_sentinels(prefix: str, keyspace: str, sentinels: Sentinels) -> str:
    """Put the known values in place. Returns an error string, or empty."""
    try:
        client = redis_store.connect()
    except (OSError, redis_store.redis.RedisError) as error:
        return f"could not connect to Redis for sentinels: {error}"
    try:
        client.set(redis_store.page_count_key(sentinels.page, prefix), sentinels.count)
        client.set(redis_store.last_page_key(sentinels.user, prefix), sentinels.page)
    except redis_store.redis.RedisError as error:
        return f"could not write Redis sentinels: {error}"
    finally:
        client.close()

    try:
        cluster, session = cassandra_store.connect(keyspace=keyspace)
    except Exception as error:  # noqa: BLE001 - reported, not swallowed
        return f"could not connect to Cassandra for sentinels: {error}"
    try:
        cassandra_store.record_pageview(
            session,
            cassandra_store.prepare_insert(session),
            {
                "user_id": sentinels.user,
                "timestamp": time.time(),
                "event_id": sentinels.event_id,
                "page": sentinels.page,
            },
        )
    except Exception as error:  # noqa: BLE001 - reported, not swallowed
        return f"could not write the Cassandra sentinel: {error}"
    finally:
        cluster.shutdown()
    return ""


API_ROUTES = {
    "/health",
    "/counts/pages",
    "/users/{user_id}/last-page",
    "/users/{user_id}/events",
    "/cluster",
    "/windows",
}


def _api_get(port: int, path: str) -> tuple[int | None, object, str]:
    """Status and parsed body, or None and why the API could not be reached."""
    try:
        with urllib.request.urlopen(  # noqa: S310 - a fixed localhost URL
            f"http://127.0.0.1:{port}{path}", timeout=5
        ) as response:
            return response.status, json.loads(response.read()), ""
    except urllib.error.HTTPError as error:
        # A 404 or a 422 is an answer, and some checks want exactly that.
        return error.code, json.loads(error.read() or b"null"), ""
    except (urllib.error.URLError, OSError, ValueError) as error:
        return None, None, f"API on port {port} not answering: {error}"


def _api_serves_the_stores(
    port: int, sentinels: Sentinels, first_snapshot: dict
) -> tuple[bool, str]:
    """The API reads back each store's sentinel, and states each contract."""
    status, body, error = _api_get(port, "/health")
    if status != 200:
        return False, error or f"/health answered {status}"

    status, body, error = _api_get(
        port, f"/counts/pages?page={urllib.parse.quote(sentinels.page)}"
    )
    counts = body.get("counts", {}) if isinstance(body, dict) else {}
    if status != 200 or counts.get(sentinels.page) != sentinels.count:
        return False, error or (
            f"/counts/pages answered {status} with {counts.get(sentinels.page)!r} "
            f"for {sentinels.page}, wanted {sentinels.count}"
        )

    status, body, error = _api_get(port, f"/users/{sentinels.user}/last-page")
    page = body.get("page") if isinstance(body, dict) else None
    if status != 200 or page != sentinels.page:
        return False, error or f"last-page answered {status} with {page!r}"

    status, _, error = _api_get(port, f"/users/nobody-{sentinels.user}/last-page")
    if status != 404:
        return False, error or f"an unknown user's last-page answered {status}"

    status, body, error = _api_get(port, f"/users/{sentinels.user}/events")
    events = body.get("events", []) if isinstance(body, dict) else []
    ours = [event for event in events if event.get("event_id") == sentinels.event_id]
    if status != 200 or not ours or not ours[0].get("written_at"):
        return False, error or f"events answered {status} without the sentinel"

    status, _, error = _api_get(port, f"/users/{sentinels.user}/events?limit=101")
    if status != 422:
        return False, error or f"limit=101 answered {status}, wanted 422"

    status, body, error = _api_get(port, "/cluster")
    if status != 200 or not isinstance(body, dict):
        return False, error or f"/cluster answered {status}"
    if not body.get("snapshot_matches_leader"):
        return False, "/cluster: the snapshot does not match the leader yet"
    seen = (body.get("leader") or {}).get("identity")
    expected = (first_snapshot.get("leader") or {}).get("identity")
    if seen != expected:
        return False, f"/cluster reports leader {seen}, ZooKeeper said {expected}"

    status, body, error = _api_get(port, "/openapi.json")
    paths = set(body.get("paths", {})) if isinstance(body, dict) else set()
    if status != 200 or paths != API_ROUTES:
        return False, error or f"/openapi.json lists {sorted(paths)}"

    return True, (
        f"API on port {port} read back the Redis, Cassandra and ZooKeeper "
        f"sentinels and states its 404 and 422 contracts"
    )


LIVE_TIMEOUT_SECONDS = 20


def _http_status(port: int, path: str) -> tuple[int | None, str, str]:
    """Status, content type and body of a non-JSON route."""
    try:
        with urllib.request.urlopen(  # noqa: S310 - a fixed localhost URL
            f"http://127.0.0.1:{port}{path}", timeout=5
        ) as response:
            return (
                response.status,
                response.headers.get("content-type", ""),
                response.read().decode(),
            )
    except (urllib.error.URLError, OSError) as error:
        return None, "", str(error)


def _socket_refused(port: int, origin: str | None) -> bool:
    """Whether the handshake is refused for this Origin, missing included."""
    try:
        with websocket_connect(
            f"ws://127.0.0.1:{port}/ws/pageviews", origin=origin, open_timeout=5
        ):
            return False
    except InvalidStatus:
        return True


def _receive_before(socket, deadline: float, clock) -> dict:
    """The next message, or TimeoutError once the deadline has passed.

    Checked explicitly. A receive timeout alone never expires while unrelated
    notifications keep arriving — and in the smoke topology they arrive every
    50ms — so a bound written as `recv(timeout=max(0.1, remaining))` once
    accepted its event 101 seconds into a 20-second check.
    """
    remaining = deadline - clock()
    if remaining <= 0:
        raise TimeoutError
    return json.loads(socket.recv(timeout=remaining))


def _a_known_event_is_notified(
    port: int, topic: str, run_id: str, clock=time.monotonic
) -> tuple[bool, str]:
    """One known event, end to end: Kafka, consumer, PUBLISH, bridge, socket.

    Subscribing first matters. Pub/sub would rightly drop a notification
    published before this socket was enrolled, so the event is produced only
    once the API says it is subscribed — `ready` with `bridge: "subscribed"`,
    or a `resubscribed` notice after a `ready` that said otherwise. Not merely
    any `ready`.
    """
    event = {
        "event_id": f"smoke-live-{run_id}",
        "user_id": f"live-{run_id}",
        "page": "/docs",
        "timestamp": time.time(),
    }
    origin = f"http://127.0.0.1:{port}"
    deadline = clock() + LIVE_TIMEOUT_SECONDS
    try:
        with websocket_connect(
            f"ws://127.0.0.1:{port}/ws/pageviews", origin=origin, open_timeout=5
        ) as socket:
            subscribed = False
            while not subscribed:
                message = _receive_before(socket, deadline, clock)
                subscribed = (
                    message["type"] == "ready" and message["bridge"] == "subscribed"
                ) or message["type"] == "resubscribed"

            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            )
            try:
                partition = get_partition(event["user_id"], KAFKA_PARTITIONS)
                producer.send(topic, event, partition=partition).get(
                    timeout=PRODUCE_TIMEOUT_SECONDS
                )
            finally:
                producer.close()

            while True:
                message = _receive_before(socket, deadline, clock)
                if (
                    message["type"] == "pageview"
                    and message["event_id"] == event["event_id"]
                ):
                    break
    except TimeoutError:
        return False, f"no notification for {event['event_id']} within the bound"
    except (WebSocketException, OSError, KafkaError, ValueError, KeyError) as error:
        return False, f"live notification check failed: {type(error).__name__}: {error}"

    if not (
        isinstance(message["partition"], int) and isinstance(message["offset"], int)
    ):
        return False, f"notification without partition and offset: {message}"

    for refused_origin in ("http://evil.example", None):
        if not _socket_refused(port, refused_origin):
            return False, f"a socket with Origin {refused_origin!r} was accepted"

    status, kind, body = _http_status(port, "/live")
    if status != 200 or "text/html" not in kind or "./live.mjs" not in body:
        return False, f"/live answered {status} {kind}"
    status, kind, body = _http_status(port, "/live.mjs")
    if status != 200 or "javascript" not in kind or "createTracker" not in body:
        return False, f"/live.mjs answered {status} {kind}"

    return True, (
        f"a known event reached a subscribed socket "
        f"(partition {message['partition']}, offset {message['offset']}); "
        f"foreign and missing origins refused"
    )


# --- The kept Flink job, in the topology (0.11) ------------------------------

# Opt-in, and on in CI: restart the jobmanager with the keeper running. Off by
# default locally, because the jobmanager is shared with a reader's own job.
RESTART_JOBMANAGER = "SMOKE_RESTART_JOBMANAGER"
WINDOW_TIMEOUT_SECONDS = 60
RESTART_TIMEOUT_SECONDS = 150


def _keeper_record(root: str) -> tuple[job_keeper.Record | None, int, str]:
    """The run's keeper record and its version, or why it cannot be read."""
    try:
        client = coordination.connect()
    except Exception as error:  # noqa: BLE001 - reported, not swallowed
        return None, -1, f"could not connect to ZooKeeper: {error}"
    try:
        raw, version = coordination.read_flink_record(
            client, coordination.Paths(root=root)
        )
        return job_keeper.Record.parse(raw), version, ""
    except Exception as error:  # noqa: BLE001 - reported, not swallowed
        return None, -1, f"reading the keeper's record failed: {error}"
    finally:
        client.stop()
        client.close()


def _current_job(record: job_keeper.Record) -> str | None:
    attempt = record.attempts.get(record.generation)
    return attempt.job_id if attempt else None


def _job_is_kept(root: str, base: str) -> tuple[bool, str]:
    """The leader's record names a running job, of the record's generation."""
    record, _version, error = _keeper_record(root)
    if record is None:
        return False, error
    job_id = _current_job(record)
    if record.generation == 0 or job_id is None:
        return False, f"no recorded job yet (generation {record.generation})"
    try:
        state = flink_cluster.job_state(job_id)
    except flink_cluster.FlinkError as error:
        return False, f"job {job_id[:8]}: {error}"
    if state != "RUNNING":
        return False, f"job {job_id[:8]} is {state}"
    name = job_keeper.job_name(base, record.generation)
    return True, f"the leader keeps {name} running as {job_id[:8]}"


def _windows_written(prefix: str) -> tuple[bool, str]:
    try:
        client = redis_store.connect()
    except (OSError, redis_store.redis.RedisError) as error:
        return False, f"could not connect to Redis: {error}"
    try:
        count = client.zcard(windows_writer.index_key(prefix))
    except redis_store.redis.RedisError as error:
        return False, f"reading Redis failed: {error}"
    finally:
        client.close()
    if not count:
        return False, f"no windows under {prefix} yet"
    return True, f"{count} window(s) written under {prefix}"


def _epoch(text: str) -> int:
    """A window bound, as the job writes it (`...Z`) or as the API serves it."""
    return int(datetime.fromisoformat(text).timestamp())


def _expected_window(records: list[dict], start: int) -> dict[str, int]:
    """What Redis must hold for one window, from the job's own output: the
    newest generation's latest result per page. A newer generation replaces a
    window whole, so pages only an older one wrote are not expected."""
    ours = [r for r in records if _epoch(r["window_start"]) == start]
    if not ours:
        return {}
    newest = max(r.get("generation", 0) for r in ours)
    return {r["page"]: r["views"] for r in ours if r.get("generation", 0) == newest}


def _served_window(port: int, start: int) -> tuple[dict | None, str]:
    status, body, error = _api_get(port, "/windows?limit=20")
    if status != 200 or not isinstance(body, dict):
        return None, error or f"/windows answered {status}"
    for served in body.get("windows", []):
        if _epoch(served["start"]) == start:
            return served, ""
    return None, f"/windows does not hold the window at {start}"


def _a_window_reaches_the_page(
    port: int, windows_topic: str, clock=time.monotonic
) -> tuple[bool, str]:
    """A window hint on a socket, then that window over HTTP with exactly the
    counts the job wrote for it.

    The expected counts are the job's own output in the run's windows topic,
    not an injected fixture: nothing but a working writer and API can produce
    them, and the 0.9 known event — page /docs, a producer page — cannot
    contaminate them. Whether Flink counts correctly is `flink windows`'s job.
    Both sides are read afresh on each attempt, since a window's pages can land
    in different batches.
    """
    origin = f"http://127.0.0.1:{port}"
    deadline = clock() + WINDOW_TIMEOUT_SECONDS
    try:
        with websocket_connect(
            f"ws://127.0.0.1:{port}/ws/pageviews", origin=origin, open_timeout=5
        ) as socket:
            subscribed = False
            while not subscribed:
                message = _receive_before(socket, deadline, clock)
                subscribed = (
                    message["type"] == "ready" and message["bridge"] == "subscribed"
                ) or message["type"] == "resubscribed"
            while True:
                message = _receive_before(socket, deadline, clock)
                if message["type"] == "window":
                    break
    except TimeoutError:
        return False, f"no window hint within {WINDOW_TIMEOUT_SECONDS}s"
    except (WebSocketException, OSError, ValueError, KeyError) as error:
        return False, f"window hint check failed: {type(error).__name__}: {error}"

    start = _epoch(message["window_start"])
    detail = "never compared"
    while clock() < deadline:
        try:
            snapshot = windows.read_all(windows_topic)
        except KafkaError as error:
            detail = f"could not read {windows_topic}: {error}"
            time.sleep(1)
            continue
        expected = _expected_window(snapshot.records, start)
        served, error = _served_window(port, start)
        if served is not None and expected and served["counts"] == expected:
            return True, (
                f"window {message['window_start']} hinted on the socket and served "
                f"over HTTP as the job wrote it: {expected}"
            )
        detail = error or f"served {served and served['counts']}, job wrote {expected}"
        time.sleep(1)
    return False, f"window {message['window_start']}: {detail}"


def _the_job_survives_a_restart(
    root: str, port: int, clock=time.monotonic
) -> tuple[bool, str]:
    """The release's central claim, with the keeper running: the jobmanager
    restarted, and the same job back — restored, not resubmitted — then
    counting input produced after the recovery.

    An unchanged record version proves no new intent was recorded, not that no
    earlier, authorised submission could still land.
    """
    deadline = clock() + RESTART_TIMEOUT_SECONDS
    record, version, error = _keeper_record(root)
    job_id = _current_job(record) if record else None
    if record is None or job_id is None:
        return False, error or "no recorded job to restart under"
    if not _wait_for(
        lambda: _quietly(lambda: flink_cluster.completed_checkpoints(job_id) >= 1),
        deadline,
        clock,
    ):
        return False, f"job {job_id[:8]} completed no checkpoint before the restart"
    try:
        subprocess.run(
            ["docker", "compose", "restart", "flink-jobmanager"],
            check=True,
            capture_output=True,
            timeout=COMMAND_TIMEOUT_SECONDS,
        )
    except (subprocess.SubprocessError, OSError) as error:
        return False, f"could not restart the jobmanager: {error}"
    if not _wait_for(
        lambda: _quietly(lambda: flink_cluster.job_state(job_id) == "RUNNING"),
        deadline,
        clock,
    ):
        return False, f"job {job_id[:8]} did not come back RUNNING"
    back_at = time.time()
    restored = _quietly(lambda: flink_cluster.restored_checkpoint(job_id))
    if not restored:
        return False, f"job {job_id[:8]} is back but restored from no checkpoint"

    after, after_version, error = _keeper_record(root)
    if after is None:
        return False, error
    if after_version != version or _current_job(after) != job_id:
        return False, (
            f"the keeper recorded new intent across the restart: version "
            f"{version} -> {after_version}, generation {record.generation} -> "
            f"{after.generation}"
        )

    while clock() < deadline:
        status, body, _error = _api_get(port, "/windows?limit=20")
        fresh = [
            w
            for w in (body or {}).get("windows", [])
            if status == 200 and _epoch(w["start"]) >= back_at
        ]
        if fresh:
            return True, (
                f"jobmanager restarted: job {job_id[:8]} back, restored from "
                f"checkpoint {restored}, record unchanged at version {version}, "
                f"and a window of post-recovery input served ({fresh[-1]['start']})"
            )
        time.sleep(1)
    return False, f"job {job_id[:8]} back, but no window of post-recovery input"


def _quietly(read):
    """A Flink read that is false, not an exception, while Flink is away."""
    try:
        return read()
    except (flink_cluster.FlinkError, KeyError, TypeError):
        return None


def _kept_jobs_stopped(root: str, base: str, clock=time.monotonic) -> str:
    """Stop every job this run's keeper may have submitted, or say why not.

    From the record: every attempt with an id is stopped and confirmed; every
    attempt without one is reconciled by its own generation's name for 0.10's
    twenty seconds, and if any is still unaccounted for, that is an error, and
    the caller keeps the topics. Finding generation 2 never accounts for
    generation 1. The prefix listing is a cross-check, for jobs no record names.
    """
    record, _version, error = _keeper_record(root)
    if record is None:
        return error
    known = {g: a.job_id for g, a in record.attempts.items()}
    unresolved = {g for g, job_id in known.items() if job_id is None}
    look_until = clock() + (FLINK_RECONCILE_SECONDS if unresolved else 0)
    found: dict[int, set[str]] = {}
    listing_error = ""
    while True:
        try:
            for job in flink_cluster.jobs_overview():
                generation = job_keeper.generation_of(base, job["name"])
                if generation is not None:
                    found.setdefault(generation, set()).add(job["id"])
            listing_error = ""
        except flink_cluster.FlinkError as error:
            listing_error = f"could not list the run's jobs: {error}"
        if (unresolved <= set(found) and not listing_error) or clock() >= look_until:
            break
        time.sleep(1)

    errors = [listing_error] if listing_error else []
    candidates = {job_id for job_id in known.values() if job_id}
    candidates |= {job_id for ids in found.values() for job_id in ids}
    for candidate in sorted(candidates):
        stop_error, _state = _flink_stop(candidate, clock)
        if stop_error:
            errors.append(stop_error)
    for generation in sorted(unresolved - set(found)):
        errors.append(
            f"generation {generation} ({job_keeper.job_name(base, generation)}) "
            f"not found within {FLINK_RECONCILE_SECONDS}s, so its submission may "
            "still land"
        )
    return "; ".join(errors)


def window_writes_follow_generations() -> tuple[bool, str]:
    """The writer's script, against Redis itself: the rules no fake can prove.

    Per window: an older generation refused once a newer one is accepted; the
    same generation updating a page; a newer one replacing the window whole; a
    window past its end plus retention skipped, not resurrected; the index
    trimmed; and the API's read taking counts and generation together.
    """
    prefix = f"smoke:windows:{uuid.uuid4().hex[:8]}:"
    try:
        client = redis_store.connect()
    except (OSError, redis_store.redis.RedisError) as error:
        return False, f"could not connect to Redis: {error}"
    now = time.time()
    start = int(now) // 10 * 10 - 60
    old = start - 7200

    def window(views: int, generation: int, page: str = "/docs", at: int = start):
        return windows_writer.Result(at, at + 10, page, views, generation)

    try:
        writer = windows_writer.Writer(client, prefix=prefix, retention=3600)
        steps = [
            (
                [window(3, 2), window(1, 2, "/pricing")],
                [1, 1],
                {"/docs": 3, "/pricing": 1},
                2,
            ),
            ([window(9, 1)], [0], {"/docs": 3, "/pricing": 1}, 2),
            ([window(4, 2)], [1], {"/docs": 4, "/pricing": 1}, 2),
            ([window(5, 3)], [1], {"/docs": 5}, 3),
            ([window(1, 3, at=old)], [-1], {"/docs": 5}, 3),
        ]
        for results, outcomes, counts, generation in steps:
            got = writer.write(results)
            served = api.read_windows(client, prefix, 20, time.time(), retention=3600)
            if got != outcomes or [(w["counts"], w["generation"]) for w in served] != [
                (counts, generation)
            ]:
                return False, (
                    f"writing {results} gave {got} and served {served}; wanted "
                    f"{outcomes} and {counts} at generation {generation}"
                )
        if client.exists(windows_writer.window_key(old, prefix)):
            return False, "a window past its retention was resurrected"
        client.zadd(windows_writer.index_key(prefix), {str(old): old})
        writer.write([window(6, 3)])
        if client.zscore(windows_writer.index_key(prefix), str(old)) is not None:
            return False, "an expired window stayed in the index after a write"
    except redis_store.redis.RedisError as error:
        return False, f"Redis failed: {error}"
    finally:
        try:
            redis_store.clear(client, prefix)
        finally:
            client.close()
    return True, (
        "older generation refused, same updated, newer replaced the window "
        "whole, an expired window skipped and trimmed — in Redis itself"
    )


def honcho_topology_does_the_work() -> tuple[bool, str]:
    """Run the real Procfile topology and assert it does its job.

    Isolated deliberately: its own topic, consumer group, Redis prefix,
    Cassandra keyspace, RabbitMQ queue and ZooKeeper root, all handed to Honcho
    through the environment. Sharing any of them would let a topology someone
    left running satisfy this check.

    Readiness is a list of predicates rather than a fixed sequence, because at
    0.7 the topology has a *state* — exactly one acknowledged leader — and not
    merely a process count.
    """
    run_id = uuid.uuid4().hex[:8]
    topic = f"smoke_topology_{run_id}"
    group = f"smoke-topology-{run_id}"
    prefix = f"smoke:{run_id}:"
    keyspace = f"smoke_{run_id}"
    queue = f"smoke_jobs_{run_id}"
    root = f"/smoke_{run_id}"
    windows_topic = f"smoke_windows_{run_id}"
    job_base = f"smoke-topology-{run_id}"
    api_port = _free_port()
    sentinels = Sentinels.for_run(run_id)

    topology = Topology(
        os.environ
        | {
            "KAFKA_TOPIC": topic,
            "CONSUMER_GROUP": group,
            "REDIS_KEY_PREFIX": prefix,
            "CASSANDRA_KEYSPACE": keyspace,
            "RABBITMQ_QUEUE": queue,
            "ZOOKEEPER_ROOT": root,
            "PRODUCER_INTERVAL_SECONDS": "0.05",
            "COMMIT_EVERY": "1",
            "CONSUMER_CRASH_AFTER": "",
            "WORKER_DELAY_SECONDS": "0.02",
            "SNAPSHOT_INTERVAL_SECONDS": "0.5",
            # The Flink job the leader keeps, on topics and a name of this
            # run's own: generations are named `smoke-topology-<run>-g<n>`.
            "KAFKA_WINDOWS_TOPIC": windows_topic,
            "WINDOWS_WRITER_GROUP": f"smoke-windows-{run_id}",
            "FLINK_JOB_BASE": job_base,
            "FLINK_GROUP": f"smoke-flink-{run_id}",
            "FLINK_CHECKPOINT_SECONDS": "2",
            "FLINK_KEEPER_INTERVAL_SECONDS": "1",
            # The server binds this port itself: the API runs inside the run's
            # environment, reading its prefix, keyspace and root.
            "API_PORT": str(api_port),
        }
    )

    observed: tuple[bool, str] = (False, "not run")
    started = False
    shutdown_error = ""
    first_snapshot: dict = {}

    try:
        setup_error = _prepare_topology(
            [topic, windows_topic], keyspace, root, prefix, sentinels
        )
        if setup_error:
            observed = (False, setup_error)
        else:
            if launch_error := topology.start():
                observed = (False, launch_error)
            else:
                started = True
                observed = topology.wait_until_ready(
                    _readiness(
                        group,
                        prefix,
                        keyspace,
                        queue,
                        root,
                        first_snapshot,
                        api=(api_port, sentinels),
                        flink=job_base,
                    ),
                    timeout=TOPOLOGY_SETTLE_SECONDS,
                )
                if observed[0]:
                    readiness_detail = observed[1]
                    working, detail = _leader_is_working(root, first_snapshot)
                    if working:
                        working, config_detail = _live_config_reaches_workers(root)
                        detail = f"{detail}; {config_detail}"
                    if working:
                        working, live_detail = _a_known_event_is_notified(
                            api_port, topic, run_id
                        )
                        detail = f"{detail}; {live_detail}"
                    if working:
                        working, window_detail = _a_window_reaches_the_page(
                            api_port, windows_topic
                        )
                        detail = f"{detail}; {window_detail}"
                    if working and os.environ.get(RESTART_JOBMANAGER) == "1":
                        working, restart_detail = _the_job_survives_a_restart(
                            root, api_port
                        )
                        detail = f"{detail}; {restart_detail}"
                    observed = (working, f"{readiness_detail}; {detail}")
    finally:
        flink_error, stopped_cleanly = "", True
        if started:
            stopped, shutdown_error = topology.stop()
            if not stopped and not shutdown_error:
                shutdown_error = "the topology did not stop"
            stopped_cleanly = stopped and not shutdown_error
            if stopped_cleanly:
                # The job outlives the leader by design — it is the pipeline's —
                # so it is stopped here, once nothing can submit another.
                flink_error = _kept_jobs_stopped(root, job_base)
        if not stopped_cleanly:
            # A process that may still run may still use every resource the run
            # made — and a coordinator may still submit — so reconciliation
            # would not be final, and nothing is removed.
            kept = (
                f"left in place for processes that may still run: {prefix}, "
                f"{keyspace}, {queue}, {root}, {topic}, {windows_topic}"
            )
            logger.warning(kept)
            shutdown_error = f"{shutdown_error}; {kept}"
        else:
            # Only after the processes have gone: a live one would write it back.
            _clear_redis_prefix(prefix)
            _drop_keyspace(keyspace)
            _delete_queue(queue)
            if flink_error:
                # Never the inputs underneath a job whose shutdown is unconfirmed —
                # 0.10's rule, for every generation the run's keeper may have
                # submitted — nor the record that says what is still owed.
                kept = f"kept {root}, {topic} and {windows_topic} for reconciliation"
                logger.warning(f"{kept}: {flink_error}")
                shutdown_error = "; ".join(
                    filter(None, [shutdown_error, flink_error, kept])
                )
            else:
                _delete_zookeeper_root(root)
                _delete_topic(topic)
                _delete_topic(windows_topic)

    # Shutdown outranks the observation: a check that leaves the topology
    # running, or cannot tell whether it did, has not finished its job.
    if shutdown_error:
        return False, f"shutdown incomplete: {shutdown_error}"
    passed, detail = observed
    if passed:
        return True, f"{detail}; nothing was left running"
    return observed


def _prepare_topology(
    topics: list[str], keyspace: str, root: str, prefix: str, sentinels: Sentinels
) -> str:
    """Create everything the topology expects to already exist."""
    try:
        for topic in topics:
            ensure_topic(topic, KAFKA_PARTITIONS, KAFKA_SERVER)
    except (KafkaError, OSError, RuntimeError) as error:
        return f"create-topics path failed: {error}"

    if schema_error := _create_keyspace(keyspace):
        return schema_error

    try:
        client = coordination.connect()
    except Exception as error:  # noqa: BLE001 - reported, not swallowed
        return f"could not connect to ZooKeeper: {error}"
    try:
        coordination.initialise(client, coordination.Paths(root=root), 0.02)
    except Exception as error:  # noqa: BLE001 - reported, not swallowed
        return f"cluster init path failed: {error}"
    finally:
        client.stop()
        client.close()
    return _write_sentinels(prefix, keyspace, sentinels)


def _readiness(
    group: str,
    prefix: str,
    keyspace: str,
    queue: str,
    root: str,
    first_snapshot: dict,
    api: tuple[int, Sentinels] | None = None,
    flink: str | None = None,
) -> list[tuple[str, object]]:
    """Everything that must hold before the topology counts as working.

    The API goes last, so the leader it reports can be compared with the one
    the ZooKeeper predicate saw in the same round.

    The API's sentinels live under the same prefix and keyspace the consumers
    write to, so the write predicates leave them out. Otherwise the sentinels
    alone would satisfy them, and a consumer that wrote nothing would pass.
    """
    sentinels = api[1] if api is not None else None

    def kafka_group() -> tuple[bool, str]:
        members, error = _group_members(group)
        if members is None:
            return False, error
        offsets, error = _group_offsets(group)
        if offsets is None:
            return False, error
        if len(members) != KAFKA_PARTITIONS or len(offsets) != KAFKA_PARTITIONS:
            return False, (
                f"{len(members)} members owning {len(offsets)} partitions, "
                f"wanted {KAFKA_PARTITIONS} of each"
            )
        return True, f"{len(members)} consumers own {len(offsets)} partitions"

    def offsets_advance() -> tuple[bool, str]:
        offsets, error = _group_offsets(group)
        if offsets is None:
            return False, error
        total = sum(offsets.values())
        previous = first_snapshot.get("offsets")
        if previous is None:
            first_snapshot["offsets"] = total
            return False, "recorded a baseline; waiting for it to advance"
        if total <= previous:
            return False, f"committed offsets have not advanced past {previous}"
        return True, f"committed offsets advanced {previous} to {total}"

    def redis_branches() -> tuple[bool, str]:
        counts, pages, error = _redis_state(prefix)
        if counts is None:
            return False, error
        if sentinels is not None:
            counts = {k: v for k, v in counts.items() if k != sentinels.page}
            pages = {k: v for k, v in pages.items() if k != sentinels.user}
        if not counts:
            return False, f"no {prefix}pageviews:* counters"
        if not pages:
            return False, f"no {prefix}user:last_page:* values"
        return True, f"{len(counts)} counters, {len(pages)} last-page values"

    def cassandra_rows() -> tuple[bool, str]:
        rows, error = _cassandra_rows(keyspace)
        if rows is None:
            return False, error
        if sentinels is not None:
            # At most one sentinel row, so LIMIT 5 still reaches consumer rows.
            rows = [row for row in rows if row.user_id != sentinels.user]
        if not rows:
            return False, f"no rows in {keyspace}"
        missing = [
            column
            for column in ("user_id", "event_time", "event_id", "page")
            if getattr(rows[0], column, None) in (None, "")
        ]
        if missing:
            return False, f"rows missing {missing}"
        return True, f"rows written to {keyspace}"

    def rabbit_workers() -> tuple[bool, str]:
        consumers, error = _queue_consumers(queue)
        if consumers is None:
            return False, error
        if consumers != KAFKA_PARTITIONS:
            return False, f"{consumers} workers on {queue}, expected {KAFKA_PARTITIONS}"
        completed, _runs, error = _job_state(prefix)
        if completed is None:
            return False, error
        if completed < 1:
            return False, "no jobs completed"
        return True, f"{consumers} workers, {completed} jobs completed"

    def one_leader() -> tuple[bool, str]:
        state, error = _coordination_state(root)
        if state is None:
            return False, error
        leader = state["leader"]
        if leader is None:
            return (
                False,
                f"no acknowledged leader among {state['contenders']} contenders",
            )
        # Every role, not just workers: the previous version passed with four
        # workers, zero consumers and any number of contenders.
        expected = {
            "contenders": (state["contenders"], COORDINATORS),
            "coordinator registrations": (len(state["coordinators"]), COORDINATORS),
            "consumer registrations": (len(state["consumers"]), KAFKA_PARTITIONS),
            "worker registrations": (len(state["workers"]), KAFKA_PARTITIONS),
        }
        for label, (seen, wanted) in expected.items():
            if seen != wanted:
                return False, f"{seen} {label}, expected {wanted}"

        # The leader must be one of the registered coordinators, not a process
        # that wrote the marker and vanished.
        identities = {entry["identity"] for entry in state["coordinators"]}
        if leader["identity"] not in identities:
            return False, (
                f"leader {leader['identity']} is not among the registered "
                f"coordinators {sorted(identities)}"
            )

        snapshot = state["snapshot"]
        if snapshot.get("epoch") != leader["epoch"]:
            return False, (
                f"snapshot epoch {snapshot.get('epoch')} does not match "
                f"leader epoch {leader['epoch']}"
            )
        first_snapshot.setdefault("version", state["snapshot_version"])
        first_snapshot.setdefault("leader", leader)
        return True, (
            f"one leader at epoch {leader['epoch']} among {state['contenders']} "
            f"contenders; {len(state['consumers'])} consumers and "
            f"{len(state['workers'])} workers registered"
        )

    predicates = [
        ("kafka group", kafka_group),
        ("offsets", offsets_advance),
        ("redis", redis_branches),
        ("cassandra", cassandra_rows),
        ("rabbitmq", rabbit_workers),
        ("zookeeper", one_leader),
    ]
    if flink is not None:
        predicates.append(("flink", lambda: _job_is_kept(root, flink)))
        predicates.append(("windows", lambda: _windows_written(prefix)))
    if api is not None:
        port = api[0]
        predicates.append(
            ("api", lambda: _api_serves_the_stores(port, sentinels, first_snapshot))
        )
    return predicates


def _live_config_reaches_workers(root: str) -> tuple[bool, str]:
    """Publish a new delay and require every worker to report that exact version.

    Without this the routine check would pass even if the whole watch path
    broke: the workers would keep their startup values and nothing would say so.
    Manual observation is not a substitute for a check that runs every time.
    """
    paths = coordination.Paths(root=root)
    try:
        client = coordination.connect()
    except Exception as error:  # noqa: BLE001 - reported, not swallowed
        return False, f"could not connect to ZooKeeper: {error}"

    try:
        new_delay = "0.03"
        stat = client.set(paths.worker_delay, new_delay.encode())
        wanted = stat.version

        deadline = time.monotonic() + LIVE_CONFIG_TIMEOUT_SECONDS
        seen: list = []
        while time.monotonic() < deadline:
            entries = coordination.registrations(client, paths, "worker")
            seen = [
                (entry.get("config_version"), str(entry.get("delay")))
                for entry in entries
            ]
            if len(seen) == KAFKA_PARTITIONS and all(
                version == wanted and delay == new_delay for version, delay in seen
            ):
                return True, (
                    f"all {KAFKA_PARTITIONS} workers applied worker_delay="
                    f"{new_delay} at config version {wanted}"
                )
            time.sleep(0.5)
        return False, (
            f"workers did not all report config version {wanted} "
            f"with delay {new_delay}; saw {seen}"
        )
    except Exception as error:  # noqa: BLE001 - reported, not swallowed
        return False, f"publishing live config failed: {error}"
    finally:
        client.stop()
        client.close()


def _leader_is_working(root: str, first: dict) -> tuple[bool, str]:
    """A genuinely new snapshot from the same leader and epoch.

    Reading the same znode twice would satisfy a naive "two snapshots", so this
    requires the version to advance while the epoch and the leader stay put.
    """
    deadline = time.monotonic() + TOPOLOGY_PROGRESS_SECONDS
    while time.monotonic() < deadline:
        state, error = _coordination_state(root)
        if state is None:
            return False, error
        if (
            state["snapshot_version"] > first["version"]
            and state["snapshot"].get("epoch") == first["leader"]["epoch"]
            and state["leader"]
            and state["leader"]["identity"] == first["leader"]["identity"]
        ):
            return True, (
                f"leader {first['leader']['identity']} wrote again at epoch "
                f"{first['leader']['epoch']} "
                f"(snapshot v{first['version']} -> v{state['snapshot_version']})"
            )
        time.sleep(0.5)
    return False, "the leader did not write a second snapshot at the same epoch"


FLINK_TIMEOUT_SECONDS = 150
FLINK_WINDOW_SECONDS = 10
FLINK_WATERMARK_DELAY_SECONDS = 2

# One user per partition under the routing rule (a-g, h-m, n-t, u-z), so every
# partition carries events and every partition's watermark must advance.
FLINK_USERS = ("ada", "hal", "ned", "uma")
FLINK_EXPECTED = {"/docs": 3, "/pricing": 2}  # per partition, so x4 in total


def _flink_events(window_start: int) -> tuple[list[tuple[int, dict]], dict[str, int]]:
    """Deterministic events in one window, then advancement past its closure.

    The advancement records sit comfortably beyond window end *plus* the
    watermark delay on every partition: past the window's end is not enough,
    because the watermark trails the newest event by the delay.
    """
    events, n = [], 0
    for user in FLINK_USERS:
        partition = get_partition(user, KAFKA_PARTITIONS)
        for page, count in FLINK_EXPECTED.items():
            for _ in range(count):
                n += 1
                events.append(
                    (
                        partition,
                        {
                            "event_id": f"smoke-flink-{n}",
                            "user_id": user,
                            "page": page,
                            "timestamp": window_start + 1 + n % 8,
                        },
                    )
                )
    beyond = window_start + FLINK_WINDOW_SECONDS + FLINK_WATERMARK_DELAY_SECONDS + 5
    for user in FLINK_USERS:
        n += 1
        events.append(
            (
                get_partition(user, KAFKA_PARTITIONS),
                {
                    "event_id": f"smoke-flink-{n}",
                    "user_id": user,
                    "page": "/advance",
                    "timestamp": beyond,
                },
            )
        )
    expected = {
        page: count * len(FLINK_USERS) for page, count in FLINK_EXPECTED.items()
    }
    return events, expected


def _wait_for(condition, deadline: float, clock=time.monotonic) -> bool:
    while clock() < deadline:
        if condition():
            return True
        time.sleep(1)
    return False


# After an uncertain submission, how long to keep looking for a job that may
# still be on its way: killing `docker compose exec` need not stop the `flink
# run` inside the container, which can submit after the client gave up.
FLINK_RECONCILE_SECONDS = 20


def _flink_stop(job_id: str, clock) -> tuple[str, str]:
    """Stop one job: (an error or empty, the state it had already stopped in).

    Any stopped state will do, not only CANCELED: a job that failed by itself
    reads nothing more. And Flink refuses to cancel a job that has already
    stopped, so the state is read first, and again if a cancel is refused.
    """
    try:
        try:
            state = flink_cluster.job_state(job_id)
        except flink_cluster.NotFound:
            # No runner and no archived copy: ended before a jobmanager restart.
            return "", "NOT FOUND"
        if state in flink_cluster.TERMINAL:
            return "", state
        try:
            flink_cluster.cancel(job_id)
        except flink_cluster.FlinkError as error:
            state = flink_cluster.job_state(job_id)
            if state in flink_cluster.TERMINAL:
                return "", state  # it stopped by itself in between
            return f"could not cancel job {job_id}: {error}", ""
        if not _wait_for(
            lambda: flink_cluster.job_state(job_id) in flink_cluster.TERMINAL,
            clock() + 30,
            clock,
        ):
            return f"job {job_id} did not stop after being cancelled", ""
        return "", ""
    except flink_cluster.FlinkError as error:
        return f"could not stop job {job_id}: {error}", ""


def _flink_shutdown(
    job_name: str, job_id: str | None, uncertain: bool, clock
) -> tuple[str, list[str]]:
    """Stop every job this run started: (an error or empty, early stops).

    Confirmed only when the run's job is known and stopped. Known means its
    captured id, or a job found under the run's unique name — which only this
    run uses, unlike a reader's `pageview-windows`; one `flink run` submits one
    job, so finding it accounts for the submission. An uncertain submission
    with nothing found stays unconfirmed however long we look, because the
    submitter inside the container may still be running.

    The name is looked up even when the id is known, every known job is
    stopped even when a lookup fails, and a failed lookup stays an error.
    Listings include stopped jobs, so the last one covers all earlier ones.
    """
    if job_id is None and not uncertain:
        return "", []  # nothing was submitted
    errors, early = [], []
    candidates = {job_id} if job_id else set()
    look_until = clock() + (FLINK_RECONCILE_SECONDS if uncertain else 0)
    while True:
        try:
            candidates |= set(flink_cluster.jobs_named(job_name))
            listing_error = ""
        except flink_cluster.FlinkError as error:
            listing_error = f"could not list jobs named {job_name}: {error}"
        if (candidates and not listing_error) or clock() >= look_until:
            break
        time.sleep(1)
    if listing_error:
        errors.append(listing_error)
    if not candidates:
        errors.append(
            f"no job named {job_name} found within {FLINK_RECONCILE_SECONDS}s, "
            "so its submission may still land"
        )
    for candidate in sorted(candidates):
        error, state = _flink_stop(candidate, clock)
        if error:
            errors.append(error)
        if state:
            early.append(f"job {candidate[:8]} had already stopped: {state}")
    return "; ".join(errors), early


def flink_windows_round_trip(clock=time.monotonic) -> tuple[bool, str]:
    """Kafka -> Flink -> Kafka, deterministic, on a job and topics of its own.

    Idleness is disabled for this job, so a window closes only because the
    events here advanced every partition's watermark — never because a
    processing-time timeout fired, which would make the check a race. And one
    completed checkpoint is required: correct output and RUNNING can both happen
    before any checkpoint, so an unwritable checkpoint directory would otherwise
    pass unnoticed.

    Every path ends in the same place: reconcile and cancel, then remove the
    topics only if every job of this run is confirmed stopped — never delete the
    inputs underneath a job whose shutdown is unconfirmed — and report both the
    check's failure and the shutdown's, if there are two.
    """
    run_id = uuid.uuid4().hex[:8]
    source, sink = f"smoke_flink_in_{run_id}", f"smoke_flink_out_{run_id}"
    job_name = f"smoke-flink-{run_id}"
    job_id: str | None = None
    uncertain = False
    observed: tuple[bool, str] = (False, "not run")
    deadline = clock() + FLINK_TIMEOUT_SECONDS
    try:
        ensure_topic(source, KAFKA_PARTITIONS, KAFKA_SERVER)
        ensure_topic(sink, KAFKA_PARTITIONS, KAFKA_SERVER)
        # Uncertain from the moment the submission starts until it says
        # otherwise: whatever escapes it may have left a job behind.
        uncertain = True
        try:
            job_id = flink_cluster.submit(
                FlinkSettings(
                    source_topic=source,
                    sink_topic=sink,
                    group=f"smoke-flink-{run_id}",
                    job_name=job_name,
                    window_seconds=FLINK_WINDOW_SECONDS,
                    watermark_delay_seconds=FLINK_WATERMARK_DELAY_SECONDS,
                    idle_timeout_seconds=0,
                    checkpoint_seconds=2,
                )
            )
            uncertain = False
        except flink_cluster.SubmissionUncertain as error:
            observed = (False, f"submission uncertain: {error}")
        except flink_cluster.FlinkError:
            uncertain = False  # the command never ran: nothing was submitted
            raise
        if job_id is not None:
            observed = _flink_observe(job_id, source, sink, deadline, clock)
    except (flink_cluster.FlinkError, KafkaError, OSError, ValueError) as error:
        observed = (False, f"Flink round trip failed: {type(error).__name__}: {error}")
    finally:
        shutdown_error, early = _flink_shutdown(job_name, job_id, uncertain, clock)
        if shutdown_error:
            logger.warning(f"leaving {source} and {sink}: {shutdown_error}")
        else:
            _delete_topic(source)
            _delete_topic(sink)

    passed, detail = observed
    # A streaming job that stopped before it was cancelled is a failure even
    # when its output was right.
    problems = early + (
        [f"shutdown incomplete: {shutdown_error}"] if shutdown_error else []
    )
    if problems:
        failure = detail if not passed else "the check itself passed"
        return False, "; ".join([failure, *problems])
    return (True, f"{detail}; cancelled") if passed else observed


def _flink_observe(
    job_id: str, source: str, sink: str, deadline: float, clock
) -> tuple[bool, str]:
    """Run the round trip against a submitted job."""
    if not _wait_for(
        lambda: flink_cluster.job_state(job_id) == "RUNNING", deadline, clock
    ):
        return False, f"job {job_id} never reached RUNNING"

    window_start = (
        int(time.time()) // FLINK_WINDOW_SECONDS
    ) * FLINK_WINDOW_SECONDS - 600
    events, expected = _flink_events(window_start)
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)
    try:
        for partition, event in events:
            producer.send(source, json.dumps(event).encode(), partition=partition)
        producer.flush(timeout=PRODUCE_TIMEOUT_SECONDS)
    finally:
        producer.close()

    wanted = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(window_start))
    counts: dict[str, int] = {}
    consumer = KafkaConsumer(
        sink,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
    )
    try:
        while clock() < deadline and counts != expected:
            for message in consumer:
                record = json.loads(message.value)
                if record["window_start"] == wanted:
                    counts[record["page"]] = record["views"]
                # Checked per record, not only when the topic goes quiet: 0.9's
                # lesson — a bound that waits for silence never expires while
                # output keeps arriving.
                if clock() >= deadline or counts == expected:
                    break
    finally:
        consumer.close()
    if counts != expected:
        return False, f"window {wanted}: wanted {expected}, got {counts}"
    if not _wait_for(
        lambda: flink_cluster.completed_checkpoints(job_id) >= 1, deadline, clock
    ):
        return False, "the right counts, but no completed checkpoint"
    return True, (
        f"job {job_id[:8]} counted {counts} in window {wanted} on its own topics; "
        f"{flink_cluster.completed_checkpoints(job_id)} checkpoint(s) completed"
    )


CHECKS: list[tuple[str, Callable[[], tuple[bool, str]]]] = [
    ("host listener", host_listener_round_trips),
    ("internal listener", internal_listener_reaches_the_same_broker),
    ("partition routing", routing_uses_every_partition),
    ("window writes", window_writes_follow_generations),
    ("honcho topology", honcho_topology_does_the_work),
    ("flink windows", flink_windows_round_trip),
]


def prepare() -> None:
    """All the setup main() needs, in one place.

    Single entry point on purpose: every network call main() makes lives here,
    so a unit test patches one name and cannot accidentally leave a new one
    reaching for a broker.
    """
    ensure_topic(SMOKE_TOPIC, KAFKA_PARTITIONS, KAFKA_SERVER)
    # Creating a topic does not mean every client has seen it yet, and the
    # metadata refresh logs errors in the meantime.
    wait_for_topic(SMOKE_TOPIC, KAFKA_SERVER)


def main() -> int:
    started = time.monotonic()
    try:
        prepare()
    except (KafkaError, OSError, RuntimeError) as error:
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
