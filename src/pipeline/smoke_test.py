"""Bounded end-to-end check of the topology described by docker-compose.yml.

Compose answers "which services run"; this answers "are they actually working".
Keeping the assertions here rather than in the CI workflow means the workflow
stays a caller and the success criteria stay reviewable code.
"""

import json
import logging
import os
import subprocess
import sys
import time
import uuid
from collections.abc import Callable

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from pipeline import (
    cassandra_store,
    coordination,
    ensure_topic,
    get_partition,
    jobs_queue,
    redis_store,
    wait_for_topic,
)
from pipeline.config import KAFKA_PARTITIONS, KAFKA_SERVER
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
        }
    )

    observed: tuple[bool, str] = (False, "not run")
    started = False
    shutdown_error = ""
    first_snapshot: dict = {}

    try:
        setup_error = _prepare_topology(topic, keyspace, root)
        if setup_error:
            observed = (False, setup_error)
        else:
            if launch_error := topology.start():
                observed = (False, launch_error)
            else:
                started = True
                observed = topology.wait_until_ready(
                    _readiness(group, prefix, keyspace, queue, root, first_snapshot),
                    timeout=TOPOLOGY_SETTLE_SECONDS,
                )
                if observed[0]:
                    readiness_detail = observed[1]
                    working, detail = _leader_is_working(root, first_snapshot)
                    if working:
                        working, config_detail = _live_config_reaches_workers(root)
                        detail = f"{detail}; {config_detail}"
                    observed = (working, f"{readiness_detail}; {detail}")
    finally:
        if started:
            stopped, shutdown_error = topology.stop()
            if not stopped and not shutdown_error:
                shutdown_error = "the topology did not stop"
        # Only after the processes have gone: a live one would write it back.
        _clear_redis_prefix(prefix)
        _drop_keyspace(keyspace)
        _delete_queue(queue)
        _delete_zookeeper_root(root)
        _delete_topic(topic)

    # Shutdown outranks the observation: a check that leaves the topology
    # running, or cannot tell whether it did, has not finished its job.
    if shutdown_error:
        return False, f"shutdown incomplete: {shutdown_error}"
    passed, detail = observed
    if passed:
        return True, f"{detail}; nothing was left running"
    return observed


def _prepare_topology(topic: str, keyspace: str, root: str) -> str:
    """Create everything the topology expects to already exist."""
    try:
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
    return ""


def _readiness(
    group: str,
    prefix: str,
    keyspace: str,
    queue: str,
    root: str,
    first_snapshot: dict,
) -> list[tuple[str, object]]:
    """Everything that must hold before the topology counts as working."""

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
        if not counts:
            return False, f"no {prefix}pageviews:* counters"
        if not pages:
            return False, f"no {prefix}user:last_page:* values"
        return True, f"{len(counts)} counters, {len(pages)} last-page values"

    def cassandra_rows() -> tuple[bool, str]:
        rows, error = _cassandra_rows(keyspace)
        if rows is None:
            return False, error
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

    return [
        ("kafka group", kafka_group),
        ("offsets", offsets_advance),
        ("redis", redis_branches),
        ("cassandra", cassandra_rows),
        ("rabbitmq", rabbit_workers),
        ("zookeeper", one_leader),
    ]


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


CHECKS: list[tuple[str, Callable[[], tuple[bool, str]]]] = [
    ("host listener", host_listener_round_trips),
    ("internal listener", internal_listener_reaches_the_same_broker),
    ("partition routing", routing_uses_every_partition),
    ("honcho topology", honcho_topology_does_the_work),
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
