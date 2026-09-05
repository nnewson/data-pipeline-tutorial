"""Bounded end-to-end check of the topology described by docker-compose.yml.

Compose answers "which services run"; this answers "are they actually working".
Keeping the assertions here rather than in the CI workflow means the workflow
stays a caller and the success criteria stay reviewable code.
"""

import json
import logging
import os
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import Callable

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from pipeline import ensure_topic, get_partition, wait_for_topic
from pipeline.config import KAFKA_PARTITIONS, KAFKA_SERVER

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
HONCHO_STOP_TIMEOUT_SECONDS = 30


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


def _process_table() -> tuple[dict[int, tuple[int, int]], str]:
    """Every process as pid -> (ppid, pgid), or a reason it is unavailable."""
    try:
        result = subprocess.run(
            ["ps", "-eo", "pid=,ppid=,pgid="],
            check=False,
            capture_output=True,
            text=True,
            timeout=COMMAND_TIMEOUT_SECONDS,
        )
    except FileNotFoundError:
        return {}, "ps is not available on PATH"
    except subprocess.TimeoutExpired:
        return {}, "listing processes timed out"

    if result.returncode != 0:
        return {}, f"listing processes failed: {result.stderr.strip()}"

    table: dict[int, tuple[int, int]] = {}
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) != 3:
            continue
        try:
            table[int(fields[0])] = (int(fields[1]), int(fields[2]))
        except ValueError:
            continue
    return table, ""


def _topology_groups(pid: int) -> tuple[set[int], str]:
    """Process groups belonging to a process and everything below it.

    Honcho puts each Procfile process in a group of its own, so its own group is
    not enough to describe the topology. Taken before shutdown, while the parent
    links still exist: afterwards the children are reparented to init.
    """
    table, error = _process_table()
    if error:
        return set(), error

    if pid not in table:
        # Honcho exited between observation and snapshot. Its children may have
        # been reparented and still be running, and there is no longer a link to
        # find them by, so this cannot be reported as "nothing was running".
        return set(), f"honcho pid {pid} was absent; shutdown cannot be verified"

    children: dict[int, list[int]] = {}
    for child, (parent, _) in table.items():
        children.setdefault(parent, []).append(child)

    groups: set[int] = {table[pid][1]}

    pending = [pid]
    while pending:
        for child in children.get(pending.pop(), []):
            groups.add(table[child][1])
            pending.append(child)
    return groups, ""


def _process_group_is_empty(pgid: int) -> bool:
    """Whether any process remains in the group.

    Signal 0 asks about the group without touching it. Nothing here ever sends a
    real signal to a captured group, so a recycled PGID can only cause a false
    failure, never a false pass.
    """
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


def _stop_topology(process: subprocess.Popen) -> tuple[bool, str]:
    """Stop the topology, then assert that nothing it started is still running.

    Shutdown itself works because Honcho forwards termination to the process
    groups it manages — the workers are not members of Honcho's own group. The
    group check afterwards is a regression assertion rather than part of the
    mechanism: it fails if a future change (reintroducing `uv run`, or another
    launcher that starts processes in their own sessions) leaves work running
    after Honcho exits.
    """
    groups, snapshot_error = _topology_groups(process.pid)

    # Shut down whether or not the snapshot succeeded.
    try:
        honcho_group = os.getpgid(process.pid)
    except ProcessLookupError:
        honcho_group = None

    if honcho_group is not None:
        try:
            os.killpg(honcho_group, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass

    # Reap the direct child: an unreaped zombie still occupies its PID, so its
    # group would look occupied by a process that has already exited.
    try:
        process.wait(timeout=HONCHO_STOP_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        if honcho_group is not None:
            try:
                os.killpg(honcho_group, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
        try:
            process.wait(timeout=HONCHO_STOP_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            pass

    if snapshot_error:
        return False, snapshot_error

    # Termination is not instant, so give the groups a moment to drain.
    deadline = time.monotonic() + HONCHO_STOP_TIMEOUT_SECONDS
    survivors: list[int] = []
    while time.monotonic() < deadline:
        survivors = [
            group for group in sorted(groups) if not _process_group_is_empty(group)
        ]
        if not survivors:
            return True, ""
        time.sleep(0.5)

    return False, f"process groups still running after shutdown: {survivors}"


def _observe_topology(
    honcho: subprocess.Popen, group: str, topic: str
) -> tuple[bool, str]:
    """Watch a running topology settle and make progress."""
    deadline = time.monotonic() + TOPOLOGY_SETTLE_SECONDS
    members: set[str] = set()
    offsets: dict[int, int] = {}
    while time.monotonic() < deadline:
        if honcho.poll() is not None:
            return False, f"honcho exited early with code {honcho.returncode}"

        members_or_none, error = _group_members(group)
        if members_or_none is None:
            return False, error
        offsets_or_none, error = _group_offsets(group)
        if offsets_or_none is None:
            return False, error

        members, offsets = members_or_none, offsets_or_none
        if len(members) == KAFKA_PARTITIONS and len(offsets) == KAFKA_PARTITIONS:
            break
        time.sleep(2)
    else:
        return False, (
            f"topology did not settle: {len(members)} members owning "
            f"{len(offsets)} partitions, wanted {KAFKA_PARTITIONS} of each"
        )

    before = sum(offsets.values())
    time.sleep(TOPOLOGY_PROGRESS_SECONDS)

    later_members, error = _group_members(group)
    if later_members is None:
        return False, error
    if len(later_members) != KAFKA_PARTITIONS:
        return False, (
            f"group ended with {len(later_members)} members, "
            f"expected exactly {KAFKA_PARTITIONS}"
        )

    after_offsets, error = _group_offsets(group)
    if after_offsets is None:
        return False, error
    after = sum(after_offsets.values())
    if after <= before:
        return False, (
            f"committed offsets did not advance ({before} then {after}); "
            "the producer or the consumers are not doing their job"
        )

    return True, (
        f"{len(members)} consumers owned {len(offsets)} partitions of {topic}, "
        f"committed offsets advanced {before} to {after}"
    )


def honcho_topology_does_the_work() -> tuple[bool, str]:
    """Run the real Procfile topology and assert it processes events.

    Isolated deliberately: its own topic and its own consumer group, both handed
    to Honcho through the environment. Sharing `pageviews` and the `pipeline`
    group would let a topology someone left running satisfy this check, and a
    live honcho process is not evidence that the members observed are its own.
    """
    run_id = uuid.uuid4().hex[:8]
    topic = f"smoke_topology_{run_id}"
    group = f"smoke-topology-{run_id}"
    honcho: subprocess.Popen | None = None
    shutdown_error = ""

    # The functional result is recorded rather than returned, so cleanup always
    # runs and its outcome can take precedence over it.
    try:
        try:
            ensure_topic(topic, KAFKA_PARTITIONS, KAFKA_SERVER)
        except (KafkaError, OSError, RuntimeError) as error:
            observed = (False, f"create-topics path failed: {error}")
        else:
            environment = os.environ | {
                "KAFKA_TOPIC": topic,
                "CONSUMER_GROUP": group,
                "PRODUCER_INTERVAL_SECONDS": "0.05",
                # Commit every message, so offsets move within the budget.
                "COMMIT_EVERY": "1",
                # Never inject a crash into the topology under test.
                "CONSUMER_CRASH_AFTER": "",
            }
            try:
                honcho = subprocess.Popen(  # noqa: S603
                    # Not `uv run honcho`: that would place Honcho in a session
                    # of its own, outside the group this code signals.
                    [".venv/bin/honcho", "start"],
                    env=environment,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    # Its own process group, so the topology can be signalled.
                    start_new_session=True,
                )
            except FileNotFoundError:
                observed = (
                    False,
                    ".venv/bin/honcho is missing; run `uv sync --all-extras`",
                )
            else:
                observed = _observe_topology(honcho, group, topic)
    finally:
        if honcho is not None:
            stopped, shutdown_error = _stop_topology(honcho)
            if not stopped and not shutdown_error:
                shutdown_error = "the topology did not stop"
        _delete_topic(topic)

    # Shutdown outranks the observation: a check that leaves the topology
    # running, or cannot tell whether it did, has not finished its job.
    if shutdown_error:
        return False, f"shutdown incomplete: {shutdown_error}"

    passed, detail = observed
    if passed:
        return True, f"{detail}, and nothing was left running"
    return passed, detail


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
