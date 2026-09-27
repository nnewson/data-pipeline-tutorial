import subprocess

import pytest
from kafka.errors import KafkaConnectionError, KafkaError

from pipeline import smoke_test


class FakeFuture:
    def __init__(self, error=None):
        self._error = error

    def get(self, timeout=None):
        if self._error:
            raise self._error
        return object()


class FakeProducer:
    def __init__(self, error=None):
        self._error = error
        self.closed = False

    def send(self, topic, value):
        return FakeFuture(self._error)

    def close(self):
        self.closed = True


class FakeCompleted:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def test_host_check_passes_when_the_event_returns(monkeypatch):
    monkeypatch.setattr(smoke_test, "KafkaProducer", lambda **kwargs: FakeProducer())
    monkeypatch.setattr(smoke_test, "_consume_until", lambda marker: {"marker": marker})

    passed, detail = smoke_test.host_listener_round_trips()

    assert passed is True
    assert "localhost:9092" in detail


def test_host_check_reports_a_broker_that_is_not_there(monkeypatch):
    def refuse(**kwargs):
        raise KafkaConnectionError("no brokers")

    monkeypatch.setattr(smoke_test, "KafkaProducer", refuse)

    passed, detail = smoke_test.host_listener_round_trips()

    assert passed is False
    assert "could not connect" in detail


def test_host_check_reports_a_failed_delivery(monkeypatch):
    # The regression this guards: flush() alone would not surface this.
    monkeypatch.setattr(
        smoke_test,
        "KafkaProducer",
        lambda **kwargs: FakeProducer(error=KafkaError("no leader")),
    )

    passed, detail = smoke_test.host_listener_round_trips()

    assert passed is False
    assert "produce to" in detail


def test_host_check_reports_an_event_that_never_arrives(monkeypatch):
    monkeypatch.setattr(smoke_test, "KafkaProducer", lambda **kwargs: FakeProducer())
    monkeypatch.setattr(smoke_test, "_consume_until", lambda marker: None)

    passed, detail = smoke_test.host_listener_round_trips()

    assert passed is False
    assert "did not come back" in detail


def test_consume_until_returns_none_when_the_broker_is_lost(monkeypatch):
    """A broker lost before consuming is a failed check, not a traceback."""

    def lose_broker(*args, **kwargs):
        raise KafkaConnectionError("broker lost")

    monkeypatch.setattr(smoke_test, "KafkaConsumer", lose_broker)

    assert smoke_test._consume_until("any-marker") is None


def test_consume_until_returns_none_when_reading_stops(monkeypatch):
    """A broker lost mid-read is handled the same way."""

    class FailingConsumer:
        def partitions_for_topic(self, topic):
            return {0}

        def __iter__(self):
            raise KafkaConnectionError("broker lost mid-read")

        def close(self):
            pass

    monkeypatch.setattr(smoke_test, "KafkaConsumer", lambda *a, **k: FailingConsumer())

    assert smoke_test._consume_until("any-marker") is None


def test_host_check_survives_a_broker_lost_before_consuming(monkeypatch):
    """The named failure surfaces rather than an exception escaping main()."""
    monkeypatch.setattr(smoke_test, "KafkaProducer", lambda **kwargs: FakeProducer())

    def lose_broker(*args, **kwargs):
        raise KafkaConnectionError("broker lost")

    monkeypatch.setattr(smoke_test, "KafkaConsumer", lose_broker)

    passed, detail = smoke_test.host_listener_round_trips()

    assert passed is False
    assert "did not come back" in detail


def test_internal_check_passes(monkeypatch):
    monkeypatch.setattr(smoke_test.subprocess, "run", lambda *a, **k: FakeCompleted())
    monkeypatch.setattr(smoke_test, "_consume_until", lambda marker: {"marker": marker})

    passed, detail = smoke_test.internal_listener_reaches_the_same_broker()

    assert passed is True
    assert "same broker" in detail


def test_internal_check_reports_a_non_zero_exit(monkeypatch):
    monkeypatch.setattr(
        smoke_test.subprocess,
        "run",
        lambda *a, **k: FakeCompleted(returncode=1, stderr="broker unreachable"),
    )

    passed, detail = smoke_test.internal_listener_reaches_the_same_broker()

    assert passed is False
    assert "broker unreachable" in detail


def test_internal_check_reports_a_hang(monkeypatch):
    def hang(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd="docker", timeout=1)

    monkeypatch.setattr(smoke_test.subprocess, "run", hang)

    passed, detail = smoke_test.internal_listener_reaches_the_same_broker()

    assert passed is False
    assert "did not finish in time" in detail


def test_internal_check_reports_missing_docker(monkeypatch):
    def missing(*args, **kwargs):
        raise FileNotFoundError

    monkeypatch.setattr(smoke_test.subprocess, "run", missing)

    passed, detail = smoke_test.internal_listener_reaches_the_same_broker()

    assert passed is False
    assert "docker" in detail


def test_internal_check_reports_an_unreadable_event(monkeypatch):
    """Produced inside the network, not readable from the host: listeners disagree."""
    monkeypatch.setattr(smoke_test.subprocess, "run", lambda *a, **k: FakeCompleted())
    monkeypatch.setattr(smoke_test, "_consume_until", lambda marker: None)

    passed, detail = smoke_test.internal_listener_reaches_the_same_broker()

    assert passed is False
    assert "not readable" in detail


def test_main_returns_zero_when_every_check_passes(monkeypatch, capsys):
    monkeypatch.setattr(smoke_test, "prepare", lambda: None)
    monkeypatch.setattr(smoke_test, "CHECKS", [("one", lambda: (True, "fine"))])

    assert smoke_test.main() == 0
    assert "all 1 checks passed" in capsys.readouterr().out


def test_main_aggregates_failures_and_returns_one(monkeypatch, capsys):
    monkeypatch.setattr(smoke_test, "prepare", lambda: None)
    monkeypatch.setattr(
        smoke_test,
        "CHECKS",
        [
            ("one", lambda: (True, "fine")),
            ("two", lambda: (False, "broken")),
            ("three", lambda: (False, "also broken")),
        ],
    )

    assert smoke_test.main() == 1
    captured = capsys.readouterr()
    assert "FAIL  two: broken" in captured.out
    assert "2 of 3 checks failed" in captured.err


EXPECTED_CHECKS = [
    "host listener",
    "internal listener",
    "partition routing",
    "honcho topology",
]


@pytest.mark.parametrize("name", EXPECTED_CHECKS)
def test_every_check_is_registered(name):
    assert name in [label for label, _ in smoke_test.CHECKS]


def test_no_check_is_silently_dropped():
    # Registering by name alone would not notice a check being removed.
    assert [label for label, _ in smoke_test.CHECKS] == EXPECTED_CHECKS


def test_main_reports_a_missing_broker_without_a_traceback(monkeypatch, capsys):
    """No broker at all is a failed run, not an exception escaping the CLI."""

    def no_broker(*args, **kwargs):
        raise KafkaConnectionError("no brokers available")

    monkeypatch.setattr(smoke_test, "prepare", no_broker)

    assert smoke_test.main() == 1
    captured = capsys.readouterr()
    assert "FAIL  setup" in captured.out
    assert "1 of 1 checks failed" in captured.err


def test_main_reports_an_over_wide_topic_without_a_traceback(monkeypatch, capsys):
    """ensure_topic raises RuntimeError for a topic with too many partitions."""

    def too_many(*args, **kwargs):
        raise RuntimeError("Topic smoke_test has 8 partitions, more than the 4")

    monkeypatch.setattr(smoke_test, "prepare", too_many)

    assert smoke_test.main() == 1
    assert "FAIL  setup" in capsys.readouterr().out


DESCRIBE_OFFSETS = """
GROUP    TOPIC       PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID  HOST  CLIENT-ID
grp      topic-x     0          10              10              0    c-1          /h    cli
grp      topic-x     1          20              25              5    c-2          /h    cli
"""

DESCRIBE_MEMBERS = """
GROUP    CONSUMER-ID  HOST  CLIENT-ID  #PARTITIONS
grp      c-1          /h    cli        1
grp      c-2          /h    cli        1
grp      c-3          /h    cli        0
"""


def _run_returning(stdout="", returncode=0, stderr=""):
    return lambda *a, **k: FakeCompleted(
        returncode=returncode, stdout=stdout, stderr=stderr
    )


def test_group_offsets_parses_committed_offsets(monkeypatch):
    monkeypatch.setattr(
        smoke_test.subprocess, "run", _run_returning(stdout=DESCRIBE_OFFSETS)
    )

    offsets, error = smoke_test._group_offsets("grp")

    assert offsets == {0: 10, 1: 20}
    assert error == ""


def test_group_members_counts_idle_members(monkeypatch):
    """--members --verbose shows a consumer holding no partition; --describe does not."""
    monkeypatch.setattr(
        smoke_test.subprocess, "run", _run_returning(stdout=DESCRIBE_MEMBERS)
    )

    members, error = smoke_test._group_members("grp")

    assert members == {"c-1", "c-2", "c-3"}
    assert error == ""


def test_group_command_reports_a_non_zero_exit(monkeypatch):
    monkeypatch.setattr(
        smoke_test.subprocess,
        "run",
        _run_returning(returncode=1, stderr="broker unreachable"),
    )

    output, error = smoke_test._group_command("grp", [])

    assert output is None
    assert "broker unreachable" in error


def test_group_command_reports_a_timeout(monkeypatch):
    def hang(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd="docker", timeout=1)

    monkeypatch.setattr(smoke_test.subprocess, "run", hang)

    output, error = smoke_test._group_command("grp", [])

    assert output is None
    assert "timed out" in error


def test_group_command_reports_missing_docker(monkeypatch):
    def missing(*args, **kwargs):
        raise FileNotFoundError

    monkeypatch.setattr(smoke_test.subprocess, "run", missing)

    output, error = smoke_test._group_command("grp", [])

    assert output is None
    assert "docker" in error


@pytest.mark.parametrize(
    "reader", [smoke_test._group_members, smoke_test._group_offsets]
)
def test_group_readers_propagate_failure_rather_than_raising(monkeypatch, reader):
    def missing(*args, **kwargs):
        raise FileNotFoundError

    monkeypatch.setattr(smoke_test.subprocess, "run", missing)

    value, error = reader("grp")

    assert value is None
    assert error


class FakeHoncho:
    """Stands in for a running Honcho process."""

    def __init__(self, returncode=None):
        self.pid = 4242
        self._returncode = returncode

    def poll(self):
        return self._returncode

    @property
    def returncode(self):
        return self._returncode

    def wait(self, timeout=None):
        return self._returncode


def _topology_scaffold(monkeypatch, stop_result):
    """Patch everything the topology check touches except the part under test."""
    monkeypatch.setattr(smoke_test, "ensure_topic", lambda *a, **k: None)
    monkeypatch.setattr(smoke_test, "_delete_topic", lambda name: None)
    monkeypatch.setattr(smoke_test, "_clear_redis_prefix", lambda prefix: None)
    monkeypatch.setattr(smoke_test, "_create_keyspace", lambda keyspace: "")
    monkeypatch.setattr(smoke_test, "_drop_keyspace", lambda keyspace: None)
    monkeypatch.setattr(smoke_test, "_delete_queue", lambda queue: None)
    monkeypatch.setattr(smoke_test.subprocess, "Popen", lambda *a, **k: FakeHoncho())
    monkeypatch.setattr(smoke_test, "_stop_topology", lambda process: stop_result)


def _state(
    leader_epoch=2,
    workers=4,
    consumers=4,
    coordinators=3,
    contenders=3,
    snapshot_epoch=2,
    version=5,
    leader="host-1",
):
    return {
        "leader": {"identity": leader, "epoch": leader_epoch} if leader else None,
        "contenders": contenders,
        "workers": [{"identity": f"w{n}"} for n in range(workers)],
        "consumers": [{"identity": f"c{n}"} for n in range(consumers)],
        "coordinators": [{"identity": "host-1"}]
        + [{"identity": f"co{n}"} for n in range(coordinators - 1)],
        "snapshot": {"epoch": snapshot_epoch},
        "snapshot_version": version,
    }


def _zookeeper_predicate(monkeypatch, state, first=None):
    monkeypatch.setattr(smoke_test, "_coordination_state", lambda root: (state, ""))
    predicates = smoke_test._readiness(
        "grp", "p:", "ks", "q", "/root", first if first is not None else {}
    )
    return dict(predicates)["zookeeper"]


def test_readiness_requires_an_acknowledged_leader(monkeypatch):
    """Contenders are candidates, not leaders; a contender count proves nothing."""
    ready, detail = _zookeeper_predicate(monkeypatch, _state(leader=None))()

    assert ready is False
    assert "no acknowledged leader among 3 contenders" in detail


def test_readiness_requires_every_worker_registered(monkeypatch):
    ready, detail = _zookeeper_predicate(monkeypatch, _state(workers=2))()

    assert ready is False
    assert "2 worker registrations, expected 4" in detail


def test_readiness_requires_the_snapshot_to_match_the_leaders_epoch(monkeypatch):
    """A snapshot from a previous epoch is a stale leader's work."""
    ready, detail = _zookeeper_predicate(
        monkeypatch, _state(leader_epoch=3, snapshot_epoch=2)
    )()

    assert ready is False
    assert "snapshot epoch 2 does not match leader epoch 3" in detail


def test_readiness_passes_with_one_leader_and_a_matching_snapshot(monkeypatch):
    ready, detail = _zookeeper_predicate(monkeypatch, _state())()

    assert ready is True
    assert "one leader at epoch 2" in detail


def test_readiness_reports_a_zookeeper_failure(monkeypatch):
    monkeypatch.setattr(
        smoke_test, "_coordination_state", lambda root: (None, "could not connect")
    )
    predicate = dict(smoke_test._readiness("grp", "p:", "ks", "q", "/root", {}))[
        "zookeeper"
    ]

    ready, detail = predicate()

    assert ready is False
    assert "could not connect" in detail


def test_a_second_snapshot_must_actually_be_new(monkeypatch):
    """Reading the same znode twice is not two snapshots."""
    first = {"version": 5, "leader": {"identity": "host-1", "epoch": 2}}
    monkeypatch.setattr(smoke_test.time, "sleep", lambda s: None)
    monkeypatch.setattr(
        smoke_test, "_coordination_state", lambda root: (_state(version=5), "")
    )
    monkeypatch.setattr(smoke_test, "TOPOLOGY_PROGRESS_SECONDS", 0.05)

    ready, detail = smoke_test._leader_is_working("/root", first)

    assert ready is False
    assert "did not write a second snapshot" in detail


def test_a_new_snapshot_from_the_same_leader_and_epoch_passes(monkeypatch):
    first = {"version": 5, "leader": {"identity": "host-1", "epoch": 2}}
    monkeypatch.setattr(smoke_test.time, "sleep", lambda s: None)
    monkeypatch.setattr(
        smoke_test, "_coordination_state", lambda root: (_state(version=6), "")
    )

    ready, detail = smoke_test._leader_is_working("/root", first)

    assert ready is True
    assert "v5 -> v6" in detail


def test_a_new_snapshot_from_a_different_leader_does_not_count(monkeypatch):
    """Leadership changing mid-check is not the same leader working."""
    first = {"version": 5, "leader": {"identity": "host-1", "epoch": 2}}
    state = _state(version=6)
    state["leader"] = {"identity": "host-2", "epoch": 3}
    monkeypatch.setattr(smoke_test.time, "sleep", lambda s: None)
    monkeypatch.setattr(smoke_test, "_coordination_state", lambda root: (state, ""))
    monkeypatch.setattr(smoke_test, "TOPOLOGY_PROGRESS_SECONDS", 0.05)

    ready, _ = smoke_test._leader_is_working("/root", first)

    assert ready is False


class FakeTopology:
    """Stands in for the extracted runner."""

    def __init__(self, ready=(True, "all held"), stop=(True, "")):
        self._ready = ready
        self._stop = stop
        self.stopped = False

    def start(self):
        return ""

    def died(self):
        return ""

    def wait_until_ready(self, predicates, timeout):
        return self._ready

    def stop(self):
        self.stopped = True
        return self._stop


def _topology_scaffold(monkeypatch, topology):
    monkeypatch.setattr(smoke_test, "_prepare_topology", lambda *a: "")
    monkeypatch.setattr(smoke_test, "Topology", lambda env: topology)
    monkeypatch.setattr(smoke_test, "_leader_is_working", lambda *a: (True, "led"))
    monkeypatch.setattr(
        smoke_test, "_live_config_reaches_workers", lambda root: (True, "config")
    )
    for name in (
        "_clear_redis_prefix",
        "_drop_keyspace",
        "_delete_queue",
        "_delete_zookeeper_root",
        "_delete_topic",
    ):
        monkeypatch.setattr(smoke_test, name, lambda *a, **k: None)


def test_a_failed_shutdown_outranks_a_passing_observation(monkeypatch):
    _topology_scaffold(monkeypatch, FakeTopology(stop=(False, "")))

    passed, detail = smoke_test.honcho_topology_does_the_work()

    assert passed is False
    assert "did not stop" in detail


def test_a_failed_shutdown_is_reported_even_when_readiness_failed(monkeypatch):
    """An early failure must not hide that processes were left behind."""
    _topology_scaffold(
        monkeypatch,
        FakeTopology(ready=(False, "zookeeper: no leader"), stop=(False, "group 42")),
    )

    passed, detail = smoke_test.honcho_topology_does_the_work()

    assert passed is False
    assert "shutdown incomplete" in detail


def test_a_clean_run_says_nothing_was_left_running(monkeypatch):
    _topology_scaffold(monkeypatch, FakeTopology())

    passed, detail = smoke_test.honcho_topology_does_the_work()

    assert passed is True
    assert "nothing was left running" in detail


def test_setup_failure_still_cleans_up(monkeypatch):
    cleaned = []
    monkeypatch.setattr(
        smoke_test, "_prepare_topology", lambda *a: "cluster init path failed"
    )
    monkeypatch.setattr(smoke_test, "Topology", lambda env: FakeTopology())
    for name in (
        "_clear_redis_prefix",
        "_drop_keyspace",
        "_delete_queue",
        "_delete_zookeeper_root",
        "_delete_topic",
    ):
        monkeypatch.setattr(smoke_test, name, lambda *a, **k: cleaned.append(1))

    passed, detail = smoke_test.honcho_topology_does_the_work()

    assert passed is False
    assert "cluster init path failed" in detail
    assert len(cleaned) == 5


def test_the_topology_gets_isolated_state(monkeypatch):
    """Its own topic, group, prefix, keyspace, queue and ZooKeeper root."""
    captured = {}
    monkeypatch.setattr(smoke_test, "_prepare_topology", lambda *a: "")
    monkeypatch.setattr(smoke_test, "_leader_is_working", lambda *a: (True, "led"))
    monkeypatch.setattr(
        smoke_test, "_live_config_reaches_workers", lambda root: (True, "config")
    )
    monkeypatch.setattr(
        smoke_test, "Topology", lambda env: (captured.update(env), FakeTopology())[1]
    )
    for name in (
        "_clear_redis_prefix",
        "_drop_keyspace",
        "_delete_queue",
        "_delete_zookeeper_root",
        "_delete_topic",
    ):
        monkeypatch.setattr(smoke_test, name, lambda *a, **k: None)

    smoke_test.honcho_topology_does_the_work()

    assert captured["KAFKA_TOPIC"].startswith("smoke_topology_")
    assert captured["CONSUMER_GROUP"].startswith("smoke-topology-")
    assert captured["REDIS_KEY_PREFIX"].startswith("smoke:")
    assert captured["CASSANDRA_KEYSPACE"].startswith("smoke_")
    assert captured["RABBITMQ_QUEUE"].startswith("smoke_jobs_")
    assert captured["ZOOKEEPER_ROOT"].startswith("/smoke_")


def test_everything_is_cleaned_up_when_honcho_cannot_start(monkeypatch):
    """A failure to launch must still remove what setup created."""
    cleaned = []

    class WontStart(FakeTopology):
        def start(self):
            return ".venv/bin/honcho is missing"

    monkeypatch.setattr(smoke_test, "_prepare_topology", lambda *a: "")
    monkeypatch.setattr(smoke_test, "Topology", lambda env: WontStart())
    for name in (
        "_clear_redis_prefix",
        "_drop_keyspace",
        "_delete_queue",
        "_delete_zookeeper_root",
        "_delete_topic",
    ):
        monkeypatch.setattr(smoke_test, name, lambda *a, **k: cleaned.append(1))

    passed, detail = smoke_test.honcho_topology_does_the_work()

    assert passed is False
    assert "honcho is missing" in detail
    assert len(cleaned) == 5


def test_readiness_requires_every_consumer_registered(monkeypatch):
    """The previous version passed with zero consumers registered."""
    ready, detail = _zookeeper_predicate(monkeypatch, _state(consumers=0))()

    assert ready is False
    assert "0 consumer registrations, expected 4" in detail


def test_readiness_requires_every_coordinator_registered(monkeypatch):
    ready, detail = _zookeeper_predicate(monkeypatch, _state(coordinators=1))()

    assert ready is False
    assert "coordinator registrations" in detail


def test_readiness_requires_every_contender(monkeypatch):
    ready, detail = _zookeeper_predicate(monkeypatch, _state(contenders=1))()

    assert ready is False
    assert "1 contenders, expected 3" in detail


def test_the_leader_must_be_a_registered_coordinator(monkeypatch):
    """A marker written by a process that then vanished is not a leader."""
    ready, detail = _zookeeper_predicate(monkeypatch, _state(leader="ghost"))()

    assert ready is False
    assert "not among the registered coordinators" in detail


def test_live_config_requires_every_worker_to_report_the_new_version(monkeypatch):
    """Would pass on manual observation alone; must not pass if the path breaks."""

    class Client:
        def __init__(self):
            self.versions = {}

        def set(self, path, value):
            class Stat:
                version = 7

            return Stat()

        def stop(self):
            pass

        def close(self):
            pass

    monkeypatch.setattr(smoke_test.coordination, "connect", lambda: Client())
    monkeypatch.setattr(smoke_test.time, "sleep", lambda s: None)
    monkeypatch.setattr(smoke_test, "LIVE_CONFIG_TIMEOUT_SECONDS", 0.05)
    # Three of four applied it.
    monkeypatch.setattr(
        smoke_test.coordination,
        "registrations",
        lambda c, p, role: [
            {"config_version": 7, "delay": "0.03"},
            {"config_version": 7, "delay": "0.03"},
            {"config_version": 7, "delay": "0.03"},
            {"config_version": 1, "delay": "0.5"},
        ],
    )

    ready, detail = smoke_test._live_config_reaches_workers("/root")

    assert ready is False
    assert "did not all report config version 7" in detail


def test_live_config_passes_when_all_four_applied_it(monkeypatch):
    class Client:
        def set(self, path, value):
            class Stat:
                version = 7

            return Stat()

        def stop(self):
            pass

        def close(self):
            pass

    monkeypatch.setattr(smoke_test.coordination, "connect", lambda: Client())
    monkeypatch.setattr(smoke_test.time, "sleep", lambda s: None)
    monkeypatch.setattr(
        smoke_test.coordination,
        "registrations",
        lambda c, p, role: [{"config_version": 7, "delay": "0.03"}] * 4,
    )

    ready, detail = smoke_test._live_config_reaches_workers("/root")

    assert ready is True
    assert "all 4 workers applied worker_delay=0.03" in detail
