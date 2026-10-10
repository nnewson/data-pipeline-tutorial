import json
import subprocess
import time
from types import SimpleNamespace

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
    "flink windows",
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
    monkeypatch.setattr(
        smoke_test, "_a_known_event_is_notified", lambda *a: (True, "notified")
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
        smoke_test, "_a_known_event_is_notified", lambda *a: (True, "notified")
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
    assert int(captured["API_PORT"]) > 0


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


# --- the API predicate --------------------------------------------------------

SENTINELS = smoke_test.Sentinels.for_run("abc")


def _api_answers(**overrides):
    """What a working API answers, with any path's answer replaced."""
    s = SENTINELS
    answers = {
        "/health": (200, {"status": "ok"}),
        f"/counts/pages?page={s.page}": (200, {"counts": {s.page: s.count}}),
        f"/users/{s.user}/last-page": (200, {"page": s.page}),
        f"/users/nobody-{s.user}/last-page": (404, {"detail": "no last page"}),
        f"/users/{s.user}/events": (
            200,
            {"events": [{"event_id": s.event_id, "written_at": "2026-09-28T00:00Z"}]},
        ),
        f"/users/{s.user}/events?limit=101": (422, {"detail": []}),
        "/cluster": (
            200,
            {"leader": {"identity": "host-1"}, "snapshot_matches_leader": True},
        ),
        "/openapi.json": (200, {"paths": dict.fromkeys(smoke_test.API_ROUTES)}),
    }
    answers.update(overrides)
    return lambda port, path: (*answers[path], "")


def _api_check(monkeypatch, **overrides):
    monkeypatch.setattr(smoke_test, "_api_get", _api_answers(**overrides))
    return smoke_test._api_serves_the_stores(
        8123, SENTINELS, {"leader": {"identity": "host-1"}}
    )


def test_the_api_predicate_passes_when_every_sentinel_reads_back(monkeypatch):
    passed, detail = _api_check(monkeypatch)

    assert passed, detail


def test_the_api_predicate_requires_the_exact_sentinel_count(monkeypatch):
    passed, detail = _api_check(
        monkeypatch,
        **{
            f"/counts/pages?page={SENTINELS.page}": (
                200,
                {"counts": {SENTINELS.page: 8}},
            )
        },
    )

    assert not passed
    assert "wanted 7" in detail


def test_the_api_predicate_requires_a_404_for_an_unknown_user(monkeypatch):
    unknown = f"/users/nobody-{SENTINELS.user}/last-page"
    passed, detail = _api_check(monkeypatch, **{unknown: (200, {"page": "/"})})

    assert not passed
    assert "unknown user" in detail


def test_the_api_predicate_requires_the_bounded_limit(monkeypatch):
    over = f"/users/{SENTINELS.user}/events?limit=101"
    passed, detail = _api_check(monkeypatch, **{over: (200, {"events": []})})

    assert not passed
    assert "wanted 422" in detail


def test_the_api_predicate_waits_for_the_snapshot_to_match(monkeypatch):
    passed, detail = _api_check(
        monkeypatch,
        **{"/cluster": (200, {"leader": None, "snapshot_matches_leader": False})},
    )

    assert not passed
    assert "does not match" in detail


def test_the_api_predicate_requires_the_leader_zookeeper_reported(monkeypatch):
    passed, detail = _api_check(
        monkeypatch,
        **{
            "/cluster": (
                200,
                {"leader": {"identity": "impostor"}, "snapshot_matches_leader": True},
            )
        },
    )

    assert not passed
    assert "impostor" in detail


def test_the_api_predicate_reports_an_unreachable_api(monkeypatch):
    monkeypatch.setattr(
        smoke_test, "_api_get", lambda port, path: (None, None, "not answering")
    )

    passed, detail = smoke_test._api_serves_the_stores(8123, SENTINELS, {})

    assert not passed
    assert detail == "not answering"


def test_readiness_checks_the_api_last_when_given_one():
    names = [
        name
        for name, _ in smoke_test._readiness(
            "grp", "p:", "ks", "q", "/root", {}, api=(8123, SENTINELS)
        )
    ]

    assert names[-1] == "api"
    assert names[-2] == "zookeeper"


def _write_predicates(monkeypatch, counts, pages, rows):
    monkeypatch.setattr(smoke_test, "_redis_state", lambda prefix: (counts, pages, ""))
    monkeypatch.setattr(smoke_test, "_cassandra_rows", lambda keyspace: (rows, ""))
    predicates = dict(
        smoke_test._readiness(
            "grp", "p:", "ks", "q", "/root", {}, api=(8123, SENTINELS)
        )
    )
    return predicates["redis"], predicates["cassandra"]


def _row(user):
    return SimpleNamespace(user_id=user, event_time="t", event_id="e", page="/docs")


def test_sentinels_alone_do_not_pass_the_write_predicates(monkeypatch):
    """The consumers must have written something; the sentinels do not count."""
    redis_ready, cassandra_ready = _write_predicates(
        monkeypatch,
        counts={SENTINELS.page: SENTINELS.count},
        pages={SENTINELS.user: SENTINELS.page},
        rows=[_row(SENTINELS.user)],
    )

    assert redis_ready()[0] is False
    assert cassandra_ready()[0] is False


def test_consumer_writes_beside_the_sentinels_pass(monkeypatch):
    redis_ready, cassandra_ready = _write_predicates(
        monkeypatch,
        counts={SENTINELS.page: SENTINELS.count, "/docs": 4},
        pages={SENTINELS.user: SENTINELS.page, "ada": "/docs"},
        rows=[_row(SENTINELS.user), _row("ada")],
    )

    assert redis_ready() == (True, "1 counters, 1 last-page values")
    assert cassandra_ready()[0] is True


# --- the live notification check ----------------------------------------------


class FakeLiveSocket:
    def __init__(self, messages):
        self.messages = [json.dumps(m) for m in messages]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def recv(self, timeout=None):
        if not self.messages:
            raise TimeoutError
        return self.messages.pop(0)


class FakeKafkaProducer:
    sent: list = []

    def __init__(self, **kwargs):
        pass

    def send(self, topic, value, partition=None):
        FakeKafkaProducer.sent.append((topic, value, partition))
        return SimpleNamespace(get=lambda timeout: None)

    def close(self):
        pass


def _live(monkeypatch, messages, refused=True, routes_ok=True):
    FakeKafkaProducer.sent = []
    monkeypatch.setattr(smoke_test, "KafkaProducer", FakeKafkaProducer)
    monkeypatch.setattr(
        smoke_test, "websocket_connect", lambda *a, **k: FakeLiveSocket(list(messages))
    )
    monkeypatch.setattr(smoke_test, "_socket_refused", lambda port, origin: refused)
    page = (200, "text/html", '<script type="module">import "./live.mjs"</script>')
    module = (200, "text/javascript", "export function createTracker")
    monkeypatch.setattr(
        smoke_test,
        "_http_status",
        lambda port, path: (
            (page if path == "/live" else module) if routes_ok else (404, "", "")
        ),
    )
    return smoke_test._a_known_event_is_notified(8123, "topic_x", "abc")


NOTIFIED = {
    "type": "pageview",
    "event_id": "smoke-live-abc",
    "partition": 2,
    "offset": 17,
}


def test_the_known_event_is_produced_only_once_subscribed(monkeypatch):
    passed, detail = _live(
        monkeypatch, [{"type": "ready", "bridge": "subscribed"}, NOTIFIED]
    )

    assert passed, detail
    ((topic, value, partition),) = FakeKafkaProducer.sent
    assert topic == "topic_x" and value["event_id"] == "smoke-live-abc"


def test_a_ready_that_is_not_subscribed_waits_for_resubscription(monkeypatch):
    """Not merely any ready: producing now could be rightly dropped."""
    passed, _ = _live(
        monkeypatch,
        [
            {"type": "ready", "bridge": "interrupted"},
            {"type": "resubscribed", "detected_at": "t", "resubscribed_at": "t"},
            NOTIFIED,
        ],
    )

    assert passed


def test_an_interrupted_ready_alone_never_produces(monkeypatch):
    passed, detail = _live(monkeypatch, [{"type": "ready", "bridge": "interrupted"}])

    assert not passed
    assert FakeKafkaProducer.sent == []
    assert "within the bound" in detail


def test_other_events_do_not_count_for_the_known_one(monkeypatch):
    other = {**NOTIFIED, "event_id": "someone-else"}
    passed, detail = _live(
        monkeypatch, [{"type": "ready", "bridge": "subscribed"}, other, other]
    )

    assert not passed
    assert "smoke-live-abc" in detail


def test_an_accepted_foreign_origin_fails_the_check(monkeypatch):
    passed, detail = _live(
        monkeypatch,
        [{"type": "ready", "bridge": "subscribed"}, NOTIFIED],
        refused=False,
    )

    assert not passed
    assert "was accepted" in detail


def test_the_page_routes_are_part_of_the_check(monkeypatch):
    passed, detail = _live(
        monkeypatch,
        [{"type": "ready", "bridge": "subscribed"}, NOTIFIED],
        routes_ok=False,
    )

    assert not passed
    assert "/live answered 404" in detail


class EndlessTraffic(FakeLiveSocket):
    """Unrelated messages for ever; every receive moves the fake clock on."""

    def __init__(self, first, clock):
        super().__init__(first)
        self.clock = clock
        self.served = 0

    def recv(self, timeout=None):
        self.served += 1
        assert self.served < 500, "kept receiving long past the deadline"
        self.clock[0] += 1.0
        if self.messages:
            return self.messages.pop(0)
        return json.dumps({**NOTIFIED, "event_id": "someone-else"})


def _endless(monkeypatch, first):
    clock = [1000.0]
    socket = EndlessTraffic(first, clock)
    FakeKafkaProducer.sent = []
    monkeypatch.setattr(smoke_test, "KafkaProducer", FakeKafkaProducer)
    monkeypatch.setattr(smoke_test, "websocket_connect", lambda *a, **k: socket)
    result = smoke_test._a_known_event_is_notified(
        8123, "topic_x", "abc", clock=lambda: clock[0]
    )
    return result, clock[0] - 1000.0


def test_the_bound_holds_under_continuous_unrelated_traffic(monkeypatch):
    """Regression: the event was once accepted 101s into a 20s check."""
    (passed, detail), elapsed = _endless(
        monkeypatch, [{"type": "ready", "bridge": "subscribed"}]
    )

    assert not passed
    assert "within the bound" in detail
    assert elapsed <= smoke_test.LIVE_TIMEOUT_SECONDS + 1


def test_waiting_to_be_subscribed_is_bounded_too(monkeypatch):
    (passed, _), elapsed = _endless(
        monkeypatch, [{"type": "ready", "bridge": "interrupted"}]
    )

    assert not passed
    assert FakeKafkaProducer.sent == [], "never subscribed, so nothing produced"
    assert elapsed <= smoke_test.LIVE_TIMEOUT_SECONDS + 1


# --- the Flink round trip ------------------------------------------------------


def test_flink_events_cover_every_partition_and_advance_past_closure():
    events, expected = smoke_test._flink_events(window_start=1000)

    assert {partition for partition, _ in events} == {0, 1, 2, 3}
    counted = [e for _, e in events if e["page"] != "/advance"]
    assert all(1000 <= e["timestamp"] < 1010 for e in counted)
    assert expected == {"/docs": 12, "/pricing": 8}
    advance = [(p, e) for p, e in events if e["page"] == "/advance"]
    assert {p for p, _ in advance} == {0, 1, 2, 3}, "on every partition"
    # Beyond window end *plus* the watermark delay, not merely past the end.
    assert all(
        e["timestamp"] > 1010 + smoke_test.FLINK_WATERMARK_DELAY_SECONDS
        for _, e in advance
    )


class FakeFlink:
    def __init__(
        self,
        states=("RUNNING",),
        checkpoints=1,
        cancel_to="CANCELED",
        named=(),
        cancel_fails=False,
        listing_fails=False,
    ):
        self.states = list(states)
        self.checkpoints = checkpoints
        self.cancel_to = cancel_to
        self.cancelled: list[str] = []
        self.submitted: list = []
        self.named = list(named)  # jobs the cluster holds under the run's name
        self.looked_up: list[str] = []
        self.cancel_fails = cancel_fails
        self.listing_fails = listing_fails

    def jobs_named(self, name):
        self.looked_up.append(name)
        if self.listing_fails:
            raise smoke_test.flink_cluster.FlinkError("connection reset")
        return list(self.named)  # stopped jobs included, as the REST API lists them

    def submit(self, settings):
        self.submitted.append(settings)
        return "f" * 32

    def job_state(self, job_id):
        if self.cancelled:
            return self.cancel_to
        return self.states.pop(0) if len(self.states) > 1 else self.states[0]

    def completed_checkpoints(self, job_id):
        return self.checkpoints

    def cancel(self, job_id):
        if self.cancel_fails:
            raise smoke_test.flink_cluster.FlinkError("connection refused")
        self.cancelled.append(job_id)


class FakeWindowsConsumer:
    def __init__(self, records):
        self.records = records

    def __iter__(self):
        return iter(
            [SimpleNamespace(value=json.dumps(r).encode()) for r in self.records]
        )

    def close(self):
        pass


def _flink_check(
    monkeypatch, flink, records, clock=None, consumer=None, reconcile_seconds=0
):
    deleted = []
    monkeypatch.setattr(
        smoke_test,
        "flink_cluster",
        SimpleNamespace(
            submit=flink.submit,
            job_state=flink.job_state,
            cancel=flink.cancel,
            completed_checkpoints=flink.completed_checkpoints,
            jobs_named=flink.jobs_named,
            FlinkError=smoke_test.flink_cluster.FlinkError,
            SubmissionUncertain=smoke_test.flink_cluster.SubmissionUncertain,
            TERMINAL=smoke_test.flink_cluster.TERMINAL,
        ),
    )
    monkeypatch.setattr(smoke_test, "FLINK_RECONCILE_SECONDS", reconcile_seconds)
    monkeypatch.setattr(smoke_test.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(smoke_test, "ensure_topic", lambda *a, **k: None)
    monkeypatch.setattr(smoke_test, "_delete_topic", deleted.append)
    monkeypatch.setattr(
        smoke_test,
        "KafkaProducer",
        lambda **k: SimpleNamespace(
            send=lambda *a, **k: None, flush=lambda **k: None, close=lambda: None
        ),
    )
    monkeypatch.setattr(
        smoke_test, "_wait_for", lambda condition, deadline, clock=None: condition()
    )
    monkeypatch.setattr(smoke_test.time, "time", lambda: 1_000_000.0)
    window = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(1_000_000 - 600))
    rendered = [{**r, "window_start": r.get("window_start", window)} for r in records]
    monkeypatch.setattr(
        smoke_test,
        "KafkaConsumer",
        lambda *a, **k: (
            consumer if consumer is not None else FakeWindowsConsumer(rendered)
        ),
    )
    kwargs = {} if clock is None else {"clock": clock}
    return smoke_test.flink_windows_round_trip(**kwargs), deleted


EXACT = [{"page": "/docs", "views": 12}, {"page": "/pricing", "views": 8}]


def test_the_flink_check_passes_on_exact_counts_and_a_checkpoint(monkeypatch):
    flink = FakeFlink()
    (passed, detail), deleted = _flink_check(monkeypatch, flink, EXACT)

    assert passed, detail
    assert flink.cancelled == ["f" * 32], "cancelled by its captured id"
    assert len(deleted) == 2
    (settings,) = flink.submitted
    assert settings.idle_timeout_seconds == 0, "closure must not depend on idleness"


def test_the_flink_check_requires_exact_counts(monkeypatch):
    (passed, detail), _ = _flink_check(
        monkeypatch,
        FakeFlink(),
        [{"page": "/docs", "views": 11}],
        clock=iter(range(0, 10_000, 50)).__next__,
    )

    assert not passed
    assert "wanted" in detail


def test_the_flink_check_requires_a_completed_checkpoint(monkeypatch):
    """Right counts and RUNNING can both come before any checkpoint."""
    (passed, detail), _ = _flink_check(monkeypatch, FakeFlink(checkpoints=0), EXACT)

    assert not passed
    assert "no completed checkpoint" in detail


def test_a_job_that_will_not_cancel_fails_the_check(monkeypatch):
    (passed, detail), deleted = _flink_check(
        monkeypatch, FakeFlink(cancel_to="RUNNING"), EXACT
    )

    assert not passed
    assert "did not stop after being cancelled" in detail
    assert "the check itself passed" in detail, "both outcomes are reported"
    assert deleted == [], "not deleted underneath a job that may still run"


def _uncertain(settings):
    raise smoke_test.flink_cluster.SubmissionUncertain("timed out without a job id")


def test_an_uncertain_submission_with_no_job_found_keeps_the_topics(monkeypatch):
    """Regression: topics deleted at the window's end, the job landed a second later.

    Not finding a job proves nothing: the submitter in the container may still
    be running, however long the window was.
    """
    flink = FakeFlink()
    flink.submit = _uncertain
    ticks = iter(range(1_000_000))
    (passed, detail), deleted = _flink_check(
        monkeypatch, flink, EXACT, clock=lambda: next(ticks), reconcile_seconds=20
    )

    assert not passed
    assert "submission uncertain" in detail
    assert len(flink.looked_up) >= 20, "looked for the whole window"
    assert flink.looked_up[0].startswith("smoke-flink-")
    assert "may still land" in detail and "shutdown incomplete" in detail
    assert deleted == [], "kept for a submission that may still land"


def test_an_uncertain_submission_is_reconciled_by_the_runs_unique_name(monkeypatch):
    """Regression: an accepted-but-unreported job was left running, topics deleted."""
    flink = FakeFlink(named=["d" * 32])
    flink.submit = _uncertain
    (passed, detail), deleted = _flink_check(monkeypatch, flink, EXACT)

    assert not passed
    assert flink.cancelled == ["d" * 32], "found by the run's name, cancelled by id"
    assert len(deleted) == 2, "only once the shutdown was confirmed"


def test_inputs_are_kept_while_a_shutdown_is_unconfirmed(monkeypatch):
    flink = FakeFlink(named=["d" * 32], cancel_fails=True)
    flink.submit = _uncertain
    (passed, detail), deleted = _flink_check(monkeypatch, flink, EXACT)

    assert not passed
    assert deleted == [], "never delete the inputs underneath a job that may run"
    assert "submission uncertain" in detail and "shutdown incomplete" in detail


def test_a_readiness_failure_and_a_shutdown_failure_are_both_reported(monkeypatch):
    """Regression: an early return once hid the cancellation failure."""
    flink = FakeFlink(states=("CREATED",), cancel_fails=True)
    monkeypatch.setattr(
        smoke_test, "_wait_for", lambda condition, deadline, clock=None: False
    )
    (passed, detail), deleted = _flink_check(monkeypatch, flink, EXACT)

    assert not passed
    assert "never reached RUNNING" in detail
    assert "could not cancel job" in detail
    assert deleted == []


def test_reconciliation_keeps_looking_after_an_uncertain_submission(monkeypatch):
    """The in-container submission may land after the client gave up."""
    flink = FakeFlink()
    flink.submit = _uncertain
    lookups = []

    def jobs_named(name):
        lookups.append(name)
        return ["e" * 32] if len(lookups) >= 3 else []

    flink.jobs_named = jobs_named
    ticks = iter(range(1_000_000))
    (passed, detail), deleted = _flink_check(
        monkeypatch, flink, EXACT, clock=lambda: next(ticks), reconcile_seconds=10
    )

    assert len(lookups) >= 3, "looked again rather than once"
    assert flink.cancelled == ["e" * 32]


def test_a_known_job_is_stopped_even_when_the_name_lookup_fails(monkeypatch):
    """Regression: a failed listing returned before cancelling the captured id."""
    flink = FakeFlink(listing_fails=True)
    (passed, detail), deleted = _flink_check(monkeypatch, flink, EXACT)

    assert flink.cancelled == ["f" * 32], "the captured id is still cancelled"
    assert not passed
    assert "could not list jobs named smoke-flink-" in detail, "the error is kept"
    assert deleted == []


def test_a_job_that_already_failed_is_not_cancelled_and_its_topics_go(monkeypatch):
    """Flink refuses to cancel a stopped job; stopped is what cleanup needs."""
    flink = FakeFlink(states=("FAILED",))
    (passed, detail), deleted = _flink_check(monkeypatch, flink, EXACT)

    assert flink.cancelled == [], "no cancel for a job that already stopped"
    assert not passed
    assert "never reached RUNNING" in detail
    assert "had already stopped: FAILED" in detail
    assert "shutdown incomplete" not in detail
    assert len(deleted) == 2


def test_a_job_that_stops_while_being_cancelled_counts_as_stopped(monkeypatch):
    """The cancel is refused because the job got there first."""
    flink = FakeFlink(states=("RUNNING", "RUNNING", "FAILED"), cancel_fails=True)
    (passed, detail), deleted = _flink_check(monkeypatch, flink, EXACT)

    assert not passed, "a streaming job that stopped by itself is a failure"
    assert "the check itself passed" in detail
    assert "had already stopped: FAILED" in detail
    assert "shutdown incomplete" not in detail
    assert len(deleted) == 2


def test_a_cancelled_job_that_ends_failed_counts_as_stopped(monkeypatch):
    flink = FakeFlink(cancel_to="FAILED")
    (passed, detail), deleted = _flink_check(monkeypatch, flink, EXACT)

    assert passed, detail
    assert len(deleted) == 2


def test_an_unexpected_submission_error_is_reconciled_as_uncertain(monkeypatch):
    """Whatever escapes the submission may have left a job behind."""
    flink = FakeFlink(named=["d" * 32])

    def undecodable(settings):
        raise UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte")

    flink.submit = undecodable
    (passed, detail), deleted = _flink_check(monkeypatch, flink, EXACT)

    assert not passed
    assert flink.cancelled == ["d" * 32]
    assert len(deleted) == 2, "confirmed: the run's one job was found and stopped"


def test_a_submission_that_never_ran_needs_no_reconciliation(monkeypatch):
    flink = FakeFlink()

    def never_ran(settings):
        raise smoke_test.flink_cluster.FlinkError("could not run the submission")

    flink.submit = never_ran
    (passed, detail), deleted = _flink_check(
        monkeypatch, flink, EXACT, reconcile_seconds=20
    )

    assert not passed
    assert "could not run the submission" in detail
    assert flink.looked_up == [], "nothing was submitted, so nothing to find"
    assert len(deleted) == 2


class EndlessWindows(FakeWindowsConsumer):
    """Unrelated results for ever — a job that never stops writing."""

    def __iter__(self):
        for served in range(1_000_000):
            # Fail fast rather than hang if the loop stops consulting the clock.
            assert served < 5_000, "kept reading long past the deadline"
            yield SimpleNamespace(
                value=json.dumps(
                    {
                        "window_start": "1970-01-01T00:00:00Z",
                        "page": "/other",
                        "views": 1,
                    }
                ).encode()
            )


def test_the_flink_bound_holds_while_output_keeps_arriving(monkeypatch):
    """0.9's lesson: a bound that waits for silence never expires under traffic."""
    calls = [0]

    def clock():
        calls[0] += 1
        assert calls[0] < 10_000, "kept reading long past the deadline"
        return calls[0] * 0.1

    (passed, detail), _ = _flink_check(
        monkeypatch, FakeFlink(), [], clock=clock, consumer=EndlessWindows([])
    )

    assert not passed
    assert "wanted" in detail
