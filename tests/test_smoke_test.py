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


PS_TABLE = """
  100     1   100
  200   100   200
  300   200   300
  400     1   400
"""


def test_topology_groups_collects_every_group_below_the_parent(monkeypatch):
    """Honcho's own group is not enough: each child has a group of its own."""
    monkeypatch.setattr(smoke_test.subprocess, "run", _run_returning(stdout=PS_TABLE))

    groups, error = smoke_test._topology_groups(100)

    assert groups == {100, 200, 300}
    assert error == ""


def test_topology_groups_excludes_unrelated_processes(monkeypatch):
    monkeypatch.setattr(smoke_test.subprocess, "run", _run_returning(stdout=PS_TABLE))

    groups, _ = smoke_test._topology_groups(100)

    assert 400 not in groups


def test_topology_groups_deduplicates_a_shared_group(monkeypatch):
    shared = """
  100     1   100
  200   100   100
  300   100   100
"""
    monkeypatch.setattr(smoke_test.subprocess, "run", _run_returning(stdout=shared))

    groups, _ = smoke_test._topology_groups(100)

    assert groups == {100}


@pytest.mark.parametrize(
    ("failure", "expected"),
    [
        (FileNotFoundError(), "ps is not available"),
        (subprocess.TimeoutExpired(cmd="ps", timeout=1), "timed out"),
    ],
)
def test_topology_groups_reports_a_failed_snapshot(monkeypatch, failure, expected):
    """An empty snapshot must never be mistaken for a clean shutdown."""

    def fail(*args, **kwargs):
        raise failure

    monkeypatch.setattr(smoke_test.subprocess, "run", fail)

    groups, error = smoke_test._topology_groups(100)

    assert groups == set()
    assert expected in error


def test_topology_groups_reports_an_absent_root(monkeypatch):
    """Valid ps output that does not contain Honcho is not an empty topology.

    If Honcho exits between observation and snapshot, its children may still be
    running with the link to them gone.
    """
    monkeypatch.setattr(smoke_test.subprocess, "run", _run_returning(stdout=PS_TABLE))

    groups, error = smoke_test._topology_groups(999)

    assert groups == set()
    assert "999 was absent" in error


def test_topology_groups_reports_a_non_zero_ps_exit(monkeypatch):
    monkeypatch.setattr(
        smoke_test.subprocess,
        "run",
        _run_returning(returncode=1, stderr="ps exploded"),
    )

    groups, error = smoke_test._topology_groups(100)

    assert groups == set()
    assert "ps exploded" in error


def test_process_group_is_empty_when_the_group_is_gone(monkeypatch):
    def gone(pgid, sig):
        raise ProcessLookupError

    monkeypatch.setattr(smoke_test.os, "killpg", gone)

    assert smoke_test._process_group_is_empty(999) is True


def test_process_group_is_not_empty_while_processes_remain(monkeypatch):
    monkeypatch.setattr(smoke_test.os, "killpg", lambda pgid, sig: None)

    assert smoke_test._process_group_is_empty(999) is False


def test_process_group_permission_denied_means_still_there(monkeypatch):
    """Cannot signal it, but something is holding the group: not empty."""

    def denied(pgid, sig):
        raise PermissionError

    monkeypatch.setattr(smoke_test.os, "killpg", denied)

    assert smoke_test._process_group_is_empty(999) is False


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
    monkeypatch.setattr(smoke_test.subprocess, "Popen", lambda *a, **k: FakeHoncho())
    monkeypatch.setattr(smoke_test, "_stop_topology", lambda process: stop_result)


def test_a_failed_shutdown_outranks_a_passing_observation(monkeypatch):
    _topology_scaffold(monkeypatch, stop_result=(False, ""))
    monkeypatch.setattr(smoke_test, "_observe_topology", lambda *a: (True, "all good"))

    passed, detail = smoke_test.honcho_topology_does_the_work()

    assert passed is False
    assert "did not stop" in detail


def test_a_failed_shutdown_is_reported_even_when_the_observation_failed(monkeypatch):
    """An early failure return must not hide that processes were left behind."""
    _topology_scaffold(monkeypatch, stop_result=(False, "group 42 still had processes"))
    monkeypatch.setattr(
        smoke_test, "_observe_topology", lambda *a: (False, "did not settle")
    )

    passed, detail = smoke_test.honcho_topology_does_the_work()

    assert passed is False
    assert "shutdown incomplete" in detail


def test_unverifiable_shutdown_fails_even_when_everything_else_passed(monkeypatch):
    _topology_scaffold(monkeypatch, stop_result=(False, "group 42 still had processes"))
    monkeypatch.setattr(smoke_test, "_observe_topology", lambda *a: (True, "all good"))

    passed, detail = smoke_test.honcho_topology_does_the_work()

    assert passed is False
    assert "shutdown incomplete" in detail


def test_a_clean_run_says_nothing_was_left_running(monkeypatch):
    _topology_scaffold(monkeypatch, stop_result=(True, ""))
    monkeypatch.setattr(
        smoke_test, "_observe_topology", lambda *a: (True, "4 consumers owned 4")
    )

    passed, detail = smoke_test.honcho_topology_does_the_work()

    assert passed is True
    assert "nothing was left running" in detail


def test_the_topic_is_deleted_even_when_honcho_cannot_start(monkeypatch):
    deleted = []
    monkeypatch.setattr(smoke_test, "ensure_topic", lambda *a, **k: None)
    monkeypatch.setattr(smoke_test, "_delete_topic", deleted.append)

    def no_honcho(*args, **kwargs):
        raise FileNotFoundError

    monkeypatch.setattr(smoke_test.subprocess, "Popen", no_honcho)

    passed, detail = smoke_test.honcho_topology_does_the_work()

    assert passed is False
    assert "honcho is missing" in detail
    assert len(deleted) == 1


def test_the_topic_is_deleted_when_it_cannot_be_created(monkeypatch):
    deleted = []

    def cannot_create(*args, **kwargs):
        raise RuntimeError("too many partitions")

    monkeypatch.setattr(smoke_test, "ensure_topic", cannot_create)
    monkeypatch.setattr(smoke_test, "_delete_topic", deleted.append)

    passed, detail = smoke_test.honcho_topology_does_the_work()

    assert passed is False
    assert "create-topics path failed" in detail
    assert len(deleted) == 1


class StoppableHoncho(FakeHoncho):
    def __init__(self):
        super().__init__(returncode=0)
        # Matches the root pid in PS_TABLE, so the snapshot actually finds it.
        self.pid = 100
        self.waits = 0

    def wait(self, timeout=None):
        self.waits += 1
        return 0


def _stop_scaffold(monkeypatch, table_stdout, empty_groups):
    # Real-time deadline, so shrink it rather than spinning for 30 seconds.
    monkeypatch.setattr(smoke_test, "HONCHO_STOP_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(
        smoke_test.subprocess, "run", _run_returning(stdout=table_stdout)
    )
    monkeypatch.setattr(smoke_test.os, "getpgid", lambda pid: 100)
    monkeypatch.setattr(smoke_test.os, "killpg", lambda pgid, sig: None)
    monkeypatch.setattr(
        smoke_test, "_process_group_is_empty", lambda pgid: pgid in empty_groups
    )
    monkeypatch.setattr(smoke_test.time, "sleep", lambda seconds: None)


def test_stop_topology_passes_when_every_group_is_gone(monkeypatch):
    _stop_scaffold(monkeypatch, PS_TABLE, empty_groups={100, 200, 300})

    stopped, error = smoke_test._stop_topology(StoppableHoncho())

    assert stopped is True
    assert error == ""


def test_stop_topology_fails_when_a_worker_group_survives(monkeypatch):
    """The regression this guards: Honcho exits, its group empties, work goes on."""
    _stop_scaffold(monkeypatch, PS_TABLE, empty_groups={100, 200})

    stopped, error = smoke_test._stop_topology(StoppableHoncho())

    assert stopped is False
    assert "still running after shutdown: [300]" in error


def test_stop_topology_reaps_honcho(monkeypatch):
    """An unreaped zombie would keep its own group looking occupied."""
    _stop_scaffold(monkeypatch, PS_TABLE, empty_groups={100, 200, 300})
    honcho = StoppableHoncho()

    smoke_test._stop_topology(honcho)

    assert honcho.waits >= 1


def test_stop_topology_still_shuts_down_when_the_snapshot_fails(monkeypatch):
    """Shutdown proceeds; the assertion fails afterwards."""
    signalled = []

    def no_ps(*args, **kwargs):
        raise FileNotFoundError

    monkeypatch.setattr(smoke_test, "HONCHO_STOP_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(smoke_test.subprocess, "run", no_ps)
    monkeypatch.setattr(smoke_test.os, "getpgid", lambda pid: 100)
    monkeypatch.setattr(
        smoke_test.os, "killpg", lambda pgid, sig: signalled.append((pgid, sig))
    )
    honcho = StoppableHoncho()

    stopped, error = smoke_test._stop_topology(honcho)

    assert stopped is False
    assert "ps is not available" in error
    assert signalled, "honcho should still have been signalled"
    assert honcho.waits >= 1


def test_stop_topology_never_signals_a_captured_group(monkeypatch):
    """Captured groups are observed, never signalled, so PGID reuse is safe."""
    signalled = []
    monkeypatch.setattr(smoke_test.subprocess, "run", _run_returning(stdout=PS_TABLE))
    monkeypatch.setattr(smoke_test.os, "getpgid", lambda pid: 100)
    monkeypatch.setattr(
        smoke_test.os, "killpg", lambda pgid, sig: signalled.append((pgid, sig))
    )
    monkeypatch.setattr(smoke_test, "_process_group_is_empty", lambda pgid: True)
    monkeypatch.setattr(smoke_test.time, "sleep", lambda seconds: None)

    smoke_test._stop_topology(StoppableHoncho())

    # Only Honcho's own group (100) is ever signalled; 200 and 300 are not.
    assert {pgid for pgid, _ in signalled} == {100}
