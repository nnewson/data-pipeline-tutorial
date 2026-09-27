"""The extracted topology runner: process mechanics and readiness.

These moved here at 0.7 with the code they exercise. The mechanics themselves
are unchanged from 0.3-0.6 — measured, hardened, and deliberately not reopened.
"""

import subprocess

import pytest

from pipeline import topology_runner


def _run_returning(stdout="", returncode=0, stderr=""):
    class Completed:
        pass

    completed = Completed()
    completed.stdout = stdout
    completed.returncode = returncode
    completed.stderr = stderr
    return lambda *a, **k: completed


PS_TABLE = """
  100     1   100
  200   100   200
  300   200   300
  400     1   400
"""


def test_topology_groups_collects_every_group_below_the_parent(monkeypatch):
    """Honcho's own group is not enough: each child has a group of its own."""
    monkeypatch.setattr(
        topology_runner.subprocess, "run", _run_returning(stdout=PS_TABLE)
    )

    groups, error = topology_runner._topology_groups(100)

    assert groups == {100, 200, 300}
    assert error == ""


def test_topology_groups_excludes_unrelated_processes(monkeypatch):
    monkeypatch.setattr(
        topology_runner.subprocess, "run", _run_returning(stdout=PS_TABLE)
    )

    groups, _ = topology_runner._topology_groups(100)

    assert 400 not in groups


def test_topology_groups_deduplicates_a_shared_group(monkeypatch):
    shared = """
  100     1   100
  200   100   100
  300   100   100
"""
    monkeypatch.setattr(
        topology_runner.subprocess, "run", _run_returning(stdout=shared)
    )

    groups, _ = topology_runner._topology_groups(100)

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

    monkeypatch.setattr(topology_runner.subprocess, "run", fail)

    groups, error = topology_runner._topology_groups(100)

    assert groups == set()
    assert expected in error


def test_topology_groups_reports_an_absent_root(monkeypatch):
    """Valid ps output that does not contain Honcho is not an empty topology.

    If Honcho exits between observation and snapshot, its children may still be
    running with the link to them gone.
    """
    monkeypatch.setattr(
        topology_runner.subprocess, "run", _run_returning(stdout=PS_TABLE)
    )

    groups, error = topology_runner._topology_groups(999)

    assert groups == set()
    assert "999 was absent" in error


def test_topology_groups_reports_a_non_zero_ps_exit(monkeypatch):
    monkeypatch.setattr(
        topology_runner.subprocess,
        "run",
        _run_returning(returncode=1, stderr="ps exploded"),
    )

    groups, error = topology_runner._topology_groups(100)

    assert groups == set()
    assert "ps exploded" in error


def test_process_group_is_empty_when_the_group_is_gone(monkeypatch):
    def gone(pgid, sig):
        raise ProcessLookupError

    monkeypatch.setattr(topology_runner.os, "killpg", gone)

    assert topology_runner._process_group_is_empty(999) is True


def test_process_group_is_not_empty_while_processes_remain(monkeypatch):
    monkeypatch.setattr(topology_runner.os, "killpg", lambda pgid, sig: None)

    assert topology_runner._process_group_is_empty(999) is False


def test_process_group_permission_denied_means_still_there(monkeypatch):
    """Cannot signal it, but something is holding the group: not empty."""

    def denied(pgid, sig):
        raise PermissionError

    monkeypatch.setattr(topology_runner.os, "killpg", denied)

    assert topology_runner._process_group_is_empty(999) is False


class FakeProcess:
    def __init__(self, returncode=None, pid=100):
        self.pid = pid
        self._returncode = returncode
        self.waits = 0

    def poll(self):
        return self._returncode

    @property
    def returncode(self):
        return self._returncode

    def wait(self, timeout=None):
        self.waits += 1
        return self._returncode


def test_ready_when_every_predicate_holds(monkeypatch):
    monkeypatch.setattr(topology_runner.time, "sleep", lambda s: None)
    topology = topology_runner.Topology({})
    topology._process = FakeProcess()

    ready, detail = topology.wait_until_ready(
        [("a", lambda: (True, "a ok")), ("b", lambda: (True, "b ok"))], timeout=5
    )

    assert ready is True
    # The detail carries what held, not just that something did.
    assert "a ok" in detail and "b ok" in detail


def test_not_ready_names_the_predicate_that_failed(monkeypatch):
    monkeypatch.setattr(topology_runner.time, "sleep", lambda s: None)
    topology = topology_runner.Topology({})
    topology._process = FakeProcess()

    ready, detail = topology.wait_until_ready(
        [("a", lambda: (True, "fine")), ("zookeeper", lambda: (False, "no leader"))],
        timeout=0.2,
    )

    assert ready is False
    assert "zookeeper: no leader" in detail


def test_a_dead_honcho_is_reported_before_any_predicate(monkeypatch):
    """A predicate reading stale state could otherwise look satisfied."""
    monkeypatch.setattr(topology_runner.time, "sleep", lambda s: None)
    topology = topology_runner.Topology({})
    topology._process = FakeProcess(returncode=1)

    ready, detail = topology.wait_until_ready(
        [("a", lambda: (True, "looks fine"))], timeout=5
    )

    assert ready is False
    assert "honcho exited early with code 1" in detail


def test_predicates_share_one_deadline(monkeypatch):
    """One deadline, not one each: a slow first predicate must not hide the rest."""
    monkeypatch.setattr(topology_runner.time, "sleep", lambda s: None)
    topology = topology_runner.Topology({})
    topology._process = FakeProcess()
    calls = []

    ready, _ = topology.wait_until_ready(
        [("slow", lambda: (calls.append(1), (False, "never"))[1])], timeout=0.2
    )

    assert ready is False
    assert calls, "the predicate should have been tried at least once"


def test_start_reports_a_missing_honcho(monkeypatch):
    def missing(*args, **kwargs):
        raise FileNotFoundError

    monkeypatch.setattr(topology_runner.subprocess, "Popen", missing)

    assert "honcho is missing" in topology_runner.Topology({}).start()


def test_stopping_something_never_started_is_fine():
    assert topology_runner.Topology({}).stop() == (True, "")


def _stop_scaffold(monkeypatch, table_stdout, empty_groups):
    # Real-time deadline, so shrink it rather than spinning for 30 seconds.
    monkeypatch.setattr(topology_runner, "HONCHO_STOP_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(
        topology_runner.subprocess, "run", _run_returning(stdout=table_stdout)
    )
    monkeypatch.setattr(topology_runner.os, "getpgid", lambda pid: 100)
    monkeypatch.setattr(topology_runner.os, "killpg", lambda pgid, sig: None)
    monkeypatch.setattr(
        topology_runner, "_process_group_is_empty", lambda pgid: pgid in empty_groups
    )
    monkeypatch.setattr(topology_runner.time, "sleep", lambda seconds: None)


def test_stop_topology_passes_when_every_group_is_gone(monkeypatch):
    _stop_scaffold(monkeypatch, PS_TABLE, empty_groups={100, 200, 300})

    stopped, error = topology_runner._stop_topology(StoppableHoncho())

    assert stopped is True
    assert error == ""


def test_stop_topology_fails_when_a_worker_group_survives(monkeypatch):
    """The regression this guards: Honcho exits, its group empties, work goes on."""
    _stop_scaffold(monkeypatch, PS_TABLE, empty_groups={100, 200})

    stopped, error = topology_runner._stop_topology(StoppableHoncho())

    assert stopped is False
    assert "still running after shutdown: [300]" in error


def test_stop_topology_reaps_honcho(monkeypatch):
    """An unreaped zombie would keep its own group looking occupied."""
    _stop_scaffold(monkeypatch, PS_TABLE, empty_groups={100, 200, 300})
    honcho = StoppableHoncho()

    topology_runner._stop_topology(honcho)

    assert honcho.waits >= 1


def test_stop_topology_still_shuts_down_when_the_snapshot_fails(monkeypatch):
    """Shutdown proceeds; the assertion fails afterwards."""
    signalled = []

    def no_ps(*args, **kwargs):
        raise FileNotFoundError

    monkeypatch.setattr(topology_runner, "HONCHO_STOP_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(topology_runner.subprocess, "run", no_ps)
    monkeypatch.setattr(topology_runner.os, "getpgid", lambda pid: 100)
    monkeypatch.setattr(
        topology_runner.os, "killpg", lambda pgid, sig: signalled.append((pgid, sig))
    )
    honcho = StoppableHoncho()

    stopped, error = topology_runner._stop_topology(honcho)

    assert stopped is False
    assert "ps is not available" in error
    assert signalled, "honcho should still have been signalled"
    assert honcho.waits >= 1


def test_stop_topology_never_signals_a_captured_group(monkeypatch):
    """Captured groups are observed, never signalled, so PGID reuse is safe."""
    signalled = []
    monkeypatch.setattr(
        topology_runner.subprocess, "run", _run_returning(stdout=PS_TABLE)
    )
    monkeypatch.setattr(topology_runner.os, "getpgid", lambda pid: 100)
    monkeypatch.setattr(
        topology_runner.os, "killpg", lambda pgid, sig: signalled.append((pgid, sig))
    )
    monkeypatch.setattr(topology_runner, "_process_group_is_empty", lambda pgid: True)
    monkeypatch.setattr(topology_runner.time, "sleep", lambda seconds: None)

    topology_runner._stop_topology(StoppableHoncho())

    # Only Honcho's own group (100) is ever signalled; 200 and 300 are not.
    assert {pgid for pgid, _ in signalled} == {100}


class StoppableHoncho(FakeProcess):
    def __init__(self):
        super().__init__(returncode=0)
        # Matches the root pid in PS_TABLE, so the snapshot actually finds it.
        self.pid = 100
        self.waits = 0

    def wait(self, timeout=None):
        self.waits += 1
        return 0
