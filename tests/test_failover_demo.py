"""The demonstration's acceptance conditions and cleanup.

Its assertions must not be decorative: each branch that prints a failure has to
return non-zero, and a frozen process must be continued even when an earlier
condition fails. The session expiry itself stays out of CI — only the decisions
around it are tested here.
"""

import subprocess
import time

from pipeline import failover_demo


class FakeProcess:
    def __init__(self, pid):
        self.pid = pid
        self.waits = 0
        self.killed = []

    def wait(self, timeout=None):
        self.waits += 1
        return 0


def _leader(identity="host-7", epoch=1):
    return {"identity": identity, "epoch": epoch}


def test_a_leader_from_a_previous_run_is_ignored(monkeypatch):
    """`/leader` outlives its process until the session expires."""
    monkeypatch.setattr(failover_demo.time, "sleep", lambda s: None)
    monkeypatch.setattr(
        failover_demo.coordination, "read_leader", lambda c, p: _leader("host-999")
    )

    found = failover_demo._wait_for_leader(
        None, None, time.monotonic() + 0.05, among={7}
    )

    assert found is None


def test_our_own_leader_is_accepted(monkeypatch):
    monkeypatch.setattr(failover_demo.time, "sleep", lambda s: None)
    monkeypatch.setattr(
        failover_demo.coordination, "read_leader", lambda c, p: _leader("host-7")
    )

    assert (
        failover_demo._wait_for_leader(None, None, time.monotonic() + 5, among={7})
        is not None
    )


def test_the_excluded_leader_is_skipped(monkeypatch):
    """Waiting for B must not match A again."""
    monkeypatch.setattr(failover_demo.time, "sleep", lambda s: None)
    monkeypatch.setattr(
        failover_demo.coordination, "read_leader", lambda c, p: _leader("host-7")
    )

    found = failover_demo._wait_for_leader(
        None, None, time.monotonic() + 0.05, among={7}, exclude="host-7"
    )

    assert found is None


def test_an_identity_without_a_pid_is_not_ours():
    assert failover_demo._pid_of({"identity": "no-pid-here"}) is None
    assert failover_demo._pid_of({}) is None
    assert failover_demo._pid_of({"identity": "host-42"}) == 42


def test_a_process_that_never_led_does_not_believe_it_leads(tmp_path):
    log = tmp_path / "c.log"
    log.write_text("INFO coordinator: Contending for leadership\n")

    assert failover_demo._believes_it_leads(log) is False


def test_a_frozen_leader_still_believes_it_leads(tmp_path):
    log = tmp_path / "c.log"
    log.write_text("INFO coordinator: Elected leader with epoch token 3\n")

    assert failover_demo._believes_it_leads(log) is True


def test_every_way_a_tenure_ends_counts_as_no_longer_leading(tmp_path):
    """Three wordings reach the log; missing one would fake the punchline."""
    for ending in (
        "session lost; this tenure is over",
        "session expired during a snapshot write: x",
        "stepping down: superseded",
    ):
        log = tmp_path / "c.log"
        log.write_text(
            f"INFO coordinator: Elected leader with epoch token 3\nWARNING {ending}\n"
        )
        assert failover_demo._believes_it_leads(log) is False, ending


def test_no_new_leader_is_a_failure(monkeypatch, tmp_path, capsys):
    """Printing the punchline regardless was the original defect."""
    monkeypatch.setattr(failover_demo.time, "sleep", lambda s: None)
    monkeypatch.setattr(failover_demo.os, "kill", lambda pid, sig: None)
    log = tmp_path / "c.log"
    monkeypatch.setattr(failover_demo, "ELECTION_TIMEOUT", 5)
    log.write_text("INFO coordinator: Elected leader with epoch token 1\n")
    leaders = iter([_leader("host-7")] + [None] * 50)
    monkeypatch.setattr(
        failover_demo.coordination, "read_leader", lambda c, p: next(leaders, None)
    )

    code = failover_demo._demonstrate(
        None, None, [(FakeProcess(7), log)], timeout=0.01, state={}
    )

    assert code == 1
    assert "no new leader" in capsys.readouterr().out


def test_a_leader_that_already_stood_down_is_a_failure(monkeypatch, tmp_path, capsys):
    """Nothing to show if A's log no longer claims leadership."""
    monkeypatch.setattr(failover_demo.time, "sleep", lambda s: None)
    monkeypatch.setattr(failover_demo.os, "kill", lambda pid, sig: None)
    log = tmp_path / "c.log"
    log.write_text(
        "INFO coordinator: Elected leader with epoch token 1\n"
        "WARNING coordinator: session lost; this tenure is over\n"
    )
    leaders = iter([_leader("host-7", 1)])
    monkeypatch.setattr(
        failover_demo.coordination,
        "read_leader",
        lambda c, p: next(leaders, _leader("host-8", 2)),
    )

    code = failover_demo._demonstrate(
        None, None, [(FakeProcess(7), log), (FakeProcess(8), log)], timeout=10, state={}
    )

    assert code == 1
    assert "nothing to show" in capsys.readouterr().out


def test_an_accepted_stale_write_is_a_failure(monkeypatch, tmp_path, capsys):
    """If the fence lets the write through, the demonstration must fail."""
    monkeypatch.setattr(failover_demo.time, "sleep", lambda s: None)
    monkeypatch.setattr(failover_demo.os, "kill", lambda pid, sig: None)
    log = tmp_path / "c.log"
    log.write_text("INFO coordinator: Elected leader with epoch token 1\n")

    states = iter([_leader("host-7", 1)])
    monkeypatch.setattr(
        failover_demo.coordination,
        "read_leader",
        lambda c, p: next(states, _leader("host-8", 2)),
    )
    # A's log flips to "not leading" so the notice step passes.
    notices = iter([True, False, False, False])
    monkeypatch.setattr(
        failover_demo, "_believes_it_leads", lambda log: next(notices, False)
    )
    monkeypatch.setattr(failover_demo, "_wait_for_snapshot_epoch", lambda *a: True)
    # The fence fails to reject.
    monkeypatch.setattr(failover_demo.coordination, "write_snapshot", lambda *a: None)

    code = failover_demo._demonstrate(
        None, None, [(FakeProcess(7), log), (FakeProcess(8), log)], timeout=10, state={}
    )

    assert code == 1
    assert "the fence is not working" in capsys.readouterr().out


def test_thaw_continues_a_frozen_process(monkeypatch):
    """Runs even when an acceptance condition failed; nothing else will."""
    signalled = []
    monkeypatch.setattr(
        failover_demo.os, "kill", lambda pid, sig: signalled.append((pid, sig))
    )

    failover_demo._thaw({"frozen": 4242})

    assert signalled == [(4242, failover_demo.signal.SIGCONT)]


def test_thaw_does_nothing_when_nothing_is_frozen(monkeypatch):
    monkeypatch.setattr(failover_demo.os, "kill", lambda pid, sig: pytest_fail())

    failover_demo._thaw({"frozen": None})
    failover_demo._thaw({})


def test_thaw_survives_an_already_dead_process(monkeypatch):
    def gone(pid, sig):
        raise ProcessLookupError

    monkeypatch.setattr(failover_demo.os, "kill", gone)

    failover_demo._thaw({"frozen": 1})  # must not raise


def test_a_process_is_reaped_after_being_killed(monkeypatch):
    """Without the second wait() it lingers as a zombie."""
    process = FakeProcess(99)
    waits = []

    def timeout_then_succeed(timeout=None):
        waits.append(1)
        if len(waits) == 1:
            raise subprocess.TimeoutExpired(cmd="coordinator", timeout=timeout)
        return 0

    process.wait = timeout_then_succeed
    monkeypatch.setattr(failover_demo.os, "killpg", lambda pgid, sig: None)
    monkeypatch.setattr(failover_demo.os, "getpgid", lambda pid: pid)

    failover_demo._stop(process)

    assert len(waits) == 2, "the killed process was not reaped"


def test_stopping_an_already_gone_process_is_harmless(monkeypatch):
    def gone(pgid, sig):
        raise ProcessLookupError

    monkeypatch.setattr(failover_demo.os, "killpg", gone)
    monkeypatch.setattr(failover_demo.os, "getpgid", lambda pid: pid)

    failover_demo._stop(FakeProcess(1))  # must not raise


def pytest_fail():
    raise AssertionError("os.kill should not have been called")


def test_thaw_runs_before_any_process_is_stopped(monkeypatch, tmp_path):
    """Callback *order*, which the direct _thaw test cannot see.

    ExitStack unwinds last-registered first, so _thaw has to be registered after
    the coordinators. Registered before them, a frozen process would receive
    SIGTERM and be killed while still stopped.
    """
    signals: list[str] = []

    monkeypatch.setattr(
        failover_demo.coordination, "connect", lambda: FakeZookeeperClient()
    )
    monkeypatch.setattr(
        failover_demo.coordination, "wait_for_initialisation", lambda c, p: None
    )
    monkeypatch.setattr(failover_demo.coordination, "negotiated_timeout", lambda c: 1.0)
    monkeypatch.setattr(
        failover_demo.subprocess, "Popen", lambda *a, **k: FakeProcess(4242)
    )
    monkeypatch.setattr(failover_demo.os, "getpgid", lambda pid: pid)
    monkeypatch.setattr(
        failover_demo.os,
        "killpg",
        lambda pgid, sig: signals.append(f"SIGTERM/{pgid}"),
    )
    monkeypatch.setattr(
        failover_demo.os, "kill", lambda pid, sig: signals.append(f"SIGCONT/{pid}")
    )
    monkeypatch.setattr(failover_demo, "_demonstrate", lambda *a: _freeze(a[-1]))
    monkeypatch.setenv("FAILOVER_LOG_DIR", str(tmp_path))

    failover_demo.main()

    assert signals, "nothing was signalled"
    assert signals[0].startswith("SIGCONT"), f"teardown ran before thaw: {signals}"


def _freeze(state):
    """Stand in for a demonstration that froze a process and then failed."""
    state["frozen"] = 4242
    return 1


class FakeZookeeperClient:
    def stop(self):
        pass

    def close(self):
        pass


def test_a_process_that_never_notices_is_a_failure(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(failover_demo.time, "sleep", lambda s: None)
    monkeypatch.setattr(failover_demo.os, "kill", lambda pid, sig: None)
    monkeypatch.setattr(failover_demo, "NOTICE_TIMEOUT", 0.05)
    log = tmp_path / "c.log"
    monkeypatch.setattr(failover_demo, "ELECTION_TIMEOUT", 5)
    log.write_text("INFO coordinator: Elected leader with epoch token 1\n")
    leaders = iter([_leader("host-7", 1)])
    monkeypatch.setattr(
        failover_demo.coordination,
        "read_leader",
        lambda c, p: next(leaders, _leader("host-8", 2)),
    )
    # It keeps believing it leads, forever.
    monkeypatch.setattr(failover_demo, "_believes_it_leads", lambda log: True)

    code = failover_demo._demonstrate(
        None, None, [(FakeProcess(7), log), (FakeProcess(8), log)], timeout=10, state={}
    )

    assert code == 1
    assert "did not notice" in capsys.readouterr().out


def test_a_missing_snapshot_from_b_is_a_failure(monkeypatch, tmp_path, capsys):
    """Without B's snapshot, "still B's" afterwards would prove nothing."""
    monkeypatch.setattr(failover_demo.time, "sleep", lambda s: None)
    monkeypatch.setattr(failover_demo.os, "kill", lambda pid, sig: None)
    log = tmp_path / "c.log"
    monkeypatch.setattr(failover_demo, "ELECTION_TIMEOUT", 5)
    log.write_text("INFO coordinator: Elected leader with epoch token 1\n")
    leaders = iter([_leader("host-7", 1)])
    monkeypatch.setattr(
        failover_demo.coordination,
        "read_leader",
        lambda c, p: next(leaders, _leader("host-8", 2)),
    )
    notices = iter([True, False])
    monkeypatch.setattr(
        failover_demo, "_believes_it_leads", lambda log: next(notices, False)
    )
    monkeypatch.setattr(failover_demo, "_wait_for_snapshot_epoch", lambda *a: False)

    code = failover_demo._demonstrate(
        None, None, [(FakeProcess(7), log), (FakeProcess(8), log)], timeout=10, state={}
    )

    assert code == 1
    assert "never wrote a snapshot" in capsys.readouterr().out


def test_a_snapshot_that_moves_off_bs_epoch_is_a_failure(monkeypatch, tmp_path, capsys):
    """Rejected, yet the snapshot is no longer B's: something else wrote it."""
    monkeypatch.setattr(failover_demo.time, "sleep", lambda s: None)
    monkeypatch.setattr(failover_demo.os, "kill", lambda pid, sig: None)
    log = tmp_path / "c.log"
    monkeypatch.setattr(failover_demo, "ELECTION_TIMEOUT", 5)
    log.write_text("INFO coordinator: Elected leader with epoch token 1\n")
    leaders = iter([_leader("host-7", 1)])
    monkeypatch.setattr(
        failover_demo.coordination,
        "read_leader",
        lambda c, p: next(leaders, _leader("host-8", 2)),
    )
    notices = iter([True, False])
    monkeypatch.setattr(
        failover_demo, "_believes_it_leads", lambda log: next(notices, False)
    )
    monkeypatch.setattr(failover_demo, "_wait_for_snapshot_epoch", lambda *a: True)

    def rejected(*args):
        raise failover_demo.StaleLeader("superseded")

    monkeypatch.setattr(failover_demo.coordination, "write_snapshot", rejected)
    # A third epoch appears: not B's.
    monkeypatch.setattr(
        failover_demo.coordination, "read_snapshot", lambda c, p: ({"epoch": 9}, 5)
    )

    code = failover_demo._demonstrate(
        None, None, [(FakeProcess(7), log), (FakeProcess(8), log)], timeout=10, state={}
    )

    assert code == 1
    assert "not B's 2" in capsys.readouterr().out


def test_the_whole_successful_path_returns_zero(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(failover_demo.time, "sleep", lambda s: None)
    monkeypatch.setattr(failover_demo.os, "kill", lambda pid, sig: None)
    log = tmp_path / "c.log"
    monkeypatch.setattr(failover_demo, "ELECTION_TIMEOUT", 5)
    log.write_text("INFO coordinator: Elected leader with epoch token 1\n")
    leaders = iter([_leader("host-7", 1)])
    monkeypatch.setattr(
        failover_demo.coordination,
        "read_leader",
        lambda c, p: next(leaders, _leader("host-8", 2)),
    )
    notices = iter([True, False])
    monkeypatch.setattr(
        failover_demo, "_believes_it_leads", lambda log: next(notices, False)
    )
    monkeypatch.setattr(failover_demo, "_wait_for_snapshot_epoch", lambda *a: True)

    def rejected(*args):
        raise failover_demo.StaleLeader("superseded")

    monkeypatch.setattr(failover_demo.coordination, "write_snapshot", rejected)
    monkeypatch.setattr(
        failover_demo.coordination, "read_snapshot", lambda c, p: ({"epoch": 2}, 5)
    )

    code = failover_demo._demonstrate(
        None, None, [(FakeProcess(7), log), (FakeProcess(8), log)], timeout=10, state={}
    )

    assert code == 0
    output = capsys.readouterr().out
    assert "two beliefs" in output
    assert "REJECTED" in output


def test_a_foreign_leader_is_named_as_the_reason(monkeypatch, tmp_path, capsys):
    """ "No leader was elected" misdiagnoses the common case.

    A topology left running is the likely cause, and the message should say so
    rather than pointing at ZooKeeper.
    """
    monkeypatch.setattr(failover_demo.time, "sleep", lambda s: None)
    monkeypatch.setattr(failover_demo, "ELECTION_TIMEOUT", 0.05)
    monkeypatch.setattr(
        failover_demo.coordination, "read_leader", lambda c, p: _leader("host-999")
    )
    log = tmp_path / "c.log"
    log.write_text("")

    code = failover_demo._demonstrate(
        None, None, [(FakeProcess(7), log)], timeout=0.01, state={}
    )

    output = capsys.readouterr().out
    assert code == 1
    assert "already leads" in output
    assert "Stop the Honcho topology first" in output


def test_no_leader_at_all_points_at_zookeeper(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(failover_demo.time, "sleep", lambda s: None)
    monkeypatch.setattr(failover_demo, "ELECTION_TIMEOUT", 0.05)
    monkeypatch.setattr(failover_demo.coordination, "read_leader", lambda c, p: None)
    log = tmp_path / "c.log"
    log.write_text("")

    code = failover_demo._demonstrate(
        None, None, [(FakeProcess(7), log)], timeout=0.01, state={}
    )

    assert code == 1
    assert "is ZooKeeper reachable" in capsys.readouterr().out
