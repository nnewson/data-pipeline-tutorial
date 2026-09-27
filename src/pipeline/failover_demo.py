"""Two leaders: a demonstration, not a test.

Kept out of the routine smoke test on purpose — it waits through a session
expiry, which would be dead time on every CI run. Run it by hand when you want to
see the thing this release is about.

Requires the stack up and `uv run cluster init` already done: this script waits
for the coordination tree rather than creating it, because `cluster init` owns
the persistent tree exclusively.

It makes **two separate observations** rather than trying to catch one race:

  1. **Stale belief.** Freeze the leader with SIGSTOP. Its session expires, a new
     leader is elected, and ZooKeeper and the frozen process then disagree about
     who leads. Shown by comparing the tree against the frozen process's own last
     words — no timing luck required.

  2. **Stale writes are refused.** Separately and deterministically, submit a
     snapshot carrying the deposed leader's epoch and watch the fenced
     transaction reject it.

A stopped process cannot write anything, so the first observation cannot prove
the second, and manufacturing a race between SIGCONT and the work loop would be
dishonest. Hence two.

Every step is an acceptance condition: the script exits non-zero rather than
printing its punchline regardless.
"""

import logging
import os
import signal
import subprocess
import sys
import time
from contextlib import ExitStack
from pathlib import Path

from pipeline import coordination
from pipeline.coordination import Paths, StaleLeader

logger = logging.getLogger("failover-demo")

COORDINATORS = 3
REPO = Path(__file__).resolve().parents[2]

# Session expiry is decided by the server, so wait against the negotiated value
# with headroom rather than the number we asked for.
EXPIRY_HEADROOM = 2.5

# How long to allow a resumed process to notice, and B to write a snapshot.
NOTICE_TIMEOUT = 30.0

# How long to wait for one of our own coordinators to win.
ELECTION_TIMEOUT = 60.0


def _stop(process: subprocess.Popen) -> None:
    """Signal a coordinator's group and reap it, however hard that has to be."""
    try:
        os.killpg(os.getpgid(process.pid), signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        pass
    try:
        process.wait(timeout=15)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(os.getpgid(process.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass
    try:
        # Reap after the kill too, or it lingers as a zombie.
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        pass


def _thaw(state: dict) -> None:
    """Continue a stopped process, whatever else went wrong.

    Registered before the demonstration runs, so it happens even when an
    acceptance condition fails partway. A stopped process is invisible to a
    process count and still holds its session, so leaving one behind poisons the
    next run.
    """
    pid = state.get("frozen")
    if pid is None:
        return
    try:
        os.kill(pid, signal.SIGCONT)
    except ProcessLookupError:
        pass


def _start_coordinators(
    log_dir: Path, resources: ExitStack
) -> list[tuple[subprocess.Popen, Path]]:
    """Start the coordinators, registering each for teardown as it starts.

    One at a time rather than after the loop: if the third fails to start, the
    first two must still be stopped.
    """
    started = []
    for index in range(COORDINATORS):
        log = log_dir / f"coordinator_{index}.log"
        process = subprocess.Popen(  # noqa: S603
            [str(REPO / ".venv/bin/coordinator")],
            cwd=REPO,
            stdout=log.open("w"),
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        resources.callback(_stop, process)
        started.append((process, log))
    return started


def _pid_of(leader: dict) -> int | None:
    """The pid embedded in an identity, or None if it is not one of ours."""
    try:
        return int(leader["identity"].rsplit("-", 1)[1])
    except (KeyError, IndexError, ValueError):
        return None


def _wait_for_leader(
    client, paths: Paths, deadline: float, among: set[int], exclude: str = ""
) -> dict | None:
    """Wait for an acknowledged leader that is one of our own processes.

    `among` matters more than it looks. `/leader` is ephemeral, but a previous
    run's node survives until its session expires — up to the session timeout
    after that process died. Accepting it would make this demonstration chase a
    leader that no longer exists, which is what happened on repeat runs.
    """
    while time.monotonic() < deadline:
        leader = coordination.read_leader(client, paths)
        if leader and leader["identity"] != exclude and _pid_of(leader) in among:
            return leader
        time.sleep(0.5)
    return None


def _believes_it_leads(log: Path) -> bool:
    """Whether a process's own record still claims leadership.

    Its log is the only statement of what it believes: while stopped it cannot
    say anything new, so "elected, and never told otherwise" is exactly the stale
    belief we are looking for.
    """
    text = log.read_text()
    if "Elected leader with epoch token" not in text:
        return False
    return not any(
        phrase in text
        for phrase in (
            "this tenure is over",
            "session expired during a snapshot write",
            "stepping down",
        )
    )


def _wait_for_snapshot_epoch(client, paths: Paths, epoch: int, deadline: float) -> bool:
    while time.monotonic() < deadline:
        snapshot, _ = coordination.read_snapshot(client, paths)
        if snapshot.get("epoch") == epoch:
            return True
        time.sleep(0.5)
    return False


def _demonstrate(client, paths, processes, timeout, state) -> int:
    ours = {process.pid for process, _ in processes}

    first = _wait_for_leader(client, paths, time.monotonic() + ELECTION_TIMEOUT, ours)
    if first is None:
        # Distinguish the two reasons, because the common one is not "broken".
        existing = coordination.read_leader(client, paths)
        if existing:
            print(
                f"  {existing['identity']} already leads, and it is not one of "
                "this demo's coordinators."
            )
            print(
                "  Stop the Honcho topology first — its coordinators compete in "
                "the same election."
            )
        else:
            print("  no leader was elected; is ZooKeeper reachable?")
        return 1
    print(f"  leader A: {first['identity']} at epoch {first['epoch']}")

    leader_pid = _pid_of(first)
    frozen_log = next(log for process, log in processes if process.pid == leader_pid)

    print(f"  SIGSTOP {leader_pid} — frozen, not killed")
    os.kill(leader_pid, signal.SIGSTOP)
    state["frozen"] = leader_pid

    # --- 1. stale belief -----------------------------------------------------
    second = _wait_for_leader(
        client,
        paths,
        time.monotonic() + timeout * EXPIRY_HEADROOM,
        ours,
        exclude=first["identity"],
    )
    if second is None:
        print(f"  no new leader within {timeout * EXPIRY_HEADROOM:.0f}s")
        return 1
    print(f"  leader B: {second['identity']} at epoch {second['epoch']}")

    print("\n  --- the split ---")
    print(f"  ZooKeeper says the leader is   {second['identity']}")
    if not _believes_it_leads(frozen_log):
        print("  A's own log no longer claims leadership — nothing to show")
        return 1
    print("  A's own log still says it is    the leader")
    print("  Two processes, two beliefs, no race required.\n")

    print(f"  SIGCONT {leader_pid} — letting A find out")
    os.kill(leader_pid, signal.SIGCONT)
    state["frozen"] = None

    deadline = time.monotonic() + NOTICE_TIMEOUT
    while time.monotonic() < deadline:
        if not _believes_it_leads(frozen_log):
            print("  A has learned its session was lost")
            break
        time.sleep(0.5)
    else:
        print(f"  A did not notice within {NOTICE_TIMEOUT:.0f}s")
        return 1

    # --- 2. the fence --------------------------------------------------------
    print("\n  --- the fence ---")
    if not _wait_for_snapshot_epoch(
        client, paths, second["epoch"], time.monotonic() + NOTICE_TIMEOUT
    ):
        print(f"  B never wrote a snapshot at epoch {second['epoch']}")
        return 1
    print(f"  the snapshot now belongs to B, at epoch {second['epoch']}")

    print(f"  submitting a snapshot with A's epoch token {first['epoch']}")
    try:
        coordination.write_snapshot(
            client, paths, first["epoch"], {"epoch": first["epoch"], "by": "A"}
        )
    except StaleLeader as error:
        print(f"  REJECTED: {error}")
    else:
        print("  !! ACCEPTED — the fence is not working")
        return 1

    snapshot, _ = coordination.read_snapshot(client, paths)
    # The *version* may legitimately have moved on, because B keeps writing.
    # The epoch is what must not have changed.
    if snapshot.get("epoch") != second["epoch"]:
        print(
            f"  !! the snapshot is at epoch {snapshot.get('epoch')}, "
            f"not B's {second['epoch']}"
        )
        return 1
    print(f"  snapshot still belongs to epoch {snapshot.get('epoch')}")
    return 0


def main() -> int:
    log_dir = Path(os.environ.get("FAILOVER_LOG_DIR", "/tmp")) / "failover-demo"
    log_dir.mkdir(parents=True, exist_ok=True)

    # One boundary covering the client and every process. Acquiring outside it
    # leaked both when initialisation failed or a coordinator would not start.
    with ExitStack() as resources:
        client = coordination.connect()
        resources.callback(client.close)
        resources.callback(client.stop)

        paths = Paths()
        coordination.wait_for_initialisation(client, paths)

        timeout = coordination.negotiated_timeout(client)
        print(f"  negotiated session timeout: {timeout:.1f}s")
        print(
            "  (no workers or consumers run here, so the snapshot's queue and "
            "job figures will be empty or unavailable)"
        )

        state: dict = {"frozen": None}
        processes = _start_coordinators(log_dir, resources)
        # Registered *after* the processes, so LIFO unwinding thaws before it
        # stops anything. Registered before them, a frozen coordinator would sit
        # through a pending SIGTERM and a 15-second wait and be SIGKILLed while
        # still stopped — no leak, but not "SIGCONT before teardown" either.
        # A partial startup failure needs no thaw: nothing has been frozen yet.
        resources.callback(_thaw, state)

        try:
            return _demonstrate(client, paths, processes, timeout, state)
        finally:
            print(f"\n  logs: {log_dir}")


if __name__ == "__main__":
    sys.exit(main())
