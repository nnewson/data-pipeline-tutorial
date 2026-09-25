"""Starting, observing and stopping the Procfile topology.

Extracted at 0.7, on the trigger agreed at 0.3: when the topology gained a
*state* — "exactly one leader" — rather than just a process count, the smoke
test's readiness condition outgrew a hard-coded sequence of checks.

The responsibility is deliberately narrow:

  * own the process and its environment;
  * preserve the start/stop mechanics **unchanged** — they were measured and
    hardened across 0.3 to 0.6 and are not reopened here;
  * accept external readiness predicates returning success plus a diagnostic;
  * apply one shared deadline across them;
  * stop the processes before any caller deletes the state they were using.

It knows nothing about Kafka, Redis, Cassandra, RabbitMQ or ZooKeeper. That all
lives in the predicates a caller supplies.
"""

import logging
import os
import signal
import subprocess
import time
from collections.abc import Callable

logger = logging.getLogger("topology-runner")

HONCHO_STOP_TIMEOUT_SECONDS = 30
COMMAND_TIMEOUT_SECONDS = 120

# A predicate reports whether the topology is ready yet, and why not.
Predicate = Callable[[], tuple[bool, str]]


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


class Topology:
    """A running Procfile topology, and the means to stop it completely."""

    def __init__(self, environment: dict[str, str]) -> None:
        self._environment = environment
        self._process: subprocess.Popen | None = None

    def start(self) -> str:
        """Start honcho. Returns an error string, or empty on success."""
        try:
            self._process = subprocess.Popen(  # noqa: S603
                # Not `uv run honcho`: `uv run` starts its child in a session of
                # its own, so the process signalled on shutdown would not be
                # Honcho, and Honcho would never forward the signal on.
                [".venv/bin/honcho", "start"],
                env=self._environment,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        except FileNotFoundError:
            return ".venv/bin/honcho is missing; run `uv sync --all-extras`"
        return ""

    def died(self) -> str:
        """Why honcho exited, or empty while it is still running."""
        if self._process is None or self._process.poll() is None:
            return ""
        return f"honcho exited early with code {self._process.returncode}"

    def wait_until_ready(
        self, predicates: list[tuple[str, Predicate]], timeout: float
    ) -> tuple[bool, str]:
        """Wait for every predicate to hold, under one shared deadline.

        One deadline rather than one each: a topology that spends the whole
        budget satisfying the first predicate has left none for the rest, and
        pretending otherwise turns a slow failure into a confusing one.
        """
        deadline = time.monotonic() + timeout
        detail = "nothing was checked"
        while time.monotonic() < deadline:
            if died := self.died():
                return False, died

            details = []
            for name, predicate in predicates:
                ready, detail = predicate()
                if not ready:
                    detail = f"{name}: {detail}"
                    break
                details.append(detail)
            else:
                return True, "; ".join(details)
            time.sleep(2)
        return False, f"not ready within {timeout:.0f}s — {detail}"

    def stop(self) -> tuple[bool, str]:
        """Stop everything, and report whether it really stopped."""
        if self._process is None:
            return True, ""
        return _stop_topology(self._process)
