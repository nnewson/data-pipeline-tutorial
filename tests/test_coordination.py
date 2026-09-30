"""Coordination: fencing, tenure, registration and the config watcher.

Written before the fixes they drive. The session-loss paths were invisible while
only the happy path had coverage.
"""

import json
import threading
import time
from types import SimpleNamespace

import pytest
from conftest import FakeClient, FakeStat, FakeTransaction  # noqa: F401
from kazoo.exceptions import BadVersionError, NodeExistsError, NoNodeError
from kazoo.handlers.threading import KazooTimeoutError
from kazoo.protocol.states import KazooState

from pipeline import coordination
from pipeline.bulkhead import DeadlineExceeded
from pipeline.coordination import Leadership, Paths


def _initialised():
    client = FakeClient()
    paths = Paths(root="/t")
    coordination.initialise(client, paths, worker_delay=0.5)
    return client, paths


# --- initialisation -------------------------------------------------------


def test_the_first_leadership_token_is_one():
    """ensure_path-then-set burns a version; create-with-data does not."""
    client, paths = _initialised()

    assert coordination.claim_epoch(client, paths) == 1


def test_the_first_config_publication_is_version_one():
    client, paths = _initialised()

    client.set(paths.worker_delay, b"0.05")

    assert client.versions[paths.worker_delay] == 1


def test_reinitialising_never_resets_the_epoch():
    """Resetting it would silently un-fence every stale leader."""
    client, paths = _initialised()
    coordination.claim_epoch(client, paths)
    coordination.claim_epoch(client, paths)
    before = client.data[paths.epoch]

    coordination.initialise(client, paths, worker_delay=0.5)

    assert client.data[paths.epoch] == before


def test_reinitialising_never_resets_live_config():
    client, paths = _initialised()
    client.set(paths.worker_delay, b"9.0")

    coordination.initialise(client, paths, worker_delay=0.5)

    assert client.data[paths.worker_delay] == b"9.0"


def test_initialisation_creates_every_role_parent():
    """Runtime processes must not create persistent nodes."""
    client, paths = _initialised()

    for role in ("coordinator", "consumer", "worker"):
        assert client.exists(f"{paths.registry}/{role}")


# --- fencing --------------------------------------------------------------


def test_epoch_claims_are_sequential():
    client, paths = _initialised()

    assert [coordination.claim_epoch(client, paths) for _ in range(3)] == [1, 2, 3]


def test_epoch_claim_retries_on_a_lost_race(monkeypatch):
    """Two coordinators claiming at once must not share a token."""
    client, paths = _initialised()
    original_set = client.set
    attempts = []

    def racy(path, value, version=-1):
        attempts.append(path)
        if len(attempts) == 1:
            # Someone else claimed in between.
            original_set(path, b"7")
            raise BadVersionError(path)
        return original_set(path, value, version)

    client.set = racy

    token = coordination.claim_epoch(client, paths)

    assert len(attempts) >= 2
    assert token == client.versions[paths.epoch]


def test_a_stale_snapshot_write_is_rejected():
    client, paths = _initialised()
    first = coordination.claim_epoch(client, paths)
    coordination.write_snapshot(client, paths, first, {"by": "A"})
    coordination.claim_epoch(client, paths)

    with pytest.raises(coordination.StaleLeader):
        coordination.write_snapshot(client, paths, first, {"by": "A-stale"})


def test_a_rejected_write_leaves_the_snapshot_untouched():
    """A fence that rejects but still mutates is not a fence."""
    client, paths = _initialised()
    first = coordination.claim_epoch(client, paths)
    second = coordination.claim_epoch(client, paths)
    coordination.write_snapshot(client, paths, second, {"by": "B"})
    before, version_before = coordination.read_snapshot(client, paths)

    with pytest.raises(coordination.StaleLeader):
        coordination.write_snapshot(client, paths, first, {"by": "A-stale"})

    assert coordination.read_snapshot(client, paths) == (before, version_before)


def test_a_non_version_failure_is_not_reported_as_fencing():
    """Only BadVersionError from the epoch check means superseded."""
    client, paths = _initialised()
    token = coordination.claim_epoch(client, paths)

    class Exploding(FakeTransaction):
        def commit(self):
            return [RuntimeError("connection lost")]

    client.transaction = lambda: Exploding(client)

    with pytest.raises(RuntimeError, match="connection lost"):
        coordination.write_snapshot(client, paths, token, {})


# --- leadership transitions ----------------------------------------------


def test_a_fresh_leadership_may_work():
    assert Leadership().may_work is True


def test_suspended_stops_work_without_voiding_the_tenure():
    leadership = Leadership()
    leadership.on_state(KazooState.SUSPENDED)

    assert leadership.may_work is False
    assert leadership.tenure_void is False


def test_reconnecting_from_suspended_resumes():
    leadership = Leadership()
    leadership.on_state(KazooState.SUSPENDED)
    leadership.on_state(KazooState.CONNECTED)

    assert leadership.may_work is True


def test_lost_voids_the_tenure():
    leadership = Leadership()
    leadership.on_state(KazooState.LOST)

    assert leadership.may_work is False
    assert leadership.tenure_void is True


def test_reconnecting_after_lost_does_not_resume_the_old_tenure():
    leadership = Leadership()
    leadership.on_state(KazooState.LOST)
    leadership.on_state(KazooState.CONNECTED)

    assert leadership.may_work is False


def test_a_new_tenure_may_work_after_a_lost_session():
    """The bug: a coordinator that wins again must be able to lead again."""
    leadership = Leadership()
    leadership.on_state(KazooState.LOST)
    leadership.on_state(KazooState.CONNECTED)

    leadership.begin_tenure()

    assert leadership.may_work is True


def test_a_tenure_cannot_begin_while_disconnected():
    leadership = Leadership()
    leadership.on_state(KazooState.SUSPENDED)

    assert leadership.begin_tenure() is False
    assert leadership.may_work is False


# --- the leader marker ----------------------------------------------------


def test_claiming_leadership_creates_an_ephemeral_node():
    client, paths = _initialised()

    coordination.acknowledge_leadership(client, paths, "host-1", 1)

    assert paths.leader in client.ephemeral


def test_leadership_is_never_taken_by_overwriting_another_session():
    """set() changes the data but not the owner: it would vanish with them."""
    client, paths = _initialised()
    client.create(paths.leader, b'{"identity": "other"}', ephemeral=True)

    with pytest.raises(NodeExistsError):
        coordination.acknowledge_leadership(client, paths, "host-2", 2)

    assert paths.leader not in client.set_calls


def test_releasing_leadership_deletes_the_marker():
    client, paths = _initialised()
    coordination.acknowledge_leadership(client, paths, "host-1", 1)

    coordination.release_leadership(client, paths, "host-1")

    assert paths.leader in client.deleted


# --- the config watcher ---------------------------------------------------


class WatchClient(FakeClient):
    """A client whose reads can be made to fail, and whose watch can be fired."""

    def __init__(self):
        super().__init__()
        self.watch = None
        self.reads = 0

    def get(self, path, watch=None):
        self.reads += 1
        if watch is not None:
            self.watch = watch
        return super().get(path, watch=None)

    def fire(self):
        assert self.watch is not None, "no watch was installed"
        watch, self.watch = self.watch, None
        watch(None)


def _watcher(client, applied):
    return coordination.ConfigWatcher(
        client,
        "/t/config/worker_delay",
        lambda raw, version: applied.append((raw, version)),
    )


def test_the_initial_value_is_applied_before_start_returns():
    """Otherwise a worker begins jobs on its environment default."""
    client, _ = _initialised()
    applied = []
    watcher = _watcher(client, applied)

    assert watcher.start() is True
    watcher.stop()

    assert applied == [("0.5", 0)]


def test_two_consecutive_updates_are_both_applied():
    client, paths = _initialised()
    watch_client = WatchClient()
    watch_client.data, watch_client.versions = client.data, client.versions
    applied = []
    watcher = _watcher(watch_client, applied)
    watcher.start()

    for value in (b"0.1", b"0.2"):
        watch_client.set(paths.worker_delay, value)
        watch_client.fire()
        watcher._read_once()

    watcher.stop()

    assert [raw for raw, _ in applied[-2:]] == ["0.1", "0.2"]


def test_a_failed_read_does_not_park_the_watcher_forever():
    """No watch is installed after a failure, so nothing would wake it again."""
    client, _ = _initialised()
    watch_client = WatchClient()
    watch_client.data, watch_client.versions = client.data, client.versions
    applied = []
    watcher = _watcher(watch_client, applied)

    watch_client.fail_next_get = RuntimeError("connection lost")
    assert watcher._read_once() is False

    # A later read recovers, because the loop waits with a timeout.
    assert watcher._read_once() is True
    assert applied


def test_a_missing_node_is_reported_rather_than_raising():
    watcher = coordination.ConfigWatcher(FakeClient(), "/nope", lambda raw, v: None)

    assert watcher._read_once() is False


def test_a_rejected_value_still_leaves_the_watch_installed():
    """A bad value may be corrected later, and we must hear about it."""
    client, _ = _initialised()
    watch_client = WatchClient()
    watch_client.data, watch_client.versions = client.data, client.versions

    def refuse(raw, version):
        raise ValueError("nope")

    watcher = coordination.ConfigWatcher(watch_client, "/t/config/worker_delay", refuse)

    # Refused, so not "applied" — but the watch is installed, so a corrected
    # value will still arrive.
    assert watcher._read_once() is False
    assert watch_client.watch is not None


def test_reconnecting_triggers_a_re_read():
    """A watch does not survive a session loss; the value may have moved."""
    client, _ = _initialised()
    watcher = _watcher(client, [])

    watcher.on_state(KazooState.CONNECTED)

    assert watcher._wake.is_set()


def test_suspension_alone_does_not_trigger_a_re_read():
    client, _ = _initialised()
    watcher = _watcher(client, [])
    watcher._wake.clear()

    watcher.on_state(KazooState.SUSPENDED)

    assert not watcher._wake.is_set()


# --- registration ---------------------------------------------------------


def test_registration_creates_an_ephemeral_sequential_node():
    """Sequential, so four identical workers cannot collide on one path."""
    client, paths = _initialised()

    first = coordination.register(client, paths, "worker", "host-1", {})
    second = coordination.register(client, paths, "worker", "host-2", {})

    assert first != second
    assert first in client.ephemeral and second in client.ephemeral


def test_presence_re_registers_after_a_session_loss(monkeypatch):
    """ZooKeeper deleted the node; nothing else will put it back.

    Exercises the background thread rather than calling register() by hand: the
    point is that the retry loop works, not that the helper it calls does.
    """
    client, paths = _initialised()
    monkeypatch.setattr(coordination.Presence, "RETRY_SECONDS", 0.01)
    presence = coordination.Presence(client, paths, "worker", "host-1")
    assert presence.start(timeout=5, delay=0.5) is True
    original = presence.path

    presence.on_state(KazooState.LOST)
    presence.on_state(KazooState.CONNECTED)

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and presence.path in (None, original):
        time.sleep(0.01)
    presence.stop()

    assert presence.path not in (None, original)


def test_the_state_listener_never_blocks_on_zookeeper(monkeypatch):
    """kazoo's docs are explicit: a listener that blocks delays every other one."""
    client, paths = _initialised()
    presence = coordination.Presence(client, paths, "worker", "host-1")
    calls = []
    monkeypatch.setattr(coordination, "register", lambda *a: calls.append(1) or "/p")

    presence.on_state(KazooState.LOST)
    presence.on_state(KazooState.CONNECTED)

    # No thread was started, so nothing registered: the listener only signalled.
    assert calls == []


def test_presence_retries_a_failed_registration(monkeypatch):
    """A create that fails must not leave the process permanently unregistered."""
    client, paths = _initialised()
    monkeypatch.setattr(coordination.Presence, "RETRY_SECONDS", 0.01)
    attempts = []
    real = coordination.register

    def flaky(*args):
        attempts.append(1)
        if len(attempts) < 3:
            raise RuntimeError("connection lost")
        return real(*args)

    monkeypatch.setattr(coordination, "register", flaky)
    presence = coordination.Presence(client, paths, "worker", "host-1")

    assert presence.start(timeout=5) is True
    presence.stop()
    assert len(attempts) >= 3


def test_presence_does_not_re_register_on_a_mere_reconnect():
    """SUSPENDED then CONNECTED keeps the same session, and the same node."""
    client, paths = _initialised()
    presence = coordination.Presence(client, paths, "worker", "host-1")
    presence.start()
    original = presence.path

    presence.on_state(KazooState.SUSPENDED)
    presence.on_state(KazooState.CONNECTED)

    assert presence.path == original


def test_presence_carries_its_payload_into_a_re_registration():
    client, paths = _initialised()
    presence = coordination.Presence(client, paths, "worker", "host-1")
    presence.start(delay=0.5, config_version=3)

    presence.on_state(KazooState.LOST)
    presence.on_state(KazooState.CONNECTED)

    latest = coordination.registrations(client, paths, "worker")[-1]
    assert latest["delay"] == 0.5
    assert latest["config_version"] == 3


def test_presence_updates_keep_the_same_node():
    client, paths = _initialised()
    presence = coordination.Presence(client, paths, "worker", "host-1")
    presence.start(delay=0.5)
    path = presence.path

    presence.update(delay=0.05, config_version=2)

    assert presence.path == path
    entry = coordination.registrations(client, paths, "worker")[0]
    assert entry["delay"] == 0.05


def test_a_resumed_stale_leader_cannot_delete_its_successors_marker():
    """The race the SIGSTOP demonstration would expose.

    A's session expires, ZooKeeper removes A's marker, B creates its own. If A
    then resumes and deletes the fixed path, it removes B's leadership.
    """
    client, paths = _initialised()
    coordination.acknowledge_leadership(client, paths, "A", 1)
    # A's session expired: ZooKeeper deleted the node, and B took over.
    client.delete(paths.leader)
    coordination.acknowledge_leadership(client, paths, "B", 2)

    coordination.release_leadership(client, paths, "A")

    assert client.exists(paths.leader), "B's marker was deleted by A"
    marker = json.loads(client.data[paths.leader])
    assert marker["identity"] == "B"


def test_a_leader_releases_its_own_marker():
    client, paths = _initialised()
    coordination.acknowledge_leadership(client, paths, "A", 1)

    coordination.release_leadership(client, paths, "A")

    assert not client.exists(paths.leader)


def test_releasing_a_marker_that_is_already_gone_is_harmless():
    client, paths = _initialised()

    coordination.release_leadership(client, paths, "A")  # must not raise


def test_the_watcher_waits_for_a_value_before_start_returns(monkeypatch):
    """Exercises the thread: a single failed read must not be taken as done."""
    client, paths = _initialised()
    watch_client = WatchClient()
    watch_client.data, watch_client.versions = client.data, client.versions
    monkeypatch.setattr(coordination.ConfigWatcher, "RETRY_SECONDS", 0.01)
    applied = []
    watcher = _watcher(watch_client, applied)

    # The first read fails; the loop must come back and succeed.
    watch_client.fail_next_get = RuntimeError("connection lost")

    assert watcher.start(timeout=5) is True
    watcher.stop()
    assert applied, "no value was ever applied"


def test_the_watcher_reports_failure_rather_than_pretending(monkeypatch):
    """A worker must be able to refuse to start rather than use its default."""
    monkeypatch.setattr(coordination.ConfigWatcher, "RETRY_SECONDS", 0.01)
    watcher = coordination.ConfigWatcher(FakeClient(), "/missing", lambda r, v: None)

    assert watcher.start(timeout=0.2) is False
    watcher.stop()


def test_a_change_after_startup_is_applied_by_the_thread(monkeypatch):
    client, paths = _initialised()
    watch_client = WatchClient()
    watch_client.data, watch_client.versions = client.data, client.versions
    monkeypatch.setattr(coordination.ConfigWatcher, "RETRY_SECONDS", 0.01)
    applied = []
    watcher = _watcher(watch_client, applied)
    watcher.start(timeout=5)

    watch_client.set(paths.worker_delay, b"0.25")

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and "0.25" not in [r for r, _ in applied]:
        time.sleep(0.01)
    watcher.stop()

    assert "0.25" in [raw for raw, _ in applied]


def test_a_marker_replaced_between_read_and_delete_survives(monkeypatch):
    """The version check, not the identity check, closes this one.

    A and B share an identity only in the sense that the read said "ours"; a
    replacement written in between must not be removed.
    """
    client, paths = _initialised()
    coordination.acknowledge_leadership(client, paths, "A", 1)

    real_get = client.get

    def get_then_replace(path, watch=None):
        value, stat = real_get(path, watch=watch)
        if path == paths.leader:
            # B replaces the marker after our read but before our delete.
            client.data[path] = json.dumps({"identity": "A", "epoch": 2}).encode()
            client.versions[path] += 1
        return value, stat

    client.get = get_then_replace

    coordination.release_leadership(client, paths, "A")

    assert client.exists(paths.leader), "the replacement was deleted"


def test_an_invalid_first_value_does_not_count_as_applied(monkeypatch):
    """Otherwise a worker starts on its environment default, which is the bug."""
    client, paths = _initialised()
    client.data[paths.worker_delay] = b"not-a-number"
    monkeypatch.setattr(coordination.ConfigWatcher, "RETRY_SECONDS", 0.01)

    def strict(raw, version):
        float(raw)

    watcher = coordination.ConfigWatcher(client, paths.worker_delay, strict)

    assert watcher.start(timeout=0.2) is False
    watcher.stop()


def test_a_correction_after_an_invalid_value_is_applied(monkeypatch):
    """The watch stays installed, so a fixed value still arrives."""
    client, paths = _initialised()
    client.data[paths.worker_delay] = b"not-a-number"
    monkeypatch.setattr(coordination.ConfigWatcher, "RETRY_SECONDS", 0.01)
    applied = []

    def strict(raw, version):
        applied.append(float(raw))

    watcher = coordination.ConfigWatcher(client, paths.worker_delay, strict)
    assert watcher.start(timeout=0.1) is False

    client.set(paths.worker_delay, b"0.25")

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and not applied:
        time.sleep(0.01)
    watcher.stop()

    # At least once, and only the corrected value. Not exactly once: the
    # watcher re-reads on every retry wake whether or not a watch fired, and at
    # the 10ms retry used here a second read can land before stop() — which
    # made `applied == [0.25]` fail about one run in twenty.
    assert applied and set(applied) == {0.25}


def test_a_registration_from_a_lost_session_is_discarded(monkeypatch):
    """The race: LOST lands while a create is in flight.

    Accepting the returned path would record a node that no longer exists and
    swallow the re-registration request with it.
    """
    client, paths = _initialised()
    monkeypatch.setattr(coordination.Presence, "RETRY_SECONDS", 0.01)
    presence = coordination.Presence(client, paths, "worker", "host-1")

    in_flight = threading.Barrier(2, timeout=5)
    real = coordination.register
    calls = []

    def register_then_lose(*args):
        calls.append(1)
        path = real(*args)
        if len(calls) == 1:
            # Simulate the session dying while this create was in flight.
            in_flight.wait()
            in_flight.wait()
        return path

    monkeypatch.setattr(coordination, "register", register_then_lose)

    thread = threading.Thread(target=lambda: presence.start(timeout=5), daemon=True)
    thread.start()

    in_flight.wait()
    presence.on_state(KazooState.LOST)
    in_flight.wait()

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and len(calls) < 2:
        time.sleep(0.01)
    presence.stop()

    assert len(calls) >= 2, "the stale registration was accepted and never retried"
    # And exactly one live registration, not two: the node created under the
    # replacement session must have been removed, not merely forgotten.
    live = coordination.registrations(client, paths, "worker")
    assert len(live) == 1, f"expected one registration, found {len(live)}"


def test_a_payload_changed_while_registering_is_not_lost(monkeypatch):
    """update() during an in-flight create must not leave stale values behind."""
    client, paths = _initialised()
    monkeypatch.setattr(coordination.Presence, "RETRY_SECONDS", 0.01)
    presence = coordination.Presence(client, paths, "worker", "host-1")

    in_flight = threading.Barrier(2, timeout=5)
    real = coordination.register

    def register_slowly(*args):
        path = real(*args)
        in_flight.wait()  # let the test call update()
        in_flight.wait()  # and finish before we publish
        return path

    monkeypatch.setattr(coordination, "register", register_slowly)

    thread = threading.Thread(
        target=lambda: presence.start(timeout=5, delay=0.5, config_version=1),
        daemon=True,
    )
    thread.start()

    in_flight.wait()
    presence.update(delay=0.05, config_version=9)
    in_flight.wait()

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and presence.path is None:
        time.sleep(0.01)
    presence.stop()

    entry = coordination.registrations(client, paths, "worker")[0]
    assert entry["config_version"] == 9, f"stale payload survived: {entry}"


def test_a_registration_accepted_while_lost_does_not_report_success(monkeypatch):
    """The handoff must publish path and events together, under the lock."""
    client, paths = _initialised()
    monkeypatch.setattr(coordination.Presence, "RETRY_SECONDS", 0.01)
    presence = coordination.Presence(client, paths, "worker", "host-1")
    presence._generation = 1

    # Every create returns while the session has moved on again, so no attempt
    # can ever be accepted.
    def always_stale(*args):
        presence._generation += 1
        return "/p"

    monkeypatch.setattr(coordination, "register", always_stale)

    started = presence.start(timeout=0.2)
    presence.stop()

    assert started is False, "reported success with no live registration"


# --- bounded reads, for the API ---------------------------------------------


class _Done:
    """An AsyncResult that has already completed."""

    def __init__(self, value=None, error=None):
        self.value, self.error = value, error
        self.linked = []

    def get(self, timeout=None):
        if self.error is not None:
            raise self.error
        return self.value

    def rawlink(self, callback):
        self.linked.append(callback)


class _Outstanding(_Done):
    """An AsyncResult whose reply has not arrived: the wait times out."""

    def get(self, timeout=None):
        self.waited = timeout
        raise KazooTimeoutError


class _AsyncClient:
    """Only the async half of kazoo, so a sync call here is a test failure."""

    def __init__(self, results):
        self.handler = SimpleNamespace(timeout_exception=KazooTimeoutError)
        self.results = results
        self.sent = []

    def _send(self, method, path):
        self.sent.append((method, path))
        return self.results[(method, path)]

    def get_async(self, path, watch=None):
        return self._send("get", path)

    def exists_async(self, path, watch=None):
        return self._send("exists", path)

    def get_children_async(self, path, watch=None):
        return self._send("get_children", path)


class _Budget:
    def __init__(self, timeout=0.7, spent=False):
        self.timeout, self.spent = timeout, spent
        self.abandoned = []

    def call_timeout(self):
        if self.spent:
            raise DeadlineExceeded
        return self.timeout

    def abandon(self, result):
        self.abandoned.append(result)


def test_a_bounded_read_waits_only_for_the_budgets_timeout():
    paths = Paths(root="/t")
    pending = _Outstanding()
    client = _AsyncClient({("get", paths.leader): pending})
    budget = _Budget(timeout=0.7)

    with pytest.raises(KazooTimeoutError):
        coordination.read_leader(client, paths, budget)

    assert pending.waited == 0.7


def test_a_timed_out_read_is_handed_over_not_forgotten():
    """A timeout ends the wait; kazoo still holds the request."""
    paths = Paths(root="/t")
    pending = _Outstanding()
    client = _AsyncClient({("get", paths.snapshot): pending})
    budget = _Budget()

    with pytest.raises(KazooTimeoutError):
        coordination.read_snapshot(client, paths, budget)

    assert budget.abandoned == [pending]


def test_a_spent_budget_sends_nothing():
    paths = Paths(root="/t")
    client = _AsyncClient({})
    with pytest.raises(DeadlineExceeded):
        coordination.read_leader(client, paths, _Budget(spent=True))

    assert client.sent == []


def test_a_missing_leader_is_a_completed_read_not_an_abandoned_one():
    paths = Paths(root="/t")
    client = _AsyncClient({("get", paths.leader): _Done(error=NoNodeError())})
    budget = _Budget()

    assert coordination.read_leader(client, paths, budget) is None
    assert budget.abandoned == []


def test_bounded_registrations_hand_over_nothing_when_every_read_completes():
    paths = Paths(root="/t")
    base = f"{paths.registry}/worker"
    client = _AsyncClient(
        {
            ("exists", base): _Done(FakeStat()),
            ("get_children", base): _Done(["a", "b"]),
            ("get", f"{base}/a"): _Done((b'{"identity": "w1"}', FakeStat())),
            ("get", f"{base}/b"): _Done((b'{"identity": "w2"}', FakeStat())),
        }
    )
    budget = _Budget()

    found = coordination.registrations(client, paths, "worker", budget)

    assert [entry["identity"] for entry in found] == ["w1", "w2"]
    assert budget.abandoned == []
    assert [method for method, _ in client.sent] == [
        "exists",
        "get_children",
        "get",
        "get",
    ]
