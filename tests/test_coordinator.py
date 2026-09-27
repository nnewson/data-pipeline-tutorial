"""The leadership state machine.

`lead()` is the most intricate function in this release — tenure, epoch, marker
ownership, three connection states, fenced writes, and the difference between a
voluntary and an involuntary exit. It had no direct tests until now, and every
regression in this release landed in it. The live smoke run proves the assembled
happy path; it cannot enumerate a state machine.
"""

import json
import threading

import pytest
from conftest import FakeClient
from kazoo.protocol.states import KazooState

from pipeline import coordination, coordinator
from pipeline.coordination import Leadership, Paths, StaleLeader


@pytest.fixture
def world(monkeypatch):
    """A coordination tree, a stop event, and a snapshot builder under control."""
    client = FakeClient()
    paths = Paths(root="/t")
    coordination.initialise(client, paths, worker_delay=0.5)

    stop = threading.Event()
    monkeypatch.setattr(coordinator, "stop_running", stop)
    monkeypatch.setattr(coordinator.time, "sleep", lambda s: None)
    monkeypatch.setattr(coordinator, "identity", lambda: "me")
    monkeypatch.setattr(coordinator, "build_snapshot", lambda *a: {"epoch": 1})
    # The loop waits this long between snapshots; the real 2s would make each
    # two-iteration test a two-second test.
    monkeypatch.setattr(coordinator, "SNAPSHOT_INTERVAL_SECONDS", 0.001)
    return client, paths, stop


def _marker(client, paths):
    if not client.exists(paths.leader):
        return None
    return json.loads(client.data[paths.leader])


def test_disconnected_before_tenure_claims_and_acknowledges_nothing(world):
    client, paths, _ = world
    leadership = Leadership()
    leadership.on_state(KazooState.SUSPENDED)

    coordinator.lead(client, paths, leadership, None, None, None)

    assert client.data[paths.epoch] == b"0", "an epoch was claimed"
    assert _marker(client, paths) is None


def test_a_session_lost_before_acknowledgement_leaves_no_marker(world, monkeypatch):
    """The marker must never be created under a session we have already lost."""
    client, paths, _ = world
    leadership = Leadership()
    real_claim = coordination.claim_epoch

    def claim_then_lose(*args):
        token = real_claim(*args)
        leadership.on_state(KazooState.LOST)
        return token

    monkeypatch.setattr(coordination, "claim_epoch", claim_then_lose)

    coordinator.lead(client, paths, leadership, None, None, None)

    assert _marker(client, paths) is None, "a marker was left behind"


def test_a_marker_created_under_a_replacement_session_is_removed(world, monkeypatch):
    """The real orphan race, in the order it actually happens.

    LOST, then CONNECTED under a *replacement* session, and only then the
    create. The node therefore belongs to a live session and ZooKeeper will not
    remove it — but the tenure that made it is void, so nothing else ever will.
    """
    client, paths, _ = world
    leadership = Leadership()
    real_ack = coordination.acknowledge_leadership

    def lose_then_create(*args):
        # The session dies and is replaced while the acknowledgement is in
        # flight; the create below lands under the new one.
        leadership.on_state(KazooState.LOST)
        leadership.on_state(KazooState.CONNECTED)
        real_ack(*args)

    monkeypatch.setattr(coordination, "acknowledge_leadership", lose_then_create)

    coordinator.lead(client, paths, leadership, None, None, None)

    assert _marker(client, paths) is None, "an orphaned marker survived"


def test_ordinary_expiry_needs_no_cleanup_call(world, monkeypatch):
    """A marker made under the session that then expires is removed by ZooKeeper.

    Our code must not depend on deleting it — the delete would be attempted
    while disconnected, and would fail.
    """
    client, paths, _ = world
    leadership = Leadership()
    real_ack = coordination.acknowledge_leadership

    def create_then_lose(*args):
        real_ack(*args)
        # ZooKeeper removes ephemeral nodes belonging to an expired session.
        client.delete(paths.leader)
        leadership.on_state(KazooState.LOST)

    monkeypatch.setattr(coordination, "acknowledge_leadership", create_then_lose)

    coordinator.lead(client, paths, leadership, None, None, None)

    assert _marker(client, paths) is None


def test_suspension_pauses_and_reconnection_resumes_the_same_tenure(world, monkeypatch):
    """SUSPENDED stops work without ending the tenure; CONNECTED resumes it."""
    client, paths, stop = world
    leadership = Leadership()
    writes = []

    def snapshot(*args):
        writes.append(1)
        if len(writes) == 1:
            leadership.on_state(KazooState.SUSPENDED)
        if len(writes) >= 2:
            stop.set()
        return {"epoch": 1}

    def resume(seconds):
        # The pause branch sleeps; reconnect from there so work can continue.
        leadership.on_state(KazooState.CONNECTED)

    monkeypatch.setattr(coordinator, "build_snapshot", snapshot)
    monkeypatch.setattr(coordinator.time, "sleep", resume)

    coordinator.lead(client, paths, leadership, None, None, None)

    assert len(writes) >= 2, "work did not resume after reconnecting"
    assert _marker(client, paths) is None, "a voluntary stop should release it"


def test_a_lost_session_does_not_resume_the_old_tenure(world, monkeypatch):
    client, paths, _ = world
    leadership = Leadership()
    writes = []

    def snapshot(*args):
        writes.append(1)
        leadership.on_state(KazooState.LOST)
        leadership.on_state(KazooState.CONNECTED)
        return {"epoch": 1}

    monkeypatch.setattr(coordinator, "build_snapshot", snapshot)

    coordinator.lead(client, paths, leadership, None, None, None)

    assert len(writes) == 1, "the old tenure resumed after a lost session"


def test_a_stale_write_does_not_delete_the_successors_marker(world, monkeypatch):
    client, paths, _ = world
    leadership = Leadership()

    def superseded(*args, **kwargs):
        # B has taken over and written its own marker.
        client.delete(paths.leader)
        client.create(
            paths.leader,
            json.dumps({"identity": "B", "epoch": 2}).encode(),
            ephemeral=True,
        )
        raise StaleLeader("superseded")

    monkeypatch.setattr(coordination, "write_snapshot", superseded)

    coordinator.lead(client, paths, leadership, None, None, None)

    assert _marker(client, paths)["identity"] == "B", "B's marker was removed"


def test_a_voluntary_stop_releases_the_owned_marker(world, monkeypatch):
    client, paths, stop = world
    leadership = Leadership()

    def snapshot(*args):
        stop.set()
        return {"epoch": 1}

    monkeypatch.setattr(coordinator, "build_snapshot", snapshot)

    coordinator.lead(client, paths, leadership, None, None, None)

    assert _marker(client, paths) is None


def test_the_previous_leaders_marker_is_never_overwritten(world):
    """NodeExistsError means stand down, not take over."""
    client, paths, _ = world
    client.create(
        paths.leader,
        json.dumps({"identity": "other", "epoch": 9}).encode(),
        ephemeral=True,
    )

    coordinator.lead(client, paths, Leadership(), None, None, None)

    assert _marker(client, paths)["identity"] == "other"


def test_an_unexpected_snapshot_failure_propagates(world, monkeypatch):
    """Documented behaviour: anything that is not StaleLeader is a real fault."""
    client, paths, _ = world

    def explode(*args, **kwargs):
        raise RuntimeError("cassandra fell over")

    monkeypatch.setattr(coordination, "write_snapshot", explode)

    with pytest.raises(RuntimeError, match="cassandra fell over"):
        coordinator.lead(client, paths, Leadership(), None, None, None)


def test_the_snapshot_reports_every_field_it_promises(monkeypatch):
    """The docstring claimed Cassandra reachability before any call existed."""

    class Redis:
        pass

    monkeypatch.setattr(
        coordinator.redis_store, "job_summary", lambda c: (7, {"e1": 2})
    )
    monkeypatch.setattr(
        coordinator.redis_store, "page_counts", lambda c: {"/docs": 5, "/": 2}
    )
    monkeypatch.setattr(coordinator.jobs_queue, "queue_state", lambda: (3, 4))

    class Session:
        def execute(self, statement):
            return True

    class Offsets:
        def committed_total(self, group):
            return 99

    snapshot = coordinator.build_snapshot(Redis(), Session(), Offsets(), epoch=2)

    assert snapshot["epoch"] == 2
    assert snapshot["pageviews"] == 7
    assert snapshot["jobs_completed"] == 7
    assert snapshot["queue_waiting"] == 3
    assert snapshot["queue_consumers"] == 4
    assert snapshot["cassandra_reachable"] is True
    assert snapshot["kafka_committed"] == 99


def test_an_unreachable_cassandra_is_recorded_not_raised(monkeypatch):
    """A status probe must not take the leader down with it."""

    class Failing:
        def execute(self, statement):
            raise RuntimeError("no host available")

    assert coordinator.cassandra_reachable(Failing()) is False


def test_an_unreachable_queue_is_recorded_not_raised(monkeypatch):
    monkeypatch.setattr(coordinator.redis_store, "job_summary", lambda c: (0, {}))
    monkeypatch.setattr(coordinator.redis_store, "page_counts", lambda c: {})

    def unavailable():
        raise RuntimeError("broker gone")

    monkeypatch.setattr(coordinator.jobs_queue, "queue_state", unavailable)

    class Session:
        def execute(self, statement):
            return True

    class Offsets:
        def committed_total(self, group):
            return None

    snapshot = coordinator.build_snapshot(None, Session(), Offsets(), epoch=1)

    assert snapshot["queue_waiting"] is None
    assert snapshot["kafka_committed"] is None


class Closeable:
    def __init__(self, name, log):
        self.name = name
        self._log = log

    def close(self):
        self._log.append(f"{self.name}.close")

    def stop(self):
        self._log.append(f"{self.name}.stop")

    def shutdown(self):
        self._log.append(f"{self.name}.shutdown")

    def add_listener(self, listener):
        pass


def _main_scaffold(monkeypatch, closed):
    """Everything main() acquires, in order, each recording its own teardown."""
    zk = Closeable("zookeeper", closed)
    monkeypatch.setattr(coordinator.coordination, "connect", lambda: zk)
    monkeypatch.setattr(
        coordinator.coordination, "wait_for_initialisation", lambda c, p: None
    )
    monkeypatch.setattr(coordinator.coordination, "negotiated_timeout", lambda c: 10.0)
    monkeypatch.setattr(
        coordinator.redis_store, "connect", lambda: Closeable("redis", closed)
    )
    return zk


def test_a_cassandra_failure_closes_redis_and_zookeeper(monkeypatch):
    """The regression this guards: acquisition used to happen outside the try."""
    closed = []
    _main_scaffold(monkeypatch, closed)

    def cassandra_fails(**kwargs):
        raise RuntimeError("no host available")

    monkeypatch.setattr(coordinator.cassandra_store, "connect", cassandra_fails)

    with pytest.raises(RuntimeError, match="no host available"):
        coordinator.main()

    assert "redis.close" in closed
    assert "zookeeper.stop" in closed and "zookeeper.close" in closed


def test_a_registration_failure_closes_everything_acquired(monkeypatch):
    closed = []
    _main_scaffold(monkeypatch, closed)
    monkeypatch.setattr(
        coordinator.cassandra_store,
        "connect",
        lambda **kwargs: (Closeable("cassandra", closed), object()),
    )
    monkeypatch.setattr(
        coordinator.kafka_offsets, "OffsetReader", lambda: Closeable("offsets", closed)
    )

    class Refusing:
        def start(self, *args, **kwargs):
            return False

        def stop(self):
            closed.append("presence.stop")

        def on_state(self, state):
            pass

    monkeypatch.setattr(coordinator.coordination, "Presence", lambda *a: Refusing())

    with pytest.raises(RuntimeError, match="could not register"):
        coordinator.main()

    for expected in (
        "presence.stop",
        "offsets.close",
        "cassandra.shutdown",
        "redis.close",
        "zookeeper.close",
    ):
        assert expected in closed, f"{expected} was not closed; got {closed}"


def test_resources_close_in_reverse_order(monkeypatch):
    """ExitStack unwinds last-acquired first; a leak here is a real one."""
    closed = []
    _main_scaffold(monkeypatch, closed)
    monkeypatch.setattr(
        coordinator.cassandra_store,
        "connect",
        lambda **kwargs: (Closeable("cassandra", closed), object()),
    )
    monkeypatch.setattr(
        coordinator.kafka_offsets, "OffsetReader", lambda: Closeable("offsets", closed)
    )

    class Refusing:
        def start(self, *args, **kwargs):
            return False

        def stop(self):
            closed.append("presence.stop")

        def on_state(self, state):
            pass

    monkeypatch.setattr(coordinator.coordination, "Presence", lambda *a: Refusing())

    with pytest.raises(RuntimeError):
        coordinator.main()

    assert closed.index("offsets.close") < closed.index("cassandra.shutdown")
    assert closed.index("cassandra.shutdown") < closed.index("redis.close")
    assert closed.index("redis.close") < closed.index("zookeeper.close")


def test_a_session_lost_during_a_write_ends_the_tenure_cleanly(world, monkeypatch):
    """What a frozen leader finds on waking: its pending transaction failed.

    Found by the failover demonstration, which is what it is for. The coordinator
    previously died with an unhandled SessionExpiredError instead of ending its
    tenure and re-entering the election.
    """
    from kazoo.exceptions import SessionExpiredError

    client, paths, _ = world

    def expired(*args, **kwargs):
        raise SessionExpiredError

    monkeypatch.setattr(coordination, "write_snapshot", expired)

    # Returns rather than raising.
    coordinator.lead(client, paths, Leadership(), None, None, None)


def test_a_connection_lost_during_a_write_resumes_the_same_tenure(world, monkeypatch):
    """A lost connection is SUSPENDED, not LOST: the session may yet survive.

    Ending the tenure here would re-enter the election while our own live marker
    still occupied /leader, and every later winner would fail to acknowledge.
    """
    from kazoo.exceptions import ConnectionLoss

    client, paths, stop = world
    leadership = Leadership()
    attempts = []
    real_write = coordination.write_snapshot

    def flaky(*args, **kwargs):
        attempts.append(1)
        if len(attempts) == 1:
            leadership.on_state(KazooState.SUSPENDED)
            raise ConnectionLoss
        real_write(*args, **kwargs)
        stop.set()

    def reconnect(seconds):
        # Both the ConnectionLoss backoff and the pause branch sleep; either way
        # the connection comes back and the same tenure continues.
        leadership.on_state(KazooState.CONNECTED)

    monkeypatch.setattr(coordination, "write_snapshot", flaky)
    monkeypatch.setattr(coordinator.time, "sleep", reconnect)

    coordinator.lead(client, paths, leadership, None, None, None)

    assert len(attempts) >= 2, "the tenure did not resume after reconnecting"
    # A voluntary stop, so its own marker is released.
    assert _marker(client, paths) is None


def test_a_connection_loss_followed_by_expiry_ends_the_tenure(world, monkeypatch):
    """Only the listener reporting LOST ends it — not the write failing."""
    from kazoo.exceptions import ConnectionLoss

    client, paths, _ = world
    leadership = Leadership()
    attempts = []

    def then_expires(*args, **kwargs):
        attempts.append(1)
        leadership.on_state(KazooState.SUSPENDED)
        if len(attempts) >= 1:
            # The session did not survive after all.
            leadership.on_state(KazooState.LOST)
            # A successor has taken over in the meantime.
            client.delete(paths.leader)
            client.create(
                paths.leader,
                json.dumps({"identity": "B", "epoch": 2}).encode(),
                ephemeral=True,
            )
        raise ConnectionLoss

    monkeypatch.setattr(coordination, "write_snapshot", then_expires)

    coordinator.lead(client, paths, leadership, None, None, None)

    assert _marker(client, paths)["identity"] == "B", "the successor's marker went"


def test_a_session_lost_during_a_write_leaves_the_marker_alone(world, monkeypatch):
    """Involuntary: ZooKeeper took ours, and a successor may own its replacement."""
    from kazoo.exceptions import SessionExpiredError

    client, paths, _ = world

    def expired(*args, **kwargs):
        # A successor has already taken over.
        client.delete(paths.leader)
        client.create(
            paths.leader,
            json.dumps({"identity": "B", "epoch": 2}).encode(),
            ephemeral=True,
        )
        raise SessionExpiredError

    monkeypatch.setattr(coordination, "write_snapshot", expired)

    coordinator.lead(client, paths, Leadership(), None, None, None)

    assert _marker(client, paths)["identity"] == "B"
