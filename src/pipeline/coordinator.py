"""A coordinator: competes for leadership, and does the leader-only work.

Several of these run. One wins. The winner writes a periodic snapshot of the
pipeline, which 0.8 will serve over HTTP.

Why a single writer is not, on its own, a correctness argument: four
snapshotters would be wasteful and would race to overwrite each other, which is
an efficiency and tidiness complaint. What makes the snapshot *correct* is that
every write is fenced — a coordinator that has lost leadership without noticing
has its writes rejected.
"""

import logging
import socket
import threading
import time
from contextlib import ExitStack

from kazoo.exceptions import NodeExistsError
from kazoo.recipe.election import Election

from pipeline import (
    cassandra_store,
    coordination,
    jobs_queue,
    kafka_offsets,
    redis_store,
)
from pipeline.config import (
    CASSANDRA_KEYSPACE,
    CONSUMER_GROUP,
    SNAPSHOT_INTERVAL_SECONDS,
)
from pipeline.coordination import Leadership, Paths, StaleLeader

logger = logging.getLogger("coordinator")

stop_running = threading.Event()


def identity() -> str:
    """Unique per process, so contenders and registrations cannot collide."""
    import os

    return f"{socket.gethostname()}-{os.getpid()}"


def build_snapshot(redis_client, cassandra_session, offsets, epoch: int) -> dict:
    """What the leader records about the pipeline.

    Deliberately no Cassandra `COUNT(*)`: 0.5 documented that as a diagnostic
    that scans every partition, and putting it on a timer would turn that
    warning into an application access pattern. Cassandra contributes
    reachability here, not a total.
    """
    completed, runs = redis_store.job_summary(redis_client)
    counts = redis_store.page_counts(redis_client)

    try:
        waiting, consumers = jobs_queue.queue_state()
    except Exception as error:  # noqa: BLE001 - recorded, not fatal
        waiting, consumers = None, None
        logger.warning(f"queue unavailable for snapshot: {error}")

    return {
        "epoch": epoch,
        "at": time.time(),
        "pageviews": sum(counts.values()),
        "pages": len(counts),
        "jobs_completed": completed,
        "jobs_distinct": len(runs),
        "queue_waiting": waiting,
        "queue_consumers": consumers,
        "kafka_committed": offsets.committed_total(CONSUMER_GROUP),
        "cassandra_reachable": cassandra_reachable(cassandra_session),
    }


def cassandra_reachable(session) -> bool:
    """Whether Cassandra answers a trivial, bounded query.

    Reachability rather than a row count: 0.5 documented COUNT(*) as a
    diagnostic that scans every partition, and putting it on a timer would turn
    that warning into an application access pattern.

    Takes a session the leader already holds. Building a Cluster per snapshot —
    every two seconds, or every half-second under the smoke topology — costs far
    more than the query, and during an outage its retry path would block the
    leader loop for much longer than the snapshot interval.
    """
    if session is None:
        return False
    try:
        session.execute("SELECT release_version FROM system.local")
        return True
    except Exception as error:  # noqa: BLE001 - recorded, not fatal
        logger.warning(f"cassandra unreachable for snapshot: {error}")
        return False


def lead(
    client,
    paths: Paths,
    leadership: Leadership,
    redis_client,
    cassandra_session,
    offsets,
) -> None:
    """Run leader-only work until leadership is lost or the process stops.

    kazoo will not interrupt this function when the contender is cancelled, so
    it cooperates: every iteration checks whether leadership may still be
    exercised, and a rejected fenced write ends it immediately.
    """
    # A tenure begins only under a connected session. Winning while SUSPENDED
    # would mean believing in a leadership nobody has confirmed.
    if not leadership.begin_tenure():
        logger.warning("won the election while disconnected; standing down")
        return

    epoch = coordination.claim_epoch(client, paths)

    # Recheck before creating anything. The session can go while the epoch is
    # being claimed, and a marker written under a session we have already lost
    # would be created by a *replacement* session and so outlive this tenure.
    if not leadership.may_work:
        logger.warning("lost the session while claiming an epoch; standing down")
        return

    try:
        coordination.acknowledge_leadership(client, paths, identity(), epoch)
    except NodeExistsError:
        # The previous leader's ephemeral marker has not expired. Never
        # overwrite it: set() changes the data but not the owner, so it would
        # vanish with their session and leave this process marker-less.
        logger.warning("previous leader's marker still present; standing down")
        return

    # And again immediately after: if the session went during the create, the
    # node belongs to a replacement session and nothing else will remove it.
    # Delete exactly the marker just written — identity *and* epoch.
    if not leadership.may_work:
        logger.warning("lost the session while acknowledging; removing the marker")
        coordination.release_leadership(client, paths, identity(), epoch)
        return

    logger.info(f"Elected leader with epoch token {epoch}")

    # Only a voluntary step-down removes the marker. After a lost session
    # ZooKeeper has already taken ours, and after a rejected write the successor
    # owns the replacement — deleting then would remove *their* marker.
    voluntary = False
    try:
        while not stop_running.is_set():
            if not leadership.may_work:
                if leadership.tenure_void:
                    # Do not touch the marker: a successor may already own it.
                    # LOST: the epoch token is void and the marker went with the
                    # session. Re-enter the election from scratch.
                    logger.warning("session lost; this tenure is over")
                    return
                # SUSPENDED: the session may yet survive, but leadership is
                # unknown until it does. Working now is how two leaders happen.
                logger.warning("pausing leader work: leadership is unknown")
                time.sleep(0.5)
                continue

            try:
                coordination.write_snapshot(
                    client,
                    paths,
                    epoch,
                    build_snapshot(redis_client, cassandra_session, offsets, epoch),
                )
            except StaleLeader as error:
                # Superseded: the marker is already someone else's.
                logger.warning(f"stepping down: {error}")
                return

            stop_running.wait(SNAPSHOT_INTERVAL_SECONDS)
        voluntary = True
    finally:
        if voluntary:
            # Before Election.run() releases the contender, so the next winner
            # does not find the marker occupied.
            coordination.release_leadership(client, paths, identity(), epoch)


def main() -> int:
    # One ownership boundary, as in the consumer and the worker. Acquiring
    # outside it meant a Cassandra failure leaked the ZooKeeper and Redis
    # connections already open, and `presence` could be unbound in the finally.
    with ExitStack() as resources:
        client = coordination.connect()
        resources.callback(client.close)
        resources.callback(client.stop)

        paths = Paths()
        coordination.wait_for_initialisation(client, paths)

        redis_client = redis_store.connect()
        resources.callback(redis_client.close)

        # One session for the life of the process, reused by every snapshot:
        # building a Cluster per snapshot costs far more than the query.
        cassandra_cluster, cassandra_session = cassandra_store.connect(
            keyspace=CASSANDRA_KEYSPACE
        )
        resources.callback(cassandra_cluster.shutdown)

        offsets = kafka_offsets.OffsetReader()
        resources.callback(offsets.close)

        leadership = Leadership()
        client.add_listener(leadership.on_state)

        presence = coordination.Presence(client, paths, "coordinator", identity())
        client.add_listener(presence.on_state)
        resources.callback(presence.stop)
        if not presence.start():
            # Registration is part of the startup barrier, not an optional
            # extra: a coordinator absent from the registry while contending
            # for leadership makes the tree lie about who is participating.
            raise RuntimeError(
                f"could not register as a coordinator under {paths.registry}"
            )

        resources.callback(stop_running.set)

        logger.info(
            f"Contending for leadership as {identity()} "
            f"(negotiated session timeout "
            f"{coordination.negotiated_timeout(client):.1f}s)"
        )
        election = Election(client, paths.election, identity())
        try:
            while not stop_running.is_set():
                election.run(
                    lead,
                    client,
                    paths,
                    leadership,
                    redis_client,
                    cassandra_session,
                    offsets,
                )
                if stop_running.is_set():
                    break
                # Lost leadership without stopping. If the session went, the old
                # epoch is void and the next tenure starts from scratch.
                logger.info("no longer leader; re-entering the election")
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down coordinator")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
