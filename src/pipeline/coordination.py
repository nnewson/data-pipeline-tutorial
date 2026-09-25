"""ZooKeeper coordination: election, fencing, registration and live config.

Nothing here concerns Kafka. Kafka has run KRaft since 0.2 and keeps its own
metadata; this is application coordination, which is what most people who deploy
ZooKeeper actually deploy it for.

The idea the module exists to make true: **electing a leader does not give you
mutual exclusion.** A leadership claim is tied to a session, and when that
session expires the old leader is not told synchronously. So every leader-only
write carries an epoch, and the write is a transaction that checks the epoch is
still current. A deposed leader's writes fail whatever it believes about itself.
"""

import json
import logging
import threading
from dataclasses import dataclass
from datetime import UTC, datetime

from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionError, NodeExistsError, NoNodeError
from kazoo.protocol.states import KazooState

from pipeline import wait_for_connection
from pipeline.config import ZOOKEEPER_HOSTS, ZOOKEEPER_ROOT, ZOOKEEPER_TIMEOUT_SECONDS

logger = logging.getLogger("coordination")

ROLES = ("coordinator", "consumer", "worker")


class StaleLeader(Exception):
    """A fenced write was rejected because its epoch is no longer current."""


@dataclass(frozen=True)
class Paths:
    """Four znodes, four distinct jobs.

    `election` holds one ephemeral sequential node per contender, so counting
    its children counts *candidates*, not leaders. `leader` is the acknowledged
    winner, written once it knows it has won — ephemeral, so a dead leader does
    not keep the role on paper.
    """

    root: str = ZOOKEEPER_ROOT

    @property
    def election(self) -> str:
        return f"{self.root}/election"

    @property
    def leader(self) -> str:
        return f"{self.root}/leader"

    @property
    def epoch(self) -> str:
        return f"{self.root}/epoch"

    @property
    def snapshot(self) -> str:
        return f"{self.root}/snapshot"

    @property
    def config(self) -> str:
        return f"{self.root}/config"

    @property
    def worker_delay(self) -> str:
        return f"{self.config}/worker_delay"

    @property
    def registry(self) -> str:
        return f"{self.root}/registry"

    @property
    def persistent_paths(self) -> list[str]:
        """Created once by initialisation, never by the processes that use them."""
        return [
            self.root,
            self.election,
            self.epoch,
            self.snapshot,
            self.config,
            self.worker_delay,
            self.registry,
        ]


def connect(hosts: str = ZOOKEEPER_HOSTS) -> KazooClient:
    """Start a client, retrying while ZooKeeper is still coming up."""

    def start() -> KazooClient:
        client = KazooClient(hosts=hosts, timeout=ZOOKEEPER_TIMEOUT_SECONDS)
        try:
            client.start(timeout=ZOOKEEPER_TIMEOUT_SECONDS)
        except Exception:
            client.stop()
            client.close()
            raise
        return client

    return wait_for_connection("ZooKeeper", start)


def negotiated_timeout(client: KazooClient) -> float:
    """The session timeout the *server* agreed, in seconds.

    ZooKeeper clamps a requested timeout against its own limits and the server
    decides expiry, so every timing claim should use this rather than the value
    we asked for.
    """
    return client._session_timeout / 1000.0


def initialise(client: KazooClient, paths: Paths, worker_delay: float) -> None:
    """Create the persistent tree, without overwriting anything.

    One path owns this, as `create-topics` owns topics and `create-schema` owns
    the keyspace. Everything else waits for it rather than inventing persistent
    state of its own.

    Re-running must never reset `/epoch` or live configuration: resetting the
    epoch would silently un-fence every stale leader.
    """
    # Parents only. Value-bearing leaves are created *with* their value below:
    # ensure_path creates an empty node and a following set() burns version 0,
    # so the first real leadership token would be 2 rather than 1, and there
    # would be a window where the node exists with no value in it.
    for path in (paths.root, paths.election, paths.config, paths.registry):
        client.ensure_path(path)

    # Role parents belong to initialisation too. A runtime process creating them
    # would make it a second owner of persistent state.
    for role in ROLES:
        client.ensure_path(f"{paths.registry}/{role}")

    # create-if-absent, literally. Re-running must never reset the epoch —
    # that would silently un-fence every stale leader — nor live configuration.
    _create_if_absent(client, paths.epoch, b"0")
    _create_if_absent(client, paths.worker_delay, str(worker_delay).encode())
    # Pre-created so a fenced write is always set_data inside a transaction.
    # Without it the first write needs a create-and-race path that behaves
    # differently from every later one, in the place correctness matters.
    _create_if_absent(client, paths.snapshot, json.dumps({"epoch": None}).encode())

    logger.info(f"Coordination tree ready under {paths.root}")


def _create_if_absent(client: KazooClient, path: str, value: bytes) -> None:
    try:
        client.create(path, value)
    except NodeExistsError:
        pass


def wait_for_initialisation(
    client: KazooClient, paths: Paths, retries: int = 10, delay: float = 3
) -> None:
    """Block until the persistent tree exists, or say what to run."""
    import time

    for attempt in range(1, retries + 1):
        if all(client.exists(path) for path in paths.persistent_paths):
            return
        logger.info(
            f"Waiting for the coordination tree ({attempt}/{retries}); "
            f"run `uv run cluster init` if it does not appear"
        )
        time.sleep(delay)
    raise RuntimeError(
        f"Coordination tree missing under {paths.root}. Run `uv run cluster init`."
    )


def claim_epoch(client: KazooClient, paths: Paths) -> int:
    """Take the next epoch, returning the version that fences writes against it.

    A compare-and-set loop, not a read-then-write: two coordinators winning in
    quick succession must not end up sharing a token. The value returned is the
    znode *version* from the successful set, which is what the snapshot
    transaction checks.
    """
    while True:
        value, stat = client.get(paths.epoch)
        current = int(value or b"0")
        try:
            new_stat = client.set(
                paths.epoch, str(current + 1).encode(), version=stat.version
            )
        except BadVersionError:
            # Somebody else claimed in between. Read again and retry.
            continue
        return new_stat.version


def write_snapshot(
    client: KazooClient, paths: Paths, epoch_version: int, payload: dict
) -> None:
    """Write the snapshot, or refuse because this leader has been superseded.

    The fence: a transaction that checks `/epoch` still has the version this
    leader was given. Once another leader claims, that check fails and so does
    the write — regardless of what this process believes about itself.

    kazoo's commit() returns a list of results rather than raising for every
    failure, so each result is inspected. Only a BadVersionError from the epoch
    check means "superseded"; anything else is a real failure and is raised.
    """
    transaction = client.transaction()  # fresh per write: they are not reusable
    transaction.check(paths.epoch, version=epoch_version)
    transaction.set_data(paths.snapshot, json.dumps(payload).encode())

    for result in transaction.commit():
        if isinstance(result, BadVersionError):
            raise StaleLeader(
                f"epoch version {epoch_version} is no longer current; "
                "another coordinator has taken leadership"
            )
        if isinstance(result, Exception):
            raise result


def read_snapshot(client: KazooClient, paths: Paths) -> tuple[dict, int]:
    """The snapshot and its znode version, so a caller can prove it advanced."""
    value, stat = client.get(paths.snapshot)
    return json.loads(value or b"{}"), stat.version


def acknowledge_leadership(
    client: KazooClient, paths: Paths, identity: str, epoch_version: int
) -> None:
    """Record who won, once they know they have won.

    Being the lowest contender does not prove a process knows it is leader, so
    the acknowledgement is separate from the election itself.
    """
    payload = json.dumps(
        {
            "identity": identity,
            "epoch": epoch_version,
            "since": datetime.now(UTC).isoformat(),
        }
    ).encode()
    # Deliberately no fallback to set(). Overwriting an existing ephemeral node
    # changes its data but *not* its owner — it would still vanish when the
    # previous session ended, leaving a leader with no marker. NodeExistsError
    # means the previous leader's session has not gone yet; the caller retries.
    client.create(paths.leader, payload, ephemeral=True)


def release_leadership(
    client: KazooClient, paths: Paths, identity: str, epoch: int | None = None
) -> None:
    """Remove the leader marker, but only if it is still ours.

    The path is fixed, so `delete()` carries no ownership: a coordinator whose
    session expired and later resumed would otherwise delete the marker that now
    belongs to its successor. Read it first and compare identity.

    This is only ever correct on a *voluntary* step-down. After a lost session
    ZooKeeper has already removed our node, and after a stale-write rejection
    somebody else owns the replacement — in both cases the caller must not call
    this at all.
    """
    try:
        value, stat = client.get(paths.leader)
    except NoNodeError:
        return

    marker = json.loads(value) if value else {}
    # Identity *and* epoch when an epoch is given: a process that lost its
    # session and won again would otherwise recognise its own later marker as
    # the one it meant to clean up.
    mine = marker.get("identity") == identity and (
        epoch is None or marker.get("epoch") == epoch
    )
    if not mine:
        logger.info(
            f"not releasing leadership: the marker is "
            f"{marker.get('identity')}@{marker.get('epoch')}, "
            f"not {identity}@{epoch}"
        )
        return

    try:
        # Versioned, so a replacement written between the read and the delete is
        # not removed either.
        client.delete(paths.leader, version=stat.version)
    except (NoNodeError, BadVersionError):
        pass


def read_leader(client: KazooClient, paths: Paths) -> dict | None:
    try:
        value, _ = client.get(paths.leader)
    except NoNodeError:
        return None
    return json.loads(value) if value else None


def register(
    client: KazooClient, paths: Paths, role: str, identity: str, extra: dict
) -> str:
    """Register this process as present, as an ephemeral sequential child.

    Sequential so four identical workers cannot collide on one path. What this
    buys is narrower than it sounds: the tree shows which **sessions currently
    hold registrations**, not what is alive. A wedged or stopped process stays
    registered until its session expires.
    """
    payload = json.dumps(
        {"identity": identity, "since": datetime.now(UTC).isoformat(), **extra}
    ).encode()
    return client.create(
        f"{paths.registry}/{role}/", payload, ephemeral=True, sequence=True
    )


def update_registration(client: KazooClient, path: str, extra: dict) -> None:
    """Replace a registration's payload, keeping the same ephemeral node."""
    try:
        value, _ = client.get(path)
        payload = json.loads(value) if value else {}
    except NoNodeError:
        return
    payload.update(extra)
    client.set(path, json.dumps(payload).encode())


def registrations(client: KazooClient, paths: Paths, role: str) -> list[dict]:
    """Every current registration for a role."""
    base = f"{paths.registry}/{role}"
    if not client.exists(base):
        return []
    found = []
    for child in client.get_children(base):
        try:
            value, _ = client.get(f"{base}/{child}")
        except NoNodeError:
            continue
        if value:
            found.append(json.loads(value))
    return found


class Presence:
    """A registration that re-creates itself after a session loss.

    ZooKeeper deletes an ephemeral node when its session expires. A process that
    registers once and never looks again is *absent* from the tree for the rest
    of its life while still doing work — so the tree under-reports exactly when
    something has gone wrong.

    Re-registration happens on a thread of its own. kazoo's documentation is
    explicit that state listeners must not block, and `create()` is a synchronous
    round trip: doing it inline would delay every other listener, including the
    one that decides whether leadership may be exercised. The listener therefore
    only sets a flag.
    """

    RETRY_SECONDS = 2.0

    def __init__(
        self, client: KazooClient, paths: Paths, role: str, identity: str
    ) -> None:
        self._client = client
        self._paths = paths
        self._role = role
        self._identity = identity
        self._lock = threading.Lock()
        self._path: str | None = None
        self._payload: dict = {}
        self._generation = 0
        self._needed = threading.Event()
        self._registered = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def path(self) -> str | None:
        with self._lock:
            return self._path

    def start(self, timeout: float = 15, **extra) -> bool:
        """Register, then keep the registration alive. Waits for the first one.

        Startup waits so a process is never doing work while absent from the
        tree — the window the smoke test's membership assertions would otherwise
        race against.
        """
        with self._lock:
            self._payload.update(extra)
        self._needed.set()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self._registered.wait(timeout)

    def stop(self) -> None:
        self._stop.set()
        self._needed.set()
        if self._thread:
            self._thread.join(timeout=5)

    def update(self, **extra) -> None:
        with self._lock:
            self._payload.update(extra)
            path, payload = self._path, dict(self._payload)
        if path:
            try:
                update_registration(self._client, path, payload)
            except Exception as error:  # noqa: BLE001 - re-registered if lost
                logger.warning(f"could not update registration: {error}")

    def on_state(self, state) -> None:
        """Signal only. No ZooKeeper calls here — kazoo's thread must not block."""
        if state == KazooState.LOST:
            with self._lock:
                self._path = None
                # Anything created before this point belongs to a session that
                # has gone. Bumping the generation lets a registration already
                # in flight recognise itself as stale when it returns.
                self._generation += 1
            self._registered.clear()
            self._needed.set()
        elif state == KazooState.CONNECTED:
            self._needed.set()

    def _run(self) -> None:
        while not self._stop.is_set():
            # Bounded wait, so a failed create is retried rather than waiting
            # for a state transition that may never come.
            self._needed.wait(timeout=self.RETRY_SECONDS)
            if self._stop.is_set():
                return
            with self._lock:
                already = self._path is not None
                payload = dict(self._payload)
                generation = self._generation
            if already:
                self._needed.clear()
                continue
            try:
                path = register(
                    self._client, self._paths, self._role, self._identity, payload
                )
            except Exception as error:  # noqa: BLE001 - retried on the next pass
                logger.warning(f"could not register {self._role}: {error}")
                continue

            with self._lock:
                stale = generation != self._generation
                if not stale:
                    # Publish the path *and* the two events under the lock. Done
                    # outside it, a LOST landing in between would clear the path
                    # and ask for a new registration, and the lines below would
                    # then announce success and erase that request.
                    self._path = path
                    self._registered.set()
                    self._needed.clear()
                    fresher = dict(self._payload)

            if stale:
                # The create may well have succeeded — under the *replacement*
                # session. Discarding the path without removing the node would
                # leave a duplicate registration nobody owns.
                logger.info(
                    f"discarding a {self._role} registration from a lost session"
                )
                self._delete_quietly(path)
                self._needed.set()
                continue

            if fresher != payload:
                # update() changed the payload while the create was in flight;
                # the node would otherwise keep the stale values forever.
                update_registration(self._client, path, fresher)
            logger.info(f"registered {self._role} at {path}")

    def _delete_quietly(self, path: str) -> None:
        try:
            self._client.delete(path)
        except Exception as error:  # noqa: BLE001 - best effort
            logger.warning(f"could not remove a stale registration: {error}")


class ConfigWatcher:
    """Keeps a RuntimeConfig in step with a znode, via a one-shot watch.

    The standard data watch fires **once** and must be re-registered, and a
    change can land between the two. That is why the refresh re-reads current
    state rather than trusting the event to carry a value.

    kazoo's own callback thread must not be blocked, so the callback only
    signals; a separate thread does the read and re-registers the watch. kazoo's
    `DataWatch` would handle both for us — this does it by hand because the
    one-shot mechanism is the thing worth seeing. (Modern ZooKeeper also offers
    persistent watches, which is a third option.)
    """

    # How long to wait before re-reading when no watch fired. Without a bound,
    # a failed read would leave the thread parked forever.
    RETRY_SECONDS = 5.0

    def __init__(self, client: KazooClient, path: str, on_value) -> None:
        self._client = client
        self._path = path
        self._on_value = on_value
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._applied = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self, timeout: float = 15) -> bool:
        """Watch for changes, and wait for the first value to be applied.

        Waits rather than trying once: a single failed read would otherwise let
        a worker begin consuming jobs on its environment default while the
        published value sat unread — exactly the behaviour this is meant to
        prevent, and one that looks like success because nothing had changed.
        """
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        self._wake.set()
        return self._applied.wait(timeout)

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _notify(self, event) -> None:
        # Runs on kazoo's thread: signal only, never read or block here.
        self._wake.set()

    def on_state(self, state) -> None:
        """Re-read after a reconnect.

        A watch does not survive a session loss, and the value may have changed
        while this client was away. Waking on CONNECTED re-reads and re-installs
        the watch; without it a reconnected process keeps its old value forever.
        """
        if state == KazooState.CONNECTED:
            self._wake.set()

    def _read_once(self) -> bool:
        """Read, apply and re-install the watch. True if a value was applied."""
        try:
            value, stat = self._client.get(self._path, watch=self._notify)
        except NoNodeError:
            logger.warning(f"{self._path} does not exist yet")
            return False
        except Exception as error:  # noqa: BLE001 - retried by the caller
            logger.warning(f"config read failed: {error}")
            return False

        try:
            self._on_value(value.decode() if value else "", stat.version)
        except Exception as error:  # noqa: BLE001 - a bad value is refused
            # The watch stays installed — a bad value may be corrected, and we
            # must hear about it — but this does *not* count as applied. An
            # earlier version set the flag here, which let a worker start on its
            # environment default: the exact outcome start() exists to prevent.
            logger.warning(f"refusing published config: {error}")
            return False

        self._applied.set()
        return True

    def _run(self) -> None:
        while not self._stop.is_set():
            # Bounded wait, not an indefinite one. A read that fails leaves no
            # watch installed, so nothing would ever wake this thread again —
            # the config would silently stop updating with no error to show for
            # it.
            self._wake.wait(timeout=self.RETRY_SECONDS)
            self._wake.clear()
            if self._stop.is_set():
                return
            self._read_once()


class Leadership:
    """Tracks whether leader-only work may run right now.

    kazoo's connection states are not interchangeable, and the difference is the
    whole release:

      CONNECTED  normal.
      SUSPENDED  connection lost, session may yet survive. Leadership is
                 *unknown*, so leader-only work must stop.
      LOST       session gone, ephemeral nodes deleted, leadership definitely
                 gone. The epoch token is void; re-enter the election.

    Code that only handles LOST keeps working through a partition, which is
    exactly how two processes end up believing they lead at once.

    Cancelling an elected contender does not interrupt the function kazoo is
    running for it, so the work loop has to cooperate by checking `may_work`
    rather than expecting to be stopped.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._connected = True
        # Scoped to the current tenure, not to the process. A session loss voids
        # *this* leadership; it must not stop the process leading again later.
        self._tenure_void = False

    def begin_tenure(self) -> bool:
        """Start a fresh tenure, if the session is currently connected.

        Called after winning the election and before claiming an epoch. Refusing
        while disconnected matters: a tenure begun during SUSPENDED would
        believe it holds a leadership nobody has confirmed.
        """
        with self._lock:
            if not self._connected:
                return False
            self._tenure_void = False
            return True

    def on_state(self, state) -> None:
        with self._lock:
            if state == KazooState.SUSPENDED:
                self._connected = False
                logger.warning("ZooKeeper suspended: leadership is unknown, pausing")
            elif state == KazooState.LOST:
                self._connected = False
                self._tenure_void = True
                logger.warning("ZooKeeper session lost: leadership and epoch void")
            elif state == KazooState.CONNECTED:
                self._connected = True

    @property
    def may_work(self) -> bool:
        with self._lock:
            return self._connected and not self._tenure_void

    @property
    def tenure_void(self) -> bool:
        """Whether this tenure is finished for good, rather than paused."""
        with self._lock:
            return self._tenure_void
