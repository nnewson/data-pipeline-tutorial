"""Guardrails shared by the whole suite.

Unit tests must not open sockets. Three times now, adding a connection to a code
path a test already exercised turned a 0.4-second suite into a 30-second one —
the retry loop makes a missing stub look like a hang rather than a failure.

This turns that into an immediate, named error instead. A test that genuinely
needs the retry helper opts in with `@pytest.mark.allow_connect`.
"""

import pytest

from pipeline import (
    cassandra_store,
    coordination,
    jobs_queue,
    kafka_consumer,
    producer,
    redis_store,
)


class RealConnectionAttempted(BaseException):
    """Raised when a unit test would have opened a socket."""


# The modules holding their own reference to the retry helper. Patching these
# covers anything reaching the network through their connect() functions —
# schema's use of cassandra_store and jobs' use of jobs_queue, for instance.
#
# It does not cover everything, and the gaps are worth naming rather than
# implying. `topics` is one: it calls `ensure_topic`, which uses the central
# `pipeline.wait_for_connection` rather than a module-level copy, so it is not
# patched here. `wait_for_topic` is the same, and smoke_test constructs Kafka
# clients directly. Those call sites are stubbed per test.
CONNECTING_MODULES = (
    redis_store,
    cassandra_store,
    jobs_queue,
    producer,
    kafka_consumer,
    coordination,
)


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "allow_connect: test may call wait_for_connection"
    )


@pytest.fixture(autouse=True)
def no_real_connections(request, monkeypatch):
    if request.node.get_closest_marker("allow_connect"):
        return

    def refuse(name, connect, *args, **kwargs):
        # BaseException on purpose: several call sites catch Exception because
        # their cleanup is best effort, and they would swallow this and pass.
        # A harness violation is not an application error.
        raise RealConnectionAttempted(
            f"unit test tried to open a real {name} connection. "
            "Stub the helper that connects, or mark the test allow_connect."
        )

    patched = 0
    for module in CONNECTING_MODULES:
        if hasattr(module, "wait_for_connection"):
            monkeypatch.setattr(module, "wait_for_connection", refuse)
            patched += 1

    # Every listed module must still expose the helper: one that stops importing
    # it would silently drop out of the guard, which is how the retry loop got
    # back in before. This proves each entry is active — not that the list names
    # every path to the network.
    assert patched == len(CONNECTING_MODULES), (
        f"{len(CONNECTING_MODULES) - patched} guarded module(s) no longer expose "
        "wait_for_connection; update CONNECTING_MODULES"
    )

    # The API's Redis subscription does not go through wait_for_connection: the
    # bridge retries for ever on its own, so a test that started a real one
    # would leave it reconnecting in the background rather than fail.
    def refuse_subscription(*args, **kwargs):
        raise RealConnectionAttempted(
            "unit test tried to open a real Redis subscription. "
            "Pass create_app a fake open_live, or mark the test allow_connect."
        )

    from pipeline import api, bridge

    monkeypatch.setattr(bridge, "open_client", refuse_subscription)
    monkeypatch.setattr(api, "open_subscription_client", refuse_subscription)

    # Flink: the REST API and `docker compose exec` are paths to a real cluster.
    # Tests of those two functions mark themselves allow_connect and stub the
    # layer beneath.
    def refuse_flink(*args, **kwargs):
        raise RealConnectionAttempted(
            "unit test tried to reach the Flink cluster. "
            "Stub flink_cluster.request or submit, or mark the test allow_connect."
        )

    from pipeline import flink_cluster

    monkeypatch.setattr(flink_cluster, "request", refuse_flink)
    monkeypatch.setattr(flink_cluster, "submit", refuse_flink)

    # Kafka clients are constructed directly rather than through the helper, so
    # the guard cannot reach them; those call sites are stubbed per test.


# --- shared ZooKeeper double -------------------------------------------------
#
# Lives here rather than in one test module because `tests` is not a package,
# so modules cannot import from each other.

from kazoo.exceptions import BadVersionError, NodeExistsError, NoNodeError  # noqa: E402


class FakeStat:
    def __init__(self, version=0):
        self.version = version


class FakeClient:
    """Enough ZooKeeper to exercise the logic, with failures on demand."""

    def __init__(self):
        self.data: dict[str, bytes] = {}
        self.versions: dict[str, int] = {}
        self.ephemeral: set[str] = set()
        self.deleted: list[str] = []
        self.created: list[str] = []
        self.set_calls: list[str] = []
        self.fail_next_get: Exception | None = None

    def ensure_path(self, path):
        self.data.setdefault(path, b"")
        self.versions.setdefault(path, 0)

    def exists(self, path):
        return path in self.data

    def create(self, path, value=b"", ephemeral=False, sequence=False):
        if sequence:
            path = f"{path}{len(self.created):010d}"
        if path in self.data:
            raise NodeExistsError(path)
        self.data[path] = value
        self.versions[path] = 0
        self.created.append(path)
        if ephemeral:
            self.ephemeral.add(path)
        return path

    def get(self, path, watch=None):
        if self.fail_next_get is not None:
            error, self.fail_next_get = self.fail_next_get, None
            raise error
        if path not in self.data:
            raise NoNodeError(path)
        return self.data[path], FakeStat(self.versions[path])

    def set(self, path, value, version=-1):
        self.set_calls.append(path)
        if path not in self.data:
            raise NoNodeError(path)
        if version != -1 and version != self.versions[path]:
            raise BadVersionError(path)
        self.data[path] = value
        self.versions[path] += 1
        return FakeStat(self.versions[path])

    def delete(self, path, recursive=False, version=-1):
        if version != -1 and self.versions.get(path) != version:
            # Honoured, so the read/delete replacement race is actually tested
            # rather than only the different-identity case.
            raise BadVersionError(path)
        self.deleted.append(path)
        self.data.pop(path, None)
        self.versions.pop(path, None)
        self.ephemeral.discard(path)

    def get_children(self, path):
        prefix = f"{path}/"
        return [k[len(prefix) :] for k in self.data if k.startswith(prefix)]

    def transaction(self):
        return FakeTransaction(self)


class FakeTransaction:
    def __init__(self, client):
        self._client = client
        self._checks = []
        self._sets = []

    def check(self, path, version):
        self._checks.append((path, version))

    def set_data(self, path, value):
        self._sets.append((path, value))

    def commit(self):
        for path, version in self._checks:
            if self._client.versions.get(path) != version:
                # kazoo returns results; it does not raise for every failure.
                return [BadVersionError(path), RuntimeError("rolled back")]
        for path, value in self._sets:
            self._client.data[path] = value
            self._client.versions[path] += 1
        return [True for _ in self._checks + self._sets]


@pytest.fixture
def zookeeper():
    """A fake ZooKeeper with the pipeline's tree already initialised."""
    from pipeline import coordination

    client = FakeClient()
    paths = coordination.Paths(root="/t")
    coordination.initialise(client, paths, worker_delay=0.5)
    return client, paths
