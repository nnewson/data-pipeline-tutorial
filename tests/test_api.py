"""The API's contracts, against fake stores.

Every route: what it returns, what "nothing there" looks like, and what a store
failure looks like. Then the bulkhead, decisively: every Cassandra slot held at
a barrier, and the other routes still answering.
"""

import json
import threading
import time
from collections import namedtuple
from contextlib import ExitStack, asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

import anyio
import httpx2
import pytest
import redis
from cassandra.cluster import NoHostAvailable
from fastapi.testclient import TestClient
from kazoo.exceptions import ConnectionLoss, NoNodeError
from kazoo.handlers.threading import KazooTimeoutError
from starlette.websockets import WebSocketDisconnect

from pipeline import api, fanout
from pipeline.api import Stores
from pipeline.coordination import Paths

ROOT = "/t"
PREFIX = "p:"

Row = namedtuple("Row", "event_time event_id page written_at")


@pytest.fixture
def anyio_backend():
    return "asyncio"


class StubBridge:
    state = "subscribed"


@asynccontextmanager
async def fake_live(prefix):
    yield api.Live(hub=fanout.Hub(), bridge=StubBridge(), origins=api.allowed_origins())


def make_app(**kwargs):
    return api.create_app(open_live=fake_live, **kwargs)


class FakeRedis:
    def __init__(self, data=None):
        self.data = dict(data or {})
        self.fail: Exception | None = None
        self.closed = False
        self.calls = 0

    def _call(self):
        self.calls += 1
        if self.fail is not None:
            raise self.fail

    def scan(self, cursor=0, match=None, count=None):
        raise AssertionError("the API must not sweep the keyspace")

    def mget(self, keys):
        self._call()
        return [self.data.get(key) for key in keys]

    def get(self, key):
        self._call()
        return self.data.get(key)

    def close(self):
        self.closed = True


class FakeSession:
    def __init__(self, rows=()):
        self.rows = list(rows)
        self.fail: Exception | None = None
        self.timeouts: list[float] = []
        self.hold: threading.Event | None = None
        self.in_flight = 0
        self._lock = threading.Lock()

    def execute(self, query, parameters=None, timeout=None):
        with self._lock:
            self.timeouts.append(timeout)
            self.in_flight += 1
        try:
            if self.fail is not None:
                raise self.fail
            if self.hold is not None:
                self.hold.wait(timeout=10)
            return list(self.rows)
        finally:
            with self._lock:
                self.in_flight -= 1


class Done:
    def __init__(self, value=None, error=None):
        self.value, self.error = value, error

    def get(self, timeout=None):
        if self.error is not None:
            raise self.error
        return self.value

    def rawlink(self, callback):
        callback(self)


class FakeZookeeper:
    """A tree of znodes, answered through kazoo's async interface only."""

    handler = SimpleNamespace(timeout_exception=KazooTimeoutError)

    def __init__(self, nodes=None):
        self.nodes: dict[str, bytes] = dict(nodes or {})
        self.connected = True
        self.fail: Exception | None = None
        self.calls = 0
        self.stopped = self.closed = False

    def _answer(self, value=None, error=None):
        self.calls += 1
        return Done(value, self.fail or error)

    def get_async(self, path, watch=None):
        if path not in self.nodes:
            return self._answer(error=NoNodeError(path))
        return self._answer((self.nodes[path], SimpleNamespace(version=7)))

    def exists_async(self, path, watch=None):
        present = path in self.nodes or any(
            n.startswith(path + "/") for n in self.nodes
        )
        return self._answer(SimpleNamespace() if present else None)

    def get_children_async(self, path, watch=None):
        children = {
            node[len(path) + 1 :].split("/")[0]
            for node in self.nodes
            if node.startswith(path + "/")
        }
        return self._answer(sorted(children))

    def stop(self):
        self.stopped = True

    def close(self):
        self.closed = True


def leader(epoch=2, identity="host-1"):
    return json.dumps(
        {"identity": identity, "epoch": epoch, "since": "2026-09-27T14:15:51+00:00"}
    ).encode()


def snapshot(epoch=2, **fields):
    return json.dumps({"epoch": epoch, "at": 1790000000.25, **fields}).encode()


def cluster_nodes(leader_epoch=2, snapshot_epoch=2):
    paths = Paths(root=ROOT)
    nodes = {
        paths.snapshot: snapshot(
            snapshot_epoch, pageviews=10, cassandra_reachable=True
        ),
        f"{paths.registry}/worker/0000000001": json.dumps(
            {"identity": "w1", "delay": 0.5, "config_version": 3}
        ).encode(),
        f"{paths.registry}/consumer/0000000002": json.dumps(
            {"identity": "c1", "group": "pipeline"}
        ).encode(),
    }
    if leader_epoch is not None:
        nodes[paths.leader] = leader(leader_epoch)
    return nodes


def make_stores(redis_data=None, rows=(), nodes=None):
    return Stores(
        redis=FakeRedis(redis_data),
        cassandra=FakeSession(rows),
        zookeeper=FakeZookeeper(cluster_nodes() if nodes is None else nodes),
        paths=Paths(root=ROOT),
        prefix=PREFIX,
    )


@pytest.fixture
def stores():
    return make_stores(
        redis_data={
            f"{PREFIX}pageviews:/docs": "3",
            f"{PREFIX}pageviews:/": "1",
            f"{PREFIX}user:last_page:ada": "/docs",
            "someone-else:pageviews:/docs": "99",
        },
        rows=[
            Row(
                datetime(2026, 9, 27, 14, 15, 51, 123000),  # naive, as the driver
                "e1",
                "/docs",
                1790000000123456,
            )
        ],
    )


@pytest.fixture
def client(stores):
    with TestClient(make_app(open_stores=lambda stack: stores)) as client:
        yield client


def is_utc_iso(value: str) -> bool:
    return value.endswith("Z") or value.endswith("+00:00")


# --- /health ----------------------------------------------------------------


def test_health_is_liveness_and_touches_no_store(client, stores):
    stores.redis.fail = stores.cassandra.fail = RuntimeError("must not be called")
    stores.zookeeper.connected = False

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


# --- Redis ------------------------------------------------------------------


def test_page_counts_default_to_the_producers_catalogue(client, stores):
    body = client.get("/counts/pages").json()

    # "/pricing" and "/checkout" are in the catalogue but hold no counter.
    assert body["counts"] == {"/docs": 3, "/": 1}
    assert is_utc_iso(body["observed_at"])
    assert stores.redis.calls == 1, "one MGET, not a sweep"


def test_page_counts_read_exactly_the_pages_asked_for(client):
    body = client.get("/counts/pages?page=/docs&page=/nope&page=/docs").json()

    assert body["counts"] == {"/docs": 3}


def test_page_counts_never_read_another_prefix(client):
    """someone-else:pageviews:/docs holds 99; this API's prefix holds 3."""
    assert client.get("/counts/pages?page=/docs").json()["counts"] == {"/docs": 3}


def test_page_selection_is_bounded_in_the_contract(client):
    too_many = "&".join(f"page=/p{n}" for n in range(api.PAGES_PER_REQUEST_MAX + 1))

    assert client.get(f"/counts/pages?{too_many}").status_code == 422
    assert client.get(f"/counts/pages?page={'x' * 129}").status_code == 422
    assert client.get("/counts/pages?page=").status_code == 422


def test_no_counters_is_an_empty_object_not_an_error(client, stores):
    stores.redis.data.clear()

    assert client.get("/counts/pages").json()["counts"] == {}


def test_last_page_returns_the_value_redis_holds(client):
    body = client.get("/users/ada/last-page").json()

    assert body["user_id"] == "ada"
    assert body["page"] == "/docs"


def test_no_last_page_is_a_404(client):
    """The resource is the value; with no value, the resource is absent."""
    response = client.get("/users/nobody/last-page")

    assert response.status_code == 404


def test_a_redis_outage_is_a_503_naming_redis(client, stores):
    stores.redis.fail = redis.ConnectionError("refused")

    for path in ("/counts/pages", "/users/ada/last-page"):
        response = client.get(path)
        assert response.status_code == 503
        assert response.json() == {"detail": "redis unavailable"}


def test_a_redis_timeout_is_a_503_too(client, stores):
    stores.redis.fail = redis.TimeoutError("Timeout reading from socket")

    assert client.get("/counts/pages").json() == {"detail": "redis unavailable"}


def test_a_redis_bug_is_a_500_not_an_outage(stores):
    stores.redis.fail = redis.ResponseError("WRONGTYPE")
    app = make_app(open_stores=lambda stack: stores)

    with TestClient(app, raise_server_exceptions=False) as client:
        assert client.get("/counts/pages").status_code == 500


# --- Cassandra --------------------------------------------------------------


def test_events_are_returned_with_utc_times(client):
    event = client.get("/users/ada/events").json()["events"][0]

    assert event["event_id"] == "e1"
    assert event["page"] == "/docs"
    # The driver's naive datetime is UTC: the offset must survive serialisation.
    assert datetime.fromisoformat(event["event_time"]) == datetime(
        2026, 9, 27, 14, 15, 51, 123000, tzinfo=UTC
    )
    # writetime() is microseconds, converted exactly rather than through a float.
    assert datetime.fromisoformat(event["written_at"]) == datetime(
        2026, 9, 21, 14, 13, 20, 123456, tzinfo=UTC
    )


def test_no_events_is_an_empty_list_not_a_404(client, stores):
    stores.cassandra.rows = []

    response = client.get("/users/nobody/events")

    assert response.status_code == 200
    assert response.json()["events"] == []


@pytest.mark.parametrize("limit", [0, 101, -1])
def test_the_limit_is_bounded_in_the_contract(client, limit):
    assert client.get(f"/users/ada/events?limit={limit}").status_code == 422


def test_an_overlong_user_id_is_refused(client):
    assert client.get(f"/users/{'x' * 129}/last-page").status_code == 422


def test_the_cassandra_query_is_bounded_by_the_request_deadline(client, stores):
    client.get("/users/ada/events")

    (timeout,) = stores.cassandra.timeouts
    assert 0 < timeout <= api.READ_DEADLINE_SECONDS


def test_a_cassandra_outage_is_a_503_naming_cassandra(client, stores):
    stores.cassandra.fail = NoHostAvailable("no hosts", {})

    response = client.get("/users/ada/events")

    assert response.status_code == 503
    assert response.json() == {"detail": "cassandra unavailable"}


# --- ZooKeeper --------------------------------------------------------------


def test_cluster_reports_leader_snapshot_and_registrations(client):
    body = client.get("/cluster").json()

    assert body["leader"]["identity"] == "host-1"
    assert body["snapshot"]["epoch"] == 2
    assert body["snapshot"]["version"] == 7
    assert body["snapshot"]["pageviews"] == 10
    assert datetime.fromisoformat(body["snapshot"]["at"]) == datetime(
        2026, 9, 21, 14, 13, 20, 250000, tzinfo=UTC
    )
    assert body["snapshot_matches_leader"] is True
    assert body["registrations"]["worker"][0]["config_version"] == 3
    assert body["registrations"]["consumer"][0]["group"] == "pipeline"
    assert body["registrations"]["coordinator"] == []


def test_a_snapshot_from_the_previous_epoch_does_not_match(stores):
    stores.zookeeper.nodes = cluster_nodes(leader_epoch=3, snapshot_epoch=2)
    with TestClient(make_app(open_stores=lambda stack: stores)) as client:
        body = client.get("/cluster").json()

    assert body["leader"]["epoch"] == 3
    assert body["snapshot"]["epoch"] == 2
    assert body["snapshot_matches_leader"] is False


def test_no_leader_and_an_initial_snapshot_is_not_a_match(stores):
    """`cluster init` writes {"epoch": null}: None == None is not agreement."""
    stores.zookeeper.nodes = cluster_nodes(leader_epoch=None, snapshot_epoch=None)
    with TestClient(make_app(open_stores=lambda stack: stores)) as client:
        body = client.get("/cluster").json()

    assert body["leader"] is None
    assert body["snapshot"]["epoch"] is None
    assert body["snapshot_matches_leader"] is False


def test_a_leader_without_a_snapshot_epoch_is_not_a_match():
    assert api.snapshot_matches_leader({"epoch": 2}, {"epoch": None}) is False
    assert api.snapshot_matches_leader({"epoch": None}, {"epoch": None}) is False
    assert api.snapshot_matches_leader(None, {"epoch": 2}) is False
    assert api.snapshot_matches_leader({"epoch": 2}, None) is False
    assert api.snapshot_matches_leader({"epoch": 2}, {"epoch": 2}) is True


def test_an_uninitialised_tree_is_reported_as_absent(stores):
    stores.zookeeper.nodes = {}
    with TestClient(make_app(open_stores=lambda stack: stores)) as client:
        body = client.get("/cluster").json()

    assert body["leader"] is None
    assert body["snapshot"] is None
    assert body["snapshot_matches_leader"] is False
    assert body["registrations"] == {"coordinator": [], "consumer": [], "worker": []}


def test_a_disconnected_zookeeper_is_refused_without_a_request(client, stores):
    stores.zookeeper.connected = False

    response = client.get("/cluster")

    assert response.status_code == 503
    assert response.json() == {"detail": "zookeeper unavailable"}
    assert stores.zookeeper.calls == 0


def test_a_zookeeper_read_failure_is_a_503(client, stores):
    stores.zookeeper.fail = ConnectionLoss()

    assert client.get("/cluster").json() == {"detail": "zookeeper unavailable"}


# --- The contract, as published ---------------------------------------------


def test_openapi_documents_every_route_and_its_failures(client):
    spec = client.get("/openapi.json").json()

    assert set(spec["paths"]) == {
        "/health",
        "/counts/pages",
        "/users/{user_id}/last-page",
        "/users/{user_id}/events",
        "/cluster",
    }
    for path in ("/counts/pages", "/users/{user_id}/events", "/cluster"):
        assert "503" in spec["paths"][path]["get"]["responses"]
    assert "404" in spec["paths"]["/users/{user_id}/last-page"]["get"]["responses"]
    assert "Replay-sensitive" in json.dumps(spec)


# --- Startup and shutdown ---------------------------------------------------


def test_startup_closes_what_it_opened_when_a_later_store_fails(monkeypatch):
    opened = FakeRedis()
    monkeypatch.setattr(api.redis_store, "connect", lambda **options: opened)

    def cassandra_down(**kwargs):
        raise NoHostAvailable("no hosts", {})

    monkeypatch.setattr(api.cassandra_store, "connect", cassandra_down)

    with ExitStack() as stack, pytest.raises(NoHostAvailable):
        api.open_stores(stack)

    assert opened.closed


def test_the_api_redis_client_is_bounded_and_does_not_retry(monkeypatch):
    seen = {}

    def connect(**options):
        seen.update(options)
        return FakeRedis()

    monkeypatch.setattr(api.redis_store, "connect", connect)
    monkeypatch.setattr(
        api.cassandra_store,
        "connect",
        lambda **kwargs: (SimpleNamespace(shutdown=lambda: None), FakeSession()),
    )
    monkeypatch.setattr(api.coordination, "connect", lambda **kwargs: FakeZookeeper())

    with ExitStack() as stack:
        api.open_stores(stack)

    assert seen["socket_timeout"] == 1.0
    assert seen["socket_connect_timeout"] == 1.0
    assert seen["retry"].get_retries() == 0


def test_startup_makes_fewer_attempts_than_the_pipeline(monkeypatch):
    attempts = {}

    def record(name, value):
        def connect(**kwargs):
            attempts[name] = kwargs["retries"]
            return value

        return connect

    monkeypatch.setattr(api.redis_store, "connect", record("redis", FakeRedis()))
    monkeypatch.setattr(
        api.cassandra_store,
        "connect",
        record("cassandra", (SimpleNamespace(shutdown=lambda: None), FakeSession())),
    )
    monkeypatch.setattr(api.coordination, "connect", record("zk", FakeZookeeper()))

    with ExitStack() as stack:
        api.open_stores(stack)

    assert attempts == dict.fromkeys(("redis", "cassandra", "zk"), 3)


def test_shutdown_closes_every_store(stores):
    with TestClient(make_app(open_stores=api_open(stores))):
        pass

    assert stores.redis.closed
    assert stores.zookeeper.stopped and stores.zookeeper.closed


def api_open(stores):
    def open_stores(stack):
        stack.callback(stores.redis.close)
        stack.callback(stores.zookeeper.close)
        stack.callback(stores.zookeeper.stop)
        return stores

    return open_stores


# --- The bulkhead, decisively -----------------------------------------------


@pytest.mark.anyio
async def test_a_saturated_cassandra_cannot_take_the_other_routes_with_it(stores):
    """Hold every Cassandra slot at a barrier and look at everything else.

    The next Cassandra request is refused as busy *without calling the store*;
    Redis and /health still answer; and once the barrier opens, Cassandra's
    capacity comes back.
    """
    app = make_app(open_stores=lambda stack: stores)
    app.state.stores = stores
    app.state.bulkheads = api.make_bulkheads(stores)
    stores.cassandra.hold = threading.Event()
    limit = api.CASSANDRA_LIMIT
    held: list[int] = []

    transport = httpx2.ASGITransport(app=app)
    async with httpx2.AsyncClient(transport=transport, base_url="http://api") as http:

        async def occupy():
            response = await http.get("/users/ada/events")
            held.append(response.status_code)

        async with anyio.create_task_group() as group:
            for _ in range(limit):
                group.start_soon(occupy)
            with anyio.fail_after(5):
                while stores.cassandra.in_flight < limit:
                    await anyio.sleep(0.01)

            calls_before = len(stores.cassandra.timeouts)
            asked = time.monotonic()
            refused = await http.get("/users/ada/events")
            # At once, not after a wait: a wait that ended in refusal would
            # still be a 503. Measured rather than bounded with fail_after,
            # which cannot interrupt a wait that blocks the event loop.
            assert time.monotonic() - asked < 0.5
            assert refused.status_code == 503
            assert refused.json() == {"detail": "cassandra busy"}
            assert len(stores.cassandra.timeouts) == calls_before, "store was called"

            assert (await http.get("/counts/pages")).status_code == 200
            assert (await http.get("/users/ada/last-page")).status_code == 200
            assert (await http.get("/health")).status_code == 200

            stores.cassandra.hold.set()

        assert held == [200] * limit
        stores.cassandra.hold = None
        assert (await http.get("/users/ada/events")).status_code == 200


def test_main_connects_before_uvicorn_and_hands_cleanup_to_the_app(monkeypatch):
    """Connecting first keeps signals ordinary; the lifespan still closes."""
    order = []
    stores = make_stores()

    def open_stores(stack):
        order.append("connected")
        stack.callback(order.append, "closed")
        return stores

    def run(app, **options):
        order.append("serving")
        with TestClient(app) as client:  # runs the app's lifespan
            assert client.get("/health").status_code == 200
        order.append("stopped")

    monkeypatch.setattr(api, "open_stores", open_stores)
    monkeypatch.setattr(api, "open_live", fake_live)
    monkeypatch.setattr(api.uvicorn, "run", run)

    api.main()

    assert order == ["connected", "serving", "closed", "stopped"]


def test_uvicorn_exiting_before_the_lifespan_still_closes_the_stores(monkeypatch):
    """The hand-over only moves ownership once a lifespan is running."""
    closed, workers = [], []
    stores = make_stores()

    def open_stores(stack):
        stack.callback(closed.append, "redis")
        return stores

    def exits_early(app, **options):
        # The real config, so an environment that asks for workers is honoured
        # or overridden exactly as uvicorn itself would.
        workers.append(api.uvicorn.Config(app, **options).workers)
        raise SystemExit(3)  # what WEB_CONCURRENCY=2 did, before any lifespan

    monkeypatch.setenv("WEB_CONCURRENCY", "2")
    monkeypatch.setattr(api, "open_stores", open_stores)
    monkeypatch.setattr(api, "open_live", fake_live)
    monkeypatch.setattr(api.uvicorn, "run", exits_early)

    with pytest.raises(SystemExit):
        api.main()

    assert closed == ["redis"]
    assert workers == [1], "WEB_CONCURRENCY must not override the single process"


# --- Live notifications: the WebSocket ---------------------------------------


def live_app(stores, *, limit=100, state="subscribed", closed=None):
    hub = fanout.Hub(limit=limit)

    class Bridge:
        pass

    bridge = Bridge()
    bridge.state = state

    @asynccontextmanager
    async def open_live(prefix):
        try:
            yield api.Live(hub=hub, bridge=bridge, origins=api.allowed_origins())
        finally:
            if closed is not None:
                closed.append(prefix)

    return api.create_app(open_stores=lambda stack: stores, open_live=open_live), hub


OURS = {"origin": "http://localhost:8000"}


@pytest.mark.parametrize(
    "headers",
    [
        {"origin": "http://evil.example"},
        {},
        {"origin": "null"},
        {"origin": "https://localhost:8000"},
        {"origin": "http://localhost:8001"},
    ],
)
def test_a_socket_from_anywhere_else_is_refused_at_the_handshake(stores, headers):
    app, hub = live_app(stores)
    with TestClient(app) as client:
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect("/ws/pageviews", headers=headers):
                pass
        assert hub.admitted == 0, "a refused socket takes no slot"


@pytest.mark.parametrize("origin", sorted(api.allowed_origins()))
def test_ready_comes_first_and_states_the_subscription(stores, origin):
    app, _ = live_app(stores, state="interrupted")
    with TestClient(app) as client:
        with client.websocket_connect(
            "/ws/pageviews", headers={"origin": origin}
        ) as ws:
            assert ws.receive_json() == {"type": "ready", "bridge": "interrupted"}


def test_a_broadcast_reaches_an_enrolled_socket(stores):
    app, hub = live_app(stores)
    with TestClient(app) as client:
        with client.websocket_connect("/ws/pageviews", headers=OURS) as ws:
            ws.receive_json()
            client.portal.call(hub.broadcast, '{"type": "pageview", "offset": 4}')
            assert ws.receive_json() == {"type": "pageview", "offset": 4}


def test_the_socket_allowance_refuses_at_the_handshake_and_comes_back(stores):
    app, hub = live_app(stores, limit=1)
    with TestClient(app) as client:
        with client.websocket_connect("/ws/pageviews", headers=OURS) as first:
            first.receive_json()
            with pytest.raises(WebSocketDisconnect):
                with client.websocket_connect("/ws/pageviews", headers=OURS):
                    pass
        with client.websocket_connect("/ws/pageviews", headers=OURS) as again:
            assert again.receive_json()["type"] == "ready"


def test_sockets_come_and_go_cleanly(stores):
    """Regression: a cancellation once leaked out of anyio's scope on disconnect."""
    app, hub = live_app(stores)
    with TestClient(app) as client:
        for _ in range(25):
            with client.websocket_connect("/ws/pageviews", headers=OURS) as ws:
                ws.receive_json()
        assert hub.admitted == 0


def test_the_page_and_its_module_are_served_outside_the_contract(stores):
    app, _ = live_app(stores)
    with TestClient(app) as client:
        page = client.get("/live")
        module = client.get("/live.mjs")
        paths = client.get("/openapi.json").json()["paths"]

    assert page.status_code == 200 and page.headers["content-type"].startswith(
        "text/html"
    )
    assert 'from "./live.mjs"' in page.text
    assert module.headers["content-type"].startswith("text/javascript")
    assert "export function createTracker" in module.text
    assert "/live" not in paths and "/live.mjs" not in paths


def test_the_page_never_interpolates_event_fields_as_html():
    """Notifications came from Redis, which anyone with access can write to."""
    page = api._static("live.html")
    assert "innerHTML" not in page and "insertAdjacentHTML" not in page


def test_shutdown_closes_the_live_path(stores):
    closed = []
    app, _ = live_app(stores, closed=closed)
    with TestClient(app):
        assert closed == []
    assert closed == [stores.prefix]
