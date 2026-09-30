"""An HTTP API over the three stores, stating only what each can prove.

Each store answers a different question with different freshness and
consistency properties, and an HTTP API is where a system either states those
properties or quietly averages them away. The rule here: **expose only metadata
the source genuinely owns**, and where a view is non-atomic or stale, make that
visible rather than retrying until it looks tidy.

One endpoint, one store. No writes, no fan-out reads, no cache.
"""

import logging
from contextlib import ExitStack, asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal

import anyio.to_thread
import redis
import uvicorn
from cassandra import OperationTimedOut, RequestExecutionException
from cassandra.cluster import NoHostAvailable, Session
from cassandra.connection import ConnectionException
from fastapi import FastAPI, HTTPException, Path, Query, Request
from fastapi.responses import JSONResponse
from kazoo.client import KazooClient
from kazoo.exceptions import (
    ConnectionClosedError,
    ConnectionLoss,
    NoNodeError,
    SessionExpiredError,
)
from kazoo.handlers.threading import KazooTimeoutError
from pydantic import BaseModel, Field, StringConstraints
from redis.backoff import NoBackoff
from redis.retry import Retry

from pipeline import cassandra_store, coordination, redis_store
from pipeline.bulkhead import Budget, Bulkhead, Busy, Unavailable
from pipeline.config import (
    API_PORT,
    CASSANDRA_KEYSPACE,
    REDIS_KEY_PREFIX,
    ZOOKEEPER_ROOT,
)
from pipeline.coordination import ROLES, Paths
from pipeline.producer import PAGES

logger = logging.getLogger("api")

# Tutorial defaults: a reasonable place to start measuring, not derived from
# anything. The two Redis routes share one allowance because they share one
# client, one pool and one server. ZooKeeper's is smaller: one session on one
# connection, served in order, so more concurrency adds queueing, not throughput.
REDIS_LIMIT = 8
CASSANDRA_LIMIT = 8
ZOOKEEPER_LIMIT = 4

# One deadline per request, spent across every read the route makes.
READ_DEADLINE_SECONDS = 2.0

# redis-py's defaults are a 5s socket timeout and ten retries with backoff: one
# GET against a paused server was measured failing after 57.9s. No retries, so a
# restart can cost one transient 503, which the contract allows; the retained
# client recovers on the next request.
REDIS_OPTIONS = {
    "socket_timeout": 1.0,
    "socket_connect_timeout": 1.0,
    "retry": Retry(NoBackoff(), 0),
}

# kazoo's synchronous calls take no timeout at all, so every read gets one.
ZOOKEEPER_CALL_TIMEOUT_SECONDS = 1.0

EVENTS_LIMIT_MAX = 100
USER_ID_MAX_LENGTH = 128
PAGES_PER_REQUEST_MAX = 20
PAGE_MAX_LENGTH = 128

# Fewer than the ten every pipeline process makes. A shared retry helper does
# not need identical patience: the documented workflow already waits for the
# stores to be healthy, and ten attempts against a paused ZooKeeper kept the API
# without a listener for three minutes.
STARTUP_ATTEMPTS = 3

# Only failures that mean "this store cannot answer" become a 503. Anything else
# — a malformed query, a wrong-type key — is a bug, and a 500 says so.
REDIS_FAILURES = (redis.ConnectionError, redis.TimeoutError, OSError)
CASSANDRA_FAILURES = (
    RequestExecutionException,
    OperationTimedOut,
    NoHostAvailable,
    ConnectionException,
    OSError,
)
ZOOKEEPER_FAILURES = (
    ConnectionLoss,
    SessionExpiredError,
    ConnectionClosedError,
    KazooTimeoutError,
)


@dataclass
class Stores:
    """The clients every request shares, held for the life of the process."""

    redis: redis.Redis
    cassandra: Session
    zookeeper: KazooClient
    paths: Paths
    prefix: str


@dataclass
class Bulkheads:
    redis: Bulkhead
    cassandra: Bulkhead
    zookeeper: Bulkhead


def open_stores(stack: ExitStack) -> Stores:
    """Connect to all three stores, registering each for cleanup as it opens.

    A barrier: the API serves nothing until every store has answered once, as
    every other process in the repo does. Registration happens the moment each
    resource exists, so a failure part-way through closes what was opened.
    """
    redis_client = redis_store.connect(retries=STARTUP_ATTEMPTS, **REDIS_OPTIONS)
    stack.callback(redis_client.close)

    cluster, session = cassandra_store.connect(
        keyspace=CASSANDRA_KEYSPACE, retries=STARTUP_ATTEMPTS
    )
    stack.callback(cluster.shutdown)

    zookeeper = coordination.connect(retries=STARTUP_ATTEMPTS)
    stack.callback(zookeeper.close)
    stack.callback(zookeeper.stop)  # LIFO: stop runs before close

    return Stores(
        redis=redis_client,
        cassandra=session,
        zookeeper=zookeeper,
        paths=Paths(root=ZOOKEEPER_ROOT),
        prefix=REDIS_KEY_PREFIX,
    )


def make_bulkheads(stores: Stores) -> Bulkheads:
    """One per store. Must run inside the event loop."""
    return Bulkheads(
        redis=Bulkhead("redis", REDIS_LIMIT, READ_DEADLINE_SECONDS, REDIS_FAILURES),
        cassandra=Bulkhead(
            "cassandra", CASSANDRA_LIMIT, READ_DEADLINE_SECONDS, CASSANDRA_FAILURES
        ),
        zookeeper=Bulkhead(
            "zookeeper",
            ZOOKEEPER_LIMIT,
            READ_DEADLINE_SECONDS,
            ZOOKEEPER_FAILURES,
            per_call_seconds=ZOOKEEPER_CALL_TIMEOUT_SECONDS,
            # While kazoo is disconnected it queues new requests rather than
            # failing them, so refusing here is what stops a backlog forming.
            connected=lambda: stores.zookeeper.connected,
        ),
    )


# --- Time -------------------------------------------------------------------

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

OBSERVED_AT = (
    "When the API finished reading, by the API's clock. Not the source's "
    "freshness: none of these stores records when a value was last brought up "
    "to date."
)


def now() -> datetime:
    return datetime.now(UTC)


def as_utc(value: datetime) -> datetime:
    """Attach UTC to the driver's naive datetimes, which are UTC.

    Without this the offset silently vanishes from the serialised value, and a
    client has to guess.
    """
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def from_microseconds(value: int) -> datetime:
    """Cassandra's writetime(), exactly: integer arithmetic, not a float."""
    return EPOCH + timedelta(microseconds=value)


# --- Response models ----------------------------------------------------------


class Health(BaseModel):
    status: Literal["ok"]


class ErrorDetail(BaseModel):
    detail: str


class PageCounts(BaseModel):
    counts: dict[str, int] = Field(
        description=(
            "Views per requested page, from Redis INCR. A page with no counter "
            "is left out. Replay-sensitive: an event processed twice is counted "
            "twice, so a sum of these is cheap but not a correct total after a "
            "replay."
        )
    )
    observed_at: datetime = Field(description=OBSERVED_AT)


class LastPage(BaseModel):
    user_id: str
    page: str = Field(description="The last page Redis holds for this user.")
    observed_at: datetime = Field(description=OBSERVED_AT)


class Event(BaseModel):
    event_time: datetime
    event_id: str
    page: str
    written_at: datetime = Field(
        description=(
            "The page cell's write timestamp, writetime(page). Generated "
            "client-side, so it records when the consumer issued the write, by "
            "the consumer host's clock. It advances when an event is replayed, "
            "while the event itself stays the same."
        )
    )


class UserEvents(BaseModel):
    user_id: str
    events: list[Event] = Field(description="Most recent first.")
    observed_at: datetime = Field(description=OBSERVED_AT)


class Leader(BaseModel):
    identity: str
    epoch: int = Field(description="The fencing token this leader writes with.")
    since: datetime | None = None


class Snapshot(BaseModel):
    """What the leader last recorded. Every field is the leader's claim."""

    version: int = Field(description="The snapshot znode's version.")
    epoch: int | None = Field(description="The epoch it was written under.")
    at: datetime | None = Field(description="When the leader wrote it, by its clock.")
    pageviews: int | None = None
    pages: int | None = None
    jobs_completed: int | None = None
    jobs_distinct: int | None = None
    queue_waiting: int | None = None
    queue_consumers: int | None = None
    kafka_committed: int | None = None
    cassandra_reachable: bool | None = None


class Registration(BaseModel):
    """A session that currently holds a registration — not proof of liveness.

    A wedged or stopped process stays registered until its session expires.
    """

    identity: str
    since: datetime | None = None


class ConsumerRegistration(Registration):
    group: str | None = None


class WorkerRegistration(Registration):
    delay: float | None = Field(default=None, description="The delay it applied.")
    config_version: int | None = Field(
        default=None, description="The config znode version it applied."
    )


class Registrations(BaseModel):
    coordinator: list[Registration]
    consumer: list[ConsumerRegistration]
    worker: list[WorkerRegistration]


class Cluster(BaseModel):
    leader: Leader | None
    snapshot: Snapshot | None
    snapshot_matches_leader: bool = Field(
        description=(
            "True only when a leader and a snapshot epoch are both present and "
            "equal. False during a handover — from read skew across these "
            "separate reads, or because a new leader has not written yet — and "
            "whenever either side is absent."
        )
    )
    registrations: Registrations
    observed_at: datetime = Field(description=OBSERVED_AT)


STORE_ERRORS = {
    503: {
        "model": ErrorDetail,
        "description": (
            "`<store> busy`: refused at admission, the store was not called. "
            "`<store> unavailable`: the call failed or ran out of time."
        ),
    }
}


# --- Reads, run on worker threads ---------------------------------------------


def snapshot_matches_leader(leader: dict | None, snapshot: dict | None) -> bool:
    """Both present and equal, never merely equal.

    `cluster init` pre-creates the snapshot as {"epoch": null}, so with nobody
    leading a naive comparison is None == None — a match on a cluster with no
    leader.
    """
    if leader is None or snapshot is None:
        return False
    leader_epoch, snapshot_epoch = leader.get("epoch"), snapshot.get("epoch")
    if leader_epoch is None or snapshot_epoch is None:
        return False
    return leader_epoch == snapshot_epoch


def read_cluster(client: KazooClient, paths: Paths, budget: Budget) -> dict:
    """Leader, snapshot and registrations: separate reads, not one view.

    They can span a handover, and the response says so through
    `snapshot_matches_leader` rather than retrying until it looks consistent.
    """
    leader = coordination.read_leader(client, paths, budget)
    try:
        snapshot, version = coordination.read_snapshot(client, paths, budget)
        snapshot = {**snapshot, "version": version}
    except NoNodeError:
        snapshot = None  # the tree has not been initialised
    registrations = {
        role: coordination.registrations(client, paths, role, budget) for role in ROLES
    }
    return {"leader": leader, "snapshot": snapshot, "registrations": registrations}


def snapshot_response(snapshot: dict | None) -> Snapshot | None:
    """The snapshot, with its Unix-seconds `at` as a UTC datetime."""
    if snapshot is None:
        return None
    at = snapshot.get("at")
    return Snapshot(
        **{**snapshot, "at": None if at is None else datetime.fromtimestamp(at, UTC)}
    )


def cluster_response(state: dict) -> Cluster:
    leader, snapshot = state["leader"], state["snapshot"]
    return Cluster(
        leader=leader,
        snapshot=snapshot_response(snapshot),
        snapshot_matches_leader=snapshot_matches_leader(leader, snapshot),
        registrations=state["registrations"],
        observed_at=now(),
    )


def event_response(row) -> Event:
    return Event(
        event_time=as_utc(row.event_time),
        event_id=row.event_id,
        page=row.page,
        written_at=from_microseconds(row.written_at),
    )


# --- The application ------------------------------------------------------------


def create_app(open_stores=open_stores) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        with ExitStack() as stack:
            # Connecting blocks, retrying for as long as a store is coming up, so
            # it runs on a thread rather than stalling the event loop.
            stores = await anyio.to_thread.run_sync(open_stores, stack)
            app.state.stores = stores
            app.state.bulkheads = make_bulkheads(stores)
            yield

    app = FastAPI(
        title="data-pipeline-tutorial",
        version="0.8",
        summary="Three stores, three kinds of truth.",
        lifespan=lifespan,
    )

    @app.exception_handler(Busy)
    async def busy(_request: Request, error: Busy) -> JSONResponse:
        return JSONResponse(status_code=503, content={"detail": str(error)})

    @app.exception_handler(Unavailable)
    async def unavailable(_request: Request, error: Unavailable) -> JSONResponse:
        return JSONResponse(status_code=503, content={"detail": str(error)})

    # Every route is `async def`, including these helpers' callers: a plain
    # `def` route, or a plain `def` dependency, would run on the shared thread
    # pool, outside any bulkhead.
    def state(request: Request) -> tuple[Stores, Bulkheads]:
        return request.app.state.stores, request.app.state.bulkheads

    UserId = Annotated[str, Path(min_length=1, max_length=USER_ID_MAX_LENGTH)]
    Page = Annotated[str, StringConstraints(min_length=1, max_length=PAGE_MAX_LENGTH)]

    @app.get("/health", summary="Liveness only: touches no store")
    async def health() -> Health:
        """If this answers, the process can serve a request. It says nothing
        about the stores, and never waits for a thread, so a stalled store
        cannot make the process look dead when it is merely busy."""
        return Health(status="ok")

    @app.get("/counts/pages", responses=STORE_ERRORS, summary="Redis: views per page")
    async def page_counts(
        request: Request,
        page: Annotated[
            list[Page] | None,
            Query(
                max_length=PAGES_PER_REQUEST_MAX,
                description="Pages to count; defaults to the producer's catalogue.",
            ),
        ] = None,
    ) -> PageCounts:
        """Named pages only, read in one MGET. Discovering every page would mean
        sweeping the whole keyspace — one key per user as well as per page — so
        its cost would grow with users, which is the trap this API refuses
        elsewhere."""
        stores, bulkheads = state(request)
        pages = list(dict.fromkeys(page)) if page else list(PAGES)
        counts = await bulkheads.redis.run(
            lambda _budget: redis_store.page_counts_for(
                stores.redis, pages, stores.prefix
            )
        )
        return PageCounts(counts=counts, observed_at=now())

    @app.get(
        "/users/{user_id}/last-page",
        responses={404: {"model": ErrorDetail}, **STORE_ERRORS},
        summary="Redis: a user's last page",
    )
    async def last_page(request: Request, user_id: UserId) -> LastPage:
        """404 when Redis holds no value: this resource *is* the value, so
        without one it is absent. Redis is deliberately volatile here, so after
        a restart this can 404 for a user whose events Cassandra still has."""
        stores, bulkheads = state(request)
        page = await bulkheads.redis.run(
            lambda _budget: redis_store.last_page(stores.redis, user_id, stores.prefix)
        )
        if page is None:
            raise HTTPException(status_code=404, detail="no last page for this user")
        return LastPage(user_id=user_id, page=page, observed_at=now())

    @app.get(
        "/users/{user_id}/events",
        responses=STORE_ERRORS,
        summary="Cassandra: a user's events",
    )
    async def events(
        request: Request,
        user_id: UserId,
        limit: Annotated[int, Query(ge=1, le=EVENTS_LIMIT_MAX)] = 20,
    ) -> UserEvents:
        """An empty list, not a 404, when there are none: this is a collection,
        and Cassandra cannot tell an unknown user from one with no events."""
        stores, bulkheads = state(request)
        rows = await bulkheads.cassandra.run(
            lambda budget: cassandra_store.user_history(
                stores.cassandra, user_id, limit, timeout=budget.call_timeout()
            )
        )
        return UserEvents(
            user_id=user_id,
            events=[event_response(row) for row in rows],
            observed_at=now(),
        )

    @app.get("/cluster", responses=STORE_ERRORS, summary="ZooKeeper: coordination")
    async def cluster(request: Request) -> Cluster:
        stores, bulkheads = state(request)
        state_ = await bulkheads.zookeeper.run(
            lambda budget: read_cluster(stores.zookeeper, stores.paths, budget)
        )
        return cluster_response(state_)

    return app


def main() -> None:
    # Connect before uvicorn starts. uvicorn captures SIGTERM and SIGINT for its
    # whole run, lifespan startup included, and acts on them only once startup
    # has finished: measured, a SIGTERM five seconds into a startup stalled on a
    # paused Redis took effect 32.5s later. Connecting first leaves both signals
    # with their ordinary meaning until there is something to serve.
    with ExitStack() as connecting:
        stores = open_stores(connecting)

        def hand_over(stack: ExitStack) -> Stores:
            # Ownership moves only once the lifespan is running, and its
            # shutdown then closes the clients — before uvicorn re-raises
            # SIGTERM, which ends the process without unwinding this block.
            # Until then this block still owns them, so a uvicorn that exits
            # before starting the lifespan cannot leak them.
            stack.enter_context(connecting.pop_all())
            return stores

        # One process: no --reload, which adds a file-watching parent, and no
        # workers, which add children. workers=1 is explicit because uvicorn
        # otherwise reads WEB_CONCURRENCY, and measured with it set to 2 it
        # exited before the lifespan ever ran. After 0.3's leak history, a
        # supervisor inside a supervised process is the last thing wanted here.
        uvicorn.run(
            create_app(open_stores=hand_over),
            host="127.0.0.1",
            port=API_PORT,
            workers=1,
        )


if __name__ == "__main__":
    main()
