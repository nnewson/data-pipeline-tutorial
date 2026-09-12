"""Redis writes, and the keys they land on.

Two writes per event, and the difference between them is the point of this
release. `INCR` is atomic but not idempotent: replay an event and the counter
climbs again. `SET` is last-writer-wins, so replaying leaves the same value.
Same handler, same replay, two outcomes.
"""

import logging

import redis

from pipeline import wait_for_connection
from pipeline.config import REDIS_HOST, REDIS_KEY_PREFIX, REDIS_PORT

logger = logging.getLogger("redis-store")

PAGE_COUNT_PREFIX = "pageviews:"
LAST_PAGE_PREFIX = "user:last_page:"
JOBS_RUNS_KEY = "jobs:runs"


def page_count_key(page: str, prefix: str = REDIS_KEY_PREFIX) -> str:
    return f"{prefix}{PAGE_COUNT_PREFIX}{page}"


def last_page_key(user_id: str, prefix: str = REDIS_KEY_PREFIX) -> str:
    return f"{prefix}{LAST_PAGE_PREFIX}{user_id}"


def connect(host: str = REDIS_HOST, port: int = REDIS_PORT) -> redis.Redis:
    """Return a Redis client that has actually reached the server.

    `redis.Redis(...)` connects lazily, so constructing one succeeds with
    nothing listening. Without the PING, wait_for_connection would report a
    connection it has not made, and the failure would surface later somewhere
    less obvious.
    """

    def open_client() -> redis.Redis:
        client = redis.Redis(host=host, port=port, decode_responses=True)
        try:
            client.ping()
        except (redis.RedisError, OSError):
            # Do not leak the socket of a client that never reached the server;
            # this runs once per retry.
            client.close()
            raise
        return client

    return wait_for_connection("Redis", open_client)


def record_pageview(
    client: redis.Redis, event: dict, prefix: str = REDIS_KEY_PREFIX
) -> None:
    """Apply one event to Redis.

    The two writes are individually atomic but not atomic together: a crash
    between them leaves partial state. The injected failure in the consumer
    happens after both, which keeps the demonstration about replay rather than
    about partial writes.
    """
    client.incr(page_count_key(event["page"], prefix))
    client.set(last_page_key(event["user_id"], prefix), event["page"])


def page_counts(client: redis.Redis, prefix: str = REDIS_KEY_PREFIX) -> dict[str, int]:
    """Every page counter under the prefix.

    SCAN rather than KEYS: KEYS blocks the server for the whole sweep, which is
    a habit worth not forming even where the keyspace is tiny.
    """
    pattern = f"{prefix}{PAGE_COUNT_PREFIX}*"
    counts: dict[str, int] = {}
    for key in client.scan_iter(match=pattern, count=100):
        value = client.get(key)
        if value is None:
            continue
        page = key[len(f"{prefix}{PAGE_COUNT_PREFIX}") :]
        counts[page] = int(value)
    return counts


def last_pages(client: redis.Redis, prefix: str = REDIS_KEY_PREFIX) -> dict[str, str]:
    """Every last-page value under the prefix."""
    pattern = f"{prefix}{LAST_PAGE_PREFIX}*"
    pages: dict[str, str] = {}
    for key in client.scan_iter(match=pattern, count=100):
        value = client.get(key)
        if value is None:
            continue
        user = key[len(f"{prefix}{LAST_PAGE_PREFIX}") :]
        pages[user] = value
    return pages


def clear(client: redis.Redis, prefix: str) -> int:
    """Delete every key under a prefix. Returns how many were removed.

    An empty prefix is refused rather than treated as "everything": the pattern
    would become `*`, and a cleanup helper that can erase the whole database on
    a missing argument is a bad thing to have lying around.
    """
    if not prefix:
        raise ValueError("clear() requires a non-empty prefix")

    removed = 0
    for key in client.scan_iter(match=f"{prefix}*", count=100):
        removed += client.delete(key)
    return removed


def jobs_runs_key(prefix: str = REDIS_KEY_PREFIX) -> str:
    return f"{prefix}{JOBS_RUNS_KEY}"


def record_execution(
    client: redis.Redis, event_id: str, prefix: str = REDIS_KEY_PREFIX
) -> None:
    """Record that a job ran.

    Queue depth cannot answer "how much work completed" — acknowledged messages
    are simply gone — so completions are counted here instead.

    **One command, deliberately.** An earlier version incremented a total and a
    per-event hash separately; a worker dying between the two left the total
    ahead of the per-event counts forever, which is 0.4's two-command trap
    recreated in the instrumentation. The total is derived from the hash instead.

    What remains true: this is a non-idempotent side effect recording a
    non-idempotent side effect. A worker dying between this call and its
    acknowledgement runs the work again and counts it again — evidence for the
    lesson, but it does mean the number is not ground truth.
    """
    client.hincrby(jobs_runs_key(prefix), event_id, 1)


def job_runs(client: redis.Redis, prefix: str = REDIS_KEY_PREFIX) -> dict[str, int]:
    """How many times each event's job has been executed."""
    raw = client.hgetall(jobs_runs_key(prefix))
    return {event_id: int(count) for event_id, count in raw.items()}


def job_summary(
    client: redis.Redis, prefix: str = REDIS_KEY_PREFIX
) -> tuple[int, dict[str, int]]:
    """Total executions and per-event counts, from one snapshot.

    Both figures come from a single read on purpose. Asking for them separately
    takes two snapshots, and a worker finishing in between reports a total that
    does not match the counts beside it. Every caller uses this rather than
    composing its own pair of reads.
    """
    runs = job_runs(client, prefix)
    return sum(runs.values()), runs
