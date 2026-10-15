"""Windowed counts, from the Flink job's topic into Redis — where the API reads them.

One process, its own consumer group, every partition of the windows topic. The
volume is one result per page per window, so nothing here is about throughput.
What it is about is which result a window ends up holding.

**For each retained window, results cannot move back from a generation Redis
has already accepted.** Every result carries the generation of the job that
produced it. A superseded job can still be running when its successor starts —
the keeper cancels it the first time it sees it, but cannot recall what it
already wrote to Kafka — and two jobs can count the same window differently. So
one Lua script per result, atomic in Redis, compares generations: older than
the window has accepted → refused; newer → the window is replaced whole, so no
page from an older generation lingers; the same → the page is updated, which is
how an HA restore's repeats converge.

That is narrower than "every superseded generation refused": an older
generation can still write a window its successor has not written yet, and its
result stands if the successor never emits that window; and a Redis restart
forgets the generations with the views, until a rebuild replays them. Refusing
against the *current* generation would need the keeper's record — another
coordination contract, deliberately not added.

Expiry is set from **event time**, the window's end plus a retention, not from
the moment of writing: a replay that rewrites a two-hour-old window finds it
already expired, and the script skips it rather than resurrect it as recent.

Offsets are committed only after the batch's scripts have all run. A batch that
fails is not committed, the consumer goes back to its first offsets, and it is
written again: at-least-once in, converging by generation.
"""

import json
import logging
import time
from collections.abc import Callable, Iterable
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import UTC, datetime

import redis
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

from pipeline import notifications, redis_store, wait_for_connection, wait_for_topic
from pipeline.config import (
    KAFKA_SERVER,
    KAFKA_WINDOWS_TOPIC,
    REDIS_KEY_PREFIX,
    WINDOW_RETENTION_SECONDS,
    WINDOWS_WRITER_GROUP,
)

logger = logging.getLogger("windows-writer")

INDEX_KEY = "windows"
ISO = "%Y-%m-%dT%H:%M:%SZ"
PAGE_MAX_LENGTH = 128

WRITTEN, REFUSED, EXPIRED = 1, 0, -1

RETRY_INITIAL_SECONDS = 0.5
RETRY_MAX_SECONDS = 10.0

# KEYS: the window's hash, its meta hash, the index.
# ARGV: generation, page, views, start, end, expire_at, now.
SCRIPT = """
local generation = tonumber(ARGV[1])
local start = tonumber(ARGV[4])
local expire_at = tonumber(ARGV[6])
local now = tonumber(ARGV[7])
if expire_at <= now then
  return -1
end
local stored = tonumber(redis.call('HGET', KEYS[2], 'generation') or '-1')
if generation < stored then
  return 0
end
if generation > stored then
  redis.call('DEL', KEYS[1])
  redis.call('HSET', KEYS[2], 'generation', ARGV[1], 'end', ARGV[5])
end
redis.call('HSET', KEYS[1], ARGV[2], ARGV[3])
redis.call('EXPIREAT', KEYS[1], expire_at)
redis.call('EXPIREAT', KEYS[2], expire_at)
redis.call('ZADD', KEYS[3], start, ARGV[4])
redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', now - (expire_at - start))
return 1
"""


def index_key(prefix: str = REDIS_KEY_PREFIX) -> str:
    """The windows by start time: scored by start, trimmed on end + retention."""
    return f"{prefix}{INDEX_KEY}"


def window_key(start: int, prefix: str = REDIS_KEY_PREFIX) -> str:
    """One window's counts: page -> views."""
    return f"{prefix}{INDEX_KEY}:{start}"


def meta_key(start: int, prefix: str = REDIS_KEY_PREFIX) -> str:
    """One window's generation and end, beside its counts so no page can
    collide with them."""
    return f"{window_key(start, prefix)}:meta"


def epoch_seconds(text: str) -> int:
    return int(datetime.strptime(text, ISO).replace(tzinfo=UTC).timestamp())


@dataclass(frozen=True)
class Result:
    start: int
    end: int
    page: str
    views: int
    generation: int


def parse(raw: bytes | str) -> Result | None:
    """One result from the topic, or None for anything misshapen."""
    try:
        value = json.loads(raw)
        start = epoch_seconds(value["window_start"])
        end = epoch_seconds(value["window_end"])
        page, views = value["page"], value["views"]
        # 0.10's jobs wrote no generation: no keeper submitted them, which is
        # what generation 0 means. Older than any kept job's, so never preferred.
        generation = value.get("generation", 0)
    except (ValueError, KeyError, TypeError):
        return None
    if not isinstance(page, str) or not 0 < len(page) <= PAGE_MAX_LENGTH:
        return None
    for number in (views, generation):
        # bool is an int to isinstance; a count of True is not a count.
        if type(number) is not int or number < 0:
            return None
    if end <= start:
        return None
    return Result(start, end, page, views, generation)


class Writer:
    """Applies results to Redis, one atomic script each, one round trip a batch."""

    def __init__(
        self,
        client: redis.Redis,
        prefix: str = REDIS_KEY_PREFIX,
        retention: int = WINDOW_RETENTION_SECONDS,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._client = client
        self._prefix = prefix
        self._retention = retention
        self._clock = clock
        self._script = client.register_script(SCRIPT)

    def write(self, results: list[Result]) -> list[int]:
        """Each result's outcome: WRITTEN, REFUSED or EXPIRED. Raises if Redis
        fails, in which case none of the batch counts as written.

        A plain pipeline, not a transaction: each script is atomic by itself,
        and redis-py raises on any failed command (raise_on_error=True)."""
        if not results:
            return []
        now = int(self._clock())
        pipe = self._client.pipeline(transaction=False)
        for result in results:
            self._script(
                keys=[
                    window_key(result.start, self._prefix),
                    meta_key(result.start, self._prefix),
                    index_key(self._prefix),
                ],
                args=[
                    result.generation,
                    result.page,
                    result.views,
                    result.start,
                    result.end,
                    result.end + self._retention,
                    now,
                ],
                client=pipe,
            )
        return [int(outcome) for outcome in pipe.execute()]


class Hints:
    """One best-effort announcement per window written, as 0.9's notifications are.

    A hint carries the window's bounds and no counts: announce, and let the
    reader fetch. An unconfirmed publish is logged and swallowed — the counts
    are already in Redis, and a replay to repeat a hint would be a bad trade.
    """

    def __init__(self, client: redis.Redis, prefix: str = REDIS_KEY_PREFIX) -> None:
        self._client = client
        self._channel = notifications.windows_channel(prefix)
        self._failing = False

    def announce(self, windows: Iterable[tuple[int, int]]) -> None:
        for start, end in sorted(set(windows)):
            try:
                self._client.publish(
                    self._channel, notifications.encode_window(start, end)
                )
            except notifications.FAILURES as error:
                if not self._failing:
                    self._failing = True
                    logger.warning(f"window hint unconfirmed: {error}")
                return
        if self._failing:
            self._failing = False
            logger.info("window hints publishing again")


def apply(batch: dict, writer: Writer) -> tuple[list[Result], int, int]:
    """Write one poll's results: (those written, refused or expired, misshapen).

    Raises if Redis fails. Misshapen records are skipped rather than retried:
    one that cannot be parsed now never will be, and would block its partition.
    """
    results, misshapen = [], 0
    for messages in batch.values():
        for message in messages:
            result = parse(message.value)
            if result is None:
                misshapen += 1
            else:
                results.append(result)
    outcomes = writer.write(results)
    written = [
        r for r, outcome in zip(results, outcomes, strict=True) if outcome == WRITTEN
    ]
    return written, len(results) - len(written), misshapen


def rewind(consumer: KafkaConsumer, batch: dict) -> None:
    """Back to each partition's first offset in a failed batch, to write it again."""
    for partition, messages in batch.items():
        if messages:
            consumer.seek(partition, messages[0].offset)


def run(
    consumer: KafkaConsumer,
    writer: Writer,
    hints: Hints | None,
    sleep: Callable[[float], None] = time.sleep,
    polls: int | None = None,
) -> None:
    """Poll, write, commit, announce — for ever, or for `polls` polls in tests.

    Announced after the commit, so every window a hint names is durably
    written: a hint is a reason to read, and the read must find it.
    """
    retry = RETRY_INITIAL_SECONDS
    done = 0
    while polls is None or done < polls:
        done += 1
        batch: dict[TopicPartition, list] = consumer.poll(timeout_ms=1000)
        if not batch:
            continue
        try:
            written, skipped, misshapen = apply(batch, writer)
        except (redis.RedisError, OSError) as error:
            logger.warning(f"Redis write failed, retrying the batch: {error}")
            rewind(consumer, batch)
            sleep(retry)
            retry = min(RETRY_MAX_SECONDS, retry * 2)
            continue
        retry = RETRY_INITIAL_SECONDS
        try:
            consumer.commit()
        except KafkaError as error:
            # The writes are in Redis; redelivering them converges by generation.
            logger.warning(f"could not commit after writing: {error}")
        if hints is not None:
            hints.announce((r.start, r.end) for r in written)
        if misshapen:
            logger.warning(f"skipped {misshapen} misshapen result(s)")
        logger.info(f"wrote {len(written)} result(s); {skipped} refused or expired")


def connect_consumer() -> KafkaConsumer:
    return wait_for_connection(
        "Kafka",
        lambda: KafkaConsumer(
            KAFKA_WINDOWS_TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            group_id=WINDOWS_WRITER_GROUP,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        ),
    )


def main() -> int:
    wait_for_topic(KAFKA_WINDOWS_TOPIC, KAFKA_SERVER)
    with ExitStack() as resources:
        client = redis_store.connect()
        resources.callback(client.close)
        # Hints get their own client, with short timeouts and no retries, as the
        # consumers' notifications do.
        hint_client = redis_store.connect(**notifications.CLIENT_OPTIONS)
        resources.callback(hint_client.close)
        consumer = connect_consumer()
        resources.callback(consumer.close)
        try:
            run(consumer, Writer(client), Hints(hint_client))
        except KeyboardInterrupt:
            logger.info("Shutting down windows writer")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
