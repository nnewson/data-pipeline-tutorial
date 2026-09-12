import json
import logging
import os
from contextlib import ExitStack

from kafka import KafkaConsumer

from pipeline import cassandra_store, jobs_queue, wait_for_connection, wait_for_topic
from pipeline.config import (
    CASSANDRA_KEYSPACE,
    COMMIT_EVERY,
    CONSUMER_CRASH_AFTER,
    CONSUMER_GROUP,
    KAFKA_SERVER,
    KAFKA_TOPIC,
)
from pipeline.redis_store import connect as connect_redis
from pipeline.redis_store import record_pageview

logger = logging.getLogger("consumer")

# One group, so the four consumers divide the partitions rather than each
# receiving every event. Its name comes from config, so a smoke run can isolate
# itself from a topology that is already running.


def connect() -> KafkaConsumer:
    return wait_for_connection(
        "Kafka",
        lambda: KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            group_id=CONSUMER_GROUP,
            auto_offset_reset="earliest",
            # Committing on a timer would make the replay window invisible and
            # non-deterministic. Committing explicitly makes "how much work can
            # be repeated" a number this code chooses.
            enable_auto_commit=False,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        ),
    )


def handle(message, redis_client, session, insert, channel) -> None:
    """The work: apply one event to both stores, then log what happened.

    All of this runs before the offset is committed, which is what makes a
    replayed event reach both stores again. What each store does with the repeat
    is up to the write, not the delivery: the Redis counter climbs, the
    Cassandra row is replaced.

    Four writes now, still not atomic together. The window between the publish
    and the commit is the one that matters most: a crash there means the job
    runs, and then runs again after the replay.
    """
    record_pageview(redis_client, message.value)
    cassandra_store.record_pageview(session, insert, message.value)
    # Raises if the publish is not confirmed as routed, so the offset below is
    # not committed and the event is redelivered.
    jobs_queue.publish(channel, message.value)
    logger.info(
        f"Consumed (partition {message.partition}, offset {message.offset}): "
        f"{message.value}"
    )


def consume_forever(
    consumer: KafkaConsumer,
    redis_client,
    session,
    insert,
    channel,
    commit_every: int = COMMIT_EVERY,
    crash_after: int | None = CONSUMER_CRASH_AFTER,
) -> None:
    processed = 0
    for message in consumer:
        handle(message, redis_client, session, insert, channel)
        processed += 1

        # The work happened before the commit, so a crash here replays it.
        # Committing first would lose it instead — at-most-once rather than
        # at-least-once. Neither option is "no duplicates and no loss".
        if crash_after is not None and processed >= crash_after:
            # Not processed % commit_every: crashing exactly on a boundary
            # leaves a full batch pending, not zero.
            uncommitted = ((processed - 1) % commit_every) + 1
            logger.warning(
                f"Injected crash after {processed} messages with "
                f"{uncommitted} uncommitted"
            )
            # os._exit skips cleanup, so nothing gets committed on the way out.
            os._exit(1)

        if processed % commit_every == 0:
            consumer.commit()
            logger.info(f"Committed offsets after {processed} messages")


def main() -> int:
    wait_for_topic(KAFKA_TOPIC, KAFKA_SERVER)
    # One ownership boundary covering setup as well as the run. Opening these
    # before the try meant a failure while preparing the statement or connecting
    # to Kafka left the earlier resources open — and the Cassandra driver keeps
    # background threads, so that is more than a leaked socket.
    with ExitStack() as resources:
        redis_client = connect_redis()
        resources.callback(redis_client.close)

        cluster, session = cassandra_store.connect(keyspace=CASSANDRA_KEYSPACE)
        resources.callback(cluster.shutdown)

        insert = cassandra_store.prepare_insert(session)

        publisher, channel = jobs_queue.open_publisher()
        resources.callback(publisher.close)

        consumer = connect()
        resources.callback(consumer.close)

        try:
            consume_forever(consumer, redis_client, session, insert, channel)
        except KeyboardInterrupt:
            logger.info("Shutting down consumer")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
