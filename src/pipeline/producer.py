import argparse
import json
import logging
import time
import uuid

from faker import Faker
from kafka import KafkaProducer

from pipeline import get_partition, wait_for_connection, wait_for_topic
from pipeline.config import (
    KAFKA_PARTITIONS,
    KAFKA_SERVER,
    KAFKA_TOPIC,
    PRODUCER_INTERVAL_SECONDS,
)

logger = logging.getLogger("producer")

PAGES = ["/", "/pricing", "/docs", "/checkout"]

# send() is asynchronous; the record is not durable until the broker says so.
ACK_TIMEOUT_SECONDS = 30


def create_event(fake: Faker, pages: list[str]) -> dict:
    """Build one synthetic pageview.

    event_id is the event's identity rather than decoration. It is what makes a
    replayed write recognisable as a repeat later in the series.
    """
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": fake.user_name(),
        "page": fake.random_element(pages),
        "timestamp": time.time(),
    }


def connect() -> KafkaProducer:
    return wait_for_connection(
        "Kafka",
        lambda: KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        ),
    )


def produce(producer: KafkaProducer, fake: Faker, count: int | None = None) -> None:
    """Produce events, either endlessly or an exact number of them.

    The bounded mode exists so the overcount demonstration can state a figure
    rather than describe an impression.
    """
    produced = 0
    while count is None or produced < count:
        event = create_event(fake, PAGES)
        # Wait for the broker to acknowledge before claiming success. send()
        # returns a future, so logging without get() would report a delivery
        # that may still fail. At one event per second, waiting costs nothing
        # and the reported partition and offset are then facts.
        # Routed explicitly rather than left to the default partitioner, so
        # the rule is visible in the code and every partition is exercised.
        partition = get_partition(event["user_id"], KAFKA_PARTITIONS)
        metadata = producer.send(KAFKA_TOPIC, event, partition=partition).get(
            timeout=ACK_TIMEOUT_SECONDS
        )
        logger.info(
            f"Produced (partition {metadata.partition}, "
            f"offset {metadata.offset}): {event}"
        )
        produced += 1
        time.sleep(PRODUCER_INTERVAL_SECONDS)

    logger.info(f"Produced {produced} events and stopped")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic pageviews.")
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="stop after this many events (default: run until interrupted)",
    )
    return parser.parse_args(argv)


def main() -> int:
    arguments = parse_args()
    wait_for_topic(KAFKA_TOPIC, KAFKA_SERVER)
    producer = connect()
    try:
        produce(producer, Faker(), count=arguments.count)
    except KeyboardInterrupt:
        logger.info("Shutting down producer")
    finally:
        producer.flush()
        producer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
