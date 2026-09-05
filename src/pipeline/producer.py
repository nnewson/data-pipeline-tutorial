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


def produce_forever(producer: KafkaProducer, fake: Faker) -> None:
    while True:
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
        time.sleep(PRODUCER_INTERVAL_SECONDS)


def main() -> int:
    wait_for_topic(KAFKA_TOPIC, KAFKA_SERVER)
    producer = connect()
    try:
        produce_forever(producer, Faker())
    except KeyboardInterrupt:
        logger.info("Shutting down producer")
    finally:
        producer.flush()
        producer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
