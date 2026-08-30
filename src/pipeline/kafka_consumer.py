import json
import logging

from kafka import KafkaConsumer

from pipeline import wait_for_connection
from pipeline.config import KAFKA_SERVER, KAFKA_TOPIC

logger = logging.getLogger("consumer")

# One group, so that adding consumers in 0.3 divides the work rather than
# duplicating it.
CONSUMER_GROUP = "pipeline"


def connect() -> KafkaConsumer:
    return wait_for_connection(
        "Kafka",
        lambda: KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_SERVER,
            group_id=CONSUMER_GROUP,
            # Without this a new group starts at the end of the log and appears
            # to do nothing until the next event is produced.
            auto_offset_reset="earliest",
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        ),
    )


def consume_forever(consumer: KafkaConsumer) -> None:
    for message in consumer:
        logger.info(
            f"Consumed (partition {message.partition}, offset {message.offset}): "
            f"{message.value}"
        )


def main() -> int:
    consumer = connect()
    try:
        consume_forever(consumer)
    except KeyboardInterrupt:
        logger.info("Shutting down consumer")
    finally:
        consumer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
