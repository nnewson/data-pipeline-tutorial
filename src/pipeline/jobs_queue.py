"""The job queue: one declaration, used by both the publisher and the workers.

The queue's properties are defined once here rather than at each end, so they
cannot drift apart — a publisher and a consumer declaring the same queue with
different arguments is an error RabbitMQ reports at the worst moment.

What a confirm proves is narrower than it looks. RabbitMQ will confirm a message
it could not route, so confirms alone do not show the job reached a queue;
`mandatory=True` is what turns an unroutable publish into a visible return. And
if the connection fails before a confirmation arrives, the outcome is genuinely
unknown — retrying then risks another duplicate, which is the same class of
problem this release is about.
"""

import json
import logging

import pika
from pika.exceptions import AMQPError, UnroutableError

from pipeline import wait_for_connection
from pipeline.config import (
    RABBITMQ_HOST,
    RABBITMQ_PASSWORD,
    RABBITMQ_PORT,
    RABBITMQ_QUEUE,
    RABBITMQ_USER,
)

logger = logging.getLogger("jobs-queue")

# Persistent messages: they survive a broker restart while they are still
# unacknowledged. Acknowledged work is gone, and cannot be replayed.
PERSISTENT = pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)


class PublishFailed(Exception):
    """The publish was not confirmed as routed, so the job may not exist."""


def close_quietly(connection) -> None:
    """Close a connection without raising over one already closed.

    pika raises ConnectionWrongStateError when closing a connection the broker
    has already dropped. In a cleanup callback that exception replaces whichever
    failure actually mattered.
    """
    try:
        if connection is not None and connection.is_open:
            connection.close()
    except AMQPError as error:
        logger.warning(f"ignoring error while closing RabbitMQ connection: {error}")


def connect(
    host: str = RABBITMQ_HOST, port: int = RABBITMQ_PORT
) -> pika.BlockingConnection:
    """Open a connection, retrying while the broker is still starting.

    The guest account cannot connect from anywhere but the broker's own
    loopback interface, so a real user is used even locally — otherwise the
    documented container address would be refused.
    """
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    return wait_for_connection(
        "RabbitMQ",
        lambda: pika.BlockingConnection(
            pika.ConnectionParameters(host=host, port=port, credentials=credentials)
        ),
    )


def declare(channel, queue: str = RABBITMQ_QUEUE) -> str:
    """Declare the shared work queue. Both ends call this, with these arguments."""
    channel.queue_declare(queue=queue, durable=True)
    return queue


def open_publisher(queue: str = RABBITMQ_QUEUE):
    """A connection and channel ready to publish confirmed, routed messages.

    Everything after the connect is inside the try: a failure declaring the
    queue or enabling confirms would otherwise leave a connection nobody holds a
    reference to, and so nobody closes.
    """
    connection = connect()
    try:
        channel = connection.channel()
        declare(channel, queue)
        # Without this, basic_publish is fire-and-forget.
        channel.confirm_delivery()
    except Exception:
        close_quietly(connection)
        raise
    return connection, channel


def queue_state(queue: str = RABBITMQ_QUEUE) -> tuple[int, int]:
    """Messages waiting and consumers attached, without creating the queue.

    passive=True asks about a queue rather than declaring one. Without it a
    missing queue would be quietly created here, and a broken declaration path
    would look healthy.
    """
    connection = connect()
    try:
        declared = connection.channel().queue_declare(queue=queue, passive=True)
        return declared.method.message_count, declared.method.consumer_count
    finally:
        close_quietly(connection)


def job_body(event: dict) -> bytes:
    """The job carries the event's identity, which is what makes duplicates visible.

    RabbitMQ can tell a worker that *it* redelivered a message. It cannot tell
    anyone that an upstream Kafka replay produced a second, distinct publish of
    the same event: at this layer those are two unrelated messages. event_id is
    the only thing that reveals them as the same work.
    """
    return json.dumps(
        {
            "event_id": event["event_id"],
            "user_id": event["user_id"],
            "page": event["page"],
        }
    ).encode("utf-8")


def publish(channel, event: dict, queue: str = RABBITMQ_QUEUE) -> None:
    """Publish one job, or raise so the Kafka offset is not committed.

    mandatory=True matters: RabbitMQ confirms unroutable messages, so publishing
    to the default exchange before the queue exists would otherwise be confirmed
    and silently discarded.
    """
    properties = pika.BasicProperties(
        delivery_mode=pika.DeliveryMode.Persistent,
        message_id=event["event_id"],
        content_type="application/json",
    )
    try:
        channel.basic_publish(
            exchange="",
            routing_key=queue,
            body=job_body(event),
            properties=properties,
            mandatory=True,
        )
    except UnroutableError as error:
        raise PublishFailed(f"job for {event['event_id']} was not routed") from error
    except AMQPError as error:
        raise PublishFailed(
            f"publishing job for {event['event_id']} failed: {error}"
        ) from error
