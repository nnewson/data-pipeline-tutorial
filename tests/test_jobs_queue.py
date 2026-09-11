import json

import pytest
from pika.exceptions import AMQPConnectionError, UnroutableError

from pipeline import jobs_queue


class FakeChannel:
    def __init__(self, error=None):
        self._error = error
        self.declared = []
        self.published = []
        self.confirms_enabled = False

    def queue_declare(self, queue, durable=False):
        self.declared.append((queue, durable))

    def confirm_delivery(self):
        self.confirms_enabled = True

    def basic_publish(self, exchange, routing_key, body, properties, mandatory=False):
        if self._error:
            raise self._error
        self.published.append((exchange, routing_key, body, properties, mandatory))


def _event(event_id="e1"):
    return {
        "event_id": event_id,
        "user_id": "ada",
        "page": "/docs",
        "timestamp": 1788894338.5,
    }


def test_declare_asks_for_a_durable_queue():
    """Both ends call this, so the arguments cannot drift apart."""
    channel = FakeChannel()

    jobs_queue.declare(channel, "jobs")

    assert channel.declared == [("jobs", True)]


def test_job_body_carries_the_event_identity():
    """event_id is the only thing that reveals a Kafka replay as a duplicate."""
    body = json.loads(jobs_queue.job_body(_event("abc")))

    assert body["event_id"] == "abc"
    assert body["user_id"] == "ada"
    assert body["page"] == "/docs"


def test_publish_is_mandatory_and_persistent():
    channel = FakeChannel()

    jobs_queue.publish(channel, _event(), "jobs")

    exchange, routing_key, _, properties, mandatory = channel.published[0]
    assert exchange == ""
    assert routing_key == "jobs"
    # mandatory turns an unroutable publish into a visible failure; RabbitMQ
    # would otherwise confirm a message it discarded.
    assert mandatory is True
    assert properties.delivery_mode == 2
    assert properties.message_id == "e1"


def test_an_unroutable_publish_raises(monkeypatch):
    """A confirm alone does not prove the job reached a queue."""
    channel = FakeChannel(error=UnroutableError([]))

    with pytest.raises(jobs_queue.PublishFailed, match="was not routed"):
        jobs_queue.publish(channel, _event(), "jobs")


def test_a_broker_failure_raises():
    channel = FakeChannel(error=AMQPConnectionError("gone"))

    with pytest.raises(jobs_queue.PublishFailed, match="failed"):
        jobs_queue.publish(channel, _event(), "jobs")


def test_publish_failure_is_not_swallowed():
    """It must propagate, so the Kafka offset is not committed."""
    channel = FakeChannel(error=UnroutableError([]))

    with pytest.raises(jobs_queue.PublishFailed):
        jobs_queue.publish(channel, _event(), "jobs")

    assert channel.published == []


class FakeConnection:
    def __init__(self, channel=None, channel_error=None):
        self._channel = channel or FakeChannel()
        self._channel_error = channel_error
        self.is_open = True
        self.closed = 0

    def channel(self):
        if self._channel_error:
            raise self._channel_error
        return self._channel

    def close(self):
        self.closed += 1
        self.is_open = False


class InspectChannel:
    def __init__(self, message_count=3, consumer_count=4):
        self.declared = []

        class Method:
            pass

        self.method = Method()
        self.method.message_count = message_count
        self.method.consumer_count = consumer_count

    def queue_declare(self, queue, durable=False, passive=False):
        self.declared.append({"queue": queue, "durable": durable, "passive": passive})
        return self

    # queue_declare returns self, so expose .method like pika's result object.


def test_queue_state_asks_passively(monkeypatch):
    """passive: report a missing queue rather than creating one."""
    channel = InspectChannel()
    connection = FakeConnection(channel)
    monkeypatch.setattr(jobs_queue, "connect", lambda *a, **k: connection)

    waiting, consumers = jobs_queue.queue_state("jobs")

    assert (waiting, consumers) == (3, 4)
    assert channel.declared == [{"queue": "jobs", "durable": False, "passive": True}]
    assert connection.closed == 1


def test_open_publisher_closes_a_partially_built_connection(monkeypatch):
    """A failure after connecting must not leave a connection nobody holds."""
    connection = FakeConnection(channel_error=AMQPConnectionError("channel refused"))
    monkeypatch.setattr(jobs_queue, "connect", lambda *a, **k: connection)

    with pytest.raises(AMQPConnectionError):
        jobs_queue.open_publisher("jobs")

    assert connection.closed == 1


def test_open_publisher_enables_confirms(monkeypatch):
    channel = FakeChannel()
    connection = FakeConnection(channel)
    monkeypatch.setattr(jobs_queue, "connect", lambda *a, **k: connection)

    jobs_queue.open_publisher("jobs")

    assert channel.confirms_enabled is True
    assert channel.declared == [("jobs", True)]


def test_close_quietly_ignores_an_already_closed_connection():
    """pika raises when closing a closed connection; that must not mask a failure."""

    class AlreadyClosed:
        is_open = False

        def close(self):
            raise AssertionError("should not be called")

    jobs_queue.close_quietly(AlreadyClosed())  # must not raise


def test_close_quietly_swallows_a_broker_error(caplog):
    class Angry:
        is_open = True

        def close(self):
            raise AMQPConnectionError("broker went away")

    jobs_queue.close_quietly(Angry())  # must not raise


def test_close_quietly_accepts_none():
    jobs_queue.close_quietly(None)
