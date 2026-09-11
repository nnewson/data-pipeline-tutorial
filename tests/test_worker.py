import json
import logging

from pipeline import worker


class FakeMethod:
    def __init__(self, redelivered=False, delivery_tag=1):
        self.redelivered = redelivered
        self.delivery_tag = delivery_tag


class FakeChannel:
    def __init__(self):
        self.acked = []

    def basic_ack(self, delivery_tag):
        self.acked.append(delivery_tag)


class FakeRedis:
    def __init__(self):
        self.calls = []

    def incr(self, key):
        self.calls.append(("incr", key))

    def hincrby(self, key, field, amount):
        self.calls.append(("hincrby", key, field, amount))


def _body(event_id="e1"):
    return json.dumps(
        {"event_id": event_id, "user_id": "ada", "page": "/docs"}
    ).encode()


def test_work_is_recorded_then_acknowledged(monkeypatch):
    """Order matters: acking first would be at-most-once."""
    order = []
    redis_client = FakeRedis()
    channel = FakeChannel()
    monkeypatch.setattr(worker, "do_work", lambda delay=0: order.append("work"))
    monkeypatch.setattr(
        worker.redis_store,
        "record_execution",
        lambda client, event_id: order.append("record"),
    )
    monkeypatch.setattr(channel, "basic_ack", lambda delivery_tag: order.append("ack"))

    worker.handle_job(redis_client, channel, FakeMethod(), None, _body(), delay=0)

    assert order == ["work", "record", "ack"]


def test_the_delivery_is_acknowledged_with_its_own_tag(monkeypatch):
    redis_client = FakeRedis()
    channel = FakeChannel()
    monkeypatch.setattr(worker, "do_work", lambda delay=0: None)

    worker.handle_job(
        redis_client, channel, FakeMethod(delivery_tag=77), None, _body(), delay=0
    )

    assert channel.acked == [77]


def test_execution_is_counted_per_event(monkeypatch):
    redis_client = FakeRedis()
    monkeypatch.setattr(worker, "do_work", lambda delay=0: None)

    worker.handle_job(
        redis_client, FakeChannel(), FakeMethod(), None, _body("abc"), delay=0
    )

    assert ("hincrby", "jobs:runs", "abc", 1) in redis_client.calls


def test_a_redelivery_is_logged_as_one(monkeypatch, caplog):
    """RabbitMQ's redelivered flag distinguishes its own repeat from a Kafka replay."""
    monkeypatch.setattr(worker, "do_work", lambda delay=0: None)

    with caplog.at_level(logging.INFO, logger="worker"):
        worker.handle_job(
            FakeRedis(),
            FakeChannel(),
            FakeMethod(redelivered=True),
            None,
            _body("abc"),
            delay=0,
        )

    assert "redelivered=True" in caplog.text
    assert "abc" in caplog.text


def test_a_first_delivery_is_logged_as_not_redelivered(monkeypatch, caplog):
    """A Kafka replay arrives as a new message: redelivered is False on both."""
    monkeypatch.setattr(worker, "do_work", lambda delay=0: None)

    with caplog.at_level(logging.INFO, logger="worker"):
        worker.handle_job(
            FakeRedis(), FakeChannel(), FakeMethod(redelivered=False), None, _body()
        )

    assert "redelivered=False" in caplog.text
