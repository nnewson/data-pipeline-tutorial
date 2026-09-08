import pytest
from faker import Faker

from pipeline import producer


def test_create_event_has_the_expected_shape():
    event = producer.create_event(Faker(), ["/pricing"])

    assert set(event) == {"event_id", "user_id", "page", "timestamp"}
    assert event["page"] == "/pricing"
    assert isinstance(event["timestamp"], float)


def test_event_ids_are_unique():
    fake = Faker()
    ids = {producer.create_event(fake, producer.PAGES)["event_id"] for _ in range(50)}

    assert len(ids) == 50


def test_produce_sends_to_the_configured_topic(monkeypatch):
    sent = []

    class FakeMetadata:
        partition = 0
        offset = 7

    class FakeFuture:
        def get(self, timeout=None):
            return FakeMetadata()

    class FakeProducer:
        def send(self, topic, value, partition=None):
            sent.append((topic, value, partition))
            if len(sent) == 3:
                raise KeyboardInterrupt
            return FakeFuture()

    monkeypatch.setattr(producer.time, "sleep", lambda seconds: None)

    with __import__("pytest").raises(KeyboardInterrupt):
        producer.produce(FakeProducer(), Faker())

    assert len(sent) == 3
    assert {topic for topic, _, _ in sent} == {producer.KAFKA_TOPIC}
    # Every event carries an explicit partition chosen by the routing rule.
    assert all(
        partition == producer.get_partition(event["user_id"], producer.KAFKA_PARTITIONS)
        for _, event, partition in sent
    )


def _acknowledged():
    """A future that resolves to plausible broker metadata."""

    class Metadata:
        partition = 0
        offset = 0

    class Future:
        def get(self, timeout=None):
            return Metadata()

    return Future()


class CountingProducer:
    def __init__(self):
        self.sent = []

    def send(self, topic, value, partition=None):
        self.sent.append(value)
        return _acknowledged()


@pytest.mark.parametrize("count", [1, 5, 20])
def test_bounded_mode_produces_exactly_that_many(monkeypatch, count):
    """The demonstration states '23, not 20', which needs an exact 20."""
    monkeypatch.setattr(producer.time, "sleep", lambda seconds: None)
    fake = CountingProducer()

    producer.produce(fake, Faker(), count=count)

    assert len(fake.sent) == count


def test_default_mode_is_unbounded(monkeypatch):
    """No count means keep going; the test stops it rather than the loop."""
    monkeypatch.setattr(producer.time, "sleep", lambda seconds: None)
    sent = []

    class Stop(Exception):
        pass

    class Endless:
        def send(self, topic, value, partition=None):
            sent.append(value)
            if len(sent) >= 50:
                raise Stop
            return _acknowledged()

    with pytest.raises(Stop):
        producer.produce(Endless(), Faker(), count=None)

    assert len(sent) == 50


def test_count_argument_parses():
    assert producer.parse_args(["--count", "20"]).count == 20
    assert producer.parse_args([]).count is None
