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


def test_produce_forever_sends_to_the_configured_topic(monkeypatch):
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
        producer.produce_forever(FakeProducer(), Faker())

    assert len(sent) == 3
    assert {topic for topic, _, _ in sent} == {producer.KAFKA_TOPIC}
    # Every event carries an explicit partition chosen by the routing rule.
    assert all(
        partition == producer.get_partition(event["user_id"], producer.KAFKA_PARTITIONS)
        for _, event, partition in sent
    )
