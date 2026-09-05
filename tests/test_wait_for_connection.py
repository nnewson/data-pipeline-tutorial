import logging

import pytest
from kafka.admin import NewPartitions
from kafka.errors import KafkaError, TopicAlreadyExistsError

import pipeline


@pytest.fixture(autouse=True)
def no_sleeping(monkeypatch):
    monkeypatch.setattr(pipeline.time, "sleep", lambda seconds: None)


def test_returns_the_connection_on_first_success():
    assert pipeline.wait_for_connection("thing", lambda: "connected") == "connected"


def test_retries_until_it_succeeds():
    attempts = []

    def connect():
        attempts.append(1)
        if len(attempts) < 3:
            raise ConnectionError("not yet")
        return "connected"

    assert pipeline.wait_for_connection("thing", connect, retries=5) == "connected"
    assert len(attempts) == 3


def test_raises_the_last_error_when_retries_are_exhausted():
    def connect():
        raise ConnectionError("refused")

    with pytest.raises(ConnectionError, match="refused"):
        pipeline.wait_for_connection("thing", connect, retries=3)


class FakeAdmin:
    def __init__(self, error=None, existing_partitions=None):
        self._error = error
        self._existing_partitions = existing_partitions
        self.created = []
        self.expanded = {}
        self.closed = False

    def create_topics(self, new_topics):
        if self._error:
            raise self._error
        self.created.extend(new_topics)

    def describe_topics(self, names):
        return [{"partitions": [None] * self._existing_partitions}]

    def create_partitions(self, spec):
        self.expanded.update(spec)

    def close(self):
        self.closed = True


def test_ensure_topic_creates_a_missing_topic(monkeypatch, caplog):
    admin = FakeAdmin()
    monkeypatch.setattr(pipeline, "KafkaAdminClient", lambda **kwargs: admin)

    with caplog.at_level(logging.INFO, logger="pipeline"):
        pipeline.ensure_topic("pageviews", 1, "localhost:9092")

    assert [topic.name for topic in admin.created] == ["pageviews"]
    assert admin.created[0].num_partitions == 1
    assert admin.closed is True
    assert "Created topic pageviews" in caplog.text


def test_ensure_topic_is_idempotent(monkeypatch, caplog):
    admin = FakeAdmin(error=TopicAlreadyExistsError("exists"), existing_partitions=1)
    monkeypatch.setattr(pipeline, "KafkaAdminClient", lambda **kwargs: admin)

    with caplog.at_level(logging.INFO, logger="pipeline"):
        pipeline.ensure_topic("pageviews", 1, "localhost:9092")

    assert admin.closed is True
    assert "already has 1 partition" in caplog.text


def test_ensure_topic_closes_the_admin_client_on_an_unexpected_error(monkeypatch):
    admin = FakeAdmin(error=KafkaError("broker went away"))
    monkeypatch.setattr(pipeline, "KafkaAdminClient", lambda **kwargs: admin)

    with pytest.raises(KafkaError):
        pipeline.ensure_topic("pageviews", 1, "localhost:9092")

    assert admin.closed is True


def test_ensure_topic_passes_the_partition_count_through(monkeypatch):
    admin = FakeAdmin()
    monkeypatch.setattr(pipeline, "KafkaAdminClient", lambda **kwargs: admin)

    # 0.3 changes this number; the plumbing must carry it.
    pipeline.ensure_topic("pageviews", 4, "localhost:9092")

    assert admin.created[0].num_partitions == 4


def test_ensure_topic_expands_a_topic_carried_over_from_an_earlier_release(
    monkeypatch, caplog
):
    """The 0.2 volume holds a one-partition pageviews; 0.3 needs four."""
    admin = FakeAdmin(error=TopicAlreadyExistsError("exists"), existing_partitions=1)
    monkeypatch.setattr(pipeline, "KafkaAdminClient", lambda **kwargs: admin)

    with caplog.at_level(logging.INFO, logger="pipeline"):
        pipeline.ensure_topic("pageviews", 4, "localhost:9092")

    assert isinstance(admin.expanded["pageviews"], NewPartitions)
    assert admin.expanded["pageviews"].total_count == 4
    assert "Expanded topic pageviews from 1 to 4" in caplog.text


def test_ensure_topic_leaves_an_already_correct_topic_alone(monkeypatch):
    admin = FakeAdmin(error=TopicAlreadyExistsError("exists"), existing_partitions=4)
    monkeypatch.setattr(pipeline, "KafkaAdminClient", lambda **kwargs: admin)

    pipeline.ensure_topic("pageviews", 4, "localhost:9092")

    assert admin.expanded == {}


def test_ensure_topic_refuses_to_shrink(monkeypatch):
    """Kafka cannot remove partitions, so say so rather than pretend."""
    admin = FakeAdmin(error=TopicAlreadyExistsError("exists"), existing_partitions=8)
    monkeypatch.setattr(pipeline, "KafkaAdminClient", lambda **kwargs: admin)

    with pytest.raises(RuntimeError, match="more than the 4"):
        pipeline.ensure_topic("pageviews", 4, "localhost:9092")

    assert admin.closed is True
