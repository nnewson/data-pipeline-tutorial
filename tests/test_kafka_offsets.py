"""Committed-offset reading, and why it is filtered and retained."""

from kafka.structs import TopicPartition

from pipeline import kafka_offsets


class Metadata:
    def __init__(self, offset):
        self.offset = offset


class FakeAdmin:
    def __init__(self, offsets=None, error=None):
        self._offsets = offsets or {}
        self._error = error
        self.closed = 0
        self.calls = 0

    def list_group_offsets(self, group):
        self.calls += 1
        if self._error:
            raise self._error
        return {group: self._offsets}

    def close(self):
        self.closed += 1


def _reader(admin, monkeypatch):
    reader = kafka_offsets.OffsetReader()
    monkeypatch.setattr(kafka_offsets, "KafkaAdminClient", lambda **kwargs: admin)
    return reader


def test_offsets_are_summed_for_the_pipeline_topic(monkeypatch):
    admin = FakeAdmin(
        {
            TopicPartition("pageviews", 0): Metadata(10),
            TopicPartition("pageviews", 1): Metadata(5),
        }
    )
    reader = _reader(admin, monkeypatch)

    assert reader.committed_total("pipeline", topic="pageviews") == 15


def test_another_topics_offsets_do_not_inflate_the_total(monkeypatch):
    """A group that once consumed something else retains offsets for it."""
    admin = FakeAdmin(
        {
            TopicPartition("pageviews", 0): Metadata(10),
            TopicPartition("something_else", 0): Metadata(1000),
        }
    )
    reader = _reader(admin, monkeypatch)

    assert reader.committed_total("pipeline", topic="pageviews") == 10


def test_unset_offsets_are_ignored(monkeypatch):
    admin = FakeAdmin(
        {
            TopicPartition("pageviews", 0): Metadata(4),
            TopicPartition("pageviews", 1): Metadata(-1),
        }
    )
    reader = _reader(admin, monkeypatch)

    assert reader.committed_total("pipeline", topic="pageviews") == 4


def test_the_admin_client_is_reused_across_reads(monkeypatch):
    """One client, not one per snapshot: the churn removed from Cassandra."""
    admin = FakeAdmin({TopicPartition("pageviews", 0): Metadata(1)})
    built = []

    reader = kafka_offsets.OffsetReader()
    monkeypatch.setattr(
        kafka_offsets,
        "KafkaAdminClient",
        lambda **kwargs: (built.append(1), admin)[1],
    )

    for _ in range(3):
        reader.committed_total("pipeline", topic="pageviews")

    assert len(built) == 1
    assert admin.calls == 3


def test_a_failed_read_drops_the_client(monkeypatch):
    """A broken client is not worth keeping for the next snapshot."""
    admin = FakeAdmin(error=RuntimeError("coordinator not available"))
    reader = _reader(admin, monkeypatch)

    assert reader.committed_total("pipeline") is None
    assert admin.closed == 1


def test_an_unavailable_broker_is_reported_as_none(monkeypatch):
    def unavailable(**kwargs):
        raise RuntimeError("no brokers")

    monkeypatch.setattr(kafka_offsets, "KafkaAdminClient", unavailable)

    assert kafka_offsets.OffsetReader().committed_total("pipeline") is None
