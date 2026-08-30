import logging

from pipeline import kafka_consumer


class FakeMessage:
    def __init__(self, value, partition=0, offset=0):
        self.value = value
        self.partition = partition
        self.offset = offset


def test_consume_forever_logs_every_message(caplog):
    messages = [
        FakeMessage({"page": "/pricing"}, partition=0, offset=17),
        FakeMessage({"page": "/docs"}, partition=0, offset=18),
    ]

    with caplog.at_level(logging.INFO, logger="consumer"):
        kafka_consumer.consume_forever(iter(messages))

    assert len(caplog.records) == 2
    assert "offset 17" in caplog.text
    assert "/docs" in caplog.text


def test_consumer_group_is_shared_so_work_divides():
    # 0.3 adds three more consumers; they must join this same group.
    assert kafka_consumer.CONSUMER_GROUP == "pipeline"
