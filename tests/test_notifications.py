"""The notification contract, and the consumer's best-effort publish."""

import json
import logging

import pytest
import redis

from pipeline import kafka_consumer, notifications

EVENT = {
    "event_id": "e-1",
    "user_id": "ada",
    "page": "/docs",
    "timestamp": 1790000000.25,
}


def test_the_channel_carries_the_key_prefix():
    """Pub/sub ignores the database, so the prefix is the only isolation."""
    assert notifications.channel("smoke:abc:") == "smoke:abc:events:pageviews"
    assert notifications.channel("") == "events:pageviews"


def test_a_notification_carries_the_offset_and_no_count():
    body = json.loads(notifications.encode(EVENT, partition=2, offset=1841))

    assert body == {**EVENT, "partition": 2, "offset": 1841}
    assert "count" not in body


def test_what_the_publisher_writes_the_bridge_will_relay():
    relayed = notifications.parse(notifications.encode(EVENT, 2, 1841))

    assert relayed == {"type": "pageview", **EVENT, "partition": 2, "offset": 1841}


@pytest.mark.parametrize(
    "raw",
    [
        None,
        b"bytes",
        "not json",
        "[1, 2]",
        json.dumps({**EVENT, "partition": 1}),  # no offset
        json.dumps({**EVENT, "partition": 1, "offset": "7"}),
        json.dumps({**EVENT, "partition": 1, "offset": True}),  # bool is not an offset
        json.dumps({**EVENT, "page": 3, "partition": 1, "offset": 7}),
        json.dumps({**EVENT, "partition": 1, "offset": 7, "pad": "x" * 2100}),
        # Not JSON, though Python's json accepts them: a browser's JSON.parse
        # throws on every one of these once relayed.
        '{"event_id": "e", "user_id": "u", "page": "/", "partition": 0, "offset": 1, "timestamp": NaN}',
        '{"event_id": "e", "user_id": "u", "page": "/", "partition": 0, "offset": 1, "timestamp": Infinity}',
        '{"event_id": "e", "user_id": "u", "page": "/", "partition": 0, "offset": 1, "timestamp": -Infinity}',
        '{"event_id": "e", "user_id": "u", "page": "/", "partition": 0, "offset": 1, "timestamp": 1e400}',
        # An integer too large for a float: 495 bytes, well under the limit, and
        # math.isfinite raises on it rather than answering.
        '{"event_id": "e", "user_id": "u", "page": "/", "partition": 0, "offset": 1, "timestamp": 1'
        + "0" * 400
        + "}",
    ],
)
def test_anything_oversized_or_misshapen_is_not_relayed(raw):
    assert notifications.parse(raw) is None


def test_extra_fields_are_not_forwarded():
    raw = json.dumps({**EVENT, "partition": 1, "offset": 7, "script": "<b>hi</b>"})

    assert "script" not in notifications.parse(raw)


class FakePublisher:
    def __init__(self):
        self.published: list[tuple[str, str]] = []
        self.fail: Exception | None = None
        self.listeners = 0

    def publish(self, channel, message):
        if self.fail is not None:
            raise self.fail
        self.published.append((channel, message))
        return self.listeners


def test_nobody_listening_is_normal_and_not_logged(caplog):
    client = FakePublisher()  # PUBLISH returns 0
    with caplog.at_level(logging.INFO, logger="notifications"):
        notifications.Notifier(client, "p:").announce(EVENT, 0, 1)

    assert client.published[0][0] == "p:events:pageviews"
    assert caplog.records == []


def test_a_failed_publish_is_swallowed_and_logged_once_until_it_recovers(caplog):
    client = FakePublisher()
    notifier = notifications.Notifier(client, "p:")
    client.fail = redis.TimeoutError("Timeout reading from socket")

    with caplog.at_level(logging.INFO, logger="notifications"):
        for offset in range(5):
            notifier.announce(EVENT, 0, offset)  # raises nothing
        client.fail = None
        notifier.announce(EVENT, 0, 5)
        notifier.announce(EVENT, 0, 6)

    messages = [record.getMessage() for record in caplog.records]
    assert len(messages) == 2
    # Unconfirmed, not failed: after a timeout the PUBLISH may have happened.
    assert messages[0].startswith("notification publish unconfirmed: TimeoutError")
    assert messages[1] == "notifications publishing again"


def test_a_bug_is_not_swallowed_as_a_publishing_failure():
    client = FakePublisher()
    client.fail = TypeError("a bug")

    with pytest.raises(TypeError):
        notifications.Notifier(client, "p:").announce(EVENT, 0, 1)


def test_the_notification_client_does_not_retry_and_times_out_fast():
    options = notifications.CLIENT_OPTIONS

    assert options["retry"].get_retries() == 0
    assert options["socket_timeout"] <= 0.5
    assert options["socket_connect_timeout"] <= 0.5


# --- In the consumer's handler ------------------------------------------------


class Recorder:
    """Every write in one list, so the order can be asserted."""

    def __init__(self):
        self.order: list[str] = []

    def incr(self, key):
        self.order.append("redis incr")

    def set(self, key, value):
        self.order.append("redis set")

    def execute(self, statement, parameters=None):
        self.order.append("cassandra")

    def basic_publish(self, exchange, routing_key, body, properties, mandatory=False):
        self.order.append("rabbitmq")

    def publish(self, channel, message):
        self.order.append("notification")
        return 0


class Message:
    value = {**EVENT}
    partition = 3
    offset = 99


def test_the_notification_is_published_after_every_other_write():
    """A page reading state when it hears one must read the state it was told about."""
    recorder = Recorder()
    notifier = notifications.Notifier(recorder, "p:")

    kafka_consumer.handle(Message(), recorder, recorder, "insert", recorder, notifier)

    assert recorder.order == [
        "redis incr",
        "redis set",
        "cassandra",
        "rabbitmq",
        "notification",
    ]


def test_a_failed_notification_does_not_fail_the_handler():
    """Failing would leave the offset uncommitted, and the replay re-runs the INCR."""
    recorder = Recorder()
    failing = FakePublisher()
    failing.fail = redis.ConnectionError("refused")

    kafka_consumer.handle(
        Message(),
        recorder,
        recorder,
        "insert",
        recorder,
        notifications.Notifier(failing, "p:"),
    )

    assert "rabbitmq" in recorder.order


def _with(field, number):
    return json.dumps({**EVENT, "partition": 1, "offset": 7, field: number})


@pytest.mark.parametrize("field", ["partition", "offset"])
@pytest.mark.parametrize(
    "number, relayed",
    [
        (-1, False),
        (0, True),
        (notifications.MAX_SAFE_INTEGER, True),
        (notifications.MAX_SAFE_INTEGER + 1, False),
        (10**400, False),
    ],
)
def test_integers_must_be_exactly_representable_in_javascript(field, number, relayed):
    """The format's limit: 0 to 2**53 - 1, compared as integers, inclusive."""
    result = notifications.parse(_with(field, number))

    assert (result is not None) is relayed
    if relayed:
        assert result[field] == number


def test_the_limit_is_javascripts_largest_exact_integer():
    assert notifications.MAX_SAFE_INTEGER == 9_007_199_254_740_991
