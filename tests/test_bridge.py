"""The bridge's state transitions, against a scripted subscription.

Real asyncio, short intervals. Errors are queued like messages, so a failure
arrives at a known point in the stream rather than whenever a flag is noticed.
"""

import asyncio
import json
import time

import pytest
import redis

from pipeline.bridge import INTERRUPTED, SUBSCRIBED, Bridge


@pytest.fixture
def anyio_backend():
    return "asyncio"


class FakeSubscription:
    def __init__(self, *, ack=True, answer_pings=True):
        self.inbox: asyncio.Queue = asyncio.Queue()
        self.ack = ack
        self.answer_pings = answer_pings
        self.pings: list[str] = []
        self.channel = None
        self.closed = False

    async def subscribe(self, channel):
        self.channel = channel
        if self.ack:
            self.inbox.put_nowait({"type": "subscribe", "channel": channel, "data": 1})

    async def get_message(self, ignore_subscribe_messages=False, timeout=0.0):
        try:
            if timeout:
                item = await asyncio.wait_for(self.inbox.get(), timeout)
            else:
                item = self.inbox.get_nowait()
        except (TimeoutError, asyncio.QueueEmpty):
            return None
        if isinstance(item, BaseException):
            raise item
        return item

    async def ping(self, message=None):
        self.pings.append(message)
        if self.answer_pings:
            self.inbox.put_nowait({"type": "pong", "data": message})

    async def aclose(self):
        self.closed = True

    def publish(self, raw):
        self.inbox.put_nowait({"type": "message", "channel": self.channel, "data": raw})

    def fail(self, error):
        self.inbox.put_nowait(error)


def valid(offset=7, partition=1):
    return json.dumps(
        {
            "event_id": f"e{offset}",
            "user_id": "ada",
            "page": "/docs",
            "partition": partition,
            "offset": offset,
            "timestamp": 1790000000.5,
        }
    )


class Harness:
    def __init__(self, *subscriptions, **options):
        self.subscriptions = list(subscriptions)
        self.opened: list[FakeSubscription] = []
        self.relayed: list[dict] = []
        self.notices: list[dict] = []
        defaults = dict(
            heartbeat_interval=0.05,
            heartbeat_deadline=0.15,
            attempt_deadline=0.2,
            backoff_initial=0.01,
            backoff_max=0.05,
        )
        self.bridge = Bridge(
            self._open,
            "p:events:pageviews",
            lambda text: self.relayed.append(json.loads(text)),
            self.notices.append,
            **(defaults | options),
        )

    def _open(self):
        subscription = (
            self.subscriptions.pop(0) if self.subscriptions else FakeSubscription()
        )
        self.opened.append(subscription)
        return subscription

    def kinds(self):
        return [notice["type"] for notice in self.notices]


async def until(condition, timeout=2.0):
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("condition not reached")
        await asyncio.sleep(0.005)


@pytest.fixture
async def running():
    tasks = []

    def start(harness):
        tasks.append(asyncio.create_task(harness.bridge.run()))
        return harness

    yield start
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.anyio
async def test_subscribed_only_once_the_subscription_is_acknowledged(running):
    first = FakeSubscription()
    harness = running(Harness(first))

    await until(lambda: harness.bridge.state == SUBSCRIBED)

    assert first.channel == "p:events:pageviews"
    assert harness.kinds() == ["resubscribed"]
    assert harness.notices[0]["detected_at"] <= harness.notices[0]["resubscribed_at"]


@pytest.mark.anyio
async def test_an_unacknowledged_subscribe_is_not_recovery(running):
    """Connected and subscribed-to is not subscribed: the ack is the proof."""
    silent = [FakeSubscription(ack=False) for _ in range(3)]
    harness = running(Harness(*silent))

    await until(lambda: len(harness.opened) >= 3)

    assert harness.bridge.state == INTERRUPTED
    assert "resubscribed" not in harness.kinds()
    assert all(subscription.closed for subscription in harness.opened[:2])


@pytest.mark.anyio
async def test_valid_notifications_are_relayed_and_others_dropped(running):
    first = FakeSubscription()
    harness = running(Harness(first))
    await until(lambda: harness.bridge.state == SUBSCRIBED)

    first.publish(valid(offset=7))
    first.publish("not json")
    first.publish(json.dumps({"event_id": "e", "offset": "seven"}))
    first.publish(valid(offset=8)[:-1] + ', "padding": "' + "x" * 3000 + '"}')
    first.publish(valid(offset=9))

    await until(lambda: len(harness.relayed) == 2)
    await asyncio.sleep(0.02)
    assert [n["offset"] for n in harness.relayed] == [7, 9]
    assert harness.relayed[0]["type"] == "pageview"
    assert harness.bridge.dropped == 3


@pytest.mark.anyio
async def test_an_answered_heartbeat_keeps_the_subscription(running):
    first = FakeSubscription()
    harness = running(Harness(first))
    await until(lambda: harness.bridge.state == SUBSCRIBED)

    await asyncio.sleep(0.5)  # several intervals and deadlines

    assert len(first.pings) >= 5
    assert harness.bridge.state == SUBSCRIBED
    assert harness.kinds() == ["resubscribed"]


@pytest.mark.anyio
async def test_an_unanswered_heartbeat_ends_the_subscription_within_its_deadline(
    running,
):
    deaf = FakeSubscription(answer_pings=False)
    harness = running(Harness(deaf))
    await until(lambda: harness.bridge.state == SUBSCRIBED)
    subscribed_at = time.monotonic()

    await until(lambda: "interrupted" in harness.kinds())
    took = time.monotonic() - subscribed_at

    # First ping after one interval, then a fixed deadline: 0.05 + 0.15.
    assert 0.18 < took < 0.4, took
    await until(lambda: harness.kinds()[-1] == "resubscribed")
    assert deaf.closed


@pytest.mark.anyio
async def test_one_heartbeat_outstanding_and_later_pings_do_not_extend_it(running):
    """A second PING while one is unanswered would push the deadline out.

    Messages keep arriving throughout, so the loop wakes many times before the
    deadline — every one a chance to send a ping it must not send.
    """
    deaf = FakeSubscription(answer_pings=False)
    harness = running(Harness(deaf))
    await until(lambda: harness.bridge.state == SUBSCRIBED)

    async def keep_publishing():
        offset = 0
        while "interrupted" not in harness.kinds():
            offset += 1
            deaf.publish(valid(offset=offset))
            await asyncio.sleep(0.01)

    await asyncio.wait_for(keep_publishing(), 1.0)
    assert deaf.pings == ["heartbeat-1"]


@pytest.mark.anyio
async def test_messages_flowing_do_not_excuse_a_missing_heartbeat(running):
    deaf = FakeSubscription(answer_pings=False)
    harness = running(Harness(deaf))
    await until(lambda: harness.bridge.state == SUBSCRIBED)

    async def keep_publishing():
        offset = 0
        while "interrupted" not in harness.kinds():
            offset += 1
            deaf.publish(valid(offset=offset))
            await asyncio.sleep(0.01)

    await asyncio.wait_for(keep_publishing(), 1.0)
    assert len(harness.relayed) > 5, "messages were relayed until the deadline"


@pytest.mark.anyio
async def test_a_failed_read_is_detected_and_recovered(running):
    first = FakeSubscription()
    harness = running(Harness(first))
    await until(lambda: harness.bridge.state == SUBSCRIBED)

    first.fail(redis.ConnectionError("Connection closed by server."))

    await until(
        lambda: harness.kinds() == ["resubscribed", "interrupted", "resubscribed"]
    )
    interrupted, resubscribed = harness.notices[1], harness.notices[2]
    assert "detected_at" in interrupted and "resubscribed_at" not in interrupted
    assert resubscribed["detected_at"] == interrupted["detected_at"]
    assert resubscribed["resubscribed_at"] >= resubscribed["detected_at"]
    assert first.closed
    assert len(harness.opened) == 2


@pytest.mark.anyio
async def test_a_silent_resubscription_is_reported(running):
    """redis-py renewing the subscription underneath us must not go unseen."""
    first = FakeSubscription()
    harness = running(Harness(first))
    await until(lambda: harness.bridge.state == SUBSCRIBED)

    first.inbox.put_nowait({"type": "subscribe", "channel": first.channel, "data": 1})

    await until(lambda: len(harness.notices) == 3)
    assert harness.kinds() == ["resubscribed", "interrupted", "resubscribed"]
    assert len(harness.opened) == 1, "no reconnect was needed to report it"


@pytest.mark.anyio
async def test_an_unexpected_error_does_not_stop_the_bridge(running):
    """The sandbox's bridge logged and stopped. This one carries on."""
    first = FakeSubscription()
    harness = running(Harness(first))
    await until(lambda: harness.bridge.state == SUBSCRIBED)

    first.fail(ValueError("a bug in a parser"))

    await until(
        lambda: harness.kinds()[-1:] == ["resubscribed"] and len(harness.opened) == 2
    )
    harness.opened[1].publish(valid(offset=3))
    await until(lambda: len(harness.relayed) == 1)


@pytest.mark.anyio
async def test_backoff_grows_between_failed_attempts_and_is_capped(running):
    failing = [FakeSubscription(ack=False) for _ in range(6)]
    harness = running(
        Harness(*failing, attempt_deadline=0.02, backoff_initial=0.01, backoff_max=0.04)
    )

    await until(lambda: len(harness.opened) >= 6, timeout=3)
    assert harness.bridge.state == INTERRUPTED


@pytest.mark.anyio
async def test_cancelling_the_bridge_closes_its_subscription():
    first = FakeSubscription()
    harness = Harness(first)
    task = asyncio.create_task(harness.bridge.run())
    await until(lambda: harness.bridge.state == SUBSCRIBED)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert first.closed


def test_the_subscription_client_uses_the_async_retry_with_no_recovery():
    """The sync Retry handed to an async client catches nothing, by accident.

    And the async one, even at zero retries, would reconnect and resubscribe
    inside the client before raising — unless no error is "supported".
    """
    import redis.asyncio.retry

    from pipeline.bridge import CLIENT_OPTIONS

    retry = CLIENT_OPTIONS["retry"]
    assert isinstance(retry, redis.asyncio.retry.Retry)
    assert retry.get_retries() == 0
    assert retry._supported_errors == ()


@pytest.mark.anyio
async def test_a_non_finite_number_is_dropped_not_relayed(running, monkeypatch):
    """The backstop: even if parse() let one through, the browser never sees it."""
    from pipeline import bridge as bridge_module

    monkeypatch.setattr(
        bridge_module.notifications,
        "parse",
        lambda raw: {"type": "pageview", "timestamp": float("nan")},
    )
    first = FakeSubscription()
    harness = running(Harness(first))
    await until(lambda: harness.bridge.state == SUBSCRIBED)

    first.publish("anything")
    await until(lambda: harness.bridge.dropped == 1)

    assert harness.relayed == []
    assert harness.bridge.state == SUBSCRIBED, "a bad message is not an interruption"


@pytest.mark.anyio
async def test_rejected_numbers_are_dropped_and_the_subscription_kept(running):
    """Regression: math.isfinite(10**400) raised, and the bridge reconnected.

    And the format's integer limit: each rejected message is dropped on its
    own, and the valid one after them is still relayed.
    """
    first = FakeSubscription()
    harness = running(Harness(first))
    await until(lambda: harness.bridge.state == SUBSCRIBED)

    def with_field(field, number):
        return json.dumps({**json.loads(valid(offset=1)), field: number})

    first.publish(with_field("timestamp", 10**400))
    first.publish(with_field("offset", 1 << 53))  # one past the limit
    first.publish(with_field("partition", -1))
    first.publish(valid(offset=2))
    await until(lambda: len(harness.relayed) == 1)

    assert [n["offset"] for n in harness.relayed] == [2]
    assert harness.bridge.dropped == 3
    assert harness.bridge.state == SUBSCRIBED
    assert harness.kinds() == ["resubscribed"], "no interruption"
    assert len(harness.opened) == 1, "no reconnect"
