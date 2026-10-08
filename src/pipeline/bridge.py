"""The API's one Redis subscription, and what it can prove about its own gaps.

A long-lived subscription owned by an asynchronous application, so it runs as
one `redis.asyncio` task — not on a thread, which would have to hand every
message to the event loop through an unbounded queue of callbacks. In one task
nothing in the application grows without bound: if fan-out falls behind, reads
slow down and Redis buffers on its side, until its own output-buffer limit for
pub/sub clients evicts this subscriber. That is the backpressure, and it shows
up as an interruption rather than as memory.

What the bridge can prove is narrow: *when it knew it was not listening*. Not
what was published meanwhile — only the offsets in later notifications can hint
at that, and only to a subscriber that saw both sides of the gap.

Three things the client does not do for us, each measured:

- **Its health check is not a failure detector.** `health_check_interval` sends
  a PING when due, filters out the reply and never waits for it. So the bridge
  sends its own, one at a time, each with a fixed deadline.
- **Its reconnection can be silent.** With retries enabled, redis-py reconnects
  and resubscribes inside the call, leaving nothing to observe. The client is
  given no retries, and an unexpected subscribe acknowledgement is treated as a
  resubscription anyway.
- **Under RESP3 a subscription's PING reply comes back mangled** — as
  `{"type": "h", "channel": "b", ...}`, the payload's characters indexed as if
  it were a list. Under RESP2 it is a proper `pong`. The client speaks RESP2.
"""

import asyncio
import json
import logging
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Protocol

import redis.asyncio
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

from pipeline import notifications

logger = logging.getLogger("bridge")

SUBSCRIBED = "subscribed"
INTERRUPTED = "interrupted"

# Tutorial defaults, not derived.
HEARTBEAT_INTERVAL_SECONDS = 5.0
HEARTBEAT_DEADLINE_SECONDS = 10.0
ATTEMPT_DEADLINE_SECONDS = 5.0
BACKOFF_INITIAL_SECONDS = 0.5
BACKOFF_MAX_SECONDS = 10.0

CLIENT_OPTIONS = {
    # RESP2: under RESP3 the heartbeat's reply cannot be recognised (above).
    "protocol": 2,
    "socket_connect_timeout": 1.0,
    "socket_timeout": 1.0,
    # No recovery inside the client at all: a failure must surface here, not be
    # repaired where nothing can see that it happened. The asynchronous Retry —
    # the synchronous one, handed to an async client, returns the coroutine
    # before it runs and so catches nothing (an earlier version did exactly
    # that, and only behaved by accident). And no supported errors: even with
    # zero retries, the async Retry calls PubSub's reconnect callback, which
    # reconnects and resubscribes before the error is raised (probed).
    "retry": Retry(NoBackoff(), 0, supported_errors=()),
}


class HeartbeatMissed(Exception):
    """The outstanding PING was not answered within its deadline."""


class Subscription(Protocol):
    """The part of a redis.asyncio PubSub the bridge uses."""

    async def subscribe(self, *channels: str) -> Any: ...

    async def get_message(
        self, ignore_subscribe_messages: bool = False, timeout: float | None = 0.0
    ) -> dict | None: ...

    async def ping(self, message: str | None = None) -> Any: ...

    async def aclose(self) -> None: ...


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


class Bridge:
    """Subscribe, relay, notice failure, back off, subscribe again — for ever.

    `relay` receives each valid notification as JSON text; `notice` receives the
    bridge's own state changes. Both are called on the event loop and must not
    block: they are the fan-out's non-blocking puts.
    """

    def __init__(
        self,
        open_subscription: Callable[[], Subscription],
        channel: str,
        relay: Callable[[str], None],
        notice: Callable[[dict], None],
        *,
        heartbeat_interval: float = HEARTBEAT_INTERVAL_SECONDS,
        heartbeat_deadline: float = HEARTBEAT_DEADLINE_SECONDS,
        attempt_deadline: float = ATTEMPT_DEADLINE_SECONDS,
        backoff_initial: float = BACKOFF_INITIAL_SECONDS,
        backoff_max: float = BACKOFF_MAX_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._open = open_subscription
        self._channel = channel
        self._relay = relay
        self._notice = notice
        self._interval = heartbeat_interval
        self._deadline = heartbeat_deadline
        self._attempt_deadline = attempt_deadline
        self._backoff_initial = backoff_initial
        self._backoff_max = backoff_max
        self._clock = clock
        self.state = INTERRUPTED
        self.dropped = 0  # oversized or misshapen messages, not relayed
        self._pings = 0

    async def run(self) -> None:
        # The bridge starts not listening, and says so from the start.
        detected_at = now_iso()
        backoff = self._backoff_initial
        while True:
            subscription: Subscription | None = None
            try:
                # One deadline for the whole attempt: connecting, negotiating,
                # SUBSCRIBE and its acknowledgement. A timeout per step would
                # let a slow-but-alive Redis spend several of them.
                async with asyncio.timeout(self._attempt_deadline):
                    subscription = self._open()
                    await subscription.subscribe(self._channel)
                    await self._acknowledged(subscription)
                self._subscribed(detected_at)
                backoff = self._backoff_initial
                await self._pump(subscription)
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001 - the bridge must outlive any one failure
                # The sandbox's bridge logged and stopped, leaving every browser
                # on a socket that would never speak again.
                if self.state == SUBSCRIBED:
                    detected_at = now_iso()
                    self._interrupted(detected_at, error)
                else:
                    logger.debug(f"subscription attempt failed: {error!r}")
            finally:
                if subscription is not None:
                    await self._close(subscription)
            await asyncio.sleep(backoff)
            backoff = min(self._backoff_max, backoff * 2)

    async def _acknowledged(self, subscription: Subscription) -> None:
        """Wait for the subscribe confirmation: recovered means confirmed."""
        while True:
            message = await subscription.get_message(timeout=0.1)
            if message and message.get("type") == "subscribe":
                return

    async def _pump(self, subscription: Subscription) -> None:
        """Relay until something fails. Never returns normally."""
        outstanding: tuple[str, float] | None = None  # (payload, fixed deadline)
        next_ping = self._clock() + self._interval
        while True:
            now = self._clock()
            if outstanding is None and now >= next_ping:
                self._pings += 1
                payload = f"heartbeat-{self._pings}"
                async with asyncio.timeout(self._deadline):
                    await subscription.ping(payload)
                outstanding = (payload, now + self._deadline)

            wake = outstanding[1] if outstanding else next_ping
            message = (
                await subscription.get_message(
                    timeout=max(0.0, min(wake - self._clock(), 1.0))
                )
                or {}
            )
            kind = message.get("type")

            if kind == "pong" and outstanding and message.get("data") == outstanding[0]:
                outstanding = None
                next_ping = self._clock() + self._interval
            elif kind == "message":
                self._forward(message.get("data"))
            elif kind == "subscribe":
                # redis-py reconnected and resubscribed underneath us. We did not
                # see the connection go, so all we can say is when we found out.
                found = now_iso()
                logger.warning("subscription was renewed underneath the bridge")
                self._notice({"type": "interrupted", "detected_at": found})
                self._notice(
                    {
                        "type": "resubscribed",
                        "detected_at": found,
                        "resubscribed_at": found,
                    }
                )

            if outstanding and self._clock() >= outstanding[1]:
                # Fixed: no further PING is sent while one is outstanding, so a
                # later ping can never extend an earlier one's deadline. Checked
                # after handling the message, so a valid one is not dropped.
                raise HeartbeatMissed(f"no reply to {outstanding[0]}")
            # Yield, so a flood of buffered messages cannot starve other tasks.
            await asyncio.sleep(0)

    def _forward(self, raw: Any) -> None:
        notification = notifications.parse(raw)
        try:
            # allow_nan=False as a backstop to parse(): never send a browser
            # something its JSON.parse will throw on.
            text = (
                None
                if notification is None
                else json.dumps(notification, allow_nan=False)
            )
        except ValueError:
            text = None
        if text is None:
            self.dropped += 1
            return
        self._relay(text)

    def _subscribed(self, detected_at: str) -> None:
        self.state = SUBSCRIBED
        resubscribed_at = now_iso()
        logger.info(f"subscribed to {self._channel}")
        self._notice(
            {
                "type": "resubscribed",
                "detected_at": detected_at,
                "resubscribed_at": resubscribed_at,
            }
        )

    def _interrupted(self, detected_at: str, error: BaseException) -> None:
        self.state = INTERRUPTED
        logger.warning(f"subscription lost: {type(error).__name__}: {error}")
        # "Detected", not "lost": when we noticed is not proof of when it went.
        self._notice({"type": "interrupted", "detected_at": detected_at})

    async def _close(self, subscription: Subscription) -> None:
        try:
            async with asyncio.timeout(1.0):
                await subscription.aclose()
        except Exception as error:  # noqa: BLE001 - closing a broken subscription
            logger.debug(f"closing the subscription failed: {error!r}")


def open_client(host: str, port: int) -> redis.asyncio.Redis:
    return redis.asyncio.Redis(
        host=host, port=port, decode_responses=True, **CLIENT_OPTIONS
    )
