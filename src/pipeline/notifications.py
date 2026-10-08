"""Notifications that the consumer has applied an event, over Redis pub/sub.

Disposable by design. Redis delivers each publication at most once to each
current subscriber and keeps nothing, so a subscriber that was not listening
has simply missed it. The pipeline adds the other half: a Kafka replay applies
an event again and publishes it again, so per event a subscriber hears it zero,
one or several times.

That is fine for what these are for — telling a live page that something
changed, so it can read the state again. A notification is a hint, never the
state itself.
"""

import json
import logging
import math
from typing import Any

import redis
from redis.backoff import NoBackoff
from redis.retry import Retry

from pipeline.config import REDIS_KEY_PREFIX

logger = logging.getLogger("notifications")

CHANNEL = "events:pageviews"

# What the bridge will relay. Anyone who can reach Redis can publish on the
# channel, so the shape and the size are checked, not assumed.
MAX_BYTES = 2048
FIELDS = {
    "event_id": str,
    "user_id": str,
    "page": str,
    "partition": int,
    "offset": int,
    "timestamp": (int, float),
}

# The notification format's limit, not Kafka's. The page's tracker does
# arithmetic on partitions and offsets, so they must be integers JavaScript
# represents exactly; above 2**53 - 1 a browser reads a JSON number as an
# approximation, or as Infinity, and one such offset would poison a
# partition's high-water mark. Kafka offsets are 64-bit and can exceed this —
# supporting them would take another representation, such as decimal strings,
# which this release does not need.
MAX_SAFE_INTEGER = (1 << 53) - 1
INTEGER_FIELDS = ("partition", "offset")

# The notification's own client: short timeouts and no retries. The consumer's
# other Redis writes keep redis-py's defaults, which took 57.9s to fail one
# command against a paused server (measured at 0.8); a best-effort notification
# must not add a stall like that to every event.
CLIENT_OPTIONS = {
    "socket_timeout": 0.5,
    "socket_connect_timeout": 0.5,
    "retry": Retry(NoBackoff(), 0),
}

# Only failures that mean "the publish was not confirmed". Not "Redis did not
# take it": after a timeout the PUBLISH may well have happened, its reply lost.
# Anything else is a bug.
FAILURES = (redis.ConnectionError, redis.TimeoutError, OSError)


def channel(prefix: str = REDIS_KEY_PREFIX) -> str:
    """The channel, under the key prefix.

    Pub/sub channels are server-wide, not per database: a PUBLISH on db 0
    reaches a subscriber on db 1 (measured). The prefix is what keeps one run's
    notifications apart from another's.
    """
    return f"{prefix}{CHANNEL}"


def encode(event: dict, partition: int, offset: int) -> str:
    """The notification for one applied event.

    Partition and offset are metadata the pipeline genuinely owns, and for this
    topic a partition's offsets are contiguous, so a subscriber can notice some
    discontinuities. No count: four consumers publish concurrently, so values
    for one page can arrive out of order. Announce that something changed and
    let the reader fetch the value.
    """
    return json.dumps(
        {
            "event_id": event["event_id"],
            "user_id": event["user_id"],
            "page": event["page"],
            "partition": partition,
            "offset": offset,
            "timestamp": event["timestamp"],
        }
    )


def parse(raw: Any) -> dict | None:
    """A relayable notification, or None for anything oversized or misshapen.

    Python's json accepts NaN and Infinity, which are not JSON: relayed as they
    are, a browser's JSON.parse throws on them. A finite timestamp is required,
    which refuses those and a number too large to be finite (1e400 parses as
    infinity) alike.
    """
    if not isinstance(raw, str) or len(raw.encode()) > MAX_BYTES:
        return None
    try:
        value = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(value, dict):
        return None
    for field, kind in FIELDS.items():
        # bool is an int to isinstance; an offset of True is not an offset.
        if not isinstance(value.get(field), kind) or isinstance(value[field], bool):
            return None
    for field in INTEGER_FIELDS:
        # Compared as integers: converting to a float first would round the
        # very values this exists to refuse.
        if not 0 <= value[field] <= MAX_SAFE_INTEGER:
            return None
    try:
        finite = math.isfinite(value["timestamp"])
    except OverflowError:
        # An integer too large for a float, such as 10**400: under the size
        # limit, and json parses it without complaint.
        return None
    if not finite:
        return None
    return {"type": "pageview", **{field: value[field] for field in FIELDS}}


class Notifier:
    """Publishes notifications, best-effort.

    An unconfirmed publish is logged and swallowed — unconfirmed, not failed:
    after a timeout the PUBLISH may have happened and only its reply been lost. The alternative — failing the
    handler like every other write — would leave the offset uncommitted, and the
    replay would run the INCR again: a lost notification would corrupt a
    counter. A write whose loss is harmless must not cause a replay that is not.

    A return of 0 from PUBLISH is not a failure. It means nobody was listening,
    which is normal, so it is not logged at all.
    """

    def __init__(self, client: redis.Redis, prefix: str = REDIS_KEY_PREFIX) -> None:
        self._client = client
        self._channel = channel(prefix)
        self._failing = False

    def announce(self, event: dict, partition: int, offset: int) -> None:
        try:
            self._client.publish(self._channel, encode(event, partition, offset))
        except FAILURES as error:
            if not self._failing:
                self._failing = True
                logger.warning(
                    f"notification publish unconfirmed: {type(error).__name__}: "
                    f"{error} (further failures are not logged until one succeeds)"
                )
            return
        if self._failing:
            self._failing = False
            logger.info("notifications publishing again")
