"""Fan-out from the one subscription to every socket, never waiting for any.

The broadcaster is a plain function, not a coroutine: it *cannot* wait for a
socket. Each socket has a bounded queue and a sender task of its own, and the
broadcaster only ever does a non-blocking put. A socket whose queue is full
cannot keep up, so it is removed from fan-out at once and closed separately —
the way Redis treats a slow subscriber, and the way it would treat this API.

What that detects is *transport* backpressure, not how fast a page's JavaScript
runs: a browser that is behind may still be draining the network perfectly
well.
"""

import asyncio
import contextlib
import json
import logging
from typing import Protocol

import anyio

logger = logging.getLogger("fanout")

# Tutorial defaults, not derived.
SOCKET_LIMIT = 100
QUEUE_SIZE = 100
CLOSE_TIMEOUT_SECONDS = 1.0

# The one close code this module sends, and only to an accepted socket. A
# refused handshake is an HTTP rejection that a browser may report as 1006, and
# at shutdown uvicorn closes every socket itself with 1012, "service restart"
# (measured) — so the page treats every close alike: back off and reconnect.
TRY_AGAIN_LATER = 1013


class Socket(Protocol):
    """The part of Starlette's WebSocket a member uses."""

    async def send_text(self, data: str) -> None: ...

    async def receive(self) -> dict: ...

    async def close(self, code: int = 1000, reason: str | None = None) -> None: ...


class Member:
    def __init__(self, queue_size: int) -> None:
        self.queue: asyncio.Queue[str] = asyncio.Queue(maxsize=queue_size)
        self.evicted = asyncio.Event()

    async def send_all(self, socket: Socket) -> None:
        while True:
            await socket.send_text(await self.queue.get())


class Hub:
    """Admission, membership and the broadcast.

    Admission and membership are separate on purpose. A socket leaves fan-out
    the moment it is evicted, but keeps its admission slot until its cleanup has
    finished — otherwise churn could pile up closing sockets beyond the limit.
    0.8's rule again: admission follows the work, not the decision to stop it.
    """

    def __init__(self, limit: int = SOCKET_LIMIT, queue_size: int = QUEUE_SIZE) -> None:
        self.limit = limit
        self._queue_size = queue_size
        self._admitted = 0
        self._members: set[Member] = set()
        self.evictions = 0

    @property
    def admitted(self) -> int:
        return self._admitted

    def admit(self) -> bool:
        if self._admitted >= self.limit:
            return False
        self._admitted += 1
        return True

    def release(self) -> None:
        self._admitted -= 1

    def join(self, first: str) -> Member:
        member = Member(self._queue_size)
        # Queued before joining, with no await in between, so `first` always
        # precedes anything broadcast.
        member.queue.put_nowait(first)
        self._members.add(member)
        return member

    def leave(self, member: Member) -> None:
        self._members.discard(member)

    def broadcast(self, text: str) -> None:
        for member in list(self._members):
            try:
                member.queue.put_nowait(text)
            except asyncio.QueueFull:
                self._evict(member)

    def notice(self, notice: dict) -> None:
        self.broadcast(json.dumps(notice))

    def _evict(self, member: Member) -> None:
        self._members.discard(member)
        self.evictions += 1
        member.evicted.set()


async def attend(
    hub: Hub,
    socket: Socket,
    first: str,
    close_timeout: float = CLOSE_TIMEOUT_SECONDS,
) -> None:
    """Serve one accepted socket until it closes, fails or is evicted.

    The caller holds the admission slot and releases it after this returns,
    which is after every task here has finished.

    An anyio task group rather than asyncio.wait and gather. Starlette runs the
    endpoint under anyio, and a cancellation arriving while asyncio.gather waited
    for tasks this function had just cancelled leaked a CancelledError out of
    anyio's cancel scope (measured, through the test client's disconnect). The
    task group also gives the ordering eviction needs for free: it exits only
    once every task has finished, so the sender has stopped before the close —
    a WebSocket must not be sent to and closed at the same time.
    """
    member = hub.join(first)
    evicted = False
    try:
        async with anyio.create_task_group() as group:

            async def until(event_source) -> None:
                # A failed send or receive ends this socket's service and nothing
                # more; whichever finishes first stops the others.
                try:
                    await event_source()
                except Exception as error:  # noqa: BLE001 - a socket that failed is a socket that closed
                    logger.debug(f"socket ended: {error!r}")
                finally:
                    group.cancel_scope.cancel()

            async def watch_eviction() -> None:
                nonlocal evicted
                await member.evicted.wait()
                evicted = True
                group.cancel_scope.cancel()

            group.start_soon(until, lambda: member.send_all(socket))
            group.start_soon(until, lambda: _until_closed(socket))
            group.start_soon(watch_eviction)
        if evicted:
            logger.info("evicted a socket that could not keep up")
            with anyio.move_on_after(close_timeout), contextlib.suppress(Exception):
                await socket.close(code=TRY_AGAIN_LATER)
    finally:
        # Including a sender that was waiting on an empty queue: the task group
        # has already cancelled it. Nothing later would — uvicorn waits for
        # connection tasks before the lifespan's shutdown ever runs.
        hub.leave(member)


async def _until_closed(socket: Socket) -> None:
    # Clients send nothing that matters; this loop exists to notice the close.
    while (await socket.receive())["type"] != "websocket.disconnect":
        pass
