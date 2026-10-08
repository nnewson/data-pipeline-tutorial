"""Fan-out: no socket can make the broadcaster wait, or outlive its admission."""

import asyncio
import inspect
import json

import pytest

from pipeline.fanout import TRY_AGAIN_LATER, Hub, attend


@pytest.fixture
def anyio_backend():
    return "asyncio"


class FakeSocket:
    def __init__(self, *, blocked=False, close_blocks=False):
        self.sent: list[str] = []
        self.blocked = blocked
        self.close_blocks = close_blocks
        self.closed_with: int | None = None
        self.sending = False
        self.incoming: asyncio.Queue = asyncio.Queue()

    async def send_text(self, data):
        self.sending = True
        try:
            if self.blocked:
                await asyncio.Event().wait()  # a send that never returns
            self.sent.append(data)
        finally:
            self.sending = False

    async def receive(self):
        return await self.incoming.get()

    async def close(self, code=1000, reason=None):
        # An ASGI WebSocket does not support a send and a close at once.
        if self.sending:
            raise RuntimeError("closed while a send was in progress")
        self.closed_with = code
        if self.close_blocks:
            await asyncio.Event().wait()

    def disconnect(self):
        self.incoming.put_nowait({"type": "websocket.disconnect", "code": 1000})


async def serve(hub, socket, **options):
    """What the endpoint does around attend(): the slot is released last."""
    assert hub.admit()
    try:
        await attend(hub, socket, json.dumps({"type": "ready"}), **options)
    finally:
        hub.release()


async def until(condition, timeout=2.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not condition():
        if loop.time() > deadline:
            raise AssertionError("condition not reached")
        await asyncio.sleep(0.005)


def test_the_broadcaster_cannot_wait():
    """Structurally: a plain function, not a coroutine."""
    assert not inspect.iscoroutinefunction(Hub.broadcast)


@pytest.mark.anyio
async def test_a_blocked_sender_is_evicted_and_nobody_else_waits():
    hub = Hub(queue_size=5)
    fast, stuck = FakeSocket(), FakeSocket(blocked=True)
    tasks = [asyncio.create_task(serve(hub, socket)) for socket in (fast, stuck)]
    await until(lambda: len(fast.sent) == 1)  # both enrolled, ready delivered

    for n in range(50):
        hub.broadcast(json.dumps({"n": n}))
        await asyncio.sleep(0)  # as the bridge does between messages

    await until(lambda: len(fast.sent) == 51)
    assert [json.loads(m).get("n") for m in fast.sent[1:]] == list(range(50))
    await until(lambda: stuck.closed_with == TRY_AGAIN_LATER)
    assert hub.evictions == 1

    fast.disconnect()
    await asyncio.wait_for(asyncio.gather(*tasks), 2)
    assert hub.admitted == 0


@pytest.mark.anyio
async def test_eviction_leaves_fan_out_at_once():
    hub = Hub(queue_size=1)
    stuck = FakeSocket(blocked=True, close_blocks=True)
    task = asyncio.create_task(serve(hub, stuck, close_timeout=5))
    await asyncio.sleep(0.01)  # the sender has taken `ready` and blocked

    hub.broadcast("a")  # fills the queue
    hub.broadcast("b")  # overflows: evicted
    member = next(iter(hub._members), None)
    assert member is None, "removed from fan-out the moment it was evicted"
    hub.broadcast("c")  # reaches nobody, and raises nothing
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


@pytest.mark.anyio
async def test_an_evicted_socket_keeps_its_slot_until_cleanup_finishes():
    """Otherwise churn could pile up closing sockets beyond the limit."""
    hub = Hub(limit=1, queue_size=1)
    stuck = FakeSocket(blocked=True, close_blocks=True)
    task = asyncio.create_task(serve(hub, stuck, close_timeout=0.3))
    await asyncio.sleep(0.01)
    hub.broadcast("a")
    hub.broadcast("b")  # evicted; its close now hangs for up to 0.3s

    await until(lambda: stuck.closed_with == TRY_AGAIN_LATER)
    assert not hub.admit(), "the slot is still held while cleanup runs"

    await asyncio.wait_for(task, 2)  # the bounded close gives up
    assert hub.admit(), "and released once cleanup has finished"


@pytest.mark.anyio
async def test_a_disconnect_cancels_a_sender_waiting_on_an_empty_queue():
    hub = Hub()
    socket = FakeSocket()
    before = len(asyncio.all_tasks())
    task = asyncio.create_task(serve(hub, socket))
    await until(lambda: socket.sent)  # ready delivered; the sender now waits

    socket.disconnect()
    await asyncio.wait_for(task, 1)

    assert len(asyncio.all_tasks()) == before, "nothing was left behind"
    assert hub.admitted == 0


@pytest.mark.anyio
async def test_ready_is_always_the_first_message():
    hub = Hub()
    socket = FakeSocket()
    task = asyncio.create_task(serve(hub, socket))
    await asyncio.sleep(0)  # joined, nothing sent yet
    hub.broadcast("early")
    await until(lambda: len(socket.sent) == 2)

    assert json.loads(socket.sent[0]) == {"type": "ready"}
    assert socket.sent[1] == "early"
    socket.disconnect()
    await task


@pytest.mark.anyio
async def test_the_limit_refuses_admission():
    hub = Hub(limit=2)
    assert hub.admit() and hub.admit()
    assert not hub.admit()
    hub.release()
    assert hub.admit()
