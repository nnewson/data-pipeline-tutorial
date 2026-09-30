import asyncio
import threading

import anyio
import pytest

from pipeline.bulkhead import (
    Budget,
    Bulkhead,
    Busy,
    DeadlineExceeded,
    Permit,
    Unavailable,
    _WorkerHold,
)


@pytest.fixture
def anyio_backend():
    return "asyncio"


class FakeResult:
    """A kazoo AsyncResult's completion behaviour, including its re-dispatch.

    kazoo's rawlink on a result that is already complete dispatches *every*
    registered callback again, not only the new one. Reproduced here, because it
    is the source of duplicate callbacks.
    """

    def __init__(self):
        self.callbacks = []
        self.done = False

    def rawlink(self, callback):
        if callback not in self.callbacks:
            self.callbacks.append(callback)
        if self.done:
            self._dispatch()

    def complete(self):
        self.done = True
        self._dispatch()

    def _dispatch(self):
        for callback in list(self.callbacks):
            callback(self)


class Gate:
    """A one-slot semaphore that records releases."""

    def __init__(self):
        self.semaphore = threading.BoundedSemaphore(1)
        self.releases = 0

    def acquire(self):
        return self.semaphore.acquire(blocking=False)

    def release(self):
        self.releases += 1
        self.semaphore.release()


def admitted(gate):
    assert gate.acquire()
    return Permit(gate.release)


# --- Permit ownership ---------------------------------------------------------


def test_a_permit_releases_once_when_the_dispatcher_finishes():
    gate = Gate()
    permit = admitted(gate)

    permit.dispatch_finished()
    permit.dispatch_finished()

    assert gate.releases == 1
    assert permit.released


def test_an_outstanding_request_keeps_admission_after_the_dispatcher_finishes():
    gate = Gate()
    permit = admitted(gate)
    result = FakeResult()

    permit.hold_until(result)
    permit.dispatch_finished()

    assert gate.releases == 0
    assert not gate.acquire(), "the slot must stay taken while the request is out"

    result.complete()
    assert gate.releases == 1
    assert gate.acquire()


def test_completion_before_the_callback_is_registered_still_waits_for_the_dispatcher():
    """The request finished between the timeout and the handover."""
    gate = Gate()
    permit = admitted(gate)
    result = FakeResult()
    result.done = True

    permit.hold_until(result)  # dispatches at once: the hold is dropped
    assert gate.releases == 0, "the dispatcher still holds the permit"

    permit.dispatch_finished()
    assert gate.releases == 1


def test_a_late_duplicate_callback_cannot_free_another_requests_slot():
    """The case a bare semaphore misses: the slot was already taken again."""
    gate = Gate()
    first = admitted(gate)
    result = FakeResult()
    first.hold_until(result)
    first.dispatch_finished()
    result.complete()
    assert gate.releases == 1

    second = admitted(gate)  # takes the slot the first request returned

    # kazoo re-dispatches every callback when another is registered on a
    # completed result: the first request's callback fires again.
    result.rawlink(lambda _result: None)

    assert gate.releases == 1
    assert not gate.acquire(), "the second request's slot must still be held"
    second.dispatch_finished()
    assert gate.acquire()


def test_a_bare_semaphore_really_does_miss_that_duplicate():
    """The reason for the permit, shown rather than asserted in a comment."""
    semaphore = threading.BoundedSemaphore(1)
    semaphore.acquire()
    semaphore.release()  # first request, released properly
    semaphore.acquire()  # second request takes the slot

    semaphore.release()  # the first request's duplicate: accepted silently

    assert semaphore.acquire(blocking=False), "a third request gets in"


def test_holding_after_release_is_a_bug_not_a_silent_success():
    gate = Gate()
    permit = admitted(gate)
    permit.dispatch_finished()

    with pytest.raises(RuntimeError):
        permit.hold_until(FakeResult())


# --- Budget -------------------------------------------------------------------


def test_a_spent_budget_raises_rather_than_returning_zero(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr("pipeline.bulkhead.time.monotonic", lambda: clock[0])
    budget = Budget(2.0, Permit(lambda: None), per_call=1.0)

    assert budget.call_timeout() == 1.0
    clock[0] = 101.5
    assert budget.call_timeout() == pytest.approx(0.5)
    clock[0] = 102.0
    with pytest.raises(DeadlineExceeded):
        budget.remaining()


# --- Bulkhead -----------------------------------------------------------------


def bulkhead(limit=1, **options):
    return Bulkhead("store", limit, 2.0, (ConnectionError,), **options)


@pytest.mark.anyio
async def test_a_full_bulkhead_refuses_at_once_without_calling_the_store():
    head = bulkhead()
    held = head.admit()  # the only slot, taken
    # Freed moments later: a gate that waited, even briefly, would get it.
    threading.Timer(0.1, held.dispatch_finished).start()
    called = []

    with pytest.raises(Busy, match="store busy"):
        await head.run(lambda budget: called.append(budget))

    assert called == []


@pytest.mark.anyio
async def test_store_failures_become_unavailable_and_return_the_slot():
    head = bulkhead()

    def fail(_budget):
        raise ConnectionError("gone")

    with pytest.raises(Unavailable, match="store unavailable"):
        await head.run(fail)

    head.admit()  # the slot came back


@pytest.mark.anyio
async def test_an_unexpected_error_is_not_dressed_up_as_unavailable():
    head = bulkhead()

    def bug(_budget):
        raise ValueError("a bug, not an outage")

    with pytest.raises(ValueError):
        await head.run(bug)
    head.admit()


@pytest.mark.anyio
async def test_a_spent_deadline_is_reported_as_unavailable():
    head = bulkhead()

    def slow(budget):
        raise DeadlineExceeded

    with pytest.raises(Unavailable):
        await head.run(slow)


@pytest.mark.anyio
async def test_a_disconnected_client_is_refused_before_admission():
    head = bulkhead(connected=lambda: False)
    called = []

    with pytest.raises(Unavailable):
        await head.run(lambda budget: called.append(budget))

    assert called == []
    head.admit()  # no slot was taken


@pytest.mark.anyio
async def test_a_disconnection_straight_after_the_check_leaves_the_permit_sound():
    """The check passed; the client dropped before the read."""
    head = bulkhead(connected=lambda: True)
    pending = FakeResult()

    def dropped_on_timeout(budget):
        budget.abandon(pending)
        raise ConnectionError("dropped")  # stands in for a kazoo timeout

    with pytest.raises(Unavailable):
        await head.run(dropped_on_timeout)
    with pytest.raises(Busy):
        head.admit()  # the abandoned request still holds it
    pending.complete()
    head.admit()

    head = bulkhead(connected=lambda: True)

    def dropped_outright(_budget):
        raise ConnectionError("ConnectionLoss")

    with pytest.raises(Unavailable):
        await head.run(dropped_outright)
    head.admit()


@pytest.mark.anyio
async def test_successful_intermediate_reads_release_exactly_once():
    """Only an abandoned request is handed over; completed ones get no hold."""
    head = bulkhead(limit=2)
    releases = []
    real_admit = head.admit

    def counting_admit():
        permit = real_admit()
        original = permit._release

        def release():
            releases.append(1)
            original()

        permit._release = release
        return permit

    head.admit = counting_admit

    def several_reads(budget):
        for _ in range(5):
            budget.call_timeout()  # each read completes within its timeout
        return "done"

    assert await head.run(several_reads) == "done"
    assert releases == [1]


@pytest.mark.anyio
async def test_admission_comes_back_only_after_the_worker_token():
    """Otherwise a newly admitted request could wait for a worker token."""
    head = bulkhead()
    tokens_at_release = []
    real_admit = head.admit

    def recording_admit():
        permit = real_admit()
        original = permit._release

        def release():
            tokens_at_release.append(head._workers.borrowed_tokens)
            original()

        permit._release = release
        return permit

    head.admit = recording_admit

    await head.run(lambda budget: None)

    assert tokens_at_release == [0]


@pytest.mark.anyio
async def test_a_cancelled_request_keeps_its_slot_until_its_thread_returns():
    head = bulkhead()
    started, finish = threading.Event(), threading.Event()

    def blocked(_budget):
        started.set()
        finish.wait(timeout=5)

    scope = anyio.CancelScope()

    async def request():
        with scope:
            await head.run(blocked)

    async with anyio.create_task_group() as group:
        group.start_soon(request)
        while not started.is_set():
            await anyio.sleep(0.01)
        scope.cancel()  # the request is cancelled mid-call
        await anyio.sleep(0.1)  # and the cancellation has had time to land

        # The await is cancelled, the thread is not: its slot stays taken, so
        # replacement work cannot push the store past its limit.
        with pytest.raises(Busy):
            head.admit()
        finish.set()

    head.admit()  # back once the thread returned


@pytest.mark.anyio
async def test_an_outage_is_logged_once_and_its_end_once(caplog):
    head = bulkhead()
    failing = [True]

    def read(_budget):
        if failing[0]:
            raise ConnectionError("refused")
        return "ok"

    with caplog.at_level("INFO", logger="bulkhead"):
        for _ in range(3):
            with pytest.raises(Unavailable):
                await head.run(read)
        failing[0] = False
        await head.run(read)
        await head.run(read)

    messages = [record.getMessage() for record in caplog.records]
    assert len(messages) == 2
    assert messages[0].startswith("store unavailable: ConnectionError: refused")
    assert messages[1] == "store answering again"


@pytest.mark.anyio
async def test_native_asyncio_cancellation_keeps_the_slot_until_the_thread_returns():
    """anyio's shield does not cover asyncio's own Task.cancel().

    Measured: the await is interrupted at once, and anyio returns the worker
    token while the thread runs on. The thread's own hold is what keeps a
    replacement out.
    """
    head = bulkhead()
    started, finish, done = threading.Event(), threading.Event(), threading.Event()

    def blocked(_budget):
        started.set()
        finish.wait(timeout=5)
        done.set()

    task = asyncio.ensure_future(head.run(blocked))
    while not started.is_set():
        await asyncio.sleep(0.01)
    task.cancel()
    await asyncio.sleep(0.1)
    assert task.done(), "the await was interrupted"
    assert not done.is_set(), "while the worker is still running"

    with pytest.raises(Busy):
        await head.run(lambda budget: "replacement")

    finish.set()
    while not done.is_set():
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.05)
    assert await head.run(lambda budget: "replacement") == "replacement"


@pytest.mark.anyio
async def test_a_request_cancelled_before_its_thread_starts_never_reads():
    head = bulkhead()
    # Every worker token taken, so the request waits for one before any thread.
    blocker = object()
    await head._workers.acquire_on_behalf_of(blocker)
    read = []

    task = asyncio.ensure_future(head.run(lambda budget: read.append(1)))
    await asyncio.sleep(0.05)
    task.cancel()
    await asyncio.sleep(0.05)
    head._workers.release_on_behalf_of(blocker)
    await asyncio.sleep(0.1)

    assert read == [], "the read ran for a request nobody was waiting on"
    head.admit()  # and its slot came back


@pytest.mark.anyio
async def test_a_read_that_finishes_past_its_deadline_is_unavailable(monkeypatch):
    """Late is not successful: Redis, which cannot be interrupted, relies on it."""
    clock = [100.0]
    monkeypatch.setattr("pipeline.bulkhead.time.monotonic", lambda: clock[0])
    head = bulkhead()  # a two-second deadline

    def slow_but_successful(budget):
        budget.remaining()  # fine at the start
        clock[0] = 102.4  # the last command returns after the deadline
        return {"/docs": 3}

    with pytest.raises(Unavailable):
        await head.run(slow_but_successful)
    head.admit()


def test_a_thread_that_starts_after_its_dispatcher_left_does_not_read():
    """The race in between: submitted, then cancelled, then the thread starts.

    anyio cannot be made to land a cancellation in that gap on demand, so the
    ordering is pinned here directly.
    """
    gate = Gate()
    permit = admitted(gate)
    worker = _WorkerHold(permit)
    read = []

    worker.dispatcher_leaving()  # the awaiting task was cancelled first
    permit.dispatch_finished()
    assert gate.releases == 1, "nothing is running, so the slot is back"

    assert worker.run(lambda budget: read.append(1), Budget(2.0, permit)) is None
    assert read == [], "a read nobody is waiting on must not run beside a replacement"


def test_a_running_thread_keeps_its_hold_when_the_dispatcher_leaves():
    gate = Gate()
    permit = admitted(gate)
    worker = _WorkerHold(permit)
    releases_during_read = []

    def read(_budget):
        worker.dispatcher_leaving()  # cancelled while the read runs
        permit.dispatch_finished()
        releases_during_read.append(gate.releases)
        return "done"

    assert worker.run(read, Budget(2.0, permit)) == "done"
    assert releases_during_read == [0]
    assert gate.releases == 1
