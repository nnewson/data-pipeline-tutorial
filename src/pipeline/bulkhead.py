"""A bulkhead per store: bounded, refusing admission to blocking store calls.

The API's clients are synchronous, so every store call runs on a worker thread.
Without a bulkhead all of them share one allowance of threads, and forty
requests stuck on a stalled Cassandra delay Redis requests while Redis is
healthy. With one, the promise is exactly this and no wider:

    A stalled store cannot consume another store's worker allowance.
    CPU, memory and the event loop remain shared.

Three rules make that true rather than decorative:

- **Admission refuses; it does not queue.** A full store answers "busy" at once
  and its client is never called. A limiter alone would *wait* for a token, and
  a stalled store would become a queue of waiting requests.
- **Admission is held until the work ends, not until the wait ends.** A thread
  cannot be interrupted, so a cancelled or timed-out request leaves its call
  running. The thread itself holds the permit until its call returns, and a
  kazoo request that outlives the thread holds it until kazoo completes it.
- **Every request has one deadline.** A route that makes several store calls
  spends one budget across all of them, not a fresh timeout on each, and a read
  that finishes past it is reported as unavailable, not as a late success.
"""

import logging
import threading
import time
from collections.abc import Callable
from typing import Protocol

import anyio
import anyio.to_thread

logger = logging.getLogger("bulkhead")


class Busy(Exception):
    """Refused at admission: the store's allowance is in use. Nothing was called."""

    def __init__(self, store: str) -> None:
        super().__init__(f"{store} busy")
        self.store = store


class Unavailable(Exception):
    """The store call failed or ran out of time, or the client is disconnected."""

    def __init__(self, store: str) -> None:
        super().__init__(f"{store} unavailable")
        self.store = store


class DeadlineExceeded(Exception):
    """The request's budget ran out before its reads finished."""


class Completion(Protocol):
    """The part of a kazoo AsyncResult a permit needs."""

    def rawlink(self, callback: Callable[..., None]) -> None: ...


_DISPATCHER = "dispatcher"


class Permit:
    """One admission, released exactly once, when every holder has let go.

    Up to three holders, and the slot goes back to the gate only when all of
    them have let go:

    - the **dispatcher**, until its `run_sync` call has returned or been
      interrupted;
    - the **worker thread**, until the read returns in that thread. This is the
      hold that tracks the work rather than the wait: a native asyncio
      cancellation interrupts the dispatcher's await while the thread runs on.
      If the task is cancelled before the thread starts, the read never runs
      and this hold is given up instead;
    - a **ZooKeeper request** the reader gave up on, until kazoo completes it.

    Why not release the semaphore directly? A duplicate release would go
    unnoticed. A `BoundedSemaphore` only complains when its count would exceed
    the starting value, so a late duplicate — arriving after another request has
    taken the returned slot — is silently accepted, and frees a slot that
    request is still using.
    """

    def __init__(self, release: Callable[[], None]) -> None:
        self._release = release
        self._lock = threading.Lock()
        self._holders: set[object] = {_DISPATCHER}
        self._released = False

    def hold(self) -> Callable[[], None]:
        """Add a holder. Returns its release, which is safe to call twice."""
        holder = object()
        with self._lock:
            if self._released:
                raise RuntimeError("permit already released; nothing left to hold")
            self._holders.add(holder)
        return lambda: self._drop(holder)

    def hold_until(self, result: Completion) -> None:
        """Keep admission until an abandoned request actually completes."""
        release = self.hold()
        # Outside the lock. kazoo runs callbacks inline once its handler has
        # stopped, and the callback takes this lock.
        result.rawlink(lambda _result: release())

    def dispatch_finished(self) -> None:
        """The dispatcher's hold, dropped after the thread call has returned."""
        self._drop(_DISPATCHER)

    @property
    def released(self) -> bool:
        with self._lock:
            return self._released

    def _drop(self, holder: object) -> None:
        # Idempotent per holder: kazoo's rawlink re-dispatches every callback on
        # a result when a new one is registered after completion, so the same
        # holder can be dropped twice.
        with self._lock:
            self._holders.discard(holder)
            if self._holders or self._released:
                return
            self._released = True
        self._release()


class Budget:
    """What one request may spend: time until its deadline, and its admission."""

    def __init__(
        self, seconds: float, permit: Permit, per_call: float | None = None
    ) -> None:
        self._deadline = time.monotonic() + seconds
        self._permit = permit
        self._per_call = per_call

    def remaining(self) -> float:
        """Seconds left, or DeadlineExceeded if there are none."""
        left = self._deadline - time.monotonic()
        if left <= 0:
            raise DeadlineExceeded
        return left

    def call_timeout(self) -> float:
        """The timeout for the next call: what remains, capped per call."""
        left = self.remaining()
        return left if self._per_call is None else min(left, self._per_call)

    def abandon(self, result: Completion) -> None:
        """Hand a still-outstanding request to the permit: it keeps admission."""
        self._permit.hold_until(result)


class _WorkerHold:
    """The worker thread's own hold on the permit.

    Held from before the thread is submitted until the read returns *in that
    thread*, so it tracks the work rather than the wait. That matters because
    anyio's shield covers anyio's cancellation and not asyncio's: measured, a
    native `Task.cancel()` interrupted the await while the thread ran on, and
    anyio returned the worker token at once. Without this hold the dispatcher's
    `finally` would have returned the slot too, admitting a replacement beside
    a call still in progress.

    If the task is cancelled before the thread ever starts, the read must not
    run at all, and the hold is given up then. A lock settles which happens.
    """

    def __init__(self, permit: Permit) -> None:
        self._release = permit.hold()
        self._lock = threading.Lock()
        self._state = "pending"

    def run[T](self, read: Callable[[Budget], T], budget: Budget) -> T | None:
        with self._lock:
            if self._state == "abandoned":
                return None  # nobody is waiting for this result
            self._state = "running"
        try:
            return read(budget)
        finally:
            self._release()

    def dispatcher_leaving(self) -> None:
        """The awaiting task is done; if the thread never started, it never will."""
        with self._lock:
            if self._state != "pending":
                return
            self._state = "abandoned"
        self._release()


class Bulkhead:
    """One store's admission gate and worker allowance.

    Two primitives, because one cannot do both jobs: acquiring a CapacityLimiter
    and then handing the same limiter to `run_sync` raises, since the task
    already holds one of its tokens. The gate is a thread-safe semaphore taken
    without blocking, because a kazoo completion can release it from kazoo's own
    thread. The worker limiter is the same size and keeps each store's threads
    out of the default allowance; these are separate *allowances*, not separate
    thread pools, since anyio reuses its worker threads across limiters. The
    permit, not the limiter, is what caps concurrent calls into the store: under
    native asyncio cancellation anyio returns the worker token while the thread
    is still running.

    Create inside the event loop: the worker limiter belongs to it.
    """

    def __init__(
        self,
        store: str,
        limit: int,
        deadline_seconds: float,
        failures: tuple[type[BaseException], ...],
        per_call_seconds: float | None = None,
        connected: Callable[[], bool] | None = None,
    ) -> None:
        self.store = store
        self.limit = limit
        self._gate = threading.BoundedSemaphore(limit)
        self._workers = anyio.CapacityLimiter(limit)
        self._deadline_seconds = deadline_seconds
        self._per_call_seconds = per_call_seconds
        self._failures = failures
        self._connected = connected
        # Only touched on the event loop, so it needs no lock.
        self._failing = False

    def admit(self) -> Permit:
        """A permit, or Busy at once. Never waits."""
        if not self._gate.acquire(blocking=False):
            raise Busy(self.store)
        return Permit(self._gate.release)

    async def run[T](self, read: Callable[[Budget], T]) -> T:
        """Run one blocking read under this store's admission and allowance."""
        # An early rejection only. The client can disconnect straight after this
        # check passes; the permit rules hold on that path too.
        if self._connected is not None and not self._connected():
            raise Unavailable(self.store)

        permit = self.admit()
        budget = Budget(self._deadline_seconds, permit, self._per_call_seconds)
        worker = _WorkerHold(permit)
        try:
            # abandon_on_cancel=False is anyio's default, written out because it
            # is anyio's half of the rule: under anyio's own cancellation the
            # await waits for its thread. The worker hold is the other half.
            result = await anyio.to_thread.run_sync(
                worker.run, read, budget, limiter=self._workers, abandon_on_cancel=False
            )
            # A read that finished past its deadline is late, not successful.
            # Redis in particular cannot be interrupted mid-command, so this is
            # where "past the deadline" becomes one rule for every store.
            budget.remaining()
        except (DeadlineExceeded, *self._failures) as error:
            self._note_failure(error)
            raise Unavailable(self.store) from error
        finally:
            worker.dispatcher_leaving()
            # After run_sync has returned, so on the ordinary path after anyio
            # has released the worker token.
            permit.dispatch_finished()
        self._note_success()
        return result

    def _note_failure(self, error: BaseException) -> None:
        # Transitions only. One line per failed request was measured at 31,000
        # lines in two minutes of a Cassandra outage under load.
        if not self._failing:
            self._failing = True
            logger.warning(
                f"{self.store} unavailable: {type(error).__name__}: {error} "
                "(further failures are not logged until it answers again)"
            )

    def _note_success(self) -> None:
        if self._failing:
            self._failing = False
            logger.info(f"{self.store} answering again")
