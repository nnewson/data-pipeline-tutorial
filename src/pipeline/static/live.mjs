// The two claims the live page makes, kept apart from the page so the tests
// exercise exactly the code the browser runs.
//
// - The tracker says what Kafka offsets can and cannot reveal about the
//   notifications this page received.
// - The scheduler decides when the page reads its counts again: notifications
//   make it responsive, reconciliation keeps it correct.

/**
 * Track notifications per partition by offset.
 *
 * For this topic a partition's offsets are contiguous, so a jump reveals
 * offsets this page never saw. That is all it reveals: not how many
 * notifications were lost (some may still arrive late), not anything before the
 * first notification per partition or after the last, and nothing across a
 * recreated topic.
 *
 * observe() returns one of:
 *   first     the baseline for a partition — not evidence of anything
 *   next      exactly the offset after the high-water mark
 *   gap       above it: `unseen` is the range this page has not seen (yet)
 *   repeat    an offset this page has seen before: a replay published it again
 *   older     below the high-water mark but not in the recent-seen set: late,
 *             or a rewind, but not proven to be a repeat
 */
export function createTracker({ recentLimit = 1000 } = {}) {
  const highWater = new Map(); // partition -> highest offset seen; never decreases
  const recent = new Set(); // "partition:offset", insertion-ordered, bounded

  function remember(key) {
    recent.add(key);
    if (recent.size > recentLimit) {
      recent.delete(recent.values().next().value);
    }
  }

  function observe(partition, offset) {
    const key = `${partition}:${offset}`;
    const high = highWater.get(partition);
    let result;
    if (recent.has(key)) {
      result = { kind: "repeat" };
    } else if (high === undefined) {
      result = { kind: "first" };
    } else if (offset === high + 1) {
      result = { kind: "next" };
    } else if (offset > high + 1) {
      result = { kind: "gap", unseen: { from: high + 1, to: offset - 1 } };
    } else {
      result = { kind: "older" };
    }
    if (high === undefined || offset > high) {
      highWater.set(partition, offset);
    }
    if (result.kind !== "repeat") {
      remember(key);
    }
    return { partition, offset, ...result };
  }

  return { observe, highWater: (partition) => highWater.get(partition) };
}

/**
 * Decide when to read the counts again.
 *
 * - single flight: never more than one read at a time;
 * - follow-up: a request during a read causes exactly one read after it;
 * - throttled, leading and trailing: at most one read start per `minInterval`,
 *   so continuous notifications still produce reads — a debounce that resets
 *   on every event would never fire under steady traffic;
 * - a failed read retries with exponential backoff, and requests during the
 *   backoff wait for it rather than hammering an API that is refusing;
 * - reconciliation: `reconcileEvery` after the last successful read, a read
 *   happens whether or not any notification arrived. That is a refresh
 *   schedule, not a freshness guarantee — during an outage every read fails,
 *   and the time of the last successful one is the honest signal.
 *
 * The clock and timers are injected, so the tests drive time themselves.
 */
export function createScheduler({
  read,
  onResult = () => {},
  now = () => Date.now(),
  setTimer = (fn, ms) => setTimeout(fn, ms),
  clearTimer = (id) => clearTimeout(id),
  minInterval = 1000,
  reconcileEvery = 30000,
  retryInitial = 1000,
  retryMax = 30000,
} = {}) {
  let active = false;
  let inFlight = false;
  let dirty = false;
  let lastStart = -Infinity;
  let failures = 0;
  let throttleTimer = null;
  let retryTimer = null;
  let reconcileTimer = null;
  let lastSuccessAt = null;

  function clear(timer) {
    if (timer !== null) clearTimer(timer);
    return null;
  }

  function request() {
    if (!active) return;
    if (inFlight || retryTimer !== null) {
      dirty = true; // picked up when the read, or its retry, finishes
      return;
    }
    const wait = lastStart + minInterval - now();
    if (wait > 0) {
      if (throttleTimer === null) {
        throttleTimer = setTimer(() => {
          throttleTimer = null;
          run();
        }, wait);
      }
      return;
    }
    run();
  }

  function run() {
    if (!active) return;
    throttleTimer = clear(throttleTimer);
    reconcileTimer = clear(reconcileTimer);
    inFlight = true;
    dirty = false;
    lastStart = now();
    Promise.resolve()
      .then(read)
      .then(succeeded, failed);
  }

  function succeeded(value) {
    inFlight = false;
    failures = 0;
    lastSuccessAt = now();
    onResult({ ok: true, value, at: lastSuccessAt });
    if (!active) return;
    reconcileTimer = setTimer(() => {
      reconcileTimer = null;
      request();
    }, reconcileEvery);
    if (dirty) request();
  }

  function failed(error) {
    inFlight = false;
    failures += 1;
    onResult({ ok: false, error, lastSuccessAt });
    if (!active) return;
    const delay = Math.min(retryMax, retryInitial * 2 ** (failures - 1));
    dirty = false; // the retry reads everything a pending request wanted
    retryTimer = setTimer(() => {
      retryTimer = null;
      run();
    }, delay);
  }

  return {
    /** Something changed, or the page needs a read: ask for one. */
    request,
    /** Start (or resume, e.g. when the page becomes visible) with a read. */
    start() {
      active = true;
      request();
    },
    /** Stop scheduling reads, e.g. while the page is hidden. */
    stop() {
      active = false;
      throttleTimer = clear(throttleTimer);
      retryTimer = clear(retryTimer);
      reconcileTimer = clear(reconcileTimer);
    },
    lastSuccessAt: () => lastSuccessAt,
  };
}
