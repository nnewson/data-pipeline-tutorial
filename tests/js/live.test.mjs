// The live page's claims, tested against the module the page itself imports.
//
//   node --test "tests/js/*.test.mjs"

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { createScheduler, createTracker } from "../../src/pipeline/static/live.mjs";

// --- The tracker ----------------------------------------------------------

function kinds(sequence, options) {
  const tracker = createTracker(options);
  return sequence.map(([partition, offset]) => {
    const result = tracker.observe(partition, offset);
    return result.unseen
      ? `${result.kind} ${result.unseen.from}-${result.unseen.to}`
      : result.kind;
  });
}

describe("tracker", () => {
  const cases = [
    {
      name: "the first offset is a baseline, not evidence",
      sequence: [[0, 41], [0, 42]],
      expected: ["first", "next"],
    },
    {
      name: "partitions are tracked independently when interleaved",
      sequence: [[0, 5], [1, 90], [0, 6], [1, 91], [0, 7]],
      expected: ["first", "first", "next", "next", "next"],
    },
    {
      name: "a jump reports the unseen range, not a count of losses",
      sequence: [[0, 8], [0, 12]],
      expected: ["first", "gap 9-11"],
    },
    {
      name: "a late arrival inside a gap is older, not a repeat",
      sequence: [[0, 8], [0, 10], [0, 9]],
      expected: ["first", "gap 9-9", "older"],
    },
    {
      name: "the high-water mark never moves backwards",
      // After 10, 8: resetting to 8 would make 11 look like a gap.
      sequence: [[0, 7], [0, 10], [0, 8], [0, 11]],
      expected: ["first", "gap 8-9", "older", "next"],
    },
    {
      name: "a replay of offsets seen recently is a proven repeat",
      sequence: [[2, 100], [2, 101], [2, 102], [2, 100], [2, 101], [2, 102], [2, 103]],
      expected: ["first", "next", "next", "repeat", "repeat", "repeat", "next"],
    },
    {
      name: "an offset the page never saw, below the mark, is older",
      sequence: [[0, 50], [0, 49]],
      expected: ["first", "older"],
    },
  ];

  for (const { name, sequence, expected } of cases) {
    test(name, () => assert.deepEqual(kinds(sequence), expected));
  }

  test("a replay older than the recent-seen set is older, not proven", () => {
    const sequence = [[0, 1], [0, 2], [0, 3], [0, 4], [0, 1]];
    assert.deepEqual(kinds(sequence, { recentLimit: 3 }), [
      "first", "next", "next", "next", "older",
    ]);
  });

  test("the recent-seen set stays bounded", () => {
    const tracker = createTracker({ recentLimit: 100 });
    for (let offset = 0; offset < 10_000; offset += 1) tracker.observe(0, offset);
    // Offset 0 was long ago evicted; 9_950 is still remembered.
    assert.equal(tracker.observe(0, 0).kind, "older");
    assert.equal(tracker.observe(0, 9_950).kind, "repeat");
  });

  test("tracking survives a reconnect, so the gap it caused is visible", () => {
    // The page keeps one tracker for its whole session; a new socket is not a
    // new baseline. Offsets 13-19 were published while it was disconnected.
    const tracker = createTracker();
    for (const offset of [10, 11, 12]) tracker.observe(3, offset);
    // ... socket closes, page reconnects ...
    assert.deepEqual(tracker.observe(3, 20), {
      partition: 3,
      offset: 20,
      kind: "gap",
      unseen: { from: 13, to: 19 },
    });
  });
});

// --- The scheduler --------------------------------------------------------

/** A clock and timers the test advances by hand. */
function fakeTime() {
  let current = 0;
  let nextId = 1;
  const timers = new Map();
  return {
    now: () => current,
    setTimer(fn, ms) {
      const id = nextId++;
      timers.set(id, { due: current + ms, fn });
      return id;
    },
    clearTimer: (id) => timers.delete(id),
    async advance(ms) {
      const until = current + ms;
      for (;;) {
        await settle();
        const due = [...timers.entries()]
          .filter(([, timer]) => timer.due <= until)
          .sort(([, a], [, b]) => a.due - b.due)[0];
        if (!due) break;
        const [id, timer] = due;
        timers.delete(id);
        current = timer.due;
        timer.fn();
      }
      current = until;
      await settle();
    },
  };
}

async function settle() {
  for (let i = 0; i < 10; i += 1) await Promise.resolve();
}

/** A read whose every call the test resolves or rejects by hand. */
function controlledRead() {
  const calls = [];
  const read = () =>
    new Promise((resolve, reject) => calls.push({ resolve, reject }));
  return { read, calls };
}

function scheduler(time, read, overrides = {}) {
  const results = [];
  const instance = createScheduler({
    read,
    onResult: (result) => results.push(result),
    now: time.now,
    setTimer: time.setTimer,
    clearTimer: time.clearTimer,
    minInterval: 1000,
    reconcileEvery: 30000,
    retryInitial: 1000,
    retryMax: 8000,
    ...overrides,
  });
  return { instance, results };
}

describe("scheduler", () => {
  test("starting reads at once", async () => {
    const time = fakeTime();
    const { read, calls } = controlledRead();
    scheduler(time, read).instance.start();
    await settle();
    assert.equal(calls.length, 1);
  });

  test("single flight: requests during a read start no second read", async () => {
    const time = fakeTime();
    const { read, calls } = controlledRead();
    const { instance } = scheduler(time, read);
    instance.start();
    await settle();
    for (let i = 0; i < 20; i += 1) instance.request();
    await time.advance(5000);
    assert.equal(calls.length, 1, "still only the first read");
  });

  test("follow-up: requests during a read cause exactly one read after it", async () => {
    const time = fakeTime();
    const { read, calls } = controlledRead();
    const { instance } = scheduler(time, read);
    instance.start();
    await settle();
    instance.request();
    instance.request();
    await time.advance(2000);
    calls[0].resolve({ "/docs": 1 });
    await time.advance(2000);
    assert.equal(calls.length, 2);
    calls[1].resolve({ "/docs": 2 });
    await time.advance(5000);
    assert.equal(calls.length, 2, "no read without a reason");
  });

  test("throttled: continuous notifications still produce reads", async () => {
    // The case a trailing debounce fails: events every 100ms for ten seconds.
    const time = fakeTime();
    let reads = 0;
    const { instance } = scheduler(time, async () => {
      reads += 1;
      return {};
    });
    instance.start();
    for (let step = 0; step < 100; step += 1) {
      instance.request();
      await time.advance(100);
    }
    assert.ok(reads >= 9 && reads <= 11, `about one read a second, got ${reads}`);
  });

  test("a failed read retries with backoff, and requests wait for it", async () => {
    const time = fakeTime();
    const { read, calls } = controlledRead();
    const { instance, results } = scheduler(time, read);
    instance.start();
    await settle();
    calls[0].reject(new Error("503 redis busy"));
    await settle();
    assert.equal(results.at(-1).ok, false);

    instance.request(); // a notification during the backoff
    await time.advance(999);
    assert.equal(calls.length, 1, "the backoff is respected");
    await time.advance(1);
    assert.equal(calls.length, 2, "retried after 1s");

    calls[1].reject(new Error("503"));
    await settle();
    // A notification during the 2s backoff. The throttle alone would allow a
    // read after 1s, so this is where respecting the backoff shows.
    instance.request();
    await time.advance(1999);
    assert.equal(calls.length, 2, "the longer backoff is respected too");
    await time.advance(1);
    assert.equal(calls.length, 3, "then after 2s");

    calls[2].resolve({ "/docs": 4 });
    await settle();
    assert.equal(results.at(-1).ok, true);
    assert.equal(instance.lastSuccessAt(), time.now());
  });

  test("backoff is capped", async () => {
    const time = fakeTime();
    const starts = [];
    const { instance } = scheduler(time, () => {
      starts.push(time.now());
      return Promise.reject(new Error("down"));
    });
    instance.start();
    await time.advance(60000);
    const gaps = starts.slice(1).map((at, i) => at - starts[i]);
    assert.deepEqual(gaps.slice(0, 5), [1000, 2000, 4000, 8000, 8000]);
  });

  test("notification-free reconciliation: reads happen with no notifications", async () => {
    const time = fakeTime();
    let reads = 0;
    const { instance } = scheduler(time, async () => {
      reads += 1;
      return {};
    });
    instance.start();
    await time.advance(95000);
    assert.equal(reads, 4, "at 0, 30s, 60s and 90s");
  });

  test("decisive: a suppressed final notification is caught by reconciliation", async () => {
    // Nothing disconnects and nothing resubscribes, so the only path left to
    // the new value is the periodic read. A recovery-triggered refresh would
    // hide a broken periodic path; this test has none.
    const time = fakeTime();
    let stored = 10;
    const { instance, results } = scheduler(time, async () => stored);
    instance.start();
    await time.advance(1000);
    assert.equal(results.at(-1).value, 10);

    stored = 11; // the event is applied; its notification never arrives
    await time.advance(29000);
    assert.equal(results.at(-1).value, 11, "caught up within one reconciliation period");
  });

  test("stopping cancels every scheduled read", async () => {
    const time = fakeTime();
    let reads = 0;
    const { instance } = scheduler(time, async () => {
      reads += 1;
      return {};
    });
    instance.start();
    await settle();
    instance.stop();
    instance.request();
    await time.advance(120000);
    assert.equal(reads, 1);
  });
});
