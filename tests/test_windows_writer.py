"""The windows writer's shell: parsing, batching, committing, announcing.

The script's own rules — refuse older, replace on newer, skip expired — run in
Redis, so they are checked against Redis: the smoke test's `window writes`.
"""

import json
from dataclasses import dataclass

import pytest
import redis

from pipeline import notifications, windows_writer
from pipeline.windows_writer import EXPIRED, REFUSED, WRITTEN, Result, Writer

START = 1_791_000_000  # a ten-second boundary
END = START + 10


def raw(page="/docs", views=3, generation=2, start=START, end=END, **extra):
    iso = "%Y-%m-%dT%H:%M:%SZ"
    from datetime import UTC, datetime

    return json.dumps(
        {
            "window_start": datetime.fromtimestamp(start, UTC).strftime(iso),
            "window_end": datetime.fromtimestamp(end, UTC).strftime(iso),
            "page": page,
            "views": views,
            "generation": generation,
            **extra,
        }
    ).encode()


def test_a_result_is_parsed_with_its_bounds_as_epoch_seconds():
    assert windows_writer.parse(raw()) == Result(START, END, "/docs", 3, 2)


def test_a_result_from_before_generations_is_generation_zero():
    """0.10's jobs wrote none: no keeper submitted them."""
    value = json.loads(raw())
    del value["generation"]

    assert windows_writer.parse(json.dumps(value)).generation == 0


@pytest.mark.parametrize(
    "value",
    [
        b"not json",
        raw(views=True),
        raw(views=-1),
        raw(generation="2"),
        raw(page=""),
        raw(page="x" * 129),
        raw(end=START),
        json.dumps({"window_start": "yesterday", "page": "/docs"}).encode(),
        json.dumps({"page": "/docs", "views": 1}).encode(),  # no bounds
    ],
)
def test_a_misshapen_result_is_refused(value):
    assert windows_writer.parse(value) is None


class FakePipe:
    def __init__(self, fake):
        self.fake, self.calls = fake, []

    def execute(self):
        if self.fake.fail:
            raise redis.ConnectionError("Redis went away")
        return [self.fake.outcomes.get(args[1], WRITTEN) for _keys, args in self.calls]


class FakeRedis:
    def __init__(self):
        self.outcomes: dict[str, int] = {}
        self.fail = False
        self.transactions: list[bool] = []
        self.published: list[tuple[str, str]] = []
        self.pipes: list[FakePipe] = []

    def register_script(self, _source):
        def script(keys, args, client):
            client.calls.append((keys, args))

        return script

    def pipeline(self, transaction=True):
        self.transactions.append(transaction)
        pipe = FakePipe(self)
        self.pipes.append(pipe)
        return pipe

    def publish(self, channel, message):
        self.published.append((channel, message))
        return 0


def test_each_result_is_one_script_call_with_expiry_from_event_time():
    fake = FakeRedis()
    writer = Writer(fake, prefix="p:", retention=3600, clock=lambda: 5000.7)

    writer.write([Result(START, END, "/docs", 3, 2)])

    ((keys, args),) = fake.pipes[0].calls
    assert keys == [f"p:windows:{START}", f"p:windows:{START}:meta", "p:windows"]
    assert args == [2, "/docs", 3, START, END, END + 3600, 5000]
    # Each script is atomic by itself; the batch needs no transaction.
    assert fake.transactions == [False]


@dataclass
class Message:
    value: bytes
    offset: int


class FakeConsumer:
    def __init__(self, batches):
        self.batches = list(batches)
        self.commits = 0
        self.seeks: list[tuple[str, int]] = []

    def poll(self, timeout_ms):
        return self.batches.pop(0) if self.batches else {}

    def commit(self):
        self.commits += 1

    def seek(self, partition, offset):
        self.seeks.append((partition, offset))


def test_offsets_are_committed_after_the_writes_and_hints_follow_written_windows():
    fake = FakeRedis()
    fake.outcomes = {"/old": REFUSED, "/gone": EXPIRED}
    batch = {
        "p0": [Message(raw(page="/docs"), 10), Message(raw(page="/pricing"), 11)],
        "p1": [Message(raw(page="/old", start=START + 10, end=END + 10), 4)],
        "p2": [Message(raw(page="/gone", start=START + 20, end=END + 20), 7)],
    }
    consumer = FakeConsumer([batch])

    windows_writer.run(
        consumer, Writer(fake), windows_writer.Hints(fake, prefix="p:"), polls=1
    )

    assert consumer.commits == 1
    # One hint for the one window written, though two of its pages were; none
    # for the refused window or the expired one.
    assert fake.published == [
        ("p:events:windows", notifications.encode_window(START, END))
    ]


def test_a_failed_batch_is_not_committed_and_is_written_again():
    fake = FakeRedis()
    fake.fail = True
    batch = {"p0": [Message(raw(), 10), Message(raw(page="/pricing"), 11)]}
    consumer = FakeConsumer([batch, batch])
    slept = []

    def sleep(seconds):
        slept.append(seconds)
        fake.fail = False  # Redis comes back

    windows_writer.run(consumer, Writer(fake), None, sleep=sleep, polls=2)

    assert consumer.seeks == [("p0", 10)]  # back to the batch's first offset
    assert slept == [windows_writer.RETRY_INITIAL_SECONDS]
    assert consumer.commits == 1  # only after the second, successful write


def test_misshapen_results_are_skipped_not_retried():
    fake = FakeRedis()
    consumer = FakeConsumer([{"p0": [Message(b"{oops", 3), Message(raw(), 4)]}])

    windows_writer.run(consumer, Writer(fake), None, polls=1)

    assert len(fake.pipes[0].calls) == 1
    assert consumer.commits == 1


def test_a_window_hint_round_trips_and_misshapen_ones_are_dropped():
    hint = notifications.encode_window(START, END)

    assert notifications.parse_window(hint) == {
        "type": "window",
        "window_start": json.loads(hint)["window_start"],
        "window_end": json.loads(hint)["window_end"],
    }
    assert notifications.parse_window('{"window_start": 1}') is None
    assert notifications.parse_window("x" * 5000) is None
    assert (
        notifications.parse_window('{"window_start": "now", "window_end": "later"}')
        is None
    )
