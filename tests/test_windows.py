"""The windows CLI: latest results, repeats counted, metrics honestly."""

import json
from types import SimpleNamespace

from kafka.errors import KafkaTimeoutError

from pipeline import windows


def result(start, page, views):
    return {"window_start": start, "window_end": "", "page": page, "views": views}


def test_latest_keeps_the_newest_windows_and_one_result_each():
    records = [
        result("t1", "/docs", 1),
        result("t2", "/docs", 2),
        result("t2", "/docs", 2),  # repeated after a restore
        result("t3", "/", 5),
    ]

    shown = windows.latest(records, windows=2)

    assert [(r["window_start"], r["page"], r["views"]) for r in shown] == [
        ("t2", "/docs", 2),
        ("t3", "/", 5),
    ]


def test_an_unreadable_metric_is_shown_as_unavailable(monkeypatch, capsys):
    monkeypatch.setattr(
        windows,
        "read_all",
        lambda: windows.Snapshot([result("t1", "/docs", 1)], 1, True),
    )
    monkeypatch.setattr(windows, "running_job", lambda: "j" * 32)
    monkeypatch.setattr(windows.flink_cluster, "metric_sum", lambda job, name: None)

    windows.main()

    assert (
        "late records dropped by the running job: unavailable"
        in capsys.readouterr().out
    )


class Clock:
    def __init__(self, step=0.0):
        self.now, self.step = 0.0, step

    def __call__(self):
        self.now += self.step
        return self.now


class FakeTopic:
    """Two partitions; results keep arriving after the read begins.

    `slow` makes a named call take that many seconds on the fake clock, and
    every timeout a call is given is recorded.
    """

    def __init__(self, ends, arrive_forever=False, stall=False, clock=None, slow=None):
        self.ends = ends
        self.positions = {}
        self.arrive_forever = arrive_forever
        self.stall = stall
        self.clock = clock
        self.slow = slow or {}
        self.timeouts: list[tuple[str, int]] = []
        self.closed = False

    def _call(self, name, timeout_ms):
        self.timeouts.append((name, timeout_ms))
        if name in self.slow:
            self.clock.now += self.slow[name]

    def partitions_for_topic(self, topic):
        return {0, 1}

    def assign(self, partitions):
        self.partitions = partitions

    def beginning_offsets(self, partitions, timeout_ms=None):
        self._call("beginning_offsets", timeout_ms)
        return {tp: 0 for tp in partitions}

    def end_offsets(self, partitions, timeout_ms=None):
        self._call("end_offsets", timeout_ms)
        return {tp: self.ends[tp.partition] for tp in partitions}

    def seek(self, tp, offset):
        self.positions[tp] = offset

    def position(self, tp, timeout_ms=None):
        self._call("position", timeout_ms)
        return self.positions[tp]

    def poll(self, timeout_ms=0):
        self._call("poll", timeout_ms)
        if self.stall:
            return {}
        batch = {}
        for tp, offset in self.positions.items():
            if offset < self.ends[tp.partition] or self.arrive_forever:
                value = json.dumps(
                    result(f"t{offset}", f"/p{tp.partition}", 1)
                ).encode()
                batch[tp] = [SimpleNamespace(offset=offset, value=value)]
                self.positions[tp] = offset + 1
        return batch

    def close(self, timeout_ms=None):
        self._call("close", timeout_ms)
        self.closed = True


def test_the_read_stops_at_the_end_it_began_with_while_results_keep_arriving():
    """Regression: a read that waited for silence need never have ended."""
    topic = FakeTopic(ends={0: 3, 1: 2}, arrive_forever=True)

    snapshot = windows.read_all(consumer=topic)

    assert snapshot.complete
    assert snapshot.expected == 5
    assert len(snapshot.records) == 5, "nothing past the endpoint"
    assert topic.closed


def test_a_read_that_misses_its_deadline_says_so(monkeypatch, capsys):
    ticks = iter(range(0, 1000, 5))
    topic = FakeTopic(ends={0: 3, 1: 2}, stall=True)

    snapshot = windows.read_all(consumer=topic, clock=lambda: next(ticks))

    assert not snapshot.complete
    monkeypatch.setattr(windows, "read_all", lambda: snapshot)
    monkeypatch.setattr(windows, "running_job", lambda: None)
    windows.main()
    assert "incomplete read: 0 of 5 results" in capsys.readouterr().out


def test_a_slow_setup_step_counts_against_the_deadline():
    """Regression: the deadline began after setup, so a 20s lookup read as complete."""
    clock = Clock()
    topic = FakeTopic(ends={0: 3, 1: 2}, clock=clock, slow={"end_offsets": 20})

    snapshot = windows.read_all(consumer=topic, clock=clock)

    assert not snapshot.complete
    assert snapshot.records == []


def test_every_blocking_call_gets_what_is_left_of_the_deadline():
    clock = Clock(step=0.25)
    topic = FakeTopic(ends={0: 3, 1: 2}, stall=True, clock=clock)

    snapshot = windows.read_all(consumer=topic, clock=clock)

    assert not snapshot.complete
    budget = windows.READ_DEADLINE_SECONDS * 1000
    bounded = [(name, ms) for name, ms in topic.timeouts if name != "close"]
    assert {name for name, _ in bounded} >= {
        "beginning_offsets",
        "end_offsets",
        "poll",
        "position",
    }
    assert all(ms is not None and 0 <= ms <= budget for _, ms in bounded)
    assert bounded[-1][1] < bounded[0][1], "the budget shrinks as time passes"
    assert ("close", windows.STEP_TIMEOUT_MS) in topic.timeouts, "cleanup bounded"


def test_a_setup_step_that_times_out_is_reported_not_raised(monkeypatch, capsys):
    def timed_out():
        raise KafkaTimeoutError("Failed to get offsets by timestamps in 15000 ms")

    monkeypatch.setattr(windows, "read_all", timed_out)

    assert windows.main() == 1
    assert "could not read pageview_windows: KafkaTimeoutError" in (
        capsys.readouterr().out
    )
