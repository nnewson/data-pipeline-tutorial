"""Print the latest windowed counts the Flink job has written.

Each record is one page's views in one ten-second window of *event* time —
when the views happened, not when they were processed. A window is written once
the watermark passes its end, so the newest results trail the present by a
window and the watermark delay, and further when a partition is quiet.

At-least-once at the edge: after a restore, results since the last checkpoint
are written again, so a window can appear more than once. The latest is shown.
"""

import json
import logging
import time
from dataclasses import dataclass

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

from pipeline import flink_cluster
from pipeline.config import KAFKA_SERVER, KAFKA_WINDOWS_TOPIC

logger = logging.getLogger("windows")

SHOW_WINDOWS = 5
READ_DEADLINE_SECONDS = 15
# Connecting is bounded by this, and so is the partition lookup — the one call
# kafka-python gives no timeout of its own — through the request timeout, which
# it can take twice: 10.1s, measured, for a broker that stopped answering just
# after the connection was made. Both together fit inside the deadline, which
# bounds everything after them; closing gets this allowance on top.
STEP_TIMEOUT_MS = 5000


@dataclass
class Snapshot:
    """What was read, and whether it reached the end the read began with."""

    records: list[dict]
    expected: int
    complete: bool

    @property
    def repeated(self) -> int:
        keys = [(r["window_start"], r["page"]) for r in self.records]
        return len(keys) - len(set(keys))


def latest(records: list[dict], windows: int = SHOW_WINDOWS) -> list[dict]:
    """The most recent `windows` windows, one result per page and window."""
    by_key = {(r["window_start"], r["page"]): r for r in records}
    starts = sorted({start for start, _ in by_key})[-windows:]
    return sorted(
        (r for (start, _), r in by_key.items() if start in starts),
        key=lambda r: (r["window_start"], r["page"]),
    )


def read_all(
    topic: str = KAFKA_WINDOWS_TOPIC, clock=time.monotonic, consumer=None
) -> Snapshot:
    """Every result up to the topic's end *as it was when the read began*.

    A fixed endpoint, not silence: a consumer timeout restarts on every record,
    so with results still arriving a read that waits for quiet need never end —
    the deadline lesson from 0.9, again. If the endpoint is not reached before
    the deadline, the snapshot says so rather than passing for the whole topic.

    A budget, not a hard guarantee: the deadline starts before connecting;
    connecting and the partition lookup have fixed bounds that fit inside it;
    every later call gets what is left; closing is bounded separately, on top.
    A setup step that times out raises: there is no partial result to report.
    """
    deadline = clock() + READ_DEADLINE_SECONDS

    def left_ms() -> int:
        return max(0, int((deadline - clock()) * 1000))

    consumer = consumer or KafkaConsumer(
        bootstrap_servers=KAFKA_SERVER,
        enable_auto_commit=False,
        bootstrap_timeout_ms=STEP_TIMEOUT_MS,
        request_timeout_ms=STEP_TIMEOUT_MS,
    )
    try:
        partitions = [
            TopicPartition(topic, p)
            for p in sorted(consumer.partitions_for_topic(topic) or ())
        ]
        if not partitions:
            return Snapshot([], 0, True)
        consumer.assign(partitions)
        starts = consumer.beginning_offsets(partitions, timeout_ms=left_ms())
        ends = consumer.end_offsets(partitions, timeout_ms=left_ms())
        for tp in partitions:
            consumer.seek(tp, starts[tp])
        expected = sum(ends[tp] - starts[tp] for tp in partitions)
        records: list[dict] = []
        remaining = {tp for tp in partitions if starts[tp] < ends[tp]}
        while remaining and clock() < deadline:
            polled = consumer.poll(timeout_ms=min(500, left_ms()))
            for tp, messages in polled.items():
                records += [
                    json.loads(m.value) for m in messages if m.offset < ends[tp]
                ]
            remaining = {
                tp
                for tp in remaining
                if (consumer.position(tp, timeout_ms=left_ms()) or 0) < ends[tp]
            }
        # A setup step that overran the deadline never enters the loop, so
        # whatever it should have read is still outstanding here.
        return Snapshot(records, expected, complete=not remaining)
    finally:
        consumer.close(timeout_ms=STEP_TIMEOUT_MS)


def running_job() -> str | None:
    try:
        jobs = flink_cluster.request("/jobs/overview")["jobs"]
    except flink_cluster.FlinkError:
        return None
    running = [
        j for j in jobs if j["state"] == "RUNNING" and j["name"] == "pageview-windows"
    ]
    return running[0]["jid"] if running else None


def main() -> int:
    try:
        snapshot = read_all()
    except KafkaError as error:
        print(f"could not read {KAFKA_WINDOWS_TOPIC}: {type(error).__name__}: {error}")
        return 1
    if not snapshot.complete:
        print(
            f"  incomplete read: {len(snapshot.records)} of {snapshot.expected} "
            f"results within {READ_DEADLINE_SECONDS}s — what follows is not the "
            "whole topic"
        )
    if not snapshot.records:
        print(f"no windows on {KAFKA_WINDOWS_TOPIC} yet")
        return 0
    for record in latest(snapshot.records):
        print(f"  {record['window_start']}  {record['page']:<12} {record['views']}")
    print(
        f"\n  {len(snapshot.records)} results; "
        f"{snapshot.repeated} repeated an earlier one"
    )
    job = running_job()
    if job:
        dropped = flink_cluster.metric_sum(job, "numLateRecordsDropped")
        # None is "could not be read", never zero.
        shown = "unavailable" if dropped is None else str(dropped)
        print(f"  late records dropped by the running job: {shown}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
