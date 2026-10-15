"""The keeper's view of the Flink job, and cancelling one job by id.

    uv run flink-job status      the keeper's record, and Flink's view of each attempt
    uv run flink-job cancel ID   cancel exactly that job

There is no `submit` since 0.11: the elected coordinator's keeper owns
submission, and a job started by hand would run beside the keeper's and write
the same windows. Cancelling is now a demonstration — the keeper notices, and
puts the next generation in its place.

By id, never by name: a name can match a job someone else submitted.
"""

import argparse
import logging

from pipeline import coordination, flink_cluster, job_keeper
from pipeline.config import FLINK_JOB_BASE

logger = logging.getLogger("flink-job")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="The keeper's Flink job.")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("status", help="the keeper's record and Flink's view of it")
    cancel = sub.add_parser("cancel", help="cancel one job by id")
    cancel.add_argument("job_id")
    return parser.parse_args(argv)


def describe(job_id: str) -> str:
    """One job's state as Flink reports it — or why it cannot say."""
    try:
        state = flink_cluster.job_state(job_id)
    except flink_cluster.NotFound:
        return "not found (ended before a jobmanager restart, or never existed)"
    except flink_cluster.FlinkError as error:
        return f"unavailable ({error})"
    if state != "RUNNING":
        return state
    try:
        checkpoints = flink_cluster.completed_checkpoints(job_id)
        restored = flink_cluster.restored_checkpoint(job_id)
    except (flink_cluster.FlinkError, KeyError):
        return state
    since = f", restored from checkpoint {restored}" if restored is not None else ""
    return f"RUNNING, {checkpoints} checkpoint(s) completed{since}"


def read_record() -> tuple[dict, int]:
    client = coordination.connect(retries=3)
    try:
        return coordination.read_flink_record(client, coordination.Paths())
    finally:
        client.stop()
        client.close()


def status() -> int:
    try:
        raw, version = read_record()
    except Exception as error:  # noqa: BLE001 - reported, not raised
        print(f"ZooKeeper: could not read the keeper's record: {error}")
        return 1
    record = job_keeper.Record.parse(raw)
    print(
        f"record (version {version}): generation {record.generation}, "
        f"written under epoch {record.epoch}, {record.failures} consecutive "
        "resubmission(s)"
    )
    if not record.attempts:
        print("  no attempts: nothing submitted, or every one accounted for")
    for generation, attempt in sorted(record.attempts.items()):
        name = job_keeper.job_name(FLINK_JOB_BASE, generation)
        role = "current" if generation == record.generation else "superseded"
        if attempt.job_id is None:
            print(f"  g{generation} {name} ({role}): no id yet — still owed")
        else:
            print(f"  g{generation} {name} ({role}): {attempt.job_id}")
            print(f"      {describe(attempt.job_id)}")
    try:
        recorded = {a.job_id for a in record.attempts.values()}
        for job in flink_cluster.jobs_overview():
            generation = job_keeper.generation_of(FLINK_JOB_BASE, job["name"])
            if generation is not None and job["id"] not in recorded:
                print(f"  unrecorded: {job['name']} {job['id']} {job['state']}")
    except flink_cluster.FlinkError as error:
        print(f"  Flink's listing unavailable: {error}")
    print("  dashboard: http://localhost:8081")
    return 0


def main(argv: list[str] | None = None) -> int:
    arguments = parse_args(argv)
    try:
        if arguments.command == "status":
            return status()
        flink_cluster.cancel(arguments.job_id)
        print(
            f"cancelling {arguments.job_id}; the keeper will submit the next generation"
        )
    except flink_cluster.FlinkError as error:
        print(f"Flink: {error}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
