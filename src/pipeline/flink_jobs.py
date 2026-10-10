"""Submit, list and cancel the Flink job.

    uv run flink-job submit      submit the windowed counts job
    uv run flink-job status      every job the cluster knows, and its state
    uv run flink-job cancel ID   cancel exactly that job

By id, never by name: a name can match a job someone else submitted. Nothing
resubmits a job the cluster loses — a jobmanager restart in this session
cluster, without high availability, loses it. That is 0.11's submitter.
"""

import argparse
import logging

from pipeline import flink_cluster
from pipeline.flink_job import Settings

logger = logging.getLogger("flink-job")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage the Flink job.")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("submit", help="submit the windowed counts job")
    sub.add_parser("status", help="list the cluster's jobs")
    cancel = sub.add_parser("cancel", help="cancel one job by id")
    cancel.add_argument("job_id")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    arguments = parse_args(argv)
    try:
        if arguments.command == "submit":
            job_id = flink_cluster.submit(Settings())
            print(f"submitted {job_id}")
            print("  dashboard: http://localhost:8081")
        elif arguments.command == "status":
            jobs = flink_cluster.request("/jobs/overview")["jobs"]
            if not jobs:
                print("no jobs")
            for job in sorted(jobs, key=lambda j: j["start-time"]):
                print(f"  {job['jid']}  {job['state']:<9}  {job['name']}")
        else:
            flink_cluster.cancel(arguments.job_id)
            print(f"cancelling {arguments.job_id}")
    except flink_cluster.FlinkError as error:
        print(f"Flink: {error}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
