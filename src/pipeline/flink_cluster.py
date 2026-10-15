"""Talking to the Flink cluster from the host: submit, observe, cancel.

Submission runs inside the jobmanager container, because a Python job's graph
is built by running its driver, and the driver, PyFlink and the internal Kafka
address are all in there. Everything else goes through the REST API on
localhost:8081 — the same API the dashboard uses.
"""

import json
import logging
import re
import subprocess
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import asdict

from pipeline.config import FLINK_REST_URL
from pipeline.flink_job import Settings

logger = logging.getLogger("flink")

JOB_PATH = "/opt/pipeline/flink_job.py"
SUBMITTED = re.compile(r"Job has been submitted with JobID ([0-9a-f]{32})")
SUBMIT_TIMEOUT_SECONDS = 120
REST_TIMEOUT_SECONDS = 10

ENVIRONMENT = {
    "bootstrap": "KAFKA_SERVER",
    "source_topic": "FLINK_SOURCE_TOPIC",
    "sink_topic": "FLINK_SINK_TOPIC",
    "group": "FLINK_GROUP",
    "job_name": "FLINK_JOB_NAME",
    "window_seconds": "FLINK_WINDOW_SECONDS",
    "watermark_delay_seconds": "FLINK_WATERMARK_DELAY_SECONDS",
    "idle_timeout_seconds": "FLINK_IDLE_TIMEOUT_SECONDS",
    "checkpoint_seconds": "FLINK_CHECKPOINT_SECONDS",
    "generation": "FLINK_GENERATION",
}


class FlinkError(Exception):
    """The cluster refused, or could not be reached."""


class NotFound(FlinkError):
    """Flink answered 404: for a job, no runner and no archived copy.

    A positive answer, unlike every other FlinkError, which means only that the
    question could not be answered. Asked of `/jobs/:id`, never of
    `/jobs/:id/status`: that endpoint reports a job it does not know as a 500,
    indistinguishable from a server error (measured on 2.2.1).
    """


class SubmissionUncertain(FlinkError):
    """The submission's outcome is unknown: a job may or may not be running.

    A timeout does not mean Flink rejected anything. Whoever submitted must
    reconcile — find the job by the run's unique name, cancel it by id — before
    removing anything it reads from.
    """


TERMINAL = {"CANCELED", "FAILED", "FINISHED"}


def submit_command(settings: Settings) -> list[str]:
    """`flink run -d -py` inside the jobmanager, with the settings as environment."""
    command = ["docker", "compose", "exec", "-T"]
    for field, variable in ENVIRONMENT.items():
        command += ["-e", f"{variable}={asdict(settings)[field]}"]
    return command + ["flink-jobmanager", "flink", "run", "-d", "-py", JOB_PATH]


def parse_job_id(output: str) -> str:
    match = SUBMITTED.search(output)
    if match is None:
        raise FlinkError(f"submission did not report a job id:\n{output[-2000:]}")
    return match.group(1)


def _text(value: str | bytes | None) -> str:
    if isinstance(value, bytes):
        return value.decode(errors="replace")
    return value or ""


def submit(
    settings: Settings, started: Callable[[subprocess.Popen], None] | None = None
) -> str:
    """Submit a job, returning its id — the only safe handle for cancelling it.

    Raises SubmissionUncertain whenever a job might exist without its id being
    known: a timeout, a terminated client, or output with no id in it. Whatever
    the command had already printed is kept, and an id found there is returned.
    A plain FlinkError means the command never ran, so nothing was submitted.

    `started` receives the client process, so an owner can terminate it — the
    keeper does at shutdown. Terminating the client need not stop the `flink
    run` inside the container, which is why that is an uncertain outcome too.
    """
    try:
        process = subprocess.Popen(
            submit_command(settings),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            # One undecodable byte must not turn a job that was submitted into
            # an exception raised after the fact; the id is ASCII either way.
            errors="replace",
        )
    except OSError as error:
        # The command never ran, so nothing can have been submitted.
        raise FlinkError(f"could not run the submission: {error}") from error
    if started is not None:
        started(process)
    try:
        stdout, stderr = process.communicate(timeout=SUBMIT_TIMEOUT_SECONDS)
        problem = None
    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate()
        problem = f"submission timed out after {SUBMIT_TIMEOUT_SECONDS}s"
    if problem is None and process.returncode is not None and process.returncode < 0:
        problem = f"submission client ended by signal {-process.returncode}"
    output = _text(stdout) + _text(stderr)
    try:
        job_id = parse_job_id(output)
    except FlinkError as error:
        raise SubmissionUncertain(
            f"{problem} without a job id" if problem else str(error)
        ) from error
    if problem:
        logger.warning(f"{problem} after reporting job {job_id}")
    return job_id


def jobs_named(name: str) -> list[str]:
    """Ids of jobs with exactly this name, stopped ones included.

    For reconciling a run's own uniquely named job after an uncertain
    submission — never for finding a reader's job, whose name is not unique.
    Stopped jobs count: one that already failed still shows the submission
    landed, and including them means each listing covers every earlier one.
    """
    jobs = request("/jobs/overview")["jobs"]
    return [j["jid"] for j in jobs if j["name"] == name]


def jobs_overview() -> list[dict]:
    """Every job the cluster lists: id, name and state.

    For discovery only. A *successful* overview can omit a live job: the
    dispatcher asks each job's runner for its details and drops any that fail to
    answer (2.2.1's `queryJobMastersForInformation`). A decision about one job
    asks about that job, with job_state().
    """
    return [
        {"id": j["jid"], "name": j["name"], "state": j["state"]}
        for j in request("/jobs/overview")["jobs"]
    ]


def request(path: str, method: str = "GET", base: str = FLINK_REST_URL) -> object:
    try:
        with urllib.request.urlopen(  # noqa: S310 - a configured localhost URL
            urllib.request.Request(f"{base}{path}", method=method),
            timeout=REST_TIMEOUT_SECONDS,
        ) as response:
            body = response.read()
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as error:
        if error.code == 404:
            raise NotFound(f"{method} {path}: {error}") from error
        raise FlinkError(f"{method} {path}: {error}") from error
    except (urllib.error.URLError, OSError, ValueError) as error:
        raise FlinkError(f"{method} {path}: {error}") from error


def job_state(job_id: str) -> str:
    """The job's state, asked of the job itself. NotFound is a confirmed absence."""
    return request(f"/jobs/{job_id}")["state"]


def completed_checkpoints(job_id: str) -> int:
    return request(f"/jobs/{job_id}/checkpoints")["counts"]["completed"]


def restored_checkpoint(job_id: str) -> int | None:
    """The id of the checkpoint the job last restored from, if it ever did."""
    restored = request(f"/jobs/{job_id}/checkpoints")["latest"].get("restored")
    return restored["id"] if restored else None


def cancel(job_id: str) -> None:
    """Cancel exactly this job. Never by name: a name can match a reader's job."""
    request(f"/jobs/{job_id}?mode=cancel", method="PATCH")


def metric_sum(job_id: str, metric_suffix: str) -> int | None:
    """A metric summed over every subtask that reports it — or None if none does.

    Searched across all vertices rather than by vertex name: the planner splits
    a window aggregation into a local phase chained onto the source and a global
    phase downstream, and `numLateRecordsDropped` is reported by the global one.

    None is "measurement unavailable", never zero. A count needs every value
    it asked for: a metric whose name is listed but whose value does not come
    back — it can vanish between the two requests — makes the total
    unavailable, not smaller. And the REST API serves metrics from a cache, so
    even a value can be seconds old.
    """
    try:
        total, values_seen = 0.0, False
        for vertex in request(f"/jobs/{job_id}")["vertices"]:
            path = f"/jobs/{job_id}/vertices/{vertex['id']}/subtasks/metrics"
            names = [m["id"] for m in request(path) if m["id"].endswith(metric_suffix)]
            if not names:
                continue
            values = {
                m["id"]: m["sum"]
                for m in request(f"{path}?get={','.join(names)}&agg=sum")
                if "sum" in m
            }
            if any(name not in values for name in names):
                return None
            for name in names:
                total += float(values[name])
                values_seen = True
        return int(total) if values_seen else None
    except (FlinkError, KeyError, TypeError, ValueError):
        return None
