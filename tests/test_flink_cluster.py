"""Submission and the REST API, without a cluster."""

import io
import json
import urllib.error

import pytest

from pipeline import flink_cluster
from pipeline.flink_job import Settings


def test_the_submission_runs_inside_the_jobmanager_with_its_settings():
    command = flink_cluster.submit_command(
        Settings(source_topic="smoke_in", idle_timeout_seconds=0)
    )

    assert command[:4] == ["docker", "compose", "exec", "-T"]
    assert "FLINK_SOURCE_TOPIC=smoke_in" in command
    assert "FLINK_IDLE_TIMEOUT_SECONDS=0" in command
    assert command[-6:] == [
        "flink-jobmanager",
        "flink",
        "run",
        "-d",
        "-py",
        flink_cluster.JOB_PATH,
    ]


def test_the_job_id_is_read_from_the_submission_output():
    output = (
        "noise\nJob has been submitted with JobID 71c1e79b68529e1b59d9ea5954314972\n"
    )

    assert flink_cluster.parse_job_id(output) == "71c1e79b68529e1b59d9ea5954314972"


def test_a_submission_without_a_job_id_is_an_error():
    with pytest.raises(flink_cluster.FlinkError, match="did not report a job id"):
        flink_cluster.parse_job_id(
            "Traceback ... ModuleNotFoundError: No module named 'ruamel'"
        )


class FakeREST:
    def __init__(self, routes):
        self.routes, self.calls = routes, []

    def __call__(self, request, timeout=None):
        self.calls.append((request.get_method(), request.full_url))
        path = request.full_url.removeprefix("http://localhost:8081")
        if path not in self.routes:
            raise urllib.error.URLError("no route")
        return io.BytesIO(json.dumps(self.routes[path]).encode())


def _io(fake):
    class Response(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def call(request, timeout=None):
        return Response(fake(request, timeout).read())

    return call


@pytest.fixture
def cluster(monkeypatch):
    def install(routes):
        fake = FakeREST(routes)
        monkeypatch.setattr(flink_cluster.urllib.request, "urlopen", _io(fake))
        return fake

    return install


JOB = "a" * 32
VERTICES = {
    "vertices": [
        {"id": "v1", "name": "Source -> LocalWindowAggregate"},
        {"id": "v2", "name": "GlobalWindowAggregate -> Writer"},
    ]
}


@pytest.mark.allow_connect
def test_the_late_drop_count_is_summed_wherever_it_is_reported(cluster):
    """The planner splits the aggregation; the global phase reports late drops."""
    base = f"/jobs/{JOB}/vertices"
    cluster(
        {
            f"/jobs/{JOB}": VERTICES,
            f"{base}/v1/subtasks/metrics": [
                {"id": "LocalWindowAggregate[3].numRecordsIn"}
            ],
            f"{base}/v2/subtasks/metrics": [
                {"id": "GlobalWindowAggregate[5].numLateRecordsDropped"}
            ],
            f"{base}/v2/subtasks/metrics?get=GlobalWindowAggregate[5].numLateRecordsDropped&agg=sum": [
                {"id": "GlobalWindowAggregate[5].numLateRecordsDropped", "sum": 3.0}
            ],
        }
    )

    assert flink_cluster.metric_sum(JOB, "numLateRecordsDropped") == 3


@pytest.mark.allow_connect
def test_a_metric_nobody_reports_is_unavailable_not_zero(cluster):
    base = f"/jobs/{JOB}/vertices"
    cluster(
        {
            f"/jobs/{JOB}": VERTICES,
            f"{base}/v1/subtasks/metrics": [],
            f"{base}/v2/subtasks/metrics": [],
        }
    )

    assert flink_cluster.metric_sum(JOB, "numLateRecordsDropped") is None


@pytest.mark.allow_connect
def test_an_unreachable_cluster_is_unavailable_not_zero(cluster):
    cluster({})

    assert flink_cluster.metric_sum(JOB, "numLateRecordsDropped") is None


@pytest.mark.allow_connect
def test_cancel_targets_exactly_one_job_by_id(cluster):
    fake = cluster({f"/jobs/{JOB}?mode=cancel": {}})

    flink_cluster.cancel(JOB)

    assert fake.calls == [("PATCH", f"http://localhost:8081/jobs/{JOB}?mode=cancel")]


@pytest.mark.allow_connect
def test_an_unreachable_cluster_raises_a_flink_error(cluster):
    cluster({})

    with pytest.raises(flink_cluster.FlinkError):
        flink_cluster.job_state(JOB)


@pytest.mark.allow_connect
def test_a_listed_metric_whose_value_vanishes_is_unavailable_not_zero(cluster):
    """Name discovery, then an empty values response: not a count of nothing."""
    base = f"/jobs/{JOB}/vertices"
    name = "GlobalWindowAggregate[5].numLateRecordsDropped"
    cluster(
        {
            f"/jobs/{JOB}": VERTICES,
            f"{base}/v1/subtasks/metrics": [],
            f"{base}/v2/subtasks/metrics": [{"id": name}],
            f"{base}/v2/subtasks/metrics?get={name}&agg=sum": [],
        }
    )

    assert flink_cluster.metric_sum(JOB, "numLateRecordsDropped") is None


def _submitting(monkeypatch, script: str, timeout: float = 5) -> None:
    """Run `script` in place of the real submission: a real process, briefly."""
    monkeypatch.setattr(
        flink_cluster, "submit_command", lambda _s: ["sh", "-c", script]
    )
    monkeypatch.setattr(flink_cluster, "SUBMIT_TIMEOUT_SECONDS", timeout)


PRINTS_ID = f"printf 'Job has been submitted with JobID {'c' * 32}\\n'"


@pytest.mark.allow_connect
def test_a_timed_out_submission_keeps_a_job_id_it_already_printed(monkeypatch):
    _submitting(monkeypatch, f"{PRINTS_ID}; exec sleep 30", timeout=0.5)

    assert flink_cluster.submit(Settings()) == "c" * 32


@pytest.mark.allow_connect
def test_a_timed_out_submission_without_an_id_is_uncertain(monkeypatch):
    """A timeout does not mean Flink rejected the job."""
    _submitting(monkeypatch, "exec sleep 30", timeout=0.5)

    with pytest.raises(flink_cluster.SubmissionUncertain, match="timed out"):
        flink_cluster.submit(Settings())


@pytest.mark.allow_connect
def test_output_without_an_id_is_uncertain_too(monkeypatch):
    _submitting(monkeypatch, "echo boom >&2; exit 1")

    with pytest.raises(flink_cluster.SubmissionUncertain):
        flink_cluster.submit(Settings())


@pytest.mark.allow_connect
def test_undecodable_output_still_yields_the_job_id(monkeypatch):
    """Regression: one stray byte raised after the job had been submitted."""
    _submitting(
        monkeypatch,
        f"printf 'Job has been submitted with JobID {'c' * 32} \\377\\n'",
    )

    assert flink_cluster.submit(Settings()) == "c" * 32


@pytest.mark.allow_connect
def test_a_terminated_client_is_an_uncertain_submission(monkeypatch):
    """The keeper terminates the client at shutdown: the job may still land."""
    _submitting(monkeypatch, "exec sleep 30")

    with pytest.raises(flink_cluster.SubmissionUncertain, match="signal"):
        flink_cluster.submit(Settings(), started=lambda process: process.terminate())


@pytest.mark.allow_connect
def test_a_command_that_never_ran_is_a_plain_error(monkeypatch):
    """Nothing was submitted, so nothing needs reconciling."""
    monkeypatch.setattr(
        flink_cluster, "submit_command", lambda _s: ["/nonexistent/flink-client"]
    )

    with pytest.raises(flink_cluster.FlinkError) as raised:
        flink_cluster.submit(Settings())
    assert not isinstance(raised.value, flink_cluster.SubmissionUncertain)


def test_the_generation_reaches_the_job_through_the_environment():
    command = flink_cluster.submit_command(Settings(generation=7))

    assert "FLINK_GENERATION=7" in command


class NotFoundREST:
    """`/jobs/:id` answers 404 for a job Flink does not know."""

    def __call__(self, request, timeout=None):
        raise urllib.error.HTTPError(request.full_url, 404, "Not Found", {}, None)


@pytest.mark.allow_connect
def test_a_404_is_a_confirmed_absence_not_merely_unavailable(monkeypatch):
    monkeypatch.setattr(flink_cluster.urllib.request, "urlopen", NotFoundREST())

    with pytest.raises(flink_cluster.NotFound):
        flink_cluster.job_state(JOB)


@pytest.mark.allow_connect
def test_a_server_error_is_unknown_not_absent(monkeypatch):
    def server_error(request, timeout=None):
        raise urllib.error.HTTPError(request.full_url, 500, "Internal", {}, None)

    monkeypatch.setattr(flink_cluster.urllib.request, "urlopen", server_error)

    with pytest.raises(flink_cluster.FlinkError) as raised:
        flink_cluster.job_state(JOB)
    assert not isinstance(raised.value, flink_cluster.NotFound)


@pytest.mark.allow_connect
def test_reconciliation_finds_this_runs_jobs_stopped_ones_included(cluster):
    """A job that already failed still shows the submission landed."""
    cluster(
        {
            "/jobs/overview": {
                "jobs": [
                    {"jid": "1" * 32, "name": "smoke-flink-abc", "state": "RUNNING"},
                    {"jid": "2" * 32, "name": "smoke-flink-abc", "state": "CANCELED"},
                    {"jid": "3" * 32, "name": "pageview-windows", "state": "RUNNING"},
                ]
            }
        }
    )

    assert flink_cluster.jobs_named("smoke-flink-abc") == ["1" * 32, "2" * 32]


@pytest.mark.allow_connect
def test_a_partial_sum_is_unavailable_not_a_smaller_count(cluster):
    """One vertex reports a value; another's vanishes. Not the total, so None."""
    base = f"/jobs/{JOB}/vertices"
    name = "WindowAggregate.numLateRecordsDropped"
    cluster(
        {
            f"/jobs/{JOB}": VERTICES,
            f"{base}/v1/subtasks/metrics": [{"id": name}],
            f"{base}/v1/subtasks/metrics?get={name}&agg=sum": [
                {"id": name, "sum": 3.0}
            ],
            f"{base}/v2/subtasks/metrics": [{"id": name}],
            f"{base}/v2/subtasks/metrics?get={name}&agg=sum": [],
        }
    )

    assert flink_cluster.metric_sum(JOB, "numLateRecordsDropped") is None


@pytest.mark.allow_connect
def test_a_value_missing_within_one_vertex_is_unavailable_too(cluster):
    """Two names asked of one vertex, one value back: not that value alone."""
    base = f"/jobs/{JOB}/vertices"
    first = "GlobalWindowAggregate[5].0.numLateRecordsDropped"
    second = "GlobalWindowAggregate[5].1.numLateRecordsDropped"
    cluster(
        {
            f"/jobs/{JOB}": VERTICES,
            f"{base}/v1/subtasks/metrics": [],
            f"{base}/v2/subtasks/metrics": [{"id": first}, {"id": second}],
            f"{base}/v2/subtasks/metrics?get={first},{second}&agg=sum": [
                {"id": first, "sum": 3.0}
            ],
        }
    )

    assert flink_cluster.metric_sum(JOB, "numLateRecordsDropped") is None
