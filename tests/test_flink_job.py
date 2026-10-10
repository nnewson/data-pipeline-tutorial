"""The job's statements, built and checked on the host without PyFlink."""

import subprocess
import sys

import pytest

from pipeline import flink_job
from pipeline.flink_job import Settings


def test_the_module_imports_without_pyflink():
    """The boundary that keeps Java and PyFlink off the host, tested."""
    blocker = (
        "import sys\n"
        "class Block:\n"
        "    def find_spec(self, name, path=None, target=None):\n"
        "        if name.split('.')[0] == 'pyflink':\n"
        "            raise ImportError('pyflink is not on the host')\n"
        "sys.meta_path.insert(0, Block())\n"
        "import pipeline.flink_job as job\n"
        "print(len(job.statements(job.Settings())))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", blocker], capture_output=True, text=True
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "4"


@pytest.mark.parametrize(
    "field, value",
    [
        ("source_topic", "pageviews'; DROP TABLE x; --"),
        ("sink_topic", "has space"),
        ("group", ""),
        ("job_name", "a" * 201),
        ("bootstrap", "kafka:29092'"),
        ("bootstrap", "kafka"),
    ],
)
def test_names_are_validated_before_interpolation(field, value):
    with pytest.raises(ValueError):
        Settings(**{field: value})


@pytest.mark.parametrize(
    "field, value",
    [
        ("window_seconds", 0),
        ("watermark_delay_seconds", 0),
        ("checkpoint_seconds", 0),
        ("idle_timeout_seconds", -1),
    ],
)
def test_durations_are_validated(field, value):
    with pytest.raises(ValueError):
        Settings(**{field: value})


def test_literals_are_escaped_as_a_second_line_of_defence():
    assert flink_job.literal("it's") == "'it''s'"


def test_settings_come_from_the_environment_with_defaults():
    settings = Settings.from_environment(
        {
            "FLINK_SOURCE_TOPIC": "smoke_in",
            "FLINK_IDLE_TIMEOUT_SECONDS": "0",
            "FLINK_GROUP": "",
        }
    )

    assert settings.source_topic == "smoke_in"
    assert settings.idle_timeout_seconds == 0
    assert settings.group == Settings().group, "an empty value means the default"
    assert settings.bootstrap == "kafka:29092", "the internal listener, not localhost"


def test_checkpoints_are_exactly_once_inside_and_the_sink_at_least_once_outside():
    """Two settings, kept apart: consistent counts, possibly repeated records."""
    settings = Settings()
    config = flink_job.configuration(settings)

    assert config["execution.checkpointing.mode"] == "EXACTLY_ONCE"
    assert config["execution.checkpointing.interval"] == "10 s"
    assert "'sink.delivery-guarantee' = 'at-least-once'" in flink_job.sink_ddl(settings)


def test_idleness_can_be_disabled():
    """The smoke job does, so closure never depends on a processing-time timeout."""
    assert "table.exec.source.idle-timeout" in flink_job.configuration(Settings())
    assert "table.exec.source.idle-timeout" not in flink_job.configuration(
        Settings(idle_timeout_seconds=0)
    )


def test_event_time_can_never_be_null_or_implausibly_far_ahead():
    ddl = flink_job.source_ddl(Settings())

    assert f"WHEN `timestamp` >= {flink_job.EARLIEST}" in ddl
    # A fixed upper bound (2100) stopped overflow but not poisoning: 2099 is
    # inside it. Measured, 2099 on every partition dropped twelve valid records.
    skew = f"`timestamp` <= UNIX_TIMESTAMP() + {flink_job.FUTURE_SKEW_SECONDS}"
    assert skew in ddl
    assert "ELSE 0" in ddl, (
        "rejected maps to the epoch, which cannot move the watermark"
    )
    assert "event_time - INTERVAL '2' SECOND" in ddl


def test_invalid_rows_are_excluded_before_aggregation():
    view = flink_job.view_ddl()

    assert "page IS NOT NULL" in view and "page <> ''" in view
    # One decision, made by the computed column; the filter only reads it.
    assert "event_time > TO_TIMESTAMP_LTZ(0, 3)" in view
    assert "UNIX_TIMESTAMP" not in view, "re-reading processing time could decide twice"
    assert (
        "FROM TABLE(\n    TUMBLE(\n        TABLE valid_pageviews"
        in flink_job.insert_dml(Settings())
    )


def test_the_window_counts_by_page_in_event_time():
    dml = flink_job.insert_dml(Settings(window_seconds=10))

    assert "DESCRIPTOR(event_time)" in dml
    assert "INTERVAL '10' SECOND" in dml
    assert "GROUP BY window_start, window_end, page" in dml
    assert "COUNT(*) AS `views`" in dml, "VIEWS is reserved in Flink SQL"


def test_every_submission_replays_from_the_start_and_reads_kafka_internally():
    """Resuming from committed offsets would emit open windows short."""
    ddl = flink_job.source_ddl(Settings(source_topic="pageviews"))

    assert "'scan.startup.mode' = 'earliest-offset'" in ddl
    assert "group-offsets" not in ddl
    assert "'properties.bootstrap.servers' = 'kafka:29092'" in ddl
    assert "'topic' = 'pageviews'" in ddl


def test_results_are_keyed_by_page():
    assert "'key.fields' = 'page'" in flink_job.sink_ddl(Settings())
