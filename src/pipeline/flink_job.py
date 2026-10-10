"""The Flink job: views per page, in ten-second event-time tumbling windows.

It runs inside the jobmanager container, submitted with `flink run -py`, and
reaches Kafka at `kafka:29092` — the internal listener 0.2 built, used by a real
client for the first time.

The host never imports PyFlink. Every statement is built by the pure functions
below, which the host imports and tests; `pyflink` is imported only in main(),
which runs in the container. That keeps Java and a several-hundred-megabyte
wheel off every reader's machine.

Two settings that are easy to run together, and must not be:

- checkpointing is EXACTLY_ONCE *inside* the job: state and source offsets are
  restored together, so a restored count holds each event's contribution once;
- the sink is AT_LEAST_ONCE at the *edge*: results written after the last
  completed checkpoint may be written again after a restore.

Consistent counts, possibly repeated output records.
"""

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass

# Kafka's own rule for topic names: letters, digits, '.', '_' and '-'.
NAME = re.compile(r"^[A-Za-z0-9._-]{1,200}$")
BOOTSTRAP = re.compile(r"^[A-Za-z0-9.-]+:[0-9]{1,5}(,[A-Za-z0-9.-]+:[0-9]{1,5})*$")

# Which event times count. Anything else is mapped to the epoch — which can
# never advance the watermark — and excluded before counting:
#
# - before EARLIEST, or missing: a missing timestamp would otherwise be a null
#   row time, which Flink refuses outright;
# - more than FUTURE_SKEW_SECONDS ahead of the moment the job reads it. Our
#   producer stamps events with the time it creates them, so a timestamp far
#   ahead is not plausible — and one is enough to poison the watermark: measured,
#   a 2099 event on every partition made the next two windows late, and twelve
#   valid records were dropped. A fixed range (2000-2100, the first version)
#   stops overflow but not that: 2099 is inside it. The check reads processing
#   time, so a rejected future event can become acceptable on a replay run after
#   its time has passed.
EARLIEST = 946684800  # 2000-01-01T00:00:00Z
FUTURE_SKEW_SECONDS = 60

ISO = "'yyyy-MM-dd''T''HH:mm:ss''Z'''"


@dataclass(frozen=True)
class Settings:
    """What a submission chooses. Defaults are the reader's running job."""

    bootstrap: str = "kafka:29092"
    source_topic: str = "pageviews"
    sink_topic: str = "pageview_windows"
    group: str = "flink-pageview-windows"
    job_name: str = "pageview-windows"
    window_seconds: int = 10
    watermark_delay_seconds: int = 2
    # 0 disables idleness: the smoke test does, so window closure depends only
    # on the records it sends and never on a processing-time timeout.
    idle_timeout_seconds: int = 10
    checkpoint_seconds: int = 10

    def __post_init__(self) -> None:
        if not BOOTSTRAP.match(self.bootstrap):
            raise ValueError(f"unsafe bootstrap servers {self.bootstrap!r}")
        for label in ("source_topic", "sink_topic", "group", "job_name"):
            value = getattr(self, label)
            if not NAME.match(value):
                raise ValueError(f"unsafe {label} {value!r}: must match {NAME.pattern}")
        for label in (
            "window_seconds",
            "watermark_delay_seconds",
            "checkpoint_seconds",
        ):
            if getattr(self, label) < 1:
                raise ValueError(f"{label} must be at least 1")
        if self.idle_timeout_seconds < 0:
            raise ValueError("idle_timeout_seconds must not be negative")

    @classmethod
    def from_environment(cls, environ: Mapping[str, str] | None = None) -> "Settings":
        env: Mapping[str, str] = os.environ if environ is None else environ
        defaults = cls()
        text = {
            "bootstrap": "KAFKA_SERVER",
            "source_topic": "FLINK_SOURCE_TOPIC",
            "sink_topic": "FLINK_SINK_TOPIC",
            "group": "FLINK_GROUP",
            "job_name": "FLINK_JOB_NAME",
        }
        numbers = {
            "window_seconds": "FLINK_WINDOW_SECONDS",
            "watermark_delay_seconds": "FLINK_WATERMARK_DELAY_SECONDS",
            "idle_timeout_seconds": "FLINK_IDLE_TIMEOUT_SECONDS",
            "checkpoint_seconds": "FLINK_CHECKPOINT_SECONDS",
        }
        values: dict = {}
        for field, variable in text.items():
            values[field] = env.get(variable) or getattr(defaults, field)
        for field, variable in numbers.items():
            raw = env.get(variable)
            values[field] = int(raw) if raw else getattr(defaults, field)
        return cls(**values)


def literal(value: str) -> str:
    """A SQL string literal. Values are validated first; this is the second line."""
    return "'" + value.replace("'", "''") + "'"


def configuration(settings: Settings) -> dict[str, str]:
    config = {
        "pipeline.name": settings.job_name,
        # Window bounds are formatted in this zone, so they read as UTC.
        "table.local-time-zone": "UTC",
        "execution.checkpointing.interval": f"{settings.checkpoint_seconds} s",
        # Inside the job: restored state counts each event once.
        "execution.checkpointing.mode": "EXACTLY_ONCE",
    }
    if settings.idle_timeout_seconds:
        # A partition with no records for this long stops holding the watermark
        # back. Decided on processing time, so which events count can differ
        # between a run and its replay.
        config["table.exec.source.idle-timeout"] = f"{settings.idle_timeout_seconds} s"
    return config


def source_ddl(settings: Settings) -> str:
    return f"""
CREATE TABLE pageviews (
    event_id STRING,
    user_id STRING,
    page STRING,
    `timestamp` DOUBLE,
    -- The producer's Unix seconds as a millisecond timestamp. Never null, and
    -- never implausibly far ahead, whatever arrives: see EARLIEST and
    -- FUTURE_SKEW_SECONDS. The multiplication happens only for accepted values,
    -- so nothing can overflow.
    event_time AS TO_TIMESTAMP_LTZ(
        CAST(
            CASE
                WHEN `timestamp` >= {EARLIEST}
                    AND `timestamp` <= UNIX_TIMESTAMP() + {FUTURE_SKEW_SECONDS}
                THEN `timestamp` * 1000
                ELSE 0
            END AS BIGINT
        ),
        3
    ),
    WATERMARK FOR event_time AS
        event_time - INTERVAL '{settings.watermark_delay_seconds}' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = {literal(settings.source_topic)},
    'properties.bootstrap.servers' = {literal(settings.bootstrap)},
    'properties.group.id' = {literal(settings.group)},
    -- Every submission replays everything Kafka retains, and so repeats
    -- results earlier jobs wrote. Resuming from committed offsets instead
    -- would start a fresh job past events whose windows were still open, with
    -- none of their partial counts, and emit those windows short. Restoring
    -- state and offsets together needs a retained checkpoint — 0.11's
    -- submitter. The group's offsets are still committed, for monitoring.
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    -- Skip what cannot be parsed. This alone is not enough: unparseable and
    -- missing fields become NULL in a row that is kept, so the view below
    -- decides what counts.
    'json.ignore-parse-errors' = 'true'
)
""".strip()


def view_ddl() -> str:
    """The rows that count: a page, and an event time this pipeline could produce.

    Filtered on `event_time` itself rather than by re-testing the timestamp: the
    skew check reads processing time, so evaluating it twice could decide twice.
    The computed column decides once; the epoch marks a rejected row.
    """
    return """
CREATE TEMPORARY VIEW valid_pageviews AS
SELECT page, event_time
FROM pageviews
WHERE page IS NOT NULL
  AND page <> ''
  AND event_time > TO_TIMESTAMP_LTZ(0, 3)
""".strip()


def sink_ddl(settings: Settings) -> str:
    return f"""
CREATE TABLE pageview_windows (
    window_start STRING,
    window_end STRING,
    page STRING,
    -- Quoted: VIEWS is a reserved word in Flink SQL.
    `views` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = {literal(settings.sink_topic)},
    'properties.bootstrap.servers' = {literal(settings.bootstrap)},
    -- Keyed by page, so one page's windows stay in order on one partition.
    'key.format' = 'raw',
    'key.fields' = 'page',
    'value.format' = 'json',
    -- At the edge: results since the last checkpoint may be written twice.
    'sink.delivery-guarantee' = 'at-least-once'
)
""".strip()


def insert_dml(settings: Settings) -> str:
    return f"""
INSERT INTO pageview_windows
SELECT
    DATE_FORMAT(window_start, {ISO}) AS window_start,
    DATE_FORMAT(window_end, {ISO}) AS window_end,
    page,
    COUNT(*) AS `views`
FROM TABLE(
    TUMBLE(
        TABLE valid_pageviews,
        DESCRIPTOR(event_time),
        INTERVAL '{settings.window_seconds}' SECOND
    )
)
GROUP BY window_start, window_end, page
""".strip()


def statements(settings: Settings) -> list[str]:
    return [source_ddl(settings), view_ddl(), sink_ddl(settings), insert_dml(settings)]


def main() -> None:
    # Imported here, not at the top: the host imports this module, and has no
    # PyFlink and no Java.
    from pyflink.table import EnvironmentSettings, TableEnvironment

    settings = Settings.from_environment()
    environment = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    config = environment.get_config()
    for key, value in configuration(settings).items():
        config.set(key, value)
    for statement in statements(settings):
        # The INSERT submits the job and returns; `flink run -d` leaves it
        # running after this driver exits.
        environment.execute_sql(statement)


if __name__ == "__main__":
    main()
