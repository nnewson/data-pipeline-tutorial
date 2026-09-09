"""Cassandra writes and reads, and the identity that makes them idempotent.

The counterpoint to `redis_store`. An insert here is an upsert: the primary key
addresses a row, and writing it again replaces it. Nothing detects a duplicate —
there is simply nowhere else for a repeat to go. That is idempotency as a
property of the write, and it is why the same replay that inflates a Redis
counter leaves this table correct.
"""

import logging
import re
from datetime import UTC, datetime

from cassandra import ProtocolVersion
from cassandra.cluster import Cluster, Session
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import PreparedStatement

from pipeline import wait_for_connection
from pipeline.config import CASSANDRA_HOSTS, CASSANDRA_LOCAL_DC, CASSANDRA_PORT

logger = logging.getLogger("cassandra-store")

TABLE = "pageviews"

# CQL identifiers cannot be bound as query parameters, so a keyspace name is
# interpolated into CREATE and DROP statements. Lowercase only: an unquoted
# uppercase identifier is folded to lowercase by Cassandra, so a name arriving
# quoted or from the driver later could disagree with the one that was created.
KEYSPACE_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,47}$")


def validate_keyspace(name: str) -> str:
    """Return name if it is safe to interpolate into a statement, else raise."""
    if not KEYSPACE_PATTERN.match(name):
        raise ValueError(
            f"unsafe keyspace name {name!r}: must match {KEYSPACE_PATTERN.pattern}"
        )
    return name


def event_timestamp(event: dict) -> datetime:
    """Convert the producer's Unix seconds into something the driver stores right.

    The driver treats a bare number as milliseconds, so binding the float
    directly stores a date in January 1970 without erroring. Timezone-aware
    datetimes are handled as UTC.
    """
    return datetime.fromtimestamp(event["timestamp"], tz=UTC)


def connect(
    hosts: list[str] = CASSANDRA_HOSTS,
    port: int = CASSANDRA_PORT,
    keyspace: str | None = None,
) -> tuple[Cluster, Session]:
    """Open a cluster connection, retrying until Cassandra accepts it."""

    def open_session() -> tuple[Cluster, Session]:
        cluster = Cluster(
            hosts,
            port=port,
            # Both specified rather than inferred. Left to itself the driver
            # negotiates the protocol downwards and warns each time, and warns
            # again about not being told which datacenter is local — noise on
            # every run that says nothing about this pipeline.
            protocol_version=ProtocolVersion.V5,
            load_balancing_policy=TokenAwarePolicy(
                DCAwareRoundRobinPolicy(local_dc=CASSANDRA_LOCAL_DC)
            ),
        )
        try:
            session = cluster.connect(keyspace) if keyspace else cluster.connect()
        except Exception:
            # Do not leak the cluster's threads and sockets on a failed attempt.
            cluster.shutdown()
            raise
        return cluster, session

    return wait_for_connection("Cassandra", open_session)


KEYSPACE_PLACEHOLDER = "${KEYSPACE}"


def apply_schema(session: Session, keyspace: str, statements: str) -> None:
    """Run a schema file, one statement at a time, against a named keyspace.

    The driver executes one statement per call, so the file is split rather than
    handed over whole. `${KEYSPACE}` is substituted after validation — an
    explicit placeholder rather than rewriting whichever word happens to appear
    first, which would silently corrupt a statement that mentioned it elsewhere.
    `USE` is skipped; the session is pointed at the keyspace instead.
    """
    validate_keyspace(keyspace)
    # Comments first, then split. A comment can contain a semicolon, and
    # splitting first turns the tail of one into a statement.
    for raw in _strip_comments(statements).split(";"):
        statement = raw.strip()
        if not statement:
            continue
        statement = statement.replace(KEYSPACE_PLACEHOLDER, keyspace)
        if statement.upper().startswith("USE "):
            session.set_keyspace(keyspace)
            continue
        session.execute(statement)


def _strip_comments(statement: str) -> str:
    return "\n".join(
        line for line in statement.splitlines() if not line.strip().startswith("--")
    )


def drop_keyspace(session: Session, keyspace: str) -> None:
    session.execute(f"DROP KEYSPACE IF EXISTS {validate_keyspace(keyspace)}")


def prepare_insert(session: Session) -> PreparedStatement:
    return session.prepare(
        f"INSERT INTO {TABLE} (user_id, event_time, event_id, page) VALUES (?, ?, ?, ?)"
    )


def record_pageview(session: Session, insert: PreparedStatement, event: dict) -> None:
    """Write one event. Writing the same event again replaces the same row."""
    session.execute(
        insert,
        (
            event["user_id"],
            event_timestamp(event),
            event["event_id"],
            event["page"],
        ),
    )


def user_history(session: Session, user_id: str, limit: int = 20) -> list:
    """One user's events, most recent first — the query the table was shaped for."""
    return list(
        session.execute(
            f"SELECT event_time, event_id, page, writetime(page) AS written_at "
            f"FROM {TABLE} WHERE user_id = %s LIMIT %s",
            (user_id, limit),
        )
    )


def row_count(session: Session) -> int:
    """Total rows.

    A diagnostic, not an access pattern: COUNT(*) scans every partition, which
    is exactly what the table's design says not to do. It earns its place here
    only because the dataset is tiny and the alternative is asking a reader to
    take the replay result on trust.
    """
    return session.execute(f"SELECT COUNT(*) AS total FROM {TABLE}").one().total
