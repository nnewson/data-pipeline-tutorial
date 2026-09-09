from datetime import UTC, datetime

import pytest

from pipeline import cassandra_store


class FakeSession:
    def __init__(self):
        self.executed = []
        self.keyspace = None
        self.rows = {}

    def execute(self, statement, parameters=None):
        self.executed.append((statement, parameters))
        if parameters and len(parameters) == 4:
            # Primary key is (user_id, event_time, event_id): a repeat replaces.
            self.rows[(parameters[0], parameters[1], parameters[2])] = parameters[3]
        return self

    def prepare(self, statement):
        return f"prepared:{statement}"

    def set_keyspace(self, keyspace):
        self.keyspace = keyspace

    def one(self):
        return self


@pytest.mark.parametrize("name", ["pipeline", "smoke_ab12cd34", "a", "a" * 48])
def test_valid_keyspace_names_are_accepted(name):
    assert cassandra_store.validate_keyspace(name) == name


@pytest.mark.parametrize(
    "name",
    [
        "",
        "1leading_digit",
        "has-hyphen",
        "has space",
        "Uppercase",  # folded to lowercase by CQL; reject rather than surprise
        "drop_it; DROP KEYSPACE pipeline",
        "a" * 49,
    ],
)
def test_unsafe_keyspace_names_are_rejected(name):
    with pytest.raises(ValueError, match="unsafe keyspace name"):
        cassandra_store.validate_keyspace(name)


def test_drop_keyspace_validates_before_interpolating():
    """The name reaches a statement by interpolation, so it is checked there."""
    session = FakeSession()

    with pytest.raises(ValueError):
        cassandra_store.drop_keyspace(session, "x; DROP KEYSPACE pipeline")

    assert session.executed == []


def test_event_timestamp_converts_unix_seconds():
    """A bare float would be read as milliseconds and land in 1970."""
    event = {"timestamp": 1788894338.843115}

    converted = cassandra_store.event_timestamp(event)

    assert converted.tzinfo is UTC
    assert converted.year == 2026
    assert converted == datetime.fromtimestamp(1788894338.843115, tz=UTC)


def test_schema_placeholder_is_substituted():
    session = FakeSession()
    statements = "CREATE KEYSPACE ${KEYSPACE} WITH x = 1;\nUSE ${KEYSPACE};"

    cassandra_store.apply_schema(session, "smoke_ab12", statements)

    assert "CREATE KEYSPACE smoke_ab12 WITH x = 1" in session.executed[0][0]
    assert session.keyspace == "smoke_ab12"


def test_schema_comments_containing_semicolons_do_not_become_statements():
    """The bug this guards: splitting on ';' before stripping comments."""
    session = FakeSession()
    statements = (
        "-- the node's; ours is set elsewhere\nCREATE TABLE t (a int PRIMARY KEY);"
    )

    cassandra_store.apply_schema(session, "smoke_ab12", statements)

    assert len(session.executed) == 1
    assert session.executed[0][0].startswith("CREATE TABLE")


def test_apply_schema_rejects_an_unsafe_keyspace():
    session = FakeSession()

    with pytest.raises(ValueError):
        cassandra_store.apply_schema(session, "Bad Name", "CREATE TABLE t (a int);")

    assert session.executed == []


def _event(user="ada", page="/docs", event_id="e1", timestamp=1788894338.5):
    return {
        "user_id": user,
        "page": page,
        "event_id": event_id,
        "timestamp": timestamp,
    }


def test_recording_an_event_binds_the_primary_key_columns():
    session = FakeSession()

    cassandra_store.record_pageview(session, "insert", _event())

    _, parameters = session.executed[0]
    assert parameters[0] == "ada"
    assert parameters[1] == cassandra_store.event_timestamp(_event())
    assert parameters[2] == "e1"
    assert parameters[3] == "/docs"


def test_replaying_an_event_replaces_the_same_row():
    """The release's thesis: the write is what makes the replay harmless."""
    session = FakeSession()
    event = _event()

    cassandra_store.record_pageview(session, "insert", event)
    cassandra_store.record_pageview(session, "insert", event)

    # Two writes, one row: the key addresses the same place.
    assert len(session.rows) == 1
    assert len(session.executed) == 2


def test_two_events_in_the_same_millisecond_are_distinct_rows():
    """Why event_id is in the key: timestamps are milliseconds."""
    session = FakeSession()
    when = 1788894338.5

    cassandra_store.record_pageview(session, "i", _event(event_id="a", timestamp=when))
    cassandra_store.record_pageview(session, "i", _event(event_id="b", timestamp=when))

    assert len(session.rows) == 2


def test_a_different_user_is_a_different_partition():
    session = FakeSession()

    cassandra_store.record_pageview(session, "i", _event(user="ada"))
    cassandra_store.record_pageview(session, "i", _event(user="bob"))

    assert len(session.rows) == 2
