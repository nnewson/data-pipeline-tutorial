"""Apply the Cassandra schema. The one path that creates keyspaces and tables.

Same shape as `create-topics`: things exist because something created them
deliberately, not because a client happened to ask first.
"""

import logging
from pathlib import Path

from pipeline import cassandra_store
from pipeline.config import CASSANDRA_KEYSPACE

logger = logging.getLogger("schema")

SCHEMA_FILE = Path(__file__).resolve().parents[2] / "cassandra_schema.cql"


def main() -> int:
    statements = SCHEMA_FILE.read_text()
    cluster, session = cassandra_store.connect()
    try:
        cassandra_store.apply_schema(session, CASSANDRA_KEYSPACE, statements)
        logger.info(f"Applied {SCHEMA_FILE.name} to keyspace {CASSANDRA_KEYSPACE}")
    finally:
        cluster.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
