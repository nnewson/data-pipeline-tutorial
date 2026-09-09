"""Read back what Cassandra stored.

`--count` is a diagnostic over a tiny dataset, not an access pattern: it scans
every partition, which is what the table's design exists to avoid. `--user` is
the query the table was actually shaped for.
"""

import argparse
import logging

from pipeline import cassandra_store
from pipeline.config import CASSANDRA_KEYSPACE

logger = logging.getLogger("events")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect stored pageview events.")
    parser.add_argument("--user", help="show this user's history, most recent first")
    parser.add_argument(
        "--count",
        action="store_true",
        help="total rows (a diagnostic: it scans every partition)",
    )
    return parser.parse_args(argv)


def main() -> int:
    arguments = parse_args()
    cluster, session = cassandra_store.connect(keyspace=CASSANDRA_KEYSPACE)
    try:
        if arguments.user:
            rows = cassandra_store.user_history(session, arguments.user)
            if not rows:
                print(f"no events for {arguments.user}")
                return 0
            for row in rows:
                print(
                    f"  {row.event_time:%Y-%m-%d %H:%M:%S.%f} "
                    f"{row.page:<12} {row.event_id}  written_at={row.written_at}"
                )
            print(f"\n  {len(rows)} events for {arguments.user}")
        else:
            print(f"  rows  {cassandra_store.row_count(session)}")
    finally:
        cluster.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
