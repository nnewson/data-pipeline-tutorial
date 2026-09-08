"""Print the Redis counters and their total.

The total is the point: it should equal the number of events produced. After a
consumer crashes with work uncommitted, it does not, and the difference is the
number of events that were replayed.
"""

import logging

from pipeline.config import REDIS_KEY_PREFIX
from pipeline.redis_store import connect, last_pages, page_counts

logger = logging.getLogger("counters")


def main() -> int:
    client = connect()
    try:
        counts = page_counts(client)
        pages = last_pages(client)
    finally:
        client.close()

    if not counts:
        print("no counters yet")
        return 0

    width = max(len(page) for page in counts)
    for page, count in sorted(counts.items()):
        print(f"  {page:<{width}}  {count}")

    total = sum(counts.values())
    print(f"\n  {'total':<{width}}  {total}")
    print(f"  {'users tracked':<{width}}  {len(pages)}")
    if REDIS_KEY_PREFIX:
        print(f"\n  (prefix {REDIS_KEY_PREFIX!r})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
