"""Topic management, run once before the pipeline starts.

Auto-creation is disabled on the broker, so topics exist because something
created them deliberately. That is the point: a topic's partition count is a
design decision, and letting a stray client invent one silently is how a
production cluster ends up with a one-partition topic nobody meant.

Running this again is safe. It creates the topic, expands it if an earlier
release left it with fewer partitions, and leaves it alone otherwise.
"""

import logging

from pipeline import ensure_topic
from pipeline.config import KAFKA_PARTITIONS, KAFKA_SERVER, KAFKA_TOPIC

logger = logging.getLogger("topics")


def main() -> int:
    ensure_topic(KAFKA_TOPIC, KAFKA_PARTITIONS, KAFKA_SERVER)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
