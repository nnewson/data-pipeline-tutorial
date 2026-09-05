import os

# Defaults target the host. Processes running inside the Compose network
# override these, because the same broker answers to a different address
# there — see the two listeners in docker-compose.yml.
KAFKA_SERVER = os.environ.get("KAFKA_SERVER", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "pageviews")

KAFKA_PARTITIONS = int(os.environ.get("KAFKA_PARTITIONS", "4"))

# Overridable so a test run can use a group of its own rather than sharing the
# one the Procfile topology uses.
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "pipeline")

# Seconds between generated events. Low enough to watch, high enough to read.
PRODUCER_INTERVAL_SECONDS = float(os.environ.get("PRODUCER_INTERVAL_SECONDS", "1"))

# Offsets are committed every this many messages rather than after each one.
# Anything processed since the last commit is replayed if the consumer dies,
# so this number is the size of the duplicate window.
COMMIT_EVERY = int(os.environ.get("COMMIT_EVERY", "5"))

# Failure injection: exit abruptly after processing this many messages, without
# committing what is pending. Unset means run normally.
_crash_after = os.environ.get("CONSUMER_CRASH_AFTER")
CONSUMER_CRASH_AFTER = int(_crash_after) if _crash_after else None
