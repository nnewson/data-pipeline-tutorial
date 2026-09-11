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

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

# Prefixes every key this process writes. Overridable so a test run can keep its
# keys apart from a topology someone else is running, the way CONSUMER_GROUP
# keeps its offsets apart.
REDIS_KEY_PREFIX = os.environ.get("REDIS_KEY_PREFIX", "")

# Split, stripped, and emptied entries dropped: "host1, host2" should not
# produce a host named " host2".
CASSANDRA_HOSTS = [
    host.strip()
    for host in os.environ.get("CASSANDRA_HOSTS", "localhost").split(",")
    if host.strip()
]
CASSANDRA_PORT = int(os.environ.get("CASSANDRA_PORT", "9042"))

# Overridable so a test run can have a keyspace of its own, the way
# CONSUMER_GROUP and REDIS_KEY_PREFIX give it offsets and keys of its own.
CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "pipeline")

# Named explicitly so the driver does not have to guess which datacenter is
# local, and so it matches the keyspace's NetworkTopologyStrategy.
CASSANDRA_LOCAL_DC = os.environ.get("CASSANDRA_LOCAL_DC", "datacenter1")

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", "5672"))

# Local demonstration credentials. RabbitMQ's built-in guest account may only
# connect over the broker's own loopback interface, so a container client using
# it would be refused — a real user is needed even for a tutorial.
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "pipeline")
RABBITMQ_PASSWORD = os.environ.get("RABBITMQ_PASSWORD", "pipeline")

# One shared queue with competing workers, not one queue per Kafka partition.
# Overridable so a test run can have a queue of its own.
RABBITMQ_QUEUE = os.environ.get("RABBITMQ_QUEUE", "analytics_jobs")

# The workers exist to be slow: a queue earns its place by keeping slow work off
# the fast path, and a delay is what makes queue depth and prefetch observable.
WORKER_DELAY_SECONDS = float(os.environ.get("WORKER_DELAY_SECONDS", "0.5"))

# How many unacknowledged messages one worker may hold. 1 spreads work evenly;
# higher values let a worker reserve a backlog and can reduce fairness.
WORKER_PREFETCH = int(os.environ.get("WORKER_PREFETCH", "1"))
