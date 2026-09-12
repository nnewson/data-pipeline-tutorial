import logging
import time
from collections.abc import Callable

from kafka.admin import KafkaAdminClient, NewPartitions, NewTopic
from kafka.errors import TopicAlreadyExistsError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

# The Kafka client logs its connection handshakes at INFO, which buries the
# pipeline's own output. Its warnings still matter, so raise the floor rather
# than silence it.
logging.getLogger("kafka").setLevel(logging.WARNING)

# The Cassandra driver narrates connection and topology changes at INFO. Its
# warnings are kept: one of them is Cassandra telling you that COUNT(*) scans
# every partition, which is worth hearing.
logging.getLogger("cassandra").setLevel(logging.WARNING)

# pika narrates every connection, channel and socket transition at INFO, which
# is many lines per run and none of them about this pipeline.
logging.getLogger("pika").setLevel(logging.WARNING)

logger = logging.getLogger("pipeline")


def wait_for_connection[T](
    name: str, connect: Callable[[], T], retries: int = 10, delay: float = 3
) -> T:
    """Retry a connection function until it succeeds or retries are exhausted.

    Infrastructure in Compose is reported healthy before it is necessarily
    accepting client connections, and host processes start whenever they are
    started. Retrying is cheaper than ordering.
    """
    for attempt in range(1, retries + 1):
        try:
            return connect()
        except Exception as error:
            logger.warning(f"{name}: connection attempt {attempt}/{retries}: {error}")
            if attempt == retries:
                raise
            time.sleep(delay)
    raise RuntimeError(f"{name}: failed to connect")


def ensure_topic(name: str, partitions: int, bootstrap_servers: str) -> None:
    """Create a topic, or bring an existing one up to the wanted partition count.

    Treating "already exists" as success is not enough. A topic carried over
    from an earlier release may have fewer partitions than this release needs,
    and the producer would then address partitions that do not exist. Kafka can
    add partitions to a live topic, so reconcile rather than assume.

    Partitions cannot be removed, so a topic with more than requested is left
    alone and reported — it is a surprise the operator should resolve, not
    something to paper over.
    """
    admin = wait_for_connection(
        "Kafka admin",
        lambda: KafkaAdminClient(bootstrap_servers=bootstrap_servers),
    )
    try:
        try:
            admin.create_topics(
                [NewTopic(name=name, num_partitions=partitions, replication_factor=1)]
            )
            logger.info(f"Created topic {name} with {partitions} partition(s)")
            return
        except TopicAlreadyExistsError:
            pass

        existing = len(admin.describe_topics([name])[0]["partitions"])
        if existing == partitions:
            logger.info(f"Topic {name} already has {partitions} partition(s)")
        elif existing < partitions:
            admin.create_partitions({name: NewPartitions(total_count=partitions)})
            logger.info(
                f"Expanded topic {name} from {existing} to {partitions} partition(s)"
            )
        else:
            raise RuntimeError(
                f"Topic {name} has {existing} partitions, more than the "
                f"{partitions} this release expects. Kafka cannot remove "
                f"partitions; delete the topic or adjust KAFKA_PARTITIONS."
            )
    finally:
        admin.close()


def wait_for_topic(
    name: str, bootstrap_servers: str, retries: int = 10, delay: float = 3
) -> None:
    """Block until a topic exists, or give up saying so.

    With auto-creation disabled, a client that subscribes to a topic nobody has
    created yet logs a metadata error on a loop. Waiting for it turns that into
    one clear line, and makes the ordering explicit: topics are a prerequisite,
    not a side effect of connecting.
    """
    admin = wait_for_connection(
        "Kafka admin",
        lambda: KafkaAdminClient(bootstrap_servers=bootstrap_servers),
    )
    try:
        for attempt in range(1, retries + 1):
            if name in admin.list_topics():
                return
            logger.info(
                f"Waiting for topic {name} ({attempt}/{retries}); "
                f"run `uv run create-topics` if it does not appear"
            )
            time.sleep(delay)
    finally:
        admin.close()

    raise RuntimeError(
        f"Topic {name} does not exist. Run `uv run create-topics` first."
    )


def get_partition(username: str, num_partitions: int) -> int:
    """Route a username to a partition by its first letter.

    Splits the alphabet evenly, so with four partitions: a-g to 0, h-m to 1,
    n-t to 2, u-z to 3. The rule matters less than the property it gives us —
    the same username always lands on the same partition, so that user's events
    stay in order relative to each other.
    """
    if not username:
        return 0

    first_char = username[0].lower()
    if not first_char.isalpha():
        return 0

    index = ord(first_char) - ord("a")
    bucket_size = 26 / num_partitions
    return min(int(index // bucket_size), num_partitions - 1)
