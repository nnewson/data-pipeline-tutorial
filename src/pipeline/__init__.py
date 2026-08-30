import logging
import time
from collections.abc import Callable

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

# The Kafka client logs its connection handshakes at INFO, which buries the
# pipeline's own output. Its warnings still matter, so raise the floor rather
# than silence it.
logging.getLogger("kafka").setLevel(logging.WARNING)

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
    """Create a topic if it is not already there.

    Kafka would auto-create it on first use, but not before logging an error
    about metadata it cannot find — which looks like a fault on a first run and
    is not one. Declaring the topic also puts its partition count somewhere
    visible, which is what 0.3 changes.
    """
    admin = wait_for_connection(
        "Kafka admin",
        lambda: KafkaAdminClient(bootstrap_servers=bootstrap_servers),
    )
    try:
        admin.create_topics(
            [NewTopic(name=name, num_partitions=partitions, replication_factor=1)]
        )
        logger.info(f"Created topic {name} with {partitions} partition(s)")
    except TopicAlreadyExistsError:
        logger.info(f"Topic {name} already exists")
    finally:
        admin.close()
