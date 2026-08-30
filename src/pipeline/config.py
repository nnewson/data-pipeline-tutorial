import os

# Defaults target the host. Processes running inside the Compose network
# override these, because the same broker answers to a different address
# there — see the two listeners in docker-compose.yml.
KAFKA_SERVER = os.environ.get("KAFKA_SERVER", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "pageviews")

# One partition at 0.2. 0.3 is about what changes when this is four.
KAFKA_PARTITIONS = int(os.environ.get("KAFKA_PARTITIONS", "1"))

# Seconds between generated events. Low enough to watch, high enough to read.
PRODUCER_INTERVAL_SECONDS = float(os.environ.get("PRODUCER_INTERVAL_SECONDS", "1"))
