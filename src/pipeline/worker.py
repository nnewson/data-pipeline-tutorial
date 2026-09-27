"""A worker: takes a job off the shared queue, does slow work, acknowledges.

Four of these compete for one queue. That is the contrast with 0.3, where four
consumers each owned a partition and a fifth would have sat idle: here the pool
has no such ceiling, and a dead worker's message goes to whoever is free.

What it costs is the ordering the log gave us. The queue is FIFO, but several
workers execute concurrently and a redelivery can reorder the effective
sequence, so two jobs for one user may be handled at the same time in either
order.
"""

import json
import logging
import os
import socket
import time
from contextlib import ExitStack

from pipeline import coordination, jobs_queue, redis_store
from pipeline.config import (
    RABBITMQ_QUEUE,
    WORKER_DELAY_SECONDS,
    WORKER_PREFETCH,
)
from pipeline.coordination import Paths
from pipeline.runtime_config import RuntimeConfig

logger = logging.getLogger("worker")


def identity() -> str:
    """Unique per process, so four identical workers do not collide."""
    return f"{socket.gethostname()}-{os.getpid()}"


def do_work(delay: float = WORKER_DELAY_SECONDS) -> None:
    """Stand in for something slow enough to be worth a queue."""
    time.sleep(delay)


def handle_job(
    redis_client,
    channel,
    method,
    properties,
    body: bytes,
    delay: float = WORKER_DELAY_SECONDS,
) -> None:
    """Do the work, record it, then acknowledge — in that order.

    Acknowledging first would be at-most-once: a worker dying mid-job would lose
    it. Acknowledging last is at-least-once, and the same trade as 0.3's offset
    commit and 0.4's write ordering, in a third mechanism.
    """
    job = json.loads(body)
    event_id = job["event_id"]

    # `redelivered` is RabbitMQ telling us it sent this same delivery before.
    # It says nothing about a Kafka replay having published the event twice —
    # those arrive as two unrelated messages, both with redelivered=False.
    logger.info(
        f"Working job {event_id} (redelivered={method.redelivered}) "
        f"for {job['user_id']} {job['page']}"
    )

    do_work(delay)
    redis_store.record_execution(redis_client, event_id)
    channel.basic_ack(delivery_tag=method.delivery_tag)


def main() -> int:
    with ExitStack() as resources:
        redis_client = redis_store.connect()
        resources.callback(redis_client.close)

        # Live settings, so a change lands on the next job rather than the next
        # restart.
        settings = RuntimeConfig(WORKER_DELAY_SECONDS)
        paths = Paths()
        zk = coordination.connect()
        resources.callback(zk.close)
        resources.callback(zk.stop)
        coordination.wait_for_initialisation(zk, paths)

        presence = coordination.Presence(zk, paths, "worker", identity())
        # Listener first, then start: registering before the listener exists
        # leaves a window where a session loss would go unnoticed.
        zk.add_listener(presence.on_state)
        resources.callback(presence.stop)
        if not presence.start(
            delay=settings.worker_delay, config_version=settings.version
        ):
            # Part of the startup barrier, like the topic and the schema. A
            # worker doing work while absent from the registry makes the tree
            # under-report exactly when something is wrong. After startup a
            # session loss is survivable: Presence re-registers in the
            # background while the data path keeps running.
            raise RuntimeError(f"could not register as a worker under {paths.registry}")

        def adopt(raw: str, version: int) -> None:
            settings.apply(raw, version)
            # Report what was applied, so a test can prove every worker took it
            # rather than inferring from aggregate throughput.
            presence.update(
                delay=settings.worker_delay, config_version=settings.version
            )

        watcher = coordination.ConfigWatcher(zk, paths.worker_delay, adopt)
        zk.add_listener(watcher.on_state)
        resources.callback(watcher.stop)
        # Waits for the published value to be applied. Consuming on the
        # environment default would make the live-config demonstration a lie,
        # so a failure here is refused rather than logged and ignored.
        if not watcher.start():
            raise RuntimeError(
                f"no worker delay applied from {paths.worker_delay}; "
                "run `uv run cluster init`"
            )

        connection = jobs_queue.connect()
        resources.callback(connection.close)

        channel = connection.channel()
        jobs_queue.declare(channel, RABBITMQ_QUEUE)
        # How many unacknowledged messages this worker may hold at once.
        channel.basic_qos(prefetch_count=WORKER_PREFETCH)
        channel.basic_consume(
            queue=RABBITMQ_QUEUE,
            on_message_callback=lambda ch, method, properties, body: handle_job(
                # Read as the job begins: that is what makes the setting live.
                redis_client,
                ch,
                method,
                properties,
                body,
                delay=settings.worker_delay,
            ),
        )

        logger.info(
            f"Worker consuming {RABBITMQ_QUEUE} "
            f"(prefetch {WORKER_PREFETCH}, delay {WORKER_DELAY_SECONDS}s)"
        )
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Shutting down worker")
            channel.stop_consuming()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
