"""Committed offsets for a consumer group.

0.6 found that kafka-python 3 dropped `describe_consumer_groups` and
`list_consumer_group_offsets` from its admin client, and I wrongly generalised
that into "the admin client cannot read group offsets". It can:
`list_group_offsets` is the renamed equivalent, and it returns
`{group: {TopicPartition: OffsetAndMetadata}}`.

Worth keeping the correction visible — an API that was renamed is a different
thing from one that was removed, and the difference changed what the snapshot
could contain.
"""

import logging

from kafka.admin import KafkaAdminClient

from pipeline.config import KAFKA_SERVER, KAFKA_TOPIC

logger = logging.getLogger("kafka-offsets")


class OffsetReader:
    """Reads committed offsets, holding one admin client.

    Built once and reused. Constructing a KafkaAdminClient per snapshot is the
    same churn that was just removed from the Cassandra path: connection setup
    costs far more than the query, and its retries would block the leader loop
    for longer than the snapshot interval during an outage.
    """

    def __init__(self, bootstrap_servers: str = KAFKA_SERVER) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._admin: KafkaAdminClient | None = None

    def _client(self) -> KafkaAdminClient | None:
        if self._admin is None:
            try:
                self._admin = KafkaAdminClient(
                    bootstrap_servers=self._bootstrap_servers
                )
            except Exception as error:  # noqa: BLE001 - retried next time
                logger.warning(f"admin client unavailable: {error}")
                return None
        return self._admin

    def committed_total(self, group: str, topic: str = KAFKA_TOPIC) -> int | None:
        """Committed offsets for one topic in a group, summed.

        Filtered by topic on purpose: a group that has ever consumed another
        topic retains offsets for it, and those would silently inflate the
        number the snapshot reports for this pipeline.
        """
        admin = self._client()
        if admin is None:
            return None
        try:
            for_group = admin.list_group_offsets(group).get(group, {})
        except Exception as error:  # noqa: BLE001 - a group may not exist yet
            logger.warning(f"could not read offsets for {group}: {error}")
            # Drop it: a broken client is not worth keeping for the next call.
            self.close()
            return None
        return sum(
            metadata.offset
            for partition, metadata in for_group.items()
            if partition.topic == topic and metadata.offset >= 0
        )

    def close(self) -> None:
        if self._admin is not None:
            try:
                self._admin.close()
            except Exception as error:  # noqa: BLE001 - closing is best effort
                logger.warning(f"could not close admin client: {error}")
            self._admin = None
