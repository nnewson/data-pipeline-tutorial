-- ${KEYSPACE} is substituted by `create-schema` after validating the name;
-- CQL identifiers cannot be bound as query parameters. It is normally
-- `pipeline`, and a smoke run points the same schema at a keyspace of its own.
--
-- NetworkTopologyStrategy even for one node: it is what Cassandra recommends
-- for anything that might grow, and changing strategy later is migration work.
-- The datacenter name must match the node's; ours is set in docker-compose.yml.
CREATE KEYSPACE IF NOT EXISTS ${KEYSPACE} WITH replication = {
    'class': 'NetworkTopologyStrategy',
    'datacenter1': 1
};

USE ${KEYSPACE};

-- Shaped by one question: what did this user do, most recent first?
--
--   user_id     partition key  - one user's history on one node, read in one go
--   event_time  clustering     - order within the partition, newest first
--   event_id    clustering     - tie-breaker; timestamps are milliseconds, so
--                                two events for one user can share event_time,
--                                and without this the second would overwrite
--                                the first
--
-- Keying on what the event carries, rather than on when the write happened, is
-- what makes a replayed insert land on the row it wrote last time.
CREATE TABLE IF NOT EXISTS pageviews (
    user_id text,
    event_time timestamp,
    event_id text,
    page text,
    PRIMARY KEY ((user_id), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC, event_id ASC);
