# Data Pipeline Tutorial

A step-by-step rebuild of [data-pipeline](https://github.com/nnewson/data-pipeline),
released one technology at a time, with a walkthrough post for each release at
[nnewson.dev](https://nnewson.dev).

**This release: 0.5 — Cassandra.** The consumers now write to a durable,
query-shaped store as well as to Redis. The same replay that inflates a Redis
counter leaves the Cassandra table correct — which is the point: idempotency is
a property of the write, not of the delivery.

## This release

```bash
git clone https://github.com/nnewson/data-pipeline-tutorial.git
cd data-pipeline-tutorial
git checkout 0.5
```

## Prerequisites

- Python 3.12
- [uv](https://docs.astral.sh/uv/)
- Docker and Docker Compose

## Setup

```bash
uv sync --all-extras
docker compose up -d --wait
```

`--wait` blocks until the broker answers an API request, not merely until the
container starts.

## Running the pipeline

Create the topic first. Nothing else creates it — auto-creation is disabled on
the broker, so a topic's partition count is a decision rather than an accident:

```bash
uv run create-topics
uv run create-schema
```

```text
INFO pipeline: Created topic pageviews with 4 partition(s)
INFO schema: Applied cassandra_schema.cql to keyspace pipeline
```

Cassandra takes 40–90 seconds to accept connections on a first start, so
`docker compose up -d --wait` will sit there for a while before returning. It
has not hung.

Then start everything — a producer and four consumers:

```bash
uv run honcho start
```

```text
consumer_3.1 | INFO consumer: Consumed (partition 2, offset 0): {'user_id': 'ssingh', ...}
consumer_1.1 | INFO consumer: Consumed (partition 0, offset 0): {'user_id': 'daniel60', ...}
consumer_4.1 | INFO consumer: Committed offsets after 5 messages
```

Four consumers in one group, four partitions, so each consumer gets exactly one.
Add a fifth and it sits idle: a partition has at most one consumer in a group,
which is the ceiling on how far a group can scale.

If you start a consumer before the topic exists it waits and tells you what to
run, rather than looping on a metadata error.

While it runs, the counters climb:

```bash
uv run counters
docker compose exec redis redis-cli --scan --pattern 'pageviews:*'
```

If you are carrying a volume over from 0.2, `create-topics` expands the existing
one-partition topic to four rather than leaving it as it found it. Without that
the producer would address partitions that do not exist. Your 0.2 events are
kept; they all live on partition 0, because that is the only partition they
could have been written to.

## Which partition, and why it matters

The producer routes on the first letter of the username:

```python
partition = get_partition(event["user_id"], KAFKA_PARTITIONS)
```

The rule itself is arbitrary. The property it buys is not: **the same user always
lands on the same partition**, and Kafka guarantees order within a partition. So
one user's events stay in order relative to each other, while unrelated users
process in parallel. Ordering is per-partition, never across the topic.

The cost is visible immediately — a real run gave 38, 37, 28 and 12 events to the
four partitions. Usernames are not uniform across the alphabet, so this key
produces skew, and the busiest partition sets the pace. Choosing a partition key
is choosing both your ordering guarantee and your load balance.

## Two writes, one replay

The consumer applies each event to Redis before committing its offset:

```python
client.incr(page_count_key(event["page"]))  # not idempotent
client.set(last_page_key(event["user_id"]), event["page"])  # idempotent
```

`INCR` is **atomic but not idempotent**, and those are different properties.
Atomicity is what makes four consumers safe to increment the same counter
concurrently — Redis executes an individual `INCR` atomically by serialising
normal command execution, so two increments cannot interleave. Idempotency would
make a *replayed* event safe, and `INCR` does not offer it.

`SET` is last-writer-wins. Apply the same event twice and the value is
unchanged, so it converges. That holds here because 0.3's routing rule keeps one
user's events on one partition, in order — the partitioning release is
load-bearing for this claim.

The pair is not atomic even though each write is. The injected crash below
happens after both, which keeps the demonstration about replay; a real crash
between them would leave partial state.

## Watching a replay corrupt a counter

Start clean, and produce an exact number of events:

```bash
docker compose down --volumes && docker compose up -d --wait
uv run create-topics
uv run producer --count 20
```

Run one consumer that dies with work uncommitted:

```bash
COMMIT_EVERY=5 CONSUMER_CRASH_AFTER=3 uv run consumer
```

```text
WARNING consumer: Injected crash after 3 messages with 3 uncommitted
```

Now run a consumer again and wait for `Committed offsets after 20 messages`
before stopping it. Watching the counters is not enough on its own — they can
reach their final value just before the last commit.

```bash
COMMIT_EVERY=5 uv run consumer
uv run counters
```

```text
  /          4
  /checkout  2
  /docs      12
  /pricing   5

  total      23
  users tracked  20
```

**Twenty events produced, twenty-three counted.** The three that were processed
before the crash were never committed, so they were delivered again — and
counted again.

Now ask Cassandra the same question:

```bash
uv run events --count
```

```text
  rows  20
```

**Twenty-three in Redis, twenty in Cassandra**, from one replay through one
handler. Nothing about the delivery differed. What differed is what each write
does with a repeat:

```python
client.incr(page_count_key(event["page"]))  # accumulates
session.execute(insert, (user_id, event_time, event_id, page))  # replaces
```

The Cassandra insert is an **upsert**, not a deduplicate. Its primary key —
`((user_id), event_time, event_id)` — addresses a row, and writing it again
replaces what was there. Nothing detected the duplicate; there was simply
nowhere else for it to go.

You can watch that happen. Replay everything a second time and look at one
user's row before and after:

```bash
uv run events --user <some-user>
```

```text
before:  2026-09-08 19:04:51.716000 /docs  aac778f3-…  written_at=1788894338135020
after:   2026-09-08 19:04:51.716000 /docs  aac778f3-…  written_at=1788894433249511
```

Same key, same values, **different write time**. The row is query-identical, not
byte-identical: Cassandra performed the write, and both versions can sit in
SSTables until compaction removes the older one. Meanwhile that same full replay
took the Redis total from 23 to 43.

`users tracked` is 20 in this run and, more to the point, unchanged by the
replay — but the number itself is not guaranteed, because the generator draws
random usernames and occasionally repeats one. Compare it before and after
rather than expecting a figure. The counter total and the row count are the
deterministic halves.

`--count` deserves a warning it gives itself:

```text
WARNING cassandra.protocol: Server warning: Aggregation query used without partition key
```

That is Cassandra pointing out that `COUNT(*)` scans every partition, which is
exactly what this table's design exists to avoid. It earns its place here
because the dataset is twenty rows and the alternative is asking you to take the
result on trust. It is a diagnostic, not an access pattern.

## Why the table looks like that

The table answers one question — *what did this user do, most recent first?* —
and its shape follows from that rather than from the shape of an event:

```sql
CREATE TABLE pageviews (
    user_id text,
    event_time timestamp,
    event_id text,
    page text,
    PRIMARY KEY ((user_id), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC, event_id ASC);
```

- **`user_id` is the partition key**: one user's history lives on one node and
  is read in one go. It is the same key 0.3 routes Kafka partitions by, so the
  property that keeps a user's events ordered in the log keeps them together in
  storage.
- **`event_time` clusters, descending**, because "most recent first" is the
  query.
- **`event_id` clusters after it** as a tie-breaker. Cassandra timestamps are
  millisecond-precision, so two events for one user can share `event_time`;
  without `event_id` in the key the second would silently overwrite the first,
  losing an event. Ordering *within* a millisecond is therefore event-id
  ordering, not chronology.

The cost is real: you cannot ask "who viewed /pricing?" of this table. The
conventional answer is a second table keyed by page, written at the same time —
a predictable access path, at the cost of writing each event twice. Cassandra 5
also offers Storage-Attached Indexes, which is a different design decision with
its own trade-offs.

One trap worth naming, because it fails silently rather than loudly. The
producer emits Unix seconds as a float, and the driver reads a bare number as
*milliseconds*:

```text
producer emits:                 1788893388.843115
bound as a float             -> 1970-01-21 16:54:53.388000
bound as a tz-aware datetime -> 2026-09-08 18:49:48.843000
```

So the insert binds `datetime.fromtimestamp(event["timestamp"], tz=UTC)`. Bind
the float and every row lands in January 1970 without an error anywhere.

## When a store goes away

Stop Cassandra while the consumer is idle and nothing happens: the driver
reconnects with backoff and the consumer carries on when it returns.

Stop it while a write is **in flight** and the story is different:

```text
cassandra.cluster.NoHostAvailable: ('Unable to complete the operation against any hosts',
  {<Host: 127.0.0.1:9042 datacenter1>: ConnectionShutdown('Connection to 127.0.0.1:9042 is closed')})
```

The consumer dies there, and where it dies matters. Redis had already been
updated for that event; the Kafka offset had not been committed. Measured: 58
events logged as consumed, **59 in the Redis counter** — the 59th event's
increment landed, and then the write that would have followed it did not.

Restart Cassandra and the consumer, let it drain, and the gap is still there:

```text
  total  398      # Redis
  rows   394      # Cassandra
```

So the lesson is narrower than "a dependency outage causes duplicates", and
narrower than its opposite:

> **Divergence needs the handler to terminate before committing.** A dependency
> that goes away while nothing is being written does not cause that. A write
> that fails does.

The injected crash from 0.3 is a controlled version of exactly this. The
difference is that a real fault picks its own moment, and that moment is
sometimes between two of the three writes.

A stopped container closes its socket, so the failure arrives as
`NoHostAvailable` almost immediately. A node that is merely unreachable would
instead exhaust the driver's request timeout — ten seconds by default — and
raise `OperationTimedOut`. Same consequence, different exception and different
delay.

## Derived state, and rebuilding it

Redis runs with persistence off:

```yaml
command: redis-server --save "" --appendonly no
```

That is deliberate, and worth being mechanical about. The image declares no
volume, but RDB snapshotting is *on* by default, so without that command Redis
would write snapshots into its own filesystem and survive a restart.

Three services, three answers to "what survives being replaced":

| | role | named volume |
|---|---|---|
| Kafka | the durable, replayable source log | yes |
| Redis | a deliberately volatile materialized view | no |
| Cassandra | a durable, query-oriented materialized view | yes |

Cassandra is still *derived* from the Kafka log — everything in it can be
rebuilt by the same replay that rebuilds Redis. Its volume changes how durable
and how queryable the view is, not where the truth lives.

Rebuilding is not automatic, though. Restart Redis and the counters are gone;
restart the pipeline and they stay gone, because the consumer group resumes from
its committed offsets and replays nothing:

```bash
docker compose restart redis
uv run consumer          # consumes nothing, counters stay empty
```

To actually rebuild, quiesce everything — **the producer too** — then reset the
group and replay:

```bash
# stop the producer and every consumer first
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group pipeline --topic pageviews --reset-offsets --to-earliest --execute
uv run consumer
uv run counters
```

```text
  total      20
  users tracked  20
```

Twenty again, not twenty-three: the same replay mechanism that corrupted the
counter repairs it, because this time nothing crashed partway.

Two conditions, both load-bearing. Stopping the producer gives the replay a
fixed endpoint, so the result is a number you can check rather than a moving
target — and if you take the other route, replaying under a *fresh* group
alongside the original one, it also stops the two groups double-counting
whatever arrives during the cutover. And replay without failure injection, or
the rebuild overcounts exactly as the original run did.

The reset also requires the group to be *inactive*. A consumer killed abruptly
stays a member until its session times out, so the command refuses for around
forty-five seconds afterwards with `the current state is Stable`.

## The log outlives the container

The broker writes to a named volume, so the log and its committed offsets
survive the container being replaced:

```bash
uv run producer          # produce a few events, then stop it
docker compose down      # note: no --volumes
docker compose up -d --wait
uv run consumer          # the earlier events are still there
```

This needs two things, not one. The image declares a volume at
`/var/lib/kafka/data` but defaults `log.dirs` to `/tmp`, so mounting the volume
alone persists nothing — `KAFKA_LOG_DIRS` has to point Kafka at it. Use
`docker compose down --volumes` to start genuinely clean.

## Two addresses, one broker

0.1 established that a service has one address from the host and another from
inside the Compose network. Kafka makes that structural rather than incidental,
because a broker *tells clients where to go next*:

```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT_HOST://localhost:9092,PLAINTEXT_INTERNAL://kafka:29092
```

A client connects to a bootstrap address, and the broker replies with the
address it advertises for that listener. The client then connects *there*. So a
wrong advertised address fails in a particular way: the connection succeeds and
every produce afterwards fails, because the client was handed somewhere it
cannot reach.

Host processes use `localhost:9092`. Containers use `kafka:29092` — nothing does
yet, but Flink will at 0.10, and the smoke test proves the path works now.

```bash
# From the host
uv run smoke-test

# The internal address, from inside the network
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --list
```

## KRaft, and where ZooKeeper went

Kafka 4.x runs KRaft only: ZooKeeper mode was deprecated in 3.5 and removed in
4.0. This single node is both broker and controller, which is fine for local
development and explicitly not a production topology, where controllers are
separate nodes.

ZooKeeper still appears in this tutorial, at 0.7 — coordinating the pipeline's
own processes, which is a different job from storing Kafka's metadata.

## Testing

Unit tests need no broker.

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

The smoke test does. It asserts both addressing paths against the running
broker, and is the same check CI runs.

```bash
docker compose up -d --wait
uv run smoke-test
docker compose down
```

```text
PASS  host listener: produced and consumed via localhost:9092
PASS  internal listener: kafka:29092 and localhost:9092 are the same broker
PASS  partition routing: all 4 partitions addressed by the routing rule
PASS  honcho topology: 4 consumers owned 4 partitions of smoke_topology_c31da1e2, committed offsets advanced 106 to 317, 4 page counters, 338 last-page values, and rows in smoke_ba0870d4, and nothing was left running

all 4 checks passed in 21.8s
```

On a brand new cluster you will also see `NotCoordinatorError` once or twice
above these lines, for the same reason a first consumer does: Kafka is creating
the internal offsets topic for the first group that asks for it. It retries and
settles, and a second run is quiet.

The last check starts the real Procfile topology, waits for four group members to
own four partitions, confirms committed offsets advance, and stops it again. The
other three build their own clients, so they would all pass with a broken
Procfile.

It runs against a topic, consumer group, Redis key prefix and Cassandra keyspace
created for that run alone, all handed to Honcho through the environment. Sharing `pageviews` and the `pipeline` group
would let a topology you happen to have running satisfy the check — and a live
`honcho` process is not evidence that the members being observed are its own.
It asserts both Redis branches and that rows reached Cassandra with their key
columns populated, so the release cannot ship with any of its writes silently
not happening. Keys and keyspace are cleared only after the consumers have
stopped, or a live one would write them straight back.

CI runs these as two jobs. `quality` covers linting, formatting, unit tests and
Compose parsing; `integration` starts the real topology and runs the smoke test.
Both run on tag pushes as well as branches. A workflow cannot stop a tag from
existing, so the gate is editorial rather than technical: a GitHub release is
published only after both tag workflows pass.

## Project structure

```text
docker-compose.yml       Kafka in KRaft mode, Redis, and Cassandra
cassandra_schema.cql     the keyspace and table, applied by create-schema
src/pipeline/
    __init__.py          logging setup and connection retry
    config.py            environment-driven settings, host addresses by default
    topics.py            the one path that creates topics
    producer.py          routes events to partitions by username
    kafka_consumer.py    reads them back, applying each to both stores
    redis_store.py       the two Redis writes, and the keys they land on
    cassandra_store.py   the durable write, and the identity that keys it
    schema.py            the one path that creates keyspaces and tables
    counters.py          prints the Redis counters and their total
    events.py            reads back what Cassandra stored
    smoke_test.py        bounded assertions against a running broker
tests/
Procfile                 the processes that make up the running system
```

`config.py` grows an entry per technology as the tutorial proceeds. It is the
one place the host-versus-container distinction is expressed in code.
