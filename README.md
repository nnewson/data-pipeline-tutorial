# Data Pipeline Tutorial

A step-by-step rebuild of [data-pipeline](https://github.com/nnewson/data-pipeline),
released one technology at a time, with a walkthrough post for each release at
[nnewson.dev](https://nnewson.dev).

**This release: 0.6 — RabbitMQ.** The consumers now publish a job per event, and
four workers compete for one queue. A replay stops being about stored values and
starts being about work that runs twice — and there are now two different ways a
job can repeat, which look nothing alike.

## This release

```bash
git clone https://github.com/nnewson/data-pipeline-tutorial.git
cd data-pipeline-tutorial
git checkout 0.6
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

Then start everything — a producer, four consumers, and four workers:

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
uv run create-schema        # --volumes wiped the keyspace too
uv run producer --count 20
```

Run one consumer that dies with work uncommitted:

```bash
COMMIT_EVERY=5 CONSUMER_CRASH_AFTER=3 uv run consumer
```

```text
WARNING consumer: Injected crash after 3 messages with 3 uncommitted
```

The jobs those events publish need someone to run them, so start the workers in
a second terminal and leave them there:

```bash
uv run honcho start worker_1 worker_2 worker_3 worker_4
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

## One queue, four workers

The consumers publish a job per event; four workers compete for a single queue.
That is deliberately *not* what 0.3 does, and the contrast is the point.

```
Kafka:     4 partitions -> 4 consumers, one each.  A fifth consumer sits idle.
RabbitMQ:  1 queue      -> 4 workers competing.    A fifth worker adds capacity.
```

Kafka partitions preserve per-key processing order but cap a consumer group's
parallelism. This queue trades that order for a worker pool that scales with the
backlog. Two jobs for the same user can be handled at the same time, in either
order — the queue itself is FIFO, but concurrent workers and redelivery decide
what actually finishes when.

The workers are deliberately slow (`WORKER_DELAY_SECONDS`, default 0.5s),
because that is the reason a queue exists: keeping slow work off the fast path.
Run the topology with a fast producer and watch the backlog build:

```bash
uv run jobs
```

```text
  waiting     579
  workers     4
  completed   172
  distinct    172
```

**On fairness, a measured caveat.** With `prefetch=1` four workers took
43/43/43/43 jobs. With `prefetch=50` they took 37/37/37/37 — also even. High
prefetch *can* reduce fairness, but not here: four identical, permanently busy
workers get round-robin deliveries either way. Prefetch bites when workers
differ in speed, with a slow one sitting on reserved messages while a fast one
idles. This topology does not demonstrate that, and pretending otherwise would
be inventing a result.

## Two ways a job repeats

There are now two independent at-least-once boundaries, and they are not the
same thing:

| Failure | What RabbitMQ sees | `redelivered` |
|---|---|---|
| A worker dies before acknowledging | the same delivery, requeued | `True` |
| A Kafka consumer crashes after publishing | two publishes, same `event_id` | `False` on both |

RabbitMQ can tell you *it* redelivered something. It cannot tell you that an
upstream Kafka replay published the same event twice — at this layer those are
two unrelated messages. Only `event_id`, carried in the job body and the AMQP
`message_id`, reveals them as the same work.

**Kill a worker mid-job** and another picks it up. With the topology running
and a backlog building, find a worker and stop it:

```bash
pgrep -f '\.venv/bin/worker' | head -1 | xargs kill -9
```

Measured at 0.5s from `SIGKILL` to another worker logging:

```text
INFO worker: Working job ecaec99b-… (redelivered=True) for esilva /docs
```

Compare that with 0.3, where a dead consumer's partitions sit idle until the
group rebalances — tens of seconds, not fractions of one.

**Replay a Kafka crash** and `uv run jobs` shows the other shape. Twenty events,
one consumer crashed with three uncommitted, then restarted:

```text
  waiting     0
  workers     4
  completed   23
  distinct    20

  3 event(s) ran more than once:
    4e13c201-…  x2
    5136445a-…  x2
    fe525fda-…  x2
```

Twenty-three executions of twenty events — and **not one `redelivered=True` in
any worker log**. RabbitMQ saw twenty-three perfectly ordinary first deliveries,
because from its side that is exactly what they were. Only `event_id` shows
three of them as repeats.

Run those two from *separate clean stacks*. Sharing state makes an excess count
ambiguous, and telling the two apart is the whole lesson.

## Four writes, four behaviours

The handler now performs four writes before committing its offset:

| write | after a replay |
|---|---|
| `INCR` (Redis) | accumulates — the count is wrong |
| `SET` (Redis) | converges — the value is right |
| `INSERT` (Cassandra) | replaces — the row is right |
| **publish (RabbitMQ)** | **re-runs — the work happens twice** |

The first three are about stored state. The fourth is not: a duplicate job is a
side effect that *executes* again. If it sent an email or charged a card, "the
value converges" would be no comfort. Idempotency has to be designed into the
consumer of the job, not only the writer of a row.

## What a publisher confirm actually proves

```python
channel.confirm_delivery()
channel.basic_publish(..., mandatory=True)
```

Both are needed, and neither is sufficient alone. RabbitMQ will **confirm a
message it could not route**, so confirms by themselves do not show the job
reached a queue — `mandatory=True` is what turns an unroutable publish into a
visible failure.

And the guarantee has a limit worth stating plainly: the consumer cannot claim
the publish succeeded until it is confirmed, and if the connection fails before
the confirmation arrives, **the outcome is unknown**. Retrying then risks another
duplicate — the same problem this release is about, one layer down.

A failed publish raises, so the Kafka offset is not committed and the event is
redelivered.

## Counting work that has finished

Queue depth cannot tell you how much work completed: acknowledged messages are
gone. So the workers record completions in Redis, under the same per-run prefix
0.4 introduced:

```text
<prefix>jobs:runs    event_id -> how many times it ran
```

**One key, not two.** An earlier version also stored a running total. A worker
dying between the two increments left the total permanently ahead of the
per-event counts — 0.4's two-command trap, recreated in the instrumentation
meant to demonstrate it. The total is summed from this hash instead, in a single
read.

That is how `uv run jobs` names the events that ran more than once. Note what
this counter is: a non-idempotent side effect recording a non-idempotent side
effect. A worker dying between the Redis update and its acknowledgement will run
the work again *and* count it again. That is evidence for the lesson rather than
noise — but it does mean the number is not ground truth.

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
| RabbitMQ | durable work awaiting completion | yes, but only while unacknowledged |

Cassandra is still *derived* from the Kafka log — everything in it can be
rebuilt by the same replay that rebuilds Redis. Its volume changes how durable
and how queryable the view is, not where the truth lives.

RabbitMQ needs all three parts to be durable: a durable queue, persistent
messages, and a broker volume — **plus a fixed hostname**. Its data directory is
named after its node, which defaults to the container hostname, which Docker
defaults to the container id. Recreate the container without `hostname:
rabbitmq` and the broker starts under a different path inside the same volume,
so the durable queue comes back empty.

With that in place, both halves hold. Measured:

```text
3 messages published, no worker running   ->  (3 waiting, 0 consumers)
docker compose down && up                 ->  (3 waiting, 0 consumers)   survived
consume and acknowledge all three         ->  (0 waiting, 0 consumers)
docker compose down && up                 ->  (0 waiting, 0 consumers)   gone
```

Which is the distinction from Kafka in two lines: pending work survives, and
completed work cannot be replayed. A queue is not a log.

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

RabbitMQ follows the same pattern: `localhost:5672` from the host,
`rabbitmq:5672` from inside the network, with the management UI published
separately at [localhost:15672](http://localhost:15672).

It also needs a real user, which the others do not. RabbitMQ's built-in `guest`
account may only connect over the broker's own loopback interface, so a client
in a sibling container using it would be refused — the documented
`rabbitmq:5672` address would not work. Compose creates a `pipeline` user
instead, which is also the management UI login. These are local demonstration
credentials and nothing more.

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
PASS  honcho topology: 4 consumers owned 4 partitions of smoke_topology_8f3413ef, committed offsets advanced 113 to 339, 4 page counters, 346 last-page values, rows in smoke_8f3413ef, and 347 job(s) across 347 event(s) completed by 4 workers, and nothing was left running

all 4 checks passed in 23.1s
```

On a brand new cluster you will also see `NotCoordinatorError` once or twice
above these lines, for the same reason a first consumer does: Kafka is creating
the internal offsets topic for the first group that asks for it. It retries and
settles, and a second run is quiet.

The last check starts the real Procfile topology, waits for four group members to
own four partitions, confirms committed offsets advance, and stops it again. The
other three build their own clients, so they would all pass with a broken
Procfile.

It runs against a topic, consumer group, Redis key prefix, Cassandra keyspace
and RabbitMQ queue created for that run alone, all handed to Honcho through the
environment. Sharing `pageviews` and the `pipeline` group
would let a topology you happen to have running satisfy the check — and a live
`honcho` process is not evidence that the members being observed are its own.
It asserts both Redis branches, that rows reached Cassandra with their key
columns populated, that exactly four workers were consuming the run's queue, and
that jobs actually completed — so the release cannot ship with any of its writes
silently not happening. Publishing without a worker completing anything would
otherwise pass. Everything it created for the run — Redis keys, Cassandra
keyspace, RabbitMQ queue and Kafka topic — is removed only after the topology
has stopped, or a live consumer or worker would write straight back into it.

CI runs these as two jobs. `quality` covers linting, formatting, unit tests and
Compose parsing; `integration` starts the real topology and runs the smoke test.
Both run on tag pushes as well as branches. A workflow cannot stop a tag from
existing, so the gate is editorial rather than technical: a GitHub release is
published only after both tag workflows pass.

## Project structure

```text
docker-compose.yml       Kafka in KRaft mode, Redis, Cassandra, and RabbitMQ
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
    jobs_queue.py        one queue declaration, used by publisher and workers
    worker.py            competes for the queue, does slow work, acknowledges
    jobs.py              queue depth and completed executions
    smoke_test.py        bounded assertions against a running broker
tests/
Procfile                 the processes that make up the running system
```

`config.py` grows an entry per technology as the tutorial proceeds. It is the
one place the host-versus-container distinction is expressed in code.
