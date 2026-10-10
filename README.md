# Data Pipeline Tutorial

A step-by-step rebuild of [data-pipeline](https://github.com/nnewson/data-pipeline),
released one technology at a time, with a walkthrough post for each release at
[nnewson.dev](https://nnewson.dev).

**This release: 0.10 — Flink: the job.** Stateful stream processing as a
self-contained unit: a Flink job counting page views in ten-second windows of
event time, from Kafka to Kafka. Event time makes the counts mean something;
watermarks decide when a window is done; and what a restart does to the results
is decided by checkpoints and the sink, not by the framework's name.

## This release

```bash
git clone https://github.com/nnewson/data-pipeline-tutorial.git
cd data-pipeline-tutorial
git checkout 0.10
```

## Prerequisites

- Python 3.12
- [uv](https://docs.astral.sh/uv/)
- Docker and Docker Compose
- Node, for the live page's JavaScript tests only

## Setup

```bash
uv sync --all-extras
docker compose up -d --wait
```

`--wait` blocks until the broker answers an API request, not merely until the
container starts. The first run also builds the Flink image from
`flink.Dockerfile`, which downloads a 1.6GB base image once.

## Running the pipeline

Create the topic first. Nothing else creates it — auto-creation is disabled on
the broker, so a topic's partition count is a decision rather than an accident:

```bash
uv run create-topics
uv run create-schema
uv run cluster init
```

```text
INFO pipeline: Created topic pageviews with 4 partition(s)
INFO schema: Applied cassandra_schema.cql to keyspace pipeline
INFO coordination: Coordination tree ready under /pipeline
```

Three commands, one per thing that has to exist before anything runs. Each is the
*only* path that creates its own persistent state, and everything else waits for
it rather than inventing its own — the same rule for topics, the Cassandra
keyspace and the ZooKeeper tree.

Cassandra takes 40–90 seconds to accept connections on a first start, so
`docker compose up -d --wait` will sit there for a while before returning. It
has not hung.

Then start everything — a producer, four consumers, four workers, three
coordinators, and the API:

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

The handler performs four writes before committing its offset — and, since 0.9,
a best-effort notification after them:

| write | after a replay |
|---|---|
| `INCR` (Redis) | accumulates — the count is wrong |
| `SET` (Redis) | converges — the value is right |
| `INSERT` (Cassandra) | replaces — the row is right |
| **publish (RabbitMQ)** | **re-runs — the work happens twice** |
| `PUBLISH` (Redis, 0.9) | a new publication of the same event — and a subscriber that was away hears neither |

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

## One leadership, three contenders

Three coordinators run; one leads. The leader alone writes a snapshot of the
pipeline every couple of seconds, which 0.8 will serve over HTTP.

```bash
uv run cluster status
```

```text
  leader      Nicks-MacBook-Pro.local-14101
  epoch       1
  contenders  3
  coordinators 3
  consumers   4
  workers     4
    config versions applied: [0]
  worker_delay 0.5 (version 0)
  snapshot    version 8: {"epoch": 1, "pageviews": 133, "jobs_completed": 108, ...}
```

Three contenders, one acknowledged leader. Run it before starting the topology and
every count is zero with no leader — the tree exists but nothing is participating
in it.

**None of this is Kafka's metadata.** Kafka has run KRaft since 0.2 and keeps its
own; nothing here touches it. This is application coordination — which is what
most people who deploy ZooKeeper actually deploy it for, and the thing the
tutorial could not show while ZooKeeper was hiding inside Kafka's setup.

Four znodes do four different jobs, and conflating them is the usual mistake:

```text
/election/<seq>   one ephemeral sequential node per contender. Counting these
                  counts candidates, not leaders.
/leader           the acknowledged winner, written once it knows it has won.
                  Ephemeral, so a dead leader does not keep the role on paper.
/epoch            persistent fencing state. Outlives every leadership change.
/snapshot         the fenced result of leader-only work.
```

The standard recipe gives the lowest sequence number the leadership, and Apache's
own recipe notes that being lowest does not prove a process *knows* it has won.
So the acknowledgement is separate: `/leader` is written by the winner, and
`cluster status` reads that rather than counting `/election` children.

## Sessions are not connections

This is the distinction the whole release turns on, and code that misses it is
subtly wrong rather than obviously broken.

A ZooKeeper client that loses its **connection** has not lost its **session**. It
has until the session timeout to reconnect, and its ephemeral nodes survive in
the meantime. kazoo reports three states:

| state | what it means | what a leader must do |
|---|---|---|
| `CONNECTED` | normal | work |
| `SUSPENDED` | connection lost, session may survive | **stop** — leadership is *unknown* |
| `LOST` | session gone, ephemeral nodes deleted | stop, void the epoch, re-enter the election |

Code that handles only `LOST` keeps doing leader work right through a partition,
which is exactly how two processes end up believing they lead at once.

The same distinction appears in the error paths, and getting it wrong here was a
real bug in this release's development. A write failing with `ConnectionLoss` is
*not* a session expiry: ending the tenure there would re-enter the election while
our own still-live `/leader` marker sat in the way, and every later winner would
then fail to acquire it. So `ConnectionLoss` pauses and retries;
`SessionExpiredError` ends the tenure.

One kazoo detail with teeth: cancelling an elected contender **does not interrupt
the function it is running**. The state listener therefore signals a cooperative
loop that checks a flag — it cannot assume the recipe will stop anything.

## Fencing: why electing a leader is not enough

A leadership claim is tied to a session. If that session expires — a long GC
pause, a partition, a slow disk — ZooKeeper elects someone else, and **the old
leader is not told synchronously**. For a while, two processes believe they hold
the role.

Election alone is therefore an *agreement with a lease*, not mutual exclusion.
What makes it safe is a **fencing token** the write itself can check:

```python
epoch = claim_epoch(client, paths)  # CAS on /epoch; keep the version

transaction = client.transaction()
transaction.check(paths.epoch, version=epoch)  # still current?
transaction.set_data(paths.snapshot, payload)
```

The moment another leader claims, `/epoch` advances and every transaction
carrying the old version fails. A deposed leader cannot write, whatever it
believes about itself.

Two details that matter more than they look:

- **kazoo's `commit()` returns a list of results** rather than raising for every
  failure, so each result is inspected. Only a `BadVersionError` from the epoch
  check means "superseded"; anything else is a genuine failure and is raised.
- **A rejected write must leave the snapshot untouched** — value *and* version. A
  fence that rejects but still mutates is not a fence.

And the reason for a single writer, stated accurately. Four coordinators all
writing snapshots would be *wasteful* and would race to overwrite each other —
that is an efficiency argument. What makes the snapshot **correct** is the fenced
write. Do not let the first claim do the second's work.

## Watching two leaders happen

```bash
uv run failover-demo
```

**Stop the Honcho topology first**, and make sure the full setup above is done —
`docker compose up`, `create-topics`, `create-schema` and `cluster init`. The demo
starts three coordinators of its own and waits for one of *them* to win; leaving
the topology running means its coordinators compete in the same election, and the
demo times out waiting for a leader it recognises. It waits for the coordination
tree rather than creating one, so `cluster init` is a prerequisite rather than
something it does for you.

It is **not** part of the routine smoke test, because it waits through a session
expiry and that would be dead time on every CI run.

It makes two observations, deliberately separate:

```text
  leader A: …-99668 at epoch 1
  SIGSTOP 99668 — frozen, not killed
  leader B: …-99670 at epoch 2

  --- the split ---
  ZooKeeper says the leader is   …-99670
  A's own log still says it is    the leader
  Two processes, two beliefs, no race required.

  SIGCONT 99668 — letting A find out
  A has learned its session was lost

  --- the fence ---
  the snapshot now belongs to B, at epoch 2
  submitting a snapshot with A's epoch token 1
  REJECTED: epoch version 1 is no longer current
  snapshot still belongs to epoch 2
```

Why two rather than one: a **stopped process cannot write anything**, so freezing
a leader proves stale *belief* but never a stale *write*. And after `SIGCONT` a
correct client may process `LOST` before its loop runs again, so trying to catch
the write in flight would be racing the scheduler. The fence is therefore tested
directly instead, by submitting the deposed leader's epoch by hand.

Every line above is an acceptance condition — the script exits non-zero rather
than printing its punchline regardless. `SIGCONT` is issued before any teardown
even when a condition fails partway, because a stopped process is invisible to a
process count and still holds its session.

## Registration is session membership, not liveness

Consumers, workers and coordinators each register an ephemeral sequential node,
so the tree shows what is participating without anyone maintaining a list, and
cleans itself up when a session ends.

Say precisely what that buys:

> the tree shows which **sessions currently hold registrations**

Not "what is alive". A wedged or `SIGSTOP`ped process stays registered until its
session expires — as the failover demonstration shows on purpose. Presence and
liveness are different questions and ZooKeeper answers only the first.

Registration is part of **startup**, not an optional extra: a process refuses to
start without it, because one doing work while absent from the registry makes the
tree under-report exactly when something is wrong. After startup it is
survivable — a session loss is retried in the background while the data path
keeps running. ZooKeeper is required for coordinated startup and observable
membership, not for consumer or worker availability afterwards.

## Configuration that changes without a restart

`WORKER_DELAY_SECONDS` lives in a znode. Change it and all four workers pick it
up on their next job:

```bash
uv run cluster set-config --worker-delay 0.05
uv run cluster status
```

```text
  workers     4
    config versions applied: [1]
  worker_delay 0.05 (version 1)
```

All four report the same version, which is the point — see below.

**Nothing in this project was live before 0.7**, and pretending otherwise would
have produced a demonstration that worked only because the process restarted.
Settings were read once at import and captured as default arguments:

```python
def handle_job(..., delay: float = WORKER_DELAY_SECONDS) -> None:   # captured at def time
```

So this release introduces a small thread-safe config object that a worker reads
as it *begins* a job. Three details earn their place:

- **Validate at both ends.** `cluster set-config` checks the value, but anyone can
  write the znode with `zkCli`, so each worker validates independently and keeps
  its last known-good setting if validation fails. A worker also refuses to
  start if no value could be applied at all.
- **Watches are one-shot.** A data watch fires once and must be re-registered,
  and a change can land in between — which is why the refresh re-reads current
  state rather than trusting the event to carry a value. kazoo's `DataWatch` hides
  this; the code here does it by hand, because the mechanism is the lesson.
  (Modern ZooKeeper also offers persistent watches.)
- **Prove it applied.** Each worker reports the applied config *version* in its
  registration, so a check can require all four to report the same version.
  Aggregate throughput would not prove that — it is consistent with one worker
  having noticed.

## What the leader records

```json
{"epoch": 1, "at": 1790514951.09, "pageviews": 133, "pages": 4,
 "jobs_completed": 108, "jobs_distinct": 108, "queue_waiting": 0,
 "queue_consumers": 4, "cassandra_reachable": true, "kafka_committed": 131}
```

A snapshot is a design decision about what is cheap enough to read on a timer.
Cassandra contributes **reachability**, not a row count: 0.5 documented
`COUNT(*)` as a diagnostic that scans every partition, and putting it on a timer
would turn that warning into an application access pattern. RabbitMQ's
*unacknowledged* count is absent because a passive AMQP declare does not expose
it, and adding the management HTTP API for one number is not worth a second
client.

Redis, Cassandra and Kafka are read through connections held for the life of the
process rather than rebuilt per snapshot. Building a Cassandra cluster or a Kafka
admin client every two seconds costs far more than the query, and during an
outage the retry path would block the leader loop for longer than the interval
itself.

RabbitMQ is the exception: `queue_state()` opens and closes an AMQP connection on
each call. It is honest to say so rather than claim a uniformity the code does not
have — and it is the obvious next thing to fix if the snapshot interval ever gets
short enough for it to matter.

`/cluster` serves this snapshot over HTTP — which is why the leader exists at all
rather than merely recording that it is the leader.

## An API over three stores

The API is one more process in the Procfile, so `honcho start` brings it up with
everything else:

```bash
curl -s localhost:8000/cluster
```

```json
{
  "leader": {"identity": "Nicks-MacBook-Pro.local-58525", "epoch": 1, "since": "2026-09-28T21:37:12.840619Z"},
  "snapshot": {"version": 6, "epoch": 1, "at": "2026-09-28T21:37:23.002586Z", "pageviews": 11, "...": "..."},
  "snapshot_matches_leader": true,
  "registrations": {"coordinator": ["..."], "consumer": ["..."], "worker": ["..."]},
  "observed_at": "2026-09-28T21:37:24.118205Z"
}
```

[localhost:8000/docs](http://localhost:8000/docs) is the generated OpenAPI page,
which is most of why this is FastAPI rather than Flask: request validation and a
published contract, both derived from the type annotations rather than written
twice.

One endpoint, one store:

| endpoint | store | nothing there | store down |
|---|---|---|---|
| `GET /health` | none | — | — |
| `GET /counts/pages?page=` | Redis | `{}` | 503 |
| `GET /users/{user_id}/last-page` | Redis | **404** | 503 |
| `GET /users/{user_id}/events?limit=` | Cassandra | `[]` | 503 |
| `GET /cluster` | ZooKeeper | leader `null` | 503 |

`/health` is liveness only: it touches no store and never waits for a thread, so
a stalled store cannot make the process look dead when it is merely busy.

**404 for a last page, `[]` for events.** The last-page resource *is* the value
Redis holds, so when there is no value the resource is absent. Events are a
collection, and Cassandra cannot tell an unknown user from one with no events.
Restart Redis and the two disagree about whether they know a user — and both
are right, because one is a deliberately volatile view (0.4) and the other a
durable one (0.5).

`limit` is bounded in the contract (1 to 100, default 20), not merely in the
query, and anything outside it is a 422. "One partition" does not make an
arbitrarily large partition read safe.

`/counts/pages` counts **named pages**: up to twenty `page` parameters, defaulting
to the producer's four-page catalogue, read in one `MGET`. A page with no counter
is left out. The first version discovered pages instead, with `SCAN` — and
`SCAN` visits every key and filters afterwards, including one last-page key per
user, so the route's cost grew with *users* rather than pages: 381 keys took four
round trips. That is the same trap as a user listing, so it goes the same way.
Discovering arbitrary pages cheaply would need a different data model — a hash
of counters, say — which is a write-path change this release does not make.

What is deliberately *not* here: no writes, because an HTTP producer would be a
second entry point with none of Kafka's partitioning, ordering or replay; no
fan-out reads, which would couple one response's availability to every store;
no cache, because Redis already is one; no user listing, which Cassandra cannot
answer without scanning; and no correct durable total. `/counts/pages` lets you
sum the counters, and its OpenAPI description says what that sum is worth after
a replay.

The API is a host process like everything else in the Procfile, so it has one
address, `localhost:8000`. There is no `api:8000`: Compose DNS names only the
services Compose runs, and nothing inside the network is a client of the API.
It is 0.1's lesson in reverse — the second address exists only for things
Compose runs.

## What each store can actually prove

The rule for every response: **expose only metadata the source genuinely owns.**

- **Redis** keeps no update timestamp and no consumed-offset watermark, so it
  cannot state its own age, and the API does not invent one.
- **Cassandra** does have per-row metadata. `written_at` is `writetime(page)`,
  **the page cell's write timestamp**. The driver generates it client-side, so it
  records when the consumer *issued* the write, by the consumer host's clock —
  not when Cassandra persisted anything. After a replay it advances while the
  event itself stays put: 0.5's upsert, visible over HTTP.
- **ZooKeeper** carries the snapshot's `at` and `epoch`, which let a client
  detect staleness rather than be promised freshness.
- **`observed_at`** is the API's own read time, labelled as exactly that.

The driver returns *naive* datetimes that are UTC, and serialising one as-is
silently drops the offset. Every timestamp here is converted to UTC-aware first,
and `written_at` (microseconds since the epoch) with integer arithmetic rather
than through a float. Checked against the stored row rather than trusted:

```text
API      event_time 2026-09-28T21:39:55.312000Z  written_at 2026-09-28T21:39:55.316788Z
cqlsh    event_time 2026-09-28 21:39:55.312000+0000  writetime 1790631595316788
```

## `/cluster` is several reads, and says so

`/leader`, `/snapshot` and the registrations are separate reads, and they can
span a handover. The response may legitimately report leader B while the latest
snapshot still belongs to A, so it carries `snapshot_matches_leader` rather than
retrying until the answer looks tidy. Two different things produce `false`: read
skew across the separate reads, and a genuine transitional state in which B
leads but has not written yet. Both are true of the system, so neither is hidden.

It is **`false` whenever either side is absent**, not only when both are present
and differ. `cluster init` pre-creates the snapshot as `{"epoch": null}`, so with
nobody leading, a naive comparison is `None == None` — a match on a cluster with
no leader.

Watched through `failover-demo`, polling every 50ms, it was less dramatic than
the design allows, and more instructive. The transitional `false` during the
handover was **missed**: B's leadership and its first snapshot arrived between
two polls. What did show up was `false` at the start, with no leader and a
snapshot left over from an earlier epoch — and **`true` for the ten seconds
leader A was frozen**. A was still the leader on record, and the latest
snapshot was its own. A match tells you whose snapshot it is, not how fresh it
is. That is what `at` is for.

## Freezing a dependency

The interesting failure is not a store that goes away but one that stops
answering. `docker compose stop` closes the socket, and clients fail at once;
`docker compose pause` freezes the container with its sockets open, so requests
hang instead. Measured with the API under load — 24 clients hammering the
affected route while others kept using the rest:

| store | affected route | the other routes | recovery |
|---|---|---|---|
| Redis stopped | `503 redis unavailable` in milliseconds | unaffected | on the next request |
| Redis paused | 8 requests admitted, each `unavailable` after 1.0s; the rest `busy` at once | unaffected | immediate |
| Cassandra stopped | `unavailable` in milliseconds | unaffected | 12s after restart, as the driver reconnects |
| Cassandra paused | 8 admitted, each `unavailable` at the 2.0s deadline; the rest `busy` | unaffected | immediate |
| ZooKeeper stopped | `unavailable` without a request reaching kazoo | unaffected | 2.5s |
| ZooKeeper paused | 4 admitted, timing out after 1.0s and *still held*; the rest `busy`; after 6.6s kazoo notices and everything is `unavailable` | unaffected | 0.1s, on a new session |

Two different 503s, deliberately:

```json
{"detail": "cassandra busy"}          refused at admission; Cassandra was never called
{"detail": "cassandra unavailable"}   the call failed or ran out of time
```

Busy means back off; unavailable means the store is the problem. And in every
case the API recovered without being restarted: it holds one client per store
for the life of the process, and the clients reconnect on their own.

## A timeout ends the wait, not the work

Keeping the other routes unaffected took more than a `timeout=` argument.

The clients are synchronous, so every store call runs on a worker thread, and by
default they all share one allowance of 40. Forty requests stuck on a frozen
Cassandra would then delay Redis requests while Redis is healthy. So each store
has a **bulkhead** — its own admission limit and worker allowance — and its
promise is exactly this wide:

> A stalled store cannot consume another store's worker allowance. CPU, memory
> and the event loop remain shared.

| store | limit | why that size |
|---|---|---|
| Redis (both routes) | 8 | one pooled connection per in-flight call |
| Cassandra | 8 | the driver multiplexes; this bounds waiting threads |
| ZooKeeper | 4 | one session on one connection, served in order |

These are tutorial defaults, not derived numbers — a reasonable place to start
measuring. Ordinary traffic leaves plenty of headroom: two pollers per route saw
no refusals, at a p95 of 7ms (21ms for `/cluster`). Bursts far past the limits
produced nothing but `busy` refusals, and every route answered normally straight
afterwards. The caveat in the promise is real, though: at 64 simultaneous
requests a refusal reached the client in about 40ms, at 800 in up to 2s. A
refusal costs no store time, but it still waits its turn on the one event loop.

The two Redis routes share one allowance because they share one client, one
pool and one server. That buys isolation between *stores*, not fairness between
*routes*: in a burst, `/counts/pages` took all eight slots and every
`/last-page` request was refused.

Two measurements show why the work, not just the wait, has to be bounded.

**redis-py's defaults take a minute to fail.** Against a server that accepts a
connection and never replies — what `pause` produces — one default `GET` took
**57.9s**: a 5s socket timeout and ten retries with backoff. The API sets a 1s
timeout and no retries, so a restart can cost one transient 503, which the
contract allows. A fresh connection is also more than one exchange: redis-py 8.1
sends `HELLO`, `CLIENT MAINT_NOTIFICATIONS` and two `CLIENT SETINFO` before the
command, so one command's worst case is the connect timeout plus five socket
timeouts. Each Redis route is a single command, one `MGET` or one `GET`, and
redis-py cannot be interrupted mid-command, so a read can finish after the
route's 2s deadline. When it does, the answer is a 503 rather than a late
success — the same rule as every other store, applied in one place.

**Abandoned ZooKeeper reads hide the outage.** kazoo's synchronous calls take no
timeout, so the API sends each read asynchronously and waits with one. But a
timeout ends the *wait*: kazoo keeps the request. Against a paused ZooKeeper,
with a read abandoned every second or so:

| admission released | held by kazoo after 20s | kazoo's state |
|---|---|---|
| when the caller gives up | 16, one more per request | `CONNECTED` throughout |
| when the request completes (limit 4) | 4 | `SUSPENDED` after 10.4s |

kazoo detects a dead server by an unanswered heartbeat, and it only sends one
after a quiet spell — and **it counts every request it sends as a heartbeat**,
reply or not. So a caller abandoning requests and sending new ones keeps the
client from ever noticing, while the abandoned reads pile up. Holding admission
until each request truly completes caps the pile at the limit, stops the traffic
that was masking the outage, and kazoo notices — one read timeout, two-thirds of
the session timeout, after the last request it sent. In that probe the four
slots filled one read at a time, so it took 10.4s; under the API's load they
filled at once, and it took 6.6s.

The same rule holds for the other stores, with different consequences. redis-py
disconnects on a timeout, so nothing lingers on the client. A timed-out
Cassandra request becomes an *orphaned stream* on its connection — the thread is
free, but the server may still run the query when it resumes — and the paused
run peaked at 112 of them, about eight every two seconds, all gone after
recovery.

## Admission, precisely

This is the part that needed the most care, and the part most likely to be
wrong in a first attempt.

Each admission is a small **permit**, and the permit releases its slot exactly
once, when every holder has let go. There are up to three holders:

- the **dispatcher**, until its `run_sync` call has returned;
- the **worker thread**, until the read returns *in that thread*;
- a **ZooKeeper request** the read gave up on, until kazoo completes it.

```python
permit = self.admit()  # or 503 busy, at once
worker = _WorkerHold(permit)  # the thread's own hold
try:
    result = await anyio.to_thread.run_sync(
        worker.run, read, budget, limiter=self._workers, abandon_on_cancel=False
    )
    budget.remaining()  # finished past the deadline: unavailable, not late
finally:
    worker.dispatcher_leaving()  # gives the hold up only if the thread never ran
    permit.dispatch_finished()
```

- **Why not release a semaphore directly?** A late duplicate release goes
  unnoticed. `BoundedSemaphore` complains only when its count would exceed its
  starting value, so a duplicate arriving after another request has taken the
  returned slot is accepted silently — freeing a slot that request is still
  using. Duplicates are not hypothetical: kazoo's `rawlink` re-dispatches every
  callback on a result that has already completed.
- **Why a thread-safe semaphore at all?** Admission happens on the event loop,
  but a kazoo completion arrives on kazoo's own thread.
- **Why does the thread hold the permit itself?** A thread cannot be
  interrupted, so a cancelled request must not give its slot back while its call
  is still running. `abandon_on_cancel=False` makes anyio's own cancellation wait
  for the thread — but not asyncio's. Measured: a native `Task.cancel()`
  interrupted the await at once, and anyio returned the *worker token* while the
  thread ran on. So the limiter keeps each store's threads out of the shared
  default, and the permit, held by the thread, is what caps calls into the store.
  If the task is cancelled before its thread has started, the read never runs
  and that hold is given up instead.
- **Why the dispatcher's hold too?** On the ordinary path it is dropped after
  `run_sync` returns, when anyio has already released the worker token, so an
  admitted request does not wait for a thread.

Each of those is a unit test, and each test was checked by reintroducing the
defect it guards against and watching it fail. That is not the same as complete:
the first version passed all of them and still let native cancellation through,
which a review found, not a test. The decisive test holds every Cassandra slot
at a barrier: the next Cassandra request is refused as busy *without the store
being called*, Redis and `/health` still answer, and Cassandra's capacity
returns when the barrier opens.

## Starting, and stopping

The API connects to all three stores before it serves anything, as every other
process here does, and retries while a store is still coming up — but three
times, not the pipeline's ten. A shared retry helper does not need identical
patience: the documented workflow already waits for the stores to be healthy,
and ten attempts against a paused ZooKeeper left the API without a listener for
three minutes. Each attempt is bounded, so a store that never answers fails
startup rather than hanging it. Observed with each store paused — durations for
that failure mode, not a universal worst case, since a store answering slowly
takes a different path:

| paused | ten attempts | three attempts |
|---|---|---|
| Redis | 37.4s | 9.5s |
| Cassandra | 127.6s | 36.5s |
| ZooKeeper | 178.0s | 66.8s |

Two three-second sleeps account for 6s of each; the rest is the attempts, which
take about 1s for Redis, 10s for Cassandra and 20s for ZooKeeper.

It connects *before* uvicorn starts, which is a detail with a measurement behind
it. uvicorn captures SIGTERM and SIGINT for its whole run, including the
lifespan's startup, and acts on them only once startup has finished. With the
connection inside the lifespan, a SIGTERM five seconds into a startup stalled on
a paused Redis took effect **32.5 seconds** later. Connecting first leaves both
signals with their ordinary meaning until there is something to serve.

Ownership then passes to the lifespan, which closes the clients on shutdown —
but only once the lifespan is actually running. uvicorn can exit before it ever
starts one: with `WEB_CONCURRENCY=2` set it refused to run workers for an app
object and exited, and a first version of the hand-over had already given the
clients away, so nothing closed them. Until the lifespan takes them, `main`
still owns them. And `workers=1` is passed explicitly, so that environment
variable cannot turn the single process into several.

uvicorn runs in-process — no `--reload`, which adds a file-watching parent, and
no `--workers`, which adds children. After 0.3's leak history, a supervisor
inside a supervised process is the last thing this topology needs. Once
serving, it starts in about 0.4s and exits about 0.2s after SIGTERM.

## Live: notifications, not state

```bash
uv run honcho start
open http://localhost:8000/live
```

The page updates without a refresh. Each consumer, once it has applied an event,
announces it on a Redis channel; the API holds **one** subscription and fans it
out to every open page over a WebSocket at `ws://localhost:8000/ws/pageviews`.

That makes pub/sub the third messaging pattern in the series, and the same verb
with opposite guarantees:

| | Kafka (0.2–0.3) | RabbitMQ (0.6) | Redis pub/sub (0.9) |
|---|---|---|---|
| shape | log | queue | broadcast |
| who gets a message | one consumer per partition, per group | one competing worker | every current subscriber |
| a subscriber that was away | resumes from its offset | finds the work waiting | has missed it |
| per-publication delivery | durable, replayable | at-least-once (redelivery) | **at-most-once** |
| what publishing returns | a broker ack | a publisher confirm | **how many subscribers were listening** |

`PUBLISH` returns an integer and keeps nothing. With the API stopped it returns
0 — measured with `PUBSUB NUMSUB` while the API was down — and that is normal,
not an error: nobody was listening, so nobody was told.

So the page treats a notification as a **hint that something changed**, never as
the state itself. It reads `/counts/pages` when it hears one, and the
notification carries no count: four consumers publish concurrently, so values
for one page could arrive out of order, and a Redis restart resets every counter
anyway. 0.4's lesson in a new place — announce that something changed, and let
the reader fetch the value.

Two levels of delivery, kept apart as 0.6 kept RabbitMQ's redelivery apart from
upstream republication:

- **Per publication, per subscriber: at most once.** Redis's own contract.
- **Per event: zero, one or several times**, because a Kafka replay applies an
  event again and publishes it again.

The publish is the handler's last write, so a page that reads state when it
hears one reads the state it was told about — and it is **best-effort**. An
*unconfirmed* publish is logged once and swallowed — unconfirmed rather than
failed, because after a timeout the `PUBLISH` may have happened and only its
reply been lost. Failing the handler instead would leave the offset uncommitted,
and the replay would run the `INCR` again, so a lost notification would corrupt
a counter. *A write whose loss is harmless must not
cause a replay that is not.* It also has a client of its own, with half-second
timeouts and no retries, because the consumer's other writes keep redis-py's
defaults and those took a minute to fail at 0.8. Measured: 264µs per event warm
(the `INCR` and `SET` before it take 573µs); 0.5s per event against a paused
Redis; and 2.26s for one cold notification against a Redis answering just inside
the timeout, because a fresh connection is five exchanges, not one.

## What the offsets can and cannot prove

Each notification carries its event's Kafka **partition and offset** — metadata
the pipeline genuinely owns. For this topic a partition's offsets are contiguous
(checked: every partition from 0, no gaps), so the page can notice some of what
it missed. Its tracker, per partition:

| it sees | it reports |
|---|---|
| the first offset | a baseline — not evidence of anything |
| the next one | nothing |
| a jump past the highest seen | the **unseen range** — not a count of losses, since some may still arrive late |
| an offset it remembers | **previously observed**: a replay published it again |
| an offset below the highest, not remembered | **older offset** — late, or a rewind, not a proven repeat |

The highest offset seen never moves backwards (received as `8, 10, 9`, the 9 is
late, not a duplicate), the memory of recent offsets is bounded, and the tracker
lives for the whole page session, so a reconnect is not a new baseline. What it
can never show: the **first** notification lost, the **last** one lost, and
anything across a recreated topic.

The tracker does arithmetic on partitions and offsets, so the bridge relays only
values from 0 to 2^53 − 1 — the largest integer JavaScript represents exactly;
above it a browser reads a JSON number as an approximation, or as `Infinity`,
and one such offset would poison a partition's high-water mark. That is **this
notification format's limit, not Kafka's**: Kafka offsets are 64-bit and can
exceed it, and carrying them would take another representation, such as decimal
strings.

Measured against the real pipeline:

- **A consumer crash.** Forty events, a consumer crashing with five uncommitted,
  then a second consumer: 45 notifications for 40 distinct events, and the
  tracker marked exactly **5 previously observed** — 0.3's replay, seen live.
- **The API restarted** for eight seconds with the topology running: no
  subscribers on the channel meanwhile, and on reconnect the tracker marked the
  unseen ranges per partition — `p0 167–170`, `p1 141–142`, `p2 101–102`, eight
  offsets for about eight events.
- **The subscription killed** with `redis-cli CLIENT KILL TYPE pubsub`, and
  **Redis restarted**: the tracker's unseen range matched the offsets actually
  never received, exactly, both times.

Notifications make the page responsive; **reconciliation** keeps it correct. A
lost *last* notification shows in no offset, so the page also reads its counts
after every reconnection, after every `resubscribed` notice, and every thirty
seconds while it is visible — one read at a time, throttled to one a second,
retried with backoff — and shows the time of its last successful read. Thirty
seconds is a refresh schedule, not a freshness guarantee: during an outage every
read fails, and the last-read time is the honest signal.

The tracker and that scheduler are JavaScript, and they are the release's
central claims, so they have tests of their own — `node --test "tests/js/*.test.mjs"`,
against the very module the page imports. The decisive one suppresses the final
notification without disconnecting anything, and requires the counts to catch
up through the periodic read alone.

## One subscription, and what it can prove about itself

The API's subscription is one `redis.asyncio` task, not a thread: a thread would
hand every message to the event loop through an unbounded queue of callbacks. In
one task nothing in the application grows without bound — if fan-out fell behind,
reads would slow, Redis would buffer on its side, and its own limit for pub/sub
clients (`32mb 8mb 60` by default) would evict the API, which shows up as an
interruption rather than as memory. That removes the *application's* backlog,
not every buffer: the kernel's and the client's own reader still hold whatever
has arrived.

What the bridge can prove is narrow: when it *knew* it was not listening. It
tells every page `{"type": "interrupted", "detected_at": …}` and later
`{"type": "resubscribed", "detected_at": …, "resubscribed_at": …}` — "detected",
because noticing is not proof of when the connection went. Three things the
client would not do for it, each found by measurement or by reading its source:

- **Its health check is not a failure detector.** `health_check_interval` sends
  a PING and never waits for the answer. So the bridge sends its own, one at a
  time, each with a fixed deadline: against a paused Redis it declared the
  subscription lost 11.9s in, inside its 15s bound.
- **Its reconnection can be silent.** On a failed read, redis-py's async retry
  calls a reconnect that resubscribes inside the client *before* raising — even
  with zero retries. So the subscription client's retry supports no errors at
  all, and an unexpected subscribe acknowledgement counts as a resubscription
  anyway. (The first version handed the async client redis-py's *synchronous*
  `Retry`, which returns the coroutine before it runs and so catches nothing. It
  behaved correctly only by accident; review caught it.)
- **Under RESP3 a subscription's PING reply comes back mangled** —
  `{"type": "h", "channel": "b", "data": "-"}`, the payload `hb-1` indexed as if
  it were a list. Under RESP2 it is a proper `pong`, so the subscription speaks
  RESP2.

Recovered means *acknowledged*: the bridge is back when Redis confirms the
subscription, with one deadline covering connect, negotiation and confirmation
together. After `CLIENT KILL` it noticed in 0.27s and was back 0.5s later; after
a Redis restart, 0.20s and 0.5s. The sandbox's bridge logged an exception and
stopped, leaving every page on a socket that would never speak again; this one
backs off and resubscribes for as long as the process runs.

## Fan-out, and the slow socket

The broadcaster is a plain function, not a coroutine, so it *cannot* wait for a
socket. Each socket has a bounded queue and a sender of its own; a socket whose
queue fills cannot keep up, so it is removed from fan-out at once and closed
with 1013, "try again later" — the way Redis treats a slow subscriber, and would
treat the API. It keeps its admission slot until that cleanup has finished, so
churn cannot pile up closing sockets beyond the limit of 100. What this detects
is transport backpressure, not how fast a page's JavaScript runs.

Measured: a flood of 20,000 notifications reached each of five sockets in full,
with no evictions, while `/health` stayed at a median of 1.7ms (0.5ms quiet).
SIGTERM with sockets open still exits in about 0.2s; uvicorn closes each one
with 1012, "service restart", and the page reconnects with backoff like any
other close.

One bug from building it is worth the space. A first version served each socket
with `asyncio.wait` and `asyncio.gather`, and a cancellation arriving while
`gather` waited for tasks it had just cancelled leaked out of anyio's cancel
scope — Starlette runs the endpoint under anyio. An anyio task group fixed it,
and gave eviction its ordering for free: the group exits only once the sender
has stopped, and a WebSocket must not be sent to and closed at the same time.

**`Origin` is checked exactly** — scheme, host and port against the API's own
origins — and a missing or `null` origin is refused. WebSockets are not covered
by CORS, so without this any page open in the same browser could read the
stream, and binding to 127.0.0.1 does not help, because the browser is on
127.0.0.1 too. It protects against browsers only; any other client can forge
the header. Refusals, like a full allowance, happen at the handshake, which is
an HTTP rejection rather than a close code — a browser may only see 1006 — so the
page answers every failure to connect the same way. Event fields come from
Redis, which anyone with access can publish to, so the page renders them with
`textContent`, never as HTML — and the bridge relays only notifications of the
right shape and size with a finite timestamp, because Python's `json` accepts
`NaN` and `Infinity`, which are not JSON, and a browser's `JSON.parse` throws on
them.

## Flink: counting in event time

```bash
uv run create-topics          # now also creates pageview_windows
uv run flink-job submit
uv run producer               # or the whole topology: uv run honcho start
uv run windows
```

```text
  …
  2026-10-08T18:42:10Z  /            4
  2026-10-08T18:42:10Z  /docs        4
  2026-10-08T18:42:10Z  /pricing     1
  2026-10-08T18:42:20Z  /            4
  2026-10-08T18:42:20Z  /checkout    5
  2026-10-08T18:42:20Z  /docs        1

  297 results; 0 repeated an earlier one
```

While the job is running it also prints how many late records the job has
dropped, read from Flink's REST API — or *unavailable*, when that cannot be read.
It reads the topic up to the end it had when it started, under a deadline, and
says so if it did not get there: with results still arriving, a read that waited
for the topic to go quiet need never end. The read has a 15-second budget,
started before connecting. Connecting and finding the topic's partitions have
fixed bounds of their own, chosen to fit inside it; every later call gets what
is left; and closing the consumer is bounded separately, on top. That is a
budget with every step bounded, not a hard end-to-end guarantee. Against a
broker paused at each step in turn, the read gave up in 5 to 15 seconds — with
a clear error when it had nothing to show, and as an incomplete read when it
had begun.

A Flink job reads `pageviews`, counts views per page in ten-second windows of
**event time** — when the views happened, by the producer's timestamp, not when
Flink processed them — and writes each window's result to `pageview_windows`.
The dashboard is at [localhost:8081](http://localhost:8081): the job graph, each
partition's watermark, checkpoints, backpressure. Nothing reads the results yet
except `uv run windows`; 0.11 wires them into the live page.

The job is Flink SQL, run through PyFlink, using the window table-valued
function:

```sql
INSERT INTO pageview_windows
SELECT window_start, window_end, page, COUNT(*) AS `views`
FROM TABLE(TUMBLE(TABLE valid_pageviews, DESCRIPTOR(event_time), INTERVAL '10' SECOND))
GROUP BY window_start, window_end, page
```

(`views` is quoted because `VIEWS` is reserved in Flink SQL.)

It is the first real client of the internal Kafka listener: Flink runs in
Compose, so it reaches the broker at `kafka:29092`, the address 0.2 built and the
smoke test has proven every release since. A job is *submitted* to Flink rather
than run under Honcho, and a Python job's graph is built by running its driver,
so submission happens inside the jobmanager container:

```bash
docker compose exec flink-jobmanager flink run -d -py /opt/pipeline/flink_job.py
```

`uv run flink-job submit` does exactly that; `uv run flink-job status` and
`uv run flink-job cancel <id>` complete it. Cancel by id, never by name.

**Every submission starts from the beginning of the topic** and writes every
window again, so cancelling and resubmitting repeats earlier results on
`pageview_windows` — `uv run windows` counts the repeats. That is deliberate. A
fresh job that resumed from the consumer group's committed offsets instead
would start past events whose windows were still open, with none of their
partial counts, and emit those windows short — silently. Restoring state and
offsets *together* needs a retained checkpoint and an explicit restore, which is
0.11's submitter; this release does not pretend a resubmission can do it.

## Watermarks: when is a window done?

Event time needs a rule for when a window is complete, because an event for it
could in principle still be on its way. That rule is the **watermark**: a claim
that no earlier event is still to come. This job's watermark is the newest event
time seen, minus two seconds.

Two precisions that are easy to get wrong:

- **An event is dropped as late only if its window has already been emitted** —
  the watermark had passed that window's end. An event merely out of order still
  enters a window that is open. The two-second delay keeps windows open that much
  longer.
- **Progress is set by the minimum watermark among a source's active
  partitions**, not by how many events each partition carries. 0.3's routing
  skew makes a lagging partition plausible, but measured over two runs no
  partition reliably set the clock: partition 3, the quietest, held the minimum
  in 25 of 57 samples in one run and 12 of 57 in the next.

A partition with nothing to say holds everything back, so the job marks a
partition **idle** after ten seconds without records, and stops waiting for it.
Measured with the producer at its default one event a second:

| idle timeout | a window's result arrives after the window's end |
|---|---|
| 10s | 8.2s median, 12.6s at most |
| off | 11.2s median, 19.7s at most |

The price of idleness is that it is decided on *processing* time: a partition
that resumes after being declared idle can deliver events whose window has
already been emitted, and they are dropped.

**Late events are dropped silently.** SQL gives them no side output, so the only
witness is a metric, `numLateRecordsDropped`, which `uv run windows` reads from
the REST API. Measured: an event sent after its window's result appeared changed
nothing on the output topic, and the metric rose by one. Two details from getting
that measurement right. The planner splits the aggregation into a local phase
chained onto the source and a global phase downstream, and the metric belongs to
the global one. And the REST API serves metrics from a cache refreshed every few
seconds, so a single reading straight after an event can still show the old
value — the measurement polls until the change appears. A metric that cannot be
read is shown as *unavailable*, never as zero.

## What the job accepts

`json.ignore-parse-errors` sounds like "skip bad records", and it is not quite:
a field it cannot parse becomes `NULL` in a row that is kept, and a missing field
is `NULL` by default. Left alone, a record with no page would be counted under a
`NULL` page. So the job names what a valid row needs — a page, and a timestamp
this pipeline could have produced — and filters before counting.

The timestamp needs more care than a filter, because it feeds the watermark
before any filter runs. A missing one would make a null row time, which Flink
refuses; one too large would overflow the conversion; and one in the future,
however plausible-looking, can push the watermark ahead and make every later
window late. So the event-time expression accepts a timestamp only if it is
after 2000 and **no more than a minute ahead of the moment the job reads it** —
our producer stamps events as it creates them — and maps anything else to the
epoch, which can never advance the watermark; the filter then excludes it.

The first version used a fixed range instead, 2000 to 2100, and that was not
enough: 2099 is inside it. Measured, a 2099 event on every partition made the
next two windows late and dropped twelve valid records; with the one-minute
rule, the same input dropped none. The rule reads processing time, so it can
decide differently on a replay: an event rejected as "future" when first read
becomes acceptable once its time has passed.

Measured with the rule in place, one window with eight valid events and nine bad
records — malformed JSON, a missing page, an empty page, and a missing, null,
non-numeric, far-future, negative and `1e400` timestamp: the result was exactly
eight, and the job never failed.

## State, checkpoints, and what a restart repeats

A window is state: the counts so far for every open window. Two settings decide
what a failure does to it, and they are easy to run together:

- **Checkpointing is `EXACTLY_ONCE` inside the job.** Every ten seconds Flink
  snapshots window state and source offsets together, to a volume both Flink
  containers share, and restores both on failure. A restored count holds each
  event's contribution once.
- **The sink is `at-least-once` at the edge.** Results written after the last
  completed checkpoint can be written again after a restore.

Consistent counts, possibly repeated output records. Measured with the failure
point controlled: a completed checkpoint; a window's result (`/docs`, 8) emitted
after it; the taskmanager killed before the next checkpoint. The job restored from
that checkpoint 20.9s later and wrote the same result again.

| write | on recovery |
|---|---|
| Flink window result → Kafka (exactly-once state, at-least-once sink) | results since the last checkpoint written again; identical when the same events are accepted, which the timing policy does not guarantee |

That last clause matters. `COUNT(*)` is deterministic for the same *accepted*
events, but idleness is decided on processing time, so an event excluded in the
first run could be counted in the replay. The repeat above was identical because
the input was controlled. A downstream writer keyed by page and window — 0.11's —
makes a *repeated* result harmless, the way 0.4's `SET` and 0.5's upsert do. It
does not choose between two *different* ones.

Exactly-once output exists — Kafka transactions committed on checkpoint, with
results visible only once a checkpoint completes and consumers reading
`read_committed` — and this release does not need it. Its only reader is a person
at a terminal.

**What recovery does not cover:** a taskmanager failure restarts the job from its
checkpoint, but a *jobmanager* restart, in this session cluster without high
availability, loses the job. Measured by accident: recreating the Flink
containers after an image rebuild left the cluster knowing nothing of the job.
Nor were its checkpoints left behind to restore from: by default Flink deletes
a job's checkpoints when it stops, and what remained on the volume was empty
directories. Nothing resubmits the job — that is 0.11's submitter, which will
have to retain checkpoints deliberately before it can restore from one.

## Pinned hard

Flink is the most version-sensitive software in the tutorial, so it is the one
exception to readable tags. Every input is fixed and recorded in
`flink.Dockerfile`:

| component | version | why |
|---|---|---|
| Flink | 2.2.1, Java 17, Ubuntu 24.04 | by the image's multi-arch **index** digest, so both amd64 and arm64 resolve. The tag moves: the 2.2.1 images were rebuilt on 2026-10-02 |
| Kafka SQL connector | 5.0.0-2.2 | Apache lists 5.0.0 as compatible with Flink 2.1.x and 2.2.x; there is none for 2.3, which is why this is not 2.3.0 |
| its integrity | SHA-256 `5605c691…62c5616` | Maven Central publishes only SHA-1 and a PGP signature. The signature was verified once against Apache Flink's `KEYS` — key `CC33 2388 50B5 A926 24ED 7F62 16AE 0DDB BB2F 380B` — and every build checks the hash |
| Python | 3.12, from Ubuntu | the minor version is fixed by the base image; the patch level is whatever the archive serves when the image is built |
| PyFlink | the copy Flink ships | needs `typing_extensions` to import and `ruamel.yaml` to execute statements — an import-only check misses the second |
| Kafka client inside the connector | 4.2.0 | against the 4.3.1 broker: clients work with newer brokers |

The host never installs PyFlink. The job's statements are built by plain
functions the host imports and tests; `pyflink` is imported only inside the
container. That keeps Java and a several-hundred-megabyte wheel off every
reader's machine.

The cost of all this is real: the image is 1.67GB, and the two Flink containers
take about 2GiB of memory between them at rest.

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
| ZooKeeper | coordination state | persistent znodes survive; **ephemeral ones die with their session**, which is the point of them |
| Flink | a stateful job's checkpoints | a volume, shared by both Flink containers, so a running job survives a taskmanager failure. Not beyond the job: Flink deletes checkpoints when a job stops, and the job itself does not survive a jobmanager restart without high availability |

Cassandra is still *derived* from the Kafka log — everything in it can be
rebuilt by the same replay that rebuilds Redis. Its volume changes how durable
and how queryable the view is, not where the truth lives.

ZooKeeper needs two named volumes rather than one: the official image keeps
snapshots in `/data` and the transaction log in `/datalog`. Its ephemeral nodes
are *not* meant to survive — dying with their session is what makes them useful
for membership and leadership.

This is a single-node ZooKeeper, deliberately. It demonstrates client
coordination semantics — sessions, watches, ephemeral ownership, fencing — and
says nothing about the availability of a replicated ensemble, which is a separate
subject with its own failure modes.

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

Host processes use `localhost:9092`. Containers use `kafka:29092` — Flink does,
from 0.10, and the smoke test has proven the path since 0.2.

```bash
# From the host
uv run smoke-test

# The internal address, from inside the network
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --list
```

RabbitMQ follows the same pattern: `localhost:5672` from the host,
`rabbitmq:5672` from inside the network, with the management UI published
separately at [localhost:15672](http://localhost:15672). ZooKeeper is
`localhost:2181` and `zookeeper:2181`. Flink's dashboard and REST API are
`localhost:8081`; its jobmanager and taskmanager reach Kafka at `kafka:29092`
from inside the network. The API has only
`localhost:8000`, because it is a host process and no container is its client.

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

ZooKeeper appears in this tutorial at 0.7 — coordinating the pipeline's own
processes, which is a different job from storing Kafka's metadata. That
separation is the point: a reader meets ZooKeeper as a coordination primitive
they chose, not as infrastructure another service dragged in.

## Testing

Unit tests need no broker.

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest
node --test "tests/js/*.test.mjs"
```

The last one is a separate step, in CI too, rather than something `pytest`
shells out to: a test that silently skips when Node is missing is a check that
did not run.

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
PASS  honcho topology: 4 consumers own 4 partitions; committed offsets advanced 128 to 230; 4 counters, 237 last-page values; rows written to smoke_7656670c; 4 workers, 238 jobs completed; one leader at epoch 1 among 3 contenders; 4 consumers and 4 workers registered; API on port 57817 read back the Redis, Cassandra and ZooKeeper sentinels and states its 404 and 422 contracts; leader …-30351 wrote again at epoch 1 (snapshot v26 -> v27); all 4 workers applied worker_delay=0.03 at config version 1; a known event reached a subscribed socket (partition 1, offset 82); foreign and missing origins refused; nothing was left running
PASS  flink windows: job 5d6d7cb7 counted {'/docs': 12, '/pricing': 8} in window 2026-10-09T15:34:50Z on its own topics; 1 checkpoint(s) completed; cancelled

all 5 checks passed in 26.8s
```

The Flink check is isolated the same way: topics, consumer group and job of its
own, cancelled afterwards by the job id it captured — never by a name a reader's
job could share. When a submission's outcome is uncertain (a timeout is not a
rejection), it reconciles by the run's own unique job name, cancels whatever it
finds by id, and deletes its topics only once every job of the run is confirmed
stopped — never the inputs underneath a job that might still run. Finding
nothing is not confirmation: the `flink run` inside the container can outlive
the client that gave up on it, so after twenty seconds without a job the topics
stay, and the check says so. A job that stopped by itself is not cancelled —
Flink refuses to cancel a stopped job — but it does fail the check, because a
streaming job should not stop. Eight task slots leave room for it beside a
reader's job.
It sends events with controlled times into one window on all four partitions,
then advancement records on every partition beyond the window's end *plus* the
watermark delay, with idleness disabled so closure never depends on a
processing-time timeout; it requires exact counts and at least one completed
checkpoint, because correct output and `RUNNING` can both come before any
checkpoint, and an unwritable checkpoint directory would otherwise pass.

On a brand new cluster you will also see `NotCoordinatorError` once or twice
above these lines, for the same reason a first consumer does: Kafka is creating
the internal offsets topic for the first group that asks for it. It retries and
settles, and a second run is quiet.

The last check starts the real Procfile topology, waits for four group members to
own four partitions, confirms committed offsets advance, and stops it again. The
other three build their own clients, so they would all pass with a broken
Procfile.

It runs against a topic, consumer group, Redis key prefix, Cassandra keyspace,
RabbitMQ queue and ZooKeeper root created for that run alone, all handed to
Honcho through the environment. Sharing `pageviews` and the `pipeline` group
would let a topology you happen to have running satisfy the check — and a live
`honcho` process is not evidence that the members being observed are its own.
Readiness is a list of predicates rather than a fixed sequence, because the
topology now has a *state* and not merely a process count. All of these must
hold, under one shared deadline:

- four Kafka group members owning four partitions, with committed offsets
  advancing;
- both Redis branches written, counters *and* last-page values;
- Cassandra rows present with their key columns populated;
- exactly four workers consuming the run's queue, and jobs completed;
- exactly one acknowledged leader among three contenders, three coordinator
  registrations, four consumers and four workers — and the leader must be one of
  the registered coordinators, not a marker written by a process that vanished;
- a second snapshot from the *same* leader at the *same* epoch, with an advanced
  version, because reading one znode twice is not two snapshots;
- a published worker delay reaching all four workers at the exact znode version;
- the API, running inside the run's environment on a port of its own, reading
  back known sentinel values from each store — a counter and a last page in
  Redis, an event in Cassandra — plus a `/cluster` whose leader matches the one
  ZooKeeper reported, a 404 for an unknown user, a 422 for `limit=101`, and an
  OpenAPI document listing every route.

After readiness, **one known event, end to end**: the check opens a WebSocket,
waits until the API says it is *subscribed* — `ready` with `bridge:
"subscribed"`, or a `resubscribed` after a `ready` that said otherwise — then
produces an event with a known id into the run's topic and requires that event's
notification, with its partition and offset — under one deadline checked
explicitly, because a receive timeout never expires while unrelated
notifications keep arriving, and in the smoke topology they arrive every 50ms.
Subscribing first matters: pub/sub
would rightly drop a notification published before the socket was enrolled. It
also requires a foreign and a missing `Origin` to be refused, and `/live` and its
module to be served. The run's channel carries the run's key prefix, because
pub/sub channels ignore the Redis database.

The API check uses sentinels rather than whatever the producer happened to
write, because it tests the read path only — the other predicates already prove
the writes — and two reads of a moving value disagree. The sentinels sit under
the same prefix and keyspace the consumers write to, so the write predicates
leave them out: otherwise the sentinels alone would satisfy them, and a consumer
that wrote nothing would pass. Its port is chosen by
binding port 0 and releasing it, so something else could take it in the moment
before the API binds it. A small window, accepted rather than hidden.

That last one matters: without it the check would pass with the whole watch path
broken, because the workers would keep their startup values and nothing would
say so. Everything it created for the run — Redis keys, Cassandra keyspace, RabbitMQ
queue, ZooKeeper subtree and Kafka topic — is removed only after the topology has
stopped, or a live consumer or worker would write straight back into it.

CI runs these as two jobs. `quality` covers linting, formatting, unit tests and
Compose parsing; `integration` starts the real topology and runs the smoke test.
Both run on tag pushes as well as branches. A workflow cannot stop a tag from
existing, so the gate is editorial rather than technical: a GitHub release is
published only after both tag workflows pass.

## Project structure

```text
docker-compose.yml       Kafka (KRaft), Redis, Cassandra, RabbitMQ, ZooKeeper, Flink
flink.Dockerfile         the Flink image, pinned by digest, connector by hash
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
    coordination.py      election, fencing, registration, watches
    kafka_offsets.py     committed offsets, through one retained admin client
    coordinator.py       contends for leadership; the winner writes snapshots
    cluster.py           creates the coordination tree, and inspects it
    runtime_config.py    settings that can change while a process runs
    failover_demo.py     the two-leaders demonstration
    flink_job.py         the Flink job: event-time windows, as Flink SQL
    flink_cluster.py     submitting to Flink and reading its REST API
    flink_jobs.py        flink-job: submit, status, cancel by id
    windows.py           prints the latest windowed counts
    api.py               the HTTP API: one endpoint per store, bounded reads
    bulkhead.py          per-store admission, worker allowance and deadline
    notifications.py     what the consumer announces, and the best-effort publish
    bridge.py            the API's one Redis subscription, heartbeat and recovery
    fanout.py            bounded per-socket queues, eviction and admission
    static/live.html     the live page
    static/live.mjs      its offset tracker and read scheduler
    topology_runner.py   starts, observes and stops the whole topology
    smoke_test.py        bounded assertions against a running broker
tests/
tests/js/                the tracker's and scheduler's own tests, for node --test
Procfile                 the processes that make up the running system
```

`config.py` grows an entry per technology as the tutorial proceeds. It is the
one place the host-versus-container distinction is expressed in code.
