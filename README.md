# Data Pipeline Tutorial

A step-by-step rebuild of [data-pipeline](https://github.com/nnewson/data-pipeline),
released one technology at a time, with a walkthrough post for each release at
[nnewson.dev](https://nnewson.dev).

**This release: 0.7 — ZooKeeper.** Three coordinators compete for one
leadership, and the winner alone writes a snapshot of the pipeline. The
delivery-semantics thread so far has been about a *message* arriving twice; this
one is about a **role** being held twice — and the fix is not a better election but a fencing token that
lets the write itself refuse the loser.

## This release

```bash
git clone https://github.com/nnewson/data-pipeline-tutorial.git
cd data-pipeline-tutorial
git checkout 0.7
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

Then start everything — a producer, four consumers, four workers, and three
coordinators:

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

0.8 serves this snapshot over HTTP — which is why the leader exists at all rather
than merely recording that it is the leader.

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
separately at [localhost:15672](http://localhost:15672). ZooKeeper is
`localhost:2181` and `zookeeper:2181` — the last new service before Flink brings
a jobmanager and taskmanager of its own at 0.10.

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
PASS  honcho topology: 4 consumers own 4 partitions; committed offsets advanced 137 to 239; 4 counters, 246 last-page values; rows written to smoke_bc837f27; 4 workers, 247 jobs completed; one leader at epoch 1 among 3 contenders; 4 consumers and 4 workers registered; leader …-99608 wrote again at epoch 1 (snapshot v27 -> v28); all 4 workers applied worker_delay=0.03 at config version 1; nothing was left running

all 4 checks passed in 18.2s
```

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
- a published worker delay reaching all four workers at the exact znode version.

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
docker-compose.yml       Kafka (KRaft), Redis, Cassandra, RabbitMQ, ZooKeeper
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
    topology_runner.py   starts, observes and stops the whole topology
    smoke_test.py        bounded assertions against a running broker
tests/
Procfile                 the processes that make up the running system
```

`config.py` grows an entry per technology as the tutorial proceeds. It is the
one place the host-versus-container distinction is expressed in code.
