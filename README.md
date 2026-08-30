# Data Pipeline Tutorial

A step-by-step rebuild of [data-pipeline](https://github.com/nnewson/data-pipeline),
released one technology at a time, with a walkthrough post for each release at
[nnewson.dev](https://nnewson.dev).

**This release: 0.2 — Kafka.** A producer generates synthetic pageviews, a
broker keeps them in a log, and a consumer reads them back. One topic, one
partition. 0.1's throwaway `web` and `shell` services are gone; Kafka's own
image carries the CLI tools this release needs.

## This release

```bash
git clone https://github.com/nnewson/data-pipeline-tutorial.git
cd data-pipeline-tutorial
git checkout 0.2
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

Two terminals, because these are two long-running host processes. Producer
first:

```bash
uv run producer
```

```text
INFO pipeline: Created topic pageviews with 1 partition(s)
INFO producer: Produced (partition 0, offset 0): {'event_id': '...', 'page': '/', ...}
INFO producer: Produced (partition 0, offset 1): {'event_id': '...', 'page': '/docs', ...}
```

The producer declares the topic before writing to it. That is transitional: the
broker still has auto-creation enabled, so declaring is belt-and-braces rather
than ownership, and the two can still race. It is here because without it the
first documented run logs `ERROR ... Topic pageviews not found in cluster
metadata` — which is not a fault, but reads exactly like one. 0.3 takes real
ownership of topic lifecycle, disables auto-creation, and changes the partition
count.

The partition and offset are reported because the producer waits for the broker
to acknowledge each record before logging it. `send()` is asynchronous and
returns a future; logging without resolving it would claim a delivery that might
still fail. At one event per second that wait costs nothing, and it means the
line you are reading is a fact rather than an intention.

Then, in a second terminal:

```bash
uv run consumer
```

```text
INFO consumer: Consumed (partition 0, offset 0): {'event_id': '...', ...}
INFO consumer: Consumed (partition 0, offset 1): {'event_id': '...', ...}
```

The consumer starts at offset 0 rather than at the end, because the log is
durable and `auto_offset_reset="earliest"` asks for everything still retained.
Stop it, restart it, and it resumes from its committed offset instead — the log
is not a queue, and reading does not consume.

On a brand new cluster the consumer logs `NotCoordinatorError` once or twice
before settling. That is Kafka creating the internal offsets topic for the first
consumer group that asks for it; the retry succeeds and later runs are silent.

Two terminals is already mildly annoying. 0.3 adds three more consumers and
introduces a Procfile so the whole topology starts as one command.

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

all 2 checks passed in 2.5s
```

CI runs these as two jobs. `quality` covers linting, formatting, unit tests and
Compose parsing; `integration` starts the real topology and runs the smoke test.
Both run on tag pushes as well as branches. A workflow cannot stop a tag from
existing, so the gate is editorial rather than technical: a GitHub release is
published only after both tag workflows pass.

## Project structure

```text
docker-compose.yml       a single Kafka broker in KRaft mode
src/pipeline/
    __init__.py          logging setup and connection retry
    config.py            environment-driven settings, host addresses by default
    producer.py          declares the topic, then produces acknowledged events
    kafka_consumer.py    reads them back, logging partition and offset
    smoke_test.py        bounded assertions against a running broker
tests/
```

`config.py` grows an entry per technology as the tutorial proceeds. It is the
one place the host-versus-container distinction is expressed in code.
