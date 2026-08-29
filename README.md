# Data Pipeline Tutorial

A step-by-step rebuild of [data-pipeline](https://github.com/nnewson/data-pipeline),
released one technology at a time, with a walkthrough post for each release at
[nnewson.dev](https://nnewson.dev).

Each tag is a working system. See [roadmap.md](roadmap.md) for the full
sequence.

**This release: 0.1 — Docker Compose.** No pipeline yet. This release
establishes the build toolchain and the orchestration vocabulary the rest of the
tutorial assumes, and it introduces one idea that everything after it depends
on: a service has one address from the host and a different one from inside the
Compose network.

## This release

```bash
git clone https://github.com/nnewson/data-pipeline-tutorial.git
cd data-pipeline-tutorial
git checkout 0.1
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

`--wait` blocks until every service with a healthcheck reports healthy, so a
successful return means the topology is ready rather than merely started.
`shell` waits for `web`'s healthcheck to pass before it starts at all.

```text
                    host
   ┌──────────────────────────────────────────┐
   │  uv run check-web                        │
   │  WEB_URL=http://localhost:8080           │
   └───────────────────┬──────────────────────┘
                       │  published port 8080 -> 80
   ┌───────────────────┴──────────────────────┐
   │            Compose network               │
   │                                          │
   │   shell ───── http://web:80 ─────► web   │
   │   WEB_URL=http://web                     │
   └──────────────────────────────────────────┘
```

## One service, two addresses

From the host, `web` is published on port 8080:

```bash
curl http://localhost:8080
uv run check-web
```

From inside the Compose network it is not on port 8080 at all. It is the
hostname `web`, on port 80:

```bash
docker compose exec shell uv run check-web
```

There is no `curl` in there to compare against. The `shell` image is a minimal
one and ships a Python toolchain, nothing else — which is the normal case, and
worth meeting early. Later releases reach for each service's own image when they
need a CLI, rather than expecting one container to carry them all.

Same service, same code, different address. `check-web` prints whichever one it
was told to use, because `WEB_URL` defaults to the host address and
`docker-compose.yml` overrides it for the container.

This matters more than it looks. In 0.2 Kafka is configured with two listeners
for exactly this reason — host processes reach it one way, containers reach it
another — and that configuration is opaque until you have seen the simple
version.

## Getting inside a running system

Two ways in, both used throughout the tutorial:

```bash
docker compose exec shell bash          # into the container that is already running
docker compose run --rm shell bash      # a fresh one-off container, removed on exit
```

Later releases lean on this constantly — loading a Cassandra schema, listing
Kafka topics, reading Redis keys — using each service's own image and CLI.

## Shutting down

```bash
docker compose down
```

## Testing

Unit tests need no containers.

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

The smoke test does. It asserts both addressing paths against the running
topology, and is the same check CI runs.

```bash
docker compose up -d --wait
uv run smoke-test
docker compose down
```

```text
PASS  host path: http://localhost:8080 served 200
PASS  container path: http://web served 200 from inside the network

all 2 checks passed
```

CI runs these as two jobs. `quality` covers linting, formatting, unit tests and
Compose parsing; `integration` starts the real topology and runs the smoke test.
Both run on tag pushes as well as branches. A workflow cannot stop a tag from
existing, so the gate is editorial rather than technical: a GitHub release is
published only after both tag workflows pass.

## Project structure

```text
docker-compose.yml       web (throwaway HTTP service) and shell
src/pipeline/
    config.py            environment-driven settings, host addresses by default
    check_web.py         fetches WEB_URL and reports what answered
    smoke_test.py        bounded assertions against a running topology
tests/
```

`config.py` grows an entry per technology as the tutorial proceeds. It is the
one place the host-versus-container distinction is expressed in code.
