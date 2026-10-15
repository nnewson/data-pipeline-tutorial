"""The job keeper: the elected coordinator's claim that the Flink job exists.

A job that must keep running has two kinds of state. Its *actual* state — the
job, its windows, its offsets — is Flink's to recover: with high availability a
jobmanager that comes back brings the same job back, from its last checkpoint.
Its *desired* state — that the job should exist at all — is ours, and this
module asserts it on behalf of the leader 0.7 elects.

Submitting and cancelling are side effects *outside* ZooKeeper, which a deposed
leader can still perform, so 0.7's fence alone does not cover them. Three rules
do, between them:

- **A fenced intent before every submission.** The record at `{root}/flink` is
  rewritten — fenced on the epoch and on its own version — before a job is
  submitted, so a deposed leader, or a keeper acting on a stale read, never
  starts one.
- **A monotonic generation.** Each submission is a new generation, named
  `<base>-g<n>`, and the record's generation only ever increases. A job older
  than the generation a keeper read is superseded for good, so cancelling it is
  safe from any keeper, current or not. A job *newer* than that is never
  cancelled: it means the read was stale.
- **Every attempt kept until it is accounted for.** An attempt starts with no
  job id — unresolved until proven otherwise — and leaves the record only once
  its job is confirmed stopped or not found. Finding generation 2 accounts for
  generation 2 only.

The listing is for discovery: a successful overview can omit a live job, so a
decision about one job asks about that job. And unknown is never absent — 0.10's
lesson — so a failed listing or lookup decides nothing.

The decision is a pure function of the record, the listing, the lookups and this
keeper's memory; the Keeper around it does the I/O, on one thread for the life
of the coordinator process, handed tenure-scoped work.
"""

import logging
import re
import subprocess
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any

from kazoo.exceptions import ConnectionClosedError, ConnectionLoss, SessionExpiredError

from pipeline import coordination, flink_cluster
from pipeline.config import (
    FLINK_CHECKPOINT_SECONDS,
    FLINK_GROUP,
    FLINK_IDLE_TIMEOUT_SECONDS,
    FLINK_JOB_BASE,
    FLINK_KEEPER_INTERVAL_SECONDS,
    FLINK_WINDOW_SECONDS,
    KAFKA_TOPIC,
    KAFKA_WINDOWS_TOPIC,
)
from pipeline.coordination import Paths, RecordChanged, StaleLeader
from pipeline.flink_job import Settings

logger = logging.getLogger("job-keeper")

# How long an attempt with no id may still land: the client's own bound, plus
# 0.10's measured allowance for a `flink run` that outlives its client. Not a
# safety bound — a late arrival is superseded and cancelled either way — but the
# price of a needless second job.
UNCERTAINTY_SECONDS = flink_cluster.SUBMIT_TIMEOUT_SECONDS + 20

# Consecutive resubmissions back off: the first is immediate, then these.
BACKOFF_INITIAL_SECONDS = 10.0
BACKOFF_MAX_SECONDS = 300.0
# A generation that has run this long resets the count.
HEALTHY_AFTER_SECONDS = 60.0
# While Flink cannot be read at all, look less often, up to this.
UNAVAILABLE_MAX_SECONDS = 60.0
# How long a record write is waited for. A timeout ends the wait, not the
# request — kazoo keeps it queued — so the outcome is uncertain, never assumed.
WRITE_TIMEOUT_SECONDS = 10.0
# Doublings beyond this are past every cap: 2**1024 is not a float.
MAX_DOUBLINGS = 32

# A lookup's confirmed absence: a 404 from `/jobs/:id`.
NOT_FOUND = "NOT_FOUND"
TERMINAL = flink_cluster.TERMINAL


def job_name(base: str, generation: int) -> str:
    return f"{base}-g{generation}"


def generation_of(base: str, name: str) -> int | None:
    """The generation a job name carries, or None if it is not one of ours."""
    match = re.fullmatch(rf"{re.escape(base)}-g(\d+)", name)
    return int(match.group(1)) if match else None


def wall_now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(frozen=True)
class Attempt:
    job_id: str | None
    submitted_at: str | None = None


@dataclass(frozen=True)
class Record:
    """The record as the keeper reasons about it; JSON keys are strings."""

    generation: int
    epoch: int | None
    failures: int
    attempts: dict[int, Attempt]

    @classmethod
    def parse(cls, raw: dict) -> "Record":
        return cls(
            generation=int(raw.get("generation", 0)),
            epoch=raw.get("epoch"),
            failures=int(raw.get("failures", 0)),
            attempts={
                int(generation): Attempt(entry.get("job_id"), entry.get("submitted_at"))
                for generation, entry in (raw.get("attempts") or {}).items()
            },
        )

    def to_json(self) -> dict:
        return {
            "generation": self.generation,
            "epoch": self.epoch,
            "failures": self.failures,
            "attempts": {
                str(generation): {
                    "job_id": attempt.job_id,
                    "submitted_at": attempt.submitted_at,
                }
                for generation, attempt in sorted(self.attempts.items())
            },
        }


@dataclass(frozen=True)
class Listed:
    """One of our jobs, as the overview listed it."""

    id: str
    generation: int
    state: str


@dataclass
class Memory:
    """What this keeper has seen, by its own monotonic clock.

    Never a timestamp another process wrote: comparing clocks across processes
    is how an uncertainty window shrinks to nothing. Reset with every tenure, so
    a new leader waits a full window — conservative by design.
    """

    first_seen: dict[tuple, float] = field(default_factory=dict)
    # Generations whose intent this tenure wrote, or may have written, and for
    # which it never called Flink: paused or stopped in between, an intent write
    # left unconfirmed, or a submission command that never ran. Only this tenure
    # could submit them (any other keeper writes a newer intent first), so they
    # cannot exist. Not a submission that ran: its outcome stays uncertain.
    never_submitted: set[int] = field(default_factory=set)

    def waited(self, key: tuple, now: float) -> float:
        return now - self.first_seen.setdefault(key, now)

    def keep_generation(self, generation: int) -> None:
        self.first_seen = {
            key: seen for key, seen in self.first_seen.items() if key[1] == generation
        }
        self.never_submitted = {g for g in self.never_submitted if g >= generation}


@dataclass(frozen=True)
class Plan:
    """What one cycle does, in order: cancel, write (fenced), then submit."""

    cancel: tuple[str, ...] = ()
    record: Record | None = None
    submit: int | None = None
    observation: dict = field(default_factory=dict)


def doubled(base: float, times: int, cap: float) -> float:
    """base * 2**times, saturating at cap — never an overflow, however long."""
    return min(cap, base * 2 ** min(times, MAX_DOUBLINGS))


def backoff(failures: int) -> float:
    """How long to wait before a resubmission, given those before it."""
    if failures <= 0:
        return 0.0
    return doubled(BACKOFF_INITIAL_SECONDS, failures - 1, BACKOFF_MAX_SECONDS)


def quiet_wait(interval: float, quiet: int) -> float:
    """How long to wait after `quiet` consecutive cycles without Flink."""
    return doubled(interval, quiet, UNAVAILABLE_MAX_SECONDS)


def lookups_needed(record: Record, listed: list[Listed]) -> set[str]:
    """The jobs to ask about directly: every recorded id, and every listed job
    at or below the record's generation. Above it the read is stale, and nothing
    is decided."""
    recorded = {a.job_id for a in record.attempts.values() if a.job_id}
    return recorded | {j.id for j in listed if j.generation <= record.generation}


def decide(
    record: Record,
    listed: list[Listed] | None,
    states: dict[str, str],
    memory: Memory,
    now: float,
    wall: str,
) -> Plan:
    """One cycle's decision. `states` holds each lookup that was answered — a
    state, or NOT_FOUND; a lookup that failed is simply absent, which is unknown.
    """
    current = record.generation

    def observed(status: str, **extra: Any) -> dict:
        return {"generation": current, "status": status, "at": wall, **extra}

    if listed is None:
        return Plan(observation=observed("unavailable", note="no job listing"))

    newer = sorted({j.generation for j in listed if j.generation > current})
    if newer:
        # Never cancelled, and nothing else decided: the record has moved on
        # since it was read, or someone started a job by hand.
        return Plan(
            observation=observed(
                "stale", note=f"generation {newer} listed above the record's"
            )
        )

    attempts = dict(record.attempts)
    notes: list[str] = []

    # Attempts without an id gain one when exactly one job carries the name.
    for generation, attempt in sorted(attempts.items()):
        if attempt.job_id is None:
            named = [j for j in listed if j.generation == generation]
            if len(named) == 1:
                attempts[generation] = replace(attempt, job_id=named[0].id)
            elif len(named) > 1:
                notes.append(f"{len(named)} jobs named for generation {generation}")

    # Superseded generations first, whatever the current one is doing: they may
    # hold the slots it needs.
    superseded = {a.job_id: g for g, a in attempts.items() if g < current and a.job_id}
    for job in listed:
        if job.generation < current:
            superseded.setdefault(job.id, job.generation)
    cancel = []
    for job_id, generation in sorted(superseded.items(), key=lambda item: item[1]):
        state = states.get(job_id)
        if state is None:
            continue  # unknown: decided on a later cycle
        if state == NOT_FOUND or state in TERMINAL:
            if generation in attempts and attempts[generation].job_id == job_id:
                del attempts[generation]
        elif state != "CANCELLING":
            cancel.append(job_id)

    extra = [
        j.id
        for j in listed
        if j.generation == current
        and attempts.get(current, Attempt(None)).job_id not in (None, j.id)
    ]
    if extra:
        notes.append(f"unexplained jobs of generation {current}: {sorted(extra)}")

    failures = record.failures
    entry = attempts.get(current)
    if entry is not None and entry.job_id is None and current in memory.never_submitted:
        # Provably never submitted, so not owed: no need to wait it out.
        del attempts[current]
        entry = None
    job_id = entry.job_id if entry else None
    stopped, drop_current, status, state = False, False, "", None

    if current == 0 or entry is None:
        stopped, status = True, "none"
    elif job_id is not None:
        state = states.get(job_id)
        if state != "RUNNING":
            # "Has run for a minute" means continuously: a restart starts over.
            memory.first_seen.pop(("running", current, job_id), None)
        if state is None:
            status = "unknown"  # never advances the generation
        elif state == "RUNNING":
            status = "running"
            ran = memory.waited(("running", current, job_id), now)
            if failures and ran >= HEALTHY_AFTER_SECONDS:
                failures = 0
        elif state == NOT_FOUND or state in TERMINAL:
            stopped, drop_current, status = True, True, "stopped"
        else:
            status = "recovering"  # INITIALIZING, RESTARTING, SUSPENDED, ...
    elif memory.waited(("unresolved", current), now) < UNCERTAINTY_SECONDS:
        status = "uncertain"
    else:
        # Superseded, but possibly still on its way: its entry stays.
        stopped, status = True, "unresolved"

    submit = None
    if stopped:
        if memory.waited(("stopped", current), now) >= backoff(failures):
            if drop_current:
                attempts.pop(current, None)
            attempts[current + 1] = Attempt(None, wall)
            # The first submission is not a resubmission.
            failures = failures + 1 if current >= 1 else 0
            submit = current + 1
        else:
            status = f"{status}, backing off"

    memory.keep_generation(current)
    new = Record(
        generation=submit if submit is not None else current,
        epoch=record.epoch,
        failures=failures,
        attempts=attempts,
    )
    observation = observed(
        "submitting" if submit else status,
        job_id=job_id,
        state=state,
        cancelling=cancel,
        unresolved=sorted(g for g, a in attempts.items() if a.job_id is None),
        notes=notes,
    )
    return Plan(
        cancel=tuple(cancel),
        record=new if new != record else None,
        submit=submit,
        observation=observation,
    )


def job_settings(generation: int) -> Settings:
    """What each generation is submitted with: the pipeline's own topics."""
    return Settings(
        source_topic=KAFKA_TOPIC,
        sink_topic=KAFKA_WINDOWS_TOPIC,
        group=FLINK_GROUP,
        job_name=job_name(FLINK_JOB_BASE, generation),
        window_seconds=FLINK_WINDOW_SECONDS,
        checkpoint_seconds=FLINK_CHECKPOINT_SECONDS,
        idle_timeout_seconds=FLINK_IDLE_TIMEOUT_SECONDS,
        generation=generation,
    )


class Tenure:
    """One leadership's claim on the keeper's work.

    The epoch never changes and the stop flag, once set, is never cleared. The
    keeper checks this, not only `Leadership.may_work`: `begin_tenure()` resets
    the shared flag, so an old tenure's work could otherwise inherit a new
    tenure's permission — and a tenure ended by a rejected write never sets
    that flag at all.
    """

    def __init__(self, epoch: int) -> None:
        self.epoch = epoch
        self._retired = threading.Event()

    def retire(self) -> None:
        self._retired.set()

    @property
    def active(self) -> bool:
        return not self._retired.is_set()


class Interrupted(Exception):
    """The tenure was retired, leadership is unknown, or the keeper stopped:
    start nothing further."""


class WriteUncertain(Exception):
    """A record write was not confirmed: it may yet be applied, or not.

    Never treated as either. Every write is versioned, so the next cycle decides
    again from a fresh read — and an unconfirmed intent never leads to a
    submission."""


class Keeper:
    """One worker for the life of the coordinator process.

    - One tenure at a time, one Flink call at a time, at most one child process
      (a submission's client). A new tenure's work starts once the old one's
      call has returned.
    - A retired tenure's work stops at the call it is in, and its outcome is
      never written: a submission that returns after retirement leaves its
      attempt id-less, for the next keeper to find by name.
    - Observations carry their tenure's epoch, and are served only for it.
    - stop() takes the lock every ZooKeeper write is *issued* under, so once it
      returns no write can start. The lock is never held across a network wait:
      a write is issued asynchronously under it and waited for outside it, so a
      blocked ZooKeeper cannot hold up shutdown or the snapshot's observation.
      stop() terminates a submission's client rather than wait for it, and never
      waits for a Flink call. So the owner can stop the keeper and then close
      the client.
    - Before every Flink action the keeper checks the tenure *and* that
      leadership may be exercised: SUSPENDED pauses it mid-cycle, not only
      between cycles.
    """

    def __init__(
        self,
        client,
        paths: Paths,
        leadership,
        flink=flink_cluster,
        base: str = FLINK_JOB_BASE,
        settings: Callable[[int], Settings] = job_settings,
        interval: float = FLINK_KEEPER_INTERVAL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client = client
        self._paths = paths
        self._leadership = leadership
        self._flink = flink
        self._base = base
        self._settings = settings
        self._interval = interval
        self._clock = clock
        self._lock = threading.Lock()
        self._stopped = threading.Event()
        self._wake = threading.Event()
        self._tenure: Tenure | None = None
        self._child: subprocess.Popen | None = None
        self._observation: tuple[int, dict] | None = None
        self._thread: threading.Thread | None = None

    # --- the owner's side ---------------------------------------------------

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run, name="job-keeper", daemon=True
        )
        self._thread.start()

    def serve(self, tenure: Tenure | None) -> None:
        """Hand over the current tenure's work (retire the previous one first)."""
        self._tenure = tenure
        self._wake.set()

    def observation(self, epoch: int) -> dict | None:
        """The latest observation, only if it was made under this epoch."""
        with self._lock:
            if self._observation and self._observation[0] == epoch:
                return dict(self._observation[1])
        return None

    def stop(self, join_seconds: float = 1.0) -> None:
        with self._lock:
            self._stopped.set()
            child = self._child
        if child is not None and child.poll() is None:
            child.terminate()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout=join_seconds)

    # --- the worker's side --------------------------------------------------

    def _run(self) -> None:
        current: Tenure | None = None
        memory = Memory()
        quiet = 0
        while not self._stopped.is_set():
            tenure = self._tenure
            if tenure is not current:
                current, memory, quiet = tenure, Memory(), 0
            wait = self._interval
            if tenure is not None and tenure.active and self._leadership.may_work:
                try:
                    status = self.cycle(tenure, memory)
                except Interrupted:
                    status = "interrupted"
                except (RecordChanged, WriteUncertain) as error:
                    logger.info(f"deciding again from a fresh read: {error}")
                    status = "changed"
                except StaleLeader as error:
                    logger.warning(f"keeper standing down: {error}")
                    tenure.retire()
                    status = "stale"
                except Exception as error:  # noqa: BLE001 - the keeper must outlive any one failure
                    logger.warning(
                        f"keeper cycle failed: {type(error).__name__}: {error}"
                    )
                    self._observe(
                        tenure,
                        {"status": "error", "note": f"{type(error).__name__}: {error}"},
                    )
                    status = "error"
                quiet = quiet + 1 if status in ("unavailable", "error") else 0
                if quiet:
                    wait = quiet_wait(self._interval, quiet)
            elif tenure is not None and tenure.active:
                wait = 0.5  # SUSPENDED: leadership is unknown, so wait and see
            self._wake.wait(wait)
            self._wake.clear()

    def cycle(self, tenure: Tenure, memory: Memory) -> str:
        """Read, decide, act. Returns the observation's status."""
        self._check(tenure)
        raw, version = coordination.read_flink_record(self._client, self._paths)
        record = Record.parse(raw)

        listed = self._list(tenure)
        states: dict[str, str] = {}
        if listed is not None:
            for job_id in sorted(lookups_needed(record, listed)):
                self._check(tenure)
                try:
                    states[job_id] = self._flink.job_state(job_id)
                except flink_cluster.NotFound:
                    states[job_id] = NOT_FOUND
                except flink_cluster.FlinkError as error:
                    logger.debug(f"job {job_id} could not be looked up: {error}")

        plan = decide(record, listed, states, memory, self._clock(), wall_now())

        for job_id in plan.cancel:
            self._check(tenure)
            try:
                self._flink.cancel(job_id)
                logger.warning(f"cancelled superseded job {job_id}")
            except flink_cluster.FlinkError as error:
                logger.warning(f"could not cancel superseded job {job_id}: {error}")

        if plan.record is not None:
            written = replace(plan.record, epoch=tenure.epoch)
            try:
                version = self._write(tenure, written, version)
            except WriteUncertain:
                if plan.submit is not None:
                    # Whatever ZooKeeper did with the intent, this tenure has not
                    # called Flink for it — and under this epoch only this keeper
                    # writes the record, so if the generation appears, it is ours.
                    memory.never_submitted.add(plan.submit)
                raise
            if plan.submit is not None:
                self._submit(tenure, memory, written, version, plan.submit)

        self._observe(tenure, plan.observation)
        return plan.observation["status"]

    def _list(self, tenure: Tenure) -> list[Listed] | None:
        self._check(tenure)
        try:
            jobs = self._flink.jobs_overview()
        except flink_cluster.FlinkError as error:
            logger.warning(f"Flink unavailable to the keeper: {error}")
            return None
        listed = []
        for job in jobs:
            generation = generation_of(self._base, job["name"])
            if generation is not None:
                listed.append(Listed(job["id"], generation, job["state"]))
        return listed

    def _submit(
        self,
        tenure: Tenure,
        memory: Memory,
        record: Record,
        version: int,
        generation: int,
    ) -> None:
        try:
            self._check(tenure)
        except Interrupted:
            # The intent is recorded and nothing was started: this tenure knows
            # the generation never existed, should it carry on.
            memory.never_submitted.add(generation)
            raise
        settings = self._settings(generation)
        # From here this tenure *has* called Flink for the generation, so an
        # earlier attempt's mark — an intent write lost before this one — no
        # longer holds. Only "the command never ran", below, puts it back.
        memory.never_submitted.discard(generation)
        logger.info(f"submitting {settings.job_name}")
        try:
            job_id = self._flink.submit(settings, started=self._track)
        except flink_cluster.SubmissionUncertain as error:
            # The entry stays id-less: found by name later, or kept as owed.
            logger.warning(f"submission of {settings.job_name} uncertain: {error}")
            return
        except flink_cluster.FlinkError as error:
            # The command never ran, so nothing was submitted — known here even
            # if the write removing the attempt goes unconfirmed.
            logger.warning(f"submission of {settings.job_name} failed: {error}")
            memory.never_submitted.add(generation)
            attempts = {g: a for g, a in record.attempts.items() if g != generation}
            self._write(tenure, replace(record, attempts=attempts), version)
            return
        finally:
            with self._lock:
                self._child = None
        logger.info(f"submitted {settings.job_name} as {job_id}")
        attempts = dict(record.attempts)
        attempts[generation] = replace(attempts[generation], job_id=job_id)
        self._write(tenure, replace(record, attempts=attempts), version)

    def _track(self, process: subprocess.Popen) -> None:
        with self._lock:
            if self._stopped.is_set():
                process.terminate()
            else:
                self._child = process

    def _check(self, tenure: Tenure) -> None:
        """Before starting anything: still this tenure's, and leadership known."""
        if self._stopped.is_set() or not tenure.active or not self._leadership.may_work:
            raise Interrupted()

    def _write(self, tenure: Tenure, record: Record, version: int) -> int:
        """Issue under the lock; wait outside it; never assume the outcome."""
        with self._lock:
            self._check(tenure)
            pending = coordination.flink_record_transaction(
                self._client, self._paths, tenure.epoch, record.to_json(), version
            ).commit_async()
        try:
            results = pending.get(timeout=WRITE_TIMEOUT_SECONDS)
        except self._client.handler.timeout_exception as error:
            raise WriteUncertain(
                f"record write unconfirmed after {WRITE_TIMEOUT_SECONDS}s"
            ) from error
        except (ConnectionLoss, ConnectionClosedError, SessionExpiredError) as error:
            raise WriteUncertain(f"record write unconfirmed: {error!r}") from error
        return coordination.flink_record_outcome(
            results, self._paths, tenure.epoch, version
        )

    def _observe(self, tenure: Tenure, observation: dict) -> None:
        with self._lock:
            if tenure.active and not self._stopped.is_set():
                self._observation = (tenure.epoch, observation)
