"""The keeper: its decision, its cycle, and who may act when."""

import threading
import time

import pytest
from kazoo.exceptions import ConnectionLoss
from kazoo.handlers.threading import KazooTimeoutError

from pipeline import coordination, flink_cluster, job_keeper
from pipeline.coordination import RecordChanged, StaleLeader
from pipeline.flink_job import Settings
from pipeline.job_keeper import (
    NOT_FOUND,
    UNCERTAINTY_SECONDS,
    Attempt,
    Keeper,
    Listed,
    Memory,
    Record,
    Tenure,
    decide,
)

BASE = "jobs"
WALL = "2026-10-12T00:00:00+00:00"


def record(generation=0, failures=0, **attempts) -> Record:
    """record(2, g1=None, g2="b") — attempts by generation, id or None."""
    return Record(
        generation=generation,
        epoch=None,
        failures=failures,
        attempts={int(k[1:]): Attempt(v, WALL) for k, v in attempts.items()},
    )


def ids(*pairs) -> list[Listed]:
    return [Listed(job_id, generation, state) for job_id, generation, state in pairs]


# --- the decision ------------------------------------------------------------


def test_the_first_submission_is_immediate_and_not_counted_as_a_failure():
    plan = decide(record(0), [], {}, Memory(), now=0, wall=WALL)

    assert plan.submit == 1
    assert plan.record.generation == 1
    assert plan.record.attempts == {1: Attempt(None, WALL)}
    assert plan.record.failures == 0


def test_a_running_recorded_job_is_adopted_and_nothing_is_written():
    plan = decide(
        record(1, g1="a"), ids(("a", 1, "RUNNING")), {"a": "RUNNING"}, Memory(), 0, WALL
    )

    assert plan.submit is None and plan.record is None and plan.cancel == ()
    assert plan.observation["status"] == "running"


def test_an_unreadable_listing_decides_nothing():
    plan = decide(record(1, g1="a"), None, {}, Memory(), 0, WALL)

    assert plan == job_keeper.Plan(observation=plan.observation)
    assert plan.observation["status"] == "unavailable"


@pytest.mark.parametrize(
    "state",
    ["INITIALIZING", "CREATED", "RESTARTING", "RECONCILING", "CANCELLING", "SUSPENDED"],
)
def test_a_recovering_job_is_waited_for_suspended_included(state):
    """SUSPENDED is only locally terminal: HA brings it back."""
    plan = decide(
        record(1, g1="a"), ids(("a", 1, state)), {"a": state}, Memory(), 0, WALL
    )

    assert plan.submit is None and plan.record is None
    assert plan.observation["status"] == "recovering"


@pytest.mark.parametrize("state", ["FAILED", "CANCELED", "FINISHED", NOT_FOUND])
def test_a_stopped_or_missing_job_is_superseded(state):
    plan = decide(record(1, g1="a"), [], {"a": state}, Memory(), 0, WALL)

    assert plan.submit == 2
    assert plan.record.attempts == {2: Attempt(None, WALL)}  # 1 confirmed stopped
    assert plan.record.failures == 1


def test_a_listing_that_omits_a_job_whose_lookup_says_running_changes_nothing():
    """A successful overview can omit a live job; the lookup decides."""
    memory = Memory()
    for now in (0, 100, 10_000):
        plan = decide(record(1, g1="a"), [], {"a": "RUNNING"}, memory, now, WALL)
        assert plan.submit is None and plan.record is None


def test_a_listing_that_omits_a_job_whose_lookup_fails_changes_nothing():
    """Unknown, however many listings omit it: never advances the generation."""
    memory = Memory()
    for now in (0, 100, 10_000):
        plan = decide(record(1, g1="a"), [], {}, memory, now, WALL)
        assert plan.submit is None and plan.record is None
        assert plan.observation["status"] == "unknown"


def test_superseded_jobs_are_cancelled_first_whatever_the_current_one_is_doing():
    """The current generation may be waiting for the slots they hold."""
    plan = decide(
        record(2, g1="old", g2="new"),
        ids(("old", 1, "RUNNING"), ("new", 2, "CREATED")),
        {"old": "RUNNING", "new": "CREATED"},
        Memory(),
        0,
        WALL,
    )

    assert plan.cancel == ("old",)
    assert plan.submit is None


def test_an_unrecorded_superseded_job_is_cancelled_too():
    plan = decide(
        record(3, g3="c"),
        ids(("stray", 1, "RUNNING"), ("c", 3, "RUNNING")),
        {"stray": "RUNNING", "c": "RUNNING"},
        Memory(),
        0,
        WALL,
    )

    assert plan.cancel == ("stray",)


def test_a_newer_generation_than_the_record_is_never_cancelled():
    """A stale read: the successor's job must survive it."""
    plan = decide(
        record(1, g1="a"),
        ids(("a", 1, "CANCELED"), ("b", 2, "RUNNING")),
        {"a": "CANCELED", "b": "RUNNING"},
        Memory(),
        0,
        WALL,
    )

    assert plan.cancel == () and plan.record is None and plan.submit is None
    assert plan.observation["status"] == "stale"


def test_an_attempt_without_an_id_gains_one_when_its_name_is_listed():
    plan = decide(
        record(1, g1=None),
        ids(("a", 1, "RUNNING")),
        {"a": "RUNNING"},
        Memory(),
        0,
        WALL,
    )

    assert plan.record.attempts[1].job_id == "a"
    assert plan.submit is None


def test_an_unresolved_attempt_is_waited_for_then_superseded_but_kept():
    memory = Memory()
    plan = decide(record(1, g1=None), [], {}, memory, now=0, wall=WALL)
    assert plan.submit is None
    assert plan.observation["status"] == "uncertain"

    plan = decide(
        record(1, g1=None), [], {}, memory, now=UNCERTAINTY_SECONDS, wall=WALL
    )

    assert plan.submit == 2
    # Superseded, but possibly still on its way: still owed.
    assert plan.record.attempts == {1: Attempt(None, WALL), 2: Attempt(None, WALL)}


def test_finding_generation_two_does_not_account_for_generation_one():
    plan = decide(
        record(2, g1=None, g2="b"),
        ids(("b", 2, "RUNNING")),
        {"b": "RUNNING"},
        Memory(),
        10_000,
        WALL,
    )

    assert plan.record is None  # nothing changed: generation 1 is still owed
    assert plan.observation["unresolved"] == [1]


def test_a_superseded_attempt_leaves_only_when_confirmed_stopped():
    unknown = decide(record(2, g1="a", g2="b"), [], {"b": "RUNNING"}, Memory(), 0, WALL)
    assert unknown.record is None

    gone = decide(
        record(2, g1="a", g2="b"),
        [],
        {"a": NOT_FOUND, "b": "RUNNING"},
        Memory(),
        0,
        WALL,
    )
    assert gone.record.attempts == {2: Attempt("b", WALL)}

    stopped = decide(
        record(2, g1="a", g2="b"),
        [],
        {"a": "FAILED", "b": "RUNNING"},
        Memory(),
        0,
        WALL,
    )
    assert stopped.record.attempts == {2: Attempt("b", WALL)}


def test_resubmissions_back_off_and_the_count_resets_after_a_healthy_minute():
    memory = Memory()
    stopped = record(2, failures=1, g2="b")
    early = decide(stopped, [], {"b": "FAILED"}, memory, now=0, wall=WALL)
    assert early.submit is None
    assert "backing off" in early.observation["status"]

    later = decide(
        stopped, [], {"b": "FAILED"}, memory, job_keeper.BACKOFF_INITIAL_SECONDS, WALL
    )
    assert later.submit == 3 and later.record.failures == 2

    memory = Memory()
    running = record(3, failures=2, g3="c")
    assert decide(running, [], {"c": "RUNNING"}, memory, 0, WALL).record is None
    healthy = decide(
        running, [], {"c": "RUNNING"}, memory, job_keeper.HEALTHY_AFTER_SECONDS, WALL
    )
    assert healthy.record.failures == 0


def test_the_healthy_minute_restarts_when_the_job_restarts():
    memory = Memory()
    running = record(3, failures=2, g3="c")
    decide(running, [], {"c": "RUNNING"}, memory, 0, WALL)
    decide(running, [], {"c": "RESTARTING"}, memory, 50, WALL)
    plan = decide(running, [], {"c": "RUNNING"}, memory, 70, WALL)

    assert plan.record is None  # 20s of running, not 70


def test_a_name_that_is_not_ours_is_ignored():
    assert job_keeper.generation_of("jobs", "jobs-g12") == 12
    assert job_keeper.generation_of("jobs", "jobs-g") is None
    assert job_keeper.generation_of("jobs", "other-g1") is None
    assert job_keeper.generation_of("jobs", "jobs-g1-copy") is None


# --- the cycle, against fake ZooKeeper and Flink --------------------------------


class FakeFlink:
    def __init__(self):
        self.jobs: dict[str, dict] = {}
        self.omit: set[str] = set()
        self.overview_fails = False
        self.lookup_fails: set[str] = set()
        self.cancelled: list[str] = []
        self.submitted: list[Settings] = []
        self.outcome = "ok"  # ok | uncertain | never-ran
        self.before_submit = None
        self.on_overview = None
        self._next = 0

    def add(self, name, state="RUNNING") -> str:
        self._next += 1
        job_id = f"{self._next:032x}"
        self.jobs[job_id] = {"name": name, "state": state}
        return job_id

    def jobs_overview(self):
        if self.on_overview:
            self.on_overview()
        if self.overview_fails:
            raise flink_cluster.FlinkError("connection refused")
        return [
            {"id": i, "name": j["name"], "state": j["state"]}
            for i, j in self.jobs.items()
            if i not in self.omit
        ]

    def job_state(self, job_id):
        if job_id in self.lookup_fails:
            raise flink_cluster.FlinkError("HTTP Error 500")
        if job_id not in self.jobs:
            raise flink_cluster.NotFound(f"GET /jobs/{job_id}: 404")
        return self.jobs[job_id]["state"]

    def cancel(self, job_id):
        self.cancelled.append(job_id)
        self.jobs[job_id]["state"] = "CANCELED"

    def submit(self, settings, started=None):
        if self.before_submit:
            self.before_submit(settings, started)
        self.submitted.append(settings)
        if self.outcome == "never-ran":
            raise flink_cluster.FlinkError("could not run the submission")
        job_id = self.add(settings.job_name)
        if self.outcome == "uncertain":
            raise flink_cluster.SubmissionUncertain("timed out without a job id")
        return job_id


class FakeLeadership:
    may_work = True


def settings(generation: int) -> Settings:
    return Settings(
        job_name=job_keeper.job_name(BASE, generation), generation=generation
    )


@pytest.fixture
def keeper(zookeeper):
    client, paths = zookeeper
    flink = FakeFlink()
    clock = [0.0]
    made = Keeper(
        client,
        paths,
        FakeLeadership(),
        flink=flink,
        base=BASE,
        settings=settings,
        interval=0.01,
        clock=lambda: clock[0],
    )
    epoch = coordination.claim_epoch(client, paths)
    return made, flink, Tenure(epoch), client, paths, clock


def stored(client, paths) -> Record:
    raw, _version = coordination.read_flink_record(client, paths)
    return Record.parse(raw)


def test_the_intent_is_recorded_before_the_submission_and_the_id_after(keeper):
    made, flink, tenure, client, paths, _clock = keeper
    seen = {}
    flink.before_submit = lambda s, _started: seen.update(
        at_submit=stored(client, paths)
    )

    made.cycle(tenure, Memory())

    assert seen["at_submit"].generation == 1
    assert seen["at_submit"].attempts[1].job_id is None
    assert flink.submitted[0].job_name == "jobs-g1"
    assert flink.submitted[0].generation == 1
    after = stored(client, paths)
    assert after.attempts[1].job_id == next(iter(flink.jobs))
    assert after.epoch == tenure.epoch


def test_a_deposed_leaders_intent_is_rejected_so_it_never_submits(keeper):
    made, flink, tenure, client, paths, _clock = keeper
    coordination.claim_epoch(client, paths)  # someone else leads now

    with pytest.raises(StaleLeader):
        made.cycle(tenure, Memory())

    assert flink.submitted == []


def test_a_decision_from_a_stale_read_is_rejected_by_the_record_version(keeper):
    made, flink, tenure, client, paths, _clock = keeper

    def meanwhile():
        _raw, version = coordination.read_flink_record(client, paths)
        coordination.write_flink_record(
            client, paths, tenure.epoch, {"generation": 5, "attempts": {}}, version
        )

    flink.on_overview = meanwhile

    with pytest.raises(RecordChanged):
        made.cycle(tenure, Memory())

    assert flink.submitted == []


def test_an_uncertain_submission_stays_owed_and_is_found_by_name_later(keeper):
    made, flink, tenure, client, paths, _clock = keeper
    flink.outcome = "uncertain"
    memory = Memory()

    made.cycle(tenure, memory)
    assert stored(client, paths).attempts[1].job_id is None

    flink.outcome = "ok"
    made.cycle(tenure, memory)

    assert stored(client, paths).attempts[1].job_id == next(iter(flink.jobs))
    assert len(flink.submitted) == 1


def test_a_submission_that_never_ran_removes_its_attempt(keeper):
    made, flink, tenure, client, paths, _clock = keeper
    flink.outcome = "never-ran"
    made.cycle(tenure, Memory())

    after = stored(client, paths)
    assert after.generation == 1 and after.attempts == {}


def test_flink_failures_become_status_not_exceptions(keeper):
    made, flink, tenure, client, paths, _clock = keeper
    made.cycle(tenure, Memory())
    job = next(iter(flink.jobs))

    flink.overview_fails = True
    assert made.cycle(tenure, Memory()) == "unavailable"

    flink.overview_fails = False
    flink.omit.add(job)
    flink.lookup_fails.add(job)
    assert made.cycle(tenure, Memory()) == "unknown"
    assert stored(client, paths).generation == 1
    assert made.observation(tenure.epoch)["status"] == "unknown"


def test_omitted_from_two_listings_but_running_means_no_new_generation(keeper):
    made, flink, tenure, client, paths, clock = keeper
    memory = Memory()
    made.cycle(tenure, memory)
    flink.omit.add(next(iter(flink.jobs)))

    for step in (1, 2):
        clock[0] = step * 1000.0
        assert made.cycle(tenure, memory) == "running"

    assert stored(client, paths).generation == 1
    assert len(flink.submitted) == 1


def test_a_cancelled_job_is_replaced_by_the_next_generation(keeper):
    made, flink, tenure, client, paths, _clock = keeper
    memory = Memory()
    made.cycle(tenure, memory)
    flink.cancel(next(iter(flink.jobs)))

    made.cycle(tenure, memory)

    after = stored(client, paths)
    assert after.generation == 2 and set(after.attempts) == {2}
    assert [s.job_name for s in flink.submitted] == ["jobs-g1", "jobs-g2"]


def test_a_late_superseded_generation_is_cancelled_when_it_lands(keeper):
    made, flink, tenure, client, paths, clock = keeper
    memory = Memory()
    flink.outcome = "uncertain"
    made.cycle(tenure, memory)  # generation 1 uncertain...
    late = next(iter(flink.jobs))
    del flink.jobs[late]  # ...and not visible yet
    flink.outcome = "ok"
    made.cycle(tenure, memory)  # first sight of it, unresolved: wait
    assert len(flink.submitted) == 1
    clock[0] = UNCERTAINTY_SECONDS
    made.cycle(tenure, memory)  # superseded by generation 2, kept as owed
    assert stored(client, paths).attempts[1].job_id is None

    flink.jobs[late] = {"name": "jobs-g1", "state": "RUNNING"}  # it lands
    made.cycle(tenure, memory)  # found by name, recorded, and cancelled

    assert late in flink.cancelled
    assert stored(client, paths).attempts[1].job_id == late
    made.cycle(tenure, memory)  # confirmed stopped: no longer owed
    assert set(stored(client, paths).attempts) == {2}


# --- who may act --------------------------------------------------------------


def test_a_stale_keeper_resuming_after_its_successor_cancels_nothing(zookeeper):
    """Keeper A reads generation 1 and blocks; B supersedes and submits
    generation 2; A resumes, lists, and must leave B's job alone."""
    client, paths = zookeeper
    flink = FakeFlink()

    def make() -> Keeper:
        return Keeper(
            client, paths, FakeLeadership(), flink=flink, base=BASE, settings=settings
        )

    a, b = make(), make()
    a_tenure = Tenure(coordination.claim_epoch(client, paths))
    a.cycle(a_tenure, Memory())  # generation 1, running
    first = next(iter(flink.jobs))

    paused, resume = threading.Event(), threading.Event()
    listings = {"a": 0}

    def a_overview():
        listings["a"] += 1
        paused.set()
        resume.wait(5)
        return FakeFlink.jobs_overview(flink)

    a_flink = type("AFlink", (), {})()
    a_flink.jobs_overview = a_overview
    a_flink.job_state, a_flink.cancel, a_flink.submit = (
        flink.job_state,
        flink.cancel,
        flink.submit,
    )
    a._flink = a_flink

    failures = []

    def run_a():
        try:
            a.cycle(a_tenure, Memory())
        except Exception as error:  # noqa: BLE001 - reported below
            failures.append(error)

    thread = threading.Thread(target=run_a)
    thread.start()
    assert paused.wait(5)  # A has read generation 1

    flink.jobs[first]["state"] = "CANCELED"  # generation 1 stops
    b_tenure = Tenure(coordination.claim_epoch(client, paths))
    b.cycle(b_tenure, Memory())  # B supersedes: generation 2
    second = [i for i, j in flink.jobs.items() if j["name"] == "jobs-g2"][0]

    resume.set()
    thread.join(5)

    assert failures == []
    assert second not in flink.cancelled
    assert flink.jobs[second]["state"] == "RUNNING"

    # From a fresh read, A adopts generation 2 rather than touching it; and if
    # A wanted to replace it, its intent write would be rejected first.
    a._flink = flink
    a.cycle(a_tenure, Memory())
    assert second not in flink.cancelled
    flink.jobs[second]["state"] = "FAILED"
    waited_out = Memory(first_seen={("stopped", 2): float("-inf")})  # past the backoff
    with pytest.raises(StaleLeader):
        a.cycle(a_tenure, waited_out)
    assert [s.job_name for s in flink.submitted] == ["jobs-g1", "jobs-g2"]


class Spy:
    """Records the epoch of every fenced write the keeper issues."""

    def __init__(self, monkeypatch):
        self.writes: list[int] = []
        real = coordination.flink_record_transaction

        def issue(client, paths, epoch, record, version):
            self.writes.append(epoch)
            return real(client, paths, epoch, record, version)

        monkeypatch.setattr(coordination, "flink_record_transaction", issue)


class HeldProcess:
    def __init__(self, release: threading.Event):
        self.release, self.terminated = release, False

    def poll(self):
        return None if not self.release.is_set() else 0

    def terminate(self):
        self.terminated = True
        self.release.set()


def wait_until(condition, seconds=5.0) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.01)
    return False


def test_a_tenures_work_ends_with_it_through_a_new_tenure_and_shutdown(
    zookeeper, monkeypatch
):
    """Blocked submission -> tenure lost -> new tenure -> shutdown."""
    client, paths = zookeeper
    flink = FakeFlink()
    spy = Spy(monkeypatch)
    release = threading.Event()
    processes: list[HeldProcess] = []
    blocked = threading.Event()

    def hold(_settings, started):
        process = HeldProcess(release)
        processes.append(process)
        started(process)
        blocked.set()
        release.wait(10)
        if process.terminated:
            raise flink_cluster.SubmissionUncertain("client terminated")

    flink.before_submit = hold
    made = Keeper(
        client,
        paths,
        FakeLeadership(),
        flink=flink,
        base=BASE,
        settings=settings,
        interval=0.01,
    )
    made.start()
    try:
        first = Tenure(coordination.claim_epoch(client, paths))
        made.serve(first)
        assert blocked.wait(5)  # generation 1 intent written, submission held
        writes_before = list(spy.writes)
        assert writes_before == [first.epoch]

        first.retire()  # the tenure ends while its submission is in flight
        second = Tenure(coordination.claim_epoch(client, paths))
        made.serve(second)
        time.sleep(0.1)
        assert spy.writes == writes_before  # nothing for the new tenure yet

        blocked.clear()
        release.set()  # the old submission returns, with an id
        # Its outcome is not written: generation 1 stays owed. The new tenure
        # then finds it by name and records it under its own epoch.
        assert wait_until(
            lambda: stored(client, paths).attempts.get(1, Attempt(None)).job_id
        )
        assert first.epoch not in spy.writes[len(writes_before) :]
        assert made.observation(first.epoch) is None
        assert wait_until(lambda: made.observation(second.epoch) is not None)

        # And at shutdown, with another submission in flight.
        release.clear()
        flink.cancel(next(iter(flink.jobs)))
        assert blocked.wait(5)
        writes_at_stop = len(spy.writes)
        started = time.monotonic()
        made.stop()
        assert time.monotonic() - started < 5  # inside Honcho's grace period
        assert processes[-1].terminated
        time.sleep(0.1)
        assert len(spy.writes) == writes_at_stop  # no ZooKeeper access after stop
    finally:
        release.set()
        made.stop()


# --- second review: the lock, the pause, and arithmetic ------------------------


class HeldTransaction:
    """A transaction whose commit never completes until released."""

    def __init__(self, release: threading.Event, started: threading.Event):
        self.release, self.started = release, started

    def check(self, path, version):
        pass

    def set_data(self, path, value, version=-1):
        pass

    def commit(self):  # the synchronous path: holds whoever calls it
        self.started.set()
        self.release.wait(30)
        raise ConnectionLoss()

    def commit_async(self):
        self.started.set()
        held = self

        class Pending:
            def get(self, timeout=None):
                if not held.release.wait(timeout):
                    raise KazooTimeoutError("held")
                raise ConnectionLoss()

        return Pending()


def test_a_blocked_zookeeper_write_blocks_neither_shutdown_nor_observation(
    zookeeper, monkeypatch
):
    client, paths = zookeeper
    flink = FakeFlink()
    release, started = threading.Event(), threading.Event()
    monkeypatch.setattr(job_keeper, "WRITE_TIMEOUT_SECONDS", 30)
    monkeypatch.setattr(
        client, "transaction", lambda: HeldTransaction(release, started)
    )
    made = Keeper(
        client,
        paths,
        FakeLeadership(),
        flink=flink,
        base=BASE,
        settings=settings,
        interval=0.01,
    )
    made.start()
    tenure = Tenure(coordination.claim_epoch(client, paths))
    try:
        made.serve(tenure)
        assert started.wait(5)  # the intent write is in flight, and held

        begun = time.monotonic()
        assert made.observation(tenure.epoch) is None
        made.stop()
        assert time.monotonic() - begun < 3  # neither waited for the write
    finally:
        release.set()
    assert flink.submitted == []  # an unconfirmed intent never leads to a submission


def test_an_unconfirmed_intent_write_is_uncertain_and_submits_nothing(
    keeper, monkeypatch
):
    made, flink, tenure, client, paths, _clock = keeper
    release, started = threading.Event(), threading.Event()
    monkeypatch.setattr(job_keeper, "WRITE_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(
        client, "transaction", lambda: HeldTransaction(release, started)
    )

    with pytest.raises(job_keeper.WriteUncertain):
        made.cycle(tenure, Memory())

    assert flink.submitted == []


class Suspending:
    """Leadership that becomes unknown the moment a given write returns."""

    def __init__(self):
        self.may_work = True


def test_a_keeper_suspended_after_its_intent_does_not_start_the_submission(
    zookeeper, monkeypatch
):
    client, paths = zookeeper
    flink = FakeFlink()
    leadership = Suspending()
    real = coordination.flink_record_outcome

    def suspend_after(*args, **kwargs):
        version = real(*args, **kwargs)
        leadership.may_work = False  # SUSPENDED, just as the intent landed
        return version

    monkeypatch.setattr(coordination, "flink_record_outcome", suspend_after)
    made = Keeper(client, paths, leadership, flink=flink, base=BASE, settings=settings)
    tenure = Tenure(coordination.claim_epoch(client, paths))
    memory = Memory()

    with pytest.raises(job_keeper.Interrupted):
        made.cycle(tenure, memory)
    assert flink.submitted == []
    assert stored(client, paths).attempts == {
        1: Attempt(None, stored(client, paths).attempts[1].submitted_at)
    }

    # Reconnected, same session, same tenure: this keeper knows generation 1 was
    # never submitted, so it need not wait out the uncertainty window for it.
    monkeypatch.setattr(coordination, "flink_record_outcome", real)
    leadership.may_work = True
    made.cycle(tenure, memory)

    after = stored(client, paths)
    assert [s.job_name for s in flink.submitted] == ["jobs-g2"]
    assert set(after.attempts) == {2} and after.attempts[2].job_id is not None


def test_waits_saturate_rather_than_overflow():
    """An overnight outage reached 2**1024; so would 1,025 resubmissions."""
    assert job_keeper.quiet_wait(5.0, 10_000) == job_keeper.UNAVAILABLE_MAX_SECONDS
    assert job_keeper.backoff(10_000) == job_keeper.BACKOFF_MAX_SECONDS
    assert job_keeper.quiet_wait(5.0, 1) == 10.0


class AppliedButUnacknowledged:
    """ZooKeeper applies the transaction; its reply never arrives in time."""

    def __init__(self, real):
        self.real = real

    def check(self, path, version):
        self.real.check(path, version)

    def set_data(self, path, value, version=-1):
        self.real.set_data(path, value, version)

    def commit_async(self):
        self.real.commit()  # applied server-side

        class Lost:
            def get(self, timeout=None):
                raise KazooTimeoutError("no reply")

        return Lost()


def test_an_intent_applied_but_unacknowledged_is_known_never_submitted(
    keeper, monkeypatch
):
    """The write's outcome is unknown; that this tenure never called Flink is not.
    So recovery under the same tenure neither waits out the uncertainty window
    nor leaves a phantom attempt behind."""
    made, flink, tenure, client, paths, clock = keeper
    real_transaction = client.transaction
    monkeypatch.setattr(
        client, "transaction", lambda: AppliedButUnacknowledged(real_transaction())
    )
    memory = Memory()

    with pytest.raises(job_keeper.WriteUncertain):
        made.cycle(tenure, memory)
    assert flink.submitted == []
    assert stored(client, paths).attempts[1].job_id is None  # it did land

    monkeypatch.setattr(client, "transaction", real_transaction)  # replies return
    made.cycle(tenure, memory)  # the clock has not moved: no window waited out
    assert [s.job_name for s in flink.submitted] == ["jobs-g2"]
    for _ in range(3):
        clock[0] += 1000.0  # healthy cycles after recovery
        made.cycle(tenure, memory)

    after = stored(client, paths)
    assert [s.job_name for s in flink.submitted] == ["jobs-g2"]
    assert set(after.attempts) == {2} and after.attempts[2].job_id is not None


def test_an_unacknowledged_id_write_after_a_submission_stays_conservative(
    keeper, monkeypatch
):
    """The job exists, so its attempt stays owed until found — then recorded."""
    made, flink, tenure, client, paths, _clock = keeper
    real_transaction = client.transaction
    writes = []

    def second_write_unacknowledged():
        writes.append(1)
        return (
            AppliedButUnacknowledged(real_transaction())
            if len(writes) == 2
            else real_transaction()
        )

    monkeypatch.setattr(client, "transaction", second_write_unacknowledged)
    memory = Memory()

    with pytest.raises(job_keeper.WriteUncertain):
        made.cycle(tenure, memory)  # intent confirmed, submitted, id write lost
    assert 1 not in memory.never_submitted

    made.cycle(tenure, memory)

    assert [s.job_name for s in flink.submitted] == ["jobs-g1"]
    assert stored(client, paths).attempts[1].job_id == next(iter(flink.jobs))


class Unacknowledged:
    """A transaction whose reply never arrives — and which never applied."""

    def check(self, path, version):
        pass

    def set_data(self, path, value, version=-1):
        pass

    def commit_async(self):
        class Lost:
            def get(self, timeout=None):
                raise KazooTimeoutError("no reply")

        return Lost()


def test_a_submission_that_never_ran_is_known_even_if_its_removal_is_lost(
    keeper, monkeypatch
):
    made, flink, tenure, client, paths, _clock = keeper
    real_transaction = client.transaction
    writes = []

    def removal_lost():
        writes.append(1)
        return Unacknowledged() if len(writes) == 2 else real_transaction()

    monkeypatch.setattr(client, "transaction", removal_lost)
    flink.outcome = "never-ran"
    memory = Memory()

    with pytest.raises(job_keeper.WriteUncertain):
        made.cycle(tenure, memory)
    assert stored(client, paths).attempts[1].job_id is None  # still recorded

    flink.outcome = "ok"
    made.cycle(tenure, memory)  # no window waited out, no phantom left

    after = stored(client, paths)
    assert [s.job_name for s in flink.submitted] == ["jobs-g1", "jobs-g2"]
    assert set(after.attempts) == {2}


def test_a_generation_that_really_submits_loses_its_never_submitted_mark(
    keeper, monkeypatch
):
    """Unapplied intent timeout -> same-generation retry -> uncertain submission,
    the job absent from discovery: the earlier mark must not discard it."""
    made, flink, tenure, client, paths, clock = keeper
    real_transaction = client.transaction
    writes = []

    def first_write_lost():
        writes.append(1)
        return Unacknowledged() if len(writes) == 1 else real_transaction()

    monkeypatch.setattr(client, "transaction", first_write_lost)
    memory = Memory()

    with pytest.raises(job_keeper.WriteUncertain):
        made.cycle(tenure, memory)  # generation 1's intent lost, never applied
    assert stored(client, paths).generation == 0
    assert 1 in memory.never_submitted

    flink.outcome = "uncertain"
    made.cycle(tenure, memory)  # generation 1 again: intent written, submitted
    assert [s.job_name for s in flink.submitted] == ["jobs-g1"]
    flink.omit.update(flink.jobs)  # the job exists, but no listing shows it yet

    flink.outcome = "ok"
    made.cycle(tenure, memory)  # still inside the uncertainty window

    assert [s.job_name for s in flink.submitted] == ["jobs-g1"]
    assert stored(client, paths).attempts == {
        1: Attempt(None, stored(client, paths).attempts[1].submitted_at)
    }

    clock[0] = UNCERTAINTY_SECONDS  # the window passes: superseded, still owed
    made.cycle(tenure, memory)
    assert set(stored(client, paths).attempts) == {1, 2}
