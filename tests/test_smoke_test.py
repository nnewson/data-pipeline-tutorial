import subprocess

import pytest
from kafka.errors import KafkaConnectionError, KafkaError

from pipeline import smoke_test


class FakeFuture:
    def __init__(self, error=None):
        self._error = error

    def get(self, timeout=None):
        if self._error:
            raise self._error
        return object()


class FakeProducer:
    def __init__(self, error=None):
        self._error = error
        self.closed = False

    def send(self, topic, value):
        return FakeFuture(self._error)

    def close(self):
        self.closed = True


class FakeCompleted:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def test_host_check_passes_when_the_event_returns(monkeypatch):
    monkeypatch.setattr(smoke_test, "KafkaProducer", lambda **kwargs: FakeProducer())
    monkeypatch.setattr(smoke_test, "_consume_until", lambda marker: {"marker": marker})

    passed, detail = smoke_test.host_listener_round_trips()

    assert passed is True
    assert "localhost:9092" in detail


def test_host_check_reports_a_broker_that_is_not_there(monkeypatch):
    def refuse(**kwargs):
        raise KafkaConnectionError("no brokers")

    monkeypatch.setattr(smoke_test, "KafkaProducer", refuse)

    passed, detail = smoke_test.host_listener_round_trips()

    assert passed is False
    assert "could not connect" in detail


def test_host_check_reports_a_failed_delivery(monkeypatch):
    # The regression this guards: flush() alone would not surface this.
    monkeypatch.setattr(
        smoke_test,
        "KafkaProducer",
        lambda **kwargs: FakeProducer(error=KafkaError("no leader")),
    )

    passed, detail = smoke_test.host_listener_round_trips()

    assert passed is False
    assert "produce to" in detail


def test_host_check_reports_an_event_that_never_arrives(monkeypatch):
    monkeypatch.setattr(smoke_test, "KafkaProducer", lambda **kwargs: FakeProducer())
    monkeypatch.setattr(smoke_test, "_consume_until", lambda marker: None)

    passed, detail = smoke_test.host_listener_round_trips()

    assert passed is False
    assert "did not come back" in detail


def test_consume_until_returns_none_when_the_broker_is_lost(monkeypatch):
    """A broker lost before consuming is a failed check, not a traceback."""

    def lose_broker(*args, **kwargs):
        raise KafkaConnectionError("broker lost")

    monkeypatch.setattr(smoke_test, "KafkaConsumer", lose_broker)

    assert smoke_test._consume_until("any-marker") is None


def test_consume_until_returns_none_when_reading_stops(monkeypatch):
    """A broker lost mid-read is handled the same way."""

    class FailingConsumer:
        def __iter__(self):
            raise KafkaConnectionError("broker lost mid-read")

        def close(self):
            pass

    monkeypatch.setattr(smoke_test, "KafkaConsumer", lambda *a, **k: FailingConsumer())

    assert smoke_test._consume_until("any-marker") is None


def test_host_check_survives_a_broker_lost_before_consuming(monkeypatch):
    """The named failure surfaces rather than an exception escaping main()."""
    monkeypatch.setattr(smoke_test, "KafkaProducer", lambda **kwargs: FakeProducer())

    def lose_broker(*args, **kwargs):
        raise KafkaConnectionError("broker lost")

    monkeypatch.setattr(smoke_test, "KafkaConsumer", lose_broker)

    passed, detail = smoke_test.host_listener_round_trips()

    assert passed is False
    assert "did not come back" in detail


def test_internal_check_passes(monkeypatch):
    monkeypatch.setattr(smoke_test.subprocess, "run", lambda *a, **k: FakeCompleted())
    monkeypatch.setattr(smoke_test, "_consume_until", lambda marker: {"marker": marker})

    passed, detail = smoke_test.internal_listener_reaches_the_same_broker()

    assert passed is True
    assert "same broker" in detail


def test_internal_check_reports_a_non_zero_exit(monkeypatch):
    monkeypatch.setattr(
        smoke_test.subprocess,
        "run",
        lambda *a, **k: FakeCompleted(returncode=1, stderr="broker unreachable"),
    )

    passed, detail = smoke_test.internal_listener_reaches_the_same_broker()

    assert passed is False
    assert "broker unreachable" in detail


def test_internal_check_reports_a_hang(monkeypatch):
    def hang(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd="docker", timeout=1)

    monkeypatch.setattr(smoke_test.subprocess, "run", hang)

    passed, detail = smoke_test.internal_listener_reaches_the_same_broker()

    assert passed is False
    assert "did not finish in time" in detail


def test_internal_check_reports_missing_docker(monkeypatch):
    def missing(*args, **kwargs):
        raise FileNotFoundError

    monkeypatch.setattr(smoke_test.subprocess, "run", missing)

    passed, detail = smoke_test.internal_listener_reaches_the_same_broker()

    assert passed is False
    assert "docker" in detail


def test_internal_check_reports_an_unreadable_event(monkeypatch):
    """Produced inside the network, not readable from the host: listeners disagree."""
    monkeypatch.setattr(smoke_test.subprocess, "run", lambda *a, **k: FakeCompleted())
    monkeypatch.setattr(smoke_test, "_consume_until", lambda marker: None)

    passed, detail = smoke_test.internal_listener_reaches_the_same_broker()

    assert passed is False
    assert "not readable" in detail


def test_main_returns_zero_when_every_check_passes(monkeypatch, capsys):
    monkeypatch.setattr(smoke_test, "ensure_topic", lambda *a, **k: None)
    monkeypatch.setattr(smoke_test, "CHECKS", [("one", lambda: (True, "fine"))])

    assert smoke_test.main() == 0
    assert "all 1 checks passed" in capsys.readouterr().out


def test_main_aggregates_failures_and_returns_one(monkeypatch, capsys):
    monkeypatch.setattr(smoke_test, "ensure_topic", lambda *a, **k: None)
    monkeypatch.setattr(
        smoke_test,
        "CHECKS",
        [
            ("one", lambda: (True, "fine")),
            ("two", lambda: (False, "broken")),
            ("three", lambda: (False, "also broken")),
        ],
    )

    assert smoke_test.main() == 1
    captured = capsys.readouterr()
    assert "FAIL  two: broken" in captured.out
    assert "2 of 3 checks failed" in captured.err


@pytest.mark.parametrize("name", ["host listener", "internal listener"])
def test_both_checks_are_registered(name):
    assert name in [label for label, _ in smoke_test.CHECKS]


def test_main_reports_a_missing_broker_without_a_traceback(monkeypatch, capsys):
    """No broker at all is a failed run, not an exception escaping the CLI."""

    def no_broker(*args, **kwargs):
        raise KafkaConnectionError("no brokers available")

    monkeypatch.setattr(smoke_test, "ensure_topic", no_broker)

    assert smoke_test.main() == 1
    captured = capsys.readouterr()
    assert "FAIL  setup" in captured.out
    assert "1 of 1 checks failed" in captured.err
