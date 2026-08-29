import subprocess
import urllib.error

import pytest

from pipeline import smoke_test


@pytest.fixture(autouse=True)
def no_sleeping(monkeypatch):
    monkeypatch.setattr(smoke_test.time, "sleep", lambda seconds: None)


class FakeCompleted:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def test_wait_for_returns_true_as_soon_as_the_probe_passes():
    attempts = []

    def probe():
        attempts.append(1)
        return len(attempts) == 3

    assert smoke_test.wait_for(probe, timeout=10) is True
    assert len(attempts) == 3


def test_wait_for_gives_up_at_the_timeout(monkeypatch):
    clock = iter([0.0, 0.0, 5.0, 10.0, 20.0])
    monkeypatch.setattr(smoke_test.time, "monotonic", lambda: next(clock))

    assert smoke_test.wait_for(lambda: False, timeout=10) is False


def test_host_path_passes_on_a_200(monkeypatch):
    monkeypatch.setattr(smoke_test, "fetch", lambda url: (200, "Welcome"))

    passed, detail = smoke_test.host_path_serves_the_published_port()

    assert passed is True
    assert "200" in detail


def test_host_path_fails_when_unreachable(monkeypatch):
    def refuse(url):
        raise urllib.error.URLError("refused")

    monkeypatch.setattr(smoke_test, "fetch", refuse)
    monkeypatch.setattr(smoke_test, "wait_for", lambda probe, timeout=0: probe())

    passed, detail = smoke_test.host_path_serves_the_published_port()

    assert passed is False
    assert "timeout" in detail


def test_host_path_fails_on_a_non_200(monkeypatch):
    monkeypatch.setattr(smoke_test, "fetch", lambda url: (503, ""))
    monkeypatch.setattr(smoke_test, "wait_for", lambda probe, timeout=0: probe())

    passed, _ = smoke_test.host_path_serves_the_published_port()

    assert passed is False


def test_container_path_passes_on_expected_output(monkeypatch):
    monkeypatch.setattr(
        smoke_test.subprocess,
        "run",
        lambda *args, **kwargs: FakeCompleted(
            stdout="http://web -> 200 <!DOCTYPE html>"
        ),
    )

    passed, detail = smoke_test.container_path_serves_the_network_alias()

    assert passed is True
    assert "inside the network" in detail


def test_container_path_fails_on_a_non_zero_exit(monkeypatch):
    monkeypatch.setattr(
        smoke_test.subprocess,
        "run",
        lambda *args, **kwargs: FakeCompleted(returncode=1, stderr="boom"),
    )

    passed, detail = smoke_test.container_path_serves_the_network_alias()

    assert passed is False
    assert "boom" in detail


def test_container_path_fails_on_unexpected_output(monkeypatch):
    monkeypatch.setattr(
        smoke_test.subprocess,
        "run",
        lambda *args, **kwargs: FakeCompleted(stdout="http://web -> 502 Bad Gateway"),
    )

    passed, detail = smoke_test.container_path_serves_the_network_alias()

    assert passed is False
    assert "unexpected" in detail


def test_container_path_fails_when_the_command_hangs(monkeypatch):
    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired(cmd="docker", timeout=1)

    monkeypatch.setattr(smoke_test.subprocess, "run", timeout)

    passed, detail = smoke_test.container_path_serves_the_network_alias()

    assert passed is False
    assert "timeout" in detail


def test_container_path_fails_without_docker(monkeypatch):
    def missing(*args, **kwargs):
        raise FileNotFoundError

    monkeypatch.setattr(smoke_test.subprocess, "run", missing)

    passed, detail = smoke_test.container_path_serves_the_network_alias()

    assert passed is False
    assert "docker" in detail


def test_main_returns_zero_when_every_check_passes(monkeypatch, capsys):
    monkeypatch.setattr(smoke_test, "CHECKS", [("one", lambda: (True, "fine"))])

    assert smoke_test.main() == 0
    assert "all 1 checks passed" in capsys.readouterr().out


def test_main_returns_one_and_names_the_failure(monkeypatch, capsys):
    monkeypatch.setattr(
        smoke_test,
        "CHECKS",
        [("one", lambda: (True, "fine")), ("two", lambda: (False, "broken"))],
    )

    assert smoke_test.main() == 1
    captured = capsys.readouterr()
    assert "FAIL  two: broken" in captured.out
    assert "1 of 2 checks failed" in captured.err
