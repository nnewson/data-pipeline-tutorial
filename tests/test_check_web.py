import urllib.error

import pytest

from pipeline import check_web


class FakeResponse:
    def __init__(self, status, body):
        self.status = status
        self._body = body.encode("utf-8")

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


def test_fetch_returns_status_and_first_non_blank_line(monkeypatch):
    monkeypatch.setattr(
        check_web.urllib.request,
        "urlopen",
        lambda url, timeout: FakeResponse(200, "\n\n  Welcome  \nsecond line\n"),
    )

    assert check_web.fetch("http://web") == (200, "  Welcome  ")


def test_fetch_handles_an_empty_body(monkeypatch):
    monkeypatch.setattr(
        check_web.urllib.request,
        "urlopen",
        lambda url, timeout: FakeResponse(204, ""),
    )

    assert check_web.fetch("http://web") == (204, "")


def test_main_reports_success(monkeypatch, capsys):
    monkeypatch.setattr(check_web.config, "WEB_URL", "http://web")
    monkeypatch.setattr(check_web, "fetch", lambda url: (200, "Welcome"))

    assert check_web.main() == 0
    assert capsys.readouterr().out == "http://web -> 200 Welcome\n"


@pytest.mark.parametrize(
    "error",
    [urllib.error.URLError("refused"), OSError("no route to host")],
)
def test_main_reports_an_unreachable_service(monkeypatch, capsys, error):
    monkeypatch.setattr(check_web.config, "WEB_URL", "http://web")

    def raise_error(url):
        raise error

    monkeypatch.setattr(check_web, "fetch", raise_error)

    assert check_web.main() == 1
    assert "unreachable" in capsys.readouterr().err
