import importlib

from pipeline import config


def test_web_url_defaults_to_the_host_address(monkeypatch):
    monkeypatch.delenv("WEB_URL", raising=False)
    reloaded = importlib.reload(config)

    assert reloaded.WEB_URL == "http://localhost:8080"


def test_web_url_can_be_overridden(monkeypatch):
    monkeypatch.setenv("WEB_URL", "http://web")
    reloaded = importlib.reload(config)

    assert reloaded.WEB_URL == "http://web"


def test_config_is_restored_for_later_tests(monkeypatch):
    monkeypatch.delenv("WEB_URL", raising=False)
    importlib.reload(config)

    assert config.WEB_URL == "http://localhost:8080"
