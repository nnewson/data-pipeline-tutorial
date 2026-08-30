import importlib

from pipeline import config


def test_kafka_server_defaults_to_the_host_address(monkeypatch):
    monkeypatch.delenv("KAFKA_SERVER", raising=False)
    reloaded = importlib.reload(config)

    assert reloaded.KAFKA_SERVER == "localhost:9092"


def test_kafka_server_can_be_overridden_for_containers(monkeypatch):
    monkeypatch.setenv("KAFKA_SERVER", "kafka:29092")
    reloaded = importlib.reload(config)

    assert reloaded.KAFKA_SERVER == "kafka:29092"


def test_topic_defaults_and_overrides(monkeypatch):
    monkeypatch.delenv("KAFKA_TOPIC", raising=False)
    assert importlib.reload(config).KAFKA_TOPIC == "pageviews"

    monkeypatch.setenv("KAFKA_TOPIC", "other")
    assert importlib.reload(config).KAFKA_TOPIC == "other"


def test_producer_interval_is_numeric(monkeypatch):
    monkeypatch.setenv("PRODUCER_INTERVAL_SECONDS", "0.25")
    assert importlib.reload(config).PRODUCER_INTERVAL_SECONDS == 0.25


def test_config_is_restored_for_later_tests(monkeypatch):
    for name in ("KAFKA_SERVER", "KAFKA_TOPIC", "PRODUCER_INTERVAL_SECONDS"):
        monkeypatch.delenv(name, raising=False)
    importlib.reload(config)

    assert config.KAFKA_SERVER == "localhost:9092"
