"""Guardrails shared by the whole suite.

Unit tests must not open sockets. Three times now, adding a connection to a code
path a test already exercised turned a 0.4-second suite into a 30-second one —
the retry loop makes a missing stub look like a hang rather than a failure.

This turns that into an immediate, named error instead. A test that genuinely
needs the retry helper opts in with `@pytest.mark.allow_connect`.
"""

import pytest

from pipeline import (
    cassandra_store,
    jobs_queue,
    kafka_consumer,
    producer,
    redis_store,
)


class RealConnectionAttempted(BaseException):
    """Raised when a unit test would have opened a socket."""


# The modules holding their own reference to the retry helper. Patching these
# covers anything reaching the network through their connect() functions —
# schema's use of cassandra_store and jobs' use of jobs_queue, for instance.
#
# It does not cover everything, and the gaps are worth naming rather than
# implying. `topics` is one: it calls `ensure_topic`, which uses the central
# `pipeline.wait_for_connection` rather than a module-level copy, so it is not
# patched here. `wait_for_topic` is the same, and smoke_test constructs Kafka
# clients directly. Those call sites are stubbed per test.
CONNECTING_MODULES = (
    redis_store,
    cassandra_store,
    jobs_queue,
    producer,
    kafka_consumer,
)


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "allow_connect: test may call wait_for_connection"
    )


@pytest.fixture(autouse=True)
def no_real_connections(request, monkeypatch):
    if request.node.get_closest_marker("allow_connect"):
        return

    def refuse(name, connect, *args, **kwargs):
        # BaseException on purpose: several call sites catch Exception because
        # their cleanup is best effort, and they would swallow this and pass.
        # A harness violation is not an application error.
        raise RealConnectionAttempted(
            f"unit test tried to open a real {name} connection. "
            "Stub the helper that connects, or mark the test allow_connect."
        )

    patched = 0
    for module in CONNECTING_MODULES:
        if hasattr(module, "wait_for_connection"):
            monkeypatch.setattr(module, "wait_for_connection", refuse)
            patched += 1

    # Every listed module must still expose the helper: one that stops importing
    # it would silently drop out of the guard, which is how the retry loop got
    # back in before. This proves each entry is active — not that the list names
    # every path to the network.
    assert patched == len(CONNECTING_MODULES), (
        f"{len(CONNECTING_MODULES) - patched} guarded module(s) no longer expose "
        "wait_for_connection; update CONNECTING_MODULES"
    )

    # Kafka clients are constructed directly rather than through the helper, so
    # the guard cannot reach them; those call sites are stubbed per test.
