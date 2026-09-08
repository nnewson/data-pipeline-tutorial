import pytest

from pipeline import redis_store


class FakeRedis:
    def __init__(self):
        self.store = {}
        self.pinged = False

    def ping(self):
        self.pinged = True
        return True

    def incr(self, key):
        self.store[key] = int(self.store.get(key, 0)) + 1
        return self.store[key]

    def set(self, key, value):
        self.store[key] = value

    def get(self, key):
        return self.store.get(key)

    def delete(self, key):
        return 1 if self.store.pop(key, None) is not None else 0

    def scan_iter(self, match=None, count=None):
        prefix = match.rstrip("*") if match else ""
        return [key for key in list(self.store) if key.startswith(prefix)]

    def close(self):
        pass


def test_keys_carry_the_prefix():
    assert (
        redis_store.page_count_key("/docs", "smoke:abc:") == "smoke:abc:pageviews:/docs"
    )
    assert (
        redis_store.last_page_key("ada", "smoke:abc:") == "smoke:abc:user:last_page:ada"
    )


def test_connect_pings_before_returning(monkeypatch):
    """A lazy client constructs fine with nothing listening; PING is the proof."""
    client = FakeRedis()
    monkeypatch.setattr(redis_store.redis, "Redis", lambda **kwargs: client)

    returned = redis_store.connect()

    assert returned is client
    assert client.pinged is True


def test_connect_retries_until_the_ping_succeeds(monkeypatch):
    attempts = []

    class Refusing(FakeRedis):
        def ping(self):
            attempts.append(1)
            if len(attempts) < 3:
                raise ConnectionError("not yet")
            return True

    monkeypatch.setattr(redis_store.redis, "Redis", lambda **kwargs: Refusing())
    monkeypatch.setattr(
        redis_store.wait_for_connection.__globals__["time"], "sleep", lambda s: None
    )

    redis_store.connect()

    assert len(attempts) == 3


def _event(page="/pricing", user="ada"):
    return {"page": page, "user_id": user, "event_id": "e1"}


def test_recording_an_event_writes_both_branches():
    client = FakeRedis()

    redis_store.record_pageview(client, _event(), prefix="p:")

    assert client.store["p:pageviews:/pricing"] == 1
    assert client.store["p:user:last_page:ada"] == "/pricing"


def test_replaying_an_event_overcounts_but_converges():
    """The release in one test: INCR accumulates, SET does not."""
    client = FakeRedis()
    event = _event()

    redis_store.record_pageview(client, event, prefix="p:")
    redis_store.record_pageview(client, event, prefix="p:")

    # Same event twice: the counter is wrong...
    assert client.store["p:pageviews:/pricing"] == 2
    # ...and the last-page value is right.
    assert client.store["p:user:last_page:ada"] == "/pricing"


def test_a_user_moving_pages_keeps_the_latest():
    client = FakeRedis()

    redis_store.record_pageview(client, _event("/docs"), prefix="p:")
    redis_store.record_pageview(client, _event("/checkout"), prefix="p:")

    assert client.store["p:user:last_page:ada"] == "/checkout"
    assert client.store["p:pageviews:/docs"] == 1
    assert client.store["p:pageviews:/checkout"] == 1


def test_page_counts_reads_back_only_its_own_prefix():
    client = FakeRedis()
    redis_store.record_pageview(client, _event("/docs", "ada"), prefix="mine:")
    redis_store.record_pageview(client, _event("/docs", "bob"), prefix="theirs:")

    assert redis_store.page_counts(client, "mine:") == {"/docs": 1}
    assert redis_store.last_pages(client, "mine:") == {"ada": "/docs"}


def test_clear_removes_only_the_given_prefix():
    client = FakeRedis()
    redis_store.record_pageview(client, _event(user="ada"), prefix="mine:")
    redis_store.record_pageview(client, _event(user="bob"), prefix="theirs:")

    removed = redis_store.clear(client, "mine:")

    assert removed == 2
    assert redis_store.page_counts(client, "theirs:") == {"/pricing": 1}


@pytest.mark.parametrize("total", [1, 5, 20])
def test_counter_total_matches_events_applied(total):
    """The claim the demonstration rests on, with no replay involved."""
    client = FakeRedis()
    for n in range(total):
        redis_store.record_pageview(client, _event(page=f"/p{n % 3}"), prefix="p:")

    assert sum(redis_store.page_counts(client, "p:").values()) == total


def test_clear_refuses_an_empty_prefix():
    """A missing prefix must not become 'delete the whole database'."""
    client = FakeRedis()
    redis_store.record_pageview(client, _event(), prefix="mine:")

    with pytest.raises(ValueError, match="non-empty prefix"):
        redis_store.clear(client, "")

    assert redis_store.page_counts(client, "mine:") == {"/pricing": 1}


def test_connect_closes_a_client_whose_ping_failed(monkeypatch):
    """Each retry opens a client; a failed one must not leak its socket."""
    closed = []
    attempts = {"count": 0}

    class Refusing(FakeRedis):
        def ping(self):
            # Shared across clients: each retry constructs a fresh one.
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise redis_store.redis.ConnectionError("refused")
            return True

        def close(self):
            closed.append(self)

    monkeypatch.setattr(redis_store.redis, "Redis", lambda **kwargs: Refusing())
    monkeypatch.setattr(
        redis_store.wait_for_connection.__globals__["time"], "sleep", lambda s: None
    )

    returned = redis_store.connect()

    assert attempts["count"] == 3
    # The two failed clients were closed; the successful one is left open.
    assert len(closed) == 2
    assert returned not in closed
