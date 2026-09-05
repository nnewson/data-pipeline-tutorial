import pytest

from pipeline import get_partition


@pytest.mark.parametrize("partitions", [1, 2, 4])
def test_every_letter_lands_in_range(partitions):
    for letter in "abcdefghijklmnopqrstuvwxyz":
        assert 0 <= get_partition(letter, partitions) < partitions


def test_four_partitions_split_the_alphabet_evenly():
    # 26 letters over 4 partitions is 6.5 each: a-g, h-m, n-t, u-z.
    assert get_partition("alice", 4) == 0
    assert get_partition("gary", 4) == 0
    assert get_partition("hannah", 4) == 1
    assert get_partition("nadia", 4) == 2
    assert get_partition("tom", 4) == 2
    assert get_partition("zoe", 4) == 3


def test_routing_is_stable_for_the_same_user():
    # The property the whole design rests on: one user, always one partition,
    # so that user's events keep their order relative to each other.
    assert get_partition("alice", 4) == get_partition("alice", 4)
    assert get_partition("alice", 4) == get_partition("alison", 4)


def test_case_is_ignored():
    assert get_partition("Alice", 4) == get_partition("alice", 4)


def test_every_partition_is_reachable():
    reached = {get_partition(letter, 4) for letter in "abcdefghijklmnopqrstuvwxyz"}

    assert reached == {0, 1, 2, 3}


@pytest.mark.parametrize("username", ["", "123user", "_underscore"])
def test_unroutable_usernames_fall_back_to_partition_zero(username):
    assert get_partition(username, 4) == 0


def test_the_last_letter_cannot_overflow_the_partition_count():
    # 26 letters over 3 partitions leaves a remainder; z must not land on 3.
    assert get_partition("z", 3) == 2
