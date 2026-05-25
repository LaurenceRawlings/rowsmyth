import pytest


def test_resolve_count_exact(User):
    b = User.factory(10)
    assert b._resolve_count() == 10


def test_resolve_count_range(User):
    b = User.factory(5, 15)
    counts = {b._resolve_count() for _ in range(50)}
    assert min(counts) >= 5
    assert max(counts) <= 15
    assert len(counts) > 1  # randomness confirmed


def test_mix_validates_non_positive_proportion(User):
    with pytest.raises(ValueError, match="must be > 0"):
        User.factory(10).mix(admin=0.0)


def test_mix_validates_negative_proportion(User):
    with pytest.raises(ValueError, match="must be > 0"):
        User.factory(10).mix(admin=-0.1)


def test_mix_validates_sum(User):
    with pytest.raises(ValueError, match=r"must be ≤ 1\.0"):
        User.factory(10).mix(admin=0.8, extra=0.5)


def test_mix_validates_unknown_variant(User):
    with pytest.raises(ValueError, match="not defined on User"):
        User.factory(10).mix(nonexistent=0.5)


def test_mix_valid(User):
    b = User.factory(10).mix(admin=0.3)
    assert b._mix == {"admin": 0.3}


def test_pick_variant_all_one_variant(User):
    b = User.factory(10).mix(admin=1.0)
    assert b._pick_variant() == "admin"


def test_pick_variant_none_when_no_mix(User):
    b = User.factory(10)
    assert b._pick_variant() is None


def test_has_attaches_children(User, Order):
    b = User.factory(10).has(Order.factory(2))
    assert len(b._children) == 1
    child_builder, via = b._children[0]
    assert child_builder.model is Order
    assert via is None


def test_has_multiple_children(User, Order):
    b = User.factory(10).has(Order.factory(1), Order.factory(2))
    assert len(b._children) == 2


def test_has_with_via(User, Order):
    b = User.factory(10).has(Order.factory(2), via="user")
    _, via = b._children[0]
    assert via == "user"


def test_has_returns_self_for_chaining(User, Order):
    b = User.factory(10)
    result = b.has(Order.factory(2))
    assert result is b


def test_pick_variant_returns_none_in_unassigned_tail(User):
    from unittest.mock import patch

    b = User.factory(10).mix(admin=0.3)
    # random() returns 0.9 > cumulative (0.3), so the loop exhausts without picking
    with patch("rowsmyth._builder.random.random", return_value=0.9):
        assert b._pick_variant() is None
