import graphlib

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from rowsmyth._execute import (
    _resolve_attr_names,
    check_cycles,
    collect_all_tables,
    collect_models,
    execute_builder,
    find_relationship,
    make_factory,
)


def test_no_cycle_passes(User, Order, OrderItem):
    builder = User.factory(5).has(Order.factory(2).has(OrderItem.factory(1)))
    check_cycles(builder)  # should not raise


def test_direct_cycle_raises(User, Order):
    a = User.factory(5)
    b = Order.factory(2)
    a._children.append((b, None))
    b._children.append((a, None))  # cycle: User -> Order -> User
    with pytest.raises(graphlib.CycleError):
        check_cycles(a)


def test_single_node_no_cycle(User):
    check_cycles(User.factory(10))


def test_collect_models_flat(User):
    builder = User.factory(5)
    models = collect_models(builder)
    assert models == {User}


def test_collect_models_tree(User, Order, OrderItem):
    builder = User.factory(5).has(Order.factory(2).has(OrderItem.factory(1)))
    models = collect_models(builder)
    assert models == {User, Order, OrderItem}


def test_collect_all_tables_includes_fk_deps(OrderItem):
    tables = collect_all_tables({OrderItem})
    table_names = {t.name for t in tables}
    assert "order_items" in table_names
    assert "orders" in table_names
    assert "users" in table_names


@pytest.fixture()
def mem_session(User):
    engine = create_engine("sqlite:///:memory:")
    User.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


def test_make_factory_creates_instance(mem_session, User):
    FactoryCls = make_factory(User, mem_session)
    user = FactoryCls()
    assert isinstance(user, User)
    assert isinstance(user.name, str)
    assert len(user.name) > 0


def test_resolve_attr_names_with_instrumented_attr(User):
    raw = {User.name: "test"}
    resolved = _resolve_attr_names(raw)
    assert resolved == {"name": "test"}


def test_resolve_attr_names_with_plain_string():
    raw = {"name": "test"}
    resolved = _resolve_attr_names(raw)
    assert resolved == {"name": "test"}


def test_find_relationship_success(User, Order):
    rel_key = find_relationship(Order, User)
    assert rel_key == "user"


def test_find_relationship_success_child_level(Order, OrderItem):
    rel_key = find_relationship(OrderItem, Order)
    assert rel_key == "order"


def test_find_relationship_no_match_raises(User, OrderItem):
    with pytest.raises(ValueError, match="No relationship from"):
        find_relationship(User, OrderItem)  # no FK from User to OrderItem


def test_find_relationship_via_bypasses_introspection(User, Order):
    rel_key = find_relationship(Order, User, via="user")
    assert rel_key == "user"


def test_execute_builder_exact_count(User):
    users = execute_builder(User.factory(5), {}, seed=1)
    assert len(users) == 5
    assert all(isinstance(u, User) for u in users)


def test_execute_builder_range_count(User):
    users = execute_builder(User.factory(3, 8), {}, seed=1)
    assert 3 <= len(users) <= 8


def test_execute_builder_override_wins(User):
    users = execute_builder(User.factory(3), {"name": "fixed"}, seed=1)
    assert all(u.name == "fixed" for u in users)


def test_execute_builder_variant_all_admin(User):
    builder = User.factory(10).mix(admin=1.0)
    users = execute_builder(builder, {}, seed=1)
    assert all(u.name == "admin" for u in users)


def test_execute_builder_override_beats_variant(User):
    builder = User.factory(5).mix(admin=1.0)
    users = execute_builder(builder, {"name": "override"}, seed=1)
    assert all(u.name == "override" for u in users)


def test_execute_builder_has_creates_children(User, Order):
    builder = User.factory(3).has(Order.factory(2))
    users = execute_builder(builder, {}, seed=1)
    assert len(users) == 3
    for user in users:
        assert len(user.orders) == 2
        assert all(o.user_id == user.id for o in user.orders)


def test_execute_builder_nested_has(User, Order, OrderItem):
    builder = User.factory(2).has(Order.factory(2).has(OrderItem.factory(3)))
    users = execute_builder(builder, {}, seed=1)
    assert len(users) == 2
    for user in users:
        assert len(user.orders) == 2
        for order in user.orders:
            assert len(order.items) == 3


def test_execute_builder_seed_reproducible(User):
    b = User.factory(5)
    first = [u.name for u in execute_builder(b, {}, seed=99)]
    second = [u.name for u in execute_builder(b, {}, seed=99)]
    assert first == second


def test_execute_builder_different_seeds_differ(User):
    b = User.factory(5)
    first = [u.name for u in execute_builder(b, {}, seed=1)]
    second = [u.name for u in execute_builder(b, {}, seed=2)]
    assert first != second


def test_find_relationship_ambiguous_raises():
    from sqlalchemy import ForeignKey
    from sqlalchemy.orm import Mapped, mapped_column, relationship

    from rowsmyth import declarative_base

    AmbBase = declarative_base()

    class Parent(AmbBase):
        __tablename__ = "parents_amb"
        id: Mapped[int] = mapped_column(primary_key=True)

    class Child(AmbBase):
        __tablename__ = "children_amb"
        id: Mapped[int] = mapped_column(primary_key=True)
        p1_id: Mapped[int] = mapped_column(ForeignKey(Parent.id))
        p2_id: Mapped[int] = mapped_column(ForeignKey(Parent.id))
        parent1: Mapped["Parent"] = relationship(foreign_keys=[p1_id])
        parent2: Mapped["Parent"] = relationship(foreign_keys=[p2_id])

    with pytest.raises(ValueError, match="Ambiguous"):
        find_relationship(Child, Parent)


def test_execute_builder_auto_creates_parent_from_generators(Order, User):
    orders = execute_builder(Order.factory(1), {}, seed=1)
    assert len(orders) == 1
    assert isinstance(orders[0].user, User)


def test_collect_all_tables_deduplicates_when_table_queued_twice(Order, OrderItem):
    # Order and OrderItem both eventually reference User's table.
    # Starting with both in the queue means Order's table will be added a second
    # time when OrderItem is processed, triggering the dedup guard.
    tables = collect_all_tables({Order, OrderItem})
    table_names = {t.name for t in tables}
    assert "orders" in table_names
    assert "order_items" in table_names
    assert "users" in table_names
