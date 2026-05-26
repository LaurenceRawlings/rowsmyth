"""End-to-end integration tests for the rowsmyth public API.

Scenarios progress from a bare zero-config model to a fully-specified
multi-model dataset. Each test is independent.
"""


def test_minimal_factory_no_generators(Minimal):
    """Minimal working example: model with no generators, no variants, no FKs."""
    things = Minimal.factory(3).random_seed(1).create()
    assert len(things) == 3
    assert all(isinstance(t, Minimal) for t in things)


def test_factory_with_generators(User):
    users = User.factory(5).random_seed(1).create()
    assert len(users) == 5
    assert all(isinstance(u.name, str) and u.name for u in users)
    assert all(u.tier in ("standard", "premium") for u in users)


def test_factory_range_count(User):
    users = User.factory(3, 8).random_seed(1).create()
    assert 3 <= len(users) <= 8


def test_factory_where_override(User):
    users = User.factory(10).where({User.name: "fixed"}).random_seed(1).create()
    assert all(u.name == "fixed" for u in users)


def test_factory_variant_all_admin(User):
    users = User.factory(20).mix(admin=1.0).random_seed(1).create()
    assert all(u.name == "admin" and u.tier == "premium" for u in users)


def test_factory_partial_variant_mix(User):
    users = User.factory(100).mix(admin=0.2).random_seed(42).create()
    admins = [u for u in users if u.name == "admin"]
    assert 0 < len(admins) < 100


def test_factory_seed_reproducible(User):
    names_a = [u.name for u in User.factory(5).random_seed(7).create()]
    names_b = [u.name for u in User.factory(5).random_seed(7).create()]
    assert names_a == names_b


def test_factory_children_exact_count_fk_wired(User, Order):
    users = User.factory(3).has(Order.factory(2)).random_seed(1).create()
    assert len(users) == 3
    for user in users:
        assert len(user.orders) == 2
        assert all(o.user_id == user.id for o in user.orders)


def test_factory_children_range_count_fk_wired(User, Order):
    users = User.factory(5).has(Order.factory(1, 5)).random_seed(1).create()
    for user in users:
        assert 1 <= len(user.orders) <= 5
        assert all(o.user_id == user.id for o in user.orders)


def test_factory_deep_nesting_fk_wired(User, Order, OrderItem):
    users = (
        User
        .factory(2)
        .has(Order.factory(2).has(OrderItem.factory(3)))
        .random_seed(1)
        .create()
    )
    for user in users:
        assert len(user.orders) == 2
        for order in user.orders:
            assert len(order.items) == 3
            assert all(item.order_id == order.id for item in order.items)


def test_minimal_dataset_one_of_each(Base):
    data = Base.dataset().random_seed(1).create()
    assert set(data.keys()) == {"roles", "users", "orders", "order_items"}
    assert all(len(rows) == 1 for rows in data.values())
    assert data["orders"][0].user_id == data["users"][0].id


def test_dataset_with_builder_overrides(Base, User, Order, OrderItem):
    data = (
        Base
        .dataset(
            User.factory(5),
            Order.factory(10),
            OrderItem.factory(20),
        )
        .random_seed(42)
        .create()
    )
    assert len(data["users"]) == 5
    assert len(data["orders"]) == 10
    assert len(data["order_items"]) == 20
    user_ids = {u.id for u in data["users"]}
    order_ids = {o.id for o in data["orders"]}
    assert all(o.user_id in user_ids for o in data["orders"])
    assert all(i.order_id in order_ids for i in data["order_items"])


def test_full_complex_dataset(Base, User, Order, OrderItem):
    data = (
        Base
        .dataset(
            User.factory(5).mix(admin=0.2),
            Order.factory(10, 20),
            OrderItem.factory(30, 60),
        )
        .random_seed(42)
        .create()
    )
    assert len(data["users"]) == 5
    assert 10 <= len(data["orders"]) <= 20
    assert 30 <= len(data["order_items"]) <= 60
    user_ids = {u.id for u in data["users"]}
    order_ids = {o.id for o in data["orders"]}
    assert all(o.user_id in user_ids for o in data["orders"])
    assert all(i.order_id in order_ids for i in data["order_items"])
