import pytest
from sqlalchemy.orm import Mapped, mapped_column


def test_dataset_returns_dict_keyed_by_tablename(Base, User):
    data = Base.dataset().random_seed(1).create()
    assert "users" in data


def test_dataset_topo_sorts_fk_order(Base, User, Order):
    data = (
        Base
        .dataset(
            Order.factory(3),
            User.factory(5),
        )
        .random_seed(1)
        .create()
    )
    assert len(data["users"]) == 5
    assert len(data["orders"]) == 3


def test_dataset_fk_pool_sampling(Base, User, Order):
    data = (
        Base
        .dataset(
            User.factory(5),
            Order.factory(20),
        )
        .random_seed(42)
        .create()
    )
    user_ids = {u.id for u in data["users"]}
    for order in data["orders"]:
        assert order.user_id in user_ids


def test_dataset_seed_reproducible(Base, User):
    def run():
        return Base.dataset(User.factory(3)).random_seed(7).create()

    first = [u.name for u in run()["users"]]
    second = [u.name for u in run()["users"]]
    assert first == second


def test_dataset_three_level_fk(Base, User, Order, OrderItem):
    data = (
        Base
        .dataset(
            User.factory(3),
            Order.factory(6),
            OrderItem.factory(12),
        )
        .random_seed(1)
        .create()
    )
    order_ids = {o.id for o in data["orders"]}
    for item in data["order_items"]:
        assert item.order_id in order_ids


def test_dataset_raises_when_parent_pool_is_empty(Base, User, Order):
    with pytest.raises(ValueError, match="has 0 instances"):
        Base.dataset(
            User.factory(0),
            Order.factory(3),
        ).create()


def test_dataset_skips_self_referential_relationship():
    from typing import Optional

    from sqlalchemy import ForeignKey
    from sqlalchemy.orm import Mapped, mapped_column, relationship

    from rowsmyth import declarative_base

    SelfBase = declarative_base()

    class Category(SelfBase):
        __tablename__ = "categories_selfref"
        id: Mapped[int] = mapped_column(primary_key=True)
        parent_id: Mapped[int | None] = mapped_column(
            ForeignKey("categories_selfref.id"), nullable=True
        )
        parent: Mapped[Optional["Category"]] = relationship(remote_side="Category.id")

    data = SelfBase.dataset().random_seed(1).create()
    assert "categories_selfref" in data
    assert len(data["categories_selfref"]) == 1
    assert data["categories_selfref"][0].parent_id is None


def test_dataset_accepts_raw_instances(Base, Role):
    roles = [Role(name="user"), Role(name="admin")]
    data = Base.dataset(*roles).random_seed(1).create()
    assert len(data["roles"]) == 2
    assert {r.name for r in data["roles"]} == {"user", "admin"}


def test_dataset_rejects_unregistered_instance(Base):
    class Stranger:
        pass

    with pytest.raises(TypeError, match="registered model instance"):
        Base.dataset(Stranger())


def test_dataset_rejects_unregistered_instance_from_other_base():
    from rowsmyth import declarative_base

    OtherBase = declarative_base()

    class Thing(OtherBase):
        __tablename__ = "things_other"
        id: Mapped[int] = mapped_column(primary_key=True)

    MainBase = declarative_base()

    class Widget(MainBase):
        __tablename__ = "widgets_other"
        id: Mapped[int] = mapped_column(primary_key=True)

    with pytest.raises(TypeError, match="registered model instance"):
        MainBase.dataset(Thing())


def test_dataset_seeded_instances_wired_as_fk_targets(Base, Role, User):
    roles = [Role(id=1, name="user"), Role(id=2, name="admin")]
    data = (
        Base
        .dataset(
            *roles,
            User.factory(10),
        )
        .random_seed(42)
        .create()
    )
    role_ids = {r.id for r in data["roles"]}
    assert role_ids == {1, 2}
    assert len(data["users"]) == 10
    for user in data["users"]:
        if user.role_id is not None:
            assert user.role_id in role_ids


def test_dataset_seeded_rows_appear_first_in_result(Base, Role, User):
    roles = [Role(id=10, name="viewer")]
    data = Base.dataset(*roles, User.factory(3)).random_seed(1).create()
    assert data["roles"][0].name == "viewer"


def test_dataset_seeded_model_gets_no_default_factory_row(Base, Role):
    roles = [Role(id=20, name="only")]
    data = Base.dataset(*roles).random_seed(1).create()
    assert len(data["roles"]) == 1
    assert data["roles"][0].name == "only"


def test_dataset_seeded_and_factory_rows_merged(Base, Role):
    roles = [Role(id=30, name="seeded")]
    data = Base.dataset(*roles, Role.factory(2)).random_seed(1).create()
    assert len(data["roles"]) == 3
    assert data["roles"][0].name == "seeded"
