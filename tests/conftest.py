from typing import Optional

import factory
import factory.fuzzy
import pytest
from sqlalchemy import CheckConstraint, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from rowsmyth import declarative_base, variant

# --- Minimal schema: no generators, no variants, no FKs ---
_MinimalBase = declarative_base()


class _Minimal(_MinimalBase):
    __tablename__ = "things"
    id: Mapped[int] = mapped_column(primary_key=True)


# --- Full schema ---
# Classes are named User/Order/OrderItem (not prefixed) so that SQLAlchemy
# error messages and __repr__ output use the clean names. Aliases are saved
# before the fixture definitions below shadow the module-level names.
FullBase = declarative_base()


class Role(FullBase):
    __tablename__ = "roles"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String)

    @classmethod
    def generators(cls):
        return {cls.name: factory.Faker("word")}


class User(FullBase):
    __tablename__ = "users"
    __table_args__ = (
        CheckConstraint("tier IN ('standard', 'premium')", name="ck_users_tier"),
        {"comment": "Application users", "info": {"domain": "auth"}},
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String, comment="Full name", info={"pii": True})
    tier: Mapped[str] = mapped_column(String)
    role_id: Mapped[int | None] = mapped_column(ForeignKey("roles.id"), nullable=True)
    role: Mapped[Optional["Role"]] = relationship()
    orders: Mapped[list["Order"]] = relationship(back_populates="user")

    @classmethod
    def generators(cls):
        return {
            cls.name: factory.Faker("name"),
            cls.tier: factory.fuzzy.FuzzyChoice(["standard", "premium"]),
        }

    @variant
    def admin(cls):
        return {cls.name: "admin", cls.tier: "premium"}


class Order(FullBase):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(primary_key=True)
    total: Mapped[float]
    user_id: Mapped[int] = mapped_column(ForeignKey(User.id))
    user: Mapped["User"] = relationship(back_populates="orders")
    items: Mapped[list["OrderItem"]] = relationship(back_populates="order")

    @classmethod
    def generators(cls):
        return {
            cls.user: _User.factory(),
            cls.total: factory.Faker("pyfloat", positive=True, max_value=500),
        }


class OrderItem(FullBase):
    __tablename__ = "order_items"

    id: Mapped[int] = mapped_column(primary_key=True)
    sku: Mapped[str] = mapped_column(String)
    order_id: Mapped[int] = mapped_column(ForeignKey(Order.id))
    order: Mapped["Order"] = relationship(back_populates="items")

    @classmethod
    def generators(cls):
        return {cls.sku: factory.Faker("ean13")}


# Save references before fixture functions shadow the module-level names.
_FullBase = FullBase
_Role, _User, _Order, _OrderItem = Role, User, Order, OrderItem


@pytest.fixture(scope="session")
def MinimalBase():
    return _MinimalBase


@pytest.fixture(scope="session")
def Minimal():
    return _Minimal


@pytest.fixture(scope="session")
def Base():
    return _FullBase


@pytest.fixture(scope="session")
def User():
    return _User


@pytest.fixture(scope="session")
def Order():
    return _Order


@pytest.fixture(scope="session")
def OrderItem():
    return _OrderItem


@pytest.fixture(scope="session")
def Role():
    return _Role
