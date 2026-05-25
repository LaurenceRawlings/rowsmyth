import pytest
from sqlalchemy import MetaData, String
from sqlalchemy.orm import Mapped, mapped_column, registry

from rowsmyth._base import declarative_base
from rowsmyth._variant import variant


def test_generators_default_empty():
    Base = declarative_base()

    class Thing(Base):
        __tablename__ = "things"
        id: Mapped[int] = mapped_column(primary_key=True)

    assert Thing.generators() == {}


def test_variants_collected_on_subclass():
    Base = declarative_base()

    class Thing(Base):
        __tablename__ = "things2"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String)

        @variant
        def special(cls):
            return {cls.name: "special"}

    assert "special" in Thing.__variants__
    assert Thing.__variants__["special"](Thing) == {Thing.name: "special"}


def test_variants_not_shared_between_classes():
    Base = declarative_base()

    class A(Base):
        __tablename__ = "things_a"
        id: Mapped[int] = mapped_column(primary_key=True)

        @variant
        def foo(cls):
            return {}

    class B(Base):
        __tablename__ = "things_b"
        id: Mapped[int] = mapped_column(primary_key=True)

    assert "foo" in A.__variants__
    assert "foo" not in B.__variants__


def test_factory_classmethod_returns_builder():
    from rowsmyth._builder import FactoryBuilder

    Base = declarative_base()

    class Thing(Base):
        __tablename__ = "things3"
        id: Mapped[int] = mapped_column(primary_key=True)

    builder = Thing.factory(10)
    assert isinstance(builder, FactoryBuilder)
    assert builder.model is Thing
    assert builder.min_count == 10
    assert builder.max_count is None


def test_factory_classmethod_with_range():
    Base = declarative_base()

    class Thing(Base):
        __tablename__ = "things4"
        id: Mapped[int] = mapped_column(primary_key=True)

    builder = Thing.factory(5, 15)
    assert builder.min_count == 5
    assert builder.max_count == 15


def test_comment_returns_table_comment(User):
    assert User.__comment__ == "Application users"


def test_table_info_returns_table_info_dict(User):
    assert User.__table_info__ == {"domain": "auth"}


def test_column_info_keyed_by_attribute_name(User):
    info = User.__column_info__
    assert info["name"] == {"pii": True}


def test_expectations_returns_check_constraint_expressions(User):
    exps = User.__expectations__
    assert "ck_users_tier" in exps
    assert "standard" in exps["ck_users_tier"]


def test_repr_format(User):
    users = User.factory(1).random_seed(1).create()
    r = repr(users[0])
    assert r.startswith("User(")
    assert "id=" in r
    assert "name=" in r
    assert "tier=" in r


def test_dataset_rejects_non_builder(Base):
    with pytest.raises(TypeError, match="Expected a FactoryBuilder"):
        Base.dataset("not-a-builder")


def test_declarative_base_accepts_custom_metadata():
    meta = MetaData()
    Base = declarative_base(metadata=meta)
    assert Base.metadata is meta


def test_declarative_base_accepts_type_annotation_map():
    Base = declarative_base(type_annotation_map={str: String(50)})

    class Thing(Base):
        __tablename__ = "tam_things"
        id: Mapped[int] = mapped_column(primary_key=True)

    assert issubclass(Base, Base)


def test_declarative_base_accepts_registry():
    reg = registry()
    Base = declarative_base(registry=reg)
    assert Base.registry is reg


def test_dataset_rejects_builder_for_foreign_model(Base):
    OtherBase = declarative_base()

    class Foreign(OtherBase):
        __tablename__ = "foreign_things"
        id: Mapped[int] = mapped_column(primary_key=True)

    with pytest.raises(ValueError, match="is not registered to this Base"):
        Base.dataset(Foreign.factory(1))
