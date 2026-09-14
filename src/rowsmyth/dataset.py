"""Dataset session, pooling and per-row context."""

from __future__ import annotations

import random as random_module
import warnings
import weakref
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

from faker import Faker

from rowsmyth.errors import (
    DatasetConfigurationError,
    DatasetContextError,
    DuplicatePrimaryKeyError,
    ForeignKeyViolationError,
    InvalidModelDefinitionError,
    RowsmythWarning,
    TableNotFoundError,
    UnknownColumnError,
    ViewCollisionError,
)
from rowsmyth.pool import Pool, PoolIndex, empty_pool_error

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

    from pyspark.sql import DataFrame, SparkSession

    from rowsmyth.errors import IntegrityError
    from rowsmyth.model import Model

INTEGRITY_MODES = ("raise", "warn", "off")

_active: ContextVar[Dataset] = ContextVar("rowsmyth_active_dataset")
_view_owners: dict[str, tuple[type[Model], weakref.ReferenceType[Any]]] = {}


def require_active() -> Dataset:
    """Return the active dataset or raise."""
    try:
        return _active.get()
    except LookupError as exc:
        msg = "rowsmyth factories must be used inside Base.dataset(spark, ...)"
        raise DatasetContextError(msg) from exc


class Dataset:
    """
    Session-scoped dataset state.

    Generated rows are kept in the driver as plain dicts. Spark is touched only
    when a table is read - through :meth:`dataframe`, :meth:`tables`,
    :meth:`write_all` or an explicit :meth:`flush` - so row creation costs no
    round trips regardless of how many rows are created at a time.
    """

    __slots__ = (
        "_dirty",
        "_frames",
        "_pk_index",
        "_pools",
        "_rows",
        "_seq",
        "_token",
        "base",
        "faker",
        "integrity",
        "random",
        "registry",
        "seed",
        "spark",
        "view_prefix",
        "views",
    )

    def __init__(
        self,
        spark: SparkSession,
        faker: Faker,
        rng: random_module.Random,
        seed: int | None,
        base: type[Model],
        integrity: str = "raise",
        *,
        views: bool = True,
        view_prefix: str | None = None,
    ) -> None:
        if integrity not in INTEGRITY_MODES:
            msg = f"integrity must be one of {list(INTEGRITY_MODES)}, got {integrity!r}"
            raise DatasetConfigurationError(msg)
        self.spark = spark
        self.faker = faker
        self.random = rng
        self.seed = seed
        self.base = base
        self.integrity = integrity
        self.views = views
        self.view_prefix = view_prefix
        self.registry = base.registry
        self._seq: dict[str, int] = {}
        self._rows: dict[str, list[dict[str, Any]]] = {}
        self._frames: dict[str, DataFrame] = {}
        self._dirty: set[str] = set()
        self._pk_index: dict[str, set[tuple[Any, ...]]] = {}
        self._pools: dict[tuple[str, str], PoolIndex] = {}
        self._token: Any = None

    def __enter__(self) -> Dataset:
        self._token = _active.set(self)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            if exc_type is None:
                self.check_integrity()
        finally:
            if self._token is not None:
                _active.reset(self._token)
                self._token = None

    def next_seq(self, name: str) -> int:
        """Return the next monotonic counter for ``name``."""
        self._seq[name] = self._seq.get(name, 0) + 1
        return self._seq[name]

    def pool(self, view: str, col: str) -> Pool:
        """
        Distinct values from a dataset table or an existing Spark temp view.

        A name declared in this dataset's registry resolves from rows already
        generated in the driver. Any other name is read once from Spark and
        cached for the rest of the session.
        """
        return Pool(self, view, col)

    def table_names(self) -> list[str]:
        """Names of every table created in this dataset, in creation order."""
        return list(self._rows)

    def rows(self, name: str) -> list[dict[str, Any]]:
        """Rows created for ``name`` as plain dicts, without touching Spark."""
        return [dict(row) for row in self._require_table(name)]

    def count(self, name: str) -> int:
        """Number of rows created for ``name``, without touching Spark."""
        return len(self._require_table(name))

    def dataframe(self, name: str) -> DataFrame:
        """Return a DataFrame for a table created in this dataset session."""
        self._require_table(name)
        self._materialise(name)
        return self._frames[name]

    def tables(self) -> dict[str, DataFrame]:
        """Every table created in this dataset, keyed by table name."""
        return {name: self.dataframe(name) for name in self._rows}

    def view_name(self, name: str) -> str:
        """Temp view name used for ``name`` in this dataset."""
        if self.view_prefix:
            return f"{self.view_prefix}{name}"
        return name

    def flush(self, name: str | None = None) -> None:
        """
        Materialise pending rows into DataFrames and temp views.

        Call this before reading generated tables through Spark directly, for
        example ``spark.table(...)`` or ``spark.sql(...)``.
        """
        if name is not None:
            self._require_table(name)
            self._materialise(name)
            return
        for target in list(self._dirty):
            self._materialise(target)

    def write_all(
        self,
        *,
        mode: str = "overwrite",
        format: str | None = None,
        options: dict[str, str] | None = None,
        name: Callable[[type[Model]], str] | None = None,
        path: Callable[[type[Model]], str] | None = None,
    ) -> dict[str, str]:
        """
        Write every table created in this dataset.

        By default each table is saved with ``saveAsTable(Model.fqn())``. Pass
        ``name`` to derive a different table name per model, or ``path`` to
        ``save()`` to a location such as a Unity Catalog volume. Returns the
        destination used for each table.
        """
        destinations: dict[str, str] = {}
        for table_name, frame in self.tables().items():
            model = self.registry[table_name]
            writer = frame.write.mode(mode)
            if format is not None:
                writer = writer.format(format)
            if options:
                writer = writer.options(**options)
            if path is not None:
                destination = path(model)
                writer.save(destination)
            else:
                destination = model.fqn() if name is None else name(model)
                writer.saveAsTable(destination)
            destinations[table_name] = destination
        return destinations

    def check_integrity(self) -> None:
        """
        Validate declared foreign keys against rows created in this dataset.

        Runs automatically when the dataset block exits cleanly. Call it
        earlier to fail closer to the fixture code that created the rows.
        """
        if self.integrity == "off":
            return
        indexes: dict[tuple[str, str], set[Any]] = {}
        for name, rows in self._rows.items():
            table = self.registry[name]
            for column, target in table.__foreign_keys__.items():
                self._check_foreign_key(name, rows, column, target, indexes)

    def _check_foreign_key(
        self,
        name: str,
        rows: list[dict[str, Any]],
        column: str,
        target: str,
        indexes: dict[tuple[str, str], set[Any]],
    ) -> None:
        parent_table, parent_column = target.split(".")
        key = (parent_table, parent_column)
        if key not in indexes:
            indexes[key] = self._column_index(name, column, parent_table, parent_column)
        values = {row[column] for row in rows if row.get(column) is not None}
        missing = sorted(values - indexes[key], key=repr)
        if not missing:
            return
        msg = (
            f"{name}.{column} -> {target}: {len(missing)} value(s) have no "
            f"matching parent row: {missing[:5]}"
        )
        self._fail_integrity(ForeignKeyViolationError, msg)

    def _column_index(
        self,
        name: str,
        column: str,
        parent_table: str,
        parent_column: str,
    ) -> set[Any]:
        parent = self.registry.get(parent_table)
        if parent is None:
            msg = (
                f"{name}.{column}: foreign key target table {parent_table!r} is "
                "not registered on this declarative base"
            )
            raise InvalidModelDefinitionError(msg)
        if parent_column not in parent._field_names():
            msg = (
                f"{name}.{column}: foreign key target column {parent_column!r} is "
                f"not in {parent_table}.__definition__"
            )
            raise InvalidModelDefinitionError(msg)
        return {
            row[parent_column]
            for row in self._rows.get(parent_table, [])
            if row.get(parent_column) is not None
        }

    def _fail_integrity(self, error: type[IntegrityError], msg: str) -> None:
        if self.integrity == "raise":
            raise error(msg)
        warnings.warn(msg, RowsmythWarning, stacklevel=4)

    def _require_table(self, name: str) -> list[dict[str, Any]]:
        """Return the rows for ``name`` or raise if it was never created."""
        try:
            return self._rows[name]
        except KeyError as exc:
            msg = f"{name!r} has not been created in this dataset"
            raise TableNotFoundError(msg) from exc

    def _commit(self, rows_by_table: dict[str, list[dict[str, Any]]]) -> None:
        """Append generated rows to the dataset. Never touches Spark."""
        for name, rows in rows_by_table.items():
            self._check_primary_keys(self.registry[name], rows)
            self._rows.setdefault(name, []).extend(rows)
            if rows or name not in self._frames:
                self._dirty.add(name)

    def _check_primary_keys(
        self,
        table: type[Model],
        rows: list[dict[str, Any]],
    ) -> None:
        if self.integrity == "off":
            return
        primary_key = table.__primary_key__
        index = self._pk_index.setdefault(table.__table_name__, set())
        added: set[tuple[Any, ...]] = set()
        for attrs in rows:
            key = tuple(attrs.get(column) for column in primary_key)
            if key not in index and key not in added:
                added.add(key)
                continue
            columns = dict(zip(primary_key, key, strict=True))
            msg = f"{table.__table_name__}: duplicate primary key {columns}"
            self._fail_integrity(DuplicatePrimaryKeyError, msg)
        index |= added

    def _materialise(self, name: str) -> None:
        if name not in self._dirty:
            return
        table = self.registry[name]
        frame = self.spark.createDataFrame(self._rows[name], table.__definition__)
        if self.views:
            view = self.view_name(name)
            self._claim_view(view, table)
            frame.createOrReplaceTempView(view)
        self._frames[name] = frame
        self._dirty.discard(name)

    def _claim_view(self, view: str, table: type[Model]) -> None:
        owner = _view_owners.get(view)
        if owner is not None and owner[1]() is self.spark and owner[0] is not self.base:
            msg = (
                f"{table.__table_name__}: temp view {view!r} is already registered "
                f"by declarative base {owner[0].__name__!r} in this session; pass "
                "view_prefix= to Base.dataset() to namespace one of them"
            )
            raise ViewCollisionError(msg)
        _view_owners[view] = (self.base, weakref.ref(self.spark))

    def _pool_values(self, view: str, column: str) -> list[Any]:
        index = self._pools.get((view, column))
        if index is None:
            index = PoolIndex(external=view not in self.registry)
            self._pools[view, column] = index
        if index.external:
            self._load_external_pool(index, view, column)
        else:
            self._load_table_pool(index, view, column)
        if not index.values:
            raise empty_pool_error(view, column, external=index.external)
        return index.values

    def _load_table_pool(self, index: PoolIndex, view: str, column: str) -> None:
        if column not in self.registry[view]._field_names():
            msg = f"pool({view!r}, {column!r}): {column!r} is not a column of {view}"
            raise UnknownColumnError(msg)
        rows = self._rows.get(view, [])
        index.extend(row.get(column) for row in rows[index.cursor :])
        index.cursor = len(rows)

    def _load_external_pool(self, index: PoolIndex, view: str, column: str) -> None:
        if index.loaded:
            return
        from pyspark.sql import functions as F

        self.flush()
        rows = (
            self.spark
            .table(view)
            .select(column)
            .where(F.col(column).isNotNull())
            .distinct()
            .orderBy(column)
            .collect()
        )
        index.extend(row[0] for row in rows)
        index.loaded = True


class RowCtx:
    """Per-row context passed to :meth:`Model.generator` and variants."""

    __slots__ = ("_acc", "_dataset", "_parents", "_table", "index", "row")

    def __init__(
        self,
        dataset: Dataset,
        table: type[Model],
        index: int,
        parents: dict[str, Model],
        acc: dict[str, list[dict[str, Any]]],
    ) -> None:
        self._dataset = dataset
        self._table = table
        self.index = index
        self.row: dict[str, Any] = {}
        self._parents = parents
        self._acc = acc

    @property
    def faker(self) -> Faker:
        return self._dataset.faker

    @property
    def random(self) -> random_module.Random:
        return self._dataset.random

    @property
    def seed(self) -> int | None:
        return self._dataset.seed

    @property
    def spark(self) -> SparkSession:
        return self._dataset.spark

    @property
    def dataset(self) -> Dataset:
        return self._dataset

    def sequence(self, name: str | None = None) -> int:
        """Monotonic counter; defaults to the current table name."""
        return self._dataset.next_seq(name or self._table.__table_name__)

    def pool(self, view: str, col: str) -> Pool:
        """Distinct values from a dataset table or an existing temp view."""
        return self._dataset.pool(view, col)

    def parent(self, table: type[Model], role: str | None = None) -> Model:
        """Resolve a parent row (injected or created once per slot)."""
        from rowsmyth.model import validate_dataset_base
        from rowsmyth.resolution import new_parent

        validate_dataset_base(table, self._dataset)
        slot = role or table.__table_name__
        if slot not in self._parents:
            self._parents[slot] = new_parent(table.factory(), self)
        return self._parents[slot]


def dataset(
    spark: SparkSession,
    base: type[Model],
    seed: int | None = None,
    integrity: str = "raise",
    *,
    views: bool = True,
    view_prefix: str | None = None,
) -> Dataset:
    """Create a dataset context manager for a declarative base."""
    faker = Faker()
    if seed is not None:
        faker.seed_instance(seed)
    rng = random_module.Random(seed)
    return Dataset(
        spark,
        faker,
        rng,
        seed,
        base,
        integrity,
        views=views,
        view_prefix=view_prefix,
    )
