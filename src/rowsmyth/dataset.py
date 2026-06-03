"""Dataset context and per-row context."""

from __future__ import annotations

import random as random_module
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

from faker import Faker

from rowsmyth._constants import INTERNAL_PREFIX
from rowsmyth.errors import (
    DataframeNotFoundError,
    DatasetContextError,
    EmptyPoolError,
    UnresolvedPoolError,
)
from rowsmyth.pool import Pool, PoolChoice

if TYPE_CHECKING:
    from types import TracebackType

    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

    from rowsmyth.model import Model

_active: ContextVar[Dataset] = ContextVar("rowsmyth_active_dataset")


def require_active() -> Dataset:
    """Return the active dataset or raise."""
    try:
        return _active.get()
    except LookupError as exc:
        msg = (
            "rowsmyth factories must be used inside dataset context via "
            "Base.dataset(spark, ...)"
        )
        raise DatasetContextError(msg) from exc


class Dataset:
    """Session-scoped dataset state."""

    __slots__ = (
        "_rows",
        "_seq",
        "_token",
        "_validated",
        "base",
        "dataframes",
        "faker",
        "random",
        "registry",
        "seed",
        "spark",
    )

    def __init__(
        self,
        spark: SparkSession,
        faker: Faker,
        rng: random_module.Random,
        seed: int | None,
        base: type[Model],
    ) -> None:
        self.spark = spark
        self.faker = faker
        self.random = rng
        self.seed = seed
        self.base = base
        self.registry = base.registry
        self._seq: dict[str, int] = {}
        self._rows: dict[str, list[dict[str, Any]]] = {}
        self._validated: set[str] = set()
        self._token: Any = None
        self.dataframes: dict[str, DataFrame] = {}

    def __enter__(self) -> Dataset:
        self._token = _active.set(self)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._token is not None:
            _active.reset(self._token)
            self._token = None

    def next_seq(self, name: str) -> int:
        """Return the next monotonic counter for ``name``."""
        self._seq[name] = self._seq.get(name, 0) + 1
        return self._seq[name]

    def pool(self, view: str, col: str) -> Pool:
        """Distinct values from a temp view column."""
        return Pool(self.spark, view, col, self.random)

    def dataframe(self, name: str) -> DataFrame:
        """Return a DataFrame created in this dataset session."""
        try:
            return self.dataframes[name]
        except KeyError as exc:
            msg = f"{name!r} has not been created in this dataset"
            raise DataframeNotFoundError(msg) from exc

    def _commit(self, rows_by_table: dict[str, list[dict[str, Any]]]) -> None:
        """Append rows, refresh DataFrames and register temp views."""
        for name, rows in rows_by_table.items():
            self._rows.setdefault(name, []).extend(rows)
            table = self.registry[name]
            public_df = self._dataframe_with_resolved_pools(name, table)
            public_df.createOrReplaceTempView(name)
            self.dataframes[name] = public_df

    def _dataframe_with_resolved_pools(
        self,
        name: str,
        table: type[Model],
    ) -> DataFrame:
        """Build a Spark DataFrame and resolve deferred pool choices in Spark."""
        public_fields = table.__definition__.fields
        if not any(
            isinstance(attrs.get(field.name), PoolChoice)
            for attrs in self._rows[name]
            for field in public_fields
        ):
            return self.spark.createDataFrame(self._rows[name], table.__definition__)

        internal_rows: list[dict[str, Any]] = []
        pool_columns: set[str] = set()
        row_id_col = f"{INTERNAL_PREFIX}row_id"

        for row_id, attrs in enumerate(self._rows[name]):
            internal_row = dict(attrs)
            internal_row[row_id_col] = row_id
            for field in public_fields:
                value = internal_row.get(field.name)
                if isinstance(value, PoolChoice):
                    pool_columns.add(field.name)
                    internal_row[f"{INTERNAL_PREFIX}{field.name}_view"] = value.view
                    internal_row[f"{INTERNAL_PREFIX}{field.name}_column"] = value.column
                    internal_row[f"{INTERNAL_PREFIX}{field.name}_seed"] = value.seed
                    internal_row[field.name] = None
            internal_rows.append(internal_row)

        schema = _internal_schema(table.__definition__, pool_columns)
        df = self.spark.createDataFrame(internal_rows, schema)
        for column in pool_columns:
            df = self._resolve_pool_column(df, column)

        pool_view_cols = {
            column: f"{INTERNAL_PREFIX}{column}_view" for column in pool_columns
        }
        resolved_rows = df.select(
            row_id_col,
            *[field.name for field in public_fields],
            *pool_view_cols.values(),
        ).collect()
        for row in resolved_rows:
            attrs = self._rows[name][row[row_id_col]]
            for field in public_fields:
                if (
                    field.name in pool_view_cols
                    and row[pool_view_cols[field.name]] is not None
                    and row[field.name] is None
                ):
                    msg = f"pool choice for {name}.{field.name} could not be resolved"
                    raise UnresolvedPoolError(msg)
                attrs[field.name] = row[field.name]
        return df.select(*[field.name for field in public_fields])

    def _resolve_pool_column(self, df: DataFrame, target_col: str) -> DataFrame:
        """Resolve one deferred pool target column against Spark temp views."""
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        view_col = f"{INTERNAL_PREFIX}{target_col}_view"
        source_col_col = f"{INTERNAL_PREFIX}{target_col}_column"
        seed_col = f"{INTERNAL_PREFIX}{target_col}_seed"
        resolved_col = f"{INTERNAL_PREFIX}{target_col}_resolved"
        groups = [
            (row[view_col], row[source_col_col])
            for row in df
            .select(view_col, source_col_col)
            .where(F.col(view_col).isNotNull())
            .distinct()
            .collect()
        ]
        for view, source_col in groups:
            source = (
                self.spark
                .table(view)
                .select(F.col(source_col).alias(resolved_col))
                .distinct()
            )
            value_count = source.count()
            if value_count == 0:
                msg = f"pool({view!r}, {source_col!r}): no values in temp view"
                raise EmptyPoolError(msg)
            ranked = source.withColumn(
                f"{resolved_col}_rank",
                F.row_number().over(Window.orderBy(F.col(resolved_col))),
            ).withColumn(f"{resolved_col}_count", F.lit(value_count))
            df = (
                df
                .join(
                    ranked,
                    (
                        (F.col(view_col) == F.lit(view))
                        & (F.col(source_col_col) == F.lit(source_col))
                        & (
                            F.pmod(F.col(seed_col), F.col(f"{resolved_col}_count"))
                            + F.lit(1)
                            == F.col(f"{resolved_col}_rank")
                        )
                    ),
                    "left",
                )
                .withColumn(
                    target_col, F.coalesce(F.col(resolved_col), F.col(target_col))
                )
                .drop(resolved_col, f"{resolved_col}_rank", f"{resolved_col}_count")
            )
        return df


class RowCtx:
    """Per-row context passed to :meth:`Model.generator` and variants."""

    __slots__ = ("_acc", "_gen", "_parents", "_table", "index", "row")

    def __init__(
        self,
        gen: Dataset,
        table: type[Model],
        index: int,
        parents: dict[str, Model],
        acc: dict[str, list[dict[str, Any]]],
    ) -> None:
        self._gen = gen
        self._table = table
        self.index = index
        self.row: dict[str, Any] = {}
        self._parents = parents
        self._acc = acc

    @property
    def faker(self) -> Faker:
        return self._gen.faker

    @property
    def random(self) -> random_module.Random:
        return self._gen.random

    @property
    def seed(self) -> int | None:
        return self._gen.seed

    @property
    def spark(self) -> SparkSession:
        return self._gen.spark

    def sequence(self, name: str | None = None) -> int:
        """Monotonic counter; defaults to the current table name."""
        return self._gen.next_seq(name or self._table.__table_name__)

    def pool(self, view: str, col: str) -> Pool:
        """Distinct values from an existing temp view."""
        return self._gen.pool(view, col)

    def parent(self, table: type[Model], role: str | None = None) -> Model:
        """Resolve a parent row (injected or created once per slot)."""
        from rowsmyth.model import validate_dataset_base
        from rowsmyth.resolution import new_parent

        validate_dataset_base(table, self._gen)
        slot = role or table.__table_name__
        if slot not in self._parents:
            self._parents[slot] = new_parent(table.factory(), self)
        return self._parents[slot]


def dataset(
    spark: SparkSession,
    base: type[Model],
    seed: int | None = None,
) -> Dataset:
    """Create a dataset context manager for a declarative base."""
    faker = Faker()
    if seed is not None:
        faker.seed_instance(seed)
    rng = random_module.Random(seed)
    return Dataset(spark, faker, rng, seed, base)


def _internal_schema(definition: StructType, pool_columns: set[str]) -> StructType:
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    fields: list[StructField] = [
        StructField(field.name, field.dataType, True, field.metadata)
        for field in definition.fields
    ]
    fields.append(StructField(f"{INTERNAL_PREFIX}row_id", LongType(), False))
    for column in sorted(pool_columns):
        fields.extend([
            StructField(f"{INTERNAL_PREFIX}{column}_view", StringType(), True),
            StructField(f"{INTERNAL_PREFIX}{column}_column", StringType(), True),
            StructField(f"{INTERNAL_PREFIX}{column}_seed", LongType(), True),
        ])
    return StructType(fields)
