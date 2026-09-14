# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- commitizen will append entries here on `cz bump` -->

## 2.0.0 (2026-09-14)

See the [v2 upgrade guide](docs/upgrade-v2.md) for migration steps.

### BREAKING CHANGES

- Row creation no longer touches Spark. `Model.create()`, `Model.bulk_create()` and
  `Factory.create()` append rows in the driver; a table is materialised (one
  `createDataFrame`, plus one `createOrReplaceTempView` when `views=True`) on its first
  read via `dataset.dataframe()`, `dataset.tables()`, `dataset.write_all()` or
  `dataset.flush()`. Creating *n* rows one at a time was `O(n^2)` in both Spark round
  trips and row serialisation; it is now one round trip per table regardless
- Call `dataset.flush()` before reading a generated table through `spark.table()` or
  `spark.sql()` inside a dataset block
- `Dataset.dataframes` removed; use `Dataset.tables()`, `Dataset.dataframe(name)` or
  `Dataset.table_names()`
- `DataframeNotFoundError` renamed to `TableNotFoundError`
- Integrity checks are on by default (`integrity="raise"`): duplicate `__primary_key__`
  tuples raise `DuplicatePrimaryKeyError` as rows are created, and declared
  `__foreign_keys__` are validated when the dataset block exits cleanly. Use
  `integrity="warn"` or `integrity="off"` to relax
- Bare callables are no longer invoked as column values; wrap them with `rowsmyth.lazy()`.
  A stray callable now fails its column's type check instead of silently storing the
  result of calling it
- Every generated value is validated against its declared Spark type; mismatches raise
  `ColumnTypeError` naming the table, column and type. String columns require `str`
- `Pool.choice()` returns a concrete value instead of a deferred token. `PoolChoice` and
  `UnresolvedPoolError` are removed. A pool name that matches a table in the declarative
  base resolves from generated rows in Python; any other name is read from Spark once and
  cached. Seeded pool selections differ from 1.x
- `Factory` methods (`count()`, `where()`, `has()`, `variant()`, `from_rows()`) return a
  new factory instead of mutating the receiver, so factories are reusable templates
- `Factory.count(0)` registers an empty DataFrame with the declared schema instead of
  registering nothing
- Column names that collide with the `Model` API (`key`, `pk`, `attrs`, `create`, ...)
  raise `ReservedColumnError` at class definition
- `DatasetContextError` and `DatasetLookupError` now share a `DatasetError` parent

### Feat

- `Model.__foreign_keys__` - declare `{column: "parent_table.parent_column"}` relationships,
  validated per dataset; `Dataset.check_integrity()` runs the checks early
- `Base.dataset(spark, seed=None, integrity="raise", *, views=True, view_prefix=None)` -
  integrity mode, temp view control and temp view namespacing
- `Dataset.rows(name)` and `Dataset.count(name)` - read generated rows in Python, without
  a Spark action
- `Dataset.tables()`, `Dataset.table_names()`, `Dataset.flush(name=None)` and
  `Dataset.view_name(name)`
- `Dataset.write_all(mode=..., format=..., options=..., name=..., path=...)` - write every
  created table with `saveAsTable` or `save`, replacing hand-maintained table lists
- `Model.bulk_create(rows, **cols)` and `Factory.from_rows(rows)` - create precomputed rows
  without running `generator()`
- `rowsmyth.lazy(fn)` / `Lazy` - explicit deferred column values
- `RowCtx.dataset` - the active `Dataset` from inside `generator()`
- `RowsmythWarning`, `DatasetError`, `DatasetConfigurationError`, `TableNotFoundError`,
  `ViewCollisionError`, `ColumnTypeError`, `IntegrityError`, `DuplicatePrimaryKeyError`,
  `ForeignKeyViolationError` and `LazyValueError`
- `ViewCollisionError` guards two declarative bases registering the same temp view name in
  one Spark session

### Perf

- Pool lookups over dataset tables use an incremental distinct-value index instead of
  re-reading and re-resolving every row on each commit
- Model field-name sets are cached on the class at definition time instead of being rebuilt
  per row

## 1.1.0 (2026-06-04)

- PySpark is no longer a core dependency; install `rowsmyth[spark]` or the `spark` uv dependency group when you need rowsmyth to pull in `pyspark` (e.g. local development). On Databricks and other managed Spark, install `rowsmyth` alone and use the cluster PySpark

## 1.0.0 (2026-06-03)

### BREAKING CHANGES

- Complete API overhaul - all `0.x` public symbols removed
- Dropped SQLAlchemy / factory-boy dependency; library now targets PySpark exclusively
- `FactoryBuilder`, `Dataset.create()` and `@variant` decorator (old form) all removed
- `Model.factory(n)` signature changed; see new `Factory.count()` fluent API
- Direct `Model` subclassing replaced by explicit scoped bases from `declarative_base()`
- `generate()` removed in favour of `Base.dataset(spark, seed=None)`
- `context.py` removed; dataset-session internals now live in `dataset.py`
- Domain failures now raise rowsmyth-specific errors rooted at `RowsmythError`;
  catch the named rowsmyth errors instead of builtin `ValueError`, `TypeError`,
  `KeyError` or `RuntimeError`

### Feat

- `declarative_base()` - creates a scoped rowsmyth base with an independent model registry; concrete subclasses declare `__table_name__`, `__definition__` (PySpark `StructType`) and `__primary_key__`; implement `generator(ctx)` to produce one row
- `Base.dataset(spark, seed=None)` context manager - activates a dataset session bound to one declarative base; all factories must run inside this block
- `Dataset` session object - exposes `spark`, `base`, `registry`, `faker`, `random`, `seed`, `dataframes`; provides `next_seq(name)`, `pool(view, col)` and `dataframe(name)`; auto-registers every committed table as a Spark temp view
- `Factory` fluent builder - `Model.factory()` returns a `Factory`; chain `.count(n)`, `.where(**cols)`, `.has(child, via=None)`, `.variant(name)`, then call `.create()` to generate rows and receive root model instances
- `Model.create(**cols)` - create a single row imperatively inside an active dataset, with optional column overrides
- `RowCtx` per-row context passed to `generator()` - exposes `faker`, `random`, `spark`, `index`, `row`; provides `sequence(name)`, `pool(view, col)` and `parent(table, role=None)` for FK resolution
- `WrongDeclarativeBaseError` - raised when a model from one declarative base is created inside a dataset for another base
- Custom error hierarchy - `RowsmythError` roots named domain failures such as `DatasetContextError`, `UnknownColumnError`, `EmptyPoolError`, `CompoundPrimaryKeyError` and `UnknownVariantError`
- `Pool` - wraps a Spark temp view column; `.choice()` returns a deferred `PoolChoice` resolved deterministically in Spark; `.sample(k)` picks distinct values without replacement
- `@variant` decorator - marks a `Model` method as a named partial override; apply with `Factory.variant(name)`
- Unity Catalog support - `__catalog__`, `__schema__`, `__table_tags__`, `__comment__`; `Model.fqn()`, `Model.uc_tag_sql()`, `Model.column_tags()`, `Model.column_comments()`
- `Model.__expectations__` - named `dict[str, str]` of check expressions for data quality frameworks

## 0.1.0 (2026-05-25)

### Feat

- `declarative_base()` with rowsmyth capabilities mixed in; accepts `metadata`, `type_annotation_map` and `registry` arguments matching `DeclarativeBase`
- `generators()` classmethod for co-located factory-boy declarations; keys are column attributes or strings, values are any factory-boy declaration
- `@variant` decorator for named model variants; returned dicts override specific generators when applied via `.mix()`
- `Model.factory(n)` / `Model.factory(min, max)` for hierarchical data generation with exact or random-range counts
- `FactoryBuilder.has(*builders, via=None)` to attach child builders; foreign keys resolved automatically from SQLAlchemy relationship metadata; use `via=` to disambiguate multiple relationships to the same parent
- `FactoryBuilder.mix(**proportions)` to distribute generated instances across named variants using proportions that sum to <= 1.0; the remainder receives no variant
- `FactoryBuilder.where(overrides)` to force fixed column values on every generated instance; takes precedence over variants and generators
- `FactoryBuilder.random_seed(value)` to seed `random` and `Faker` for reproducible output
- `FactoryBuilder.create()` to generate and persist instances to an in-memory SQLite database; returns a list of root model instances
- `Base.dataset(*builders)` for flat multi-table generation; rows are created in FK dependency order with foreign keys sampled randomly from the created pool
- `Dataset.random_seed(value)` to seed `random` and `Faker` for reproducible output
- `Dataset.create()` to generate and persist instances; returns `dict[str, list[Model]]` keyed by `__tablename__`
- `Model.__comment__` classproperty for table-level comment from `__table_args__`
- `Model.__table_info__` classproperty for table-level `info` dict from `__table_args__`
- `Model.__column_info__` classproperty for per-column `info` dicts, keyed by column name
- `Model.__expectations__` classproperty for named `CheckConstraint` expressions keyed by constraint name; maps directly to data quality frameworks
- `Model.__spark_schema__` classproperty to convert the model to a PySpark `StructType` preserving nullability and column metadata; requires `rowsmyth[spark]`
