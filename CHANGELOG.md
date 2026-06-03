# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- commitizen will append entries here on `cz bump` -->

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
