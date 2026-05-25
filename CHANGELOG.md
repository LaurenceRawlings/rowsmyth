# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- commitizen will append entries here on `cz bump` -->

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
