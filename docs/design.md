# Design & Architecture

This document is for contributors who want to understand how rowsmyth works internally.

## Why rowsmyth exists

factory-boy is excellent at generating realistic values but requires significant
scaffolding to use with SQLAlchemy for multi-table datasets: separate factory
classes in separate files, explicit session wiring and manual loops to avoid
the SubFactory explosion - where each child creates its own independent parent.

rowsmyth eliminates that scaffolding. Generators live co-located with models,
and FK wiring is handled automatically.

## Module Map

```
src/rowsmyth/
├── _base.py      # TableSpecMixin + declarative_base() - the user entry point
├── _variant.py   # @variant decorator - marks classmethods as named overrides
├── _builder.py   # FactoryBuilder - fluent builder rooted at one model
├── _dataset.py   # Dataset - generates across all registered models in FK order
├── _execute.py   # Execution engine - session, factory creation, FK wiring
└── _spark.py     # SQLAlchemy column type → PySpark StructType conversion
```

## Data Flow: `.factory(n).has(...).create()`

1. `Model.factory(n)` (in `_base.py`) creates `FactoryBuilder(Model, n)`
2. `.has(Child.factory(1, 3))` appends a child builder to `_children`
3. `.create()` calls `execute_builder(builder, overrides, seed)` in `_execute.py`
4. `execute_builder`:
   - Seeds `random` and `Faker` if a seed was set
   - Calls `check_cycles()` - raises `CycleError` on circular `.has()` chains
   - Calls `collect_models()` - walks the full tree including nested `.has()` and FK builders from `generators()`
   - Creates an in-memory SQLite engine + session via `build_session()`
   - Creates a `SQLAlchemyModelFactory` subclass per model via `make_factory()`
   - Calls `_create_instance()` once per root instance

5. `_create_instance()`:
   - Picks a variant via `_pick_variant()` (weighted random draw from `_mix`)
   - Applies extra overrides (variant overrides, then FK overrides, then caller overrides)
   - Resolves default FK builders from `generators()` - creates parent instances if not already overridden
   - Calls the factory to create and persist the instance
   - Recursively creates child instances, wiring `{rel_key: instance}` as an override

## How `__variants__` is collected

`TableSpecMixin.__init_subclass__` fires when each model class is defined.
It scans `vars(cls)` for callables with `_is_variant = True` (set by `@variant`)
and stores them in `cls.__variants__`. Variant lookup at generate time is O(1).

## FK resolution without SubFactory explosion

factory-boy's `SubFactory` creates a brand-new parent for every child, producing
an explosion of unrelated parent rows. rowsmyth avoids this in two ways:

- **`.has()` mode**: the parent instance is passed as an explicit FK override
  to every child created in the loop - all children share the same parent.
- **`Dataset.create()` mode**: a `pool` dict (keyed by model) accumulates
  instances as they're created. When a child model has an FK to a parent model,
  `random.choice(pool[parent])` picks an existing parent rather than creating
  a new one.

## Extension Points

- **Custom generators**: override `generators()` on your model
- **Named variants**: use `@variant` to define override sets, then `.mix(name=proportion)`
- **Explicit FK wiring**: pass `via="rel_name"` to `.has()` when multiple
  relationships exist to the same model
- **Spark schema metadata**: set `info={"description": "..."}` on `mapped_column()`
  to include metadata in the generated `StructField`
