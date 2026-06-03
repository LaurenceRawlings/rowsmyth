# Contributing to rowsmyth

Thank you for your interest in contributing!

## Prerequisites

- Python 3.12+
- Java 17+ with `java` on your `PATH` (or `JAVA_HOME` set) for PySpark integration tests
- [uv](https://docs.astral.sh/uv/) - package manager
- [pre-commit](https://pre-commit.com/) - git hooks

PySpark 4.x requires a JDK; CI installs Temurin 17 automatically. Set `JAVA_HOME`
or add `java` to your `PATH` before running `make test`.

## Setup

```bash
git clone https://github.com/LaurenceRawlings/rowsmyth.git
cd rowsmyth
make install
```

## Pull requests

PRs are merged via **squash merge** - the PR title becomes the single commit
on `main`. Keep each PR focused on one type of change so `cz bump` can
determine the correct version bump from the commit history.

**PR title format:** `type(optional-scope): short description`

Example: `feat: add ... to ...`

See the PR template for the full type reference and version bump rules.

## Commit convention

This project uses [Conventional Commits](https://www.conventionalcommits.org/).
The `commit-msg` hook enforces this on local commits automatically.

Commits within a branch are squashed on merge, so local commit messages are
flexible - what matters is the **PR title**.

| Type | Version bump | When to use |
|------|-------------|-------------|
| `feat` | minor | New user-visible feature |
| `fix` | patch | Bug fix |
| `feat!` / `fix!` | major | Breaking change (append `!`) |
| `docs` | none | Documentation only |
| `style` | none | Formatting, whitespace - no logic change |
| `refactor` | none | No functional change |
| `test` | none | Tests only |
| `perf` | patch | Performance improvement |
| `build` | none | Build system or dependency changes |
| `ci` | none | CI/CD configuration changes |
| `chore` | none | Maintenance not covered by other types |
| `revert` | patch | Reverts a previous commit |

## Running checks

```bash
make test        # tests only
make lint        # ruff check + format check
make typecheck   # ty strict mode
make security    # bandit scan
make pre-commit  # run all pre-commit hooks on all files
```

## Adding tests

All PRs must maintain **100% test coverage**. If you add or change code, add
tests that cover it. Docstring examples in `src/` must also be valid doctests -
run `pytest --doctest-modules src/rowsmyth/` to verify them separately.

Spark integration tests use [chispa](https://github.com/MrPowers/chispa) for
DataFrame assertions (`assert_df_equality`, `assert_column_equality`, etc.).

## Understanding the codebase

See [docs/design.md](docs/design.md) for the product design and API behaviour.

Source layout:

- `src/rowsmyth/table.py` - declarative `Model` base, `variant` decorator
- `src/rowsmyth/factory.py` - fluent `Factory` and `create()`
- `src/rowsmyth/context.py` - `generate()`, `RowCtx`, `Generation`
- `src/rowsmyth/resolution.py` - FK resolution and validation
- `src/rowsmyth/pool.py` - `Pool` and deferred pool tokens

## Releasing (maintainers only)

```bash
cz bump
git push
git push --tags
```

`cz bump` reads the commit history, determines the next
SemVer, updates `CHANGELOG.md` and creates a bump commit + `vX.Y.Z` tag.
The CD workflow on GitHub Actions handles the rest.
