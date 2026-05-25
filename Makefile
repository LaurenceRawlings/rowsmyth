.PHONY: install lint format format-docs typecheck security test pre-commit \
        docs docs-serve

MD_FILES = README.md CONTRIBUTING.md SECURITY.md CHANGELOG.md docs/

install:
	uv sync --all-groups
	uv run pre-commit install --hook-type pre-commit --hook-type commit-msg

lint:
	uv run ruff check --fix .

format: format-docs
	uv run ruff format .

format-docs:
	uv run ruff format $(MD_FILES)

typecheck:
	uv run ty check src/

security:
	uv run bandit -r src/ -c pyproject.toml

test:
	uv run pytest

pre-commit:
	uv run pre-commit run --all-files

docs:
	uv run portray as_html

docs-serve:
	uv run portray in_browser
