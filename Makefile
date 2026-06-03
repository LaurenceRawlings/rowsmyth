.PHONY: install lint format typecheck security test pre-commit \
        docs docs-serve

install:
	uv sync --all-groups
	uv run pre-commit install --hook-type pre-commit --hook-type commit-msg

lint:
	uv run ruff check --fix .

format:
	uv run ruff format .

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
