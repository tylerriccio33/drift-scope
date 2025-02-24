
.PHONY: test
test: ## Run pytest and calculate cov
	@uv run pytest \
		--cov src \
		--cov-report term-missing \
		--randomly-seed 123

.PHONY: pre-commit
pre-commit: ## Run pre-commit hooks
	@uv run pre-commit

.PHONY: lint
lint: ## Run ruff and mypy
	@uv run ruff format
	@uv run ruff check --fix
	@uv run mypy src
	@uv run mypy tests

.PHONY: typer
typer: ## Run right typer
	@uv run righttyper \
		--overwrite \
		--srcdir src/drift_scope \
		--ignore-annotations \
		--no-output-files \
		--verbose \
		-m pytest

.PHONY: deps
deps: ## Check dependancies for unused
	@uv run deptry .

.PHONY: clean
clean:
	rm -rf .mypy_cache
	rm -rf .pytest_cache
	rm -rf .venv
	rm -rf tests/__pycache__
	rm -rf src/drift_scope/__pycache
	rm -rf .ruff_cache
