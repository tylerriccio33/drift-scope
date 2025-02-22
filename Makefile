
test: ## Run pytest and calculate cov
	@uv run pytest \
		--cov src \
		--cov-report term-missing \
		--randomly-seed 123

lint: ## Run ruff and mypy
	@uv run ruff check --fix
	@uv run mypy src
	@uv run mypy tests

.PHONY: clean
clean:
	rm -rf .mypy_cache
	rm -rf .pytest_cache
	rm -rf .venv
	rm -rf tests/__pycache__
	rm -rf src/ddirft/__pycache
	rm -rf .ruff_cache