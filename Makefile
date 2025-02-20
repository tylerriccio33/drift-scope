
test: ## Run pytest and calculate cov
	@uv run pytest \
		--cov src \
		--cov-report term-missing \
		--randomly-seed 123

lint: ## Run ruff and mypy
	@uv run ruff check --fix
	@uv run mypy src
	@uv run mypy tests