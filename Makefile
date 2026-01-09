.PHONY: help install format lint lint-fix typecheck test check clean pre-commit

# Default target
help:
	@echo "Available targets:"
	@echo "  install     - Install dependencies with uv"
	@echo "  format      - Format code with ruff"
	@echo "  lint        - Check code with ruff (no fixes)"
	@echo "  lint-fix    - Check and fix code with ruff"
	@echo "  typecheck   - Run mypy type checking"
	@echo "  test        - Run pytest"
	@echo "  check       - Run all checks (lint, typecheck, test)"
	@echo "  pre-commit  - Run pre-commit hooks on all files"
	@echo "  clean       - Remove cache directories"

# Install dependencies
install:
	uv sync

# Format code
format:
	uv run ruff format .

# Lint without fixing
lint:
	uv run ruff check .

# Lint with auto-fix
lint-fix:
	uv run ruff check --fix .

# Type checking
typecheck:
	uv run mypy packages/

# Run tests
test:
	uv run pytest

# Run all checks
check: lint typecheck test

# Run pre-commit on all files
pre-commit:
	uv run pre-commit run --all-files

# Clean cache directories
clean:
	rm -rf .mypy_cache .pytest_cache .ruff_cache
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true