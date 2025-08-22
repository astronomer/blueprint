# Claude Code Instructions for Airflow Blueprint

This is an Airflow Blueprint tool for creating reusable DAG templates with validated configurations.

## Project Overview
- Python package for generating Airflow DAG templates
- CLI tool accessible via `blueprint` command
- Supports Python 3.8+ and Apache Airflow 2.5.0+
- Uses Pydantic for configuration validation
- Templates stored in `examples/.astro/templates/`

## Development Setup
- **Package Manager**: Use `uv` for all Python operations (NOT pip, poetry, or conda)
- **Python Version**: Development uses Python 3.12, but maintain compatibility with 3.8+
- **Dependencies**: Install with `uv sync --all-extras --dev`

## Code Quality Commands
- **Linting**: `uv run ruff check blueprint/ tests/`
- **Formatting**: `uv run ruff format blueprint/ tests/`
- **Type Checking**: `uv run ty check blueprint/`
- **Testing**: `uv run pytest tests/ -v`
- **Pre-commit**: `uv run pre-commit run --all-files`

## Blueprint CLI Commands
- **List templates**: `uv run blueprint list` or `cd examples && uv run blueprint list`
- **Initialize**: `uv run blueprint init`
- **Create from template**: `uv run blueprint new <template_name> <dag_id>`
- **Describe template**: `uv run blueprint describe <template_name>`
- **Lint DAG**: `uv run blueprint lint <dag_file>`
- **With custom template path**: `BLUEPRINT_TEMPLATE_PATH=examples/.astro/templates uv run blueprint list`

## Testing
- Run all tests: `uv run pytest tests/`
- Run specific test: `uv run pytest tests/test_<module>.py`
- Run with coverage: `uv run pytest --cov=blueprint tests/`
- Integration test in examples: `cd examples && uv run blueprint list`

## Code Style Guidelines
- Follow Ruff configuration in `pyproject.toml`
- Line length: 100 characters
- Use Google docstring convention
- Imports sorted with isort (via Ruff)
- Type hints required for all public functions
- No comments unless explicitly requested

## Project Structure
- `blueprint/`: Main package code
  - `cli.py`: CLI implementation using Click
  - `models.py`: Pydantic models for blueprint configs
  - `loader.py`: Template loading logic
  - `validator.py`: DAG validation logic
  - `dag_loader.py`: Dynamic DAG loading for Airflow
- `tests/`: Test files
- `examples/`: Example blueprints and templates
  - `.astro/templates/`: Blueprint template definitions
- `.github/workflows/`: CI/CD pipelines

## Git Workflow
- Main branch: `main`
- Run tests before committing
- Ensure all linting and type checks pass
- Use conventional commit messages

## Building & Publishing
- Build package: `uv build`
- Package published to PyPI as `airflow-blueprint`
- Version managed in `blueprint/__init__.py`

## Important Notes
- Always verify template paths when working with blueprint commands
- The examples directory contains working templates for testing
- Maintain backward compatibility with Airflow 2.5.0+
- Use pathlib for file operations (enforced by Ruff)
