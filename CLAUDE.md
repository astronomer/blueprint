# Claude Code Instructions for Airflow Blueprint

Reusable task group templates composed into Airflow DAGs via YAML.

## Project Overview
- Python package for defining reusable Airflow task group templates (Blueprints)
- Blueprints render into tasks or TaskGroups; DAGs are composed from YAML
- CLI tool accessible via `blueprint` command
- Supports Python 3.10+ and Apache Airflow 2.5.0+
- Uses Pydantic for configuration validation
- Template versioning via separate classes with V{N} suffix

## Development Setup
- **Package Manager**: Use `uv` for all Python operations (NOT pip, poetry, or conda)
- **Python Version**: Development uses Python 3.12, but maintain compatibility with 3.10+
- **Dependencies**: Install with `uv sync --all-extras --dev`

## Code Quality Commands
- **Linting**: `uv run ruff check blueprint/ tests/`
- **Formatting**: `uv run ruff format blueprint/ tests/`
- **Type Checking**: `uv run ty check blueprint/`
- **Testing**: `uv run pytest tests/ -v`
- **Pre-commit**: `uv run pre-commit run --all-files`

## Blueprint CLI Commands
- **List blueprints**: `uv run blueprint list --template-dir dags/`
- **Describe a blueprint**: `uv run blueprint describe extract`
- **Describe specific version**: `uv run blueprint describe extract -v 1`
- **Validate DAG YAML**: `uv run blueprint lint path/to/dag.dag.yaml`
- **Generate JSON schema**: `uv run blueprint schema extract`
- **Create DAG interactively**: `uv run blueprint new`

## Testing
- Run all tests: `uv run pytest tests/`
- Run specific test: `uv run pytest tests/test_<module>.py`
- Run with coverage: `uv run pytest --cov=blueprint tests/`

## Code Style Guidelines
- Follow Ruff configuration in `pyproject.toml`
- Line length: 100 characters
- Use Google docstring convention
- Imports sorted with isort (via Ruff)
- Type hints required for all public functions
- No comments unless explicitly requested

## Project Structure
- `blueprint/`: Main package code
  - `core.py`: Blueprint base class (renders TaskOrGroup, has step_id)
  - `builder.py`: DAGConfig, StepConfig, Builder, build_all()
  - `registry.py`: Version-aware blueprint discovery (V{N} class name parsing)
  - `loaders.py`: YAML loading, Jinja2 rendering, blueprint discovery helpers
  - `cli.py`: CLI implementation using Click
  - `models.py`: Pydantic model re-exports
  - `errors.py`: Custom exceptions (CyclicDependencyError, InvalidVersionError, etc.)
  - `utils.py`: Common utilities
- `tests/`: Test files
- `examples/`: Example blueprints and YAML DAG definitions
  - `airflow2/`: Dockerfile and docker-compose for Airflow 2
  - `airflow3/`: Dockerfile and docker-compose for Airflow 3
  - `dags/etl_blueprints.py`: Blueprint class definitions
  - `dags/*.dag.yaml`: DAG definitions composed from blueprints
  - `dags/loader.py`: DAG loader calling build_all()
- `.github/workflows/`: CI/CD pipelines

## Architecture
- **Blueprint** classes define reusable task group templates (render -> TaskOrGroup)
- **DAGs** are defined in YAML as compositions of blueprint steps
- **Builder** resolves blueprints from registry, validates configs, renders tasks, wires dependencies
- **Registry** auto-discovers blueprints and tracks versions (name -> {version: class})
- **Versioning**: Extract (v1), ExtractV2 (v2) -- separate classes, separate configs
- **Step context**: Each task instance gets blueprint_step_config and blueprint_step_code in template_fields

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
- Blueprints render tasks/TaskGroups, not DAGs
- DAGs are always defined via YAML with steps referencing blueprints
- build_all() is the main entry point for DAG loading
- Use pathlib for file operations (enforced by Ruff)
