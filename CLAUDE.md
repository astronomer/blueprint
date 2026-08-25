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
- **Validate one profile**: `uv run blueprint lint --profile prod` (no flag validates every declared profile)
- **Inspect variables**: `uv run blueprint vars path/to/dag.dag.yaml --profile prod --unused`
- **Generate JSON schema**: `uv run blueprint schema extract`
- **Create DAG interactively**: `uv run blueprint new`

## Testing
- Run unit tests: `uv run pytest tests/ --ignore=tests/integration -v`
- Run specific test: `uv run pytest tests/test_<module>.py`
- Run with coverage: `uv run pytest --cov=blueprint tests/`
- Run integration tests locally: `uv run pytest tests/integration/ -v` (requires Astro CLI — `astro version` to verify; starts a local Airflow instance via `astro dev start --standalone`, runs tests against the REST API, then tears down)
- New features must include integration test coverage (`tests/integration/`) and be demonstrated in `examples/` -- added to the example whose idea it belongs to, or a new single-idea directory. See "Adding an Example" in `docs/CONTRIBUTING.md`. Do not grow one example into a kitchen sink.

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
  - `vars.py`: Declarative `${...}` variables and profiles resolved from `blueprint.vars.yaml`
  - `utils.py`: Common utilities
- `tests/`: Test files
- `examples/`: One directory per idea; each is independent and self-contained
  - `README.md`: Index of every example
  - `run.sh`: Launch an example locally -- `./run.sh <example> [2|3]`
  - `check.sh`: Validate every example (run by CI)
  - `_runtime/`: Shared orchestration only (docker-compose and Tiltfile), parametrised by
    the `EXAMPLE` variable
  - `<example>/`: A real Astro project -- `Dockerfile`, `requirements.txt`, `packages.txt`,
    `dags/`. The example directory is the Docker build context, so it builds exactly as a
    standalone project would, and nothing in it is specific to this repo (the `Dockerfile` is
    one `FROM` line). Keep it that way: the compose file mounts `blueprint/` over the released
    package via `PYTHONPATH`, so examples exercise the working tree -- which is also why an
    example can use an unreleased feature without a release. The mount supplies code, not
    dependencies: a new third-party dependency must go in the example's `requirements.txt`
    until the next release.
  - Examples target Airflow 3 and use its import paths directly, with no 2/3 compatibility
    shim. The package still supports 2.5.0+; that is covered by the unit test matrix.
  - `<example>/package/`: Optional installable blueprint package (shared-blueprints-package)
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
