# Contributing to Blueprint

Thank you for your interest in contributing to Blueprint! This guide will help you get started with local development, testing, and contributing to the project.

## Table of Contents

- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Running Tests](#running-tests)
- [Code Style](#code-style)
- [Making Changes](#making-changes)
- [Testing Your Changes](#testing-your-changes)
- [Submitting Pull Requests](#submitting-pull-requests)
- [Release Process](#release-process)
- [Project Structure](#project-structure)
- [Troubleshooting](#troubleshooting)

## Getting Started

### Prerequisites

- Python 3.10+ (we test on 3.10, 3.11, 3.12)
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- Git

### Fork and Clone

1. Fork the repository on GitHub
2. Clone your fork locally:

```bash
git clone https://github.com/YOUR-USERNAME/blueprint.git
cd blueprint
```

## Development Setup

We use `uv` for dependency management and development. If you don't have it installed:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Install Dependencies

```bash
uv sync --all-extras --dev
source .venv/bin/activate
```

### Verify Installation

```bash
uv run blueprint --help
uv run pytest tests/test_core.py -v
```

## Running Tests

### Full Test Suite

```bash
uv run pytest
uv run pytest --cov=blueprint --cov-report=html
```

### Specific Test Categories

```bash
uv run pytest tests/test_core.py -v       # Core Blueprint class
uv run pytest tests/test_builder.py -v     # Builder, DAGConfig, dependency validation
uv run pytest tests/test_registry.py -v    # Version-aware registry
uv run pytest tests/test_loaders.py -v     # YAML loading, Jinja2, discovery
uv run pytest tests/test_cli.py -v         # CLI commands
uv run pytest tests/test_errors.py -v      # Error types and messages
```

### Testing with Different Python Versions

```bash
uv python install 3.10
uv run --python 3.10 pytest
```

## Code Style

### Linting and Formatting

```bash
uv run ruff check blueprint/ tests/
uv run ruff check blueprint/ tests/ --fix
uv run ruff format blueprint/ tests/
uv run ty check blueprint/
```

### Pre-commit Hooks

```bash
uv run pre-commit install
uv run pre-commit run --all-files
```

### Code Style Guidelines

- Follow PEP 8
- Use type hints for all public APIs
- Add docstrings to all public functions and classes
- Keep line length at or under 100 characters
- Use descriptive variable names
- Use pathlib for file operations

## Making Changes

### Development Workflow

1. **Create a branch** for your feature/fix:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** following the coding standards

3. **Add tests** for new functionality

4. **Run tests** to ensure everything works:
   ```bash
   uv run pytest
   uv run ruff check blueprint/ tests/
   uv run ty check blueprint/
   ```

5. **Update documentation** if needed

### Adding a New Blueprint Feature

When adding features to the Blueprint framework:

1. Start with tests describing the expected behavior
2. Update `core.py` if the Blueprint base class is affected
3. Update `builder.py` if DAG building logic changes
4. Update `registry.py` if discovery/versioning is affected
5. Update CLI commands in `cli.py` if user-facing
6. Add examples in `examples/dags/`

### Adding a New Error Type

1. Add the error class in `errors.py`
2. Export it from `__init__.py`
3. Add tests in `test_errors.py`

## Testing Your Changes

### Manual Testing

```bash
# Test CLI commands
uv run blueprint list --template-dir examples/dags/
uv run blueprint describe extract --template-dir examples/dags/
uv run blueprint lint examples/dags/customer_etl.dag.yaml --template-dir examples/dags/
```

### Testing with Real Airflow

You can test with a local Airflow instance using Tilt:

```bash
cd examples/airflow3   # or examples/airflow2
tilt up

# Access Airflow at http://localhost:8080
# Tilt dashboard at http://localhost:10350
```

### Testing Edge Cases

Consider testing:

- Empty YAML files
- Invalid YAML syntax
- Missing blueprint names
- Cyclic dependencies between steps
- Invalid version references
- Unicode in configuration values
- Deep-merging of default_args

## Submitting Pull Requests

### Before Submitting

1. Rebase on latest main
2. Run full test suite
3. Ensure lint and type checks pass
4. Update documentation if needed

### PR Guidelines

- Clear title describing the change
- Link any related issues
- Include tests for new functionality
- Update CLAUDE.md if project structure changes

## Release Process

### Version Bumping

We follow [Semantic Versioning](https://semver.org/):

- **MAJOR**: Breaking changes
- **MINOR**: New features, backwards compatible
- **PATCH**: Bug fixes

### Release Steps

1. Update version in `blueprint/__init__.py`
2. Create release commit and tag
3. GitHub Actions builds and publishes to PyPI

## Project Structure

```
blueprint/
├── __init__.py          # Public API exports
├── core.py              # Blueprint base class (renders TaskOrGroup)
├── builder.py           # DAGConfig, StepConfig, Builder, build_all()
├── registry.py          # Version-aware blueprint discovery
├── loaders.py           # YAML loading, Jinja2 rendering
├── cli.py               # CLI using Click
├── errors.py            # Custom exceptions
├── models.py            # Pydantic re-exports
└── utils.py             # Common utilities

tests/
├── conftest.py          # Pytest fixtures
├── test_core.py         # Blueprint base class tests
├── test_builder.py      # Builder, DAGConfig, dependencies, cycles
├── test_registry.py     # Version-aware registry tests
├── test_loaders.py      # YAML loading, discovery tests
├── test_cli.py          # CLI command tests
└── test_errors.py       # Error handling tests

examples/
├── airflow2/            # Airflow 2 infrastructure
│   ├── Dockerfile
│   ├── Tiltfile
│   └── docker-compose.yaml
├── airflow3/            # Airflow 3 infrastructure
│   ├── Dockerfile
│   ├── Tiltfile
│   └── docker-compose.yaml
└── dags/                # Shared DAGs directory
    ├── etl_blueprints.py       # Blueprint class definitions
    ├── customer_etl.dag.yaml   # DAG composed from steps
    ├── simple_pipeline.dag.yaml
    └── loader.py               # DAG loader (build_all)
```

### Key Components

- **Blueprint** (`core.py`): Base class for reusable task group templates. Renders into `TaskOrGroup`.
- **Builder** (`builder.py`): Composes DAGs from YAML step definitions. Handles dependency validation, cycle detection, config merging, and step context injection.
- **Registry** (`registry.py`): Discovers blueprints and tracks versions. Auto-detects version from `V{N}` class name suffix.
- **Loaders** (`loaders.py`): YAML loading with Jinja2 support, blueprint discovery helpers.
- **CLI** (`cli.py`): Commands for listing, describing, validating, and creating DAGs.

## Troubleshooting

### Common Issues

**Import errors when running tests:**
```bash
source .venv/bin/activate
uv sync --all-extras --dev
```

**Tests fail with path issues:**
```bash
cd /path/to/blueprint
uv run pytest
```

**Linting errors:**
```bash
uv run ruff check blueprint/ tests/ --fix
uv run ruff format blueprint/ tests/
```

### Getting Help

- **GitHub Issues**: For bugs and feature requests
- **GitHub Discussions**: For questions and general discussion

## Thank You!

Thank you for contributing to Blueprint! Your efforts help make data pipeline development easier for everyone.
