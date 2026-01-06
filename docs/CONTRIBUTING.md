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

- Python 3.8+ (we test on 3.8, 3.9, 3.10, 3.11, 3.12)
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
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Install Dependencies

```bash
# Install all dependencies including dev dependencies
uv sync --all-extras --dev

# Activate the virtual environment
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

### Verify Installation

```bash
# Test that Blueprint is installed correctly
uv run blueprint --help

# Run a quick test
uv run pytest tests/test_core.py -v
```

## Running Tests

### Full Test Suite

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=blueprint --cov-report=html

# Run tests in parallel (faster)
uv run pytest -n auto
```

### Specific Test Categories

```bash
# Unit tests only
uv run pytest tests/test_*.py

# Integration tests
uv run pytest tests/test_integration.py

# CLI tests
uv run pytest tests/test_cli.py

# Run tests for a specific module
uv run pytest tests/test_registry.py -v
```

### Testing with Different Python Versions

```bash
# Install and test with Python 3.10
uv python install 3.10
uv run --python 3.10 pytest

# Test with Python 3.11
uv python install 3.11
uv run --python 3.11 pytest
```

## Code Style

We use several tools to maintain code quality:

### Linting and Formatting

```bash
# Check code style (this runs in CI)
uv run ruff check blueprint/ tests/

# Auto-fix issues where possible
uv run ruff check blueprint/ tests/ --fix

# Format code
uv run ruff format blueprint/ tests/

# Type checking
uv run ty check blueprint/
```

### Pre-commit Hooks

We recommend setting up pre-commit hooks:

```bash
# Install pre-commit hooks
uv run pre-commit install

# Run hooks manually
uv run pre-commit run --all-files
```

### Code Style Guidelines

- Follow PEP 8
- Use type hints for all public APIs
- Add docstrings to all public functions and classes
- Keep line length at or under 100 characters
- Use descriptive variable names
- Add comments for complex logic

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

### Adding New Features

When adding new features:

1. **Start with tests** - write tests that describe the expected behavior
2. **Implement the feature** to make tests pass
3. **Add integration tests** if the feature affects multiple components
4. **Update CLI help text** and documentation
5. **Add examples** in the `examples/` directory if applicable

### Bug Fixes

For bug fixes:

1. **Write a test** that reproduces the bug
2. **Fix the bug** so the test passes
3. **Ensure no regressions** by running the full test suite

## Testing Your Changes

### Manual Testing

Test your changes manually with the examples:

```bash
# Test CLI commands from project root
uv run blueprint list
uv run blueprint describe daily_etl
uv run blueprint lint

# Test DAG generation
python dags/blueprint_loader.py
```

### Testing with Real Airflow

You can test with a local Airflow instance using Tilt:

```bash
# Install Tilt (if not already installed)
# macOS: brew install tilt-dev/tap/tilt
# Other platforms: https://docs.tilt.dev/install.html

# Start the development environment
tilt up

# Access Airflow at http://localhost:8080
# Username: airflow, Password: airflow

# View Tilt dashboard at http://localhost:10350
```

This will:
- Build a custom Airflow image with Blueprint installed
- Set up live code reloading when you make changes
- Mount your local Blueprint code into the container
- Automatically restart services when needed

### Testing Edge Cases

Consider testing:

- Empty configuration files
- Invalid YAML syntax
- Missing blueprint files
- Circular imports
- Large configuration files
- Unicode in configuration values

## Submitting Pull Requests

### Before Submitting

1. **Rebase** your branch on the latest main:
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```

2. **Run the full test suite**:
   ```bash
   uv run pytest
   uv run ruff check blueprint/ tests/
   uv run ty check blueprint/
   ```

3. **Update documentation** if your changes affect user-facing behavior

4. **Ensure CI passes** by checking GitHub Actions

### Pull Request Guidelines

- **Clear title**: Describe what the PR does in one line
- **Detailed description**: Explain the problem and solution
- **Link issues**: Reference any related GitHub issues
- **Add screenshots**: For UI changes, include before/after screenshots
- **Request reviews**: Tag relevant maintainers

### PR Template

```markdown
## What this PR does
Brief description of the changes

## Why this change is needed
Explain the problem this solves

## Testing
- [ ] Added/updated tests
- [ ] Manual testing performed
- [ ] All tests pass locally

## Documentation
- [ ] Updated docstrings
- [ ] Updated README/docs if needed
- [ ] Added examples if applicable

## Checklist
- [ ] Code follows style guidelines
- [ ] No breaking changes (or they're documented)
- [ ] Backwards compatibility maintained
```

## Release Process

The release process is handled by maintainers, but here's how it works:

### Version Bumping

We follow [Semantic Versioning](https://semver.org/):

- **MAJOR** (1.0.0): Breaking changes
- **MINOR** (0.1.0): New features, backwards compatible
- **PATCH** (0.0.1): Bug fixes, backwards compatible

### Release Steps

1. **Update version** in `pyproject.toml`
2. **Create release notes** describing the changes
3. **Create release commit**:
   ```bash
   git commit -m "Release v0.1.0"
   git tag v0.1.0
   ```
4. **Push to GitHub**:
   ```bash
   git push origin main --tags
   ```
5. **GitHub Actions** automatically builds and publishes to PyPI

### Release Notes

When contributing to releases, help by:

- Writing clear, user-focused descriptions of changes
- Categorizing changes (Features, Bug Fixes, Breaking Changes)
- Including examples for new features
- Documenting any migration steps needed

## Project Structure

Understanding the codebase structure:

```
blueprint/
├── __init__.py          # Main public API
├── cli.py              # Command-line interface
├── config.py           # Configuration management
├── core.py             # Blueprint base class
├── errors.py           # Custom exceptions
├── loaders.py          # YAML/Python loading
├── models.py           # Pydantic models
├── registry.py         # Blueprint discovery
└── template_loader.py  # Template path management

Tiltfile                 # Development environment setup

tests/
├── conftest.py         # Pytest fixtures
├── test_cli.py         # CLI testing
├── test_config.py      # Configuration tests
├── test_core.py        # Core functionality tests
├── test_errors.py      # Error handling tests
├── test_loaders.py     # Loading mechanism tests
└── test_registry.py    # Registry tests

examples/
├── dags/               # Example DAG configurations
├── blueprint/templates/   # Example blueprint templates
├── docker-compose.yaml # Airflow services definition
└── Dockerfile          # Custom Airflow image
```

### Key Components

- **Blueprint Core** (`core.py`): Base class for all blueprints
- **Registry** (`registry.py`): Discovers and manages blueprints
- **Loaders** (`loaders.py`): Load blueprints from YAML and Python
- **CLI** (`cli.py`): Command-line interface using Click
- **Configuration** (`config.py`): Handle settings and paths

## Troubleshooting

### Common Issues

**Import errors when running tests:**
```bash
# Make sure you're in the virtual environment
source .venv/bin/activate

# Reinstall in development mode
uv sync --all-extras --dev
```

**Tests fail with path issues:**
```bash
# Run tests from the project root
cd /path/to/blueprint
uv run pytest
```

**Linting errors:**
```bash
# Auto-fix most issues
uv run ruff check blueprint/ tests/ --fix

# Format code
uv run ruff format blueprint/ tests/
```

**Type checking errors:**
```bash
# Install type checking dependencies
uv sync --all-extras --dev

# Run type checker
uv run ty check blueprint/
```

### Getting Help

- **GitHub Issues**: For bugs and feature requests
- **GitHub Discussions**: For questions and general discussion
- **Code Review**: Don't hesitate to ask for feedback on draft PRs

### Development Tips

1. **Use the examples**: The `examples/` directory is great for testing changes
2. **Test incrementally**: Run specific tests while developing
3. **Read existing tests**: They show how components are intended to work
4. **Use debug mode**: Set `BLUEPRINT_DEBUG=1` for verbose output
5. **Profile performance**: For large changes, consider performance impact

## Thank You!

Thank you for contributing to Blueprint! Your efforts help make data pipeline development easier for everyone. If you have questions or need help getting started, don't hesitate to open an issue or discussion.
