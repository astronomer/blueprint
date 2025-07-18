"""Shared test fixtures and utilities."""

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import pytest


@contextmanager
def change_dir(path: Path) -> Generator[None, None, None]:
    """Context manager to temporarily change the working directory."""
    original_cwd = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(original_cwd)


@pytest.fixture
def chdir():
    """Fixture to provide the change_dir context manager."""
    return change_dir
