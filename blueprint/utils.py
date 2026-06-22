"""Common utilities shared across the blueprint package."""

from pathlib import Path


def display_path(path: str | Path, base: Path | None = None) -> str:
    """Render a path relative to a base directory for display.

    Falls back to the absolute path when it is not located under the base
    directory (e.g. a sibling tree or a different drive on Windows).

    Args:
        path: Absolute or relative filesystem path.
        base: Directory to render relative to. Defaults to the current working
            directory, resolved at call time.

    Returns:
        The path relative to ``base`` when it is below it, otherwise the
        absolute path. A falsy ``path`` is returned unchanged.
    """
    if not path:
        return str(path)
    base = (base or Path.cwd()).resolve()
    resolved = Path(path).resolve()
    try:
        return str(resolved.relative_to(base))
    except ValueError:
        return str(resolved)
