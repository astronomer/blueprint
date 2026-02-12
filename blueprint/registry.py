"""Global registry for Blueprint discovery and management with version tracking."""

import importlib.util
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any

from blueprint.core import Blueprint
from blueprint.errors import BlueprintNotFoundError, DuplicateBlueprintError, InvalidVersionError

logger = logging.getLogger(__name__)


class BlueprintRegistry:
    """Registry for discovered blueprints with version tracking.

    Blueprints are tracked as: name -> {version: class}

    Version is auto-detected from the class name:
        Extract -> ('extract', 1)
        ExtractV2 -> ('extract', 2)
        MultiSourceETLV3 -> ('multi_source_etl', 3)
    """

    def __init__(
        self,
        template_dirs: list[Path] | None = None,
        exclude_files: set[Path] | None = None,
    ) -> None:
        self._blueprints: dict[str, dict[int, type[Blueprint]]] = {}
        self._blueprint_locations: dict[str, dict[int, str]] = {}
        self._discovered = False
        self._discovery_in_progress = False
        self._template_dirs = template_dirs
        self._exclude_files = {p.resolve() for p in exclude_files} if exclude_files else set()

    def get_template_dirs(self) -> list[Path]:
        """Get all template directories to search."""
        if self._template_dirs is not None:
            return list(self._template_dirs)

        dirs: list[Path] = []

        airflow_home = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
        default_path = Path(airflow_home) / "dags"
        dirs.append(default_path)

        local_path = Path("dags").resolve()
        if local_path != default_path.resolve() and local_path.exists():
            dirs.append(local_path)

        return dirs

    def discover(self, force: bool = False) -> None:
        """Discover all blueprints in template directories.

        Args:
            force: Force re-discovery even if already discovered
        """
        if self._discovery_in_progress:
            return

        if self._discovered and not force:
            return

        self._blueprints.clear()
        self._blueprint_locations.clear()

        self._discovery_in_progress = True
        try:
            for template_dir in self.get_template_dirs():
                if template_dir.exists():
                    self._discover_in_directory(template_dir)
        finally:
            self._discovery_in_progress = False

        self._discovered = True

    def _discover_in_directory(self, directory: Path) -> None:
        """Discover blueprints in a specific directory by importing modules."""
        for py_file in directory.rglob("*.py"):
            if py_file.name.startswith("_"):
                continue
            if py_file.resolve() in self._exclude_files:
                continue

            try:
                relative_path = py_file.relative_to(directory)
                module_path_parts = list(relative_path.parts)
                module_path_parts[-1] = relative_path.stem
                flat_module_path = "_".join(module_path_parts)

                module_name = f"_blueprint_templates_{directory.name}_{flat_module_path}"
                spec = importlib.util.spec_from_file_location(module_name, py_file)
                if spec and spec.loader:
                    module = importlib.util.module_from_spec(spec)
                    sys.modules[module_name] = module
                    spec.loader.exec_module(module)

                    for name in dir(module):
                        obj = getattr(module, name)
                        if (
                            isinstance(obj, type)
                            and issubclass(obj, Blueprint)
                            and obj is not Blueprint
                            and obj.__module__ == module_name
                        ):
                            self._register_class(obj, py_file, directory)

            except DuplicateBlueprintError:
                raise
            except (ImportError, SyntaxError) as e:
                logger.warning("Failed to load %s: %s", py_file, e)

    def _register_class(self, cls: type[Blueprint], py_file: Path, base_dir: Path) -> None:
        """Register a blueprint class with its parsed name and version."""
        bp_name, version = cls.parse_name_and_version()

        try:
            location = str(py_file.relative_to(base_dir.parent.parent))
        except ValueError:
            location = str(py_file)

        if bp_name not in self._blueprints:
            self._blueprints[bp_name] = {}
            self._blueprint_locations[bp_name] = {}

        if version in self._blueprints[bp_name]:
            existing_loc = self._blueprint_locations[bp_name][version]
            dup_name = f"{bp_name} (v{version})"
            raise DuplicateBlueprintError(dup_name, [existing_loc, location])

        self._blueprints[bp_name][version] = cls
        self._blueprint_locations[bp_name][version] = location

    def get(self, name: str, version: int | None = None) -> type[Blueprint]:
        """Get a blueprint by name and optional version.

        Args:
            name: Blueprint name (snake_case)
            version: Specific version number. None returns the latest version.

        Returns:
            The Blueprint class

        Raises:
            BlueprintNotFoundError: If blueprint name not found
            InvalidVersionError: If specific version not found
        """
        self.discover()

        if name not in self._blueprints:
            available = list(self._blueprints.keys())
            raise BlueprintNotFoundError(name, available)

        versions = self._blueprints[name]

        if version is None:
            latest = max(versions.keys())
            return versions[latest]

        if version not in versions:
            raise InvalidVersionError(name, version, list(versions.keys()))

        return versions[version]

    def get_latest_version(self, name: str) -> int:
        """Get the latest version number for a blueprint."""
        self.discover()

        if name not in self._blueprints:
            available = list(self._blueprints.keys())
            raise BlueprintNotFoundError(name, available)

        return max(self._blueprints[name].keys())

    def list_blueprints(self) -> list[dict[str, Any]]:
        """List all available blueprints with metadata.

        Returns:
            List of blueprint information dictionaries
        """
        self.discover()

        result = []
        for name, versions in sorted(self._blueprints.items()):
            latest_version = max(versions.keys())
            latest_cls = versions[latest_version]
            all_versions = sorted(versions.keys())

            result.append(
                {
                    "name": name,
                    "class": latest_cls.__name__,
                    "description": latest_cls.__doc__ or "No description",
                    "versions": all_versions,
                    "latest_version": latest_version,
                    "locations": self._blueprint_locations.get(name, {}),
                }
            )

        return result

    def get_blueprint_info(self, name: str, version: int | None = None) -> dict[str, Any]:
        """Get detailed information about a specific blueprint.

        Args:
            name: Blueprint name
            version: Specific version (None for latest)

        Returns:
            Dictionary with blueprint information including schema
        """
        cls = self.get(name, version)
        resolved_version = version if version is not None else self.get_latest_version(name)
        schema = cls.get_schema()

        parameters = {}
        if "properties" in schema:
            for param_name, param_schema in schema["properties"].items():
                parameters[param_name] = {
                    "type": param_schema.get("type", "string"),
                    "description": param_schema.get("description", ""),
                    "default": param_schema.get("default"),
                    "required": param_name in schema.get("required", []),
                    "pattern": param_schema.get("pattern"),
                    "minimum": param_schema.get("minimum"),
                    "maximum": param_schema.get("maximum"),
                    "enum": param_schema.get("enum"),
                }

        all_versions = sorted(self._blueprints.get(name, {}).keys())

        return {
            "name": name,
            "class": cls.__name__,
            "version": resolved_version,
            "versions": all_versions,
            "description": cls.__doc__ or "No description",
            "parameters": parameters,
            "schema": schema,
            "locations": self._blueprint_locations.get(name, {}),
        }

    def get_all_versions_info(self, name: str) -> list[dict[str, Any]]:
        """Get schema information for all versions of a blueprint.

        Args:
            name: Blueprint name (snake_case)

        Returns:
            List of dicts sorted by version, each with version, class name,
            base_name (class name without V{N} suffix), and schema.
        """
        self.discover()

        if name not in self._blueprints:
            available = list(self._blueprints.keys())
            raise BlueprintNotFoundError(name, available)

        result = []
        for version in sorted(self._blueprints[name]):
            cls = self._blueprints[name][version]
            bp_name, _ = cls.parse_name_and_version()

            base_class_name = cls.__name__
            version_match = re.match(r"^(.+?)V\d+$", base_class_name)
            if version_match:
                base_class_name = version_match.group(1)

            result.append(
                {
                    "version": version,
                    "class": cls.__name__,
                    "base_name": base_class_name,
                    "schema": cls.get_schema(),
                }
            )

        return result

    def clear(self) -> None:
        """Clear the registry and force re-discovery on next access."""
        self._blueprints.clear()
        self._blueprint_locations.clear()
        self._discovered = False
        self._discovery_in_progress = False


registry = BlueprintRegistry()
