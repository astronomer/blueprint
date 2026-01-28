"""Global registry for Blueprint discovery and management."""

import ast
import importlib.util
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Type

from blueprint.core import Blueprint
from blueprint.errors import BlueprintNotFoundError, DuplicateBlueprintError


class BlueprintRegistry:
    """Registry for discovered blueprints with caching and conflict detection."""

    def __init__(self):
        self._blueprints: Dict[str, Type[Blueprint]] = {}
        self._blueprint_locations: Dict[str, List[str]] = {}
        self._discovered = False
        self._template_dirs: List[Path] = []
        self._cached_list: Optional[List[Dict[str, Any]]] = None

    def get_template_dirs(self) -> List[Path]:
        """Get all template directories to search."""
        dirs = []

        # First priority: AIRFLOW_HOME/dags (where self-rendering Blueprint templates live)
        airflow_home = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
        default_path = Path(airflow_home) / "dags"
        dirs.append(default_path)

        # Second priority: dags/ in current directory
        local_path = Path("dags")
        if local_path not in dirs and local_path.exists():
            dirs.append(local_path)

        return dirs

    def discover_blueprints(self, force: bool = False) -> None:
        """Discover all blueprints in template directories.

        Args:
            force: Force re-discovery even if already discovered
        """
        if self._discovered and not force:
            return

        # Clear existing registrations
        self._blueprints.clear()
        self._blueprint_locations.clear()
        self._cached_list = None

        # Get template directories
        self._template_dirs = self.get_template_dirs()

        # Discover blueprints in each directory
        for template_dir in self._template_dirs:
            if template_dir.exists():
                self._discover_in_directory(template_dir)

        self._discovered = True

    def _discover_in_directory(self, directory: Path) -> None:
        """Discover blueprints in a specific directory."""
        for py_file in directory.rglob("*.py"):
            if py_file.name.startswith("_"):
                continue

            try:
                # Create a unique module name from the relative path
                relative_path = py_file.relative_to(directory)
                module_path_parts = list(relative_path.parts)
                module_path_parts[-1] = relative_path.stem
                flat_module_path = "_".join(module_path_parts)

                # Load the module
                module_name = f"_blueprint_templates_{directory.name}_{flat_module_path}"
                spec = importlib.util.spec_from_file_location(module_name, py_file)
                if spec and spec.loader:
                    module = importlib.util.module_from_spec(spec)
                    sys.modules[module_name] = module
                    spec.loader.exec_module(module)

                    # Find all Blueprint subclasses defined in this module
                    # (not just imported from elsewhere)
                    for name in dir(module):
                        obj = getattr(module, name)
                        if (
                            isinstance(obj, type)
                            and issubclass(obj, Blueprint)
                            and obj is not Blueprint
                            and obj.__module__ == module_name  # Only if defined here
                        ):
                            blueprint_name = self._get_blueprint_name(obj.__name__)
                            location = f"{py_file.relative_to(directory.parent.parent)}"

                            # Track locations for conflict detection
                            if blueprint_name not in self._blueprint_locations:
                                self._blueprint_locations[blueprint_name] = []
                            self._blueprint_locations[blueprint_name].append(location)

                            # Register blueprint (last one wins for now)
                            self._blueprints[blueprint_name] = obj

            except Exception as e:
                # Log but continue discovering other files
                print(f"Warning: Failed to load {py_file}: {e}")

    def _get_blueprint_name(self, class_name: str) -> str:
        """Convert class name to blueprint name (CamelCase to snake_case)."""
        # Handle consecutive capitals and normal camelCase
        name = re.sub("([A-Z]+)([A-Z][a-z])", r"\1_\2", class_name)
        name = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", name)
        return name.lower()

    def get_blueprint(self, name: str) -> Type[Blueprint]:
        """Get a blueprint by name.

        Args:
            name: Blueprint name (snake_case)

        Returns:
            The Blueprint class

        Raises:
            BlueprintNotFoundError: If blueprint not found
            DuplicateBlueprintError: If multiple blueprints with same name
        """
        # Ensure discovery has run
        self.discover_blueprints()

        # Check for conflicts
        if name in self._blueprint_locations and len(self._blueprint_locations[name]) > 1:
            raise DuplicateBlueprintError(name, self._blueprint_locations[name])

        # Get blueprint
        if name not in self._blueprints:
            raise BlueprintNotFoundError(name, list(self._blueprints.keys()))

        return self._blueprints[name]

    def list_blueprints(self) -> List[Dict[str, Any]]:
        """List all available blueprints with metadata.

        Returns:
            List of blueprint information dictionaries
        """
        # Ensure discovery has run
        self.discover_blueprints()

        # Use cached list if available
        if self._cached_list is not None:
            return self._cached_list

        # For listing, we don't need to import - just scan files
        self._cached_list = self._list_blueprints_without_import()
        return self._cached_list

    def _list_blueprints_without_import(self) -> List[Dict[str, Any]]:
        """List blueprints by scanning files without importing them."""

        blueprints = {}  # Use dict to detect duplicates
        blueprint_locations = {}  # Track all locations for each blueprint name
        template_dirs = self.get_template_dirs()

        for template_dir in template_dirs:
            if not template_dir.exists():
                continue

            for py_file in template_dir.rglob("*.py"):
                if py_file.name.startswith("_"):
                    continue

                try:
                    with py_file.open() as f:
                        content = f.read()

                    tree = ast.parse(content)

                    for node in ast.walk(tree):
                        if isinstance(node, ast.ClassDef) and self._is_blueprint_class_ast(node):
                            blueprint_name = self._get_blueprint_name(node.name)

                            # Track location for duplicate detection
                            if blueprint_name not in blueprint_locations:
                                blueprint_locations[blueprint_name] = []

                            location = (
                                str(py_file.relative_to(template_dir.parent.parent))
                                if template_dir.parent.parent in py_file.parents
                                else str(py_file)
                            )
                            blueprint_locations[blueprint_name].append(location)

                            # Extract docstring
                            description = "No description"
                            if (
                                node.body
                                and isinstance(node.body[0], ast.Expr)
                                and isinstance(node.body[0].value, ast.Constant)
                                and isinstance(node.body[0].value.value, str)
                            ):
                                description = node.body[0].value.value

                            # Check for duplicates
                            if blueprint_name in blueprints:
                                # Duplicate found - raise error
                                locations = blueprint_locations[blueprint_name]
                                self._raise_duplicate_error(blueprint_name, locations)

                            blueprints[blueprint_name] = {
                                "name": blueprint_name,
                                "class": node.name,
                                "module": location,  # Use full path instead of just filename
                                "description": description,
                                "schema": {},  # Would need full import to get schema
                                "locations": [location],
                            }

                except Exception as e:
                    # Skip files that can't be parsed, but re-raise DuplicateBlueprintError
                    if isinstance(e, DuplicateBlueprintError):
                        raise
                    print(f"Warning: Could not parse {py_file}: {e}")
                    continue

        return sorted(blueprints.values(), key=lambda x: x["name"])

    def _raise_duplicate_error(self, blueprint_name: str, locations: List[str]) -> None:
        """Raise a DuplicateBlueprintError."""
        raise DuplicateBlueprintError(blueprint_name, locations)

    def _is_blueprint_class_ast(self, node: "ast.ClassDef") -> bool:
        """Check if AST node represents a Blueprint subclass."""
        for base in node.bases:
            # Check for Blueprint[ConfigType] or just Blueprint
            if isinstance(base, ast.Name) and base.id == "Blueprint":
                return True
            if (
                isinstance(base, ast.Subscript)
                and isinstance(base.value, ast.Name)
                and base.value.id == "Blueprint"
            ):
                return True
        return False

    def get_blueprint_info(self, name: str) -> Dict[str, Any]:
        """Get detailed information about a specific blueprint.

        Args:
            name: Blueprint name

        Returns:
            Dictionary with blueprint information including schema
        """
        blueprint_class = self.get_blueprint(name)

        # Get schema
        schema = blueprint_class.get_schema()

        # Extract parameter info
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

        # Get defaults from the schema
        defaults = {
            name: info["default"]
            for name, info in parameters.items()
            if "default" in info and info["default"] is not None
        }

        return {
            "name": name,
            "class": blueprint_class.__name__,
            "description": blueprint_class.__doc__ or "No description",
            "parameters": parameters,
            "defaults": defaults,
            "schema": schema,
            "locations": self._blueprint_locations.get(name, []),
        }

    def clear(self) -> None:
        """Clear the registry and force re-discovery on next access."""
        self._blueprints.clear()
        self._blueprint_locations.clear()
        self._discovered = False


# Global registry instance
registry = BlueprintRegistry()


# Convenience functions that use the global registry
def get_blueprint(name: str) -> Type[Blueprint]:
    """Get a blueprint by name from the global registry."""
    return registry.get_blueprint(name)


def list_blueprints() -> List[Dict[str, Any]]:
    """List all available blueprints from the global registry."""
    return registry.list_blueprints()


def get_blueprint_info(name: str) -> Dict[str, Any]:
    """Get detailed information about a blueprint from the global registry."""
    return registry.get_blueprint_info(name)


def discover_blueprints(force: bool = False) -> None:
    """Discover all blueprints in template directories."""
    registry.discover_blueprints(force)
