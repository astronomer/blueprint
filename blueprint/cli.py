"""Blueprint CLI for managing reusable task templates and validating DAG definitions."""

import copy
import json
import sys
from pathlib import Path
from typing import Any

import click
import yaml
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from blueprint.loaders import discover_blueprints, get_blueprint_info, validate_yaml
from blueprint.registry import BlueprintRegistry

console = Console()


@click.group()
@click.version_option(package_name="airflow-blueprint")
def cli():
    """Blueprint - Reusable task templates composed into Airflow DAGs.

    Define reusable blueprint classes in Python, compose them into DAGs
    with YAML, and let Blueprint handle validation and wiring.
    """


def _get_configs_to_check(path: str | None) -> list[Path]:
    """Get list of configuration files to check."""
    configs_to_check = []

    if path:
        configs_to_check.append(Path(path))
    else:
        for yaml_file in Path().rglob("*.dag.yaml"):
            configs_to_check.append(yaml_file)

    return configs_to_check


def _validate_config(config_path: Path, template_dir: str | None) -> tuple[bool, str | None]:
    """Validate a single configuration file.

    Returns:
        tuple of (success, dag_id)
    """
    try:
        result = validate_yaml(str(config_path), template_dir=template_dir)
    except Exception as e:
        console.print(f"[red]FAIL[/red] {config_path}")
        if hasattr(e, "_format_message") and callable(e._format_message):
            console.print(e._format_message())
        else:
            console.print(f"  [red]Error:[/red] {e}")
        return False, None
    else:
        dag_id = result.get("dag_id")
        console.print(f"[green]PASS[/green] {config_path} (dag_id={dag_id})")
        return True, dag_id


def _check_duplicate_dag_ids(dag_ids_to_files: dict[str, list[Path]]) -> bool:
    """Check for duplicate DAG IDs and report errors.

    Returns:
        True if duplicates found, False otherwise
    """
    from blueprint.errors import DuplicateDAGIdError

    errors_found = False
    for dag_id, config_files in dag_ids_to_files.items():
        if len(config_files) > 1:
            errors_found = True
            console.print("\n[red]Duplicate DAG ID detected:[/red]")
            error = DuplicateDAGIdError(dag_id, config_files)
            console.print(str(error))
    return errors_found


@cli.command()
@click.argument("path", required=False, type=click.Path(exists=True))
@click.option("--template-dir", default=None, help="Directory containing blueprint files")
def lint(path: str | None, template_dir: str | None):
    """Validate DAG YAML definitions.

    If PATH is provided, validate a specific file.
    Otherwise, validate all .dag.yaml files in the current directory tree.
    """
    configs_to_check = _get_configs_to_check(path)

    if not configs_to_check:
        console.print("[yellow]No .dag.yaml files found.[/yellow]")
        return

    errors_found = False
    dag_ids_to_files: dict[str, list[Path]] = {}
    valid_count = 0

    for config_path in configs_to_check:
        success, dag_id = _validate_config(config_path, template_dir)

        if success and dag_id:
            if dag_id in dag_ids_to_files:
                dag_ids_to_files[dag_id].append(config_path)
            else:
                dag_ids_to_files[dag_id] = [config_path]
            valid_count += 1
        elif not success:
            errors_found = True

    if valid_count > 1 and _check_duplicate_dag_ids(dag_ids_to_files):
        errors_found = True

    if errors_found:
        sys.exit(1)


@cli.command("list")
@click.option("--template-dir", default=None, help="Directory containing blueprint files")
def list_blueprints(template_dir: str | None):
    """List available blueprints."""
    blueprints = discover_blueprints(template_dir)

    if not blueprints:
        console.print("[yellow]No blueprints found.[/yellow]")
        return

    table = Table(title="Available Blueprints", show_lines=True)
    table.add_column("Name", style="cyan", no_wrap=True)
    table.add_column("Versions", style="green", no_wrap=True)
    table.add_column("Description", overflow="fold")
    table.add_column("Class", style="dim", no_wrap=False)

    for bp in blueprints:
        versions_str = ", ".join(str(v) for v in bp["versions"])
        desc = bp["description"].split("\n")[0] if bp["description"] else "-"
        table.add_row(bp["name"], versions_str, desc, bp["class"])

    console.print(table)


@cli.command()
@click.argument("blueprint_name")
@click.option("--version", "-v", type=int, default=None, help="Specific version (default: latest)")
@click.option("--template-dir", default=None, help="Directory containing blueprint files")
def describe(blueprint_name: str, version: int | None, template_dir: str | None):
    """Show blueprint parameters and documentation."""
    try:
        info = get_blueprint_info(blueprint_name, template_dir, version=version)
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)

    versions_str = ", ".join(str(v) for v in info["versions"])
    console.print(
        Panel(
            f"[bold cyan]{info['class']}[/bold cyan] (v{info['version']})\n"
            f"{info['description']}\n"
            f"Available versions: {versions_str}",
            title=f"Blueprint: {info['name']}",
        )
    )

    if info["parameters"]:
        table = Table(title="Parameters")
        table.add_column("Name", style="cyan")
        table.add_column("Type", style="green")
        table.add_column("Required", style="yellow")
        table.add_column("Default", style="magenta")
        table.add_column("Description")

        for param_name, param_info in info["parameters"].items():
            table.add_row(
                param_name,
                param_info["type"],
                "Yes" if param_info["required"] else "No",
                str(param_info.get("default", "-")),
                param_info.get("description", "-") or "-",
            )

        console.print(table)

    console.print("\n[bold]Example YAML step:[/bold]")

    example_step: dict[str, object] = {"blueprint": blueprint_name}
    if version:
        example_step["version"] = version
    for param_name, param_info in info["parameters"].items():
        if param_info["required"]:
            example_step[param_name] = f"<{param_info['type']}>"
        elif param_info.get("default") is not None:
            example_step[param_name] = param_info["default"]

    yaml_syntax = Syntax(
        yaml.dump({"my_step": example_step}, default_flow_style=False, sort_keys=False),
        "yaml",
        theme="monokai",
    )
    console.print(yaml_syntax)


def _get_registry(template_dir: str | None) -> BlueprintRegistry:
    """Get a BlueprintRegistry for the given template directory."""
    from blueprint.loaders import get_registry

    return get_registry(template_dir)


def _build_version_schema(
    blueprint_name: str,
    version: int,
    base_name: str,
    raw_schema: dict,
) -> dict:
    """Build a schema variant for a single version of a blueprint."""
    schema_data = copy.deepcopy(raw_schema)

    if "properties" not in schema_data:
        schema_data["properties"] = {}

    schema_data["properties"]["blueprint"] = {
        "type": "string",
        "const": blueprint_name,
        "description": "The blueprint template to use",
    }
    schema_data["properties"]["version"] = {
        "type": "integer",
        "const": version,
        "description": "The blueprint version",
    }

    if "required" not in schema_data:
        schema_data["required"] = []
    schema_data["required"].insert(0, "blueprint")
    if "version" not in schema_data["required"]:
        schema_data["required"].insert(1, "version")

    schema_data["title"] = base_name
    schema_data.pop("$schema", None)

    return schema_data


@cli.command()
@click.argument("blueprint_name")
@click.option("--output", "-o", type=click.Path(), help="Output file (default: stdout)")
@click.option("--template-dir", default=None, help="Directory containing blueprint files")
def schema(blueprint_name: str, output: str | None, template_dir: str | None):
    """Generate JSON Schema for a blueprint's configuration.

    Emits a single schema covering all versions. Multi-version blueprints
    use oneOf discriminated by the version field.
    """
    try:
        reg = _get_registry(template_dir)
        versions_info = reg.get_all_versions_info(blueprint_name)
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)

    variants = [
        _build_version_schema(
            blueprint_name,
            vi["version"],
            vi["base_name"],
            vi["schema"],
        )
        for vi in versions_info
    ]

    if len(variants) == 1:
        schema_data = variants[0]
        schema_data["$schema"] = "http://json-schema.org/draft-07/schema#"
    else:
        schema_data = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": blueprint_name,
            "oneOf": variants,
            "discriminator": {"propertyName": "version"},
        }

    json_output = json.dumps(schema_data, indent=2)

    if output:
        Path(output).write_text(json_output)
        console.print(f"[green]Schema written to {output}[/green]")
    else:
        syntax = Syntax(json_output, "json", theme="monokai")
        console.print(syntax)


def _select_blueprint(blueprints: list[dict[str, Any]]) -> dict[str, Any]:
    """Select a blueprint from the available options."""
    console.print("[bold]Available blueprints:[/bold]")
    for i, bp in enumerate(blueprints):
        desc = bp["description"].split("\n")[0] if bp["description"] else "-"
        versions_str = ", ".join(str(v) for v in bp["versions"])
        console.print(f"  {i + 1}. [cyan]{bp['name']}[/cyan] (v{versions_str}) - {desc}")

    while True:
        try:
            choice = int(console.input("\nSelect blueprint (number): ")) - 1
            if 0 <= choice < len(blueprints):
                return blueprints[choice]
            console.print("[red]Invalid selection[/red]")
        except (ValueError, KeyboardInterrupt):
            console.print("\n[yellow]Cancelled[/yellow]")
            sys.exit(0)


def _convert_param_value(value: object, param_info: dict[str, Any]) -> object:
    if value:
        if param_info["type"] == "integer":
            try:
                return int(value)
            except ValueError:
                console.print("[yellow]Warning: Expected integer, using string[/yellow]")
                return value
        elif param_info["type"] == "boolean":
            return value.lower() in ("true", "yes", "1", "on")
        elif param_info["type"] == "array" and isinstance(value, str):
            return [v.strip() for v in value.split(",")]
    return value


def _collect_parameters(info: dict[str, Any]) -> dict[str, object]:
    """Collect parameter values from user input."""
    config: dict[str, object] = {}

    console.print("\n[bold]Enter configuration values:[/bold]")
    for param_name, param_info in info["parameters"].items():
        prompt = f"{param_name}"
        if param_info.get("description"):
            prompt += f" ({param_info['description']})"

        if not param_info["required"] and param_info.get("default") is not None:
            prompt += f" [default: {param_info['default']}]"

        prompt += ": "

        if param_info["required"]:
            while True:
                value = console.input(prompt)
                if value:
                    break
                console.print("[red]This field is required[/red]")
        else:
            value = console.input(prompt)
            if not value and param_info.get("default") is not None:
                value = param_info["default"]

        config[param_name] = _convert_param_value(value, param_info)

    return config


@cli.command()
@click.option("--template-dir", default=None, help="Directory containing blueprint files")
@click.option("--output-dir", default=".", help="Output directory for YAML config")
def new(template_dir: str | None, output_dir: str):
    """Interactively create a new DAG YAML definition."""
    blueprints = discover_blueprints(template_dir)

    if not blueprints:
        console.print("[red]No blueprints found.[/red]")
        sys.exit(1)

    selected = _select_blueprint(blueprints)
    console.print(f"\n[green]Selected:[/green] {selected['name']}")

    info = get_blueprint_info(selected["name"], template_dir)

    dag_id = console.input("\nDAG ID: ")
    if not dag_id:
        console.print("[red]DAG ID is required[/red]")
        sys.exit(1)

    schedule = console.input("Schedule [default: @daily]: ") or "@daily"

    step_name = console.input(f"Step name [default: {selected['name']}]: ") or selected["name"]

    step_config = _collect_parameters(info)

    dag_def: dict[str, object] = {
        "dag_id": dag_id,
        "schedule": schedule,
        "steps": {
            step_name: {
                "blueprint": selected["name"],
                **step_config,
            },
        },
    }

    filename = dag_id.replace("-", "_") + ".dag.yaml"
    file_path = Path(output_dir) / filename

    if file_path.exists() and not click.confirm(f"{file_path} already exists. Overwrite?"):
        sys.exit(0)

    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("w") as f:
        yaml.dump(dag_def, f, default_flow_style=False, sort_keys=False)

    console.print(f"\n[green]Created {file_path}[/green]")
    console.print("\nTo load this DAG, add a loader.py to your dags/ directory:")
    console.print("  from blueprint import build_all")
    console.print("  build_all()")


def main():
    """Main entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
