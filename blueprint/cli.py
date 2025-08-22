"""Blueprint CLI for managing and validating DAG templates."""

import json
import sys
from pathlib import Path
from typing import Dict, List, Optional

import click
import yaml
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from blueprint import (
    discover_blueprints,
    from_yaml,
    get_blueprint_info,
    load_blueprint,
)
from blueprint.config import get_output_dir, get_template_path
from blueprint.errors import DuplicateDAGIdError
from blueprint.utils import get_airflow_dags_folder

console = Console()

# Constants
DESCRIPTION_LENGTH_LIMIT = 50
DESCRIPTION_TRUNCATE_LENGTH = 47


@click.group()
@click.version_option(package_name="airflow-blueprint")
def cli():
    """Blueprint - Reusable, validated Airflow DAG templates.

    Create and manage templated Airflow DAGs with type-safe configurations.
    """


def _get_configs_to_check(path: Optional[str]) -> List[Path]:
    """Get list of configuration files to check."""
    configs_to_check = []

    if path:
        configs_to_check.append(Path(path))
    else:
        # Find all .dag.yaml files
        for yaml_file in Path().rglob("*.dag.yaml"):
            configs_to_check.append(yaml_file)

    return configs_to_check


def _validate_config(
    config_path: Path, template_dir: str
) -> tuple[bool, Optional[str], Optional[object]]:
    """Validate a single configuration file.

    Returns:
        tuple of (success, job_id, config)
    """
    try:
        config = from_yaml(str(config_path), template_dir=template_dir, validate_only=True)
    except Exception as e:
        console.print(f"❌ {config_path}")
        if hasattr(e, "_format_message") and callable(e._format_message):
            console.print(e._format_message())  # type: ignore[misc]
        else:
            console.print(f"  [red]Error:[/red] {e}")
        return False, None, None
    else:
        console.print(f"✅ {config_path} - Valid")
        job_id = getattr(config, "job_id", None)
        return True, job_id, config


def _check_duplicate_dag_ids(dag_ids_to_files: Dict[str, List[Path]]) -> bool:
    """Check for duplicate DAG IDs and report errors.

    Returns:
        True if duplicates found, False otherwise
    """
    errors_found = False
    for dag_id, config_files in dag_ids_to_files.items():
        if len(config_files) > 1:
            errors_found = True
            console.print("\n❌ Duplicate DAG ID detected:")
            error = DuplicateDAGIdError(dag_id, config_files)
            console.print(str(error))
    return errors_found


@cli.command()
@click.argument("path", required=False, type=click.Path(exists=True))
@click.option("--template-dir", default=None, help="Template directory path")
def lint(path: Optional[str], template_dir: Optional[str]):
    """Validate blueprint configurations.

    If PATH is provided, validate a specific file.
    Otherwise, validate all .dag.yaml files in the current directory.
    """
    template_dir = get_template_path(template_dir)
    configs_to_check = _get_configs_to_check(path)

    if not configs_to_check:
        console.print("[yellow]No configuration files found.[/yellow]")
        return

    errors_found = False
    dag_ids_to_files = {}  # Track DAG IDs to detect duplicates
    valid_configs = []  # Track successfully validated configs

    # First pass: validate individual configurations
    for config_path in configs_to_check:
        success, job_id, config = _validate_config(config_path, template_dir)

        if success and config:
            if job_id:
                if job_id in dag_ids_to_files:
                    dag_ids_to_files[job_id].append(config_path)
                else:
                    dag_ids_to_files[job_id] = [config_path]
            valid_configs.append((config_path, config))
        else:
            errors_found = True

    # Second pass: check for duplicate DAG IDs (only if multiple files and no validation errors)
    if len(valid_configs) > 1 and not errors_found and _check_duplicate_dag_ids(dag_ids_to_files):
        errors_found = True

    if errors_found:
        sys.exit(1)


@cli.command("list")
@click.option("--template-dir", default=None, help="Template directory path")
def list_blueprints(template_dir: Optional[str]):
    """List available blueprints."""
    template_dir = get_template_path(template_dir)
    blueprints = discover_blueprints(template_dir)

    if not blueprints:
        console.print(f"[yellow]No blueprints found in {template_dir}[/yellow]")
        return

    table = Table(title="Available Blueprints", show_lines=True)
    table.add_column("Name", style="cyan", no_wrap=True)
    table.add_column("Description", overflow="fold")
    table.add_column("Class / Path", style="dim", no_wrap=False)

    for bp in blueprints:
        # Combine class and path info
        metadata = f"{bp['class']}\n{bp['module']}"
        table.add_row(bp["name"], bp["description"], metadata)

    console.print(table)


@cli.command()
@click.argument("blueprint_name")
@click.option("--template-dir", default=None, help="Template directory path")
def describe(blueprint_name: str, template_dir: Optional[str]):
    """Show blueprint parameters and documentation."""
    template_dir = get_template_path(template_dir)
    try:
        info = get_blueprint_info(blueprint_name, template_dir)
    except ValueError as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)

    # Print header
    console.print(
        Panel(
            f"[bold cyan]{info['class']}[/bold cyan]\n{info['description']}",
            title=f"Blueprint: {info['name']}",
        )
    )

    # Parameters table
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
                param_info.get("description", "-"),
            )

        console.print(table)

    # Example usage
    console.print("\n[bold]Example YAML configuration:[/bold]")

    example_config = {"blueprint": blueprint_name}
    for param_name, param_info in info["parameters"].items():
        if param_info["required"]:
            example_config[param_name] = f"<{param_info['type']}>"
        elif param_info.get("default") is not None:
            example_config[param_name] = param_info["default"]

    yaml_syntax = Syntax(
        yaml.dump(example_config, default_flow_style=False), "yaml", theme="monokai"
    )
    console.print(yaml_syntax)


@cli.command()
@click.argument("blueprint_name")
@click.option("--output", "-o", type=click.Path(), help="Output file (default: stdout)")
@click.option("--template-dir", default="dags/blueprints/templates", help="Template directory path")
def schema(blueprint_name: str, output: Optional[str], template_dir: str):
    """Generate JSON Schema for a blueprint.

    This can be used for YAML validation in editors like VS Code.
    """
    try:
        info = get_blueprint_info(blueprint_name, template_dir)
    except ValueError as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)

    # Add blueprint field to schema
    schema = info["schema"]
    if "properties" not in schema:
        schema["properties"] = {}

    schema["properties"]["blueprint"] = {
        "type": "string",
        "const": blueprint_name,
        "description": "The blueprint template to use",
    }

    if "required" not in schema:
        schema["required"] = []
    schema["required"].insert(0, "blueprint")

    # Add schema metadata
    schema["$schema"] = "http://json-schema.org/draft-07/schema#"
    schema["title"] = f"{info['class']} Configuration"

    # Output
    json_output = json.dumps(schema, indent=2)

    if output:
        Path(output).write_text(json_output)
        console.print(f"[green]Schema written to {output}[/green]")
    else:
        syntax = Syntax(json_output, "json", theme="monokai")
        console.print(syntax)


def _select_blueprint(blueprints):
    """Select a blueprint from the available options."""
    console.print("[bold]Available blueprints:[/bold]")
    for i, bp in enumerate(blueprints):
        console.print(f"  {i + 1}. [cyan]{bp['name']}[/cyan] - {bp['description']}")

    while True:
        try:
            choice = int(console.input("\nSelect blueprint (number): ")) - 1
            if 0 <= choice < len(blueprints):
                return blueprints[choice]
            console.print("[red]Invalid selection[/red]")
        except (ValueError, KeyboardInterrupt):
            console.print("\n[yellow]Cancelled[/yellow]")
            sys.exit(0)


def _convert_param_value(value, param_info):
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
            # Simple comma-separated list
            return [v.strip() for v in value.split(",")]
    return value


def _collect_parameters(info):
    """Collect parameter values from user input."""
    config = {"blueprint": info["name"]}

    console.print("\n[bold]Enter configuration values:[/bold]")
    for param_name, param_info in info["parameters"].items():
        # Build prompt
        prompt = f"{param_name}"
        if param_info.get("description"):
            prompt += f" ({param_info['description']})"

        if not param_info["required"] and param_info.get("default") is not None:
            prompt += f" [default: {param_info['default']}]"

        prompt += ": "

        # Get value
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


def _validate_configuration(blueprint_name, config, template_dir):
    """Validate the configuration by attempting to build the DAG."""
    try:
        blueprint_class = load_blueprint(blueprint_name, template_dir)
        # This will validate the config
        _ = blueprint_class.build(**{k: v for k, v in config.items() if k != "blueprint"})
        console.print("\n[green]✅ Configuration is valid![/green]")
    except Exception as e:
        console.print(f"\n[red]Configuration error:[/red] {e}")
        return click.confirm("Save anyway?")
    else:
        return True


def _save_configuration(config, blueprint_name, output_dir):
    """Save the configuration to a YAML file."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Generate filename from job_id or blueprint name
    filename = config.get("job_id", config.get("dag_id", blueprint_name))
    filename = filename.replace("-", "_") + ".dag.yaml"

    file_path = output_path / filename

    # Check if file exists
    if file_path.exists() and not click.confirm(f"{file_path} already exists. Overwrite?"):
        sys.exit(0)

    # Write YAML
    with file_path.open("w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    console.print(f"\n[green]Created {file_path}[/green]")
    console.print("\nTo use this DAG, ensure the YAML loader is in your DAGs directory.")


@cli.command()
@click.option("--template-dir", default=None, help="Template directory path")
@click.option("--output-dir", default=None, help="Output directory for YAML config")
def new(template_dir: Optional[str], output_dir: Optional[str]):
    """Interactively create a new DAG from a blueprint."""
    template_dir = get_template_path(template_dir)
    output_dir = get_output_dir(output_dir)
    # Discover available blueprints
    blueprints = discover_blueprints(template_dir)

    if not blueprints:
        console.print(f"[red]No blueprints found in {template_dir}[/red]")
        sys.exit(1)

    # Select blueprint
    selected = _select_blueprint(blueprints)
    console.print(f"\n[green]Selected:[/green] {selected['name']}")

    # Get blueprint info
    info = get_blueprint_info(selected["name"], template_dir)

    # Collect parameters
    config = _collect_parameters(info)

    # Validate configuration
    if not _validate_configuration(selected["name"], config, template_dir):
        sys.exit(1)

    # Save configuration
    _save_configuration(config, selected["name"], output_dir)


def _load_template(template_name: str) -> str:
    """Load a template file from the templates directory."""
    template_path = Path(__file__).parent / "templates" / template_name
    if not template_path.exists():
        msg = f"Template not found: {template_path}"
        raise FileNotFoundError(msg)
    return template_path.read_text()


def detect_environment() -> Dict[str, str]:
    """Detect project structure and suggest defaults."""
    suggestions = {}

    # Check for dags directory
    if Path("dags").exists():
        suggestions["template_path"] = "dags/blueprints/templates"
        suggestions["dags_folder"] = "dags"
    else:
        # Try to get dags folder from Airflow
        try:
            suggestions["dags_folder"] = str(get_airflow_dags_folder())
            suggestions["template_path"] = f"{suggestions['dags_folder']}/blueprints/templates"
        except Exception:
            suggestions["dags_folder"] = "dags"
            suggestions["template_path"] = "dags/blueprints/templates"

    suggestions["output_dir"] = f"{suggestions.get('dags_folder', 'dags')}/blueprints/instances"
    return suggestions


def create_dag_loader(path: str, template_path: str, output_dir: str, force: bool):
    """Create the DAG loader file."""
    # template_path and output_dir are kept for backward compatibility
    # but are no longer used since the loader reads from blueprint.toml
    _ = template_path
    _ = output_dir
    loader_content = _load_template("dag_loader.py.template")

    loader_path = Path(path)
    if loader_path.exists() and not force:
        console.print(f"[yellow]  {path} already exists. Use --force to overwrite.[/yellow]")
        return

    loader_path.parent.mkdir(parents=True, exist_ok=True)
    loader_path.write_text(loader_content)
    console.print(f"  ✅ Created DAG loader: {path}")


def create_example_blueprint(template_path: str, force: bool):
    """Create an example blueprint template."""
    example_path = Path(template_path) / "example_etl.py"

    if example_path.exists() and not force:
        console.print(
            f"[yellow]  {example_path} already exists. Use --force to overwrite.[/yellow]"
        )
        return

    example_content = _load_template("example_etl.py.template")

    example_path.parent.mkdir(parents=True, exist_ok=True)
    example_path.write_text(example_content)
    console.print(f"  ✅ Created example blueprint: {example_path}")


def handle_requirements_txt():
    """Check and update requirements.txt with airflow-blueprint package."""
    requirements_path = Path("requirements.txt")
    package_name = "airflow-blueprint"

    if requirements_path.exists():
        content = requirements_path.read_text()
        # Check if package is already in requirements
        if package_name in content:
            return

        # Ask user if they want to add it
        if click.confirm(f"\n  Add {package_name} to requirements.txt?", default=True):
            with requirements_path.open("a") as f:
                f.write(f"\n{package_name}\n")
            console.print(f"  ✅ Added {package_name} to requirements.txt")
        else:
            console.print(
                f"\n  [yellow]⚠️  Remember to add '{package_name}' to your requirements.txt[/yellow]"
            )
    else:
        console.print(
            f"\n  [yellow]⚠️  No requirements.txt found. Make sure to install '{package_name}' "
            "in your Airflow environment.[/yellow]"
        )


@cli.command()
@click.option("--force", is_flag=True, help="Overwrite existing files")
def init(force: bool):
    """Initialize Blueprint in your Airflow project."""
    console.print("[bold]Blueprint Setup Wizard[/bold]\n")

    # Check if already initialized
    if Path("blueprint.toml").exists() and not force:
        console.print(
            "[yellow]blueprint.toml already exists. Use --force to reinitialize.[/yellow]"
        )
        if not click.confirm("Continue anyway?"):
            return

    # 1. Detect environment
    detected_env = detect_environment()

    # 2. Configure paths
    console.print("📁 [bold]Configure Paths[/bold]")
    template_path = click.prompt(
        "  Template directory",
        default=detected_env.get("template_path", "dags/blueprints/templates"),
    )
    output_dir = click.prompt(
        "  Output directory for configs",
        default=detected_env.get("output_dir", "dags/blueprints/instances"),
    )

    # 3. DAG loader setup
    console.print("\n🔧 [bold]DAG Loader Setup[/bold]")
    setup_loader = click.confirm("  Create DAG loader file?", default=True)

    loader_path = None
    if setup_loader:
        default_loader_path = f"{detected_env.get('dags_folder', 'dags')}/blueprints/loader.py"
        loader_path = click.prompt("  DAG loader location", default=default_loader_path)

    # 4. Example blueprint
    console.print("\n📝 [bold]Example Blueprint[/bold]")
    create_example = click.confirm("  Create example blueprint?", default=True)

    # 5. Create files
    console.print("\n[bold]Creating files...[/bold]")

    # Create blueprint.toml
    config_content = _load_template("blueprint.toml.template").format(
        template_path=template_path, output_dir=output_dir
    )

    config_path = Path("blueprint.toml")
    if config_path.exists() and not force:
        console.print("[yellow]  blueprint.toml already exists. Use --force to overwrite.[/yellow]")
    else:
        config_path.write_text(config_content)
        console.print("  ✅ Created blueprint.toml")

    # Create DAG loader if requested
    if setup_loader and loader_path:
        create_dag_loader(loader_path, template_path, output_dir, force)

    # Create example blueprint
    if create_example:
        create_example_blueprint(template_path, force)

    # Handle requirements.txt
    handle_requirements_txt()

    # 6. Next steps
    console.print("\n[green]✨ Blueprint initialized![/green]")
    console.print("\n[bold]Next steps:[/bold]")
    console.print("  1. Run [cyan]blueprint list[/cyan] to see available blueprints")
    if create_example:
        console.print("  2. Run [cyan]blueprint new[/cyan] to create a DAG from the example")
    console.print("  3. Commit [cyan]blueprint.toml[/cyan] to share configuration with your team")
    if setup_loader:
        console.print(f"  4. Ensure [cyan]{loader_path}[/cyan] is in your DAGs folder")


def main():
    """Main entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
