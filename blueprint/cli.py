"""Blueprint CLI for managing and validating DAG templates."""

import json
import sys
from pathlib import Path
from typing import Dict, Optional

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
from blueprint.template_loader import get_airflow_dags_folder

console = Console()

# Constants
DESCRIPTION_LENGTH_LIMIT = 50
DESCRIPTION_TRUNCATE_LENGTH = 47


@click.group()
@click.version_option()
def cli():
    """Blueprint - Reusable, validated Airflow DAG templates.

    Create and manage templated Airflow DAGs with type-safe configurations.
    """


@cli.command()
@click.argument("path", required=False, type=click.Path(exists=True))
@click.option("--template-dir", default=None, help="Template directory path")
def lint(path: Optional[str], template_dir: Optional[str]):
    """Validate blueprint configurations.

    If PATH is provided, validate a specific file.
    Otherwise, validate all .dag.yaml files in the current directory.
    """
    template_dir = get_template_path(template_dir)
    configs_to_check = []

    if path:
        configs_to_check.append(Path(path))
    else:
        # Find all .dag.yaml files
        for yaml_file in Path().rglob("*.dag.yaml"):
            configs_to_check.append(yaml_file)

    if not configs_to_check:
        console.print("[yellow]No configuration files found.[/yellow]")
        return

    errors_found = False

    for config_path in configs_to_check:
        try:
            # Validate configuration without rendering DAG (no Airflow import needed)
            _ = from_yaml(str(config_path), template_dir=template_dir, validate_only=True)
            console.print(f"✅ {config_path} - Valid")
        except Exception as e:
            errors_found = True
            console.print(f"❌ {config_path}")
            # The new errors format themselves nicely
            if hasattr(e, "_format_message") and callable(e._format_message):
                console.print(e._format_message())  # type: ignore[misc]
            else:
                console.print(f"  [red]Error:[/red] {e}")

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
@click.option("--template-dir", default=".astro/templates", help="Template directory path")
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


def detect_environment() -> Dict[str, str]:
    """Detect project structure and suggest defaults."""
    suggestions = {}

    # Check for Astronomer
    if Path(".astro").exists():
        suggestions["template_path"] = ".astro/templates"
        suggestions["dags_folder"] = "dags"
    # Check for standalone Airflow
    elif Path("dags").exists():
        suggestions["template_path"] = "dags/templates"
        suggestions["dags_folder"] = "dags"
    else:
        # Try to get dags folder from Airflow
        try:
            suggestions["dags_folder"] = str(get_airflow_dags_folder())
        except Exception:
            suggestions["dags_folder"] = "dags"

    suggestions["output_dir"] = f"{suggestions.get('dags_folder', 'dags')}/configs"
    return suggestions


def create_dag_loader(path: str, template_path: str, output_dir: str, force: bool):
    """Create the DAG loader file."""
    loader_content = f'''"""
Auto-generated Blueprint DAG loader.
This file automatically loads all Blueprint YAML configurations as Airflow DAGs.
Generated by: blueprint init
"""

from pathlib import Path
from blueprint import auto_load_yaml_dags

# Auto-discover and load all Blueprint DAGs
auto_load_yaml_dags(
    configs_dir="{output_dir}",
    template_dir="{template_path}"
)

# Alternatively, you can manually load specific patterns:
# from blueprint import discover_yaml_dags
#
# globals().update(
#     discover_yaml_dags(
#         configs_dir="{output_dir}",
#         template_dir="{template_path}",
#         pattern="*.dag.yaml"
#     )
# )
'''

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

    example_content = '''"""Example ETL Blueprint template."""

from datetime import datetime, timedelta
from typing import List

from airflow.decorators import dag, task
from pydantic import BaseModel, Field


class ExampleETLConfig(BaseModel):
    """Configuration for the Example ETL blueprint."""
    source_table: str = Field(description="Source table to read from")
    destination_table: str = Field(description="Destination table to write to")

    # Optional parameters with defaults
    batch_size: int = Field(default=1000, description="Number of records to process at once")

    # DAG configuration
    dag_id: str = Field(default="example_etl", description="Unique identifier for the DAG")
    schedule: str = Field(default="@daily", description="DAG schedule interval")
    start_date: datetime = Field(
        default_factory=lambda: datetime(2024, 1, 1),
        description="DAG start date"
    )
    catchup: bool = Field(default=False, description="Whether to backfill missed runs")
    tags: List[str] = Field(
        default=["example", "etl", "blueprint"],
        description="DAG tags for organization"
    )


def create_dag(config: ExampleETLConfig):
    """Create an ETL DAG from configuration."""
    @dag(
        dag_id=config.dag_id,
        description=f"ETL from {config.source_table} to {config.destination_table}",
        schedule=config.schedule,
        start_date=config.start_date,
        catchup=config.catchup,
        tags=config.tags,
        default_args={
            "retries": 2,
            "retry_delay": timedelta(minutes=5),
        },
    )
    def etl_dag():
        @task
        def extract():
            """Extract data from source."""
            print(f"Extracting from {config.source_table}")
            print(f"Using batch size: {config.batch_size}")
            # Your extraction logic here
            return {"records": 100}

        @task
        def transform(data: dict):
            """Transform the data."""
            print(f"Transforming {data['records']} records")
            # Your transformation logic here
            return {"transformed_records": data["records"]}

        @task
        def load(data: dict):
            """Load data to destination."""
            print(f"Loading {data['transformed_records']} records to {config.destination_table}")
            # Your loading logic here

        # Define task dependencies
        data = extract()
        transformed = transform(data)
        load(transformed)

    return etl_dag()
'''

    example_path.parent.mkdir(parents=True, exist_ok=True)
    example_path.write_text(example_content)
    console.print(f"  ✅ Created example blueprint: {example_path}")


def update_gitignore():
    """Add blueprint.toml to .gitignore if not already there."""
    gitignore_path = Path(".gitignore")

    entries_to_add = []

    if gitignore_path.exists():
        content = gitignore_path.read_text()
        if "blueprint.toml" not in content:
            entries_to_add.append("blueprint.toml")
    else:
        entries_to_add.append("blueprint.toml")

    if entries_to_add:
        with gitignore_path.open("a") as f:
            f.write("\n# Blueprint local config\n")
            for entry in entries_to_add:
                f.write(f"{entry}\n")
        console.print("  ✅ Updated .gitignore")


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
        "  Template directory", default=detected_env.get("template_path", ".astro/templates")
    )
    output_dir = click.prompt(
        "  Output directory for configs", default=detected_env.get("output_dir", "dags/configs")
    )

    # 3. DAG loader setup
    console.print("\n🔧 [bold]DAG Loader Setup[/bold]")
    setup_loader = click.confirm("  Create DAG loader file?", default=True)

    loader_path = None
    if setup_loader:
        default_loader_path = f"{detected_env.get('dags_folder', 'dags')}/load_blueprints.py"
        loader_path = click.prompt("  DAG loader location", default=default_loader_path)

    # 4. Example blueprint
    console.print("\n📝 [bold]Example Blueprint[/bold]")
    create_example = click.confirm("  Create example blueprint?", default=True)

    # 5. Create files
    console.print("\n[bold]Creating files...[/bold]")

    # Create blueprint.toml
    config_content = f"""# Blueprint configuration
template_path = "{template_path}"
output_dir = "{output_dir}"
"""

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

    # Update .gitignore
    update_gitignore()

    # 6. Next steps
    console.print("\n[green]✨ Blueprint initialized![/green]")
    console.print("\n[bold]Next steps:[/bold]")
    console.print("  1. Run [cyan]blueprint list[/cyan] to see available blueprints")
    if create_example:
        console.print("  2. Run [cyan]blueprint new[/cyan] to create a DAG from the example")
    console.print("  3. Edit [cyan]blueprint.toml[/cyan] to adjust settings")
    if setup_loader:
        console.print(f"  4. Ensure [cyan]{loader_path}[/cyan] is in your DAGs folder")


def main():
    """Main entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
