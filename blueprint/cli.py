"""Blueprint CLI for managing reusable task templates and validating DAG definitions."""

import copy
import json
import sys
from pathlib import Path
from typing import Any

import click
import yaml
from rich.console import Console
from rich.markup import escape
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from blueprint.core import BlueprintDagArgs, DefaultDagArgs
from blueprint.errors import DagArgsNotFoundError, MultipleDagArgsError
from blueprint.loaders import discover_yaml_files, get_blueprint_info, validate_yaml
from blueprint.registry import BlueprintRegistry
from blueprint.utils import display_path

console = Console()


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(None, "-v", "--version", package_name="airflow-blueprint")
def cli():
    """Blueprint - Reusable task templates composed into Airflow DAGs.

    Define reusable blueprint classes in Python, compose them into DAGs
    with YAML, and let Blueprint handle validation and wiring.
    """


def _get_configs_to_check(path: str | None) -> list[Path]:
    """Get list of configuration files to check.

    An explicit path is always checked, even if an ``.airflowignore`` entry
    matches it; directory-wide discovery skips ignored files.
    """
    if path:
        return [Path(path)]

    return discover_yaml_files(Path(), "*.dag.yaml")


def _validate_config(
    config_path: Path,
    bp_registry: BlueprintRegistry,
    profile: str | None = None,
    search_root: Path | None = None,
) -> tuple[bool, str | None]:
    """Validate a single configuration file.

    Args:
        config_path: Path to the .dag.yaml file to validate.
        bp_registry: An already-discovered registry to validate against.
        profile: Active variable profile.
        search_root: Outermost directory searched for vars files.

    Returns:
        tuple of (success, dag_id)
    """
    label = escape(f"{config_path}" + (f" [{profile}]" if profile else ""))
    try:
        result = validate_yaml(
            str(config_path),
            bp_registry=bp_registry,
            profile=profile,
            search_root=search_root,
        )
    except Exception as e:
        console.print(f"[red]FAIL[/red] {label}")
        if hasattr(e, "_format_message") and callable(e._format_message):
            console.print(e._format_message())
        else:
            console.print(f"  [red]Error:[/red] {e}")
        return False, None
    else:
        dag_id = result.get("dag_id")
        template = bp_registry.resolve_dag_args(config_path)
        applied = "" if template is DefaultDagArgs else f", dag_args={template.template_name()}"
        console.print(f"[green]PASS[/green] {label} (dag_id={dag_id}{applied})")
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
@click.option(
    "--entry-points/--no-entry-points",
    default=True,
    help="Discover blueprints from installed packages via entry points.",
)
@click.option(
    "--root",
    default=None,
    type=click.Path(exists=True, file_okay=False),
    help="Outermost directory searched for blueprint.vars.yaml. Defaults to the current "
    "directory; set it to the same path build_all_airflow_dags() uses.",
)
@click.option("--profile", default=None, help="Variable profile to resolve against.")
def lint(
    path: str | None,
    template_dir: str | None,
    entry_points: bool,
    root: str | None,
    profile: str | None,
):
    """Validate DAG YAML definitions.

    If PATH is provided, validate a specific file.
    Otherwise, validate all .dag.yaml files in the current directory tree,
    skipping files matched by .airflowignore entries.

    Top-level DAG fields are validated against the DAG args template defined
    closest above each file, the same one the builder uses.

    --root scopes the search for blueprint.vars.yaml files only; it does not
    change which .dag.yaml files are validated.
    """
    vars_root = Path(root) if root else Path.cwd()
    configs_to_check = _get_configs_to_check(path)

    if not configs_to_check:
        console.print("[yellow]No .dag.yaml files found.[/yellow]")
        return

    reg = _discover(template_dir, entry_points)

    errors_found = False
    dag_ids_to_files: dict[str, list[Path]] = {}
    valid_count = 0

    for config_path in configs_to_check:
        # With no profile named, validate against every profile the project
        # declares -- strictly more checking than picking one arbitrarily.
        if profile is not None:
            profiles_to_check: list[str | None] = [profile]
        else:
            from blueprint.vars import declared_profiles

            declared = declared_profiles(config_path, search_root=vars_root)
            profiles_to_check = list(declared) if declared else [None]

        results = [
            _validate_config(config_path, reg, profile=p, search_root=vars_root)
            for p in profiles_to_check
        ]
        success = all(ok for ok, _ in results)

        # Record every profile's dag_id: a collision may only exist under one
        # of them, e.g. when the id itself is built from a variable.
        seen_ids = {d for ok, d in results if ok and d}
        for dag_id in seen_ids:
            dag_ids_to_files.setdefault(dag_id, []).append(config_path)

        if success and seen_ids:
            valid_count += 1
        elif not success:
            errors_found = True

    if valid_count > 1 and _check_duplicate_dag_ids(dag_ids_to_files):
        errors_found = True

    if errors_found:
        sys.exit(1)


def _print_dag_args_table(templates: list[dict[str, Any]], base: Path) -> None:
    """Render discovered DAG args templates, flagging the project default."""
    table = Table(title="DAG Args Templates", show_lines=True)
    table.add_column("Name", style="cyan", no_wrap=True)
    table.add_column("Applies to", style="green", overflow="fold")
    table.add_column("Fallback", style="yellow", no_wrap=True)
    table.add_column("Description", overflow="fold")
    table.add_column("Location", style="dim", overflow="fold")

    for template in templates:
        desc = template["description"].split("\n")[0] if template["description"] else "-"
        directory = template["directory"]
        applies_to = f"{display_path(directory, base=base)}/" if directory else "installed package"
        if template["ambiguous"]:
            applies_to += " (ambiguous)"
        location = template["location"]
        fallback = "declared" if template["is_default"] else ""
        if template["is_fallback"] and not template["is_default"]:
            fallback = "only template"
        table.add_row(
            template["name"],
            applies_to,
            fallback,
            desc,
            display_path(location, base=base) if location else "-",
        )

    console.print(table)


def _vars_table(resolved, config_path: Path, declared: list[str]) -> Table:
    """Build the table of resolved variables shown by `blueprint vars`."""
    title = f"Variables for {config_path.name}"
    if resolved.profile:
        title += f" (profile: {resolved.profile})"
    elif declared:
        title += f" (no profile selected; declared: {', '.join(declared)})"

    table = Table(title=title, show_lines=False)
    table.add_column("Variable", style="cyan", no_wrap=True)
    table.add_column("Value", style="green", overflow="fold")
    table.add_column("Source", style="dim", overflow="fold")

    base = config_path.parent.resolve()
    for name in sorted(resolved.available):
        try:
            value = escape(repr(resolved.resolve_name(name)))
        except Exception as e:
            value = f"[red]{type(e).__name__}[/red]"
            if isinstance(resolved.available[name], dict):
                value = "[yellow]varies by profile[/yellow]"
        source = resolved.sources.get(name)
        source_str = display_path(str(source), base=base) if source else "-"
        table.add_row(name, value, source_str)

    return table


@cli.command("vars")
@click.argument("path", type=click.Path(exists=True))
@click.option(
    "--root",
    default=None,
    type=click.Path(exists=True, file_okay=False),
    help="Outermost directory searched for blueprint.vars.yaml. Defaults to the current "
    "directory; set it to the same path build_all_airflow_dags() uses.",
)
@click.option("--profile", default=None, help="Variable profile to resolve against.")
@click.option(
    "--unused",
    is_flag=True,
    default=False,
    help="Show variables this DAG does not reference.",
)
def show_vars(path: str, profile: str | None, unused: bool, root: str | None):
    """Show resolved variables for a DAG YAML file, and where each came from."""
    from blueprint import vars as bp_vars
    from blueprint.loaders import render_yaml_template

    config_path = Path(path)

    try:
        config, _ = render_yaml_template(config_path, use_airflow_context=False)
        vars_root = Path(root) if root else Path.cwd()
        declared = bp_vars.declared_profiles(config_path, search_root=vars_root)
        resolved = bp_vars.collect(config, config_path, profile=profile, search_root=vars_root)
        referenced_known = False
        if profile is not None or not declared:
            _remaining, full = bp_vars.resolve(
                config, config_path, profile=profile, search_root=vars_root
            )
            resolved, referenced_known = full, True
    except Exception as e:
        if hasattr(e, "_format_message") and callable(e._format_message):
            console.print(e._format_message())
        else:
            console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)

    console.print(_vars_table(resolved, config_path, declared))

    if unused and not referenced_known:
        console.print(
            "\n[yellow]Pass --profile to see which variables this DAG references.[/yellow]"
        )
    elif unused:
        never_used = bp_vars.unused_variables(resolved)
        if never_used:
            console.print(f"\n[yellow]Not referenced by this DAG:[/yellow] {', '.join(never_used)}")
        else:
            console.print("\n[green]Every variable in scope is referenced by this DAG.[/green]")


@cli.command("list")
@click.option("--template-dir", default=None, help="Directory containing blueprint files")
@click.option(
    "--entry-points/--no-entry-points",
    default=True,
    help="Discover blueprints from installed packages via entry points.",
)
def list_blueprints(template_dir: str | None, entry_points: bool):
    """List available blueprints and DAG args templates."""
    reg = _discover(template_dir, entry_points)
    blueprints = reg.list_blueprints()
    dag_args_templates = reg.list_dag_args()

    if not blueprints and not dag_args_templates:
        console.print("[yellow]No blueprints or DAG args templates found.[/yellow]")
        return

    base = Path(template_dir).resolve() if template_dir else Path.cwd()

    if dag_args_templates:
        _print_dag_args_table(dag_args_templates, base)

    if not blueprints:
        return

    table = Table(title="Available Blueprints", show_lines=True)
    table.add_column("Name", style="cyan", no_wrap=True)
    table.add_column("Versions", style="green", no_wrap=True)
    table.add_column("Description", overflow="fold")
    table.add_column("Class", style="dim", no_wrap=False)
    table.add_column("Location", style="dim", overflow="fold")

    for bp in blueprints:
        versions_str = ", ".join(str(v) for v in bp["versions"])
        desc = bp["description"].split("\n")[0] if bp["description"] else "-"
        location = bp["locations"].get(bp["latest_version"])
        location_str = display_path(location, base=base) if location else "-"
        table.add_row(bp["name"], versions_str, desc, bp["class"], location_str)

    console.print(table)


@cli.command()
@click.argument("blueprint_name")
@click.option("--version", "-v", type=int, default=None, help="Specific version (default: latest)")
@click.option("--template-dir", default=None, help="Directory containing blueprint files")
@click.option(
    "--entry-points/--no-entry-points",
    default=True,
    help="Discover blueprints from installed packages via entry points.",
)
def describe(
    blueprint_name: str, version: int | None, template_dir: str | None, entry_points: bool
):
    """Show blueprint parameters and documentation."""
    try:
        info = get_blueprint_info(
            blueprint_name, template_dir, version=version, discover_entry_points=entry_points
        )
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


def _dag_args_template_or_exit(
    bp_registry: BlueprintRegistry, name: str, template_dir: str | None
) -> type[BlueprintDagArgs]:
    """Look up a named DAG args template, or resolve the one applying in a directory."""
    if not name:
        return _resolve_dag_args_or_exit(
            bp_registry,
            Path(template_dir or "."),
            hint="Or name one with --dag-args NAME",
        )

    try:
        return bp_registry.get_dag_args(name)
    except DagArgsNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)


def _resolve_dag_args_or_exit(
    bp_registry: BlueprintRegistry, target_dir: Path, hint: str
) -> type[BlueprintDagArgs]:
    """Resolve the template that applies in a directory, or explain the choice and exit.

    Args:
        bp_registry: An already-discovered registry.
        target_dir: Directory to resolve the template for.
        hint: What the calling command offers for narrowing the choice.
    """
    try:
        return bp_registry.resolve_dag_args(target_dir)
    except MultipleDagArgsError as e:
        console.print(f"[red]Error:[/red] {e}\n  • {hint}")
        sys.exit(1)


def _discover(template_dir: str | None, entry_points: bool) -> BlueprintRegistry:
    """Discover blueprints and templates, reporting discovery failures instead of raising."""
    from blueprint.loaders import get_registry

    try:
        return get_registry(template_dir, discover_entry_points=entry_points)
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        sys.exit(1)


def _get_trigger_rule_values() -> list[str]:
    """Get valid trigger rule values from the installed Airflow version."""
    import contextlib
    import io
    import warnings

    with warnings.catch_warnings(), contextlib.redirect_stderr(io.StringIO()):
        warnings.simplefilter("ignore")
        from airflow.utils.trigger_rule import TriggerRule

    return sorted(str(r.value) for r in TriggerRule)


def _build_version_schema(
    blueprint_name: str,
    version: int,
    raw_schema: dict,
    trigger_rule_values: list[str],
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
    schema_data["properties"]["depends_on"] = {
        "type": "array",
        "items": {"type": "string"},
        "description": "Steps that must complete before this step runs",
        "default": [],
    }
    schema_data["properties"]["trigger_rule"] = {
        "type": "string",
        "enum": trigger_rule_values,
        "description": "Trigger rule for this step (default: all_success)",
    }

    if "required" not in schema_data:
        schema_data["required"] = []
    schema_data["required"].insert(0, "blueprint")
    if "version" not in schema_data["required"]:
        schema_data["required"].insert(1, "version")

    schema_data["title"] = blueprint_name
    schema_data["templateType"] = "blueprint"
    schema_data.pop("$schema", None)

    return schema_data


def _build_dag_yaml_schema(dag_args_schema: dict) -> dict:
    schema = copy.deepcopy(dag_args_schema)
    schema["$schema"] = "http://json-schema.org/draft-07/schema#"
    schema["title"] = "DAG"
    schema["templateType"] = "dag_args"
    props = schema.setdefault("properties", {})
    props["dag_id"] = {"type": "string", "description": "Unique DAG identifier"}
    props["steps"] = {
        "type": "object",
        "description": "Blueprint step definitions",
        "additionalProperties": {"type": "object"},
    }
    required = schema.setdefault("required", [])
    for field in ("dag_id", "steps"):
        if field not in required:
            required.insert(0, field)
    return schema


@cli.command()
@click.argument("blueprint_name", required=False, default=None)
@click.option(
    "--dag-args",
    "dag_args",
    is_flag=False,
    flag_value="",
    default=None,
    help="Emit schema for DAG-level YAML fields. Omit the value to resolve the template "
    "that applies in --template-dir.",
)
@click.option("--output", "-o", type=click.Path(), help="Output file (default: stdout)")
@click.option("--template-dir", default=None, help="Directory containing blueprint files")
@click.option(
    "--entry-points/--no-entry-points",
    default=True,
    help="Discover blueprints from installed packages via entry points.",
)
def schema(
    blueprint_name: str | None,
    dag_args: str | None,
    output: str | None,
    template_dir: str | None,
    entry_points: bool,
):
    """Generate JSON Schema for a blueprint's configuration.

    Emits a single schema covering all versions. Multi-version blueprints
    use oneOf discriminated by the version field.

    With --dag-args, emits the schema for DAG-level YAML (dag_id, steps, and
    any custom dag args fields). A project with several DAG args templates has
    one DAG schema per template: pass --dag-args NAME for a specific one, or
    leave it bare for the template covering the template directory itself.
    """
    if dag_args is not None and blueprint_name:
        console.print("[red]Error:[/red] Cannot use --dag-args with a blueprint name.")
        sys.exit(1)

    if dag_args is None and not blueprint_name:
        console.print("[red]Error:[/red] Provide a blueprint name or use --dag-args.")
        sys.exit(1)

    reg = _discover(template_dir, entry_points)

    if dag_args is not None:
        dag_args_cls = _dag_args_template_or_exit(reg, dag_args, template_dir)
        schema_data = _build_dag_yaml_schema(dag_args_cls.get_schema())
    else:
        assert blueprint_name is not None
        try:
            versions_info = reg.get_all_versions_info(blueprint_name)
        except Exception as e:
            console.print(f"[red]Error:[/red] {e}")
            sys.exit(1)

        trigger_rule_values = _get_trigger_rule_values()
        variants = [
            _build_version_schema(
                blueprint_name,
                vi["version"],
                vi["schema"],
                trigger_rule_values,
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
                "templateType": "blueprint",
                "oneOf": variants,
                "discriminator": {"propertyName": "version"},
            }

    json_output = json.dumps(schema_data, indent=2)

    if output:
        Path(output).write_text(json_output)
        console.print(f"[green]Schema written to {output}[/green]")
    elif sys.stdout.isatty():
        syntax = Syntax(json_output, "json", theme="monokai")
        console.print(syntax)
    else:
        click.echo(json_output)


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
@click.option(
    "--output-dir",
    default=".",
    help="Output directory for YAML config, which also selects the DAG args template "
    "that applies there.",
)
@click.option(
    "--entry-points/--no-entry-points",
    default=True,
    help="Discover blueprints from installed packages via entry points.",
)
def new(template_dir: str | None, output_dir: str, entry_points: bool):
    """Interactively create a new DAG YAML definition."""
    target_dir = Path(output_dir)

    reg = _discover(template_dir, entry_points)
    blueprints = reg.list_blueprints()

    if not blueprints:
        console.print("[red]No blueprints found.[/red]")
        sys.exit(1)

    dag_args_cls = _resolve_dag_args_or_exit(
        reg, target_dir, hint="Point --output-dir at the directory the DAG will live in"
    )

    selected = _select_blueprint(blueprints)
    console.print(f"\n[green]Selected:[/green] {selected['name']}")

    info = get_blueprint_info(selected["name"], template_dir, discover_entry_points=entry_points)

    dag_id = console.input("\nDAG ID: ")
    if not dag_id:
        console.print("[red]DAG ID is required[/red]")
        sys.exit(1)

    dag_args_schema = dag_args_cls.get_schema()
    dag_args_params = dag_args_schema.get("properties", {})

    dag_args_values: dict[str, object] = {}
    if dag_args_params:
        console.print("\n[bold]DAG arguments:[/bold]")
        dag_args_info = {
            "parameters": {
                name: {
                    "type": prop.get("type", "string"),
                    "description": prop.get("description", ""),
                    "default": prop.get("default"),
                    "required": name in dag_args_schema.get("required", []),
                }
                for name, prop in dag_args_params.items()
            }
        }
        dag_args_values = _collect_parameters(dag_args_info)

    step_name = console.input(f"\nStep name [default: {selected['name']}]: ") or selected["name"]

    step_config = _collect_parameters(info)

    dag_def: dict[str, object] = {
        "dag_id": dag_id,
        **{k: v for k, v in dag_args_values.items() if v},
        "steps": {
            step_name: {
                "blueprint": selected["name"],
                **step_config,
            },
        },
    }

    filename = dag_id.replace("-", "_") + ".dag.yaml"
    file_path = target_dir / filename

    if file_path.exists() and not click.confirm(f"{file_path} already exists. Overwrite?"):
        sys.exit(0)

    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("w") as f:
        yaml.dump(dag_def, f, default_flow_style=False, sort_keys=False)

    console.print(f"\n[green]Created {file_path}[/green]")
    console.print("\nTo load this DAG, add a loader.py to your dags/ directory:")
    console.print("  from blueprint import build_all_airflow_dags")
    console.print("  build_all_airflow_dags()")


def main():
    """Main entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
