# Blueprint

Reusable task group templates composed into Airflow DAGs via YAML.

## What is Blueprint?

Blueprint lets data platform teams define reusable task group templates (Blueprints) in Python
and compose them into Airflow DAGs using simple YAML files. Each Blueprint defines a validated
Pydantic config and a `render()` method that produces a TaskGroup. DAGs are defined
declaratively in YAML by composing blueprint instances as steps with explicit dependencies.

This splits responsibilities: a platform team owns the Python that builds tasks, and other teams
compose DAGs in YAML without writing operators or being able to pass a value the config model
disallows.

With Blueprint, you can:

- Define **reusable task group templates** with type-safe, validated configurations
- **Compose DAGs from YAML** by assembling blueprint instances as steps
- **Version blueprints** so DAGs can pin to specific template versions
- Keep **environment differences in YAML** with `${...}` variables and per-environment profiles
- Get **clear error messages** when configs are invalid
- Use a **CLI** to list blueprints, validate YAML, and generate schemas
- See **step config and blueprint source code** in Airflow's rendered templates UI

## Installation

```bash
uv add airflow-blueprint
```

Supports Python 3.10+ and Apache Airflow 2.5.0+.

## Quick Start

### 1. Define Blueprint templates

```python
# dags/blueprints.py
from airflow.providers.standard.operators.bash import BashOperator  # Airflow 3
from airflow.sdk import TaskGroup
from blueprint import Blueprint, BaseModel, Field

class ExtractConfig(BaseModel):
    source_table: str = Field(description="Source table (schema.table)")
    batch_size: int = Field(default=1000, ge=1)

class Extract(Blueprint[ExtractConfig]):
    """Extract data from a source table."""

    def render(self, config: ExtractConfig) -> TaskGroup:
        with TaskGroup(group_id=self.step_id) as group:
            BashOperator(task_id="validate", bash_command=f"echo 'Validating {config.source_table}'")
            BashOperator(task_id="extract", bash_command=f"echo 'Extracting {config.batch_size} rows'")
        return group

class LoadConfig(BaseModel):
    target_table: str
    mode: str = Field(default="append", pattern="^(append|overwrite)$")

class Load(Blueprint[LoadConfig]):
    """Load data to a target table."""

    def render(self, config: LoadConfig) -> BashOperator:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Loading to {config.target_table} ({config.mode})'"
        )
```

Blueprints typically return a **TaskGroup** containing multiple tasks. For simple cases,
`render()` can also return a single **BaseOperator** -- the framework handles both uniformly.

`self.step_id` is the step name from YAML; using it as the `task_id`/`group_id` is what keeps
two steps built from the same blueprint from colliding.

The imports above are the Airflow 3 paths. On Airflow 2 they are `airflow.operators.bash` and
`airflow.utils.task_group` — the blueprint itself is unchanged.

### 2. Compose a DAG in YAML

```yaml
# dags/customer_pipeline.dag.yaml
dag_id: customer_pipeline
schedule: "@daily"

steps:
  extract_customers:
    blueprint: extract
    source_table: raw.customers
    batch_size: 500

  extract_orders:
    blueprint: extract
    source_table: raw.orders

  load:
    blueprint: load
    depends_on: [extract_customers, extract_orders]
    target_table: analytics.customer_orders
    mode: overwrite
```

Step config is flat. `blueprint`, `depends_on`, `version` and `trigger_rule` are reserved keys;
everything else is passed to the blueprint's config model. Steps with no `depends_on` run in
parallel, and `trigger_rule` controls when a step runs relative to its upstream dependencies
(the valid values come from your installed Airflow version).

### 3. Load DAGs

```python
# dags/loader.py
from blueprint import build_all_airflow_dags

build_all_airflow_dags()
```

This discovers every `*.dag.yaml` beside it. The function name contains both `airflow` and
`dag` on purpose, so this one-liner satisfies Airflow's safe-mode file scanner.

### 4. Validate

```bash
$ blueprint lint
PASS customer_pipeline.dag.yaml (dag_id=customer_pipeline)
```

## Blueprint Names and Versions

The name a YAML file references, and the version it resolves to, are inferred from the class
name. A trailing `V{N}` is the version; no suffix means version 1.

| Class name         | Blueprint name       | Version |
|--------------------|----------------------|---------|
| `Extract`          | `extract`            | 1       |
| `ExtractV2`        | `extract`            | 2       |
| `MultiSourceETL`   | `multi_source_etl`   | 1       |
| `MultiSourceETLV3` | `multi_source_etl`   | 3       |

Set `name` and/or `version` as class attributes to override either:

```python
class S3DataIngester(Blueprint[IngestConfig]):
    name = "ingest"
    version = 1
```

Each version is a separate class with its own config model, so breaking changes are free. In
YAML, `version: 1` pins a step; omitting it resolves to the latest. See
[examples/versioning](examples/versioning/) for the full migration workflow.

## Rendered Templates in the Airflow UI

Every task instance gets two extra fields in Airflow's "Rendered Template" tab:

- **`blueprint_step_config`** -- the resolved YAML config for the step
- **`blueprint_step_code`** -- the full source of the blueprint class that built it

So when a task looks wrong, the config that produced it and the code that consumed that config
are both one click away. This is automatic; there is nothing to configure.

## Examples

The [`examples/`](examples/) directory contains small, independent examples, each covering one
idea and each a real Astro project you can run:

```bash
cd examples
./run.sh getting-started
```

| Example | What it covers |
|---|---|
| [getting-started](examples/getting-started/) | Two blueprints, one YAML DAG, a one-line loader |
| [tasks-and-taskgroups](examples/tasks-and-taskgroups/) | What `render()` may return, and config-driven task structure |
| [step-dependencies](examples/step-dependencies/) | `depends_on`, parallelism, fan-in/fan-out, `trigger_rule` |
| [config-validation](examples/config-validation/) | Strict configs, custom validators, and the errors authors see |
| [composing-blueprints](examples/composing-blueprints/) | Building a high-level blueprint from lower-level ones |
| [versioning](examples/versioning/) | Shipping a breaking config change without breaking DAGs |
| [runtime-params](examples/runtime-params/) | `supports_params`, `self.param()`, `self.resolve_config()`, trigger forms |
| [dag-arguments](examples/dag-arguments/) | `BlueprintDagArgs`, and one template per directory |
| [dag-post-processing](examples/dag-post-processing/) | The `on_dag_built` callback |
| [variables-and-profiles](examples/variables-and-profiles/) | `${...}` variables, per-environment profiles, `blueprint vars` |
| [shared-blueprints-package](examples/shared-blueprints-package/) | Publishing blueprints as an installable package |
| [resilient-loading](examples/resilient-loading/) | `skip_invalid_dags` and `.airflowignore` |
| [templating](examples/templating/) | Jinja2 in YAML: parse-time vs run-time evaluation |
| [dags-from-data](examples/dags-from-data/) | Generating DAGs with the `Builder` API instead of YAML |
| [python-dag-interop](examples/python-dag-interop/) | Blueprints inside hand-written Python DAGs |
| [editor-and-ci](examples/editor-and-ci/) | JSON Schema autocomplete, pre-commit and CI linting |
| [testing-blueprints](examples/testing-blueprints/) | Unit-testing configs, rendered structure and DAG integrity |

Several need nothing but the CLI. See the [examples README](examples/README.md) for the full
index and setup details.

## CLI Commands

```bash
# List available blueprints and DAG args templates
blueprint list

# Describe a blueprint's config schema
blueprint describe extract
blueprint describe extract -v 1

# Validate DAG definitions
blueprint lint                     # everything below the current directory
blueprint lint pipeline.dag.yaml   # one file
blueprint lint --profile prod      # one profile; without this, every declared one

# Show resolved variables and where each came from
blueprint vars pipeline.dag.yaml --profile prod --unused

# Generate JSON schema for editor support
# (each schema includes a top-level "templateType" field -- "blueprint" for a
# step template, or "dag_args" for DAG-level fields via `blueprint schema --dag-args`)
blueprint schema extract > extract.schema.json

# A project with several DAG args templates has one DAG schema per template
blueprint schema --dag-args sandbox_dag_args > sandbox.dag.schema.json

# Create a new DAG interactively
blueprint new
```

## How is this different from DAG Factory?

[DAG Factory](https://github.com/astronomer/dag-factory) exposes Airflow's full API via YAML.
Blueprint hides that complexity behind safe, reusable task group templates with validation.

### DAG Factory

```yaml
my_dag:
  default_args:
    owner: 'data-team'
  schedule_interval: '@daily'
  tasks:
    extract_data:
      operator: airflow.operators.python.PythonOperator
      python_callable_name: extract_from_api
      python_callable_file: /opt/airflow/dags/etl/extract.py
```

### Blueprint

```yaml
dag_id: customer_pipeline
schedule: "@daily"

steps:
  extract:
    blueprint: extract
    source_table: raw.customers
  load:
    blueprint: load
    depends_on: [extract]
    target_table: analytics.customers
```

**Use DAG Factory if:** you need full Airflow flexibility and your users understand Airflow
concepts.

**Use Blueprint if:** you want standardized, validated task group templates with type safety
for teams.

## Contributing

We welcome contributions! Please see our [Contributing Guide](docs/CONTRIBUTING.md) for details.

## License

Apache 2.0
