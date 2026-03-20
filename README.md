# Blueprint

Reusable task group templates composed into Airflow DAGs via YAML.

## What is Blueprint?

Blueprint lets data platform teams define reusable task group templates (Blueprints) in Python and compose them into Airflow DAGs using simple YAML files. Each Blueprint defines a validated Pydantic config and a `render()` method that produces a TaskGroup. DAGs are defined declaratively in YAML by composing blueprint instances as steps with explicit dependencies.

With Blueprint, you can:

- Define **reusable task group templates** with type-safe, validated configurations
- **Compose DAGs from YAML** by assembling blueprint instances as steps
- **Version blueprints** so DAGs can pin to specific template versions
- Get **clear error messages** when configs are invalid
- Use a **CLI** to list blueprints, validate YAML, and generate schemas
- See **step config and blueprint source code** in Airflow's rendered templates UI

## Quick Start

### 1. Define Blueprint templates

```python
# dags/etl_blueprints.py
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
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

Blueprints typically return a **TaskGroup** containing multiple tasks. For simple cases, `render()` can also return a single **BaseOperator** -- the framework handles both uniformly.

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

Step config is flat -- `blueprint:`, `depends_on:`, and `version:` are reserved keys; everything else is passed to the blueprint's config model. Steps with no `depends_on` run in parallel.

The `blueprint:` value is the snake_case form of the class name. `Extract` becomes `extract`, `MultiSourceETL` becomes `multi_source_etl`. See [Template Versioning](#template-versioning) for details on how names and versions are determined.

### 3. Load DAGs

```python
# dags/loader.py
from blueprint import build_all

build_all()
```

### 4. Validate

```bash
$ blueprint lint
PASS customer_pipeline.dag.yaml (dag_id=customer_pipeline)
```

## Try It Out

The [`examples/`](examples/) directory contains working Airflow environments (both Airflow 2 and 3) with Blueprint DAGs you can run locally using Docker and [Tilt](https://tilt.dev/):

```bash
cd examples/airflow3   # or examples/airflow2
tilt up
```

See the [examples README](examples/README.md) for full setup details.

## DAG Arguments

By default, DAG YAML files support `schedule` and `description` at the top level (alongside `dag_id` and `steps`). For more control over DAG construction, define a `BlueprintDagArgs` template.

A `BlueprintDagArgs` subclass works like a Blueprint but for DAG-level arguments. It defines a Pydantic config model whose fields become the valid top-level YAML fields, and a `render()` method that returns DAG constructor kwargs. At most one may exist per project.

```python
from datetime import timedelta
from blueprint import BlueprintDagArgs, BaseModel, Field

class ProjectDagArgsConfig(BaseModel):
    schedule: str | None = None
    owner: str = "data-team"
    retries: int = Field(default=2, ge=0)

class ProjectDagArgs(BlueprintDagArgs[ProjectDagArgsConfig]):
    """Project-wide DAG argument template."""

    def render(self, config: ProjectDagArgsConfig) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "default_args": {
                "owner": config.owner,
                "retries": config.retries,
            },
        }
        if config.schedule is not None:
            kwargs["schedule"] = config.schedule
        return kwargs
```

DAG YAML files then use the config fields directly:

```yaml
dag_id: customer_pipeline
schedule: "@daily"
owner: analytics-team
retries: 3

steps:
  extract:
    blueprint: extract
    source_table: raw.customers
```

When no `BlueprintDagArgs` is defined, the built-in `DefaultDagArgs` provides `schedule` and `description` pass-through.

## Template Versioning

### How names and versions are determined

By default, the blueprint name and version are inferred from the class name. The class name is converted to snake_case, and a trailing `V{N}` suffix is parsed as the version number:

| Class name         | Blueprint name       | Version |
|--------------------|----------------------|---------|
| `Extract`          | `extract`            | 1       |
| `ExtractV2`        | `extract`            | 2       |
| `MultiSourceETL`   | `multi_source_etl`   | 1       |
| `MultiSourceETLV3` | `multi_source_etl`   | 3       |

Classes without a `V{N}` suffix are version 1.

#### Explicit name and version

When the class name doesn't match the desired blueprint name, set `name` and/or `version` as class attributes. This is useful when you want descriptive class names that don't dictate the registry identity:

```python
class S3DataIngester(Blueprint[IngestConfig]):
    name = "ingest"
    version = 1

    def render(self, config: IngestConfig) -> TaskGroup: ...

class StreamingIngester(Blueprint[IngestV2Config]):
    name = "ingest"
    version = 2

    def render(self, config: IngestV2Config) -> TaskGroup: ...
```

Both register under `ingest` despite having unrelated class names. You can also set just one -- an explicit `name` with an inferred version from the class suffix, or an explicit `version` with a name inferred from the class name.

### Versioning workflow

Each blueprint version is a separate class with its own config model. The initial version uses a clean name. Later versions add a `V{N}` suffix (or use explicit attributes). Breaking config changes are fine -- each version has an independent schema.

```python
# v1 -- clean, no version thinking
class ExtractConfig(BaseModel):
    source_table: str
    batch_size: int = 1000

class Extract(Blueprint[ExtractConfig]):
    def render(self, config: ExtractConfig) -> TaskGroup:
        with TaskGroup(group_id=self.step_id) as group:
            BashOperator(task_id="validate", bash_command=f"echo 'Validating {config.source_table}'")
            BashOperator(task_id="extract", bash_command=f"echo 'Extracting {config.batch_size} rows'")
        return group

# v2 -- new class, new config, breaking changes are fine
class ExtractV2Config(BaseModel):
    sources: list[SourceDef]
    parallel: bool = True

class ExtractV2(Blueprint[ExtractV2Config]):
    def render(self, config: ExtractV2Config) -> TaskGroup:
        with TaskGroup(group_id=self.step_id) as group:
            for src in config.sources:
                BashOperator(task_id=f"extract_{src.table}", bash_command=f"echo 'Extracting {src.schema_name}.{src.table}'")
        return group
```

In YAML, pin to a version or omit to get the latest:

```yaml
steps:
  # Pinned to v1
  extract_legacy:
    blueprint: extract
    version: 1
    source_table: raw.customers

  # Latest (v2)
  extract_new:
    blueprint: extract
    sources:
      - schema_name: raw
        table: orders
```

## Airflow Rendered Templates

Every task instance gets two extra fields visible in Airflow's "Rendered Template" tab:

- **blueprint_step_config** -- the resolved YAML config for the step
- **blueprint_step_code** -- the full Python source file of the blueprint class

This makes it easy to understand what generated each task instance without leaving the Airflow UI.

## Jinja2 Templating in YAML

YAML files support Jinja2 templates with Airflow context:

```yaml
dag_id: "{{ env.get('ENV', 'dev') }}_customer_etl"
schedule: "{{ var.value.etl_schedule | default('@daily') }}"

steps:
  extract:
    blueprint: extract
    source_table: "{{ var.value.source_schema }}.customers"
```

## Programmatic Building

For advanced use cases, build DAGs programmatically:

```python
from blueprint import Builder, DAGConfig

config = DAGConfig(
    dag_id="dynamic_pipeline",
    schedule="@hourly",
    steps={
        "step1": {"blueprint": "extract", "source_table": "raw.data"},
        "step2": {"blueprint": "load", "depends_on": ["step1"], "target_table": "out"},
    },
)

dag = Builder().build(config)
```

## Post-Processing DAGs

The `on_dag_built` callback lets you modify each DAG after it's built from YAML. It receives the DAG and the path to the source YAML file:

```python
# dags/loader.py
from pathlib import Path
from airflow import DAG
from blueprint import build_all

def post_process(dag: DAG, yaml_path: Path) -> None:
    dag.tags = [*(dag.tags or []), "managed-by-blueprint"]
    dag.access_control = {"data-team": {"can_read", "can_edit"}}

build_all(on_dag_built=post_process)
```

This is useful for applying cross-cutting concerns like access controls, tags, or custom metadata that shouldn't live in individual YAML files. The callback runs once per DAG, after all steps are wired up.

## Type Safety and Validation

Blueprint uses Pydantic for robust validation:

- **Type coercion** -- converts compatible types automatically
- **Field validation** -- min/max values, regex patterns, enums
- **Custom validators** -- add your own validation logic
- **Clear error messages** -- know exactly what went wrong

```python
class ETLConfig(BaseModel):
    retries: int = Field(ge=0, le=5)
    timeout_minutes: int = Field(gt=0, le=1440)

    @field_validator('schedule')
    def validate_schedule(cls, v):
        valid = ['@once', '@hourly', '@daily', '@weekly', '@monthly']
        if v not in valid:
            raise ValueError(f'Must be one of {valid}')
        return v
```

## Config Options for Template Authors

Pydantic offers model-level configuration that can make your Blueprint configs stricter or more flexible. Two options are particularly useful for YAML-based composition:

### Rejecting Unknown Fields

By default, Pydantic silently ignores fields it doesn't recognize. This means a typo in a YAML step (e.g. `batchsize` instead of `batch_size`) is silently dropped and the default is used. Set `extra="forbid"` to catch this:

```python
from pydantic import BaseModel, ConfigDict

class ExtractConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_table: str
    batch_size: int = 1000
```

With this, `batchsize: 500` in YAML raises a clear validation error instead of being silently ignored. This is recommended for configs where typos could cause hard-to-debug issues.

### Internal Fields Not Settable from YAML

Template authors may want fields that exist on the config for use in `render()` but that cannot be overridden from YAML. `Field(init=False)` excludes a field from the constructor, so it always uses its default:

```python
from pydantic import BaseModel, Field

class ExtractConfig(BaseModel):
    source_table: str
    _internal_batch_multiplier: int = Field(default=4, init=False)
```

`init=False` fields are excluded from the JSON Schema output since YAML authors cannot set them. This is useful for internal tuning parameters that should not be exposed as part of the public config interface.

## Composing Templates

A blueprint can use other blueprints inside its `render()` method. This lets you build higher-level templates from lower-level building blocks while exposing a single, simplified config to YAML authors.

```python
# dags/quality_blueprints.py
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from blueprint import Blueprint, BaseModel, Field


class ValidateConfig(BaseModel):
    checks: list[str] = Field(description="List of checks to run")

class Validate(Blueprint[ValidateConfig]):
    """Run data quality checks."""

    def render(self, config: ValidateConfig) -> TaskGroup:
        with TaskGroup(group_id=self.step_id) as group:
            for check in config.checks:
                BashOperator(task_id=check, bash_command=f"echo 'Running {check}'")
        return group


class ReportConfig(BaseModel):
    channel: str = Field(description="Notification channel")

class Report(Blueprint[ReportConfig]):
    """Send a quality report."""

    def render(self, config: ReportConfig) -> BashOperator:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Sending report to {config.channel}'"
        )


class QualityGateConfig(BaseModel):
    checks: list[str] = Field(default=["nulls", "duplicates"])
    report_channel: str = Field(default="data-alerts")

class QualityGate(Blueprint[QualityGateConfig]):
    """Run checks then send a report -- composed from Validate and Report."""

    def render(self, config: QualityGateConfig) -> TaskGroup:
        with TaskGroup(group_id=self.step_id) as group:
            validate = Validate()
            validate.step_id = "validate"
            validate_group = validate.render(ValidateConfig(checks=config.checks))

            report = Report()
            report.step_id = "report"
            report_task = report.render(ReportConfig(channel=config.report_channel))

            validate_group >> report_task
        return group
```

YAML authors see a single step with a flat config:

```yaml
steps:
  quality:
    blueprint: quality_gate
    checks: [nulls, duplicates, freshness]
    report_channel: "#data-alerts"
```

## Installation

```bash
uv add airflow-blueprint
```

## CLI Commands

```bash
# List available blueprints
blueprint list

# Describe a blueprint's config schema
blueprint describe extract

# Describe a specific version
blueprint describe extract -v 1

# Validate DAG definitions
blueprint lint pipeline.dag.yaml

# Generate JSON schema for editor support
blueprint schema extract > extract.schema.json

# Create new DAG interactively
blueprint new
```

## How is this different from DAG Factory?

[DAG Factory](https://github.com/astronomer/dag-factory) exposes Airflow's full API via YAML. Blueprint hides that complexity behind safe, reusable task group templates with validation.

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

**Use DAG Factory if:** you need full Airflow flexibility and your users understand Airflow concepts.

**Use Blueprint if:** you want standardized, validated task group templates with type safety for teams.

## Contributing

We welcome contributions! Please see our [Contributing Guide](docs/CONTRIBUTING.md) for details.

## License

Apache 2.0
