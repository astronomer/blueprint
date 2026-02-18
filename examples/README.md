# Blueprint Examples

This directory contains working examples of Blueprint composing Airflow DAGs from reusable task group templates via YAML. Both Airflow 2 and Airflow 3 are supported with separate infrastructure configs sharing the same DAGs.

## Quick Start

### Airflow 3

```bash
cd examples/airflow3
tilt up
```

### Airflow 2

```bash
cd examples/airflow2
tilt up
```

### Without Tilt

```bash
docker compose -f examples/airflow3/docker-compose.yaml up --build
# or
docker compose -f examples/airflow2/docker-compose.yaml up --build
```

### Access Airflow UI

- URL: http://localhost:8080
- Airflow 3: no login required (SimpleAuthManager with all admins)
- Airflow 2: username `admin`, password `admin`

### View the example DAGs

- `customer_etl` -- Demonstrates versioning (v1 and v2 extract), dependencies, and both single-task and multi-task blueprints
- `simple_pipeline` -- Minimal DAG inheriting defaults from `build_all()`

## Directory Structure

```
examples/
├── airflow2/
│   ├── Dockerfile                # Astro Runtime 13 (Airflow 2.x)
│   ├── docker-compose.yaml       # webserver, scheduler, triggerer
│   └── Tiltfile                  # Tilt config for hot-reload dev
├── airflow3/
│   ├── Dockerfile                # Astro Runtime 3 (Airflow 3.x)
│   ├── docker-compose.yaml       # api-server, scheduler, dag-processor, triggerer
│   └── Tiltfile                  # Tilt config for hot-reload dev
├── dags/                          # Shared DAGs (works on both AF2 and AF3)
│   ├── etl_blueprints.py         # Blueprint class definitions
│   ├── customer_etl.dag.yaml     # DAG composed from blueprint steps
│   ├── simple_pipeline.dag.yaml  # Minimal DAG example
│   └── loader.py                 # DAG loader that calls build_all()
├── requirements.txt               # Python dependencies
└── .env                           # Environment overrides
```

## Airflow 2 vs Airflow 3

The DAG definitions and blueprint classes are identical -- only the infrastructure differs:

| | Airflow 2 | Airflow 3 |
|---|---|---|
| Base image | `quay.io/astronomer/astro-runtime:13.4.0-base` | `astrocrpublic.azurecr.io/runtime:3.1-12-base` |
| UI server | `webserver` | `api-server` |
| DAG parsing | Built into scheduler | Standalone `dag-processor` |
| DB init | `airflow db upgrade` | `airflow db migrate` |
| Auth | RBAC with default admin user | SimpleAuthManager |

## Example Blueprints

### Extract (v1) -- Task group template for a single source table

```python
class ExtractConfig(BaseModel):
    source_table: str = Field(description="Source table (schema.table)")
    batch_size: int = Field(default=1000, ge=1)

class Extract(Blueprint[ExtractConfig]):
    def render(self, config: ExtractConfig) -> TaskGroup:
        with TaskGroup(group_id=self.step_id) as group:
            BashOperator(task_id="validate", bash_command=f"echo 'Validating {config.source_table}'")
            BashOperator(task_id="extract", bash_command=f"echo 'Extracting {config.batch_size} rows'")
        return group
```

### ExtractV2 -- Breaking change: replaced single source_table with structured sources list

```python
class ExtractV2Config(BaseModel):
    sources: list[SourceDef] = Field(description="List of source definitions")
    parallel: bool = Field(default=True)

class ExtractV2(Blueprint[ExtractV2Config]):
    def render(self, config: ExtractV2Config) -> TaskGroup:
        with TaskGroup(group_id=self.step_id) as group:
            for src in config.sources:
                BashOperator(task_id=f"extract_{src.table}", bash_command=f"echo 'Extracting {src.schema_name}.{src.table}'")
        return group
```

### Load -- Simple single-task blueprint (returns BaseOperator directly)

```python
class LoadConfig(BaseModel):
    target_table: str
    mode: str = Field(default="append", pattern="^(append|overwrite)$")

class Load(Blueprint[LoadConfig]):
    def render(self, config: LoadConfig) -> BashOperator:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Loading to {config.target_table} ({config.mode})'"
        )
```

## Example YAML DAG

```yaml
# customer_etl.dag.yaml
dag_id: customer_etl
schedule: "@daily"
tags: [etl, customers]

steps:
  extract_legacy:
    blueprint: extract
    version: 1
    source_table: raw.customers
    batch_size: 500

  extract_multi:
    blueprint: extract
    sources:
      - schema_name: raw
        table: orders
      - schema_name: raw
        table: order_items
        filter: "status = 'active'"
    parallel: true

  transform:
    blueprint: transform
    depends_on: [extract_legacy, extract_multi]
    operations: [dedupe, normalize, enrich]

  load_warehouse:
    blueprint: load
    depends_on: [transform]
    target_table: analytics.customer_360
    mode: overwrite
```

## Creating Your Own Blueprint

1. Define a Pydantic config model with your parameters
2. Create a Blueprint class that implements `render()`
3. `render()` typically returns a `TaskGroup` (can also return a single `BaseOperator` for simple cases)
4. Use `self.step_id` for the group_id (or task_id for single-task blueprints)
5. Create a `.dag.yaml` file composing your blueprints as steps

```python
from blueprint import Blueprint, BaseModel, Field
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

class MyConfig(BaseModel):
    name: str
    count: int = Field(default=5, ge=1)

class MyBlueprint(Blueprint[MyConfig]):
    """Does something useful."""

    def render(self, config: MyConfig) -> TaskGroup:
        with TaskGroup(group_id=self.step_id) as group:
            for i in range(config.count):
                BashOperator(task_id=f"run_{i}", bash_command=f"echo '{config.name}: {i}'")
        return group
```

```yaml
# my_dag.dag.yaml
dag_id: my_dag
schedule: "@daily"
steps:
  my_step:
    blueprint: my_blueprint
    name: hello
    count: 10
```

## Development with Tilt

Each example directory has its own Tiltfile for hot-reload development:
- Changes to Blueprint source code automatically reload in the container
- Local changes sync instantly to the running Airflow instance
- Monitor all services in the Tilt UI at http://localhost:10350

```bash
cd examples/airflow3   # or examples/airflow2
tilt up                # Start
tilt down              # Stop
tilt logs              # View logs
```
