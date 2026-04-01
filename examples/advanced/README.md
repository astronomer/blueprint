# Advanced Example

Space-themed example demonstrating many Blueprint features across two DAGs and five blueprints.

## Quick Start

```bash
cd examples/advanced/airflow3   # or airflow2
tilt up
```

Or without Tilt:

```bash
docker compose -f examples/advanced/airflow3/docker-compose.yaml up --build
```

Airflow UI: http://localhost:8080
- Airflow 3: no login required
- Airflow 2: username `admin`, password `admin`

## What's Demonstrated

### Blueprints (`dags/blueprints.py`)

| Blueprint | Features |
|---|---|
| **Scan** (v1) | Single-operator return, default version |
| **ScanV2** (v2) | Versioning via class name suffix, nested `BaseModel` config (`FrequencyBand`), `Field(ge=, le=)` constraints, TaskGroup return |
| **Transmit** | `supports_params`, `self.param()` for template fields, `self.resolve_config()` in `@task`, `Literal` type |
| **Analyze** | `field_validator`, `ConfigDict(extra="forbid")`, sequential task chaining in TaskGroup |
| **Orbit** | Explicit `name`/`version` attributes, `model_validator`, `Field(pattern=)`, nested config |

### DAG Arguments (`dags/dag_args.py`)

Custom `BlueprintDagArgs` subclass that converts a `priority` field into a DAG tag.

### DAG Definitions

| DAG | Features |
|---|---|
| **satellite_telemetry** | Jinja2 `env` access, `var` access, version pinning, context proxy (`{{ context.ds_nodash }}`), custom DAG args |
| **deep_space_survey** | Custom `template_context` variable (`{{ agency }}`), mixed v1/v2 usage, complex dependency graph, context proxy |

### Loader (`dags/loader.py`)

`build_all()` with `on_dag_built` callback and `template_context`.
