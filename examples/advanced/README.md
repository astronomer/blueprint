# Advanced Example

Space-themed example demonstrating many Blueprint features across two YAML DAGs, a set of programmatically-built DAGs, and five blueprints.

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
| **satellite_telemetry** | Per-profile DAG args (`schedule`), variable composition, Jinja2 `env` access, version pinning, context proxy (`{{ context.ds_nodash }}`), custom DAG args |
| **deep_space_survey** | Project variable (`${agency}`), partial per-profile override, mixed v1/v2 usage, complex dependency graph, context proxy |

### Variables (`dags/blueprint.vars.yaml`)

Declares two profiles, `flight` and `sim`, and the variables every DAG shares.
`ground_station` differs per profile; `archive_prefix` and `agency` are the
same everywhere.

`satellite_telemetry` adds DAG-local variables (a per-profile `schedule`, and
`archive_path` composed from `${archive_prefix}`). `deep_space_survey` overrides
only the `sim` value of `ground_station`, inheriting `flight` from the project file.

Try both resolutions without running Airflow:

```bash
blueprint lint
blueprint vars dags/satellite_telemetry.dag.yaml --profile flight
```

### Loader (`dags/loader.py`)

`build_all_airflow_dags()` with an `on_dag_built` callback and a `profile`. The
only logic in Python is choosing *which* profile is active -- every value lives
in YAML, so `blueprint lint` resolves exactly what the DAG processor does.

### Ignoring DAG YAML (`dags/.airflowignore`)

`build_all_airflow_dags()` honors `.airflowignore` with the same syntax and
semantics as Airflow's DAG processor. Here `dags/.airflowignore` lists the
`drafts` directory, so `drafts/lunar_relay.dag.yaml` never builds.

### Programmatic Building (`dags/programmatic_dags.py`)

Builds DAGs in a loop with the `Builder` API instead of YAML. One telemetry DAG
is generated per satellite from a plain Python list (`telemetry_sat_001`,
`telemetry_sat_002`, `telemetry_sat_003`), reusing the same blueprints. Use this
when the set of DAGs is data-driven -- one per satellite, region, or tenant.
