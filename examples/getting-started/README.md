# Getting Started

The smallest complete Blueprint project: two blueprints, one YAML DAG, a one-line loader.

## Why you'd do this

Blueprint splits a DAG in two. A platform or data engineering team writes the blueprints —
Python classes that know how to build tasks correctly, with a validated config describing what
callers are allowed to change. Everyone else composes DAGs in YAML by naming a blueprint and
filling in its config.

YAML authors do not write operators or choose retry counts, and cannot pass a value the config
model disallows. Blueprint authors change an implementation in one place and every DAG using it
picks up the change on the next parse.

## Files

| File | What it does |
|---|---|
| `dags/blueprints.py` | Defines the `extract` and `load` blueprints and their config models |
| `dags/pipeline.dag.yaml` | Composes those two blueprints into a DAG |
| `dags/loader.py` | Turns every `*.dag.yaml` in this folder into an Airflow DAG |
| `Dockerfile`, `requirements.txt`, `packages.txt` | Standard Astro project files — `airflow-blueprint` is an ordinary dependency |

This is a real Astro project, laid out the way `astro dev init` produces and the way you would
commit it in your own repository. Adopting Blueprint adds one line to `requirements.txt`; the
rest is the `dags/` folder.

## Run it

```bash
../run.sh getting-started
```

Airflow UI: http://localhost:8080

Or without Docker:

```bash
blueprint list    # run from this directory
blueprint lint
```

## Walk-through

### 1. A blueprint is a config model plus a `render()`

```python
class ExtractConfig(BaseModel):
    source: str = Field(description="Name of the source system to read from")
    batch_size: int = Field(default=1000, ge=1, description="Rows to read per batch")


class Extract(Blueprint[ExtractConfig]):
    """Pull data from a source system."""

    def render(self, config: ExtractConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Extracting {config.source} in batches of {config.batch_size}'",
        )
```

`ExtractConfig` is the contract with YAML authors: `source` is required, `batch_size` is
optional and must be at least 1. Anything else they write is not part of the interface.

By the time `render()` runs, `config` is already validated — there is no need to check types or
fill in defaults. `self.step_id` is the name the YAML author gave this step, and using it as the
`task_id` is what keeps two steps from the same blueprint from colliding.

The class docstring is what `blueprint list` and `blueprint describe` show, so it is the
description someone reads when choosing a blueprint.

### 2. The class name becomes the YAML name

`Extract` is referenced in YAML as `extract`, `Load` as `load`. The rule is snake_case of the
class name, so `MultiSourceETL` would be `multi_source_etl`. See
[versioning](../versioning/) for how a `V2` suffix fits in.

### 3. YAML names blueprints and fills in configs

```yaml
dag_id: getting_started_pipeline
schedule: "@daily"

steps:
  extract_customers:
    blueprint: extract
    source: customers
    batch_size: 500

  load_customers:
    blueprint: load
    depends_on: [extract_customers]
    target: warehouse.customers
```

Each key under `steps` is a step name, which becomes the task ID. Inside a step, `blueprint`,
`depends_on`, `version` and `trigger_rule` are reserved — everything else is handed to the
blueprint's config model. So `source` and `batch_size` go to `ExtractConfig`, and a typo like
`batchsize` would be silently ignored unless the config opts into strictness (see
[config-validation](../config-validation/)).

### 4. The loader discovers everything

```python
from blueprint import build_all_airflow_dags

build_all_airflow_dags()
```

This finds every `*.dag.yaml` beside it, builds each into a DAG, and registers them so Airflow
picks them up. Adding a new DAG means adding a YAML file — the loader never changes.

The function name contains both `airflow` and `dag` on purpose: Airflow's safe-mode file
scanner only parses files containing both substrings, so this one-liner is enough for Airflow
to find the loader.

## What to look at in the UI

Open the `getting_started_pipeline` DAG. Two tasks, `extract_customers` and `load_customers`,
wired in sequence.

Click `extract_customers` and open the **Rendered Template** tab. Alongside `bash_command`,
Blueprint adds two fields to every task it builds:

- **`blueprint_step_config`** — the resolved config this task was built from
- **`blueprint_step_code`** — the source of the blueprint class that built it

So when a task looks wrong, the config that produced it and the code that consumed that config
are both one click away. You get this on every task in every example; it is not something you
configure.

## Related

- [tasks-and-taskgroups](../tasks-and-taskgroups/) — returning a TaskGroup instead of one task
- [step-dependencies](../step-dependencies/) — fan-out, fan-in, and trigger rules
- [config-validation](../config-validation/) — making the config contract strict
