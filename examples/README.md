# Blueprint Examples

Each example demonstrates one idea, in as little code as that idea needs. They are independent
— start anywhere, read in any order, copy whichever one resembles your problem.

Every example has its own README explaining what it shows, why you would do it, and what to
look for in the Airflow UI.

## Start here

| Example | What it shows | |
|---|---|---|
| [getting-started](getting-started/) | The smallest complete project: two blueprints, one YAML DAG, a one-line loader. How a config model becomes the contract with DAG authors. | Docker |
| [tasks-and-taskgroups](tasks-and-taskgroups/) | What `render()` may return — a single operator, a TaskGroup, or a group whose shape is computed from the config. Why `self.step_id` matters. | Docker |
| [step-dependencies](step-dependencies/) | Wiring steps in YAML: parallelism, fan-in, fan-out, and `trigger_rule` for cleanup that must run after a failure. | Docker |

## Authoring templates

| Example | What it shows | |
|---|---|---|
| [config-validation](config-validation/) | Making bad YAML impossible: `extra="forbid"`, field constraints, nested models, custom validators, and fields YAML must not set. Includes four deliberately broken files and the exact errors they produce. | CLI only |
| [composing-blueprints](composing-blueprints/) | Building a high-level blueprint from lower-level ones, and deciding what the composite should *not* expose. | Docker |
| [versioning](versioning/) | Shipping a breaking config change without breaking existing DAGs. Version-by-class-suffix, explicit `name`/`version`, pinning, and the migration workflow. | Docker |
| [runtime-params](runtime-params/) | Overriding a step's config at trigger time. `self.param()` vs `self.resolve_config()`, shaping the trigger form, and which validation it does not enforce. | Docker |

## Platform standards

| Example | What it shows | |
|---|---|---|
| [platform-defaults](platform-defaults/) | Enforcing DAG-level standards: `BlueprintDagArgs` for the fields authors may set, `on_dag_built` for the ones they never see. | Docker |
| [shared-blueprints-package](shared-blueprints-package/) | Publishing blueprints as an installable package so many repos share one set of templates. Entry points, scoping them, collisions, and cross-version imports. | Docker |
| [resilient-loading](resilient-loading/) | Stopping one bad YAML file from unloading every DAG beside it — `skip_invalid_dags` for accidents, `.airflowignore` for drafts. | Docker |

## Scaling out

| Example | What it shows | |
|---|---|---|
| [templating](templating/) | Jinja2 in YAML, and the two evaluation times that cause most confusion: parse time (`env`, `var`, loader context) versus run time (`context.*`). | Docker |
| [dags-from-data](dags-from-data/) | Generating one DAG per tenant from a data file with the `Builder` API, instead of a YAML file each. | Docker |
| [python-dag-interop](python-dag-interop/) | Using blueprints inside hand-written Python DAGs, for incremental adoption in an existing codebase. | Docker |

## Tooling

| Example | What it shows | |
|---|---|---|
| [editor-and-ci](editor-and-ci/) | Autocomplete in the editor via generated JSON Schemas, plus `blueprint lint` as a pre-commit hook and a CI job. | CLI only |
| [testing-blueprints](testing-blueprints/) | Unit-testing configs and rendered structure, and an integrity test that every DAG in the repo still builds. | CLI only |

**CLI only** examples need nothing but the `blueprint` command (and `pytest` for one of them).
The rest start a local Airflow.

## Running an example

```bash
./run.sh <example>        # Airflow 3 (default)
./run.sh <example> 2      # Airflow 2
```

```bash
./run.sh runtime-params
```

Airflow UI: http://localhost:8080 — Airflow 3 needs no login; Airflow 2 uses `admin`/`admin`.

With [Tilt](https://tilt.dev/), which live-syncs the `blueprint` source into the running
containers:

```bash
cd _runtime/airflow3        # or airflow2
tilt up -- --example=runtime-params
```

Every example also works without Docker for the CLI parts. From an example directory:

```bash
blueprint list          # blueprints this example defines
blueprint describe <name>
blueprint lint          # validate its DAG YAML
```

> `blueprint lint` **fails on purpose** in `resilient-loading`, which ships a deliberately
> broken DAG file, and `config-validation` keeps its broken files in `invalid/` where
> directory-wide lint will not find them.

## How the examples are laid out

Each example is a **real Astro project** — the same layout `astro dev init` produces, and the
same layout you would commit in your own DAG repository:

```
examples/<example>/
├── Dockerfile          # FROM astrocrpublic.azurecr.io/runtime:3.3
├── requirements.txt    # airflow-blueprint
├── packages.txt        # OS packages (empty in most examples)
├── README.md
├── dags/               # blueprints.py, loader.py, *.dag.yaml
└── package/            # only in shared-blueprints-package
```

The example directory is the Docker build context, so the image is built exactly as it would be
in a standalone project — Astro Runtime installs `packages.txt` and `requirements.txt` and
copies the project in.

What is *not* realistic, and is confined to two places:

- The `Dockerfile` takes a `BASE_IMAGE` build argument so one file can run on both Airflow 2
  and 3. Your own project would write the `FROM` line directly.
- Below a marked comment, the `Dockerfile` installs a wheel from `.wheels/` if one is present.
  `run.sh` builds that from this repository's working tree so the examples exercise local
  changes rather than the released package. Delete `.wheels/` and the example runs against the
  real `airflow-blueprint` from PyPI.

Only the orchestration around the projects is shared:

```
examples/
├── run.sh          # launcher: builds the dev wheel, then docker compose
├── check.sh        # validates every example; run by CI
└── _runtime/
    ├── airflow2/   # docker-compose.yaml + Tiltfile
    └── airflow3/
```

Those compose files exist so one command can run any example on either Airflow version. In a
real project you would use `astro dev start` instead and have no `_runtime/` at all.

## Airflow 2 and 3

Every example runs on both. The blueprints and YAML are identical; only the import paths
differ, which each `blueprints.py` handles with a try/except at the top:

```python
try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.sdk import TaskGroup
except ImportError:  # Airflow 2
    from airflow.operators.bash import BashOperator
    from airflow.utils.task_group import TaskGroup
```

The legacy paths still resolve on Airflow 3 but are deprecated, so the shim is what a real
cross-version project needs rather than example decoration.

| | Airflow 2 | Airflow 3 |
|---|---|---|
| Base image | `astro-runtime:13.4.0` | `runtime:3.3` (the default) |
| UI server | `webserver` | `api-server` |
| DAG parsing | Built into the scheduler | Standalone `dag-processor` |
| DB init | `airflow db init` | `airflow db migrate` |
| Auth | RBAC, `admin`/`admin` | SimpleAuthManager, no login |

## A note when working in this repository

Blueprints from installed packages are discovered globally, which the Docker runtimes isolate
per example but a shared dev environment does not. Two consequences when running the CLI
directly from a clone:

- `blueprint list` shows an extra `entry_point_bp_test`, a test fixture installed by
  `uv sync`.
- After `examples/check.sh` has run, `acme-blueprints` is installed, and its `publish`
  blueprint collides with the local one in `tasks-and-taskgroups` — `blueprint list` there
  reports a `DuplicateBlueprintError`.

Neither affects the Docker runtimes. Pass `--no-entry-points` to look at an example in
isolation, which is what `check.sh` does:

```bash
blueprint list --no-entry-points
blueprint lint --no-entry-points
```
