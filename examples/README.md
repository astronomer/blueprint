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
| [dag-arguments](dag-arguments/) | What a DAG YAML may set at its top level: `BlueprintDagArgs` as the contract, and one template per directory when different areas need different standards. | Docker |
| [dag-post-processing](dag-post-processing/) | The `on_dag_built` callback, for settings authors never see — provenance, ownership derived from the file's location, generated docs. | Docker |
| [variables-and-profiles](variables-and-profiles/) | Declaring a value once as `${name}` and letting it differ per environment. Project and per-directory vars files, profiles, and `blueprint vars`. | Docker |
| [shared-blueprints-package](shared-blueprints-package/) | Publishing blueprints as an installable package so many repos share one set of templates. Entry points, scoping them, collisions, and cross-version imports. | Docker |
| [resilient-loading](resilient-loading/) | Stopping one bad YAML file from unloading every DAG beside it — `skip_invalid_dags` for accidents, `.airflowignore` for drafts. | Docker |

## Scaling out

| Example | What it shows | |
|---|---|---|
| [templating](templating/) | Jinja2 in YAML, and the two evaluation times that cause most confusion: parse time (`env`, `var`, loader context) versus run time (`context.*`). Also when to prefer `${...}` variables. | Docker |
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
./run.sh <example>
```

```bash
./run.sh runtime-params
```

Airflow UI: http://localhost:8080 — no login required.

Or with [Tilt](https://tilt.dev/):

```bash
cd _runtime
tilt up -- --example=runtime-params
```

Every example also works without Docker for the CLI parts. From an example directory:

```bash
blueprint list          # blueprints and DAG args templates this example defines
blueprint describe <name>
blueprint lint          # validate its DAG YAML, under every declared profile
blueprint vars <file>   # resolved variables and where each came from
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
├── dags/               # blueprints.py, loader.py, *.dag.yaml, dag_args.py,
│                       # blueprint.vars.yaml -- whichever the example needs
└── package/            # only in shared-blueprints-package
```

The example directory is the Docker build context, so the image is built exactly as it would be
in a standalone project — Astro Runtime installs `packages.txt` and `requirements.txt` and
copies the project in.

Nothing in an example's own files is specific to this repository. You can copy any of these
directories out, run `astro dev start`, and it works.

Only the orchestration around the projects is shared:

```
examples/
├── run.sh          # launcher
├── check.sh        # validates every example; run by CI
└── _runtime/       # docker-compose.yaml + Tiltfile
```

That compose file exists so one command can run any example. In a real project you would use
`astro dev start` and have no `_runtime/` at all.

## What the examples actually run against

An example's `requirements.txt` lists `airflow-blueprint`, so building its image installs the
**released** version from PyPI, exactly as a reader's project would.

That is not what the containers import. `_runtime/docker-compose.yaml` mounts this repository's
`blueprint/` at `/opt/blueprint-src` and sets `PYTHONPATH` so it takes precedence over the
installed package. `import blueprint` resolves to the working tree, and an edit to the library
takes effect on the next DAG parse with no rebuild.

Two consequences worth knowing:

- **An unreleased feature works in the examples immediately.** You do not need to publish a
  version to demonstrate a change. `examples/check.sh` gets the same result a different way: it
  runs in this repo's venv, where `airflow-blueprint` is an editable install of the working tree.
- **The mount supplies Python code, not dependencies.** Blueprint's current dependencies
  (`pyyaml`, `click`, `pydantic`, `rich`) all happen to be installed by Airflow itself, so this
  is invisible today. If you add a new third-party dependency to `pyproject.toml`, the mount will
  not install it and neither will the released package — add it to the affected example's
  `requirements.txt` until the next release, or the DAG will fail to import.

Delete the `blueprint/` volume and the `PYTHONPATH` entry and the examples run against PyPI,
which is the configuration a reader gets.

### Examples on `main` can use unreleased features

Because of the mount, an example on `main` may use an API that is not in the latest release. That
is intentional — `main`'s examples document `main`. It does mean someone copying an example out
before the feature ships gets the released `airflow-blueprint` and an import or attribute error.
If that matters for a particular change, pin a floor in the example's `requirements.txt`
(`airflow-blueprint>=0.5.0`) as part of the release, not before it: an unpublished version in
`requirements.txt` breaks the image build.

## Airflow version

The examples target **Airflow 3** and use its import paths directly:

```python
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import TaskGroup
```

`airflow-blueprint` itself supports Airflow 2.5.0+, and blueprints written against Airflow 2
work the same way — only these import paths differ (`airflow.operators.bash` and
`airflow.utils.task_group`). The examples do not carry a compatibility shim for both, because it
would sit at the top of every file and obscure the thing each example is there to teach. If you
publish blueprints for consumers on either version, see
[shared-blueprints-package](shared-blueprints-package/), which covers what that costs you.

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
