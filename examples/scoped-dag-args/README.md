# Scoped DAG Args

Several `BlueprintDagArgs` templates in one project, each governing the directory it sits in.

## Why you'd do this

One set of DAG-level standards rarely fits a whole repository. Production DAGs should require an
owning team, retry, and alert on failure. A `sandbox/` directory where analysts prototype should
require none of that -- but it should require an expiry date, so abandoned experiments are
findable.

Those are different sets of allowed fields, not different defaults for the same set. Defining a
template per directory lets each area declare its own YAML surface, with no flag in the loader
and no `if` in the template.

## Files

| File | What it does |
|---|---|
| `dags/dag_args.py` | `ProjectDagArgs` -- production standards; scopes all of `dags/` |
| `dags/sandbox/dag_args.py` | `SandboxDagArgs` -- prototype standards; scopes `dags/sandbox/` |
| `dags/customer_orders.dag.yaml` | Production DAG; sets `team` and `sla_minutes` |
| `dags/sandbox/churn_experiment.dag.yaml` | Prototype; sets `expires` and `author` |
| `dags/loader.py` | One `build_all_airflow_dags()`, unaware of either template |

## Run it

```bash
../run.sh scoped-dag-args
```

```bash
blueprint list                     # both templates, and what each applies to
blueprint lint                     # which template each DAG resolved to
blueprint schema --dag-args sandbox_dag_args
```

## Walk-through

### How a DAG finds its template

The search starts in the DAG file's own directory and walks up. The first template found wins,
so a subdirectory overrides its parents:

```
dags/
  dag_args.py                     ProjectDagArgs
  customer_orders.dag.yaml        -> project_dag_args
  sandbox/
    dag_args.py                   SandboxDagArgs
    churn_experiment.dag.yaml     -> sandbox_dag_args
```

The directory that scopes a template is the one holding the `.py` file that defines it -- not
where the class is imported, and not where the loader lives.

`blueprint list` shows the mapping directly:

```console
$ blueprint list
                             DAG Args Templates
┏━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Name             ┃ Applies to    ┃ Fallback ┃ Location                       ┃
┡━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ project_dag_args │ dags/         │          │ dags/dag_args.py               │
│ sandbox_dag_args │ dags/sandbox/ │          │ dags/sandbox/dag_args.py       │
└──────────────────┴───────────────┴──────────┴────────────────────────────────┘
```

And `blueprint lint` reports what each file resolved to, which is the quickest way to confirm a
new directory is picking up the template you meant:

```console
$ blueprint lint
PASS dags/customer_orders.dag.yaml (dag_id=customer_orders, dag_args=project_dag_args)
PASS dags/sandbox/churn_experiment.dag.yaml (dag_id=churn_experiment, dag_args=sandbox_dag_args)
```

### The surfaces differ in both directions

`customer_orders.dag.yaml` must set `team`; it may not set `expires`. The sandbox DAG is the
reverse. A field from the wrong template is rejected with the accepted set spelled out:

```console
$ blueprint lint dags/customer_orders.dag.yaml
FAIL dags/customer_orders.dag.yaml
❌ Configuration Error in customer_orders.dag.yaml
  DAG arguments rejected by template 'project_dag_args' (dags/dag_args.py):
  - 'expires': Extra inputs are not permitted

  💡 Suggestions:
    • Accepted DAG arguments: description, schedule, sla_minutes, team
```

The error names the template that rejected it, so a mis-scoped file is obvious rather than
mysterious.

### DAGs with no template above them

A DAG that no template's directory contains has nothing to resolve, and with more than one
registered, Blueprint will not guess:

```console
$ blueprint schema --dag-args
Error: No BlueprintDagArgs template is defined in the directory of '.' or any
directory above it, and several are registered with none declared as the
fallback:
  • project_dag_args (dags/dag_args.py)
  • sandbox_dag_args (dags/sandbox/dag_args.py)
```

Declare one as the fallback for that case:

```python
class ProjectDagArgs(BlueprintDagArgs[ProjectDagArgsConfig], default=True):
    ...
```

This matters most for templates that have no directory at all -- one shipped from an installed
package covers nothing by location, so `default=True` is how it applies to anything. See
[shared-blueprints-package](../shared-blueprints-package/).

A project with exactly one template needs none of this: the sole template is used everywhere.

### Two in one directory

Two templates in the same directory are ambiguous, and Blueprint raises rather than picking one:

```console
FAIL dags/customer_orders.dag.yaml
  Error: Multiple BlueprintDagArgs templates are defined in dags:
  • extra_dag_args (dags/dag_args.py)
  • project_dag_args (dags/dag_args.py)
PASS dags/sandbox/churn_experiment.dag.yaml (dag_id=churn_experiment, dag_args=sandbox_dag_args)
```

Note the second line: the failure is raised per DAG that resolves to the contested directory, so
a mistake in one area does not unload the rest of the project.

Two templates sharing a *name* also collide; set `name` on the class to register one under a
different one:

```python
class SandboxDagArgs(BlueprintDagArgs[SandboxDagArgsConfig]):
    name = "prototype_dag_args"
```

### Schemas, one per template

Each template describes a different DAG YAML, so each gets its own schema:

```bash
blueprint schema --dag-args project_dag_args > schemas/dag.schema.json
blueprint schema --dag-args sandbox_dag_args > schemas/sandbox.dag.schema.json
```

Point your editor at the matching one per directory. Bare `--dag-args` resolves the template
covering `--template-dir` instead of naming one. See [editor-and-ci](../editor-and-ci/).

### The Builder API

DAGs built directly in Python resolve from the file that builds them, so pass `source_path`:

```python
builder.build(dag_config, source_path=__file__)
```

Without it there is no location to resolve from and only the fallbacks apply. See
[dags-from-data](../dags-from-data/).

## What to look at in the UI

`customer_orders` is tagged `team:orders` and `tier:production`, with 3 retries and a 30-minute
`dagrun_timeout` from `sla_minutes`.

`churn_experiment` arrives **paused**, tagged `tier:sandbox` and `expires:2026-12-31`, with 0
retries and no failure email. Neither DAG file says anything about retries or pausing; both got
them from the template above it.

## Related

- [platform-defaults](../platform-defaults/) -- what a single `BlueprintDagArgs` does, plus
  `on_dag_built` for settings authors never see
- [shared-blueprints-package](../shared-blueprints-package/) -- templates from an installed
  package
- [editor-and-ci](../editor-and-ci/) -- generating and checking schemas
