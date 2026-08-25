# DAG Arguments

What a DAG YAML may set at its top level, and which template decides that.

## Why you'd do this

Blueprints standardise what happens *inside* a step. A `BlueprintDagArgs` template standardises
everything around it — retries, timeouts, ownership, tags, alerting.

Left to convention these values drift: one DAG has 10 retries left over from a debugging
session, another has none, and untagged DAGs have no recorded owner when they fail. Rather than
asking authors for each value, derive them from two fields they can answer reliably — who owns
the DAG, and how important it is.

One set of standards rarely fits a whole repository, so a project can define several templates,
each governing the directory it sits in. Here production DAGs live at the root and prototypes
under `sandbox/`, with genuinely different fields rather than different defaults.

## Files

| File | What it does |
|---|---|
| `dags/dag_args.py` | `ProjectDagArgs` — turns `team` + `tier` into DAG settings; scopes all of `dags/` |
| `dags/sandbox/dag_args.py` | `SandboxDagArgs` — prototype standards; scopes `dags/sandbox/` |
| `dags/payments_ledger.dag.yaml` | Critical tier |
| `dags/customer_orders.dag.yaml` | Tier omitted, so it defaults to standard |
| `dags/sandbox/churn_experiment.dag.yaml` | Different fields entirely: `expires`, `author` |
| `dags/loader.py` | One `build_all_airflow_dags()`, unaware of either template |
| `dags/blueprints.py` | Ordinary extract/load — not the point of this example |

## Run it

```bash
../run.sh dag-arguments
```

Or check the policy without Docker:

```bash
blueprint list                     # both templates, and what each applies to
blueprint lint                     # which template each DAG resolved to
blueprint schema --dag-args project_dag_args   # the DAG-level contract, as JSON Schema
```

## Walk-through

### The config model is the permitted field list

A `BlueprintDagArgs` subclass works like a blueprint, but for the top level of a DAG YAML. Its
config model *is* the list of permitted top-level fields:

```python
class ProjectDagArgsConfig(BaseModel):
    schedule: str | None = None
    description: str | None = None
    team: str = Field(pattern=r"^[a-z][a-z0-9-]*$", ...)
    tier: Literal["critical", "standard", "experimental"] = "standard"
```

There is no escape hatch here. `retries` is not a field, so no YAML can set it — unlike a
defaults dict, which authors can always override locally. If a setting should not vary, leave
it out of the config.

Note `team` has no default, which makes it required. That single line is how you guarantee
every DAG in the repository is attributable:

```console
FAIL dags/noteam.dag.yaml
❌ Configuration Error in noteam.dag.yaml
  DAG arguments rejected by template 'project_dag_args' (dags/dag_args.py):
  - 'team': Field required

  💡 Suggestions:
    • Accepted DAG arguments: description, schedule, team, tier
```

The error names the template doing the rejecting and lists the whole accepted surface, which is
also the fastest way to see what a DAG in this directory is allowed to set.

### render() maps those fields onto DAG kwargs

```python
def render(self, config: ProjectDagArgsConfig) -> dict[str, Any]:
    policy = TIER_POLICY[config.tier]
    return {
        "catchup": False,
        "max_active_runs": 1,
        "tags": [f"team:{config.team}", f"tier:{config.tier}"],
        "dagrun_timeout": timedelta(hours=policy["timeout_hours"]),
        "default_args": {
            "owner": config.team,
            "retries": policy["retries"],
            "email_on_failure": config.tier == "critical",
        },
        ...
    }
```

`catchup=False` and `max_active_runs=1` are unconditional — the house style, applied whether or
not anyone remembers it. Everything else follows from `tier`, so changing the retry policy for
every critical DAG in the company is a one-line edit to `TIER_POLICY`.

Only pass through what was actually set. Writing `"schedule": config.schedule` unconditionally
would push `None` into every DAG that omitted it, which is not the same as leaving it alone.

With no template defined at all, the built-in `DefaultDagArgs` allows just `schedule` and
`description`.

### How a DAG finds its template

The search starts in the DAG file's own directory and walks up. The first template found wins,
so a subdirectory overrides its parents:

```
dags/
  dag_args.py                     ProjectDagArgs
  payments_ledger.dag.yaml        -> project_dag_args
  customer_orders.dag.yaml        -> project_dag_args
  sandbox/
    dag_args.py                   SandboxDagArgs
    churn_experiment.dag.yaml     -> sandbox_dag_args
```

The directory that scopes a template is the one holding the `.py` file that defines it — not
where the class is imported, and not where the loader lives. The loader itself selects nothing.

`blueprint list` shows the mapping directly (Description column elided here for width):

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
PASS dags/payments_ledger.dag.yaml (dag_id=payments_ledger, dag_args=project_dag_args)
PASS dags/sandbox/churn_experiment.dag.yaml (dag_id=churn_experiment, dag_args=sandbox_dag_args)
```

### The surfaces differ in both directions

The two config models are unrelated. A production DAG must set `team` and may not set
`expires`; the sandbox DAG is the reverse, and knows nothing about `tier`. A field from the
wrong template is rejected with the accepted set spelled out, exactly as a missing required
field is.

That asymmetry is the reason to scope templates rather than add optional fields to one. A single
template covering both would have to accept `team` and `expires` as optional, which means
neither is enforced anywhere.

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

This matters most for templates that have no directory at all — one shipped from an installed
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

### Which settings belong in a config field?

Ask whether a DAG author would ever legitimately want it different.

- **Yes, within limits** → a config field, constrained. `tier` is the example: three values,
  each with a defined meaning.
- **Yes, but only in one part of the repo** → a template scoped to that directory. `expires` is
  required of prototypes and meaningless elsewhere.
- **No** → `render()`, unconditionally. `catchup=False` is not a matter of opinion.
- **Not their concern at all** → the `on_dag_built` callback, covered in
  [dag-post-processing](../dag-post-processing/).

The mistake to avoid is a passthrough field — accepting `retries` and forwarding it. That is
the defaults dict again, with extra steps.

## What to look at in the UI

All three DAGs in the list view, tagged from fields no YAML file spells out as tags.

Open `payments_ledger` → any task → **Details**: 5 retries, a 2-hour DAG timeout, email on
failure. The same view on `customer_orders`, which omitted `tier` entirely: 2 retries, 6-hour
timeout, no email.

`churn_experiment` arrives **paused**, tagged `tier:sandbox` and `expires:2026-12-31`, with 0
retries. Its YAML says nothing about retries or pausing; it got them from the template in its
own directory.

## Related

- [dag-post-processing](../dag-post-processing/) — the settings authors never see at all
- [composing-blueprints](../composing-blueprints/) — the same instinct, applied inside a step
- [config-validation](../config-validation/) — constraining what the fields accept
- [shared-blueprints-package](../shared-blueprints-package/) — templates from an installed package
- [editor-and-ci](../editor-and-ci/) — generating and checking schemas
- [dags-from-data](../dags-from-data/) — DAG args when building without YAML
