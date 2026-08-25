# Variables and Profiles

Declaring a value once as `${name}`, and letting it differ between environments without a
second copy of the DAG.

## Why you'd do this

A pipeline is usually the same everywhere except for a handful of values: which database it
writes to, how often it runs. The usual workarounds are a `dev` copy of every YAML file that
drifts from the `prod` one, or an `env.get()` chain repeated in each file.

Variables put those values in one place and leave the DAG files identical across environments.
Unlike Jinja2, they resolve *before* validation, so `blueprint lint` checks the real values --
and it checks them under every profile, catching a value you only defined for one.

## Files

| File | What it does |
|---|---|
| `dags/blueprint.vars.yaml` | Project variables; declares the `prod` and `dev` profiles |
| `dags/clickstream.dag.yaml` | Adds its own variables, one of them per-profile |
| `dags/finance/blueprint.vars.yaml` | Overrides one variable for the DAGs in that directory |
| `dags/finance/revenue.dag.yaml` | Picks up the override without referring to it |
| `dags/loader.py` | Chooses the active profile -- the only decision made in Python |

## Run it

```bash
../run.sh variables-and-profiles
```

```bash
blueprint lint                                              # every declared profile
blueprint lint --profile prod                               # narrow to one
blueprint vars dags/clickstream.dag.yaml --profile prod --unused
```

The runtime has no `DEPLOY_ENV` set, so it loads the `dev` profile. Add `DEPLOY_ENV=prod` to a
`.env` file in this directory to see the other one.

## Walk-through

### Declaring variables

Project-wide values go in a `blueprint.vars.yaml` beside the DAGs. A value that differs per
environment is keyed by profile; one that does not is written plainly:

```yaml
profiles: [prod, dev]

vars:
  warehouse_db:
    prod: analytics
    dev: analytics_dev

  landing_schema: raw
  retention_days: 90
```

Keying a variable by profile when it never varies costs two lines and gives you two places to
forget. Only key what actually differs.

Profiles are optional. Everything else here works with no `profiles:` line at all -- declare
them when you have a value that varies.

### Referencing them

A DAG adds its own variables and references any that are in scope:

```yaml
schedule: ${schedule}

vars:
  schedule:
    prod: "@hourly"
    dev: "@daily"
  stream: clickstream

steps:
  materialize_events:
    blueprint: materialize
    target_table: ${warehouse_db}.events.${stream}
    expire_after_days: ${retention_days}
```

References resolve after YAML parsing but before validation, so a blueprint's config model
never sees a `${...}`. It receives an ordinary value and validates it normally.

### Types survive

`expire_after_days: ${retention_days}` stays an `int`, so it satisfies the field's `ge=1`
constraint. A reference occupying the entire value keeps that value's type; embedding one in a
larger string produces a string, which is what `${warehouse_db}.events.${stream}` relies on.

This is the practical difference from Jinja2, where everything comes out a string and a numeric
field needs a cast.

### Selecting a profile

```python
profile = "prod" if os.environ.get("DEPLOY_ENV") == "prod" else "dev"

build_all_airflow_dags(profile=profile)
```

That is the whole of the loader. Python picks *which* profile; it never holds a value the
profile selects between. Keeping the values in YAML is what lets `blueprint lint` resolve
exactly what the DAG processor will.

A profile only has to be selected for variables a DAG actually references. A DAG using nothing
but invariant values needs none, even when the project declares them.

### Overriding per directory

Vars files are read from the search root down to the DAG's own directory, then the DAG's own
`vars:` block. Nearest wins. `dags/finance/blueprint.vars.yaml` changes one variable:

```yaml
vars:
  warehouse_db:
    prod: finance
    dev: finance_dev
```

`revenue.dag.yaml` says nothing about this. It writes to `${warehouse_db}.reporting.invoices`
and gets `finance_dev`, while `landing_schema` and `retention_days` still come from the project
file. An override may also name only the profiles it changes -- listing just `dev` leaves `prod`
inheriting the project value.

`profiles:` is declared once, in the outermost vars file; declaring it again in a nested file is
an error.

The search root is wherever `build_all_airflow_dags()` builds from, and files above it are never
read. `blueprint lint` and `blueprint vars` default their root to the current directory, so run
them from the project root or pass `--root`.

### Seeing what resolved

```console
$ blueprint vars dags/finance/revenue.dag.yaml --profile dev --unused
            Variables for revenue.dag.yaml (profile: dev)
┏━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Variable       ┃ Value         ┃ Source                           ┃
┡━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ landing_schema │ 'raw'         │ dags/blueprint.vars.yaml         │
│ retention_days │ 90            │ dags/blueprint.vars.yaml         │
│ stream         │ 'invoices'    │ dags/finance/revenue.dag.yaml    │
│ warehouse_db   │ 'finance_dev' │ dags/finance/blueprint.vars.yaml │
└────────────────┴───────────────┴──────────────────────────────────┘

Not referenced by this DAG: retention_days
```

The Source column is the resolution order made visible. Without `--profile`, anything
profile-keyed shows as `varies by profile` instead of a value.

### Linting every profile

With no `--profile`, `blueprint lint` validates each file against every declared profile:

```console
$ blueprint lint
PASS dags/clickstream.dag.yaml [prod] (dag_id=clickstream_events)
PASS dags/clickstream.dag.yaml [dev] (dag_id=clickstream_events)
PASS dags/finance/revenue.dag.yaml [prod] (dag_id=finance_revenue)
PASS dags/finance/revenue.dag.yaml [dev] (dag_id=finance_revenue)
```

This is the check worth having in CI. A profile-keyed variable must have a value for the
profile being resolved -- it does not quietly fall back to another profile's -- so a half-filled
variable fails only under the profile that is missing:

```console
FAIL dags/finance/revenue.dag.yaml [dev]
  Error: ❌ Variable 'region' has no value under profile 'dev'
  Defined in dags/finance/revenue.dag.yaml for: prod
```

Validating one profile would have passed this file.

### Escaping

`$${...}` is a literal `${...}`, which matters wherever a value reaches a shell:

```yaml
post_hook: 'echo "published $${TABLE:-unknown}"'
```

Without the doubled `$`, that would be read as a reference to an undefined variable `TABLE`. A
bare `$$` -- the shell PID -- is left alone.

### Values are scalars or lists

A variable is a scalar or a list of scalars. A map is only ever a set of per-profile values: a
map whose keys are not all declared profiles is an error rather than literal data, so `${a.b}`
never has a second meaning. Group related values in the DAG instead:

```yaml
vars:
  bucket: s3://data
steps:
  load:
    paths:
      raw: ${bucket}/raw
      curated: ${bucket}/curated
```

## What to look at in the UI

`clickstream_events` is scheduled `@daily`, from the `dev` value of `schedule`.

Open `materialize_events` → **Rendered Template**. `blueprint_step_config` shows
`analytics_dev.events.clickstream` -- the resolved value, with no trace of the three variables
that produced it. Variables are gone by the time the DAG exists, so nothing in Airflow needs to
know about them.

`finance_revenue` writes to `finance_dev.reporting.invoices` under the same profile, from the
directory override.

## Variables or Jinja2?

Both work in a `.dag.yaml`, and Jinja2 renders first.

Use `${...}` for values that vary by environment. They resolve before validation, keep their
type, are checked by `blueprint lint` under every profile, and are safe for values containing
YAML punctuation.

Use `{{ ... }}` for Airflow runtime context (`{{ context.ds }}`), environment variables, and
anything computed. The active profile is available as `{{ profile }}` for the cases that
genuinely need a conditional.

`blueprint.vars.yaml` is not Jinja2-rendered; only `.dag.yaml` files are.

## Related

- [templating](../templating/) -- Jinja2 in YAML, and its two evaluation times
- [runtime-params](../runtime-params/) -- values chosen when a run is triggered
- [platform-defaults](../platform-defaults/) -- settings computed in Python instead of YAML
- [editor-and-ci](../editor-and-ci/) -- running `blueprint lint` in CI
