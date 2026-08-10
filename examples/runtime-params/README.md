# Runtime Parameters

Letting someone override a step's config when they trigger the DAG, without editing YAML.

## Why you'd do this

Some pipelines are triggered by a person with an intent: rebuild *this* table for *these*
dates, just this once. Without runtime params the options are all bad — edit the YAML and
revert it afterwards, keep a near-duplicate "manual" DAG, or hand the job to whoever can deploy.

A blueprint that opts into params turns its config into an Airflow trigger form, pre-filled
from the YAML. The YAML still describes the normal case; the form is how you depart from it for
one run.

## Files

| File | What it does |
|---|---|
| `dags/blueprints.py` | `backfill` opts into params and reads them two ways; `notify` does not |
| `dags/backfill.dag.yaml` | An unscheduled DAG whose values become the form defaults |
| `dags/loader.py` | Builds the DAG |

## Run it

This one is worth actually running — the trigger form is the feature.

```bash
../run.sh runtime-params
```

Then open `events_backfill` and hit **Trigger DAG w/ config**.

## Walk-through

### Opting in

```python
class Backfill(Blueprint[BackfillConfig]):
    supports_params = True
```

Every field of `BackfillConfig` is registered as an Airflow DAG param named
`{step}__{field}` — so the step `rebuild_events` produces `rebuild_events__target_table`,
`rebuild_events__start_date`, and so on. The namespacing is what lets two steps of the same
blueprint coexist in one form.

Only opt in if `render()` actually reads the params. `Notify` does not, and that is deliberate:
its `render()` bakes `config.channel` into the command at parse time, so a `channel` box on the
trigger form would accept input and change nothing. A param that silently does nothing is worse
than no param.

### Two ways to read a param

**`self.param()`** returns a Jinja2 string that Airflow resolves at execution time:

```python
plan = BashOperator(
    task_id="plan",
    bash_command=f"echo 'Planning rebuild of {self.param('target_table')} ...'",
)
```

This only works in fields the operator declares as templated — `bash_command`, a
BigQuery `configuration`, a SQL `sql` field. Put it anywhere else and you get the literal
`{{ params.… }}` string.

**`self.resolve_config()`** merges the overrides back into the Pydantic model, for Python:

```python
@task(task_id="execute")
def execute(**context):
    resolved = self.resolve_config(config, context)
    if resolved.dry_run:
        print(f"DRY RUN: would rebuild {resolved.target_table}")
```

You get a real validated `BackfillConfig` — typed values, defaults applied, every validator
re-run against the overrides. Prefer this for anything with logic in it. The two mix freely in
one blueprint, as they do here.

### Shaping the form

Airflow renders each field from its JSON Schema, and Blueprint passes your Pydantic metadata
through, so `json_schema_extra` controls the widget:

```python
start_date: str = Field(default="2024-01-01", json_schema_extra={"format": "date"})
query: str = Field(default="...", json_schema_extra={"format": "multiline"})
warehouse_size: Literal["xsmall", "small", "medium", "large"] = Field(
    default="small",
    json_schema_extra={"values_display": {"xsmall": "X-Small (cheapest)", ...}},
)
```

`format` accepts `multiline`, `date`, `date-time` and `time`. `values_display` labels enum
options, `examples` gives a dropdown that still accepts free text, and `description_md` allows
Markdown. A `Literal` becomes a dropdown on its own; `values_display` only makes it readable.

Field descriptions become the help text under each input, so they are worth writing.

### Which validation the form enforces

Constraints that map to JSON Schema are enforced everywhere. Custom validators are not — the
form does not know about them.

| Validation | Build time | Trigger form | `resolve_config()` |
|---|---|---|---|
| `Field(ge=1)` | Yes | Yes | Yes |
| `Field(pattern=...)` | Yes | Yes | Yes |
| `Literal[...]` | Yes | Yes | Yes |
| `@field_validator` | Yes | **No** | Yes |
| `@model_validator` | Yes | **No** | Yes |

The practical consequence: if a rule only exists in a custom validator, a trigger-form override
can violate it. Read the config through `resolve_config()` in a `@task` and the validator runs
before anything acts on the value. Rely on `self.param()` alone and it does not.

### Keep params scalar

Scalars and `Literal`s render as real controls. Nested models, unions and lists render as JSON
text boxes — they work, but they are a poor thing to hand someone at 3am. Fields people
override often are worth flattening for that reason alone.

### Triggering without the UI

```bash
curl -X POST http://localhost:8080/api/v2/dags/events_backfill/dagRuns \
  -H 'Content-Type: application/json' \
  -d '{"logical_date": "2024-02-01T00:00:00Z",
       "conf": {"rebuild_events__target_table": "analytics.events_staging",
                "rebuild_events__dry_run": false}}'
```

Same namespaced keys as the form.

## What to look at in the UI

Open `events_backfill` → **Trigger DAG w/ config**. The form shows a `rebuild_events` section
with a date picker for the dates, a textarea for the query, and a labelled dropdown for the
warehouse size — all pre-filled from YAML. `announce` has no section at all, because `notify`
did not opt in.

Trigger once with `dry_run` left on, then again with it off, and compare the `execute` task
logs. Then check `plan`'s **Rendered Template** tab to see `self.param()` resolved.

## Related

- [config-validation](../config-validation/) — the validators the form cannot enforce
- [templating](../templating/) — parse-time Jinja, which resolves much earlier than this
- [getting-started](../getting-started/) — where `blueprint_step_config` comes from
