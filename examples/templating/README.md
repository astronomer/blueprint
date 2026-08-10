# Templating

Jinja2 in DAG YAML: what resolves when the DAG is parsed, what is deferred to when a task runs,
and how to keep the two apart.

## Why you'd do this

The same pipeline usually has to differ slightly per environment, per region, or per run. A
bucket name changes between dev and prod; a partition path depends on the run date.

Blueprint renders each YAML file through Jinja2 before parsing it, so one file can cover all of
them. Two different evaluation times are involved and the syntax does not clearly distinguish
them, which accounts for most templating errors.

## Files

| File | What it does |
|---|---|
| `dags/orders.dag.yaml` | Uses every accessor: `env`, `var`, `context`, loader variables, a raw block |
| `dags/loader.py` | Supplies extra variables through `template_context` |
| `dags/blueprints.py` | Passes config straight to a templated operator field |

## Run it

```bash
../run.sh templating
```

```bash
blueprint lint                          # renders the templates, then validates
DEPLOY_ENV=prod blueprint lint          # note the dag_id change
```

## The two evaluation times

**Parse time** is when Airflow loads the DAG file, every 30 seconds or so. Blueprint renders
the Jinja2, and whatever it produces is baked into the DAG object. `env`, `var`, `conn` and
anything from `template_context` resolve here.

**Run time** is when a task actually executes. Airflow resolves its own macros — `ds`,
`data_interval_start`, `ti` — in fields the operator declares as templated. `context.*`
resolves here.

The test: if the value could differ between two runs of the same DAG, it must be run time. A
partition date cannot be baked in at parse time, or every run reprocesses whatever day the
scheduler last reparsed the file.

## Walk-through

### Parse-time accessors

```yaml
dag_id: "{{ env.get('DEPLOY_ENV', 'dev') }}_orders_etl"
schedule: "{{ var.get('orders_schedule', '@daily') }}"
```

`env` is `os.environ`, so `env.get(key, default)` behaves as you would expect. `var` reads
Airflow Variables; always pass a default, because a missing variable at parse time breaks DAG
loading for everyone.

There is also a `conn` accessor (`conn.get('warehouse').host`) for Airflow Connections. Be
aware that it is only available when Airflow itself is doing the parsing — `blueprint lint`
does not stub it, so a file using `conn` cannot be linted. Prefer `var` or `env` for anything
you want covered in CI.

Because these resolve at parse time, they are real strings by the time any blueprint sees them:
`dag_id` becomes `dev_orders_etl`, and the DAG's identity itself is environment-dependent.

### Variables from the loader

```python
build_all_airflow_dags(
    template_context={
        "region": os.environ.get("DEPLOY_REGION", "us-east-1"),
        "warehouse": "analytics",
    },
)
```

Anything in `template_context` becomes a plain Jinja2 variable in every YAML the loader builds.
This is the cleanest way to inject deployment facts: the loader computes them once, in Python,
where the logic is testable, instead of every YAML file repeating an `env.get` chain.

One wrinkle: `blueprint lint` builds the YAML without your loader, so those variables do not
exist there. Give them a default and the file stays lintable:

```yaml
source: "orders_{{ region | default('us-east-1') }}"
```

Without the filter, lint fails with `'region' is undefined` even though the DAG builds fine
inside Airflow. Worth doing for any loader-supplied variable.

### Deferring to run time with `context`

```yaml
partition: "{{ context.ds_nodash }}"
output_path: "s3://.../orders/{{ context.ds }}/data.parquet"
```

The `context` accessor does not read anything. It records the attribute path and renders as a
literal Airflow macro, so `{{ context.ds_nodash }}` becomes the string `{{ ds_nodash }}` in the
built DAG, which Airflow resolves per run. Chained access and calls work too:

```yaml
prev_result: "{{ context.ti.xcom_pull('extract_orders') }}"
```

This only does anything if the value lands in a **templated field** of an operator. Here
`BashOperator.bash_command` is templated, so the macro resolves; put the same string in a
plain Python attribute and the task gets the literal `{{ ds }}`.

Filters and arithmetic on `context` values are *not* supported —
`{{ context.ds | replace('-','') }}` will not work, because `context.ds` is a proxy object,
not a string.

### Escaping with a raw block

When you need an Airflow expression that parse-time Jinja must not touch — anything with a
filter in it — wrap it:

```yaml
label: "run {% raw %}{{ ds | replace('-', '') }}{% endraw %} in {{ region | default('us-east-1') }}"
```

The raw block passes through verbatim and Airflow evaluates it at run time; the `region`
expression outside it still resolves now. Without the raw block, parse-time rendering fails
with `'ds' is undefined`.

### Comments are not exempt

Jinja renders the entire file as text before YAML ever sees it, so a `#` comment is not a
comment to Jinja:

```yaml
# Every {{ ... }} here is evaluated     <- breaks parsing: "unexpected '.'"
# A {% raw %} block protects this       <- opens a raw block that swallows the rest
```

Both of these are real errors, and the message points at the file rather than the line, which
makes them annoying to find. Describe templating in prose in comments, and keep the delimiters
out.

## What to look at in the UI

The DAG is called `dev_orders_etl` — the `dev` came from `DEPLOY_ENV` at parse time.

Open `extract_orders` → **Rendered Template**. `bash_command` shows a real date in the partition
and path, because those were still macros in the DAG and Airflow resolved them for this run.
Compare with **blueprint_step_config**, which shows the config as the DAG stored it, macros and
all. That side-by-side is the clearest picture of the two evaluation times.

## Related

- [runtime-params](../runtime-params/) — a third timing: values chosen when a run is triggered
- [platform-defaults](../platform-defaults/) — computing settings in Python rather than YAML
- [editor-and-ci](../editor-and-ci/) — linting templated YAML in CI
