# DAGs From Data

Generating a family of near-identical DAGs from a data source, using the `Builder` API instead
of one YAML file each.

## Why you'd do this

Sometimes the set of DAGs follows from data rather than from an authoring decision: one pipeline
per tenant, per region, or per source system. The pipelines are structurally identical and only
a few values differ.

Maintaining one file per tenant by hand lets the DAG list drift from the data: a new tenant has
no DAG until someone adds the YAML, and a removed tenant keeps running. Generating the DAGs from
the same data that defines the tenants keeps the two consistent.

## Files

| File | What it does |
|---|---|
| `dags/tenants.json` | The data: three tenants with different sources, schedules and datasets |
| `dags/generated_dags.py` | Loops over it, building one DAG per tenant |
| `dags/blueprints.py` | The blueprints every generated DAG reuses |

There is no `loader.py` and no `*.dag.yaml` here — this example is entirely programmatic.

## Run it

```bash
../run.sh dags-from-data
```

`blueprint lint` reports no files to check, which is correct: there is no YAML to lint. See
[testing-blueprints](../testing-blueprints/) for how to cover generated DAGs instead.

## Walk-through

### `DAGConfig` is the YAML file, as an object

```python
config = DAGConfig(
    dag_id=f"tenant_{tenant['id']}_etl",
    schedule=tenant["schedule"],
    description=f"ETL for {tenant['id']}",
    steps={
        "extract": {
            "blueprint": "extract",
            "source": tenant["source"],
            "datasets": tenant["datasets"],
        },
        "load": {
            "blueprint": "load",
            "depends_on": ["extract"],
            "target_schema": f"warehouse_{tenant['id']}",
        },
    },
)

dag = builder.build(config)
globals()[dag.dag_id] = dag
```

`DAGConfig` accepts exactly the fields a YAML file would: `dag_id`, `steps`, and whatever your
`BlueprintDagArgs` allows at the top level. Step dicts use the same reserved keys —
`blueprint`, `depends_on`, `version`, `trigger_rule` — with everything else passed to the
blueprint's config model.

This means you get the same validation. A tenant record with a bad `source` fails the same way
it would from YAML, naming the same field.

### Registering in `globals()`

Airflow finds DAGs by scanning a module's global namespace, so each built DAG needs a distinct
module-level name. `globals()[dag.dag_id] = dag` is the idiom. Assigning to a loop variable
instead leaves you with exactly one DAG — the last one.

### Keep the source of truth outside the loop

`tenants.json` is a file here to keep the example self-contained, but the point is that it
could be anything: a config repo, an API response cached to disk, a table.

One firm constraint. This module is re-executed on **every DAG parse**, so whatever it reads
must be fast and must not fail. Querying a warehouse here means the query runs every 30 seconds
and an outage empties your DAG list. Read from a file the deployment writes, or a cache
refreshed on a schedule — not the live system.

Keep Airflow settings out of the data. `tenants.json` records facts about tenants; the mapping
from a tenant to a schedule and a set of steps stays in Python, where it is reviewed alongside
the rest of the code.

### YAML and generated DAGs coexist

Nothing stops a folder having both — a `loader.py` handling hand-written YAML alongside a
module like this one. They share the same registry and the same blueprints. Reach for
generation when the *set* of DAGs is data-driven, and keep YAML for the ones a person
deliberately authored.

The trade you are making is discoverability: `grep dag_id` no longer finds these, and reviewing
a change means reading a loop rather than a diff. That is a good trade for thirty
machine-generated pipelines and a bad one for three.

## What to look at in the UI

Three DAGs — `tenant_acme_etl`, `tenant_globex_etl`, `tenant_initech_etl`. They are not
identical: acme extracts three datasets on an hourly schedule, initech extracts one, daily.
Each is driven by its record in `tenants.json`.

Add a fourth tenant to the JSON and a fourth DAG appears at the next parse, with no new file.

## Related

- [platform-defaults](../platform-defaults/) — applying policy to generated DAGs too
- [python-dag-interop](../python-dag-interop/) — blueprints inside hand-written DAGs
- [testing-blueprints](../testing-blueprints/) — asserting generated DAGs are what you expect
