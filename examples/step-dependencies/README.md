# Step Dependencies

Wiring steps together in YAML: what runs in parallel, what waits, and what runs even after
something upstream fails.

## Why you'd do this

`depends_on` and `trigger_rule` are the only two knobs a YAML author has over DAG shape, and
they are the ones people reach for daily. Getting them right is the difference between a
pipeline that leaves scratch data behind on every failure and one that cleans up after itself.

Every blueprint here renders a single task on purpose — the interesting part is the graph, not
what the tasks do.

## Files

| File | What it does |
|---|---|
| `dags/blueprints.py` | Six one-task blueprints: extract, merge, quality_check, publish, notify, cleanup |
| `dags/pipeline.dag.yaml` | One DAG containing every wiring shape below |
| `dags/loader.py` | Builds the DAG |

## Run it

```bash
../run.sh step-dependencies      # Airflow 3
../run.sh step-dependencies 2    # Airflow 2
```

Or check the wiring without Docker, from this directory:

```bash
blueprint lint
```

## Walk-through

### No `depends_on` means "start immediately"

```yaml
  extract_primary:
    blueprint: extract
    source: primary_db

  extract_backup:
    blueprint: extract
    source: backup_db
```

Two steps, no dependencies, so both start as soon as the DAG run does. There is no `parallel:`
flag — parallelism is just the absence of a dependency. Whether they truly run at the same time
depends on your executor and pool slots.

Note that both steps use the same blueprint with different configs. That is the normal case,
not a special one.

### Listing several dependencies is a fan-in

```yaml
  merge_sources:
    blueprint: merge
    depends_on: [extract_primary, extract_backup]
    trigger_rule: one_success
    output: staging.merged
```

`depends_on` takes a list of step names. The names are the YAML keys under `steps`, not blueprint
names and not Airflow task IDs — which matters once a step renders a TaskGroup, since you depend
on the step and Airflow resolves that to the whole group.

### Several steps naming one upstream is a fan-out

```yaml
  publish_dataset:
    blueprint: publish
    depends_on: [check_merged]
    dataset: merged_customers

  announce:
    blueprint: notify
    depends_on: [check_merged]
    channel: "#data-releases"
```

Nothing special to declare: two steps naming the same upstream both wait for it, then run
concurrently.

### `trigger_rule` decides what "ready" means

The default is `all_success` — every upstream step must succeed. Two departures from that are
worth knowing:

```yaml
  merge_sources:
    depends_on: [extract_primary, extract_backup]
    trigger_rule: one_success      # continue if either source came through
```

```yaml
  cleanup:
    depends_on: [publish_dataset, announce]
    trigger_rule: all_done         # run even if upstream failed
```

`one_success` suits a primary/backup pair where either is enough. `all_done` suits cleanup,
teardown and failure notifications — anything that must happen regardless of the outcome. Without
it, a failed publish leaves the scratch bucket full, because `cleanup` would be skipped.

The valid values come from the Airflow version you have installed, so the full set differs
between Airflow 2 and 3. An invalid value is rejected at build time with the list of valid ones.

## Errors you will hit

Both of these are caught by `blueprint lint` before Airflow ever sees the DAG.

A typo in a dependency:

```
FAIL missing.dag.yaml
  Error: Step 'merge_sources' depends on 'extract_primry', which does not exist
  Did you mean: 'extract_primary'?
  Available steps: extract_primary
```

A cycle:

```
FAIL cycle.dag.yaml
  Error: Cyclic dependency detected: extract_primary -> merge_sources ->
publish_dataset -> extract_primary

💡 Suggestions:
  • Review the 'depends_on' fields in your DAG YAML
  • Remove one of the dependencies to break the cycle
```

The cycle error prints the actual path round the loop, which is what you need when the cycle
runs through six steps rather than three.

## What to look at in the UI

Open `dependency_shapes` in the graph view. The two extracts sit side by side at the top,
funnel into `merge_sources`, then `check_merged` fans back out to `publish_dataset` and
`announce`, and both feed `cleanup`.

Hover `merge_sources` and `cleanup` to see their non-default trigger rules. To watch `all_done`
earn its keep, mark `publish_dataset` as failed in a run and confirm `cleanup` still runs.

## Related

- [getting-started](../getting-started/) — the basics
- [tasks-and-taskgroups](../tasks-and-taskgroups/) — depending on a step that renders a group
- [resilient-loading](../resilient-loading/) — when a whole DAG file is broken, not just an edge
