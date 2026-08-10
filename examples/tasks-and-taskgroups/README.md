# Tasks and TaskGroups

What `render()` may return: a single operator, a TaskGroup, or a TaskGroup whose shape is
computed from the config.

## Why you'd do this

A step in YAML is one line, but the work behind it usually isn't. A blueprint decides how much
structure sits behind that line, and the YAML author is insulated from the choice — they write
`blueprint: extract` either way.

That insulation is the point. A blueprint can start as a single task and grow into a
five-task group with a retry loop and a validation step, and no DAG YAML has to change. So
return whatever shape the work actually needs, and change it freely later.

## Files

| File | What it does |
|---|---|
| `dags/blueprints.py` | `notify` (single task), `extract` (group), `publish` (config-driven group) |
| `dags/shapes.dag.yaml` | One DAG using all three, so the shapes sit side by side in the graph |
| `dags/loader.py` | Builds the DAG |

## Run it

```bash
../run.sh tasks-and-taskgroups      # Airflow 3
../run.sh tasks-and-taskgroups 2    # Airflow 2
```

The graph view is the whole point of this example, so it is worth actually starting.

## Walk-through

### `self.step_id` means two different things

This is the one rule to internalise. `self.step_id` is the step name from YAML, and where you
put it determines the naming of everything the step produces.

Returning a single operator, it is the **task ID**:

```python
def render(self, config: NotifyConfig) -> TaskOrGroup:
    return BashOperator(
        task_id=self.step_id,
        bash_command=f"echo 'Notifying {config.channel}'",
    )
```

Returning a group, it is the **group ID**, and the child task IDs are yours to pick:

```python
def render(self, config: ExtractConfig) -> TaskOrGroup:
    with TaskGroup(group_id=self.step_id) as group:
        validate = BashOperator(task_id="validate", ...)
        fetch = BashOperator(task_id="fetch", ...)
        validate >> fetch
    return group
```

Child IDs only need to be unique within the group — Airflow prefixes them, so the step
`extract_orders` yields `extract_orders.validate` and `extract_orders.fetch`. That prefixing is
why two steps using the same blueprint never collide, and why hardcoding `task_id="extract"`
instead of using `self.step_id` breaks the moment someone adds a second step.

### Structure can come from the config

`Publish` builds one task per region and nests them in a subgroup:

```python
with TaskGroup(group_id=self.step_id) as group:
    with TaskGroup(group_id="regions") as regions:
        for region in config.regions:
            BashOperator(task_id=region, ...)

    verify = BashOperator(task_id="verify", ...)
    regions >> verify
return group
```

`render()` is ordinary Python running at DAG parse time, so loops and conditionals are fair
game. A three-region config produces five tasks; a one-region config produces three. The YAML
author changes a list, not a graph.

Two cautions. Task IDs derived from config values inherit whatever those values are, so
constrain them in the config model if they might contain characters Airflow dislikes — see
[config-validation](../config-validation/). And because this runs on every parse, keep
`render()` cheap: no network calls, no database queries.

### Returning either type from one blueprint

The `TaskOrGroup` return annotation covers both, and the builder wires dependencies the same
way regardless — depending on a group means depending on the whole group. A blueprint may even
choose at runtime:

```python
def render(self, config: MaybeGroupConfig) -> TaskOrGroup:
    if not config.validate_first:
        return BashOperator(task_id=self.step_id, ...)
    with TaskGroup(group_id=self.step_id) as group:
        ...
    return group
```

## What to look at in the UI

Open `task_shapes` in the graph view:

- `announce` is a bare task.
- `extract_orders` is a collapsed group — expand it for `validate` → `fetch`.
- `publish_orders` expands to a `regions` subgroup of three tasks, then `verify`.

Note the edge from `extract_orders` to `publish_orders`. The YAML said the step depends on the
step; Airflow resolves that to the group boundary, so everything in `publish_orders` waits for
everything in `extract_orders`.

## Related

- [getting-started](../getting-started/) — the basics, if this is your first stop
- [step-dependencies](../step-dependencies/) — wiring steps together in YAML
- [composing-blueprints](../composing-blueprints/) — building a group out of other blueprints
