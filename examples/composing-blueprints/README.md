# Composing Blueprints

Building a high-level blueprint out of lower-level ones, so a common combination becomes a
single step with a smaller config.

## Why you'd do this

Patterns repeat. If every DAG ends with "run the quality checks, then post the results", that
pair will be copied into thirty YAML files — and the day you add a mandatory check, you edit
thirty files and miss two.

Wrapping the pair in one blueprint moves that decision into Python, where it is made once. The
composite also gets to be *opinionated*: it can fix the settings that should not vary and
expose only the ones that should.

## Files

| File | What it does |
|---|---|
| `dags/blueprints.py` | `validate` and `report` as building blocks, `quality_gate` composing them |
| `dags/gated_release.dag.yaml` | The composite: one step, three keys |
| `dags/assembled_release.dag.yaml` | The same pieces wired by hand, for a case the composite does not cover |
| `dags/loader.py` | Builds both DAGs |

## Run it

```bash
../run.sh composing-blueprints
```

Or compare the two configs without Docker:

```bash
blueprint describe quality_gate
blueprint describe validate
```

## Walk-through

### The mechanics

There is no special API. Instantiate the blueprint, set `step_id`, call `render()`:

```python
def render(self, config: QualityGateConfig) -> TaskOrGroup:
    with TaskGroup(group_id=self.step_id) as group:
        validate = Validate()
        validate.step_id = "validate"
        validate_group = validate.render(
            ValidateConfig(table=config.table, checks=config.checks, fail_fast=True)
        )

        report = Report()
        report.step_id = "report"
        report_task = report.render(
            ReportConfig(channel=config.channel, mention_on_failure="@data-oncall")
        )

        validate_group >> report_task
    return group
```

Two things to note. You construct the inner config yourself, so it is type-checked like any
other Pydantic model — a change to `ValidateConfig` breaks the composite at parse time rather
than silently. And `step_id` only needs to be unique within the enclosing group, so `"validate"`
is safe even if the DAG has another step by that name.

### The composite decides what not to expose

`QualityGateConfig` has three fields; the two configs behind it have five. `fail_fast=True` and
`mention_on_failure="@data-oncall"` are hardcoded, not passed through.

That is the point rather than an oversight. A gate that blocks a release should behave the same
everywhere, and every field you expose is a decision you have delegated. Start with the
smallest config that works and widen it when a real DAG needs it — widening a config is
backwards compatible, narrowing one is not.

### The building blocks stay available

Composing does not hide `validate` and `report`. Both are still registered and still usable
directly, which `assembled_release.dag.yaml` does — it needs all checks to run rather than fail
fast, and a different on-call team, so it wires the pieces itself with `trigger_rule: all_done`
on the report.

This is the healthy end state: a composite for the common case, raw blocks for the exceptions.
If most DAGs end up assembling by hand, the composite is wrong; if none ever do, consider
whether the blocks need to be public at all.

### When not to compose

Composition costs indirection — a reader chasing what `gate_customers` does now reads two
configs and two `render()` methods. It earns that cost when the combination is genuinely
standard.

If the two steps are usually used together but wired differently each time, leave them separate
and let YAML do the wiring; that is what `depends_on` is for. Nesting more than two levels deep
is usually a sign the top-level config has become a pass-through.

## What to look at in the UI

Open `gated_release`. The single `gate_customers` step expands to a `validate` subgroup of three
sequential checks — sequential because the composite set `fail_fast=True` — followed by
`report`. Full task IDs read `gate_customers.validate.nulls`.

Compare `assembled_release`: the same work, two top-level steps, four checks in parallel, and
the group boundary in a different place.

## Related

- [tasks-and-taskgroups](../tasks-and-taskgroups/) — nesting groups, which this relies on
- [step-dependencies](../step-dependencies/) — the alternative: let YAML do the wiring
- [platform-defaults](../platform-defaults/) — standardising DAG-level settings the same way
