# Python DAG Interop

Using blueprints inside hand-written Airflow DAGs, so an existing codebase can adopt them
gradually instead of all at once.

## Why you'd do this

An existing codebase of Python DAGs cannot be converted to YAML in one step, and some DAGs are
not worth converting at all — one with a bespoke reconciliation step and an external lock has
little in common with a template.

A blueprint is a class whose `render()` returns ordinary Airflow objects, so it can be used
inside a `with DAG(...)` block alongside hand-written operators. Convert the parts that follow a
pattern and leave the rest.

## Files

| File | What it does |
|---|---|
| `dags/legacy_hybrid_dag.py` | A hand-written DAG with two steps rendered from blueprints |
| `dags/fully_declarative.dag.yaml` | The same pipeline once migration finishes |
| `dags/blueprints.py` | Blueprints used by both, unaware of the difference |
| `dags/loader.py` | Builds the YAML DAG; the Python DAG needs no loader |

## Run it

```bash
../run.sh python-dag-interop
```

Both DAGs appear. `blueprint lint` covers only the YAML one.

## Walk-through

### Rendering a blueprint by hand

```python
extract = Extract()
extract.step_id = "extract_customers"
extract_group = extract.render(ExtractConfig(source="crm", batch_size=500))
```

Three lines: instantiate, set `step_id`, call `render()`. `step_id` is what the builder would
normally set from the YAML step name, and it determines the `task_id` or `group_id` the
blueprint renders under — so it is required, not optional.

You build the config object yourself, which means your type checker sees it. That is a small
advantage over YAML: a wrong field name here is caught before the DAG is parsed.

### Wiring it to everything else

```python
acquire_lock >> extract_group >> load_task >> reconcile >> release_lock
```

`render()` returns a `BaseOperator` or a `TaskGroup` — plain Airflow objects — so `>>` works
exactly as usual and it does not matter which one you got back. Blueprint-rendered and
hand-written tasks are indistinguishable to Airflow.

### These files need no loader

`build_all_airflow_dags()` discovers `*.dag.yaml`. A hand-written DAG is found by Airflow's own
scanner, the same as any other DAG file, so the two mechanisms coexist without knowing about
each other. Both kinds of file can sit in one folder.

Keep the safe-mode rule in mind: Airflow only parses a file whose contents mention both
`airflow` and `dag`. A hybrid module that imports everything from `blueprints` and never says
`airflow` will be silently skipped.

### A migration path

1. **Extract the pattern.** Take the shape repeated across DAGs and make it a blueprint. The
   first one is a refactor with no visible change.
2. **Render it in place**, as above. Each DAG shrinks; behaviour does not change. Reviewable
   one file at a time, revertible one file at a time.
3. **Convert the DAGs that are now entirely blueprint calls** to YAML — compare
   `legacy_hybrid_dag.py` with `fully_declarative.dag.yaml`, which describe nearly the same
   pipeline.
4. **Leave the rest.** A DAG that is mostly bespoke can stay in Python indefinitely; a partly
   converted DAG is a valid end state.

Avoid the opposite approach: a blueprint carrying an `if` for every caller's special case. At
that point the step is not standard, and the DAG that needs the variation should keep its
hand-written task.

## What to look at in the UI

`legacy_hybrid` shows `acquire_lock`, then an `extract_customers` group containing `validate`
and `fetch`, then `load_customers`, `reconcile_legacy_totals` and `release_lock`. Only the
middle two came from blueprints, and nothing in the graph distinguishes them.

`fully_declarative` has the same two blueprint steps and none of the bespoke ones.

The **Rendered Template** tab still shows `blueprint_step_config` and `blueprint_step_code` on
the blueprint-rendered tasks in the hand-written DAG.

## Related

- [getting-started](../getting-started/) — the YAML flow this migrates towards
- [dags-from-data](../dags-from-data/) — the other non-YAML path, via `Builder`
- [composing-blueprints](../composing-blueprints/) — the same `render()` call, from a blueprint
