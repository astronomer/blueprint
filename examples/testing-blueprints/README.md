# Testing Blueprints

Unit-testing blueprint configs and rendered structure, plus an integrity test asserting every
DAG in the repository still builds.

## Why you'd do this

A blueprint is used by DAGs you do not own, in repositories you may not watch. That inverts the
usual testing calculus: a change that looks harmless — renaming a config field, adding a
required one, reordering a dependency inside a group — breaks other people's pipelines, and you
find out from them.

Blueprints are unusually easy to test, because a config is a plain Pydantic model and
`render()` is a plain function returning Airflow objects. No scheduler, no database, no
executor. The whole suite here runs in about a second.

## Files

| File | What it does |
|---|---|
| `tests/test_configs.py` | The config contract: defaults, rejections, custom validators |
| `tests/test_render.py` | The task structure a config produces |
| `tests/test_dag_integrity.py` | Every `*.dag.yaml` still validates and builds |
| `tests/conftest.py` | A DAG-context fixture and two small helpers |
| `pytest.ini` | `pythonpath = dags`, so tests can import `blueprints` |

## Run it

No Docker, no Airflow instance:

```bash
pytest -q
```

```
18 passed in 1.32s
```

## Walk-through

### Three layers, cheapest first

**Config tests** need nothing but Pydantic. They are the highest value per line, because the
config *is* the public interface:

```python
def test_rejects_unknown_field(self):
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        ExtractConfig(source="crm", datasets=["customers"], dataset="typo")


def test_upsert_requires_dedupe_keys(self):
    with pytest.raises(ValidationError, match="requires at least one entry"):
        LoadConfig(target="warehouse.customers", mode="upsert")
```

Test the *rejections* especially. `extra="forbid"` and a `model_validator` are one line each and
easy to delete by accident during a refactor; a test is what makes their removal show up.

**Render tests** cover what a config turns into — the branches a config test cannot reach:

```python
def test_one_task_per_dataset(self, dag):
    group = render(Extract, ExtractConfig(source="crm", datasets=["customers", "orders"]))
    assert task_ids(group) == {"validate", "pull_customers", "pull_orders"}


def test_validation_task_is_optional(self, dag):
    group = render(Extract, ExtractConfig(source="crm", datasets=["customers"],
                                          validate_first=False))
    assert task_ids(group) == {"pull_customers"}
```

Worth asserting `group_id` comes from `step_id`, too — hardcoding an ID instead of using
`self.step_id` is the classic blueprint bug, and it only shows up once a DAG has two steps of
the same blueprint.

**Integrity tests** catch the cross-cutting break: a blueprint change invalidating YAML nobody
edited.

```python
@pytest.mark.parametrize("path", YAML_FILES, ids=lambda p: p.name)
def test_yaml_is_valid(path):
    result = validate_yaml(str(path), template_dir=str(DAGS_DIR))
    assert result["dag_id"]
```

`validate_yaml()` is the same check `blueprint lint` performs, available as a function.
Parametrising over discovered files means new DAGs are covered automatically — with
`test_there_are_dag_files` guarding against the suite passing because discovery quietly
returned nothing.

Building the DAGs goes further than lint, since it actually runs `render()` and wires
dependencies:

```python
@pytest.fixture(scope="module")
def dags():
    built = build_all_airflow_dags(search_path=str(DAGS_DIR), register_globals={})
    return {dag.dag_id: dag for dag in built}
```

Passing `register_globals={}` keeps the built DAGs out of the test module's namespace, which is
what you want in a test.

### The DAG context fixture

`TaskGroup` cannot be constructed outside a DAG, so render tests need one:

```python
@pytest.fixture
def dag():
    with DAG(dag_id="test_dag", schedule=None, start_date=datetime(2024, 1, 1)) as test_dag:
        yield test_dag
```

Tests take the fixture without using it directly — it is the ambient context that makes
`render()` legal. A blueprint returning a bare operator does not strictly need it, but taking it
uniformly avoids a confusing failure the day that blueprint grows a group.

### What to assert, and what not to

Assert the things a downstream DAG depends on: the set of task IDs, the group ID, the wiring
between tasks, and the config rules. These are the blueprint's contract.

Avoid asserting the exact text of a `bash_command`. It makes every cosmetic change a test
failure and proves nothing about behaviour. `test_mode_reaches_the_command` checks that
`"overwrite"` appears somewhere in it — enough to catch a config value being dropped, loose
enough to survive rewording.

### Where this fits

`blueprint lint` in CI ([editor-and-ci](../editor-and-ci/)) validates configs. These tests go
further: they check the structure `render()` produces and the rules the config enforces, which
lint cannot see. Run both.

Neither runs your tasks. For that you need a real Airflow — the integration suite in this
repository's own `tests/integration/` is one way to do it.

## Related

- [editor-and-ci](../editor-and-ci/) — lint, the layer below this
- [config-validation](../config-validation/) — the validation rules being tested
- [shared-blueprints-package](../shared-blueprints-package/) — why this matters most when publishing
