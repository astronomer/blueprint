# Config Validation

Using the Pydantic config model as a contract with DAG authors, so bad YAML fails with a clear
message instead of producing a subtly wrong DAG.

## Why you'd do this

The config model is the entire public interface of a blueprint. Whatever it permits, someone
will eventually write — and the further a bad value travels, the worse the failure gets. A
`batch_size` of 0 caught at lint time is a two-second fix; the same value caught in production
is a 3am page.

So the goal is to make invalid configurations unrepresentable, and to make the resulting error
message the best documentation the DAG author will ever read.

## Files

| File | What it does |
|---|---|
| `dags/blueprints.py` | A permissive `extract` config next to a strict `load` config |
| `dags/warehouse_load.dag.yaml` | A DAG whose configs all pass |
| `invalid/*.yaml` | Four DAGs that each fail a different way |
| `dags/loader.py` | Builds the valid DAG |

The files in `invalid/` are deliberately broken. They are named `.yaml` rather than
`.dag.yaml`, so neither Airflow nor a bare `blueprint lint` picks them up — you lint them
by name.

## Run it

No Docker needed. From this directory:

```bash
blueprint lint                                # the valid DAG
blueprint lint invalid/unknown-field.yaml     # and each broken one
blueprint describe load                       # the contract, as authors see it
```

To see the DAG in Airflow anyway:

```bash
../run.sh config-validation
```

## Walk-through

### Not every config should be strict

`ExtractConfig` has one required field and no constraints. That is a legitimate choice for a
blueprint that is hard to misuse. Strictness has a cost — every constraint is a thing a DAG
author can trip over — so spend it where mistakes are plausible or expensive.

`LoadConfig` is the opposite, and the rest of this section is what it buys.

### Reject unknown fields

```python
model_config = ConfigDict(extra="forbid")
```

This is the single highest-value line in the file. Pydantic's default is to **ignore** fields
it does not recognise, so `batchsize: 5000` is silently dropped and the default of 1000 is used.
The DAG builds, runs, and is quietly wrong.

```
FAIL invalid/unknown-field.yaml
  Error: 1 validation error for LoadConfig
batchsize
  Extra inputs are not permitted [type=extra_forbidden, input_value=5000,
input_type=int]
```

### Constrain values, not just types

```python
mode: Literal["append", "overwrite", "upsert"] = Field(default="append", ...)
batch_size: int = Field(default=1000, ge=1, le=100_000, ...)
```

`Literal` gives an enum — and doubles as a dropdown if the blueprint ever exposes runtime
params. `ge`/`le` bound the range:

```
FAIL invalid/out-of-range.yaml
  Error: 1 validation error for LoadConfig
batch_size
  Input should be less than or equal to 100000 [type=less_than_equal,
input_value=250000, input_type=int]
```

### Nested models validate too

`destination` is a `Warehouse`, whose `schema_name` and `table` are pattern-constrained. Errors
are reported against the nested path, so the author knows exactly which key to fix:

```
FAIL invalid/bad-schema-name.yaml
  Error: 1 validation error for LoadConfig
destination.schema_name
  String should match pattern '^[a-z][a-z0-9_]*$' [type=string_pattern_mismatch,
input_value='Core Analytics', input_type=str]
```

Patterns are worth applying to anything that ends up in a task ID, a table name or a file path.

### Custom validators for rules types cannot express

A field validator sees one field after its type has been checked:

```python
@field_validator("dedupe_keys")
@classmethod
def keys_must_be_unique(cls, value: list[str]) -> list[str]:
    if len(set(value)) != len(value):
        raise ValueError(f"dedupe_keys contains duplicates: {value}")
    return value
```

A model validator sees the whole config, so it can relate fields to each other:

```python
@model_validator(mode="after")
def upsert_requires_keys(self) -> "LoadConfig":
    if self.mode == "upsert" and not self.dedupe_keys:
        raise ValueError("mode 'upsert' requires at least one entry in dedupe_keys")
    return self
```

Every field in `invalid/upsert-without-keys.yaml` is individually valid; the combination is not:

```
FAIL invalid/upsert-without-keys.yaml
  Error: 1 validation error for LoadConfig
  Value error, mode 'upsert' requires at least one entry in dedupe_keys
[type=value_error, input_value={'destination': {'databas...ers'}, 'mode':
'upsert'}, input_type=dict]
```

Write the message for the person who has to fix the YAML — name the field and say what would
be acceptable.

The payoff shows up in `render()`, which needs no defensive checks:

```python
if config.mode == "upsert":
    # config.dedupe_keys is guaranteed non-empty here.
    keys = ",".join(config.dedupe_keys)
```

One caveat: custom validators run at build time and in `resolve_config()`, but they do **not**
map to JSON Schema, so Airflow's trigger form cannot enforce them. See
[runtime-params](../runtime-params/) for where that matters.

### Fields that YAML must not set

For an internal knob, use a private attribute — not a regular field:

```python
_shard_factor: int = PrivateAttr(default=4)

@property
def shards(self) -> int:
    return max(1, self.batch_size // 250) * self._shard_factor
```

A private attribute is not a model field: it is absent from the JSON schema, absent from
`blueprint describe`, and cannot be set from YAML. A `@property` is the companion pattern for
values *derived* from real fields — `render()` reads `config.shards` without recomputing it.

Note that `Field(init=False)` does **not** achieve this on a `BaseModel`. The field remains
settable and still appears in the schema.

### Where these run

Validation happens when the DAG is built, which means `blueprint lint` catches everything above
without an Airflow instance — fast enough for a pre-commit hook. See
[editor-and-ci](../editor-and-ci/) for wiring that up.

## What to look at in the UI

Open `warehouse_load`. `load_customer_events` is in `upsert` mode, so it has a `dedupe` task;
`load_customers` is in `overwrite` mode and does not. Same blueprint, different shape, decided
by a validated config.

## Related

- [editor-and-ci](../editor-and-ci/) — catching these errors before commit
- [runtime-params](../runtime-params/) — which constraints survive into the trigger form
- [testing-blueprints](../testing-blueprints/) — asserting a config rejects what it should
