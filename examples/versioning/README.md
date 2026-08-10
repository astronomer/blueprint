# Versioning

Shipping a breaking change to a blueprint without breaking the DAGs already using it.

## Why you'd do this

Once several teams depend on a blueprint, you cannot change its config in place — you would
break every DAG that used the old shape, all at once, on someone else's schedule.

Each version is a separate class. v1 keeps working, unmodified, for as long as it is needed, and
v2 can take a different config. DAGs move over individually, and the migration is visible in git
as a `version:` line being removed from a YAML file.

## Files

| File | What it does |
|---|---|
| `dags/blueprints.py` | `extract` v1/v2 named by class suffix; `load` v1/v2 named explicitly |
| `dags/legacy_pipeline.dag.yaml` | Pinned to v1 throughout |
| `dags/migrated_pipeline.dag.yaml` | On v2, with one step still pinned to v1 mid-migration |
| `dags/loader.py` | Builds both DAGs |

## Run it

```bash
blueprint list          # from this directory: shows extract 1, 2 and load 1, 2
blueprint describe extract -v 1
blueprint describe extract        # latest
```

```bash
../run.sh versioning
```

## Walk-through

### A version is a separate class

```python
class Extract(Blueprint[ExtractConfig]):          # v1: source_table: str
class ExtractV2(Blueprint[ExtractV2Config]):      # v2: sources: list[Source]
```

v2 drops `source_table` entirely and takes a list of nested models instead. No YAML written
against v1 would validate against it, and that is acceptable because v1 remains registered and
unchanged. There is no shared config base class to keep compatible and no migration branch inside
`render()`.

Both register under the name `extract`:

| Class name | Blueprint name | Version |
|---|---|---|
| `Extract` | `extract` | 1 |
| `ExtractV2` | `extract` | 2 |
| `MultiSourceETL` | `multi_source_etl` | 1 |
| `MultiSourceETLV3` | `multi_source_etl` | 3 |

A trailing `V{N}` is parsed as the version; no suffix means version 1.

### Or name the version explicitly

The suffix convention forces the class name to carry the version, which reads badly when the
class name should describe the implementation:

```python
class SingleStatementLoader(Blueprint[SingleStatementLoadConfig]):
    name = "load"
    version = 1

class BulkCopyLoader(Blueprint[BulkCopyLoadConfig]):
    name = "load"
    version = 2
```

Both register under `load`, and the class names stay meaningful. You can set either attribute
alone — an explicit `name` with the version inferred from the suffix, or the reverse.

### Pinning in YAML

Omitting `version` means "latest", which is the right default for a DAG being actively
maintained and the wrong one for a DAG nobody has touched in a year:

```yaml
  extract_customers:
    blueprint: extract
    version: 1              # stays on v1 no matter what ships later
    source_table: raw.customers
```

Steps in the same DAG can be on different versions — `migrated_pipeline` has `load_customers`
on v1 and `load_orders` on v2, which is what a half-finished migration actually looks like.

An unpinned step whose config is still in the old shape fails with a message about the *new*
config model, which is worth recognising:

```
FAIL unpinned.dag.yaml
  Error: 1 validation error for ExtractV2Config
sources
  Field required [type=missing, input_value={'source_table': 'raw.customers'},
input_type=dict]
```

`ExtractV2Config` in the first line is the tell: the step resolved to v2 because it was not
pinned.

### The migration workflow

1. Add the new class alongside the old one. Nothing changes for existing DAGs — unpinned steps
   move to the new version, so pin the laggards first if you are not ready.
2. Move DAGs over one at a time, each a small reviewable diff.
3. Deprecate v1 in its docstring, since that is what `blueprint list` and `blueprint describe`
   show.
4. Delete v1 when `grep -rn "version: 1"` across your DAG repos comes back empty.

Versions must stay contiguous — with v1 and v3 registered and no v2, discovery raises
`NonContiguousVersionError`. That means step 4 is delete-the-oldest, not delete-any.

## What to look at in the UI

`legacy_pipeline` has a single `extract_customers` task. `migrated_pipeline` has an
`extract_core` **group** containing `raw_customers` and `raw_orders`, because v2 renders one
task per source. Same step name, same blueprint name, different version, different shape.

## Related

- [config-validation](../config-validation/) — designing the config a version commits to
- [shared-blueprints-package](../shared-blueprints-package/) — versioning across repositories
- [editor-and-ci](../editor-and-ci/) — catching unpinned steps before they merge
