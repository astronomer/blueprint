# Editor and CI

Giving DAG authors autocomplete in their editor and a lint gate in CI, so config mistakes are
caught before review rather than after deploy.

## Why you'd do this

Blueprint validates configs at build time, which is early — but "early" still means after
someone wrote the file, pushed it, and waited for a DAG to appear. The feedback that actually
changes behaviour is the kind that arrives while typing.

Two cheap pieces of wiring get most of it: a JSON Schema the editor can read, and
`blueprint lint` in a pre-commit hook and a CI job. Neither needs a running Airflow.

## Files

| File | What it does |
|---|---|
| `schemas/*.json` | Committed JSON Schemas, generated from the blueprints |
| `regenerate-schemas.sh` | Rewrites them, or `--check`s that they are current |
| `dags/pipeline.dag.yaml` | Carries a `yaml-language-server` modeline |
| `.vscode/settings.json` | Applies the schema to every `*.dag.yaml` project-wide |
| `.pre-commit-config.yaml` | Runs lint and the schema check on commit |
| `ci-workflow.yml` | A sample GitHub Actions workflow to copy into your own repo |

## Run it

No Docker needed:

```bash
blueprint lint                  # what CI runs
./regenerate-schemas.sh --check # what the pre-commit hook runs
./regenerate-schemas.sh         # after changing a config model
```

Open `dags/pipeline.dag.yaml` in an editor with the YAML language server and you should get
completion on the top-level keys.

## Walk-through

### Generating schemas

```bash
blueprint schema --dag-args -o schemas/dag.schema.json   # the DAG file itself
blueprint schema extract    -o schemas/extract.schema.json
blueprint schema load       -o schemas/load.schema.json
```

`--dag-args` describes the whole DAG file — `dag_id`, `steps`, and whatever your
`BlueprintDagArgs` permits at the top level. The per-blueprint schemas describe one step's
config. Each carries a `templateType` of `dag_args` or `blueprint` so tooling can tell them
apart.

Note the output directory must already exist; `-o` will not create it.

### Wiring the editor

Per file, with a modeline:

```yaml
# yaml-language-server: $schema=../schemas/dag.schema.json
dag_id: editor_demo
```

Or once for the project, in `.vscode/settings.json`:

```json
"yaml.schemas": { "./schemas/dag.schema.json": ["**/*.dag.yaml"] }
```

Both work with the Red Hat YAML extension in VS Code and with any editor using
`yaml-language-server` — Neovim via lspconfig, JetBrains via its JSON Schema mappings.

**Be clear about what this covers.** The DAG schema validates the top level: it will flag a
misspelled `dag_id`, a missing `steps`, or a DAG-level key your `BlueprintDagArgs` does not
accept. It does **not** validate inside a step, because which config applies depends on that
step's `blueprint:` value, and JSON Schema cannot dispatch on it here — `steps` is typed as an
object of objects.

So the per-blueprint schemas are documentation and tooling input rather than live step
validation. Step configs are checked by `blueprint lint`, which is why the lint gate is the
part you should not skip.

### Keeping schemas honest

A committed schema is a copy, and copies go stale — a field added to `ExtractConfig` without
regenerating leaves editors advertising an interface that no longer exists. Worse, they will
flag a *valid* new field as unknown.

`regenerate-schemas.sh --check` regenerates into a temp directory and diffs, so the hook fails
with instructions rather than silently drifting. The alternative is not committing schemas at
all and generating them in a setup step; committing them means the editor works on a fresh
clone with nothing installed.

### The pre-commit hook

```yaml
- id: blueprint-lint
  name: blueprint lint
  entry: blueprint lint
  language: system
  pass_filenames: false
  files: '\.(dag\.yaml|py)$'
```

`pass_filenames: false` is deliberate. Bare `blueprint lint` validates the whole tree, and that
is what you want: editing `blueprints.py` can invalidate a YAML file that was not touched in
this commit. Passing only changed files would miss exactly that case. Triggering on `.py` as
well as `.dag.yaml` is the same reasoning.

Bare `blueprint lint` also honours `.airflowignore`, so drafts stay excluded — see
[resilient-loading](../resilient-loading/).

### The CI job

`ci-workflow.yml` is a copyable GitHub Actions workflow. The important parts:

- Install whatever provides your blueprints first — for a consumer repo that is the shared
  package, and lint cannot resolve `blueprint:` names without it.
- `blueprint lint` catches unknown blueprints, invalid configs, unknown or cyclic dependencies,
  and duplicate DAG IDs across files. It exits non-zero on any of them.
- No Airflow instance, no database, no scheduler. It runs in seconds, which is what makes it
  usable as a required check.

It is named `ci-workflow.yml` rather than living in `.github/` so this repository's own Actions
do not pick it up.

### What lint does not catch

Lint builds and validates configs; it does not execute tasks. A blueprint whose `render()`
produces a task with a broken command passes lint every time. For that, and for asserting the
structure a blueprint renders, see [testing-blueprints](../testing-blueprints/).

## Related

- [config-validation](../config-validation/) — the errors this surfaces earlier
- [testing-blueprints](../testing-blueprints/) — the layer of checking above lint
- [resilient-loading](../resilient-loading/) — the safety net for what still gets through
