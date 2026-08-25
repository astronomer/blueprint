# Editor and CI

Giving DAG authors autocomplete in their editor and a lint gate in CI, so config mistakes are
caught before review rather than after deploy.

## Why you'd do this

Blueprint validates configs at build time, but that still requires writing the file, pushing it,
and waiting for a DAG to appear. Editor and pre-commit feedback arrives before any of that.

Two pieces of wiring cover most of it: a JSON Schema the editor can read, and `blueprint lint` in
a pre-commit hook and a CI job. Neither needs a running Airflow.

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

A project with several `BlueprintDagArgs` templates gets one DAG schema per template, since each
describes a different set of top-level fields. Name the one you want, and point each directory's
editor config at the matching file:

```bash
blueprint schema --dag-args project_dag_args -o schemas/dag.schema.json
blueprint schema --dag-args sandbox_dag_args -o schemas/sandbox.dag.schema.json
```

Bare `--dag-args` resolves whichever template covers `--template-dir`, which is unambiguous only
when the project has one. See [scoped-dag-args](../scoped-dag-args/).

### Optional fields carry a single type

An optional field — `schedule: str | None` — reaches JSON Schema as an `anyOf` of `string` and
`null`. Generated schemas collapse that to a plain type:

```json
"schedule": { "type": "string", "title": "Schedule" }
```

Absence from `required` already marks the field optional, so the type does not spell nullability
out a second time. This matters because editors, form builders and client generators read `type`
as a single string and handle an `anyOf` poorly or not at all — a nullable `anyOf` is a common
reason a field renders as an untyped text box.

Params work the other way round: a param always holds a value, so an unset optional field is an
explicit null and the type keeps `null` in it. See [runtime-params](../runtime-params/).

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
validation. Step configs are checked by `blueprint lint`, which is why the lint gate matters more
than the editor wiring.

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
- With no `--profile`, it validates each file against **every** declared variable profile, so a
  value defined for `prod` but missing for `dev` fails in CI rather than at deploy. Pass
  `--root` if your loader builds from somewhere other than the working directory. See
  [variables-and-profiles](../variables-and-profiles/).
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
- [scoped-dag-args](../scoped-dag-args/) — one DAG schema per directory
- [variables-and-profiles](../variables-and-profiles/) — what lint checks across profiles
