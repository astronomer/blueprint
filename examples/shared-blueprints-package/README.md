# Shared Blueprints Package

Publishing blueprints as an installable Python package, so many DAG repositories can use one
team's templates without copying code.

## Why you'd do this

The common shape in a larger organisation is one platform team writing templates and several
DAG-authoring teams using them. Those teams have their own repositories.

Copying `blueprints.py` into each repository leads to drift: a fix lands in some repos and not
others, and there is no reliable way to tell which version a given DAG is running. Shipping a
package makes the dependency explicit and versioned — consumers upgrade deliberately, and
`pip list` reports what they are on.

## Files

This example is two projects in one directory — the publisher and a consumer.

| File | What it does |
|---|---|
| `package/pyproject.toml` | Declares the `airflow_blueprint.blueprints` entry point |
| `package/acme_blueprints/blueprints/` | The published templates: `ingest`, `publish` |
| `package/acme_blueprints/utils.py` | Helper code deliberately *outside* the entry point |
| `dags/customer_ingest.dag.yaml` | A consumer DAG using templates it does not define |
| `dags/loader.py` | A consumer loader — note there is no `blueprints.py` beside it |

## Run it

```bash
../run.sh shared-blueprints-package
```

A real consumer repo would list `acme-blueprints` in `requirements.txt` and install it from a
package index. Here the package is a sibling directory, so this example's `Dockerfile` has one
extra line installing it from source — that is the only difference.

Outside Docker, install it first:

```bash
pip install -e package
blueprint list      # ingest and publish, located in acme_blueprints
blueprint lint
```

## Walk-through

### The entry point is the whole integration

```toml
[project.entry-points."airflow_blueprint.blueprints"]
acme_blueprints = "acme_blueprints.blueprints"
```

`BlueprintRegistry` scans every installed package advertising this group, so consumers get the
templates by installing the package and nothing else. No import in the loader, no path
configuration, no registration call. `dags/loader.py` is the same one-liner as a project with
purely local blueprints, and `dags/customer_ingest.dag.yaml` names `ingest` and `publish`
exactly as if they were defined next door.

The key on the left is a label and only needs to be unique within your package; the value on
the right is the module actually scanned.

### Scope the entry point narrowly

Point it at the module holding blueprints, not the top of your package:

```
acme_blueprints/
├── utils.py        <- not scanned
└── blueprints/     <- the entry point target
    ├── ingest.py
    └── publish.py
```

Every submodule under the target is imported on **every DAG parse**. Aim it at
`acme_blueprints` and you pull in `utils.py`, plus any custom operators, cloud SDK imports and
module-level side effects, several times a minute across every scheduler and worker.

Scoping limits *discovery*, not imports. `ingest.py` imports `sla_minutes` from `utils.py`
normally — that module is simply not scanned for blueprint classes.

### Version the package, not just the blueprints

Two independent version numbers are in play, and they answer different questions.

The package version (`0.2.0`) is what consumers pin, and it governs when they take a change.
The blueprint version is what a DAG pins with `version:`, and it governs config compatibility —
see [versioning](../versioning/).

Adding `IngestV2` is a minor package release: existing DAGs pinned to v1 are unaffected, and
unpinned steps move to v2 on upgrade. That last part is the sharp edge of publishing — a
consumer running `pip install -U` can silently change which blueprint version their unpinned
DAGs resolve to. Tell consumers to pin `version:` on anything they care about, and treat
removing a blueprint version as a major release.

### Name collisions are an error, not a merge

Two templates with the same name and version raise `DuplicateBlueprintError`, including when
one is local and one comes from a package. There is no shadowing rule and no precedence order —
a local `ingest.py` does not quietly override the published `ingest`.

That is deliberate: silent shadowing is how you get a DAG that behaves differently in two
environments. If a consumer needs a local variant, it needs a different name. Prefixing
published names (`acme_ingest`) is worth considering if collisions are likely.

### Cross-version imports are your problem now

A DAG repository knows which Airflow it runs. A published package does not, and this example's
code targets Airflow 3 like every other example here. If your consumers span Airflow 2 and 3,
that is a real constraint you inherit, because the import paths moved:

```python
try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.sdk import TaskGroup
except ImportError:  # Airflow 2
    from airflow.operators.bash import BashOperator
    from airflow.utils.task_group import TaskGroup
```

The legacy paths still resolve on Airflow 3 but are deprecated, so importing them
unconditionally is not a shortcut. Test the package against every Airflow version you claim to
support: the failure mode is an `ImportError` during DAG parsing in someone else's repository,
which you will hear about from them rather than from your own CI.

If you do not intend to support both, declare a narrow `apache-airflow` range in
`pyproject.toml` so consumers get a resolution error rather than an import error.

## What to look at in the UI

`customer_ingest` has an `ingest_customers` group (`pull` → `land`) and a `publish_customers`
task. Nothing in the consumer repository defines either.

Open `ingest_customers.pull` → **Rendered Template** → **blueprint_step_code**: the source of
the blueprint, read out of the installed package. When a consumer asks what a template actually
does, that is the answer, without them cloning the platform repo.

## Related

- [versioning](../versioning/) — rolling out a breaking change to consumers you do not control
- [dag-arguments](../dag-arguments/) — the other half of a platform team's surface area
- [editor-and-ci](../editor-and-ci/) — giving consumers schemas for published templates
