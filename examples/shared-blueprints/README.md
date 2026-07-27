# Shared Blueprints Example Package

This folder contains an installable package demonstrating how to distribute Blueprint templates via a Python package. This avoids having to copy-paste code to multiple repositories that want to leverage the same Blueprint templates.

Each repository using this package must still define `.dag.yaml` templates and a `loader` file.

## How it works

`pyproject.toml` declares an entry point:

```toml
[project.entry-points."airflow_blueprint.blueprints"]
shared_blueprints = "shared_blueprints"
```

Projects that install this `shared-blueprints` package will then be able to use the additional templates. The `BlueprintRegistry` auto-discovers templates from the `shared-blueprints` package. Listing the templates using `blueprint list` also displays the templates from the package:

```bash
$ blueprint list
                                             Available Blueprints
┏━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Name    ┃ Versions ┃ Description                                     ┃ Class   ┃ Location                  ┃
┡━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ example │ 1        │ Example Blueprint template from shared package. │ Example │ shared_blueprints.example │
├─────────┼──────────┼─────────────────────────────────────────────────┼─────────┼───────────────────────────┤
│ extract │ 1        │ Pull data from a source system.                 │ Extract │ dags/blueprints.py        │
├─────────┼──────────┼─────────────────────────────────────────────────┼─────────┼───────────────────────────┤
│ load    │ 1        │ Load data into a destination.                   │ Load    │ dags/blueprints.py        │
└─────────┴──────────┴─────────────────────────────────────────────────┴─────────┴───────────────────────────┘
```
