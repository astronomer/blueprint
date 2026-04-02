# Blueprint Examples

## [Simple](simple/)

One DAG, two blueprints, zero advanced features. Start here.

```bash
cd examples/simple/airflow3
tilt up
```

## [Advanced](advanced/)

Space-themed example demonstrating many Blueprint features: versioning, runtime params, custom DAG args, Jinja2 templating, `on_dag_built` callbacks, and more.

```bash
cd examples/advanced/airflow3
tilt up
```

## Airflow 2 vs 3

Both examples support Airflow 2 and 3. The DAGs and blueprints are identical -- only the infrastructure differs. Use `airflow2/` instead of `airflow3/` to run on Airflow 2.

| | Airflow 2 | Airflow 3 |
|---|---|---|
| Base image | `astro-runtime:13.4.0-base` | `runtime:3.1-13-base` |
| UI server | `webserver` | `api-server` |
| DAG parsing | Built into scheduler | Standalone `dag-processor` |
| DB init | `airflow db init` | `airflow db migrate` |
| Auth | RBAC with admin/admin | SimpleAuthManager (no login) |

## Airflow UI

http://localhost:8080 after starting either example.
