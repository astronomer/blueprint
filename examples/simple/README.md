# Simple Example

One DAG with two concurrent extract steps followed by a load step. Demonstrates the basics: defining blueprints, composing them via YAML, and loading with `build_all()`.

## Quick Start

```bash
cd examples/simple/airflow3   # or airflow2
tilt up
```

Or without Tilt:

```bash
docker compose -f examples/simple/airflow3/docker-compose.yaml up --build
```

Airflow UI: http://localhost:8080
- Airflow 3: no login required
- Airflow 2: username `admin`, password `admin`
