# Blueprint

Build reusable, validated Airflow DAG templates that anyone on your team can discover and use.

## What is Blueprint?

Blueprint helps data platform teams define reusable, parameterized DAG templates for Apache Airflow. These templates can be safely configured by other team members, like data analysts or less-experienced engineers, using simple YAML files with optional Jinja2 templating.

With Blueprint, you can:

- **Define reusable patterns** – Write a DAG pattern once, use it many times
- **Enforce type-safe parameters** – Pydantic validation catches errors before deployment
- **Get clear error messages** – Know exactly what's wrong and how to fix it
- **Use Jinja2 templating** – Dynamic YAML configs with env vars, Airflow Variables, and Connections
- **See full code in Airflow UI** – Blueprint code is visible in the Code tab
- **Auto-discover configs** – `build_all()` finds all YAML files that use your Blueprint

## Installation

```bash
pip install airflow-blueprint
```

**Requirements:** Python 3.8+ and Apache Airflow 2.5.0+

## Quick Start

### 1. Create a Blueprint template

Save this in `dags/daily_etl.py`:

```python
from datetime import datetime, timezone
from blueprint import Blueprint, BaseModel, Field

class DailyETLConfig(BaseModel):
    """Configuration for daily ETL jobs."""
    job_id: str = Field(description="Unique identifier for this job")
    source_table: str = Field(description="Table to read data from")
    target_table: str = Field(description="Table to write processed data to")
    schedule: str = Field(default="@daily", description="Cron expression or Airflow preset")
    retries: int = Field(default=2, description="Number of retry attempts on task failure")

class DailyETL(Blueprint[DailyETLConfig]):
    """Daily ETL job that moves data between tables."""

    def render(self, config: DailyETLConfig):
        from airflow import DAG
        from airflow.operators.python import PythonOperator

        dag = DAG(
            dag_id=config.job_id,
            schedule=config.schedule,
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            catchup=False,
            default_args={"retries": config.retries}
        )
        with dag:
            PythonOperator(
                task_id="extract_transform_load",
                python_callable=lambda: print(
                    f"Moving data from {config.source_table} to {config.target_table}"
                )
            )
        return dag

# Auto-discover and build DAGs from *.dag.yaml files
DailyETL.build_all()
```

### 2. Create a YAML config

Save this as `dags/customer_etl.dag.yaml`:

```yaml
blueprint: daily_etl
job_id: customer-daily-sync
source_table: raw.customers
target_table: analytics.dim_customers
schedule: "@hourly"
retries: 4
```

### 3. Validate your config (optional)

```bash
$ blueprint lint dags/customer_etl.dag.yaml
✅ customer_etl.dag.yaml - Valid
```

**Done!** Airflow will now load a DAG with ID `customer-daily-sync`.

### 4. View in Airflow UI

- **Code tab**: See the full Blueprint Python code
- **Rendered Templates tab** (select a task instance): See the resolved YAML configuration

## Project Structure

Place Blueprint templates and YAML configs together in your `dags/` folder:

```
dags/
├── daily_etl.py              # Blueprint template
├── customer_etl.dag.yaml     # Config for customer ETL
├── orders_etl.dag.yaml       # Config for orders ETL
├── multi_source_etl.py       # Another Blueprint template
└── sales_analytics.dag.yaml  # Config for sales analytics
```

This structure provides:

1. **UI Code Visibility** – Airflow shows full code in the Code tab
2. **Self-Documenting** – The Pydantic config is visible with the DAG logic
3. **Auto-Discovery** – `build_all()` finds all `.dag.yaml` files that reference the Blueprint
4. **Rendered Config View** – Select a task instance and view "Rendered Templates" to see resolved YAML values

## Jinja2 Templating in YAML

YAML configs support Jinja2 templating for dynamic values at DAG parse time:

```yaml
# dags/templated_etl.dag.yaml
blueprint: daily_etl

# Environment variables
job_id: "{{ env.get('ENV', 'dev') }}-orders-etl"

# Airflow Variables
source_table: "{{ var.value.source_schema }}.orders"
target_table: "{{ var.value.target_schema }}.orders_clean"

# With default values
schedule: "{{ var.value.etl_schedule | default('@daily') }}"
retries: "{{ env.get('ETL_RETRIES', '2') | int }}"
```

### Available Template Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `env.VAR_NAME` | Environment variable (raises error if missing) | `{{ env.DATABASE_HOST }}` |
| `env.get('VAR', 'default')` | Environment variable with default | `{{ env.get('ENV', 'dev') }}` |
| `var.value.key` | Airflow Variable value | `{{ var.value.schema_name }}` |
| `conn.conn_id.host` | Airflow Connection attribute | `{{ conn.postgres_default.host }}` |

## Python API

In addition to YAML configs, you can create DAGs programmatically using `build()`. This is done directly in the Blueprint template file alongside `build_all()`:

```python
# In dags/daily_etl.py

# ... Blueprint class definition ...

# Auto-discover and build DAGs from YAML files
DailyETL.build_all()

# Also create DAGs programmatically
DailyETL.build(
    job_id="customer-daily-sync",
    source_table="raw.customers",
    target_table="analytics.dim_customers",
    schedule="@hourly",
    retries=4
)
```

### Dynamic DAG Generation

The Python API shines when you need to create DAGs dynamically based on configuration. `build()` automatically registers DAGs in globals using the `dag_id`, so Airflow discovers them:

```python
# In dags/daily_etl.py

# ... Blueprint class definition ...

# Auto-discover YAML configs
DailyETL.build_all()

# Dynamic DAG generation from a list of configurations
TABLES_TO_SYNC = [
    {"source": "raw.users", "target": "staging.users", "schedule": "@daily"},
    {"source": "raw.products", "target": "staging.products", "schedule": "@hourly"},
]

for table_config in TABLES_TO_SYNC:
    table_name = table_config["source"].split(".")[1]
    DailyETL.build(
        job_id=f"{table_name}-sync",
        source_table=table_config["source"],
        target_table=table_config["target"],
        schedule=table_config["schedule"],
    )
```

### `build_all()` Options

The `build_all()` method discovers YAML configs and builds DAGs:

```python
# Basic usage - discovers *.dag.yaml files in dags/ directory
DailyETL.build_all()

# With custom options
DailyETL.build_all(
    search_path="configs/",              # Custom search directory
    pattern="*.dag.yaml",                # File pattern (default)
    render_templates=True,               # Enable Jinja2 (default)
    template_context={"custom": "value"} # Extra template variables
)
```

### Loading from YAML with Overrides

Use `build_from_yaml()` to load a YAML config with runtime overrides:

```python
# In dags/daily_etl.py

# ... Blueprint class definition ...

DailyETL.build_all()

# Load YAML with runtime overrides (e.g., for environment-specific adjustments)
DailyETL.build_from_yaml("customer_etl.dag.yaml", overrides={
    "job_id": "customer-etl-staging",  # Different ID to avoid conflict
    "retries": 5,
    "schedule": "@hourly"
})
```

## Type Safety and Validation

Blueprint uses Pydantic for validation with helpful error messages:

```python
from blueprint import BaseModel, Field, field_validator

class ETLConfig(BaseModel):
    # Basic constraints
    job_id: str = Field(pattern=r'^[a-zA-Z0-9_-]+$')
    retries: int = Field(ge=0, le=5)
    timeout_minutes: int = Field(gt=0, le=1440)

    # Custom validation
    @field_validator('schedule')
    @classmethod
    def validate_schedule(cls, v):
        valid_presets = ['@once', '@hourly', '@daily', '@weekly', '@monthly', '@yearly']
        is_cron = v.count(' ') >= 4  # Simple cron check
        if v not in valid_presets and not is_cron:
            raise ValueError(f'Must be a preset or valid cron expression, got: {v}')
        return v
```

When validation fails, you get clear feedback:

```bash
$ blueprint lint dags/customer_etl.dag.yaml
❌ customer_etl.dag.yaml
  ValidationError: 2 validation errors for DailyETLConfig

  job_id
    String should match pattern '^[a-zA-Z0-9_-]+$'
    Given: "customer sync!" (contains invalid characters)

  retries
    Input should be less than or equal to 5
    Given: 10
```

## Complex Configurations

### Nested Objects and Lists

```python
from blueprint import BaseModel, Blueprint, Field
from typing import Optional, List

class SourceConfig(BaseModel):
    database: str = Field(description="Database connection name")
    table: str = Field(description="Table to extract from")

class NotificationConfig(BaseModel):
    email: Optional[str] = Field(default=None)
    slack_channel: Optional[str] = Field(default=None)

class MultiSourceConfig(BaseModel):
    job_id: str
    sources: List[SourceConfig] = Field(description="Data sources to process")
    notifications: NotificationConfig = Field(default_factory=NotificationConfig)

class MultiSourceETL(Blueprint[MultiSourceConfig]):
    def render(self, config: MultiSourceConfig):
        from airflow import DAG
        # Access nested data with full type safety
        for source in config.sources:
            print(f"Processing {source.table} from {source.database}")
        # ... create and return DAG

MultiSourceETL.build_all()
```

```yaml
# dags/sales_analytics.dag.yaml
blueprint: multi_source_etl
job_id: sales-pipeline

sources:
  - database: postgres
    table: users
  - database: mysql
    table: orders

notifications:
  email: data-team@company.com
  slack_channel: "#data-alerts"
```

### Config Inheritance

Share common parameters across configs:

```python
class BaseETLConfig(BaseModel):
    owner: str = Field(default="data-team")
    retries: int = Field(default=2, ge=0, le=5)
    email_on_failure: str = Field(default="alerts@company.com")

class S3ImportConfig(BaseETLConfig):
    """Inherits owner, retries, email_on_failure from BaseETLConfig."""
    job_id: str
    bucket: str = Field(description="S3 bucket name")
    prefix: str = Field(description="S3 key prefix")
```

## CLI Commands

```bash
# List available blueprints
blueprint list

# Show blueprint parameters and documentation
blueprint describe daily_etl

# Validate configuration files
blueprint lint                              # All *.dag.yaml files
blueprint lint dags/customer_etl.dag.yaml   # Specific file

# Create new YAML config interactively
blueprint new

# Initialize Blueprint in a new project
blueprint init

# Generate JSON schema for IDE autocompletion
blueprint schema daily_etl --output schema.json
```

## Testing Blueprints

```python
import pytest
from dags.daily_etl import DailyETL, DailyETLConfig
from pydantic import ValidationError

def test_valid_config():
    """Test that valid configs produce DAGs."""
    dag = DailyETL.build(
        job_id="test-etl",
        source_table="raw.test",
        target_table="analytics.test"
    )
    assert dag.dag_id == "test-etl"
    assert len(dag.tasks) > 0

def test_invalid_config():
    """Test that invalid configs raise errors."""
    with pytest.raises(ValidationError):
        DailyETL.build(
            job_id="invalid job!",  # Contains space and !
            source_table="raw.test",
            target_table="analytics.test"
        )

def test_config_defaults():
    """Test default values are applied."""
    config = DailyETLConfig(
        job_id="test",
        source_table="raw.test",
        target_table="analytics.test"
    )
    assert config.schedule == "@daily"
    assert config.retries == 2
```

## Configuration

Blueprint CLI looks for templates in these locations (in order):
1. `--template-dir` CLI option
2. `BLUEPRINT_TEMPLATE_PATH` environment variable
3. `template_path` in `blueprint.toml` config file
4. `$AIRFLOW_HOME/dags` (default)

Example using environment variable:

```bash
export BLUEPRINT_TEMPLATE_PATH=/path/to/templates
```

Example `blueprint.toml`:

```toml
template_path = "dags"
output_dir = "dags"
```

## Best Practices

1. **Keep Blueprints focused** – Each Blueprint should represent one type of workflow
2. **Use descriptive parameter names** – `source_table` is clearer than `src`
3. **Add descriptions to all fields** – `Field(description="...")` helps users understand parameters
4. **Document your Blueprints** – Add docstrings explaining the workflow
5. **Provide sensible defaults** – Common values as defaults, critical values as required
6. **Validate in CI** – Add `blueprint lint` to your CI pipeline
7. **Use Jinja2 for environment-specific values** – Keep sensitive data out of YAML files

## Blueprint vs DAG Factory

[DAG Factory](https://github.com/astronomer/dag-factory) gives full control of Airflow via YAML:

```yaml
# DAG Factory - full Airflow configuration exposed
my_dag:
  default_args:
    owner: 'data-team'
    retries: 2
  schedule_interval: '@daily'
  tasks:
    extract_data:
      operator: airflow.operators.python.PythonOperator
      python_callable_name: extract_from_api
      python_callable_file: /opt/airflow/dags/etl/extract.py
```

Blueprint hides Airflow complexity behind validated, domain-specific parameters:

```yaml
# Blueprint - simple, validated parameters
blueprint: daily_etl
job_id: customer-sync
source_table: raw.customers
target_table: analytics.dim_customers
schedule: "@hourly"
```

**Use DAG Factory if:** You need full Airflow flexibility and your users understand Airflow

**Use Blueprint if:** You want standardized patterns with validation, type safety, and simplicity

## Contributing

We welcome contributions! Please see our [Contributing Guide](docs/CONTRIBUTING.md) for details.

## License

Apache 2.0
