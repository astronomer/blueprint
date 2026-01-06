# Blueprint

Build reusable, validated Airflow DAG templates that anyone on your team can discover and use.

## What is Blueprint?

Blueprint helps data platform teams define reusable, parameterized DAG templates for Apache Airflow. These templates can be safely configured by other team members, like data analysts or less-experienced engineers, using simple YAML files.

With Blueprint, you can:

- ✅ Enforce **type-safe parameters** with validation
- 🚫 Get **clear error messages** when configs are invalid
- 🛠️ Use a **CLI** to validate configs before deployment
- 🔍 Automatically **discover available templates** and **generate new DAGs** from them

## Why Blueprint?

In most data teams, the same kind of DAG is built over and over with small variations. This usually means lots of copy-pasting and hard-to-maintain code. Blueprint solves this by letting you:

- **Create once, use everywhere** – Write a DAG pattern once as a template
- **Reduce errors** – Validate configurations before deployment
- **Build guardrails** – Enforce your standards and best practices
- **Help non-engineers** – Let others safely define DAGs without touching Python

## Example Workflow

### 1. Create a Blueprint template

Save this in `blueprint/templates/etl_blueprints.py`:

```python
from blueprint import Blueprint, BaseModel, Field
from airflow import DAG

class DailyETLConfig(BaseModel):
    job_id: str = Field(description="Unique identifier for this job")
    source_table: str = Field(description="Table to read data from")
    target_table: str = Field(description="Table to write processed data to")
    schedule: str = Field(default="@daily", description="Cron expression or Airflow preset")
    retries: int = Field(default=2, description="Number of retry attempts on task failure")

class DailyETL(Blueprint[DailyETLConfig]):
    """Daily ETL job that moves data between tables with configurable scheduling."""

    # Name is auto-generated as "daily_etl" from class name
    # Or specify explicitly:
    # name = "daily_etl_job"

    def render(self, config: DailyETLConfig) -> DAG:
        from airflow.operators.python import PythonOperator
        from datetime import datetime

        with DAG(
            dag_id=config.job_id,
            schedule=config.schedule,
            start_date=datetime(2024, 1, 1),
            catchup=False,
            default_args={"retries": config.retries}
        ) as dag:
            PythonOperator(
                task_id="extract_transform_load",
                python_callable=lambda: print(
                    f"Moving data from {config.source_table} to {config.target_table}"
                )
            )
        return dag
```

### 2. Create a YAML config

Save this as `dags/configs/customer_etl.dag.yaml`:

```yaml
blueprint: daily_etl  # Auto-generated from class name DailyETL
job_id: customer-daily-sync
source_table: raw.customers
target_table: analytics.dim_customers
schedule: "@hourly"
retries: 4
```

### 3. Validate your config

```bash
$ blueprint lint
  customer_etl.dag.yaml - Valid
```

🎉 **Done!** Blueprint builds your DAG with ID `customer_etl`.

## Python API

Blueprint templates can also be consumed directly in Python, providing full type safety and IDE support:

```python
from etl_blueprints import DailyETL

# Create DAG with keyword arguments
dag = DailyETL.build(
    job_id="customer-daily-sync",
    source_table="raw.customers",
    target_table="analytics.dim_customers",
    schedule="@hourly",
    retries=4
)
```

### Benefits of Python API

- **Full IDE support** - Autocomplete, type checking, and inline documentation
- **Runtime validation** - Catch configuration errors before deployment
- **Dynamic DAG generation** - Create multiple DAGs programmatically
- **Testing support** - Easy unit testing of your DAG configurations

### Dynamic DAG Generation

The Python API shines when you need to create DAGs dynamically based on external configuration:

```python
from etl_blueprints import DailyETL
import json

# Load table configurations from external source
with open('etl_config.json') as f:
    table_configs = json.load(f)

# Generate a DAG for each table with custom logic
for config in table_configs:
    schedule = "@hourly" if config["priority"] == "high" else "@daily"
    retries = 5 if config["is_critical"] else 2

    dag = DailyETL.build(
        job_id=f"{config['name']}-etl",
        source_table=config["source"],
        target_table=config["target"],
        schedule=schedule,
        retries=retries
    )
```

### Creating Conditional DAGs

```python
from etl_blueprints import DailyETL
import os

# Only create production DAGs in production environment
if os.getenv("AIRFLOW_ENV") == "production":
    critical_tables = ["users", "transactions", "orders"]

    for table in critical_tables:
        dag = DailyETL.build(
            job_id=f"prod-{table}-sync",
            source_table=f"raw.{table}",
            target_table=f"warehouse.{table}",
            schedule="@hourly",
            retries=5
        )
```

### Python Example

```python
from blueprint import Blueprint, BaseModel, Field, field_validator
from airflow import DAG
from datetime import datetime

class DailyETLConfig(BaseModel):
    job_id: str = Field(pattern=r'^[a-zA-Z0-9_-]+$', description="Unique job identifier")
    source_table: str = Field(description="Source table name")
    target_table: str = Field(description="Target table name")
    schedule: str = Field(default="@daily", description="Cron or preset schedule")
    retries: int = Field(default=2, ge=0, le=5, description="Number of retries")

    @field_validator('schedule')
    def validate_schedule(cls, v):
        valid_presets = ['@once', '@hourly', '@daily', '@weekly', '@monthly', '@yearly']
        if not (v in valid_presets or v.startswith('0 ') or v.count(' ') == 4):
            raise ValueError(f'Invalid schedule: {v}')
        return v

class DailyETL(Blueprint[DailyETLConfig]):
    """Daily ETL job that moves data between tables."""

    def render(self, config: DailyETLConfig) -> DAG:
        with DAG(
            dag_id=config.job_id,
            schedule=config.schedule,
            start_date=datetime(2024, 1, 1),
            catchup=False,
            default_args={"retries": config.retries}
        ) as dag:
            # Define your tasks here
            pass
        return dag
```

### Loading from YAML in Python

You can also load YAML configs in Python code:

```python
from blueprint import from_yaml

# Load existing YAML config
dag = from_yaml("configs/customer_etl.dag.yaml")

# Or with runtime overrides
dag = from_yaml("configs/customer_etl.dag.yaml", overrides={
    "retries": 5,
    "schedule": "@hourly"
})
```

### Testing Blueprints

```python
import pytest
from etl_blueprints import DailyETL

def test_daily_etl_config():
    # Test valid configuration
    dag = DailyETL.build(
        job_id="test-etl",
        source_table="test.source",
        target_table="test.target"
    )
    assert dag.dag_id == "test-etl"
    assert dag.schedule_interval == "@daily"

    # Test validation errors
    with pytest.raises(ValueError, match="Invalid schedule"):
        DailyETL.build(
            job_id="test-etl",
            source_table="test.source",
            target_table="test.target",
            schedule="invalid"
        )
```

## Type Safety and Validation

Blueprint uses Pydantic under the hood for robust validation with helpful error messages. This gives you:

- **Type coercion** - Automatically converts compatible types (e.g., string "5" to integer 5)
- **Field validation** - Set constraints like min/max values, regex patterns, etc.
- **Custom validators** - Add your own validation logic for complex rules
- **Clear error messages** - Know exactly what went wrong and how to fix it

When validation fails, you get clear feedback:

```bash
$ blueprint lint
✗ customer_etl.dag.yaml
  ValidationError: 3 validation errors for DailyETLConfig

  job_id
    String does not match pattern '^[a-zA-Z0-9_-]+$' (type=value_error.str.regex)
    Given: "customer sync!" (contains spaces)

  retries
    ensure this value is less than or equal to 5 (type=value_error.number.not_le)
    Given: 10

  schedule
    Invalid schedule format (type=value_error)
    Given: "every hour" (use "@hourly" or valid cron expression)
```

### Field Validation Examples

```python
from blueprint import BaseModel, Field, field_validator

class ETLConfig(BaseModel):
    # Basic constraints
    job_id: str = Field(pattern=r'^[a-zA-Z0-9_-]+$')
    retries: int = Field(ge=0, le=5)
    timeout_minutes: int = Field(gt=0, le=1440)  # 1-1440 minutes

    # Custom validation
    @field_validator('schedule')
    def validate_schedule(cls, v):
        valid_presets = ['@once', '@hourly', '@daily', '@weekly', '@monthly']
        if v not in valid_presets and not cls._is_valid_cron(v):
            raise ValueError(f'Must be a preset ({", ".join(valid_presets)}) or valid cron')
        return v
```

## More Examples

### Complex Parameters

Blueprints support nested objects and lists:

```python
from blueprint import BaseModel, Field
from typing import Optional, List

class SourceConfig(BaseModel):
    database: str = Field(description="Database connection name")
    table: str = Field(description="Table to extract data from")

class NotificationConfig(BaseModel):
    email: Optional[str] = Field(default=None, description="Email for notifications")
    slack: Optional[str] = Field(default=None, description="Slack channel (#data-alerts)")

class MultiSourceConfig(BaseModel):
    sources: List[SourceConfig] = Field(description="List of data sources")
    notifications: NotificationConfig = Field(default_factory=NotificationConfig)

class MultiSourceETL(Blueprint[MultiSourceConfig]):
    """ETL pipeline that processes multiple data sources in parallel."""

    def render(self, config: MultiSourceConfig) -> DAG:
        # Access nested data with type safety
        for source in config.sources:
            print(f"Processing {source.table} from {source.database}")
```

```yaml
blueprint: multi_source_etl
sources:
  - database: postgres
    table: users
  - database: mysql
    table: orders
notifications:
  email: data-team@company.com
  slack: "#data-alerts"
```

### Blueprint Inheritance

Use standard Python inheritance to share common parameters:

```python
class BaseETLConfig(BaseModel):
    owner: str = Field(default="data-team", description="Team responsible for DAG")
    retries: int = Field(default=2, ge=0, le=5, description="Number of retries")
    email_on_failure: str = Field(default="alerts@company.com", description="Alert email")

class S3ImportConfig(BaseETLConfig):
    bucket: str = Field(description="S3 bucket name")
    prefix: str = Field(description="S3 key prefix")

class BaseETL(Blueprint[BaseETLConfig]):
    """Base blueprint with common ETL parameters."""

    def get_default_args(self, config: BaseETLConfig):
        return {
            "owner": config.owner,
            "retries": config.retries,
            "email_on_failure": [config.email_on_failure]
        }

class S3Import(Blueprint[S3ImportConfig]):
    """Import data from S3."""

    def render(self, config: S3ImportConfig) -> DAG:
        # Has access to all BaseETLConfig fields plus S3-specific ones
        default_args = self.get_default_args(config)
        # ... create DAG with S3 operators
```

## Installation

```bash
pip install airflow-blueprint
```

## Configuration

Blueprint looks for templates in `blueprint/templates/` by default. Override with:

```bash
export BLUEPRINT_TEMPLATES_DIR=/path/to/templates
```

## CLI Commands

```bash
# Validate all configs
blueprint lint

# Validate specific config
blueprint lint dags/configs/my_job.dag.yaml

# List available blueprints
blueprint list

# Show blueprint parameters
blueprint describe daily_etl

# Interactive scaffolding (primary interface)
blueprint new
# Prompts for: DAG name, blueprint selection, parameters

# Direct scaffolding with specific blueprint
blueprint new daily_etl my_new_etl

# Scaffold to specific path
blueprint new dags/configs/prod/my_new_etl.dag.yaml daily_etl

# Quick mode with parameter overrides
blueprint new daily_etl my_new_etl --set job_id=customer-sync --set retries=3
```

## Error Messages

Blueprint provides clear, actionable error messages:

```bash
$ blueprint lint
✗ marketing_etl.dag.yaml
  Line 3: Missing required parameter 'source_table' for blueprint 'daily_etl'

  Your configuration:
    2 | blueprint: daily_etl
    3 | job_id: "marketing-sync"
    4 | target_table: "analytics.marketing_facts"

  Add the missing parameter:
    3 | job_id: "marketing-sync"
  + 4 | source_table: "raw.marketing_events"
    5 | target_table: "analytics.marketing_facts"
```

## Best Practices

1. **Keep blueprints focused** - Each blueprint should represent one type of workflow
2. **Use descriptive parameter names** - `source_table` is clearer than `src`
3. **Always add parameter descriptions** - Use `Annotated[type, "description"]` for all parameters
4. **Document your blueprints** - Add docstrings to blueprint classes explaining their purpose
5. **Provide defaults wisely** - Common values as defaults, critical values as required
6. **Validate in CI** - Add `blueprint lint` to your CI pipeline

## How is this different from DAG Factory?

[DAG Factory](https://github.com/astronomer/dag-factory) gives full control of Airflow via YAML.

Blueprint hides that complexity behind safe, pre-built templates with validation.

### DAG Factory

```yaml
my_dag:
  default_args:
    owner: 'data-team'
    retries: 2
    retry_delay_seconds: 300
  start_date: 2024-01-01
  schedule_interval: '@daily'
  tasks:
    extract_data:
      operator: airflow.operators.python.PythonOperator
      python_callable_name: extract_from_api
      python_callable_file: /opt/airflow/dags/etl/extract.py
    transform_data:
      operator: airflow.operators.python.PythonOperator
      dependencies: [extract_data]
      # ... many more Airflow-specific configurations
```

### Blueprint

```yaml
blueprint: daily_etl
job_id: customer-sync
source_table: raw.customers
target_table: analytics.dim_customers
schedule: "@hourly"
```

Or in Python:

```python
dag = DailyETL.build(
    job_id="customer-sync",
    source_table="raw.customers",
    target_table="analytics.dim_customers",
    schedule="@hourly"
)
```

**Use DAG Factory if:** You need full Airflow flexibility and your users understand Airflow concepts

**Use Blueprint if:** You want standardized, validated patterns with type safety for teams

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## License

Apache 2.0
