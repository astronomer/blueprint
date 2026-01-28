# Blueprint Examples

This directory contains examples of using Blueprint to create Airflow DAGs.

## Quick Start

1. **Start Airflow with Tilt (from root directory):**
   ```bash
   # Install Tilt if you haven't already
   # https://tilt.dev/

   # From the root directory (not examples/)
   tilt up
   ```

2. **Access Airflow UI:**
   - URL: http://localhost:8080
   - No authentication required (SimpleAuthManager with all admins enabled)

3. **View the example DAGs:**
   - `customer-daily-sync` - Daily ETL from YAML config
   - `sales-analytics-pipeline` - Multi-source ETL from YAML config
   - DAGs from `templated_etl.dag.yaml` (if Airflow Variables are set)

## Directory Structure

The recommended pattern puts Blueprint templates **directly in dags/** so they're visible in Airflow's UI Code view:

```
examples/
├── dags/
│   ├── daily_etl.py              # Blueprint template + build_all() + build() examples
│   ├── customer_etl.dag.yaml     # YAML config for customer ETL
│   ├── templated_etl.dag.yaml    # YAML config with Jinja2 templating
│   ├── multi_source_etl.py       # Another Blueprint template
│   └── sales_analytics.dag.yaml  # YAML config for sales analytics
├── blueprint.toml                # Blueprint CLI configuration
├── docker-compose.yaml
├── Dockerfile
└── requirements.txt
```

## UI-Visible Blueprint Pattern

### 1. Create a Blueprint Template (in dags/)

```python
# dags/daily_etl.py
from datetime import datetime, timezone
from blueprint import BaseModel, Blueprint, Field

class DailyETLConfig(BaseModel):
    """Configuration for daily ETL jobs."""
    job_id: str = Field(description="Unique identifier for this job")
    source_table: str = Field(description="Table to read from")
    target_table: str = Field(description="Table to write to")
    schedule: str = Field(default="@daily")

class DailyETL(Blueprint[DailyETLConfig]):
    """Daily ETL job - visible in Airflow UI Code view."""

    def render(self, config: DailyETLConfig):
        from airflow import DAG
        from airflow.operators.bash import BashOperator

        dag = DAG(
            dag_id=config.job_id,
            schedule=config.schedule,
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        with dag:
            # ... define tasks ...
        return dag

# Auto-discover and build DAGs from *.dag.yaml files
DailyETL.build_all()

# Programmatic DAG creation (alternative to YAML)
DailyETL.build(
    job_id="customer-etl-python",
    source_table="raw.customers",
    target_table="staging.customers",
    schedule="@hourly",
)

# Dynamic DAG generation
for table in ["users", "products", "orders"]:
    DailyETL.build(
        job_id=f"{table}-sync",
        source_table=f"raw.{table}",
        target_table=f"staging.{table}",
    )
```

### 2. Create YAML Config Files (in dags/)

```yaml
# dags/customer_etl.dag.yaml
blueprint: daily_etl

job_id: customer-daily-sync
source_table: raw.customers
target_table: analytics.dim_customers
schedule: "@daily"
```

### 3. Jinja2 Templating Support

YAML configs support Jinja2 templating for dynamic values:

```yaml
# dags/templated_etl.dag.yaml
blueprint: daily_etl

# Environment variables
job_id: "{{ env.ENV }}-orders-etl"

# Airflow Variables
source_table: "{{ var.value.source_schema }}.orders"
target_table: "{{ var.value.target_schema }}.orders_clean"

# With default values
schedule: "{{ var.value.etl_schedule | default('@daily') }}"
```

## Benefits of the UI-Visible Pattern

1. **Full Code Visibility**: View the complete Blueprint code in Airflow's UI Code tab
2. **Self-Documenting**: Pydantic config model shows the schema right in the code
3. **Auto-Discovery**: `build_all()` finds all `.dag.yaml` files that reference the blueprint
4. **Rendered Config View**: Select a task instance and view "Rendered Templates" to see resolved YAML values

## Creating New DAG Configs

Use the CLI to interactively create a new YAML configuration:

```bash
blueprint new
```

This prompts you to:
1. Select a Blueprint template
2. Enter values for each parameter
3. Validates the configuration
4. Creates a `.dag.yaml` file

## Development with Tilt

Tilt provides a great development experience with:
- **Hot Reload**: Changes to Blueprint source code automatically reload
- **Live Sync**: Your local changes sync instantly to Airflow
- **Build Logs**: Real-time feedback on builds

### Managing the Environment

```bash
# Start development environment
tilt up

# Stop and clean up
tilt down

# View logs
tilt logs

# Open Tilt UI
tilt ui  # or visit http://localhost:10350
```

## Next Steps

- Explore `daily_etl.py` and `multi_source_etl.py` for full Blueprint examples with both YAML and Python API usage
- Try the templating features in `templated_etl.dag.yaml`
- Run `blueprint new` to create your own DAG configuration
- Check out the main README for more documentation
