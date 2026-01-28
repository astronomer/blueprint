from datetime import datetime, timedelta, timezone

from blueprint import BaseModel, Blueprint, Field, field_validator

CRON_EXPRESSION_MIN_SPACES = 4


class DailyETLConfig(BaseModel):
    """Configuration for daily ETL jobs."""

    job_id: str = Field(
        pattern=r"^[a-zA-Z0-9_-]+$",
        description="Unique identifier for this job (used as DAG ID)",
    )
    source_table: str = Field(description="Table to read data from (schema.table)")
    target_table: str = Field(description="Table to write processed data to (schema.table)")
    schedule: str = Field(
        default="@daily",
        description="Cron expression or Airflow preset (@daily, @hourly, etc.)",
    )
    retries: int = Field(
        default=2,
        ge=0,
        le=5,
        description="Number of retry attempts on task failure",
    )

    @field_validator("schedule")
    @classmethod
    def validate_schedule(cls, v):
        valid_presets = ["@once", "@hourly", "@daily", "@weekly", "@monthly", "@yearly"]
        if v not in valid_presets and not (v.count(" ") >= CRON_EXPRESSION_MIN_SPACES):
            msg = f"Schedule must be one of {valid_presets} or a valid cron expression"
            raise ValueError(msg)
        return v

    @field_validator("source_table", "target_table")
    @classmethod
    def validate_table_name(cls, v):
        if "." not in v:
            msg = f"Table name must include schema (e.g., raw.customers): {v}"
            raise ValueError(msg)
        return v


class DailyETL(Blueprint[DailyETLConfig]):
    """Daily ETL job that moves data between tables with configurable scheduling.

    This blueprint creates a DAG with the following tasks:
    1. check_source_data - Verify source table has data
    2. extract_transform - Extract and transform the data
    3. load_data - Load into target table
    4. data_quality_check - Run quality checks
    """

    def render(self, config: DailyETLConfig):
        from airflow import DAG
        from airflow.decorators import task
        from airflow.operators.bash import BashOperator

        default_args = {
            "owner": "data-team",
            "retries": config.retries,
            "retry_delay": timedelta(minutes=5),
            "email_on_failure": False,
        }

        dag = DAG(
            dag_id=config.job_id,
            default_args=default_args,
            description=f"ETL from {config.source_table} to {config.target_table}",
            schedule=config.schedule,
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            catchup=False,
            tags=["etl", "blueprint"],
        )

        with dag:
            check_source = BashOperator(
                task_id="check_source_data",
                bash_command=f'echo "Checking if {config.source_table} has data..."',
            )

            @task
            def extract_transform() -> dict[str, int]:
                """Extract data from source table and apply transformations."""
                print(f"Extracting data from {config.source_table}")
                print("Applying transformations...")
                print(f"Preparing to load into {config.target_table}")
                return {"records_processed": 1000}

            etl_result = extract_transform()

            load_data = BashOperator(
                task_id="load_data",
                bash_command=f'echo "Loading data into {config.target_table}..."',
            )

            quality_check = BashOperator(
                task_id="data_quality_check",
                bash_command=f'echo "Running quality checks on {config.target_table}..."',
            )

            check_source >> etl_result >> load_data >> quality_check

        return dag


# ==============================================================================
# Auto-discover and build DAGs from YAML files
# ==============================================================================
# This looks for *.dag.yaml files in dags/ with `blueprint: daily_etl`
DailyETL.build_all()


# ==============================================================================
# Programmatic DAG creation with build()
# ==============================================================================
# You can also create DAGs directly in Python without YAML files.
# This is useful for dynamic DAG generation or when you prefer code over config.

# Example 1: Simple DAG creation
DailyETL.build(
    job_id="customer-etl-python",
    source_table="raw.customers",
    target_table="staging.customers_clean",
    schedule="@hourly",
    retries=2,
)

# Example 2: Dynamic DAG generation from a list of configurations
TABLES_TO_SYNC = [
    {"source": "raw.users", "target": "staging.users", "schedule": "@daily"},
    {"source": "raw.products", "target": "staging.products", "schedule": "@hourly"},
    {"source": "raw.orders", "target": "staging.orders", "schedule": "@hourly"},
]

for table_config in TABLES_TO_SYNC:
    table_name = table_config["source"].split(".")[1]
    DailyETL.build(
        job_id=f"{table_name}-sync",
        source_table=table_config["source"],
        target_table=table_config["target"],
        schedule=table_config["schedule"],
        retries=3,
    )

# Example 3: Load YAML with runtime overrides using build_from_yaml()
# Useful for environment-specific adjustments or testing with modified configs
DailyETL.build_from_yaml(
    "customer_etl.dag.yaml",
    overrides={
        "job_id": "customer-etl-override",  # Different DAG ID to avoid conflict
        "retries": 5,
        "schedule": "@hourly",
    },
)
