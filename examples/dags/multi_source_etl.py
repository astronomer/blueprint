from datetime import datetime, timezone

from blueprint import BaseModel, Blueprint, Field


class MultiSourceETLConfig(BaseModel):
    """Configuration for ETL jobs that process multiple sources."""

    job_id: str = Field(
        pattern=r"^[a-zA-Z0-9_-]+$",
        description="Unique identifier for this job (used as DAG ID)",
    )
    source_tables: list[str] = Field(
        description="List of tables to read data from (schema.table format)",
    )
    target_table: str = Field(
        description="Table to write combined data to (schema.table format)",
    )
    schedule: str = Field(
        default="@daily",
        description="Cron expression or Airflow preset",
    )
    parallel: bool = Field(
        default=True,
        description="Whether to process source tables in parallel",
    )


class MultiSourceETL(Blueprint[MultiSourceETLConfig]):
    """ETL job that combines data from multiple sources.

    This blueprint creates a DAG that:
    1. Extracts data from each source table (parallel or sequential)
    2. Combines all source data
    3. Loads into the target table
    """

    def render(self, config: MultiSourceETLConfig):
        from airflow import DAG
        from airflow.operators.bash import BashOperator

        dag = DAG(
            dag_id=config.job_id,
            description=f"Combine {len(config.source_tables)} sources into {config.target_table}",
            schedule=config.schedule,
            start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
            catchup=False,
            tags=["etl", "blueprint", "multi-source"],
        )

        with dag:
            # Create extract tasks for each source
            extract_tasks = []
            for source_table in config.source_tables:
                task = BashOperator(
                    task_id=f"extract_{source_table.replace('.', '_')}",
                    bash_command=f'echo "Extracting from {source_table}..."',
                )
                extract_tasks.append(task)

            # Combine task
            combine = BashOperator(
                task_id="combine_sources",
                bash_command='echo "Combining all source data..."',
            )

            # Load task
            load = BashOperator(
                task_id="load_combined_data",
                bash_command=f'echo "Loading into {config.target_table}..."',
            )

            # Set up dependencies based on parallel flag
            if config.parallel:
                # All extract tasks run in parallel, then combine, then load
                extract_tasks >> combine >> load
            else:
                # Extract tasks run sequentially
                for i in range(len(extract_tasks) - 1):
                    extract_tasks[i] >> extract_tasks[i + 1]
                if extract_tasks:
                    extract_tasks[-1] >> combine >> load

        return dag


# ==============================================================================
# Auto-discover and build DAGs from YAML files
# ==============================================================================
# This looks for *.dag.yaml files in dags/ with `blueprint: multi_source_etl`
MultiSourceETL.build_all()


# ==============================================================================
# Programmatic DAG creation with build()
# ==============================================================================
# You can also create DAGs directly in Python without YAML files.

MultiSourceETL.build(
    job_id="unified-analytics",
    source_tables=["staging.users", "staging.products", "staging.orders"],
    target_table="analytics.unified_view",
    schedule="0 2 * * *",  # 2 AM daily
    parallel=True,
)
