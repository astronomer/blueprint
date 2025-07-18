"""Example of using Blueprint's Python API to create DAGs dynamically."""

# Import the template loader from the blueprint package
from blueprint import load_template

# Load templates dynamically
DailyETL = load_template("daily_etl", "DailyETL")
MultiSourceETL = load_template("multi_source_etl", "MultiSourceETL")

# Example 1: Simple DAG creation
customer_etl_python_dag = DailyETL.build(
    job_id="customer-etl-python",
    source_table="raw.customers",
    target_table="staging.customers_clean",
    schedule="@hourly",
    retries=2,
)

# Example 2: Dynamic DAG generation based on configuration
TABLES_TO_SYNC = [
    {"source": "raw.users", "target": "staging.users", "schedule": "@daily"},
    {"source": "raw.products", "target": "staging.products", "schedule": "@hourly"},
    {"source": "raw.orders", "target": "staging.orders", "schedule": "@hourly"},
]

# Generate a DAG for each table configuration
for table_config in TABLES_TO_SYNC:
    table_name = table_config["source"].split(".")[1]

    dag = DailyETL.build(
        job_id=f"{table_name}_sync",
        source_table=table_config["source"],
        target_table=table_config["target"],
        schedule=table_config["schedule"],
        retries=3,
    )

    # The DAG is automatically registered with Airflow
    globals()[f"{table_name}_sync_dag"] = dag

# Example 3: Multi-source ETL
unified_analytics_dag = MultiSourceETL.build(
    job_id="unified-analytics",
    source_tables=["staging.users", "staging.products", "staging.orders"],
    target_table="analytics.unified_view",
    schedule="0 2 * * *",  # 2 AM daily
    parallel=True,
)
