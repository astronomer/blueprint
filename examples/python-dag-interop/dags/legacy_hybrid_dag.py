"""An existing hand-written Airflow DAG, partly migrated to blueprints.

The bespoke tasks around the edges stay as they are. The two steps that follow
a standard pattern are rendered from blueprints instead of being hand-rolled.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from blueprints import Extract, ExtractConfig, Load, LoadConfig

with DAG(
    dag_id="legacy_hybrid",
    description="Hand-written DAG with two blueprint-rendered steps",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    # An ordinary operator, exactly as it was before the migration.
    acquire_lock = BashOperator(
        task_id="acquire_lock",
        bash_command="echo 'Taking the legacy pipeline lock'",
    )

    # A blueprint used directly: instantiate, set step_id, call render().
    # step_id determines the task_id or group_id it renders under.
    extract = Extract()
    extract.step_id = "extract_customers"
    extract_group = extract.render(ExtractConfig(source="crm", batch_size=500))

    load = Load()
    load.step_id = "load_customers"
    load_task = load.render(LoadConfig(target="warehouse.customers"))

    # More bespoke work that has no blueprint yet.
    reconcile = BashOperator(
        task_id="reconcile_legacy_totals",
        bash_command="echo 'Reconciling against the legacy ledger'",
    )
    release_lock = BashOperator(
        task_id="release_lock",
        bash_command="echo 'Releasing the legacy pipeline lock'",
        trigger_rule="all_done",
    )

    # render() returns a normal BaseOperator or TaskGroup, so the usual
    # dependency operators work on it.
    acquire_lock >> extract_group >> load_task >> reconcile >> release_lock
