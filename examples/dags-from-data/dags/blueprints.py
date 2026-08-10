"""Blueprints reused across every generated DAG."""

# Blueprints run unchanged on Airflow 2 and 3 -- only the import paths differ.
try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.sdk import TaskGroup
except ImportError:  # Airflow 2
    from airflow.operators.bash import BashOperator
    from airflow.utils.task_group import TaskGroup

from blueprint import BaseModel, Blueprint, Field, TaskOrGroup


class ExtractConfig(BaseModel):
    source: str = Field(description="Source system to read from")
    datasets: list[str] = Field(description="Datasets to pull from that source")


class Extract(Blueprint[ExtractConfig]):
    """Read a set of datasets from one tenant's source system."""

    def render(self, config: ExtractConfig) -> TaskOrGroup:
        with TaskGroup(group_id=self.step_id) as group:
            for dataset in config.datasets:
                BashOperator(
                    task_id=dataset,
                    bash_command=f"echo 'Extracting {dataset} from {config.source}'",
                )
        return group


class LoadConfig(BaseModel):
    target_schema: str = Field(description="Schema to write into")


class Load(Blueprint[LoadConfig]):
    """Write into a tenant's schema."""

    def render(self, config: LoadConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Loading into {config.target_schema}'",
        )
