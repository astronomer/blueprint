"""Blueprint definitions for a simple ETL pipeline.

Extract returns a TaskGroup; Load returns a single operator.
"""

from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from blueprint import BaseModel, Blueprint, Field, TaskOrGroup


class ExtractConfig(BaseModel):
    source: str
    batch_size: int = Field(default=1000, ge=1)


class Extract(Blueprint[ExtractConfig]):
    """Pull data from a source system."""

    def render(self, config: ExtractConfig) -> TaskOrGroup:
        with TaskGroup(group_id=self.step_id) as group:
            BashOperator(
                task_id="validate",
                bash_command=f"echo 'Validating {config.source}'",
            )
            BashOperator(
                task_id="extract",
                bash_command=(
                    f"echo 'Extracting from {config.source} "
                    f"(batch_size={config.batch_size})'"
                ),
            )
        return group


class LoadConfig(BaseModel):
    target: str


class Load(Blueprint[LoadConfig]):
    """Load data into a destination."""

    def render(self, config: LoadConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Loading into {config.target}'",
        )
