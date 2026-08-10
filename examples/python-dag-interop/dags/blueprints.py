"""Blueprints used from YAML and from a hand-written Python DAG alike.

Nothing here is aware of how it will be called.
"""

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import TaskGroup

from blueprint import BaseModel, Blueprint, Field, TaskOrGroup


class ExtractConfig(BaseModel):
    source: str = Field(description="Source system to read from")
    batch_size: int = Field(default=1000, ge=1)


class Extract(Blueprint[ExtractConfig]):
    """Validate a source, then read from it."""

    def render(self, config: ExtractConfig) -> TaskOrGroup:
        with TaskGroup(group_id=self.step_id) as group:
            validate = BashOperator(
                task_id="validate",
                bash_command=f"echo 'Checking {config.source} is reachable'",
            )
            fetch = BashOperator(
                task_id="fetch",
                bash_command=(
                    f"echo 'Reading {config.source} in batches of {config.batch_size}'"
                ),
            )
            validate >> fetch
        return group


class LoadConfig(BaseModel):
    target: str = Field(description="Destination table")


class Load(Blueprint[LoadConfig]):
    """Write to a destination table."""

    def render(self, config: LoadConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Loading into {config.target}'",
        )
