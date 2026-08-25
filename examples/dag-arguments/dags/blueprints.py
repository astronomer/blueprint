"""Ordinary blueprints. The interesting part of this example is dag_args.py."""

from airflow.providers.standard.operators.bash import BashOperator

from blueprint import BaseModel, Blueprint, Field, TaskOrGroup


class ExtractConfig(BaseModel):
    source: str = Field(description="Source system to read from")


class Extract(Blueprint[ExtractConfig]):
    """Read from a source system."""

    def render(self, config: ExtractConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Extracting {config.source}'",
        )


class LoadConfig(BaseModel):
    target: str = Field(description="Destination table")


class Load(Blueprint[LoadConfig]):
    """Write to a destination table."""

    def render(self, config: LoadConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Loading into {config.target}'",
        )
