"""Blueprints used by both the production and the sandbox DAGs.

Step templates are unaffected by DAG args scoping: they are discovered for the
whole project, and the same step works under either template.
"""

from airflow.providers.standard.operators.bash import BashOperator

from blueprint import BaseModel, Blueprint, Field, TaskOrGroup


class ReadConfig(BaseModel):
    """Fields a YAML author may set on a `read` step."""

    dataset: str = Field(description="Dataset to read from")


class Read(Blueprint[ReadConfig]):
    """Read a dataset."""

    def render(self, config: ReadConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Reading {config.dataset}'",
        )


class WriteConfig(BaseModel):
    """Fields a YAML author may set on a `write` step."""

    table: str = Field(description="Destination table")


class Write(Blueprint[WriteConfig]):
    """Write a result table."""

    def render(self, config: WriteConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Writing {config.table}'",
        )
