"""Blueprints for the variables example.

Nothing here knows about variables or profiles. References are resolved before
a config model ever sees them, so a blueprint receives ordinary validated
values -- which is the point: the environment is a YAML concern, not a
template-authoring one.
"""

from airflow.providers.standard.operators.bash import BashOperator

from blueprint import BaseModel, Blueprint, Field, TaskOrGroup


class StageConfig(BaseModel):
    """Fields a YAML author may set on a `stage` step."""

    source_table: str = Field(description="Landing table to read, as schema.table")
    batch_size: int = Field(default=1000, ge=1, description="Rows to read per batch")


class Stage(Blueprint[StageConfig]):
    """Read a landing table into the warehouse."""

    def render(self, config: StageConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=(
                f"echo 'Staging {config.source_table} in batches of {config.batch_size}'"
            ),
        )


class MaterializeConfig(BaseModel):
    """Fields a YAML author may set on a `materialize` step."""

    target_table: str = Field(description="Destination table, as database.schema.table")
    expire_after_days: int = Field(ge=1, description="Days before partitions are dropped")
    post_hook: str | None = Field(default=None, description="Shell command to run after loading")


class Materialize(Blueprint[MaterializeConfig]):
    """Write a modelled table into the warehouse."""

    def render(self, config: MaterializeConfig) -> TaskOrGroup:
        command = (
            f"echo 'Materialising {config.target_table}, "
            f"expiring after {config.expire_after_days} days'"
        )
        if config.post_hook:
            command = f"{command} && {config.post_hook}"

        return BashOperator(task_id=self.step_id, bash_command=command)
