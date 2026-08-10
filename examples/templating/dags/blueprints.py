"""Blueprints that pass their config straight through to a command.

Nothing here knows about templating. Values arrive as ordinary strings; some
of them happen to be Airflow template expressions that resolve later.
"""

# Blueprints run unchanged on Airflow 2 and 3 -- only the import paths differ.
try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2
    from airflow.operators.bash import BashOperator

from blueprint import BaseModel, Blueprint, Field, TaskOrGroup


class ExtractConfig(BaseModel):
    source: str = Field(description="Source system to read from")
    partition: str = Field(description="Partition being read")
    output_path: str = Field(description="Where the extract is written")


class Extract(Blueprint[ExtractConfig]):
    """Read one partition from a source system."""

    def render(self, config: ExtractConfig) -> TaskOrGroup:
        # bash_command is a templated field, so any Airflow macros still
        # present in these values are resolved when the task runs.
        return BashOperator(
            task_id=self.step_id,
            bash_command=(
                f"echo 'Extracting {config.source} partition {config.partition} "
                f"to {config.output_path}'"
            ),
        )


class LoadConfig(BaseModel):
    target: str = Field(description="Destination table")
    label: str = Field(description="Human-readable label for the run")


class Load(Blueprint[LoadConfig]):
    """Write to a destination table."""

    def render(self, config: LoadConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Loading into {config.target} ({config.label})'",
        )
