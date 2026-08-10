"""Blueprints whose schemas are exported for editor validation."""

# Blueprints run unchanged on Airflow 2 and 3 -- only the import paths differ.
try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2
    from airflow.operators.bash import BashOperator

from typing import Literal

from blueprint import BaseModel, Blueprint, ConfigDict, Field, TaskOrGroup


class ExtractConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str = Field(description="Source system to read from")
    batch_size: int = Field(default=1000, ge=1, le=100_000, description="Rows per batch")


class Extract(Blueprint[ExtractConfig]):
    """Read from a source system."""

    def render(self, config: ExtractConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Extracting {config.source} ({config.batch_size})'",
        )


class LoadConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target: str = Field(description="Destination table")
    mode: Literal["append", "overwrite"] = Field(
        default="append", description="How rows are written"
    )


class Load(Blueprint[LoadConfig]):
    """Write to a destination table."""

    def render(self, config: LoadConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Loading into {config.target} ({config.mode})'",
        )
