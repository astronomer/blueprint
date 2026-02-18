"""Example blueprint definitions for an ETL pipeline.

Demonstrates:
- Single-task blueprints (Load)
- Multi-task blueprints (Extract, Transform)
- Versioning with breaking changes (Extract v1 -> ExtractV2)
"""

from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from blueprint import BaseModel, Blueprint, Field


# --- Extract v1: single source table as a string ---


class ExtractConfig(BaseModel):
    source_table: str = Field(description="Source table (schema.table)")
    batch_size: int = Field(default=1000, ge=1)


class Extract(Blueprint[ExtractConfig]):
    """Extract data from a single database table."""

    def render(self, config: ExtractConfig) -> TaskGroup:
        with TaskGroup(group_id=self.step_id) as group:
            BashOperator(
                task_id="validate",
                bash_command=f"echo 'Validating {config.source_table}'",
            )
            BashOperator(
                task_id="extract",
                bash_command=(
                    f"echo 'Extracting {config.source_table} "
                    f"batch_size={config.batch_size}'"
                ),
            )
        return group


# --- Extract v2: breaking change -- replaced source_table with sources list ---


class SourceDef(BaseModel):
    schema_name: str
    table: str
    filter: str | None = None


class ExtractV2Config(BaseModel):
    sources: list[SourceDef] = Field(description="List of source definitions")
    parallel: bool = Field(default=True)


class ExtractV2(Blueprint[ExtractV2Config]):
    """Extract data from multiple sources with optional filtering."""

    def render(self, config: ExtractV2Config) -> TaskGroup:
        with TaskGroup(group_id=self.step_id) as group:
            for src in config.sources:
                fqn = f"{src.schema_name}.{src.table}"
                BashOperator(
                    task_id=f"extract_{src.table}",
                    bash_command=f"echo 'Extracting {fqn} (filter={src.filter})'",
                )
        return group


# --- Transform (no versioning needed) ---


class TransformConfig(BaseModel):
    operations: list[str] = Field(default=["clean"])


class Transform(Blueprint[TransformConfig]):
    """Apply transformations to extracted data."""

    def render(self, config: TransformConfig) -> TaskGroup:
        with TaskGroup(group_id=self.step_id) as group:
            prev = None
            for op in config.operations:
                task = BashOperator(task_id=op, bash_command=f"echo 'Running {op}'")
                if prev:
                    prev >> task
                prev = task
        return group


# --- Load (single-task blueprint, no versioning) ---


class LoadConfig(BaseModel):
    target_table: str
    mode: str = Field(default="append", pattern="^(append|overwrite)$")


class Load(Blueprint[LoadConfig]):
    """Load data to target table."""

    def render(self, config: LoadConfig) -> BashOperator:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Loading to {config.target_table} ({config.mode})'",
        )
