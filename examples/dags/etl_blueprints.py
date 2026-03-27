"""Example blueprint definitions for an ETL pipeline.

Demonstrates:
- Single-task blueprints (Load)
- Multi-task blueprints (Extract, Transform)
- Versioning with breaking changes (Extract v1 -> ExtractV2)
- Runtime params via self.param() and self.resolve_config() (Load)
"""

from airflow.decorators import task
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
                    f"echo 'Extracting {config.source_table} batch_size={config.batch_size}'"
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


# --- Load (runtime params demo, no versioning) ---
#
# Demonstrates both param access patterns:
#   - Template access: self.param() in operator template fields
#   - Variable access: self.resolve_config() inside @task


class LoadConfig(BaseModel):
    target_table: str = Field(description="Destination table")
    mode: str = Field(default="append", pattern="^(append|overwrite)$")


class Load(Blueprint[LoadConfig]):
    """Load data to target table.

    Config fields are automatically registered as Airflow params, so they
    can be overridden via the trigger form or ``dag_run.conf``.
    """

    supports_params = True

    def render(self, config: LoadConfig) -> TaskGroup:
        with TaskGroup(group_id=self.step_id) as group:
            run_load = BashOperator(
                task_id="run_load",
                bash_command=(
                    f"echo 'Loading to {self.param('target_table')} mode={self.param('mode')}'"
                ),
            )

            @task(task_id="verify_load")
            def verify_load(**context):
                cfg = self.resolve_config(config, context)
                print(f"Verifying {cfg.target_table} ({cfg.mode})")

            run_load >> verify_load()
        return group
