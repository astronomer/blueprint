"""Two blueprints, each with two versions, named two different ways.

`extract` takes its name and version from the class names Extract / ExtractV2.
`load` sets name and version explicitly, so the class names are free to
describe the implementation instead of the registry entry.
"""

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import TaskGroup

from blueprint import BaseModel, Blueprint, Field, TaskOrGroup


# --- extract v1: one table per step -----------------------------------------


class ExtractConfig(BaseModel):
    source_table: str = Field(description="Table to read, as schema.table")
    batch_size: int = Field(default=1000, ge=1)


class Extract(Blueprint[ExtractConfig]):
    """Read a single source table."""

    def render(self, config: ExtractConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=(
                f"echo 'Extracting {config.source_table} in batches of {config.batch_size}'"
            ),
        )


# --- extract v2: many tables per step ---------------------------------------
#
# A breaking change: source_table is gone, replaced by a list of nested models.
# v2 is a new class with its own config, so v1 keeps working untouched.


class Source(BaseModel):
    schema_name: str = Field(description="Schema the table lives in")
    table: str = Field(description="Table to read")


class ExtractV2Config(BaseModel):
    sources: list[Source] = Field(description="One or more tables to read")
    batch_size: int = Field(default=1000, ge=1)


class ExtractV2(Blueprint[ExtractV2Config]):
    """Read several source tables in one step."""

    def render(self, config: ExtractV2Config) -> TaskOrGroup:
        with TaskGroup(group_id=self.step_id) as group:
            for source in config.sources:
                BashOperator(
                    task_id=f"{source.schema_name}_{source.table}",
                    bash_command=(
                        f"echo 'Extracting {source.schema_name}.{source.table} "
                        f"in batches of {config.batch_size}'"
                    ),
                )
        return group


# --- load v1 and v2: explicit name and version ------------------------------
#
# Neither class name says "load", so both set `name` explicitly. Without it
# these would register as `single_statement_loader` and `bulk_copy_loader`.


class SingleStatementLoadConfig(BaseModel):
    target_table: str = Field(description="Table to write to")


class SingleStatementLoader(Blueprint[SingleStatementLoadConfig]):
    """Load rows with one INSERT ... SELECT statement."""

    name = "load"
    version = 1

    def render(self, config: SingleStatementLoadConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'INSERT INTO {config.target_table}'",
        )


class BulkCopyLoadConfig(BaseModel):
    target_table: str = Field(description="Table to write to")
    stage: str = Field(description="Object storage stage to copy from")


class BulkCopyLoader(Blueprint[BulkCopyLoadConfig]):
    """Load rows with a bulk COPY from object storage."""

    name = "load"
    version = 2

    def render(self, config: BulkCopyLoadConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'COPY INTO {config.target_table} FROM {config.stage}'",
        )
