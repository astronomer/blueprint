"""Blueprint definitions for integration testing.

Covers: single-task, multi-task, versioning (v1/v2), dependency chaining,
and a custom BlueprintDagArgs template for DAG-level arguments.
"""

from datetime import timedelta
from typing import Any, Literal

from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from pydantic import ConfigDict

from blueprint import BaseModel, Blueprint, BlueprintDagArgs, Field


# --- Custom DAG arguments template ---
#
# Instead of exposing raw Airflow kwargs (retries, retry_delay, owner, tags),
# this template abstracts them behind project-level concepts:
#   - team:  identifies the owning team (becomes owner + auto-tag)
#   - tier:  "critical" | "standard" | "experimental" — controls retry behavior
#
# It also injects a project-wide on_success_callback that records completed
# tasks to an Airflow Variable, proving that DagArgs can inject non-serializable
# values (functions) and that they actually execute at runtime.


TIER_RETRIES = {"critical": 3, "standard": 1, "experimental": 0}
TIER_RETRY_DELAY_SECONDS = {"critical": 300, "standard": 60, "experimental": 0}

CALLBACK_VARIABLE_PREFIX = "blueprint_callback_"


def _on_task_success(context):
    """Project-wide success callback: records each completed task to an Airflow Variable."""
    from airflow.models import Variable

    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    key = f"{CALLBACK_VARIABLE_PREFIX}{dag_id}"
    existing = Variable.get(key, default_var="")
    tasks = existing.split(",") if existing else []
    tasks.append(task_id)
    Variable.set(key, ",".join(tasks))


class ProjectDagArgsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schedule: str | None = None
    description: str | None = None
    team: str = "platform"
    tier: Literal["critical", "standard", "experimental"] = "standard"


class ProjectDagArgs(BlueprintDagArgs[ProjectDagArgsConfig]):
    """Project-wide DAG argument template for integration tests.

    Translates high-level team/tier config into Airflow DAG kwargs:
      team  -> default_args.owner, auto-generated "team:<name>" tag
      tier  -> default_args.retries, default_args.retry_delay, tier tag

    Also injects _on_task_success as on_success_callback for all tasks.
    """

    def render(self, config: ProjectDagArgsConfig) -> dict[str, Any]:
        retries = TIER_RETRIES[config.tier]
        retry_delay = timedelta(seconds=TIER_RETRY_DELAY_SECONDS[config.tier])

        kwargs: dict[str, Any] = {
            "tags": [f"team:{config.team}", config.tier],
            "default_args": {
                "owner": config.team,
                "retries": retries,
                "retry_delay": retry_delay,
                "on_success_callback": _on_task_success,
            },
        }
        if config.schedule is not None:
            kwargs["schedule"] = config.schedule
        if config.description is not None:
            kwargs["description"] = config.description
        return kwargs


# --- Extract v1: single source table as a string ---


class ExtractConfig(BaseModel):
    source_table: str = Field(description="Source table (schema.table)")
    batch_size: int = Field(default=1000, ge=1)


class Extract(Blueprint[ExtractConfig]):
    """Extract data from a single database table (v1)."""

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
    """Extract data from multiple sources with optional filtering (v2)."""

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
