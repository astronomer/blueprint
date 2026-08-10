"""A blueprint whose config can be overridden when the DAG is triggered.

Backfill opts into runtime params and reads them two different ways.
Notify deliberately does not opt in -- see the README for why that matters.
"""

from typing import Literal

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import TaskGroup, task

from blueprint import BaseModel, Blueprint, Field, TaskOrGroup


class BackfillConfig(BaseModel):
    target_table: str = Field(description="Table to rebuild")

    # `format` controls how Airflow's trigger form renders the field: a date
    # picker here, a textarea for the query below.
    start_date: str = Field(
        default="2024-01-01",
        description="First day to rebuild, inclusive",
        json_schema_extra={"format": "date"},
    )
    end_date: str = Field(
        default="2024-01-31",
        description="Last day to rebuild, inclusive",
        json_schema_extra={"format": "date"},
    )
    query: str = Field(
        default="SELECT * FROM raw.events",
        description="SQL used to rebuild the partition",
        json_schema_extra={"format": "multiline"},
    )

    # Literal renders as a dropdown; values_display gives each option a label.
    warehouse_size: Literal["xsmall", "small", "medium", "large"] = Field(
        default="small",
        description="Warehouse to run the rebuild on",
        json_schema_extra={
            "values_display": {
                "xsmall": "X-Small (cheapest)",
                "small": "Small",
                "medium": "Medium",
                "large": "Large (fastest)",
            }
        },
    )
    dry_run: bool = Field(default=True, description="Plan the rebuild without writing")


class Backfill(Blueprint[BackfillConfig]):
    """Rebuild a table over a date range. Overridable at trigger time."""

    # Registers every BackfillConfig field as an Airflow DAG param, namespaced
    # as {step}__{field}. Only opt in if render() actually reads the params --
    # otherwise the trigger form offers fields that silently do nothing.
    supports_params = True

    def render(self, config: BackfillConfig) -> TaskOrGroup:
        with TaskGroup(group_id=self.step_id) as group:
            # self.param() returns a Jinja2 string that Airflow resolves at
            # execution time, so this picks up trigger-time overrides. It only
            # works in fields the operator declares as templated.
            plan = BashOperator(
                task_id="plan",
                bash_command=(
                    f"echo 'Planning rebuild of {self.param('target_table')} "
                    f"from {self.param('start_date')} to {self.param('end_date')} "
                    f"on a {self.param('warehouse_size')} warehouse'"
                ),
            )

            @task(task_id="execute")
            def execute(**context):
                # resolve_config() merges the runtime params back into the
                # Pydantic model, so Python logic sees real typed values and
                # every validator runs again.
                resolved = self.resolve_config(config, context)
                if resolved.dry_run:
                    print(f"DRY RUN: would rebuild {resolved.target_table}")
                    return
                print(
                    f"Rebuilding {resolved.target_table} "
                    f"[{resolved.start_date}..{resolved.end_date}] "
                    f"on {resolved.warehouse_size}: {resolved.query}"
                )

            plan >> execute()
        return group


class NotifyConfig(BaseModel):
    channel: str = Field(description="Channel to post to")


class Notify(Blueprint[NotifyConfig]):
    """Post a notification. Config is fixed at DAG-parse time."""

    # No supports_params: render() bakes the value in, so exposing `channel`
    # on the trigger form would be a lie.
    def render(self, config: NotifyConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Notifying {config.channel}'",
        )
