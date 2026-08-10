"""Standard ingestion template, maintained by the platform team."""

from typing import Literal

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import TaskGroup

from acme_blueprints.utils import sla_minutes
from blueprint import BaseModel, Blueprint, Field, TaskOrGroup


class IngestConfig(BaseModel):
    source: str = Field(description="Registered source system name")
    dataset: str = Field(description="Dataset to ingest from that source")
    tier: Literal["gold", "silver", "bronze"] = Field(
        default="bronze", description="Service tier; determines the expected SLA"
    )


class Ingest(Blueprint[IngestConfig]):
    """Ingest a dataset from a registered source into the landing zone."""

    def render(self, config: IngestConfig) -> TaskOrGroup:
        # Imported from a module outside the entry point target.
        sla = sla_minutes(config.tier)

        with TaskGroup(group_id=self.step_id) as group:
            pull = BashOperator(
                task_id="pull",
                bash_command=(
                    f"echo 'Pulling {config.dataset} from {config.source} "
                    f"(SLA {sla}m)'"
                ),
            )
            land = BashOperator(
                task_id="land",
                bash_command=f"echo 'Landing {config.dataset} in raw.{config.dataset}'",
            )
            pull >> land
        return group
