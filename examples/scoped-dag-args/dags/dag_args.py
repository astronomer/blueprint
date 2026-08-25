"""DAG-level standards for the whole project.

This template scopes the directory holding this file -- dags/ -- and every
directory below it, unless one of those defines its own.
"""

from datetime import timedelta
from typing import Any

from blueprint import BaseModel, BlueprintDagArgs, Field


class ProjectDagArgsConfig(BaseModel):
    """Top-level fields a production DAG YAML may set."""

    schedule: str | None = Field(default=None, description="Cron or preset schedule")
    description: str | None = Field(default=None, description="Shown in the Airflow UI")

    team: str = Field(
        pattern=r"^[a-z][a-z0-9-]*$", description="Owning team; required for production DAGs"
    )
    sla_minutes: int = Field(default=60, ge=5, description="Alert if a run exceeds this")


class ProjectDagArgs(BlueprintDagArgs[ProjectDagArgsConfig]):
    """Production standards: an owning team, retries and failure alerting."""

    def render(self, config: ProjectDagArgsConfig) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "catchup": False,
            "max_active_runs": 1,
            "tags": [f"team:{config.team}", "tier:production"],
            "dagrun_timeout": timedelta(minutes=config.sla_minutes),
            "default_args": {
                "owner": config.team,
                "retries": 3,
                "retry_delay": timedelta(minutes=5),
                "email_on_failure": True,
            },
        }

        if config.schedule is not None:
            kwargs["schedule"] = config.schedule
        if config.description is not None:
            kwargs["description"] = config.description

        return kwargs
