"""DAG-level standards for prototypes under dags/sandbox/.

Defining a template here overrides the project one for this directory. The two
config models are unrelated: this is a different set of allowed fields, not a
set of different defaults.
"""

from datetime import timedelta
from typing import Any

from blueprint import BaseModel, BlueprintDagArgs, Field


class SandboxDagArgsConfig(BaseModel):
    """Top-level fields a sandbox DAG YAML may set.

    No owning team is required -- a prototype does not have one yet. An expiry
    date is, so that abandoned prototypes can be found and removed.
    """

    schedule: str | None = Field(default=None, description="Cron or preset schedule")
    description: str | None = Field(default=None, description="Shown in the Airflow UI")

    expires: str = Field(
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="Date after which this DAG should be deleted, as YYYY-MM-DD",
    )
    author: str = Field(default="unassigned", description="Who to ask about this DAG")


class SandboxDagArgs(BlueprintDagArgs[SandboxDagArgsConfig]):
    """Prototype standards: paused on arrival, no retries, no alerting."""

    def render(self, config: SandboxDagArgsConfig) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "catchup": False,
            "max_active_runs": 1,
            "is_paused_upon_creation": True,
            "tags": ["tier:sandbox", f"expires:{config.expires}"],
            "dagrun_timeout": timedelta(hours=12),
            "default_args": {
                "owner": config.author,
                "retries": 0,
                "email_on_failure": False,
            },
        }

        if config.schedule is not None:
            kwargs["schedule"] = config.schedule
        if config.description is not None:
            kwargs["description"] = config.description

        return kwargs
