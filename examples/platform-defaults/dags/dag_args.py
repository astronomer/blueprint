"""Project-wide DAG arguments.

A BlueprintDagArgs subclass defines which top-level fields a DAG YAML may set,
and turns them into DAG constructor kwargs. At most one may exist per project.
"""

from datetime import timedelta
from typing import Any, Literal

from blueprint import BaseModel, BlueprintDagArgs, Field

# Retry and timeout policy per tier, decided once for the whole platform.
TIER_POLICY = {
    "critical": {"retries": 5, "retry_delay_minutes": 2, "timeout_hours": 2},
    "standard": {"retries": 2, "retry_delay_minutes": 5, "timeout_hours": 6},
    "experimental": {"retries": 0, "retry_delay_minutes": 5, "timeout_hours": 12},
}


class ProjectDagArgsConfig(BaseModel):
    """The complete set of top-level fields a DAG YAML may set.

    Anything not listed here is not available to DAG authors -- which is the
    point. There is no way to reach past this into the DAG constructor.
    """

    schedule: str | None = Field(default=None, description="Cron or preset schedule")
    description: str | None = Field(default=None, description="Shown in the Airflow UI")

    # Required: every DAG must name an owning team.
    team: str = Field(
        pattern=r"^[a-z][a-z0-9-]*$", description="Owning team, lowercase and hyphenated"
    )
    tier: Literal["critical", "standard", "experimental"] = Field(
        default="standard", description="Drives retries, timeout and alerting"
    )


class ProjectDagArgs(BlueprintDagArgs[ProjectDagArgsConfig]):
    """Turns a team and a tier into the DAG settings the platform requires."""

    def render(self, config: ProjectDagArgsConfig) -> dict[str, Any]:
        policy = TIER_POLICY[config.tier]

        kwargs: dict[str, Any] = {
            "catchup": False,
            "max_active_runs": 1,
            "tags": [f"team:{config.team}", f"tier:{config.tier}"],
            "dagrun_timeout": timedelta(hours=policy["timeout_hours"]),
            "default_args": {
                "owner": config.team,
                "retries": policy["retries"],
                "retry_delay": timedelta(minutes=policy["retry_delay_minutes"]),
                "email_on_failure": config.tier == "critical",
            },
        }

        # Only pass through what was actually set, so Airflow's own defaults
        # apply otherwise.
        if config.schedule is not None:
            kwargs["schedule"] = config.schedule
        if config.description is not None:
            kwargs["description"] = config.description

        return kwargs
