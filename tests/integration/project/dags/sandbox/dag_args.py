"""A second BlueprintDagArgs template, scoped to this directory.

ProjectDagArgs in ../test_blueprints.py is the project fallback and applies to
the DAG files beside it. The DAG files in this directory get these looser
arguments instead, because this template is the one defined closest above them.
The same loader builds both.
"""

from typing import Any

from pydantic import ConfigDict

from blueprint import BaseModel, BlueprintDagArgs, Field


class SandboxDagArgsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    description: str | None = None
    experiment: str
    ttl_days: int = Field(default=7, ge=1)


class SandboxDagArgs(BlueprintDagArgs[SandboxDagArgsConfig]):
    """DAG arguments for throwaway sandbox pipelines."""

    def render(self, config: SandboxDagArgsConfig) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "tags": [
                "sandbox",
                f"experiment:{config.experiment}",
                f"ttl:{config.ttl_days}d",
            ],
            "default_args": {"owner": "sandbox", "retries": 0},
        }
        if config.description is not None:
            kwargs["description"] = config.description
        return kwargs
