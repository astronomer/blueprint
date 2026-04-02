"""Custom DAG-level arguments for space missions."""

from typing import Any, Literal

from blueprint import BaseModel, BlueprintDagArgs


class MissionConfig(BaseModel):
    schedule: str | None = None
    description: str | None = None
    priority: Literal["low", "medium", "high", "critical"] = "medium"


class MissionDagArgs(BlueprintDagArgs[MissionConfig]):
    """Converts mission priority into a DAG tag."""

    def render(self, config: MissionConfig) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if config.schedule is not None:
            result["schedule"] = config.schedule
        if config.description is not None:
            result["description"] = config.description
        result["tags"] = [f"priority:{config.priority}"]
        return result
