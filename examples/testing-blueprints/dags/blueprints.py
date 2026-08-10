"""Blueprints with behaviour worth asserting: conditional structure and a
config rule that types alone cannot express."""

from typing import Literal

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import TaskGroup

from blueprint import (
    BaseModel,
    Blueprint,
    ConfigDict,
    Field,
    TaskOrGroup,
    model_validator,
)


class ExtractConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str = Field(description="Source system to read from")
    datasets: list[str] = Field(min_length=1, description="Datasets to pull")
    validate_first: bool = Field(
        default=True, description="Run a reachability check before extracting"
    )


class Extract(Blueprint[ExtractConfig]):
    """Read datasets from a source, optionally validating it first."""

    def render(self, config: ExtractConfig) -> TaskOrGroup:
        with TaskGroup(group_id=self.step_id) as group:
            check = None
            if config.validate_first:
                check = BashOperator(
                    task_id="validate",
                    bash_command=f"echo 'Checking {config.source}'",
                )

            for dataset in config.datasets:
                pull = BashOperator(
                    task_id=f"pull_{dataset}",
                    bash_command=f"echo 'Pulling {dataset} from {config.source}'",
                )
                if check is not None:
                    check >> pull
        return group


class LoadConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target: str = Field(description="Destination table")
    mode: Literal["append", "overwrite", "upsert"] = Field(default="append")
    dedupe_keys: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def upsert_requires_keys(self) -> "LoadConfig":
        if self.mode == "upsert" and not self.dedupe_keys:
            msg = "mode 'upsert' requires at least one entry in dedupe_keys"
            raise ValueError(msg)
        return self


class Load(Blueprint[LoadConfig]):
    """Write to a destination table."""

    def render(self, config: LoadConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Loading into {config.target} ({config.mode})'",
        )
