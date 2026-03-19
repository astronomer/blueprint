"""Blueprint definitions using explicit name and version declarations."""

from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from blueprint import BaseModel, Blueprint, Field


class IngestConfig(BaseModel):
    source: str


class S3DataIngester(Blueprint[IngestConfig]):
    """Registered as 'ingest' v1 — class name decoupled from registry identity."""

    name = "ingest"
    version = 1

    def render(self, config: IngestConfig) -> TaskGroup:
        with TaskGroup(group_id=self.step_id) as group:
            BashOperator(task_id="download", bash_command=f"echo '{config.source}'")
            BashOperator(task_id="parse", bash_command="echo 'parse'")
        return group


class IngestV2Config(BaseModel):
    sources: list[str]


class StreamingIngester(Blueprint[IngestV2Config]):
    """Registered as 'ingest' v2 — explicit attrs, multi-source."""

    name = "ingest"
    version = 2

    def render(self, config: IngestV2Config) -> TaskGroup:
        with TaskGroup(group_id=self.step_id) as group:
            for src in config.sources:
                BashOperator(task_id=f"stream_{src}", bash_command=f"echo '{src}'")
        return group


class CheckConfig(BaseModel):
    checks: list[str] = Field(default=["not_null"])


class DataQualityValidator(Blueprint[CheckConfig]):
    """Registered as 'quality_check' v1 — explicit name, version inferred."""

    name = "quality_check"

    def render(self, config: CheckConfig) -> TaskGroup:
        with TaskGroup(group_id=self.step_id) as group:
            for check in config.checks:
                BashOperator(task_id=check, bash_command=f"echo '{check}'")
        return group


class NotifyConfig(BaseModel):
    message: str = Field(default="done")


class Notify(Blueprint[NotifyConfig]):
    """Registered as 'notify' v1 — explicit version, name inferred from class."""

    version = 1

    def render(self, config: NotifyConfig) -> BashOperator:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo '{config.message}'",
        )
