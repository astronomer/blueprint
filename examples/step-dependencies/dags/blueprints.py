"""Deliberately trivial blueprints.

This example is about wiring steps together in YAML, so every blueprint here
renders a single task and the graph in the UI stays readable.
"""

from airflow.providers.standard.operators.bash import BashOperator

from blueprint import BaseModel, Blueprint, Field, TaskOrGroup


class ExtractConfig(BaseModel):
    source: str = Field(description="Source system to read from")


class Extract(Blueprint[ExtractConfig]):
    """Read from a source system."""

    def render(self, config: ExtractConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Extracting {config.source}'",
        )


class MergeConfig(BaseModel):
    output: str = Field(description="Table the merged result is written to")


class Merge(Blueprint[MergeConfig]):
    """Combine whatever upstream extracts produced."""

    def render(self, config: MergeConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Merging into {config.output}'",
        )


class QualityCheckConfig(BaseModel):
    table: str = Field(description="Table to check")


class QualityCheck(Blueprint[QualityCheckConfig]):
    """Run data quality checks on a table."""

    def render(self, config: QualityCheckConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Checking {config.table}'",
        )


class PublishConfig(BaseModel):
    dataset: str = Field(description="Dataset to publish")


class Publish(Blueprint[PublishConfig]):
    """Publish a dataset to consumers."""

    def render(self, config: PublishConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Publishing {config.dataset}'",
        )


class NotifyConfig(BaseModel):
    channel: str = Field(description="Channel to post to")


class Notify(Blueprint[NotifyConfig]):
    """Post a notification."""

    def render(self, config: NotifyConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Notifying {config.channel}'",
        )


class CleanupConfig(BaseModel):
    path: str = Field(description="Scratch location to clear")


class Cleanup(Blueprint[CleanupConfig]):
    """Remove scratch data. Meant to run whether or not the run succeeded."""

    def render(self, config: CleanupConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Clearing {config.path}'",
        )
