"""Three blueprints showing what render() is allowed to return.

Notify  -> a single operator
Extract -> a TaskGroup with a fixed set of tasks
Publish -> a TaskGroup whose contents are driven by the config, including a
           nested TaskGroup
"""

# Blueprints run unchanged on Airflow 2 and 3 -- only the import paths differ.
try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.sdk import TaskGroup
except ImportError:  # Airflow 2
    from airflow.operators.bash import BashOperator
    from airflow.utils.task_group import TaskGroup

from blueprint import BaseModel, Blueprint, Field, TaskOrGroup


class NotifyConfig(BaseModel):
    channel: str = Field(description="Channel to post the notification to")


class Notify(Blueprint[NotifyConfig]):
    """Post a notification. Renders as a single task."""

    def render(self, config: NotifyConfig) -> TaskOrGroup:
        # One task, so step_id is the task_id. In the graph this appears as a
        # bare task named after the step.
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Notifying {config.channel}'",
        )


class ExtractConfig(BaseModel):
    source: str = Field(description="Source system to read from")


class Extract(Blueprint[ExtractConfig]):
    """Validate a source, then read from it. Renders as a TaskGroup."""

    def render(self, config: ExtractConfig) -> TaskOrGroup:
        # A group, so step_id is the group_id and the child task_ids are ours
        # to choose. They only need to be unique within the group.
        with TaskGroup(group_id=self.step_id) as group:
            validate = BashOperator(
                task_id="validate",
                bash_command=f"echo 'Checking {config.source} is reachable'",
            )
            fetch = BashOperator(
                task_id="fetch",
                bash_command=f"echo 'Reading from {config.source}'",
            )
            validate >> fetch
        return group


class PublishConfig(BaseModel):
    dataset: str = Field(description="Dataset being published")
    regions: list[str] = Field(description="Regions to publish the dataset to")


class Publish(Blueprint[PublishConfig]):
    """Publish a dataset to one or more regions.

    The task structure is derived from the config: one task per region, wrapped
    in a nested group, with a single verification task afterwards.
    """

    def render(self, config: PublishConfig) -> TaskOrGroup:
        with TaskGroup(group_id=self.step_id) as group:
            # Nested groups are just TaskGroups created inside another one.
            with TaskGroup(group_id="regions") as regions:
                for region in config.regions:
                    BashOperator(
                        task_id=region,
                        bash_command=f"echo 'Publishing {config.dataset} to {region}'",
                    )

            verify = BashOperator(
                task_id="verify",
                bash_command=f"echo 'Verifying {config.dataset} in all regions'",
            )
            regions >> verify
        return group
