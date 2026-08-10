"""Standard publication template, maintained by the platform team."""

try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2
    from airflow.operators.bash import BashOperator

from blueprint import BaseModel, Blueprint, Field, TaskOrGroup


class PublishConfig(BaseModel):
    dataset: str = Field(description="Dataset to publish")
    consumers: list[str] = Field(
        default_factory=list, description="Teams notified when the dataset lands"
    )


class Publish(Blueprint[PublishConfig]):
    """Publish a landed dataset and notify its consumers."""

    def render(self, config: PublishConfig) -> TaskOrGroup:
        consumers = ", ".join(config.consumers) if config.consumers else "nobody"
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Publishing {config.dataset} to {consumers}'",
        )
