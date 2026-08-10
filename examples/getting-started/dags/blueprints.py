"""The two blueprints used by pipeline.dag.yaml.

A blueprint is a class with a Pydantic config model and a render() method.
The config model defines what YAML authors may set; render() turns a validated
config into Airflow tasks.
"""

from airflow.providers.standard.operators.bash import BashOperator

from blueprint import BaseModel, Blueprint, Field, TaskOrGroup


class ExtractConfig(BaseModel):
    """Fields a YAML author may set on an `extract` step."""

    source: str = Field(description="Name of the source system to read from")
    batch_size: int = Field(default=1000, ge=1, description="Rows to read per batch")


class Extract(Blueprint[ExtractConfig]):
    """Pull data from a source system."""

    def render(self, config: ExtractConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Extracting {config.source} in batches of {config.batch_size}'",
        )


class LoadConfig(BaseModel):
    """Fields a YAML author may set on a `load` step."""

    target: str = Field(description="Destination table")


class Load(Blueprint[LoadConfig]):
    """Load data into a destination table."""

    def render(self, config: LoadConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Loading into {config.target}'",
        )
