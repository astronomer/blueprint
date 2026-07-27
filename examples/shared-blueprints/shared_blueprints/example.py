"""Example Blueprint template."""

try:
    # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.sdk import TaskGroup
except ImportError:
    # Airflow 2
    from airflow.operators.bash import BashOperator
    from airflow.utils.task_group import TaskGroup

from blueprint import BaseModel, Blueprint, TaskOrGroup


class ExampleConfig(BaseModel):
    foo: str
    bar: str


class Example(Blueprint[ExampleConfig]):
    """Example Blueprint template from shared package."""

    def render(self, config: ExampleConfig) -> TaskOrGroup:
        with TaskGroup(group_id=self.step_id) as group:
            foo = BashOperator(task_id="foo", bash_command=f"echo {config.foo}")
            bar = BashOperator(task_id="bar", bash_command=f"echo {config.bar}")
            foo >> bar
        return group
