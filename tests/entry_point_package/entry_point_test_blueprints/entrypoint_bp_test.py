"""A blueprint for testing the entry-point packaging mechanism."""

from blueprint import BaseModel, Blueprint, TaskOrGroup


class EntryPointBpTestConfig(BaseModel):
    message: str


class EntryPointBpTest(Blueprint[EntryPointBpTestConfig]):
    """Test-only blueprint shipped via an installed package entry point."""

    def render(self, config: EntryPointBpTestConfig) -> TaskOrGroup:
        try:
            from airflow.providers.standard.operators.bash import BashOperator
        except ImportError:
            from airflow.operators.bash import BashOperator

        return BashOperator(task_id=self.step_id, bash_command=f"echo {config.message}")
