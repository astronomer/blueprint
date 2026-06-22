"""Blueprint for the bare-minimum safe-mode discovery probe."""

from airflow.operators.bash import BashOperator

from blueprint import BaseModel, Blueprint


class SafeModeProbeConfig(BaseModel):
    message: str = "safe-mode probe"


class SafeModeProbe(Blueprint[SafeModeProbeConfig]):
    """Single-task blueprint used to prove safe-mode discovery end-to-end."""

    def render(self, config: SafeModeProbeConfig) -> BashOperator:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo '{config.message}'",
        )
