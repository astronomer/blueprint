"""Building a high-level blueprint out of lower-level ones.

Validate and Report are usable on their own from YAML. QualityGate wires them
together behind a single, smaller config.
"""

# Blueprints run unchanged on Airflow 2 and 3 -- only the import paths differ.
try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.sdk import TaskGroup
except ImportError:  # Airflow 2
    from airflow.operators.bash import BashOperator
    from airflow.utils.task_group import TaskGroup

from blueprint import BaseModel, Blueprint, Field, TaskOrGroup


# --- the building blocks ----------------------------------------------------


class ValidateConfig(BaseModel):
    table: str = Field(description="Table to check")
    checks: list[str] = Field(description="Named checks to run against the table")
    fail_fast: bool = Field(default=False, description="Stop at the first failing check")


class Validate(Blueprint[ValidateConfig]):
    """Run named data quality checks against a table."""

    def render(self, config: ValidateConfig) -> TaskOrGroup:
        with TaskGroup(group_id=self.step_id) as group:
            previous = None
            for check in config.checks:
                current = BashOperator(
                    task_id=check,
                    bash_command=f"echo 'Checking {config.table} for {check}'",
                )
                # Sequential when failing fast, so the first failure stops the
                # rest; otherwise every check runs and reports.
                if config.fail_fast and previous is not None:
                    previous >> current
                previous = current
        return group


class ReportConfig(BaseModel):
    channel: str = Field(description="Channel to post results to")
    mention_on_failure: str | None = Field(
        default=None, description="Team to @-mention when a check fails"
    )


class Report(Blueprint[ReportConfig]):
    """Post quality results to a channel."""

    def render(self, config: ReportConfig) -> TaskOrGroup:
        mention = f" cc {config.mention_on_failure}" if config.mention_on_failure else ""
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Posting quality report to {config.channel}{mention}'",
        )


# --- the composite ----------------------------------------------------------


class QualityGateConfig(BaseModel):
    """Deliberately smaller than the two configs it drives.

    `fail_fast` and `mention_on_failure` are decided here rather than exposed,
    because a gate that blocks a release should behave the same everywhere.
    """

    table: str = Field(description="Table to gate on")
    checks: list[str] = Field(
        default=["nulls", "duplicates", "freshness"],
        description="Named checks to run",
    )
    channel: str = Field(default="#data-quality", description="Where results are posted")


class QualityGate(Blueprint[QualityGateConfig]):
    """Run quality checks, then report -- composed from validate and report."""

    def render(self, config: QualityGateConfig) -> TaskOrGroup:
        with TaskGroup(group_id=self.step_id) as group:
            # Instantiate the blueprint, give it a step_id, call render().
            # Inside a group, step_id only has to be unique within the group.
            validate = Validate()
            validate.step_id = "validate"
            validate_group = validate.render(
                ValidateConfig(
                    table=config.table,
                    checks=config.checks,
                    fail_fast=True,
                )
            )

            report = Report()
            report.step_id = "report"
            report_task = report.render(
                ReportConfig(
                    channel=config.channel,
                    mention_on_failure="@data-oncall",
                )
            )

            validate_group >> report_task
        return group
