"""Render tests: assert the task structure a config produces.

These matter for blueprints whose shape depends on the config -- the branches
a config test cannot reach.
"""

from blueprints import Extract, ExtractConfig, Load, LoadConfig
from conftest import render, task_ids


class TestExtractRender:
    def test_one_task_per_dataset(self, dag):  # noqa: ARG002 -- DAG context
        group = render(
            Extract,
            ExtractConfig(source="crm", datasets=["customers", "orders"]),
            step_id="extract_crm",
        )
        assert task_ids(group) == {"validate", "pull_customers", "pull_orders"}

    def test_validation_task_is_optional(self, dag):  # noqa: ARG002
        group = render(
            Extract,
            ExtractConfig(source="crm", datasets=["customers"], validate_first=False),
            step_id="extract_crm",
        )
        assert task_ids(group) == {"pull_customers"}

    def test_group_id_comes_from_step_id(self, dag):  # noqa: ARG002
        group = render(
            Extract,
            ExtractConfig(source="crm", datasets=["customers"]),
            step_id="extract_crm",
        )
        # Getting this wrong -- hardcoding a task_id instead of using
        # self.step_id -- breaks the moment a DAG has two extract steps.
        assert group.group_id == "extract_crm"

    def test_pulls_wait_for_validation(self, dag):  # noqa: ARG002
        group = render(
            Extract,
            ExtractConfig(source="crm", datasets=["customers"]),
            step_id="extract_crm",
        )
        tasks = {task.task_id.split(".")[-1]: task for task in group}
        assert "extract_crm.validate" in tasks["pull_customers"].upstream_task_ids


class TestLoadRender:
    def test_renders_single_task_named_after_step(self, dag):  # noqa: ARG002
        task = render(
            Load, LoadConfig(target="warehouse.customers"), step_id="load_customers"
        )
        assert task.task_id == "load_customers"

    def test_mode_reaches_the_command(self, dag):  # noqa: ARG002
        task = render(
            Load,
            LoadConfig(target="warehouse.customers", mode="overwrite"),
            step_id="load_customers",
        )
        assert "overwrite" in task.bash_command
