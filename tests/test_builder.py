"""Tests for the DAG builder: DAGConfig, StepConfig, Builder, build_all_dags."""

from pathlib import Path
from typing import Literal

import pytest
import yaml
from pydantic import BaseModel, ConfigDict, Field

from blueprint.builder import Builder, DAGConfig, StepConfig, _config_to_params
from blueprint.core import Blueprint, TaskOrGroup
from blueprint.errors import (
    CyclicDependencyError,
    InvalidDependencyError,
)
from blueprint.registry import BlueprintRegistry

# --- Test blueprint classes ---


class ExtractConfig(BaseModel):
    source_table: str
    batch_size: int = 1000


class Extract(Blueprint[ExtractConfig]):
    """Extract data from a source."""

    def render(self, config: ExtractConfig) -> TaskOrGroup:
        from airflow.operators.bash import BashOperator
        from airflow.utils.task_group import TaskGroup

        with TaskGroup(group_id=self.step_id) as group:
            BashOperator(
                task_id="validate",
                bash_command=f"echo 'validate {config.source_table}'",
            )
            BashOperator(
                task_id="extract",
                bash_command=f"echo 'extract {config.batch_size}'",
            )
        return group


class ExtractV2Config(BaseModel):
    sources: list[str]


class ExtractV2(Blueprint[ExtractV2Config]):
    """Extract v2 with multi-source."""

    def render(self, config: ExtractV2Config) -> TaskOrGroup:
        from airflow.operators.bash import BashOperator
        from airflow.utils.task_group import TaskGroup

        with TaskGroup(group_id=self.step_id) as group:
            for src in config.sources:
                BashOperator(task_id=f"extract_{src}", bash_command=f"echo '{src}'")
        return group


class LoadConfig(BaseModel):
    target_table: str
    mode: str = "append"


class Load(Blueprint[LoadConfig]):
    """Load data to a target."""

    def render(self, config: LoadConfig) -> TaskOrGroup:
        from airflow.operators.bash import BashOperator

        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'load {config.target_table}'",
        )


class NestedConfig(BaseModel):
    value: str = "default"


class Nested(Blueprint[NestedConfig]):
    """Blueprint that renders nested TaskGroups."""

    def render(self, config: NestedConfig) -> TaskOrGroup:
        from airflow.operators.bash import BashOperator
        from airflow.utils.task_group import TaskGroup

        with TaskGroup(group_id=self.step_id) as outer:
            BashOperator(task_id="top_level", bash_command="echo top")
            with TaskGroup(group_id="inner") as _inner:
                BashOperator(task_id="deep", bash_command=f"echo {config.value}")
        return outer


class ChainedConfig(BaseModel):
    operations: list[str] = Field(default_factory=lambda: ["first", "second"])


class Chained(Blueprint[ChainedConfig]):
    """Blueprint with internally chained tasks for trigger_rule testing."""

    def render(self, config: ChainedConfig) -> TaskOrGroup:
        from airflow.operators.bash import BashOperator
        from airflow.utils.task_group import TaskGroup

        with TaskGroup(group_id=self.step_id) as group:
            prev = None
            for op in config.operations:
                t = BashOperator(task_id=op, bash_command=f"echo '{op}'")
                if prev:
                    prev >> t
                prev = t
        return group


# --- Fixtures ---


@pytest.fixture
def test_registry():
    reg = BlueprintRegistry()
    reg._blueprints = {
        "extract": {1: Extract, 2: ExtractV2},
        "load": {1: Load},
        "nested": {1: Nested},
        "chained": {1: Chained},
    }
    reg._blueprint_locations = {
        "extract": {1: "test.py", 2: "test.py"},
        "load": {1: "test.py"},
        "nested": {1: "test.py"},
        "chained": {1: "test.py"},
    }
    reg._discovered = True
    return reg


@pytest.fixture
def builder(test_registry):
    return Builder(bp_registry=test_registry)


# --- StepConfig tests ---


class TestStepConfig:
    def test_basic_step(self):
        step = StepConfig(blueprint="extract", source_table="raw.data")
        assert step.blueprint == "extract"
        assert step.depends_on == []
        assert step.version is None
        assert step.get_blueprint_config() == {"source_table": "raw.data"}

    def test_step_with_dependencies(self):
        step = StepConfig(blueprint="load", depends_on=["extract"], target_table="out")
        assert step.depends_on == ["extract"]
        assert step.get_blueprint_config() == {"target_table": "out"}

    def test_step_with_version(self):
        step = StepConfig(blueprint="extract", version=2, sources=["a", "b"])
        assert step.version == 2
        assert step.get_blueprint_config() == {"sources": ["a", "b"]}

    def test_step_with_trigger_rule(self):
        step = StepConfig(blueprint="load", trigger_rule="one_success", target_table="out")
        assert step.trigger_rule == "one_success"
        assert "trigger_rule" not in step.get_blueprint_config()

    def test_step_trigger_rule_default_none(self):
        step = StepConfig(blueprint="load", target_table="out")
        assert step.trigger_rule is None

    def test_step_invalid_trigger_rule(self):
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="Invalid trigger rule"):
            StepConfig(blueprint="load", trigger_rule="bogus", target_table="out")


# --- DAGConfig tests ---


class TestDAGConfig:
    def test_basic_dag_config(self):
        config = DAGConfig(
            dag_id="test_dag",
            steps={"s1": StepConfig(blueprint="extract", source_table="raw.data")},
        )
        assert config.dag_id == "test_dag"
        assert config.get_extra_fields() == {}

    def test_dag_config_with_extras(self):
        config = DAGConfig(
            dag_id="test_dag",
            schedule="@daily",
            description="A test",
            steps={"s1": StepConfig(blueprint="extract", source_table="raw.data")},
        )
        extras = config.get_extra_fields()
        assert extras["schedule"] == "@daily"
        assert extras["description"] == "A test"

    def test_empty_steps_rejected(self):
        with pytest.raises(ValueError, match="at least one step"):
            DAGConfig(dag_id="test", steps={})

    def test_from_yaml_dict(self):
        raw = {
            "dag_id": "pipeline",
            "schedule": "@hourly",
            "steps": {
                "extract": {"blueprint": "extract", "source_table": "raw.data"},
                "load": {
                    "blueprint": "load",
                    "depends_on": ["extract"],
                    "target_table": "out",
                },
            },
        }
        config = DAGConfig(**raw)
        assert len(config.steps) == 2
        assert config.steps["load"].depends_on == ["extract"]


# --- Builder tests ---


class TestBuilder:
    def test_build_simple_dag(self, builder):
        config = DAGConfig(
            dag_id="simple_dag",
            steps={
                "my_extract": StepConfig(blueprint="extract", version=1, source_table="raw.data"),
            },
        )
        dag = builder.build(config)
        assert dag.dag_id == "simple_dag"
        assert "my_extract" in dag.task_group_dict

    def test_build_with_dependencies(self, builder):
        config = DAGConfig(
            dag_id="dep_dag",
            steps={
                "my_extract": StepConfig(blueprint="extract", version=1, source_table="raw.data"),
                "my_load": StepConfig(
                    blueprint="load",
                    depends_on=["my_extract"],
                    target_table="out",
                ),
            },
        )
        dag = builder.build(config)
        assert dag.dag_id == "dep_dag"

        load_task = dag.task_dict.get("my_load")
        assert load_task is not None

        upstream_ids = {t.task_id for t in load_task.upstream_list}
        upstream_group_ids = {t.group_id for t in load_task.upstream_list if hasattr(t, "group_id")}
        assert upstream_ids or upstream_group_ids, "my_load should have upstream dependencies"

    def test_build_single_task_blueprint(self, builder):
        config = DAGConfig(
            dag_id="single_task",
            steps={
                "my_load": StepConfig(blueprint="load", target_table="out"),
            },
        )
        dag = builder.build(config)
        assert "my_load" in dag.task_dict

    def test_build_with_version_pin(self, builder):
        config = DAGConfig(
            dag_id="versioned",
            steps={
                "old_extract": StepConfig(blueprint="extract", version=1, source_table="raw.data"),
                "new_extract": StepConfig(blueprint="extract", version=2, sources=["a", "b"]),
            },
        )
        dag = builder.build(config)
        assert dag.dag_id == "versioned"

    def test_invalid_dependency_raises(self, builder):
        config = DAGConfig(
            dag_id="bad_dep",
            steps={
                "load": StepConfig(
                    blueprint="load",
                    depends_on=["nonexistent"],
                    target_table="out",
                ),
            },
        )
        with pytest.raises(InvalidDependencyError, match="nonexistent"):
            builder.build(config)

    def test_cyclic_dependency_raises(self, builder):
        config = DAGConfig(
            dag_id="cycle",
            steps={
                "a": StepConfig(blueprint="load", depends_on=["b"], target_table="x"),
                "b": StepConfig(blueprint="load", depends_on=["a"], target_table="y"),
            },
        )
        with pytest.raises(CyclicDependencyError):
            builder.build(config)

    def test_two_node_cycle_content(self, builder):
        config = DAGConfig(
            dag_id="cycle2",
            steps={
                "a": StepConfig(blueprint="load", depends_on=["b"], target_table="x"),
                "b": StepConfig(blueprint="load", depends_on=["a"], target_table="y"),
            },
        )
        with pytest.raises(CyclicDependencyError) as exc_info:
            builder.build(config)

        cycle = exc_info.value.cycle
        assert cycle[0] == cycle[-1], "Cycle must start and end with same node"
        assert len(cycle) == 3, f"2-node cycle should have 3 elements (a->b->a), got {cycle}"

    def test_three_node_cycle(self, builder):
        config = DAGConfig(
            dag_id="cycle3",
            steps={
                "a": StepConfig(blueprint="load", depends_on=["c"], target_table="x"),
                "b": StepConfig(blueprint="load", depends_on=["a"], target_table="y"),
                "c": StepConfig(blueprint="load", depends_on=["b"], target_table="z"),
            },
        )
        with pytest.raises(CyclicDependencyError):
            builder.build(config)

    def test_three_node_cycle_content(self, builder):
        config = DAGConfig(
            dag_id="cycle3c",
            steps={
                "a": StepConfig(blueprint="load", depends_on=["c"], target_table="x"),
                "b": StepConfig(blueprint="load", depends_on=["a"], target_table="y"),
                "c": StepConfig(blueprint="load", depends_on=["b"], target_table="z"),
            },
        )
        with pytest.raises(CyclicDependencyError) as exc_info:
            builder.build(config)

        cycle = exc_info.value.cycle
        assert cycle[0] == cycle[-1], "Cycle must start and end with same node"
        assert len(cycle) == 4, f"3-node cycle should have 4 elements (a->b->c->a), got {cycle}"
        inner = set(cycle[:-1])
        assert inner == {"a", "b", "c"}

    def test_self_loop_cycle_content(self, builder):
        config = DAGConfig(
            dag_id="self_loop",
            steps={
                "a": StepConfig(blueprint="load", depends_on=["a"], target_table="x"),
            },
        )
        with pytest.raises(CyclicDependencyError) as exc_info:
            builder.build(config)

        cycle = exc_info.value.cycle
        assert cycle == ["a", "a"], f"Self-loop should be ['a', 'a'], got {cycle}"

    def test_step_context_injection(self, builder):
        config = DAGConfig(
            dag_id="context_dag",
            steps={
                "my_load": StepConfig(blueprint="load", target_table="out"),
            },
        )
        dag = builder.build(config)
        task = dag.task_dict["my_load"]

        assert hasattr(task, "blueprint_step_config")
        assert hasattr(task, "blueprint_step_code")
        assert "target_table" in task.blueprint_step_config
        assert "blueprint_step_config" in task.template_fields
        assert "blueprint_step_code" in task.template_fields

    def test_step_config_shows_resolved_version(self, builder):
        config = DAGConfig(
            dag_id="version_ctx",
            steps={
                "my_load": StepConfig(blueprint="load", target_table="out"),
            },
        )
        dag = builder.build(config)
        task = dag.task_dict["my_load"]

        parsed = yaml.safe_load(task.blueprint_step_config)
        assert parsed["version"] == 1

    def test_source_code_in_context(self, builder):
        config = DAGConfig(
            dag_id="source_ctx",
            steps={
                "my_load": StepConfig(blueprint="load", target_table="out"),
            },
        )
        dag = builder.build(config)
        task = dag.task_dict["my_load"]

        assert "class Load" in task.blueprint_step_code

    def test_nested_task_group_context_injection(self, builder):
        config = DAGConfig(
            dag_id="nested_ctx",
            steps={
                "my_nested": StepConfig(blueprint="nested", value="hello"),
            },
        )
        dag = builder.build(config)

        all_tasks = list(dag.task_dict.values())
        assert len(all_tasks) == 2
        for task in all_tasks:
            assert hasattr(task, "blueprint_step_config"), (
                f"Task {task.task_id} missing blueprint_step_config"
            )
            assert hasattr(task, "blueprint_step_code"), (
                f"Task {task.task_id} missing blueprint_step_code"
            )
            assert "blueprint_step_config" in task.template_fields
            assert "blueprint_step_code" in task.template_fields

    def test_trigger_rule_single_task(self, builder):
        config = DAGConfig(
            dag_id="tr_single",
            steps={
                "my_extract": StepConfig(blueprint="extract", version=1, source_table="raw.data"),
                "my_load": StepConfig(
                    blueprint="load",
                    depends_on=["my_extract"],
                    trigger_rule="one_success",
                    target_table="out",
                ),
            },
        )
        dag = builder.build(config)
        task = dag.task_dict["my_load"]
        assert str(task.trigger_rule) == "one_success"

    def test_trigger_rule_task_group_root_only(self, builder):
        config = DAGConfig(
            dag_id="tr_group",
            steps={
                "my_load": StepConfig(blueprint="load", target_table="out"),
                "my_chained": StepConfig(
                    blueprint="chained",
                    depends_on=["my_load"],
                    trigger_rule="all_done",
                    operations=["first", "second"],
                ),
            },
        )
        dag = builder.build(config)
        first = dag.task_dict["my_chained.first"]
        second = dag.task_dict["my_chained.second"]
        assert str(first.trigger_rule) == "all_done"
        assert str(second.trigger_rule) == "all_success"

    def test_trigger_rule_none_preserves_default(self, builder):
        config = DAGConfig(
            dag_id="tr_default",
            steps={
                "my_load": StepConfig(blueprint="load", target_table="out"),
            },
        )
        dag = builder.build(config)
        task = dag.task_dict["my_load"]
        assert str(task.trigger_rule) == "all_success"

    def test_trigger_rule_without_depends_on(self, builder):
        config = DAGConfig(
            dag_id="tr_no_deps",
            steps={
                "my_load": StepConfig(
                    blueprint="load",
                    trigger_rule="always",
                    target_table="out",
                ),
            },
        )
        dag = builder.build(config)
        task = dag.task_dict["my_load"]
        assert str(task.trigger_rule) == "always"


class TestBuilderDefaultDagArgs:
    def test_description_passed_to_dag(self, builder):
        config = DAGConfig(
            dag_id="desc_dag",
            description="A test DAG",
            steps={"s": StepConfig(blueprint="load", target_table="out")},
        )
        dag = builder.build(config)
        assert dag.description == "A test DAG"

    def test_schedule_passed_to_dag(self, builder):
        config = DAGConfig(
            dag_id="sched_dag",
            schedule="@hourly",
            steps={"s": StepConfig(blueprint="load", target_table="out")},
        )
        dag = builder.build(config)
        assert dag.schedule == "@hourly"

    def test_default_start_date_injected(self, builder):
        from datetime import datetime, timezone

        config = DAGConfig(
            dag_id="start_dag",
            steps={"s": StepConfig(blueprint="load", target_table="out")},
        )
        dag = builder.build(config)
        assert dag.start_date == datetime(2024, 1, 1, tzinfo=timezone.utc)

    def test_default_catchup_false(self, builder):
        config = DAGConfig(
            dag_id="catchup_dag",
            steps={"s": StepConfig(blueprint="load", target_table="out")},
        )
        dag = builder.build(config)
        assert dag.catchup is False

    def test_unknown_extra_field_rejected(self, builder):
        from pydantic import ValidationError

        config = DAGConfig(
            dag_id="bad_dag",
            unknown_field="oops",
            steps={"s": StepConfig(blueprint="load", target_table="out")},
        )
        with pytest.raises(ValidationError):
            builder.build(config)


class TestBuilderCustomDagArgs:
    @pytest.fixture
    def custom_dag_args_registry(self):
        from typing import Any

        from blueprint.core import BlueprintDagArgs

        class CustomConfig(BaseModel):
            model_config = ConfigDict(extra="forbid")

            schedule: str | None = None
            owner: str = "data-team"
            retries: int = 2

        class CustomDagArgs(BlueprintDagArgs[CustomConfig]):
            def render(self, config: CustomConfig) -> dict[str, Any]:
                kwargs: dict[str, Any] = {
                    "default_args": {
                        "owner": config.owner,
                        "retries": config.retries,
                    },
                }
                if config.schedule is not None:
                    kwargs["schedule"] = config.schedule
                return kwargs

        reg = BlueprintRegistry()
        reg._blueprints = {
            "load": {1: Load},
        }
        reg._blueprint_locations = {
            "load": {1: "test.py"},
        }
        reg._dag_args = CustomDagArgs
        reg._dag_args_location = "test.py"
        reg._discovered = True
        return reg

    def test_custom_dag_args_applied(self, custom_dag_args_registry):
        builder = Builder(bp_registry=custom_dag_args_registry)
        config = DAGConfig(
            dag_id="custom_dag",
            schedule="@daily",
            owner="analytics-team",
            retries=5,
            steps={"s": StepConfig(blueprint="load", target_table="out")},
        )
        dag = builder.build(config)
        assert dag.schedule == "@daily"
        assert dag.default_args["owner"] == "analytics-team"
        assert dag.default_args["retries"] == 5

    def test_custom_dag_args_defaults(self, custom_dag_args_registry):
        builder = Builder(bp_registry=custom_dag_args_registry)
        config = DAGConfig(
            dag_id="default_custom",
            steps={"s": StepConfig(blueprint="load", target_table="out")},
        )
        dag = builder.build(config)
        assert dag.default_args["owner"] == "data-team"
        assert dag.default_args["retries"] == 2

    def test_custom_dag_args_rejects_unknown_fields(self, custom_dag_args_registry):
        from pydantic import ValidationError

        builder = Builder(bp_registry=custom_dag_args_registry)
        config = DAGConfig(
            dag_id="bad_dag",
            tags=["oops"],
            steps={"s": StepConfig(blueprint="load", target_table="out")},
        )
        with pytest.raises(ValidationError):
            builder.build(config)

    def test_custom_dag_args_start_date_default(self, custom_dag_args_registry):
        from datetime import datetime, timezone

        builder = Builder(bp_registry=custom_dag_args_registry)
        config = DAGConfig(
            dag_id="sd_dag",
            steps={"s": StepConfig(blueprint="load", target_table="out")},
        )
        dag = builder.build(config)
        assert dag.start_date == datetime(2024, 1, 1, tzinfo=timezone.utc)
        assert dag.catchup is False


class TestDagArgsRejectsParams:
    def test_dag_args_returning_params_raises(self):
        from typing import Any

        from blueprint.core import BlueprintDagArgs
        from blueprint.errors import ConfigurationError

        class ParamsConfig(BaseModel):
            model_config = ConfigDict(extra="forbid")

        class ParamsDagArgs(BlueprintDagArgs[ParamsConfig]):
            def render(self, config: ParamsConfig) -> dict[str, Any]:  # noqa: ARG002
                return {"params": {"custom_param": "value"}}

        reg = BlueprintRegistry()
        reg._blueprints = {"load": {1: Load}}
        reg._blueprint_locations = {"load": {1: "test.py"}}
        reg._dag_args = ParamsDagArgs
        reg._dag_args_location = "test.py"
        reg._discovered = True

        builder = Builder(bp_registry=reg)
        config = DAGConfig(
            dag_id="bad_params_dag",
            steps={"s": StepConfig(blueprint="load", target_table="out")},
        )
        with pytest.raises(ConfigurationError, match="must not return 'params'"):
            builder.build(config)


class TestBuildFromYaml:
    def test_build_from_yaml_file(self, builder, tmp_path):
        yaml_content = """
dag_id: yaml_dag
schedule: "@daily"
steps:
  my_extract:
    blueprint: extract
    version: 1
    source_table: raw.data
  my_load:
    blueprint: load
    depends_on: [my_extract]
    target_table: out
"""
        yaml_file = tmp_path / "test.dag.yaml"
        yaml_file.write_text(yaml_content)

        dag = builder.build_from_yaml(yaml_file, render_template=False)
        assert dag.dag_id == "yaml_dag"
        assert "my_extract" in dag.task_group_dict
        assert "my_load" in dag.task_dict

        load_task = dag.task_dict["my_load"]
        upstream_ids = {t.task_id for t in load_task.upstream_list}
        upstream_group_ids = {t.group_id for t in load_task.upstream_list if hasattr(t, "group_id")}
        assert upstream_ids or upstream_group_ids, "my_load should depend on my_extract"

    def test_build_from_yaml_with_trigger_rule(self, builder, tmp_path):
        yaml_content = """
dag_id: yaml_tr_dag
steps:
  my_extract:
    blueprint: extract
    version: 1
    source_table: raw.data
  my_load:
    blueprint: load
    depends_on: [my_extract]
    trigger_rule: one_success
    target_table: out
"""
        yaml_file = tmp_path / "tr.dag.yaml"
        yaml_file.write_text(yaml_content)

        dag = builder.build_from_yaml(yaml_file, render_template=False)
        task = dag.task_dict["my_load"]
        assert str(task.trigger_rule) == "one_success"


class TestBuildFromYamlJinja:
    def test_build_from_yaml_with_jinja(self, builder, tmp_path):
        yaml_content = (
            "dag_id: {{ prefix }}_pipeline\n"
            "steps:\n"
            "  my_load:\n"
            "    blueprint: load\n"
            "    target_table: {{ prefix }}.output\n"
        )
        yaml_file = tmp_path / "jinja.dag.yaml"
        yaml_file.write_text(yaml_content)

        dag = builder.build_from_yaml(
            yaml_file,
            render_template=True,
            template_context={"prefix": "prod"},
        )
        assert dag.dag_id == "prod_pipeline"
        task = dag.task_dict["my_load"]
        assert "prod.output" in task.bash_command


class TestResolveSearchPath:
    def test_resolve_explicit_path(self):
        from blueprint.builder import _resolve_search_path

        result = _resolve_search_path("/some/path")
        assert result == Path("/some/path")


class TestBuildAll:
    def test_build_all_discovers_yamls(self, tmp_path):
        from blueprint.builder import build_all_dags

        bp_file = tmp_path / "blueprints.py"
        bp_file.write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class ProcConfig(BaseModel):
    cmd: str = "echo hello"

class Proc(Blueprint[ProcConfig]):
    def render(self, config):
        from airflow.operators.bash import BashOperator
        return BashOperator(task_id=self.step_id, bash_command=config.cmd)
""")

        yaml_file = tmp_path / "pipeline.dag.yaml"
        yaml_file.write_text("""
dag_id: build_all_test
steps:
  step1:
    blueprint: proc
""")

        globals_dict = {}
        dags = build_all_dags(
            search_path=tmp_path,
            register_globals=globals_dict,
            render_templates=False,
        )
        assert len(dags) == 1
        assert dags[0].dag_id == "build_all_test"
        assert "build_all_test" in globals_dict

    def test_build_all_deprecated_alias_warns_and_forwards(self, tmp_path):
        from blueprint.builder import build_all

        bp_file = tmp_path / "blueprints.py"
        bp_file.write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class ProcConfig(BaseModel):
    cmd: str = "echo hello"

class Proc(Blueprint[ProcConfig]):
    def render(self, config):
        from airflow.operators.bash import BashOperator
        return BashOperator(task_id=self.step_id, bash_command=config.cmd)
""")

        yaml_file = tmp_path / "pipeline.dag.yaml"
        yaml_file.write_text("""
dag_id: deprecated_alias_test
steps:
  step1:
    blueprint: proc
""")

        globals_dict: dict = {}
        with pytest.warns(DeprecationWarning, match="build_all_dags"):
            dags = build_all(
                search_path=tmp_path,
                register_globals=globals_dict,
                render_templates=False,
            )
        assert len(dags) == 1
        assert dags[0].dag_id == "deprecated_alias_test"
        assert "deprecated_alias_test" in globals_dict

    def test_build_all_with_custom_dag_args(self, tmp_path):
        from blueprint.builder import build_all_dags

        bp_file = tmp_path / "blueprints.py"
        bp_file.write_text("""
from typing import Any
from pydantic import BaseModel
from blueprint.core import Blueprint, BlueprintDagArgs

class StubConfig(BaseModel):
    x: int = 1

class Stub(Blueprint[StubConfig]):
    def render(self, config):
        from airflow.operators.bash import BashOperator
        return BashOperator(task_id=self.step_id, bash_command="echo ok")

class MyDagArgsConfig(BaseModel):
    schedule: str | None = None
    team: str = "default-team"

class MyDagArgs(BlueprintDagArgs[MyDagArgsConfig]):
    def render(self, config: MyDagArgsConfig) -> dict[str, Any]:
        kwargs = {"default_args": {"owner": config.team}}
        if config.schedule is not None:
            kwargs["schedule"] = config.schedule
        return kwargs
""")

        yaml_file = tmp_path / "test.dag.yaml"
        yaml_file.write_text("""
dag_id: dagargs_test
schedule: "@weekly"
team: analytics
steps:
  s1:
    blueprint: stub
""")

        globals_dict = {}
        dags = build_all_dags(
            search_path=tmp_path,
            register_globals=globals_dict,
            render_templates=False,
        )
        assert len(dags) == 1
        assert dags[0].schedule == "@weekly"
        assert dags[0].default_args["owner"] == "analytics"

    def test_build_all_no_yamls(self, tmp_path):
        from blueprint.builder import build_all_dags

        globals_dict = {}
        dags = build_all_dags(search_path=tmp_path, register_globals=globals_dict)
        assert dags == []

    def test_build_all_duplicate_dag_id(self, tmp_path):
        from blueprint.builder import build_all_dags
        from blueprint.errors import DuplicateDAGIdError

        bp_file = tmp_path / "blueprints.py"
        bp_file.write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class StubConfig(BaseModel):
    x: int = 1

class Stub(Blueprint[StubConfig]):
    def render(self, config):
        from airflow.operators.bash import BashOperator
        return BashOperator(task_id=self.step_id, bash_command="echo ok")
""")

        yaml1 = tmp_path / "first.dag.yaml"
        yaml1.write_text("dag_id: same_id\nsteps:\n  s1:\n    blueprint: stub\n")

        yaml2 = tmp_path / "second.dag.yaml"
        yaml2.write_text("dag_id: same_id\nsteps:\n  s2:\n    blueprint: stub\n")

        globals_dict = {}
        with pytest.raises(DuplicateDAGIdError, match="same_id"):
            build_all_dags(
                search_path=tmp_path,
                register_globals=globals_dict,
                render_templates=False,
            )

    def test_build_all_duplicate_dag_id_only_first_registered(self, tmp_path):
        from blueprint.builder import build_all_dags
        from blueprint.errors import DuplicateDAGIdError

        bp_file = tmp_path / "blueprints.py"
        bp_file.write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class StubConfig(BaseModel):
    x: int = 1

class Stub(Blueprint[StubConfig]):
    def render(self, config):
        from airflow.operators.bash import BashOperator
        return BashOperator(task_id=self.step_id, bash_command="echo ok")
""")

        yaml1 = tmp_path / "first.dag.yaml"
        yaml1.write_text("dag_id: dup_id\nsteps:\n  s1:\n    blueprint: stub\n")

        yaml2 = tmp_path / "second.dag.yaml"
        yaml2.write_text("dag_id: dup_id\nsteps:\n  s2:\n    blueprint: stub\n")

        globals_dict = {}
        with pytest.raises(DuplicateDAGIdError):
            build_all_dags(
                search_path=tmp_path,
                register_globals=globals_dict,
                render_templates=False,
            )
        assert len(globals_dict) <= 1

    def test_build_all_raises_on_error(self, tmp_path):
        from blueprint.builder import build_all_dags
        from blueprint.errors import BlueprintNotFoundError

        bp_file = tmp_path / "blueprints.py"
        bp_file.write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class StubConfig(BaseModel):
    x: int = 1

class Stub(Blueprint[StubConfig]):
    def render(self, config):
        from airflow.operators.bash import BashOperator
        return BashOperator(task_id=self.step_id, bash_command="echo ok")
""")

        yaml_bad = tmp_path / "bad.dag.yaml"
        yaml_bad.write_text("dag_id: bad\nsteps:\n  s:\n    blueprint: nonexistent\n")

        globals_dict = {}
        with pytest.raises(BlueprintNotFoundError):
            build_all_dags(
                search_path=tmp_path,
                register_globals=globals_dict,
                render_templates=False,
            )


class TestOnDagBuilt:
    def test_build_all_on_dag_built_called(self, tmp_path):
        from blueprint.builder import build_all_dags

        bp_file = tmp_path / "blueprints.py"
        bp_file.write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class StubConfig(BaseModel):
    x: int = 1

class Stub(Blueprint[StubConfig]):
    def render(self, config):
        from airflow.operators.bash import BashOperator
        return BashOperator(task_id=self.step_id, bash_command="echo ok")
""")

        yaml_file = tmp_path / "test.dag.yaml"
        yaml_file.write_text("dag_id: cb_test\nsteps:\n  s1:\n    blueprint: stub\n")

        calls = []

        def callback(dag, yaml_path):
            calls.append((dag.dag_id, yaml_path))

        globals_dict = {}
        build_all_dags(
            search_path=tmp_path,
            register_globals=globals_dict,
            render_templates=False,
            on_dag_built=callback,
        )
        assert len(calls) == 1
        assert calls[0][0] == "cb_test"
        assert calls[0][1] == yaml_file

    def test_build_all_on_dag_built_mutates_dag(self, tmp_path):
        from blueprint.builder import build_all_dags

        bp_file = tmp_path / "blueprints.py"
        bp_file.write_text("""
from pydantic import BaseModel
from blueprint.core import Blueprint

class StubConfig(BaseModel):
    x: int = 1

class Stub(Blueprint[StubConfig]):
    def render(self, config):
        from airflow.operators.bash import BashOperator
        return BashOperator(task_id=self.step_id, bash_command="echo ok")
""")

        yaml_file = tmp_path / "test.dag.yaml"
        yaml_file.write_text("dag_id: mutate_test\nsteps:\n  s1:\n    blueprint: stub\n")

        def add_tag(dag, _yaml_path):
            dag.tags = [*(dag.tags or []), "post-processed"]

        globals_dict = {}
        dags = build_all_dags(
            search_path=tmp_path,
            register_globals=globals_dict,
            render_templates=False,
            on_dag_built=add_tag,
        )
        assert "post-processed" in dags[0].tags

    def test_build_from_yaml_on_dag_built_called(self, tmp_path, test_registry):
        yaml_content = """
dag_id: yaml_cb_dag
steps:
  my_load:
    blueprint: load
    target_table: out
"""
        yaml_file = tmp_path / "test.dag.yaml"
        yaml_file.write_text(yaml_content)

        calls = []

        def callback(dag, yaml_path):
            calls.append((dag.dag_id, yaml_path))

        cb_builder = Builder(bp_registry=test_registry, on_dag_built=callback)
        dag = cb_builder.build_from_yaml(yaml_file, render_template=False)
        assert dag.dag_id == "yaml_cb_dag"
        assert len(calls) == 1
        assert calls[0][0] == "yaml_cb_dag"
        assert calls[0][1] == yaml_file


# --- Param-related test blueprints ---


class ParamBlueprintConfig(BaseModel):
    message: str = Field(description="Greeting message")
    count: int = Field(default=1, ge=1)
    mode: Literal["fast", "slow"] = "fast"


class ParamBlueprint(Blueprint[ParamBlueprintConfig]):
    supports_params = True

    def render(self, config: ParamBlueprintConfig) -> TaskOrGroup:  # noqa: ARG002
        from airflow.operators.bash import BashOperator

        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo '{self.param('message')}' mode={self.param('mode')}",
        )


@pytest.fixture
def param_registry():
    reg = BlueprintRegistry()
    reg._blueprints = {
        "extract": {1: Extract},
        "param_bp": {1: ParamBlueprint},
    }
    reg._blueprint_locations = {
        "extract": {1: "test.py"},
        "param_bp": {1: "test.py"},
    }
    reg._discovered = True
    return reg


@pytest.fixture
def param_builder(param_registry):
    return Builder(bp_registry=param_registry)


# --- _config_to_params tests ---


class TestConfigToParams:
    def test_basic_scalar_fields(self):
        params = _config_to_params(
            ParamBlueprintConfig,
            {"message": "hello", "count": 5, "mode": "fast"},
            "greet",
        )
        assert "greet__message" in params
        assert "greet__count" in params
        assert "greet__mode" in params

        assert params["greet__message"].value == "hello"
        assert params["greet__count"].value == 5

    def test_literal_to_enum(self):
        params = _config_to_params(ParamBlueprintConfig, {"message": "hi", "mode": "fast"}, "step")
        mode_schema = params["step__mode"].schema
        assert "enum" in mode_schema
        assert set(mode_schema["enum"]) == {"fast", "slow"}

    def test_field_descriptions(self):
        params = _config_to_params(ParamBlueprintConfig, {"message": "hi"}, "step")
        assert params["step__message"].description == "Greeting message"

    def test_yaml_values_override_model_defaults(self):
        params = _config_to_params(ParamBlueprintConfig, {"message": "hi", "count": 99}, "step")
        assert params["step__count"].value == 99

    def test_model_default_used_when_no_yaml_value(self):
        params = _config_to_params(ParamBlueprintConfig, {"message": "hi"}, "step")
        assert params["step__count"].value == 1

    def test_namespacing(self):
        params = _config_to_params(ParamBlueprintConfig, {"message": "hi"}, "my_step")
        assert all(k.startswith("my_step__") for k in params)

    def test_section_set(self):
        params = _config_to_params(ParamBlueprintConfig, {"message": "hi"}, "greet")
        for p in params.values():
            assert p.schema.get("section") == "greet"

    def test_field_constraints(self):
        params = _config_to_params(ParamBlueprintConfig, {"message": "hi", "count": 1}, "step")
        count_schema = params["step__count"].schema
        assert count_schema.get("minimum") == 1

    def test_format_passthrough(self):
        class FormatConfig(BaseModel):
            query: str = Field(default="SELECT 1", json_schema_extra={"format": "multiline"})

        params = _config_to_params(FormatConfig, {"query": "SELECT 1"}, "step")
        assert params["step__query"].schema.get("format") == "multiline"

    def test_examples_passthrough(self):
        class ExamplesConfig(BaseModel):
            env: str = Field(
                default="staging",
                json_schema_extra={"examples": ["dev", "staging", "prod"]},
            )

        params = _config_to_params(ExamplesConfig, {"env": "staging"}, "step")
        assert params["step__env"].schema.get("examples") == ["dev", "staging", "prod"]

    def test_values_display_passthrough(self):
        class DisplayConfig(BaseModel):
            env: str = Field(
                default="staging",
                json_schema_extra={
                    "examples": ["dev", "staging", "prod"],
                    "values_display": {"dev": "Development", "staging": "Staging"},
                },
            )

        params = _config_to_params(DisplayConfig, {"env": "staging"}, "step")
        assert params["step__env"].schema.get("values_display") == {
            "dev": "Development",
            "staging": "Staging",
        }

    def test_description_md_passthrough(self):
        class MdConfig(BaseModel):
            name: str = Field(
                default="x",
                json_schema_extra={"description_md": "**Bold** description"},
            )

        params = _config_to_params(MdConfig, {"name": "x"}, "step")
        assert params["step__name"].schema.get("description_md") == "**Bold** description"


# --- Builder params integration tests ---


class TestBuilderParams:
    def test_dag_has_params(self, param_builder):
        from airflow import DAG

        config = DAGConfig.model_validate(
            {
                "dag_id": "params_dag",
                "steps": {
                    "greet": {
                        "blueprint": "param_bp",
                        "message": "hello",
                        "count": 3,
                    }
                },
            }
        )
        dag = param_builder.build(config)
        assert isinstance(dag, DAG)
        assert "greet__message" in dag.params
        assert "greet__count" in dag.params
        assert "greet__mode" in dag.params

    def test_multiple_steps_params(self, param_builder):
        config = DAGConfig.model_validate(
            {
                "dag_id": "multi_params",
                "steps": {
                    "greet1": {
                        "blueprint": "param_bp",
                        "message": "hello",
                    },
                    "greet2": {
                        "blueprint": "param_bp",
                        "message": "world",
                    },
                },
            }
        )
        dag = param_builder.build(config)
        assert "greet1__message" in dag.params
        assert "greet2__message" in dag.params

    def test_param_defaults_from_yaml(self, param_builder):
        config = DAGConfig.model_validate(
            {
                "dag_id": "yaml_defaults",
                "steps": {
                    "greet": {
                        "blueprint": "param_bp",
                        "message": "from-yaml",
                        "count": 42,
                    }
                },
            }
        )
        dag = param_builder.build(config)
        assert dag.params["greet__message"] == "from-yaml"
        assert dag.params["greet__count"] == 42

    def test_no_params_for_unsupported_blueprint(self, param_builder):
        config = DAGConfig.model_validate(
            {
                "dag_id": "mixed_dag",
                "steps": {
                    "e": {
                        "blueprint": "extract",
                        "source_table": "raw.data",
                    },
                    "greet": {
                        "blueprint": "param_bp",
                        "message": "hello",
                    },
                },
            }
        )
        dag = param_builder.build(config)
        assert "greet__message" in dag.params
        assert "e__source_table" not in dag.params
        assert "e__batch_size" not in dag.params


# --- Blueprint.param() tests ---


class TestBlueprintParam:
    def test_param_returns_template_string(self):
        bp = ParamBlueprint()
        bp.step_id = "greet"
        assert bp.param("message") == "{{ params.greet__message }}"
        assert bp.param("count") == "{{ params.greet__count }}"

    def test_param_invalid_field_raises(self):
        bp = ParamBlueprint()
        bp.step_id = "greet"
        with pytest.raises(ValueError, match="Unknown field 'nonexistent'"):
            bp.param("nonexistent")


# --- Blueprint.resolve_config() tests ---


class TestResolveConfig:
    def test_resolve_with_overrides(self):
        bp = ParamBlueprint()
        bp.step_id = "greet"
        config = ParamBlueprintConfig(message="default", count=1, mode="fast")
        context = {"params": {"greet__message": "overridden", "greet__count": 5}}
        resolved = bp.resolve_config(config, context)
        assert resolved.message == "overridden"
        assert resolved.count == 5
        assert resolved.mode == "fast"

    def test_resolve_without_overrides(self):
        bp = ParamBlueprint()
        bp.step_id = "greet"
        config = ParamBlueprintConfig(message="original", count=3, mode="slow")
        context = {"params": {}}
        resolved = bp.resolve_config(config, context)
        assert resolved.message == "original"
        assert resolved.count == 3
        assert resolved.mode == "slow"

    def test_resolve_ignores_other_step_params(self):
        bp = ParamBlueprint()
        bp.step_id = "greet"
        config = ParamBlueprintConfig(message="mine", count=1, mode="fast")
        context = {"params": {"other__message": "not mine"}}
        resolved = bp.resolve_config(config, context)
        assert resolved.message == "mine"

    def test_resolve_validates(self):
        from pydantic import ValidationError

        bp = ParamBlueprint()
        bp.step_id = "greet"
        config = ParamBlueprintConfig(message="ok", count=1, mode="fast")
        context = {"params": {"greet__count": -1}}
        with pytest.raises(ValidationError):
            bp.resolve_config(config, context)
