"""Tests for the DAG builder: DAGConfig, StepConfig, Builder, build_all."""

from pathlib import Path

import pytest
import yaml
from pydantic import BaseModel

from blueprint.builder import Builder, DAGConfig, StepConfig
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


# --- Fixtures ---


@pytest.fixture
def test_registry():
    reg = BlueprintRegistry()
    reg._blueprints = {
        "extract": {1: Extract, 2: ExtractV2},
        "load": {1: Load},
        "nested": {1: Nested},
    }
    reg._blueprint_locations = {
        "extract": {1: "test.py", 2: "test.py"},
        "load": {1: "test.py"},
        "nested": {1: "test.py"},
    }
    reg._discovered = True
    return reg


@pytest.fixture
def builder(test_registry):
    return Builder(bp_registry=test_registry)


@pytest.fixture
def builder_with_defaults(test_registry):
    return Builder(
        bp_registry=test_registry,
        dag_defaults={
            "schedule": "@daily",
            "tags": ["managed"],
            "default_args": {
                "owner": "data-team",
                "retries": 2,
            },
        },
    )


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


# --- DAGConfig tests ---


class TestDAGConfig:
    def test_basic_dag_config(self):
        config = DAGConfig(
            dag_id="test_dag",
            steps={"s1": StepConfig(blueprint="extract", source_table="raw.data")},
        )
        assert config.dag_id == "test_dag"
        assert config.schedule is None
        assert config.catchup is False

    def test_full_dag_config(self):
        config = DAGConfig(
            dag_id="test_dag",
            schedule="@daily",
            tags=["etl"],
            catchup=True,
            default_args={"owner": "me"},
            steps={"s1": StepConfig(blueprint="extract", source_table="raw.data")},
        )
        assert config.schedule == "@daily"
        assert config.tags == ["etl"]
        assert config.catchup is True
        assert config.default_args == {"owner": "me"}

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


class TestBuilderStartDateAndRetry:
    def test_retry_delay_seconds_converted(self, builder):
        config = DAGConfig(
            dag_id="retry_dag",
            default_args={"retry_delay_seconds": 300},
            steps={"s": StepConfig(blueprint="load", target_table="out")},
        )
        dag = builder.build(config)
        from datetime import timedelta

        assert dag.default_args["retry_delay"] == timedelta(seconds=300)
        assert "retry_delay_seconds" not in dag.default_args

    def test_retry_delay_seconds_string_coerced(self, builder):
        config = DAGConfig(
            dag_id="retry_str_dag",
            default_args={"retry_delay_seconds": "120"},
            steps={"s": StepConfig(blueprint="load", target_table="out")},
        )
        dag = builder.build(config)
        from datetime import timedelta

        assert dag.default_args["retry_delay"] == timedelta(seconds=120)

    def test_retry_delay_seconds_invalid_raises(self, builder):
        from blueprint.errors import ConfigurationError

        config = DAGConfig(
            dag_id="retry_bad_dag",
            default_args={"retry_delay_seconds": "not_a_number"},
            steps={"s": StepConfig(blueprint="load", target_table="out")},
        )
        with pytest.raises(ConfigurationError, match="retry_delay_seconds must be a number"):
            builder.build(config)

    def test_start_date_iso_with_tz(self, builder):
        config = DAGConfig(
            dag_id="tz_dag",
            start_date="2025-06-01T00:00:00+00:00",
            steps={"s": StepConfig(blueprint="load", target_table="out")},
        )
        dag = builder.build(config)
        from datetime import datetime, timezone

        assert dag.start_date == datetime(2025, 6, 1, tzinfo=timezone.utc)

    def test_start_date_naive_gets_utc(self, builder):
        config = DAGConfig(
            dag_id="naive_dag",
            start_date="2025-06-01",
            steps={"s": StepConfig(blueprint="load", target_table="out")},
        )
        dag = builder.build(config)

        assert dag.start_date.tzinfo is not None
        assert dag.start_date.utcoffset().total_seconds() == 0

    def test_description_passed_to_dag(self, builder):
        config = DAGConfig(
            dag_id="desc_dag",
            description="A test DAG",
            steps={"s": StepConfig(blueprint="load", target_table="out")},
        )
        dag = builder.build(config)
        assert dag.description == "A test DAG"


class TestBuilderDefaults:
    def test_defaults_applied(self, builder_with_defaults):
        config = DAGConfig(
            dag_id="defaulted",
            steps={"s": StepConfig(blueprint="load", target_table="out")},
        )
        dag = builder_with_defaults.build(config)
        assert dag.schedule == "@daily"
        tags = set(dag.tags) if dag.tags else set()
        assert "managed" in tags

    def test_yaml_overrides_defaults(self, builder_with_defaults):
        config = DAGConfig(
            dag_id="overridden",
            schedule="@hourly",
            tags=["custom"],
            steps={"s": StepConfig(blueprint="load", target_table="out")},
        )
        dag = builder_with_defaults.build(config)
        assert dag.schedule == "@hourly"
        assert "custom" in dag.tags

    def test_default_args_deep_merged(self, builder_with_defaults):
        config = DAGConfig(
            dag_id="merged_args",
            default_args={"retries": 5, "email": "me@test.com"},
            steps={"s": StepConfig(blueprint="load", target_table="out")},
        )
        dag = builder_with_defaults.build(config)
        assert dag.default_args["owner"] == "data-team"
        assert dag.default_args["retries"] == 5
        assert dag.default_args["email"] == "me@test.com"

    def test_explicit_null_schedule_preserved(self, builder_with_defaults):
        config = DAGConfig(
            dag_id="unscheduled",
            schedule=None,
            steps={"s": StepConfig(blueprint="load", target_table="out")},
        )
        raw_yaml = {
            "dag_id": "unscheduled",
            "schedule": None,
            "steps": {"s": {"blueprint": "load", "target_table": "out"}},
        }
        dag = builder_with_defaults.build(config, raw_yaml=raw_yaml)
        assert dag.schedule is None

    def test_absent_schedule_gets_default(self, builder_with_defaults):
        config = DAGConfig(
            dag_id="defaulted_schedule",
            steps={"s": StepConfig(blueprint="load", target_table="out")},
        )
        raw_yaml = {
            "dag_id": "defaulted_schedule",
            "steps": {"s": {"blueprint": "load", "target_table": "out"}},
        }
        dag = builder_with_defaults.build(config, raw_yaml=raw_yaml)
        assert dag.schedule == "@daily"


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
dag_id: build_all_test
steps:
  step1:
    blueprint: proc
""")

        globals_dict = {}
        dags = build_all(
            search_path=tmp_path,
            register_globals=globals_dict,
            render_templates=False,
        )
        assert len(dags) == 1
        assert dags[0].dag_id == "build_all_test"
        assert "build_all_test" in globals_dict

    def test_build_all_with_defaults(self, tmp_path):
        from blueprint.builder import build_all

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
        yaml_file.write_text("""
dag_id: defaults_test
steps:
  s1:
    blueprint: stub
""")

        globals_dict = {}
        dags = build_all(
            search_path=tmp_path,
            dag_defaults={"schedule": "@weekly", "tags": ["auto"]},
            register_globals=globals_dict,
            render_templates=False,
        )
        assert len(dags) == 1
        assert dags[0].schedule == "@weekly"
        assert "auto" in dags[0].tags

    def test_build_all_no_yamls(self, tmp_path):
        from blueprint.builder import build_all

        globals_dict = {}
        dags = build_all(search_path=tmp_path, register_globals=globals_dict)
        assert dags == []

    def test_build_all_duplicate_dag_id(self, tmp_path):
        from blueprint.builder import build_all
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
            build_all(
                search_path=tmp_path,
                register_globals=globals_dict,
                render_templates=False,
            )

    def test_build_all_duplicate_dag_id_only_first_registered(self, tmp_path):
        from blueprint.builder import build_all
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
            build_all(
                search_path=tmp_path,
                register_globals=globals_dict,
                render_templates=False,
            )
        assert len(globals_dict) <= 1

    def test_build_all_fail_fast_raises(self, tmp_path):
        from blueprint.builder import build_all
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
            build_all(
                search_path=tmp_path,
                register_globals=globals_dict,
                render_templates=False,
                fail_fast=True,
            )

    def test_build_all_no_fail_fast_continues(self, tmp_path):
        from blueprint.builder import build_all

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

        yaml_good = tmp_path / "good.dag.yaml"
        yaml_good.write_text("dag_id: good\nsteps:\n  s:\n    blueprint: stub\n")

        globals_dict = {}
        dags = build_all(
            search_path=tmp_path,
            register_globals=globals_dict,
            render_templates=False,
            fail_fast=False,
        )
        dag_ids = [d.dag_id for d in dags]
        assert "good" in dag_ids
