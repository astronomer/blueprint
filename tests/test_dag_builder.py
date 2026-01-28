"""Tests for Blueprint.build_all() and related functionality."""

import pytest
from airflow import DAG

from blueprint import Blueprint, BaseModel, Field
from blueprint.errors import DuplicateDAGIdError


class TestGetBlueprintName:
    """Tests for Blueprint._get_blueprint_name()."""

    def test_simple_name(self):
        """Test simple class name conversion."""

        class DailyETL(Blueprint):
            pass

        assert DailyETL._get_blueprint_name() == "daily_etl"

    def test_consecutive_capitals(self):
        """Test class names with consecutive capitals."""

        class HTTPAPIClient(Blueprint):
            pass

        assert HTTPAPIClient._get_blueprint_name() == "httpapi_client"

    def test_single_word(self):
        """Test single word class name."""

        class Pipeline(Blueprint):
            pass

        assert Pipeline._get_blueprint_name() == "pipeline"

    def test_multiple_parts(self):
        """Test class name with multiple parts."""

        class DataWarehouseETLPipeline(Blueprint):
            pass

        assert DataWarehouseETLPipeline._get_blueprint_name() == "data_warehouse_etl_pipeline"


class TestInjectBlueprintConfig:
    """Tests for Blueprint._inject_blueprint_config()."""

    def test_injects_config_to_all_tasks(self, tmp_path):
        """Test that config is injected into all tasks."""
        from datetime import datetime

        from airflow.operators.bash import BashOperator

        dag = DAG(dag_id="test_dag", start_date=datetime(2024, 1, 1))
        with dag:
            task1 = BashOperator(task_id="task1", bash_command="echo 1")
            task2 = BashOperator(task_id="task2", bash_command="echo 2")
            task1 >> task2

        rendered_yaml = "job_id: test\nschedule: '@daily'"

        Blueprint._inject_blueprint_config(dag, rendered_yaml)

        # Check that config was injected into ALL tasks
        for task in [task1, task2]:
            assert hasattr(task, "blueprint_config")
            assert task.blueprint_config == rendered_yaml
            assert "blueprint_config" in task.template_fields

    def test_no_tasks(self):
        """Test with DAG that has no tasks."""
        from datetime import datetime

        dag = DAG(dag_id="empty_dag", start_date=datetime(2024, 1, 1))

        # Should not raise
        Blueprint._inject_blueprint_config(dag, "config: value")


class TestBuildAll:
    """Tests for Blueprint.build_all()."""

    def test_discovers_matching_yaml(self, tmp_path):
        """Test that build_all discovers YAML files for the blueprint."""
        # Create a test blueprint
        class TestConfig(BaseModel):
            job_id: str
            schedule: str = "@daily"

        class TestBlueprint(Blueprint[TestConfig]):
            def render(self, config: TestConfig) -> DAG:
                from datetime import datetime

                return DAG(
                    dag_id=config.job_id,
                    schedule=config.schedule,
                    start_date=datetime(2024, 1, 1),
                )

        # Create YAML files
        yaml1 = tmp_path / "test1.dag.yaml"
        yaml1.write_text("blueprint: test_blueprint\njob_id: test-job-1\n")

        yaml2 = tmp_path / "test2.dag.yaml"
        yaml2.write_text("blueprint: test_blueprint\njob_id: test-job-2\n")

        # Create a YAML for a different blueprint (should be ignored)
        yaml3 = tmp_path / "other.dag.yaml"
        yaml3.write_text("blueprint: other_blueprint\njob_id: other-job\n")

        # Build DAGs
        test_globals = {}
        dags = TestBlueprint.build_all(register_globals=test_globals, search_path=tmp_path)

        assert len(dags) == 2
        assert "test-job-1" in test_globals
        assert "test-job-2" in test_globals
        assert "other-job" not in test_globals

    def test_detects_duplicate_dag_ids(self, tmp_path):
        """Test that duplicate DAG IDs raise an error."""

        class TestConfig(BaseModel):
            job_id: str

        class TestBlueprint(Blueprint[TestConfig]):
            def render(self, config: TestConfig) -> DAG:
                from datetime import datetime

                return DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))

        # Create YAML files with same job_id
        yaml1 = tmp_path / "config1.dag.yaml"
        yaml1.write_text("blueprint: test_blueprint\njob_id: duplicate-id\n")

        yaml2 = tmp_path / "config2.dag.yaml"
        yaml2.write_text("blueprint: test_blueprint\njob_id: duplicate-id\n")

        test_globals = {}
        with pytest.raises(DuplicateDAGIdError):
            TestBlueprint.build_all(register_globals=test_globals, search_path=tmp_path)

    def test_renders_jinja_templates(self, tmp_path, monkeypatch):
        """Test that Jinja2 templates in YAML are rendered."""
        monkeypatch.setenv("TEST_ENV", "production")

        class TestConfig(BaseModel):
            job_id: str

        class TestBlueprint(Blueprint[TestConfig]):
            def render(self, config: TestConfig) -> DAG:
                from datetime import datetime

                return DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))

        # Create YAML with Jinja2 template
        yaml_file = tmp_path / "templated.dag.yaml"
        yaml_file.write_text('blueprint: test_blueprint\njob_id: "{{ env.TEST_ENV }}-job"\n')

        test_globals = {}
        dags = TestBlueprint.build_all(register_globals=test_globals, search_path=tmp_path)

        assert len(dags) == 1
        assert "production-job" in test_globals

    def test_recursive_discovery(self, tmp_path):
        """Test that YAML files in subdirectories are discovered."""

        class TestConfig(BaseModel):
            job_id: str

        class TestBlueprint(Blueprint[TestConfig]):
            def render(self, config: TestConfig) -> DAG:
                from datetime import datetime

                return DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))

        # Create nested directory structure
        subdir = tmp_path / "subdir" / "nested"
        subdir.mkdir(parents=True)

        yaml_root = tmp_path / "root.dag.yaml"
        yaml_root.write_text("blueprint: test_blueprint\njob_id: root-job\n")

        yaml_nested = subdir / "nested.dag.yaml"
        yaml_nested.write_text("blueprint: test_blueprint\njob_id: nested-job\n")

        test_globals = {}
        dags = TestBlueprint.build_all(register_globals=test_globals, search_path=tmp_path)

        assert len(dags) == 2
        assert "root-job" in test_globals
        assert "nested-job" in test_globals

    def test_empty_directory(self, tmp_path):
        """Test with directory containing no YAML files."""

        class TestConfig(BaseModel):
            job_id: str

        class TestBlueprint(Blueprint[TestConfig]):
            def render(self, config: TestConfig) -> DAG:
                from datetime import datetime

                return DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))

        test_globals = {}
        dags = TestBlueprint.build_all(register_globals=test_globals, search_path=tmp_path)

        assert len(dags) == 0

    def test_nonexistent_directory(self, tmp_path):
        """Test with nonexistent directory."""

        class TestConfig(BaseModel):
            job_id: str

        class TestBlueprint(Blueprint[TestConfig]):
            def render(self, config: TestConfig) -> DAG:
                from datetime import datetime

                return DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))

        nonexistent = tmp_path / "does_not_exist"

        test_globals = {}
        dags = TestBlueprint.build_all(register_globals=test_globals, search_path=nonexistent)

        assert len(dags) == 0

    def test_custom_pattern(self, tmp_path):
        """Test with custom file pattern."""

        class TestConfig(BaseModel):
            job_id: str

        class TestBlueprint(Blueprint[TestConfig]):
            def render(self, config: TestConfig) -> DAG:
                from datetime import datetime

                return DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))

        # Create files with different extensions
        yaml1 = tmp_path / "config.dag.yaml"
        yaml1.write_text("blueprint: test_blueprint\njob_id: dag-yaml-job\n")

        yaml2 = tmp_path / "config.yaml"
        yaml2.write_text("blueprint: test_blueprint\njob_id: plain-yaml-job\n")

        # Only discover *.yaml (not *.dag.yaml)
        test_globals = {}
        dags = TestBlueprint.build_all(
            register_globals=test_globals, search_path=tmp_path, pattern="*.yaml"
        )

        # Both should be found with *.yaml pattern
        assert len(dags) == 2

    def test_render_templates_disabled(self, tmp_path, monkeypatch):
        """Test with template rendering disabled uses literal values."""
        monkeypatch.setenv("TEST_ENV", "production")

        class TestConfig(BaseModel):
            job_id: str
            env_value: str = ""

        class TestBlueprint(Blueprint[TestConfig]):
            def render(self, config: TestConfig) -> DAG:
                from datetime import datetime

                return DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))

        # Create YAML - job_id is static, but env_value uses template
        yaml_file = tmp_path / "templated.dag.yaml"
        yaml_file.write_text(
            'blueprint: test_blueprint\njob_id: static-job\nenv_value: "{{ env.TEST_ENV }}"\n'
        )

        test_globals = {}
        dags = TestBlueprint.build_all(
            register_globals=test_globals, search_path=tmp_path, render_templates=False
        )

        # DAG should be built with the static job_id
        assert len(dags) == 1
        assert "static-job" in test_globals

    def test_custom_template_context(self, tmp_path):
        """Test that custom template_context is passed through to rendering."""

        class TestConfig(BaseModel):
            job_id: str

        class TestBlueprint(Blueprint[TestConfig]):
            def render(self, config: TestConfig) -> DAG:
                from datetime import datetime

                return DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))

        # Create YAML with custom variable
        yaml_file = tmp_path / "custom.dag.yaml"
        yaml_file.write_text('blueprint: test_blueprint\njob_id: "{{ custom_var }}-job"\n')

        test_globals = {}
        dags = TestBlueprint.build_all(
            register_globals=test_globals,
            search_path=tmp_path,
            template_context={"custom_var": "my_custom"},
        )

        assert len(dags) == 1
        assert "my_custom-job" in test_globals

    def test_continues_on_invalid_yaml(self, tmp_path):
        """Test that build_all continues processing after encountering invalid YAML."""

        class TestConfig(BaseModel):
            job_id: str

        class TestBlueprint(Blueprint[TestConfig]):
            def render(self, config: TestConfig) -> DAG:
                from datetime import datetime

                return DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))

        # Create one valid and one invalid YAML
        valid_yaml = tmp_path / "valid.dag.yaml"
        valid_yaml.write_text("blueprint: test_blueprint\njob_id: valid-job\n")

        invalid_yaml = tmp_path / "invalid.dag.yaml"
        invalid_yaml.write_text("blueprint: test_blueprint\njob_id: {{ broken template\n")

        test_globals = {}
        dags = TestBlueprint.build_all(register_globals=test_globals, search_path=tmp_path)

        # Should have processed the valid one despite the error on invalid
        assert len(dags) == 1
        assert "valid-job" in test_globals

    def test_injects_blueprint_config_to_dag(self, tmp_path):
        """Test that rendered YAML is injected into DAG tasks."""

        class TestConfig(BaseModel):
            job_id: str
            schedule: str = "@daily"

        class TestBlueprint(Blueprint[TestConfig]):
            def render(self, config: TestConfig) -> DAG:
                from datetime import datetime
                from airflow.operators.bash import BashOperator

                dag = DAG(dag_id=config.job_id, start_date=datetime(2024, 1, 1))
                with dag:
                    BashOperator(task_id="task1", bash_command="echo 1")
                return dag

        yaml_file = tmp_path / "config.dag.yaml"
        yaml_file.write_text("blueprint: test_blueprint\njob_id: inject-test\nschedule: '@hourly'\n")

        test_globals = {}
        dags = TestBlueprint.build_all(register_globals=test_globals, search_path=tmp_path)

        assert len(dags) == 1
        dag = dags[0]
        task = dag.tasks[0]

        # Verify blueprint_config was injected
        assert hasattr(task, "blueprint_config")
        assert "inject-test" in task.blueprint_config
        assert "@hourly" in task.blueprint_config
        assert "blueprint_config" in task.template_fields


class TestBuildFromYaml:
    """Test the build_from_yaml class method."""

    def test_basic_build_from_yaml(self, tmp_path):
        """Test basic YAML loading and DAG building."""

        class TestConfig(BaseModel):
            job_id: str
            value: str = "default"

        class TestBlueprint(Blueprint[TestConfig]):
            def render(self, config: TestConfig):
                from airflow import DAG

                return DAG(dag_id=config.job_id)

        # Create YAML config
        yaml_content = """
blueprint: test_blueprint
job_id: yaml-test-dag
value: from-yaml
"""
        yaml_file = tmp_path / "test.dag.yaml"
        yaml_file.write_text(yaml_content)

        # Build from YAML
        dag = TestBlueprint.build_from_yaml(yaml_file)

        assert dag.dag_id == "yaml-test-dag"

    def test_build_from_yaml_with_overrides(self, tmp_path):
        """Test YAML loading with runtime overrides."""

        class TestConfig(BaseModel):
            job_id: str
            retries: int = 2

        class TestBlueprint(Blueprint[TestConfig]):
            def render(self, config: TestConfig):
                from airflow import DAG

                return DAG(dag_id=config.job_id, default_args={"retries": config.retries})

        # Create YAML config
        yaml_content = """
blueprint: test_blueprint
job_id: override-test
retries: 1
"""
        yaml_file = tmp_path / "test.dag.yaml"
        yaml_file.write_text(yaml_content)

        # Build with overrides
        dag = TestBlueprint.build_from_yaml(yaml_file, overrides={"retries": 5})

        assert dag.dag_id == "override-test"
        assert dag.default_args["retries"] == 5

    def test_build_from_yaml_with_jinja_templates(self, tmp_path, monkeypatch):
        """Test YAML loading with Jinja2 template rendering."""
        monkeypatch.setenv("TEST_ENV_VAR", "env-value")

        class TestConfig(BaseModel):
            job_id: str

        class TestBlueprint(Blueprint[TestConfig]):
            def render(self, config: TestConfig):
                from airflow import DAG

                return DAG(dag_id=config.job_id)

        # Create YAML config with Jinja2 template
        yaml_content = """
blueprint: test_blueprint
job_id: "{{ env.TEST_ENV_VAR }}-dag"
"""
        yaml_file = tmp_path / "test.dag.yaml"
        yaml_file.write_text(yaml_content)

        # Build from YAML (templates rendered by default)
        dag = TestBlueprint.build_from_yaml(yaml_file)

        assert dag.dag_id == "env-value-dag"

    def test_build_from_yaml_without_template_rendering(self, tmp_path):
        """Test YAML loading with template rendering disabled."""

        class TestConfig(BaseModel):
            job_id: str

        class TestBlueprint(Blueprint[TestConfig]):
            def render(self, config: TestConfig):
                from airflow import DAG

                return DAG(dag_id=config.job_id)

        # Create YAML config with literal braces (not Jinja2)
        yaml_content = """
blueprint: test_blueprint
job_id: literal-dag
"""
        yaml_file = tmp_path / "test.dag.yaml"
        yaml_file.write_text(yaml_content)

        # Build without template rendering
        dag = TestBlueprint.build_from_yaml(yaml_file, render_template=False)

        assert dag.dag_id == "literal-dag"

    def test_build_from_yaml_absolute_path(self, tmp_path):
        """Test that absolute paths work correctly."""

        class TestConfig(BaseModel):
            job_id: str

        class TestBlueprint(Blueprint[TestConfig]):
            def render(self, config: TestConfig):
                from airflow import DAG

                return DAG(dag_id=config.job_id)

        # Create YAML config in tmp_path
        yaml_content = """
blueprint: test_blueprint
job_id: absolute-path-test
"""
        yaml_file = tmp_path / "config.dag.yaml"
        yaml_file.write_text(yaml_content)

        # Use absolute path
        dag = TestBlueprint.build_from_yaml(yaml_file)
        assert dag.dag_id == "absolute-path-test"

    def test_build_from_yaml_injects_config(self, tmp_path):
        """Test that rendered YAML config is injected into task template_fields."""
        from airflow.operators.bash import BashOperator

        class TestConfig(BaseModel):
            job_id: str
            message: str = "hello"

        class TestBlueprint(Blueprint[TestConfig]):
            def render(self, config: TestConfig):
                from airflow import DAG

                dag = DAG(dag_id=config.job_id)
                with dag:
                    BashOperator(task_id="task", bash_command=f"echo {config.message}")
                return dag

        # Create YAML config
        yaml_content = """
blueprint: test_blueprint
job_id: inject-config-test
message: world
"""
        yaml_file = tmp_path / "test.dag.yaml"
        yaml_file.write_text(yaml_content)

        dag = TestBlueprint.build_from_yaml(yaml_file)
        task = dag.tasks[0]

        # Verify blueprint_config was injected
        assert hasattr(task, "blueprint_config")
        assert "inject-config-test" in task.blueprint_config
        assert "world" in task.blueprint_config
        assert "blueprint_config" in task.template_fields
