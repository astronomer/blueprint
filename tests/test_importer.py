"""Tests for the native Airflow Dag importer."""

from types import SimpleNamespace

import pytest

pytest.importorskip("airflow.sdk.importers", reason="DagImporter needs Airflow 3.4+")

from airflow.sdk.importers import DagImporterRegistry, FilesystemDagDefinition

from blueprint.builder import SOURCE_YAML_KEY
from blueprint.importer import BlueprintDagImporter
from tests.conftest import write_dag_args, write_dag_yaml, write_stub_blueprint


def _bundle(path):
    return SimpleNamespace(name="test", path=path)


@pytest.fixture
def bundle_dir(tmp_path):
    write_stub_blueprint(tmp_path)
    return tmp_path


def _import(importer, bundle_dir, name):
    definition = FilesystemDagDefinition(path=bundle_dir / name)
    return importer.import_definition(definition, _bundle(bundle_dir))


class TestListing:
    def test_lists_only_dag_yaml_files(self, bundle_dir):
        """Only files matching the default ``*.dag.yaml`` pattern are listed; other YAML in the bundle is ignored."""
        write_dag_yaml(bundle_dir, "one")
        (bundle_dir / "blueprint.vars.yaml").write_text("steps: {}\n")
        (bundle_dir / "notes.yaml").write_text("a: 1\n")

        found = list(BlueprintDagImporter().list_dag_definitions(_bundle(bundle_dir)))

        assert [d.path.name for d in found] == ["one.dag.yaml"]

    def test_safe_mode_skips_files_without_steps(self, bundle_dir):
        """With safe mode on, a ``.dag.yaml`` with no ``steps`` key is skipped; with it off, the file is still listed."""
        (bundle_dir / "other.dag.yaml").write_text("dag_id: x\n")

        importer = BlueprintDagImporter()

        assert list(importer.list_dag_definitions(_bundle(bundle_dir), safe_mode=True)) == []
        assert len(list(importer.list_dag_definitions(_bundle(bundle_dir), safe_mode=False))) == 1

    def test_respects_airflowignore(self, bundle_dir):
        """Files under a path matched by ``.airflowignore`` are not listed."""
        write_dag_yaml(bundle_dir, "kept")
        write_dag_yaml(bundle_dir / "skipped", "ignored")
        (bundle_dir / ".airflowignore").write_text("skipped\n")

        found = list(BlueprintDagImporter().list_dag_definitions(_bundle(bundle_dir)))

        assert [d.path.name for d in found] == ["kept.dag.yaml"]

    def test_custom_pattern(self, bundle_dir):
        """A custom ``pattern`` replaces the default, so a plain ``.yaml`` file is listed when the pattern is ``*``."""
        (bundle_dir / "plain.yaml").write_text("dag_id: p\nsteps:\n  s1:\n    blueprint: stub\n")

        found = list(BlueprintDagImporter(pattern="*").list_dag_definitions(_bundle(bundle_dir)))

        assert [d.path.name for d in found] == ["plain.yaml"]

    def test_can_handle(self):
        """By default only ``.yaml`` names are claimed, not ``.yml`` or Python files."""
        importer = BlueprintDagImporter()
        assert importer.can_handle("a.dag.yaml")
        assert not importer.can_handle("a.yml")
        assert not importer.can_handle("a.py")

    def test_extensions_override(self):
        """The ``extensions`` argument replaces the default, claiming ``.yml`` and dropping ``.yaml``."""
        importer = BlueprintDagImporter(extensions=["yml"])
        assert importer.can_handle("a.dag.yml")
        assert not importer.can_handle("a.dag.yaml")


class TestImport:
    def test_builds_dag(self, bundle_dir):
        """A valid ``.dag.yaml`` imports into a single Dag with the right id and tasks and no errors."""
        write_dag_yaml(bundle_dir, "my_dag")

        result = _import(BlueprintDagImporter(), bundle_dir, "my_dag.dag.yaml")

        assert result.success
        assert [d.dag_id for d in result.dags] == ["my_dag"]
        assert set(result.dags[0].task_dict) == {"s1"}

    def test_embeds_source_by_default(self, bundle_dir):
        """The YAML text is stored in ``default_args`` for the UI plugin, and ``embed_source=False`` turns that off."""
        path = write_dag_yaml(bundle_dir, "my_dag")

        dag = _import(BlueprintDagImporter(), bundle_dir, "my_dag.dag.yaml").dags[0]
        assert dag.default_args[SOURCE_YAML_KEY] == path.read_text()

        dag = _import(BlueprintDagImporter(embed_source=False), bundle_dir, "my_dag.dag.yaml").dags[
            0
        ]
        assert SOURCE_YAML_KEY not in dag.default_args

    def test_uses_dag_args_from_bundle(self, bundle_dir):
        """A ``BlueprintDagArgs`` template defined in the bundle is found and applied to the built Dag."""
        write_dag_args(bundle_dir, "Team", default=True)
        write_dag_yaml(bundle_dir, "my_dag")

        dag = _import(BlueprintDagImporter(), bundle_dir, "my_dag.dag.yaml").dags[0]

        assert "Team" in dag.tags

    def test_unknown_blueprint_is_import_error(self, bundle_dir):
        """Referencing a blueprint that does not exist is reported as an import error naming it, and no Dag is built."""
        (bundle_dir / "bad.dag.yaml").write_text(
            "dag_id: bad\nsteps:\n  s1:\n    blueprint: nope\n"
        )

        result = _import(BlueprintDagImporter(), bundle_dir, "bad.dag.yaml")

        assert not result.success
        assert result.dags == []
        assert "nope" in result.errors[0].message

    def test_invalid_yaml_is_import_error(self, bundle_dir):
        """A file that is not valid YAML is reported as an import error instead of raising."""
        (bundle_dir / "bad.dag.yaml").write_text("dag_id: x\n  bad: indent\n")

        result = _import(BlueprintDagImporter(), bundle_dir, "bad.dag.yaml")

        assert not result.success

    def test_file_without_steps_yields_nothing(self, bundle_dir):
        """A YAML file with no ``steps`` is not a Dag definition and imports as an empty, successful result."""
        (bundle_dir / "other.dag.yaml").write_text("dag_id: x\n")

        result = _import(BlueprintDagImporter(), bundle_dir, "other.dag.yaml")

        assert result.success
        assert result.dags == []

    def test_file_scoped_bundle_path(self, bundle_dir):
        """When the bundle path is the single file being parsed, the file's directory is used to find blueprints."""
        path = write_dag_yaml(bundle_dir, "my_dag")

        result = BlueprintDagImporter().import_definition(
            FilesystemDagDefinition(path=path), _bundle(path)
        )

        assert [d.dag_id for d in result.dags] == ["my_dag"]

    def test_source_code(self, bundle_dir):
        """``get_source_code`` returns the file's text with language ``yaml``, as shown on the Code tab."""
        path = write_dag_yaml(bundle_dir, "my_dag")

        source = BlueprintDagImporter().get_source_code(FilesystemDagDefinition(path=path))

        assert source.language == "yaml"
        assert source.source_code == path.read_text()


class TestRegistry:
    def test_configured_via_dag_importer_configs(self, bundle_dir):
        """Registering the importer by classpath alone routes ``.yaml`` files to it and lists them from a bundle."""
        registry = DagImporterRegistry(register_defaults=True)
        registry.register_specs(
            ["blueprint.importer.BlueprintDagImporter"],
            context="test",
        )
        write_dag_yaml(bundle_dir, "my_dag")

        importer = registry.get_importer(bundle_dir / "my_dag.dag.yaml")

        assert isinstance(importer, BlueprintDagImporter)
        items = [d for _, d in registry.list_dag_definitions(_bundle(bundle_dir))]
        assert [d.path.name for d in items] == ["my_dag.dag.yaml"]
