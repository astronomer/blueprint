"""Airflow Dag importer that builds Dags from Blueprint YAML files.

Requires Airflow 3.4+, which introduced the ``AbstractDagImporter`` interface. Register it with::

    [dag_processor]
    dag_importer_configs = ["blueprint.importer.BlueprintDagImporter"]

With the importer registered, no loader file calling ``build_all_airflow_dags()`` is needed.
"""

import logging
import re
import traceback
from collections.abc import Iterator
from fnmatch import fnmatch
from pathlib import Path
from typing import TYPE_CHECKING

from airflow.sdk.importers import (  # ty: ignore[unresolved-import]
    AbstractDagImporter,
    DagDefinition,
    DagImportError,
    DagImportResult,
    DagSourceCode,
    FileDagDefinition,
    find_file_dag_definitions,
    get_file_suffix,
)

from blueprint.builder import Builder, _load_dag_config, embed_source_yaml
from blueprint.errors import BlueprintError
from blueprint.registry import BlueprintRegistry

if TYPE_CHECKING:
    from airflow.dag_processing.bundles.base import BaseDagBundle

logger = logging.getLogger(__name__)

_DAG_MARKER_RE = re.compile(rb"^steps\s*:", re.MULTILINE)


class BlueprintDagImporter(AbstractDagImporter[FileDagDefinition]):
    """Build Dags from Blueprint YAML files in a bundle.

    Blueprint templates and ``BlueprintDagArgs`` are discovered from the Python files in the
    bundle (and from installed entry points), exactly as ``build_all_airflow_dags`` does.

    :param extensions: File extensions to handle. Defaults to ``.yaml``. Each extension is
        routed to one importer, so do not register this alongside the built-in
        ``YamlDagImporter`` for the same extension. To also handle ``.yml``, widen ``pattern``
        as well, because the default pattern only matches ``.yaml`` names.
    :param pattern: Glob that file names must match to be listed. Defaults to ``*.dag.yaml``,
        so ``blueprint.vars.yaml`` and other YAML in the bundle are left alone. Use ``*`` to
        list every file with a supported extension.
    :param profile: Active variable profile, as for ``build_all_airflow_dags``.
    :param discover_entry_points: Whether to also discover blueprints from installed packages.
    :param embed_source: Store each Dag's YAML in ``default_args`` for the Blueprint UI plugin.
    """

    supported_extensions = [".yaml"]  # noqa: RUF012

    def __init__(
        self,
        extensions: list[str] | None = None,
        pattern: str = "*.dag.yaml",
        profile: str | None = None,
        discover_entry_points: bool = True,
        embed_source: bool = True,
    ) -> None:
        """Configure the importer.

        Blueprint discovery is deferred to the first import, so creating the importer is cheap.

        :param extensions: File extensions to handle, with or without the leading dot.
        :param pattern: Glob that file names must match to be listed.
        :param profile: Active variable profile.
        :param discover_entry_points: Whether to also discover blueprints from installed packages.
        :param embed_source: Whether to store each Dag's YAML in ``default_args``.
        """
        if extensions is not None:
            self.supported_extensions = [
                e.lower() if e.startswith(".") else f".{e.lower()}" for e in extensions
            ]
        self.pattern = pattern
        self.profile = profile
        self.discover_entry_points = discover_entry_points
        self.embed_source = embed_source
        self._registries: dict[Path, BlueprintRegistry] = {}

    def can_handle(self, definition: DagDefinition | str | Path) -> bool:
        """Check whether this importer handles a definition, path, or file name.

        :param definition: A Dag definition, a path, or a file name.
        :returns: True if its suffix is one of :attr:`supported_extensions`.
        """
        suffix = get_file_suffix(definition)
        return suffix in self.supported_extensions if suffix else False

    def list_dag_definitions(
        self,
        bundle: "BaseDagBundle",
        *,
        safe_mode: bool = True,
    ) -> Iterator[FileDagDefinition | DagImportError]:
        """List the Blueprint YAML files in a bundle.

        Files are found with Airflow's own walker, so ``.airflowignore`` is honored. Only files
        whose name matches :attr:`pattern` and that pass :meth:`might_contain_dag` are yielded.

        :param bundle: The bundle to list. Only ``bundle.path`` is used, and it may be a
            directory or a single file.
        :param safe_mode: Whether to skip files that clearly hold no Dag.
        :returns: An iterator of file definitions to import.
        """
        for definition in find_file_dag_definitions(Path(bundle.path), self.supported_extensions):
            if fnmatch(Path(repr(definition)).name, self.pattern) and self.might_contain_dag(
                definition, safe_mode
            ):
                yield definition

    def might_contain_dag(self, definition: DagDefinition, safe_mode: bool) -> bool:
        """Skip YAML files with no top-level ``steps`` key when safe mode is on.

        :param definition: The definition to check.
        :param safe_mode: Whether safe mode is on. When off, every definition is kept.
        :returns: False only if safe mode is on and the file has no ``steps:`` key. A file that
            cannot be read is kept so that :meth:`import_definition` reports the error.
        """
        if not safe_mode:
            return True
        try:
            return _DAG_MARKER_RE.search(definition.read_bytes()) is not None
        except OSError:
            return True

    def import_definition(
        self,
        definition: FileDagDefinition,
        bundle: "BaseDagBundle",
    ) -> DagImportResult:
        """Build the Dag described by one YAML file.

        Failures are reported as :class:`~airflow.sdk.importers.DagImportError` entries in the
        result rather than raised, so Airflow shows them as import errors. A file with no
        ``steps`` field is not a Dag definition and yields an empty, successful result.

        :param definition: The YAML file to import.
        :param bundle: The bundle holding the file, which is the root that blueprints, DAG args
            and variables are searched in.
        :returns: A result holding the built Dag, or the errors that prevented it.
        """
        result = DagImportResult(definition=definition)
        ref = repr(definition)
        yaml_path = Path(ref)
        bundle_root = self._bundle_root(bundle)

        try:
            with definition.import_context():
                dag_config = _load_dag_config(
                    yaml_path,
                    bundle_root,
                    render_templates=True,
                    template_context=None,
                    profile=self.profile,
                )
                if dag_config is None:
                    return result
                dag = Builder(bp_registry=self._registry(bundle_root)).build(
                    dag_config, source_path=yaml_path
                )
        except BlueprintError as exc:
            result.errors.append(
                DagImportError(
                    source_reference=ref,
                    message=str(exc),
                    error_type="import",
                    stacktrace=traceback.format_exc(),
                )
            )
            return result
        except Exception as exc:
            result.errors.append(
                DagImportError(
                    source_reference=ref,
                    message=f"{type(exc).__name__}: {exc}",
                    error_type="import",
                    stacktrace=traceback.format_exc(),
                )
            )
            return result

        if self.embed_source:
            embed_source_yaml(dag, yaml_path)
        result.dags.append(dag)
        logger.debug("Built DAG '%s' from %s", dag.dag_id, ref)
        return result

    def get_source_code(
        self,
        definition: DagDefinition,
        dag_id: str | None = None,  # noqa: ARG002
    ) -> DagSourceCode:
        """Return the YAML source shown on the Dag's Code tab.

        :param definition: The YAML file.
        :param dag_id: Ignored, because a file defines exactly one Dag.
        :returns: The file's text, with language ``yaml``.
        """
        return DagSourceCode(source_code=definition.read_text(encoding="utf-8"), language="yaml")

    @staticmethod
    def _bundle_root(bundle: "BaseDagBundle") -> Path:
        """Return the directory blueprints, DAG args and variables are searched in.

        Parse processes and workers may scope ``bundle.path`` to the single file being parsed,
        in which case the file's parent directory is used.

        :param bundle: The bundle being imported from.
        :returns: The bundle directory.
        """
        bundle_path = Path(bundle.path)
        return bundle_path.parent if bundle_path.is_file() else bundle_path

    def _registry(self, root: Path) -> BlueprintRegistry:
        """Return the blueprint registry for a directory, discovering it on first use.

        Registries are cached per importer instance, so discovery runs once per directory in
        each process.

        :param root: The directory to discover blueprints in.
        :returns: A registry with discovery already run.
        """
        if root not in self._registries:
            reg = BlueprintRegistry(
                template_dirs=[root],
                discover_entry_points=self.discover_entry_points,
            )
            reg.discover(force=True)
            self._registries[root] = reg
        return self._registries[root]
