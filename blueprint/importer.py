"""Airflow Dag importer that builds Dags from Blueprint YAML files.

Requires Airflow 3.4+, which introduced the ``AbstractDagImporter`` interface. Register it with::

    [dag_processor]
    dag_importer_configs = [{"classpath": "blueprint.importer.BlueprintDagImporter",
                             "extensions": [".yaml"]}]

With the importer registered, no loader file calling ``build_all_airflow_dags()`` is needed.
"""

import logging
import re
import traceback
from collections.abc import Iterator
from fnmatch import fnmatch
from pathlib import Path
from typing import TYPE_CHECKING

from airflow.sdk.importers import (
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

    Args:
        extensions: File extensions to handle. Defaults to ``.yaml`` and ``.yml``. Extensions are
            routed to one importer each, so give ``[".yaml"]`` here when the built-in
            ``YamlDagImporter`` handles the same files, or run only one of the two.
        pattern: Glob that file names must match to be listed. Defaults to ``*.dag.yaml``, so
            ``blueprint.vars.yaml`` and other YAML in the bundle are left alone. Use ``*`` to
            list every file with a supported extension.
        profile: Active variable profile, as for ``build_all_airflow_dags``.
        discover_entry_points: Whether to also discover blueprints from installed packages.
        embed_source: Store each Dag's YAML in ``default_args`` for the Blueprint UI plugin.
    """

    supported_extensions = [".yaml", ".yml"]  # noqa: RUF012

    def __init__(
        self,
        extensions: list[str] | None = None,
        pattern: str = "*.dag.yaml",
        profile: str | None = None,
        discover_entry_points: bool = True,
        embed_source: bool = True,
    ) -> None:
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
        suffix = get_file_suffix(definition)
        return suffix in self.supported_extensions if suffix else False

    def list_dag_definitions(
        self,
        bundle: "BaseDagBundle",
        *,
        safe_mode: bool = True,
    ) -> Iterator[FileDagDefinition | DagImportError]:
        for definition in find_file_dag_definitions(Path(bundle.path), self.supported_extensions):
            if fnmatch(Path(repr(definition)).name, self.pattern) and self.might_contain_dag(
                definition, safe_mode
            ):
                yield definition

    def might_contain_dag(self, definition: DagDefinition, safe_mode: bool) -> bool:
        """Skip YAML files with no top-level ``steps`` key when safe mode is on."""
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
        result = DagImportResult(definition=definition)
        ref = repr(definition)
        yaml_path = Path(ref)
        root = self._root(bundle)

        try:
            with definition.import_context():
                dag_config = _load_dag_config(
                    yaml_path,
                    root,
                    render_templates=True,
                    template_context=None,
                    profile=self.profile,
                )
                if dag_config is None:
                    return result
                dag = Builder(bp_registry=self._registry(root)).build(
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
        return DagSourceCode(source_code=definition.read_text(encoding="utf-8"), language="yaml")

    @staticmethod
    def _root(bundle: "BaseDagBundle") -> Path:
        """Directory blueprints, DAG args and variables are searched in.

        Parse processes and workers may scope ``bundle.path`` to the single file being parsed.
        """
        bundle_path = Path(bundle.path)
        return bundle_path.parent if bundle_path.is_file() else bundle_path

    def _registry(self, root: Path) -> BlueprintRegistry:
        if root not in self._registries:
            reg = BlueprintRegistry(
                template_dirs=[root],
                discover_entry_points=self.discover_entry_points,
            )
            reg.discover(force=True)
            self._registries[root] = reg
        return self._registries[root]
