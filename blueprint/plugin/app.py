"""FastAPI app serving the Blueprint UI pages inside Airflow.

The app is mounted under the Airflow API server (see ``blueprint.plugin``)
and renders the source YAML and blueprint Python behind a DAG, and the
per-step config and code behind a task.

DAG-to-YAML resolution prefers the ``blueprint:<path>`` tag stamped by
``build_all_airflow_dags`` (one metadata-DB lookup), falling back to a
scan of the dags folder for older builds.
"""

import logging
import os
import re
import time
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar
from urllib.parse import unquote

import yaml

from blueprint.builder import SOURCE_TAG_PREFIX, _relative_source
from blueprint.registry import BlueprintRegistry

if TYPE_CHECKING:
    from fastapi import FastAPI

logger = logging.getLogger(__name__)

T = TypeVar("T")

_CACHE_TTL_SECONDS = 30.0
_cache: dict[str, tuple[float, Any]] = {}


def _cached(key: str, factory: Callable[[], T]) -> T:
    """Return a cached value, refreshing it after a short TTL."""
    now = time.monotonic()
    hit = _cache.get(key)
    if hit is not None and hit[0] > now:
        return hit[1]
    value = factory()
    _cache[key] = (now + _CACHE_TTL_SECONDS, value)
    return value


def clear_cache() -> None:
    """Drop all cached scan results (used by tests)."""
    _cache.clear()


def code_html(code: str, language: str, line_numbers: bool = False) -> str:
    """Render code as highlighted HTML, mirroring Airflow's Code tab.

    Falls back to an escaped ``<pre>`` if Pygments is unavailable.
    """
    try:
        from pygments import highlight
        from pygments.formatters.html import HtmlFormatter
        from pygments.lexers import get_lexer_by_name

        formatter = HtmlFormatter(
            cssclass="highlight",
            linenos="table" if line_numbers else False,
        )
        rendered = highlight(code, get_lexer_by_name(language, stripnl=False), formatter)
    except Exception:
        import html

        logger.debug("Pygments highlighting failed", exc_info=True)
        return f'<pre class="code">{html.escape(code)}</pre>'
    return f'<div class="codeblock">{rendered}</div>'


def pygments_stylesheet() -> str:
    """Build highlight CSS: light by default, dark under the synced theme attr."""

    def scoped(fmt: Any, scope: str) -> str:
        # Pygments emits line-number rules unprefixed; drop them — the base
        # stylesheet styles the gutter to match the surrounding theme.
        return "\n".join(
            line
            for line in fmt.get_style_defs(scope).splitlines()
            if "linenos" not in line and not line.startswith("pre ")
        )

    try:
        from pygments.formatters.html import HtmlFormatter

        light = scoped(HtmlFormatter(style="xcode"), ".highlight")
        dark_fmt = HtmlFormatter(style="github-dark")
        dark = scoped(dark_fmt, ':root[data-theme="dark"] .highlight')
        dark_media = scoped(dark_fmt, ':root:not([data-theme="light"]) .highlight')
    except Exception:
        logger.debug("Pygments stylesheet generation failed", exc_info=True)
        return ""
    return f"{light}\n{dark}\n@media (prefers-color-scheme: dark) {{\n{dark_media}\n}}"


def _default_dags_folder() -> Path:
    """Resolve the dags folder from Airflow config, with an env fallback."""
    try:
        from airflow.configuration import conf

        return Path(conf.get("core", "dags_folder"))
    except Exception:
        return Path(os.environ.get("AIRFLOW_HOME", ".")) / "dags"


def _parse_dag_yaml(path: Path) -> dict[str, Any] | None:
    """Parse a DAG YAML file leniently.

    Jinja2 expressions are rendered with undefined variables collapsing to
    empty strings, so files that need build-time context still yield their
    ``dag_id`` and ``steps`` structure. Returns None if the file cannot be
    parsed into a mapping.
    """
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return None

    if "{{" in raw or "{%" in raw:
        try:
            import jinja2

            from blueprint.loaders import _ContextProxy, _StubVarAccessor

            env = jinja2.Environment(undefined=jinja2.ChainableUndefined)
            raw = env.from_string(raw).render(
                env=os.environ,
                var=_StubVarAccessor(),
                context=_ContextProxy(),
            )
        except Exception:
            logger.debug("Tolerant Jinja2 render failed for %s", path, exc_info=True)

    try:
        config = yaml.safe_load(raw)
    except yaml.YAMLError:
        return None
    return config if isinstance(config, dict) else None


def _dag_sources(dags_folder: Path) -> dict[str, Path]:
    """Map dag_id to its YAML path by scanning the dags folder."""

    def scan() -> dict[str, Path]:
        from blueprint.loaders import discover_yaml_files

        sources: dict[str, Path] = {}
        if not dags_folder.exists():
            return sources
        for path in discover_yaml_files(dags_folder, "*.dag.yaml"):
            config = _parse_dag_yaml(path)
            dag_id = (config or {}).get("dag_id")
            if isinstance(dag_id, str):
                sources[dag_id] = path
        return sources

    return _cached(f"sources:{dags_folder}", scan)


def _source_from_tags(dag_id: str) -> str | None:
    """Read the ``blueprint:<path>`` tag for a DAG from the metadata DB."""
    try:
        from airflow.models.dag import DagModel
        from airflow.utils.session import create_session

        with create_session() as session:
            dag_model = session.get(DagModel, dag_id)
            if dag_model is None:
                return None
            for tag in dag_model.tags or []:
                name = getattr(tag, "name", None) or str(tag)
                if name.startswith(SOURCE_TAG_PREFIX):
                    return name[len(SOURCE_TAG_PREFIX) :]
    except Exception:
        logger.debug("Source tag lookup failed for %s", dag_id, exc_info=True)
    return None


def resolve_dag_source(dag_id: str, dags_folder: Path) -> Path | None:
    """Find the YAML file a DAG was built from.

    Tries the source tag first, then falls back to scanning the dags folder.
    Tag values must resolve to a file inside the dags folder.
    """
    rel = _source_from_tags(dag_id)
    if rel:
        candidate = Path(rel)
        if not candidate.is_absolute():
            candidate = dags_folder / candidate
        resolved = candidate.resolve()
        if resolved.is_file() and resolved.is_relative_to(dags_folder.resolve()):
            return resolved
    return _dag_sources(dags_folder).get(dag_id)


def resolve_task_step(
    dag_id: str, task_id: str, dags_folder: Path
) -> tuple[str, dict[str, Any]] | None:
    """Find the YAML step that renders a task.

    A step rendering a TaskGroup produces task ids like ``step.child``, so
    the step name is the task id itself or its prefix before a dot.

    Returns:
        (step_name, step_dict) or None if the DAG or step cannot be found.
    """
    path = resolve_dag_source(dag_id, dags_folder)
    if path is None:
        return None
    config = _parse_dag_yaml(path)
    steps = (config or {}).get("steps")
    if not isinstance(steps, dict):
        return None
    candidates = [
        name
        for name, step in steps.items()
        if isinstance(step, dict) and (task_id == name or task_id.startswith(f"{name}."))
    ]
    if not candidates:
        return None
    step_name = max(candidates, key=len)
    return step_name, steps[step_name]


def _step_context_from_rtif(
    dag_id: str, run_id: str, task_id: str, map_index: int
) -> tuple[str | None, str | None]:
    """Fetch stamped step fields as rendered for a specific task instance.

    Airflow stores rendered template fields per run, so this is the exact
    config (and blueprint source) the task ran with — including runtime
    param overrides. Rows are pruned to the most recent runs per task, so
    misses are expected.

    Returns:
        (blueprint_step_config, blueprint_step_code), each None if absent.
    """
    try:
        from airflow.models.renderedtifields import RenderedTaskInstanceFields
        from airflow.utils.session import create_session
        from sqlalchemy import select

        with create_session() as session:
            stmt = select(RenderedTaskInstanceFields).filter_by(
                dag_id=dag_id, task_id=task_id, run_id=run_id, map_index=map_index
            )
            row = session.scalars(stmt).first()
            if row is None:
                return None, None
            fields = row.rendered_fields or {}
            return (
                _clean_stamped_value(fields.get("blueprint_step_config")),
                _clean_stamped_value(fields.get("blueprint_step_code")),
            )
    except Exception:
        logger.debug("RTIF lookup failed for %s/%s/%s", dag_id, run_id, task_id, exc_info=True)
        return None, None


def _clean_stamped_value(value: Any) -> str | None:
    """Drop non-strings and values Airflow truncated when storing them.

    Both rendered task fields and serialized DAGs replace template field
    values longer than ``[core] max_templated_field_length`` with a
    "Truncated. ..." placeholder — blueprint source files often exceed it,
    so fall through to the next source instead of showing the placeholder.
    """
    if not isinstance(value, str) or value.startswith("Truncated."):
        return None
    return value


def _step_context_from_serialized_dag(dag_id: str, task_id: str) -> tuple[str | None, str | None]:
    """Fetch stamped step fields from the serialized DAG in the metadata DB.

    This is the config and blueprint source as built (resolved version
    included) for the current DAG version — independent of what the files
    on disk say now.

    Returns:
        (blueprint_step_config, blueprint_step_code), each None if absent.
    """
    try:
        from airflow.models.serialized_dag import SerializedDagModel
        from airflow.utils.session import create_session

        with create_session() as session:
            sdm = SerializedDagModel.get(dag_id, session=session)
            if sdm is None:
                return None, None
            task = sdm.dag.get_task(task_id)
            return (
                _clean_stamped_value(getattr(task, "blueprint_step_config", None)),
                _clean_stamped_value(getattr(task, "blueprint_step_code", None)),
            )
    except Exception:
        logger.debug("Serialized DAG lookup failed for %s/%s", dag_id, task_id, exc_info=True)
        return None, None


def _parse_step_context(step_yaml: str) -> dict[str, Any] | None:
    """Parse a stamped ``blueprint_step_config`` YAML string."""
    try:
        data = yaml.safe_load(step_yaml)
    except yaml.YAMLError:
        return None
    if isinstance(data, dict) and isinstance(data.get("blueprint"), str):
        return data
    return None


def _load_registry(dags_folder: Path) -> BlueprintRegistry:
    """Build (and cache) a registry discovered from the dags folder."""

    def make() -> BlueprintRegistry:
        return BlueprintRegistry(template_dirs=[dags_folder])

    return _cached(f"registry:{dags_folder}", make)


_GROUP_REFERER_RE = re.compile(
    r"/dags/[^/]+(?:/runs/(?P<run>[^/]+))?/tasks/group/(?P<group>[^/?#]+)"
)


def group_from_referer(referer: str | None) -> tuple[str | None, str | None]:
    """Recover (group_id, run_id) from the embedding page's URL.

    Task group pages render task-destination external views but leave the
    ``{TASK_ID}`` token unsubstituted (there is no task in context). The
    iframe request's Referer still names the group, and a group id equals
    its step name.
    """
    if not referer:
        return None, None
    m = _GROUP_REFERER_RE.search(referer)
    if m is None:
        return None, None
    run = m.group("run")
    return unquote(m.group("group")), unquote(run) if run else None


def dag_code_sections(config: dict[str, Any] | None, dags_folder: Path) -> list[dict[str, Any]]:
    """Collect source code for each distinct blueprint a DAG's YAML uses."""
    import inspect

    steps = (config or {}).get("steps")
    if not isinstance(steps, dict):
        return []

    reg = _load_registry(dags_folder)
    sections: dict[tuple[str, int], dict[str, Any]] = {}
    for step in steps.values():
        if not isinstance(step, dict):
            continue
        name = step.get("blueprint")
        version = step.get("version")
        if not isinstance(name, str):
            continue
        try:
            cls = reg.get(name, version if isinstance(version, int) else None)
            resolved = version if isinstance(version, int) else reg.get_latest_version(name)
            key = (name, resolved)
            if key in sections:
                continue
            sections[key] = {
                "name": name,
                "version": resolved,
                "class": cls.__name__,
                "location": _relative_source(Path(inspect.getfile(cls)), dags_folder),
                "source_code": cls.get_source_code(),
            }
        except Exception:
            logger.debug("Could not resolve blueprint %s for code section", name, exc_info=True)
    return [sections[key] for key in sorted(sections)]


def _normalize_task_ref(
    task_id: str,
    run_id: str | None,
    map_index: str | None,
    referer: str | None,
) -> tuple[str, str | None, int]:
    """Resolve unsubstituted UI tokens into a usable (task_id, run_id, map_index)."""
    if "{" in task_id:
        group_id, ref_run = group_from_referer(referer)
        if group_id:
            task_id = group_id
            if not run_id or "{" in run_id:
                run_id = ref_run
    if run_id and "{" in run_id:
        run_id = None
    try:
        mi = int(map_index) if map_index else -1
    except ValueError:
        mi = -1
    return task_id, run_id, mi


def _stamped_step_fields(
    dag_id: str,
    task_id: str,
    run_id: str | None,
    map_index: int,
) -> tuple[str | None, str | None, str | None, str | None, str | None]:
    """Fetch stamped step config and code, each with its provenance.

    Config and code fall back independently from the run's rendered fields
    to the serialized DAG — a run's code copy is often truncated (see
    ``_clean_rtif_value``) while its config is not.

    Returns:
        (config, code, config_provenance, code_provenance, effective_run_id).
    """
    stamped, code = None, None
    provenance = None
    code_provenance = None
    if run_id:
        stamped, code = _step_context_from_rtif(dag_id, run_id, task_id, map_index)
        if stamped is None and " " in run_id:
            # The UI substitutes {RUN_ID} without URL-encoding, so a "+"
            # in timestamped run ids arrives as a space.
            run_id = run_id.replace(" ", "+")
            stamped, code = _step_context_from_rtif(dag_id, run_id, task_id, map_index)
        if stamped:
            provenance = "run"
        if code:
            code_provenance = "run"
    if stamped is None or code is None:
        ser_config, ser_code = _step_context_from_serialized_dag(dag_id, task_id)
        if stamped is None and ser_config:
            stamped = ser_config
            provenance = "serialized"
        if code is None and ser_code:
            code = ser_code
            code_provenance = "serialized"
    return stamped, code, provenance, code_provenance, run_id


def task_step_context(
    dag_id: str,
    task_id: str,
    run_id: str | None,
    map_index: str | None,
    dags_folder: Path,
    referer: str | None = None,
) -> tuple[dict[str, Any], bool]:
    """Build the task step page context via the metadata lookup ladder.

    Prefers the rendered fields of the given run (exact as-run config), then
    the serialized DAG (as-built config), then the source YAML on disk.
    An unsubstituted ``{TASK_ID}`` (task group pages) is recovered from the
    Referer header, since a group id equals its step name.

    Returns:
        (template context, whether a step was found).
    """
    task_id, run_id, mi = _normalize_task_ref(task_id, run_id, map_index, referer)

    context: dict[str, Any] = {
        "dag_id": dag_id,
        "task_id": task_id,
        "step_name": None,
        "run_id": None,
        "source_code": None,
        "dags_folder": str(dags_folder),
    }

    stamped, code, provenance, code_provenance, run_id = _stamped_step_fields(
        dag_id, task_id, run_id, mi
    )
    if provenance == "run":
        context["run_id"] = run_id

    match = resolve_task_step(dag_id, task_id, dags_folder)

    data = _parse_step_context(stamped) if stamped else None
    if data is not None:
        if code is None:
            code = _registry_source_code(data["blueprint"], data.get("version"), dags_folder)
            code_provenance = "yaml" if code else None
        context.update(
            {
                "step_name": match[0] if match else task_id.split(".", 1)[0],
                "blueprint": data["blueprint"],
                "version": data.get("version"),
                "version_label": None,
                "provenance": provenance,
                "code_provenance": code_provenance,
                "step_yaml": stamped,
                "source_code": code,
            }
        )
        return context, True

    if match is not None:
        step_name, step = match
        blueprint_name = step.get("blueprint")
        version = step.get("version")
        version_label = "pinned" if version else "latest"
        if isinstance(blueprint_name, str):
            context["source_code"] = _registry_source_code(blueprint_name, version, dags_folder)
            context["code_provenance"] = "yaml" if context["source_code"] else None
            if version is None:
                try:
                    version = _load_registry(dags_folder).get_latest_version(blueprint_name)
                except Exception:
                    logger.debug("Could not resolve latest version", exc_info=True)
        context.update(
            {
                "step_name": step_name,
                "blueprint": blueprint_name,
                "version": version,
                "version_label": version_label,
                "provenance": "yaml",
                "step_yaml": yaml.dump(
                    {step_name: step}, default_flow_style=False, sort_keys=False
                ),
            }
        )
        return context, True

    return context, False


def _registry_source_code(name: str, version: int | None, dags_folder: Path) -> str | None:
    """Read a blueprint's current source file via the registry, fail-soft."""
    try:
        return _load_registry(dags_folder).get(name, version).get_source_code() or None
    except Exception:
        logger.debug("Could not resolve blueprint %s from registry", name, exc_info=True)
        return None


def create_app(dags_folder: Path | None = None) -> "FastAPI":
    """Create the Blueprint UI FastAPI app.

    Args:
        dags_folder: Directory holding DAG YAML files and blueprint modules.
            Defaults to Airflow's configured dags folder, resolved per request.

    Returns:
        The FastAPI app, ready to mount under the Airflow API server.
    """
    import jinja2
    from fastapi import FastAPI, Request, Response
    from fastapi.responses import HTMLResponse

    templates = jinja2.Environment(
        loader=jinja2.FileSystemLoader(Path(__file__).parent / "templates"),
        autoescape=True,
    )

    def _folder() -> Path:
        return dags_folder if dags_folder is not None else _default_dags_folder()

    app = FastAPI(title="Blueprint", docs_url=None, redoc_url=None, openapi_url=None)

    @app.get("/static/pygments.css")
    def pygments_css() -> Response:
        return Response(
            pygments_stylesheet(),
            media_type="text/css",
            headers={"Cache-Control": "max-age=3600"},
        )

    @app.get("/dags/{dag_id}/yaml", response_class=HTMLResponse)
    def dag_yaml_page(dag_id: str) -> HTMLResponse:
        folder = _folder()
        path = resolve_dag_source(dag_id, folder)
        content_html = None
        source = None
        code_sections: list[dict[str, Any]] = []
        if path is not None:
            source = _relative_source(path, folder)
            try:
                content_html = code_html(
                    path.read_text(encoding="utf-8"), "yaml", line_numbers=True
                )
            except OSError:
                logger.warning("Could not read %s", path, exc_info=True)
            code_sections = dag_code_sections(_parse_dag_yaml(path), folder)
            for section in code_sections:
                section["source_html"] = code_html(
                    section["source_code"], "python", line_numbers=True
                )
        html = templates.get_template("dag_yaml.html").render(
            dag_id=dag_id,
            source=source,
            content_html=content_html,
            code_sections=code_sections,
            dags_folder=str(folder),
        )
        return HTMLResponse(html, status_code=200 if content_html is not None else 404)

    @app.get("/dags/{dag_id}/tasks/{task_id}", response_class=HTMLResponse)
    def task_step_page(
        request: Request,
        dag_id: str,
        task_id: str,
        run_id: str | None = None,
        map_index: str | None = None,
    ) -> HTMLResponse:
        context, found = task_step_context(
            dag_id,
            task_id,
            run_id,
            map_index,
            _folder(),
            referer=request.headers.get("referer"),
        )
        if context.get("step_yaml"):
            context["step_yaml_html"] = code_html(context["step_yaml"], "yaml")
        if context.get("source_code"):
            context["source_code_html"] = code_html(
                context["source_code"], "python", line_numbers=True
            )
        html = templates.get_template("task_step.html").render(**context)
        return HTMLResponse(html, status_code=200 if found else 404)

    return app
