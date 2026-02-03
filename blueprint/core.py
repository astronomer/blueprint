"""Core Blueprint base class with magic method generation."""

import inspect
import logging
import os
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Generic, List, Optional, Tuple, Type, TypeVar, Union

import yaml
from pydantic import BaseModel

if TYPE_CHECKING:
    from airflow import DAG

T = TypeVar("T", bound=BaseModel)

logger = logging.getLogger(__name__)


class Blueprint(Generic[T]):
    """Base class for all Blueprint templates.

    This class uses __init_subclass__ to dynamically generate a `build` method
    with the proper signature based on the Pydantic model used as the type parameter.

    Example:
        class MyConfig(BaseModel):
            job_id: str
            schedule: str = "@daily"

        class MyBlueprint(Blueprint[MyConfig]):
            def render(self, config: MyConfig) -> DAG:
                return DAG(dag_id=config.job_id, schedule=config.schedule)

        # The build method is automatically generated with proper types
        dag = MyBlueprint.build(job_id="my_job", schedule="@hourly")
    """

    _config_type: Type[BaseModel]

    def __init_subclass__(cls, **kwargs):
        """Generate the build method when a Blueprint subclass is defined."""
        super().__init_subclass__(**kwargs)

        # Extract config type from Generic[T] parameter
        orig_bases = getattr(cls, "__orig_bases__", ())
        for base in orig_bases:
            if hasattr(base, "__args__") and base.__args__:
                config_type = base.__args__[0]
                # Verify it's a Pydantic model
                if isinstance(config_type, type) and issubclass(config_type, BaseModel):
                    cls._config_type = config_type
                    cls._generate_build_method(config_type)
                break

    @classmethod
    def _generate_build_method(cls, config_type: Type[BaseModel]) -> None:
        """Generate the build method with proper signature from Pydantic model."""

        # Get model fields from Pydantic

        # Create parameters from Pydantic model fields
        params = [inspect.Parameter("cls", inspect.Parameter.POSITIONAL_OR_KEYWORD)]

        # Get field definitions from the model
        for field_name, field_info in config_type.model_fields.items():
            # Determine if field has a default
            if field_info.is_required():
                default = inspect.Parameter.empty
            else:
                default = field_info.get_default(call_default_factory=True)

            # Create parameter with proper annotation
            param = inspect.Parameter(
                field_name,
                inspect.Parameter.KEYWORD_ONLY,
                default=default,
                annotation=field_info.annotation,
            )
            params.append(param)

        # Create the build method
        def build(cls, **kwargs: Any):
            """Build a DAG from the provided configuration.

            This method is dynamically generated with the proper signature
            based on the Blueprint's configuration model.

            The DAG is automatically registered in the caller's globals
            so Airflow can discover it.
            """
            # Create the config instance - Pydantic handles validation
            config = cls._config_type(**kwargs)

            # Create blueprint instance and render
            instance = cls()
            dag = instance.render(config)

            # Validate dag_id matches config if applicable
            cls._validate_dag_id(config, dag)

            # Inject config for UI visibility
            config_yaml = yaml.dump(
                {"blueprint": cls._get_blueprint_name(), **config.model_dump()},
                default_flow_style=False,
                sort_keys=False,
            )
            cls._inject_blueprint_config(dag, config_yaml)

            # Auto-register the DAG in the caller's globals
            frame = inspect.currentframe()
            if frame and frame.f_back:
                frame.f_back.f_globals[dag.dag_id] = dag

            return dag

        # Set the proper signature on the build method
        build.__signature__ = inspect.Signature(params, return_annotation="DAG")  # type: ignore[assignment]

        # Bind as classmethod
        cls.build = classmethod(build)

    def render(self, config: T) -> "DAG":
        """Render the DAG with validated configuration.

        This method must be implemented by Blueprint subclasses.

        Args:
            config: The validated configuration model instance

        Returns:
            The rendered Airflow DAG

        Example:
            ```python
            class MyConfig(BaseModel):
                dag_id: str
                schedule: str = "@daily"

            class MyBlueprint(Blueprint[MyConfig]):
                def render(self, config: MyConfig) -> DAG:
                    return DAG(
                        dag_id=config.dag_id,
                        schedule=config.schedule,
                        start_date=datetime(2024, 1, 1)
                    )
            ```
        """
        msg = f"{self.__class__.__name__} must implement the render() method"
        raise NotImplementedError(msg)

    @classmethod
    def get_config_type(cls) -> Type[BaseModel]:
        """Get the configuration type for this Blueprint."""
        if not hasattr(cls, "_config_type"):
            msg = (
                f"{cls.__name__} was not properly initialized. "
                "Make sure it inherits from Blueprint[ConfigType]"
            )
            raise RuntimeError(msg)
        return cls._config_type

    @classmethod
    def get_schema(cls) -> Dict[str, Any]:
        """Get the JSON Schema for this Blueprint's configuration."""
        return cls.get_config_type().model_json_schema()

    @classmethod
    def _validate_dag_id(cls, config: BaseModel, dag: "DAG") -> None:
        """Validate that the DAG's dag_id matches the expected value from config.

        Checks if the config has a 'job_id' or 'dag_id' field and validates
        that the rendered DAG's dag_id matches.

        Args:
            config: The validated configuration model
            dag: The rendered DAG

        Raises:
            ValueError: If the dag_id doesn't match the expected config value
        """
        # Check common field names for dag_id
        expected_id = None
        for field_name in ("job_id", "dag_id"):
            if hasattr(config, field_name):
                expected_id = getattr(config, field_name)
                break

        if expected_id is not None and dag.dag_id != expected_id:
            msg = (
                f"DAG ID mismatch: config specifies '{expected_id}' "
                f"but render() returned DAG with id '{dag.dag_id}'"
            )
            raise ValueError(msg)

    @classmethod
    def build_from_yaml(
        cls,
        config_path: Union[str, Path],
        overrides: Optional[Dict[str, Any]] = None,
        render_template: bool = True,
        template_context: Optional[Dict[str, Any]] = None,
    ) -> "DAG":
        """Build a DAG from a YAML configuration file.

        This provides a simple way to load a YAML config and build a DAG without
        needing the blueprint registry. Since you already have the Blueprint class,
        you can call this method directly.

        Args:
            config_path: Path to the YAML config file. Relative paths are resolved
                         relative to the caller's file location.
            overrides: Optional dict of values to override from the YAML
            render_template: Whether to render Jinja2 templates in the YAML (default: True)
            template_context: Additional context for Jinja2 rendering

        Returns:
            The built DAG

        Example:
            ```python
            from daily_etl import DailyETL

            # Simple usage - path relative to this file
            dag = DailyETL.build_from_yaml("customer_etl.dag.yaml")

            # With overrides
            dag = DailyETL.build_from_yaml(
                "customer_etl.dag.yaml",
                overrides={"job_id": "custom-id", "retries": 5},
            )
            ```
        """
        import yaml

        from blueprint.errors import ConfigurationError, YAMLParseError
        from blueprint.loaders import render_yaml_template

        config_path = Path(config_path)

        # If path is relative, resolve it relative to the caller's file location
        if not config_path.is_absolute():
            caller_dir = cls._get_caller_directory()
            config_path = caller_dir / config_path

        # Load and optionally render the YAML
        if render_template:
            config, rendered_yaml = render_yaml_template(
                config_path,
                context=template_context,
                use_airflow_context=True,
            )
        else:
            try:
                raw_content = config_path.read_text()
                config = yaml.safe_load(raw_content)
                rendered_yaml = raw_content
            except yaml.YAMLError as e:
                raise YAMLParseError.from_yaml_error(e, config_path) from e
            except Exception as e:
                msg = f"Failed to read configuration file: {e}"
                raise ConfigurationError(msg, config_path) from e

            if not config:
                msg = "Configuration file is empty"
                raise ConfigurationError(msg, config_path)

        # Remove the blueprint field (not a config param)
        config.pop("blueprint", None)

        # Apply overrides
        if overrides:
            config.update(overrides)

        # Build the DAG
        dag = cls.build(**config)

        # Inject the rendered config for UI visibility
        cls._inject_blueprint_config(dag, rendered_yaml)

        return dag

    @classmethod
    def _get_blueprint_name(cls) -> str:
        """Convert this Blueprint class name to its snake_case name.

        Returns:
            Snake case name (e.g., DailyETL -> daily_etl)
        """
        name = cls.__name__
        # Handle consecutive capitals and normal camelCase
        name = re.sub("([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
        name = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", name)
        return name.lower()

    @classmethod
    def _resolve_search_path(cls, search_path: Optional[Union[str, Path]]) -> Path:
        """Resolve the search path for YAML discovery."""
        if search_path is not None:
            return Path(search_path)

        # Check for AIRFLOW_HOME/dags first
        airflow_home = os.environ.get("AIRFLOW_HOME")
        if airflow_home:
            dags_dir = Path(airflow_home) / "dags"
            if dags_dir.exists():
                return dags_dir

        # Fall back to caller's directory
        return cls._get_caller_directory()

    @classmethod
    def _load_yaml_config(
        cls,
        yaml_path: Path,
        render_templates: bool,
        template_context: Optional[Dict[str, Any]],
    ) -> Tuple[Dict[str, Any], str]:
        """Load and optionally render a YAML configuration file.

        Returns:
            Tuple of (config dict, rendered yaml string)

        Raises:
            YAMLParseError: If YAML parsing fails
            ConfigurationError: If file is empty or unreadable
        """
        from blueprint.errors import ConfigurationError, YAMLParseError
        from blueprint.loaders import render_yaml_template

        if render_templates:
            return render_yaml_template(
                yaml_path,
                context=template_context,
                use_airflow_context=True,
            )

        # Load without template rendering
        try:
            raw_content = yaml_path.read_text()
            config = yaml.safe_load(raw_content)
        except yaml.YAMLError as e:
            raise YAMLParseError.from_yaml_error(e, yaml_path) from e
        except Exception as e:
            msg = f"Failed to read configuration file: {e}"
            raise ConfigurationError(msg, yaml_path) from e

        if not config:
            msg = "Configuration file is empty"
            raise ConfigurationError(msg, yaml_path)

        return config, raw_content

    @classmethod
    def build_all(
        cls,
        register_globals: Optional[dict] = None,
        search_path: Optional[Union[str, Path]] = None,
        pattern: str = "*.dag.yaml",
        render_templates: bool = True,
        template_context: Optional[Dict[str, Any]] = None,
        fail_fast: bool = False,
    ) -> List["DAG"]:
        """Discover and build all DAGs that use this Blueprint.

        This method discovers .dag.yaml files, filters to those that reference
        this blueprint, renders Jinja2 templates, validates configs, and
        builds DAGs.

        Args:
            register_globals: Dict to register DAGs in. If not provided,
                              automatically uses the caller's globals().
            search_path: Directory to search for YAML files. Defaults to dags/ or
                         the directory containing the calling file.
            pattern: Glob pattern for YAML discovery (default: *.dag.yaml)
            render_templates: Whether to render Jinja2 templates in YAML files
            template_context: Additional context variables for template rendering
            fail_fast: If True, raise immediately on first error. If False (default),
                       log errors and continue processing remaining files.

        Returns:
            List of built DAGs

        Example:
            ```python
            # In dags/daily_etl.py
            from blueprint import Blueprint, BaseModel

            class DailyETLConfig(BaseModel):
                job_id: str
                schedule: str = "@daily"

            class DailyETL(Blueprint[DailyETLConfig]):
                def render(self, config):
                    # ... create DAG ...
                    return dag

            # Discover and build all DAGs that use this blueprint
            DailyETL.build_all()
            ```
        """
        # Auto-capture caller's globals if not provided
        if register_globals is None:
            frame = inspect.currentframe()
            register_globals = frame.f_back.f_globals if frame and frame.f_back else {}

        resolved_path = cls._resolve_search_path(search_path)
        blueprint_name = cls._get_blueprint_name()

        logger.info(
            "Discovering DAGs for blueprint '%s' in %s (pattern: %s)",
            blueprint_name,
            resolved_path,
            pattern,
        )

        yaml_files = list(resolved_path.rglob(pattern)) if resolved_path.exists() else []
        if not yaml_files:
            logger.debug("No YAML files found matching pattern '%s' in %s", pattern, resolved_path)
            return []

        logger.debug("Found %d YAML files to check", len(yaml_files))

        dags: List[DAG] = []
        dag_id_to_file: Dict[str, Path] = {}

        for yaml_path in yaml_files:
            result = cls._process_yaml_file(
                yaml_path,
                blueprint_name,
                render_templates,
                template_context,
                register_globals,
                dag_id_to_file,
                fail_fast,
            )
            if result:
                dags.append(result)

        cls._log_build_summary(dags, blueprint_name)
        return dags

    @classmethod
    def _process_yaml_file(
        cls,
        yaml_path: Path,
        blueprint_name: str,
        render_templates: bool,
        template_context: Optional[Dict[str, Any]],
        register_globals: dict,
        dag_id_to_file: Dict[str, Path],
        fail_fast: bool,
    ) -> Optional["DAG"]:
        """Process a single YAML file and return the built DAG if successful."""
        from blueprint.errors import DuplicateDAGIdError

        dag: Optional[DAG] = None
        rendered_yaml: str = ""
        try:
            config, rendered_yaml = cls._load_yaml_config(
                yaml_path, render_templates, template_context
            )

            # Check if this YAML is for our blueprint
            if config.get("blueprint") != blueprint_name:
                logger.debug(
                    "Skipping %s: blueprint is '%s', not '%s'",
                    yaml_path.name,
                    config.get("blueprint"),
                    blueprint_name,
                )
                return None

            config.pop("blueprint", None)
            logger.info("Building DAG from %s", yaml_path.name)
            dag = cls.build(**config)
        except DuplicateDAGIdError:
            raise
        except Exception:
            if fail_fast:
                raise
            logger.exception("❌ Failed to build DAG from %s", yaml_path.name)
            return None

        # Check for duplicates (outside try to satisfy TRY301)
        dag_id = dag.dag_id
        if dag_id in dag_id_to_file:
            _raise_duplicate_error(dag_id, dag_id_to_file[dag_id], yaml_path)

        if dag_id in register_globals:
            logger.debug("Skipping already registered DAG: %s", dag_id)
            return None

        dag_id_to_file[dag_id] = yaml_path
        cls._inject_blueprint_config(dag, rendered_yaml)
        register_globals[dag_id] = dag
        logger.info("✅ Built DAG '%s' from %s", dag_id, yaml_path.name)
        return dag

    @staticmethod
    def _log_build_summary(dags: List["DAG"], blueprint_name: str) -> None:
        """Log a summary of built DAGs."""
        if dags:
            logger.info(
                "Successfully built %d DAG(s) for blueprint '%s': %s",
                len(dags),
                blueprint_name,
                ", ".join(d.dag_id for d in dags),
            )
        else:
            logger.debug("No DAGs built for blueprint '%s'", blueprint_name)

    @staticmethod
    def _get_caller_directory() -> Path:
        """Get the directory of the file that called build_all().

        Returns:
            Path to the caller's directory
        """
        # Walk up the stack to find the first frame outside this module
        for frame_info in inspect.stack():
            if frame_info.filename != __file__:
                return Path(frame_info.filename).parent
        # Fallback to current directory
        return Path.cwd()

    @staticmethod
    def _inject_blueprint_config(dag: "DAG", rendered_yaml: str) -> None:
        """Inject rendered config into all tasks for UI visibility.

        This adds a `blueprint_config` attribute to every task and extends
        their template_fields so users can view the resolved config in Airflow's
        Rendered Template view on any task.

        Args:
            dag: The DAG to modify
            rendered_yaml: The rendered YAML config string
        """
        for task in dag.tasks:
            # Add the rendered config as an attribute
            task.blueprint_config = rendered_yaml  # type: ignore[attr-defined]

            # Extend template_fields to include our new field
            existing_fields = getattr(task, "template_fields", ()) or ()
            if "blueprint_config" not in existing_fields:
                task.template_fields = (*existing_fields, "blueprint_config")


def _raise_duplicate_error(dag_id: str, existing_path: Path, new_path: Path) -> None:
    """Raise DuplicateDAGIdError for duplicate DAG IDs.

    This is a module-level helper to satisfy TRY301 (no raise inside try blocks).
    """
    from blueprint.errors import DuplicateDAGIdError

    raise DuplicateDAGIdError(dag_id, [existing_path, new_path])
