"""Tests for core Blueprint functionality."""

import datetime
import enum
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

import pytest
from pydantic import BaseModel, Field

from blueprint.core import Blueprint, TaskOrGroup, _resolve_refs


class SimpleConfig(BaseModel):
    name: str
    count: int = 5


class SimpleBlueprint(Blueprint[SimpleConfig]):
    """A simple test blueprint."""

    def render(self, config: SimpleConfig) -> TaskOrGroup:
        from airflow.operators.bash import BashOperator

        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo '{config.name} {config.count}'",
        )


class GroupConfig(BaseModel):
    items: list[str] = Field(default_factory=lambda: ["a", "b"])


class GroupBlueprint(Blueprint[GroupConfig]):
    """A blueprint that returns a TaskGroup."""

    def render(self, config: GroupConfig) -> TaskOrGroup:
        from airflow.operators.bash import BashOperator
        from airflow.utils.task_group import TaskGroup

        with TaskGroup(group_id=self.step_id) as group:
            for item in config.items:
                BashOperator(task_id=f"process_{item}", bash_command=f"echo '{item}'")
        return group


class TestBlueprint:
    """Test the core Blueprint class functionality."""

    def test_config_type_extracted(self):
        assert SimpleBlueprint.get_config_type() is SimpleConfig

    def test_config_type_extracted_group(self):
        assert GroupBlueprint.get_config_type() is GroupConfig

    def test_render_single_task(self):
        from datetime import datetime, timezone

        from airflow import DAG
        from airflow.models import BaseOperator

        bp = SimpleBlueprint()
        bp.step_id = "test_step"

        with DAG("test_dag", start_date=datetime(2024, 1, 1, tzinfo=timezone.utc)):
            result = bp.render(SimpleConfig(name="hello"))

        assert isinstance(result, BaseOperator)
        assert result.task_id == "test_step"

    def test_render_task_group(self):
        from datetime import datetime, timezone

        from airflow import DAG
        from airflow.utils.task_group import TaskGroup

        bp = GroupBlueprint()
        bp.step_id = "my_group"

        with DAG("test_dag", start_date=datetime(2024, 1, 1, tzinfo=timezone.utc)):
            result = bp.render(GroupConfig(items=["x", "y"]))

        assert isinstance(result, TaskGroup)

    def test_render_not_implemented(self):
        class EmptyConfig(BaseModel):
            pass

        class EmptyBlueprint(Blueprint[EmptyConfig]):
            pass

        bp = EmptyBlueprint()
        bp.step_id = "test"

        with pytest.raises(NotImplementedError, match="EmptyBlueprint must implement"):
            bp.render(EmptyConfig())

    def test_get_schema(self):
        schema = SimpleBlueprint.get_schema()
        assert schema["type"] == "object"
        assert "name" in schema["properties"]
        assert "count" in schema["properties"]
        assert schema["properties"]["count"]["default"] == 5

    def test_get_source_code(self):
        source = SimpleBlueprint.get_source_code()
        assert "class SimpleBlueprint" in source
        assert "class SimpleConfig" in source

    def test_config_type_missing(self):
        class NoConfigBlueprint(Blueprint):
            pass

        with pytest.raises(RuntimeError, match="not properly initialized"):
            NoConfigBlueprint.get_config_type()


class TestParseNameAndVersion:
    """Test blueprint name and version parsing from class names."""

    def test_simple_name_v1(self):
        name, version = SimpleBlueprint.parse_name_and_version()
        assert name == "simple_blueprint"
        assert version == 1

    def test_versioned_class(self):
        class ExtractV2Config(BaseModel):
            x: int = 1

        class ExtractV2(Blueprint[ExtractV2Config]):
            def render(self, config):
                pass

        name, version = ExtractV2.parse_name_and_version()
        assert name == "extract"
        assert version == 2

    def test_multi_word_versioned(self):
        class MultiSourceETLV3Config(BaseModel):
            x: int = 1

        class MultiSourceETLV3(Blueprint[MultiSourceETLV3Config]):
            def render(self, config):
                pass

        name, version = MultiSourceETLV3.parse_name_and_version()
        assert name == "multi_source_etl"
        assert version == 3

    def test_no_version_suffix(self):
        name, version = GroupBlueprint.parse_name_and_version()
        assert name == "group_blueprint"
        assert version == 1

    def test_camel_case_conversion(self):
        class MyComplexBlueprint(Blueprint[SimpleConfig]):
            def render(self, config):
                pass

        name, version = MyComplexBlueprint.parse_name_and_version()
        assert name == "my_complex_blueprint"
        assert version == 1


class TestYamlTypeValidation:
    """Test that Blueprint config models are validated for YAML-compatible types."""

    def test_valid_str_int_float_bool(self):
        class ScalarConfig(BaseModel):
            s: str
            i: int
            f: float
            b: bool

        class ScalarBp(Blueprint[ScalarConfig]):
            def render(self, config):
                pass

        assert ScalarBp.get_config_type() is ScalarConfig

    def test_valid_none_optional(self):
        class OptConfig(BaseModel):
            x: str | None = None
            y: int | None = None

        class OptBp(Blueprint[OptConfig]):
            def render(self, config):
                pass

        assert OptBp.get_config_type() is OptConfig

    def test_valid_list_dict(self):
        class CollConfig(BaseModel):
            tags: list[str] = Field(default_factory=list)
            metadata: dict[str, int] = Field(default_factory=dict)

        class CollBp(Blueprint[CollConfig]):
            def render(self, config):
                pass

        assert CollBp.get_config_type() is CollConfig

    def test_valid_nested_model(self):
        class Inner(BaseModel):
            value: str

        class OuterConfig(BaseModel):
            inner: Inner
            inners: list[Inner] = Field(default_factory=list)

        class OuterBp(Blueprint[OuterConfig]):
            def render(self, config):
                pass

        assert OuterBp.get_config_type() is OuterConfig

    def test_valid_literal(self):
        class LitConfig(BaseModel):
            mode: Literal["append", "overwrite"] = "append"

        class LitBp(Blueprint[LitConfig]):
            def render(self, config):
                pass

        assert LitBp.get_config_type() is LitConfig

    def test_valid_literal_mixed_scalars(self):
        class LitMixedConfig(BaseModel):
            val: Literal[1, 2, "three", True]

        class LitMixedBp(Blueprint[LitMixedConfig]):
            def render(self, config):
                pass

        assert LitMixedBp.get_config_type() is LitMixedConfig

    def test_valid_union(self):
        class UnionConfig(BaseModel):
            value: str | int

        class UnionBp(Blueprint[UnionConfig]):
            def render(self, config):
                pass

        assert UnionBp.get_config_type() is UnionConfig

    def test_invalid_enum(self):
        class Color(enum.Enum):
            RED = "red"
            BLUE = "blue"

        class EnumConfig(BaseModel):
            color: Color = Color.RED

        with pytest.raises(TypeError, match="non-YAML-compatible"):

            class EnumBp(Blueprint[EnumConfig]):
                def render(self, config):
                    pass

    def test_invalid_datetime(self):
        class DtConfig(BaseModel):
            dt: datetime.datetime

        with pytest.raises(TypeError, match="non-YAML-compatible"):

            class DtBp(Blueprint[DtConfig]):
                def render(self, config):
                    pass

    def test_invalid_uuid(self):
        class UuidConfig(BaseModel):
            uid: uuid.UUID

        with pytest.raises(TypeError, match="non-YAML-compatible"):

            class UuidBp(Blueprint[UuidConfig]):
                def render(self, config):
                    pass

    def test_invalid_path(self):
        class PathConfig(BaseModel):
            p: Path

        with pytest.raises(TypeError, match="non-YAML-compatible"):

            class PathBp(Blueprint[PathConfig]):
                def render(self, config):
                    pass

    def test_invalid_decimal(self):
        class DecConfig(BaseModel):
            d: Decimal

        with pytest.raises(TypeError, match="non-YAML-compatible"):

            class DecBp(Blueprint[DecConfig]):
                def render(self, config):
                    pass

    def test_invalid_any(self):
        class AnyConfig(BaseModel):
            x: Any

        with pytest.raises(TypeError, match="non-YAML-compatible"):

            class AnyBp(Blueprint[AnyConfig]):
                def render(self, config):
                    pass

    def test_invalid_standalone_none(self):
        class NoneConfig(BaseModel):
            x: None = None

        with pytest.raises(TypeError, match="non-YAML-compatible"):

            class NoneBp(Blueprint[NoneConfig]):
                def render(self, config):
                    pass

    def test_invalid_literal_none(self):
        class LitNoneConfig(BaseModel):
            x: Literal["a", None] = "a"

        with pytest.raises(TypeError, match="Literal value None"):

            class LitNoneBp(Blueprint[LitNoneConfig]):
                def render(self, config):
                    pass

    def test_invalid_bytes(self):
        class BytesConfig(BaseModel):
            data: bytes

        with pytest.raises(TypeError, match="non-YAML-compatible"):

            class BytesBp(Blueprint[BytesConfig]):
                def render(self, config):
                    pass

    def test_invalid_set(self):
        class SetConfig(BaseModel):
            tags: set[str]

        with pytest.raises(TypeError, match="non-YAML-compatible"):

            class SetBp(Blueprint[SetConfig]):
                def render(self, config):
                    pass

    def test_invalid_tuple(self):
        class TupleConfig(BaseModel):
            pair: tuple[str, int]

        with pytest.raises(TypeError, match="non-YAML-compatible"):

            class TupleBp(Blueprint[TupleConfig]):
                def render(self, config):
                    pass

    def test_invalid_complex(self):
        class ComplexConfig(BaseModel):
            c: complex

        with pytest.raises(TypeError, match="non-YAML-compatible"):

            class ComplexBp(Blueprint[ComplexConfig]):
                def render(self, config):
                    pass

    def test_invalid_dict_int_keys(self):
        class IntKeyConfig(BaseModel):
            mapping: dict[int, str]

        with pytest.raises(TypeError, match="dict key type must be str"):

            class IntKeyBp(Blueprint[IntKeyConfig]):
                def render(self, config):
                    pass

    def test_invalid_nested_set_in_list(self):
        class NestedBadConfig(BaseModel):
            items: list[set[str]]

        with pytest.raises(TypeError, match="non-YAML-compatible"):

            class NestedBadBp(Blueprint[NestedBadConfig]):
                def render(self, config):
                    pass

    def test_invalid_nested_model_field(self):
        class BadInner(BaseModel):
            data: bytes

        class OuterBadConfig(BaseModel):
            inner: BadInner

        with pytest.raises(TypeError, match="non-YAML-compatible"):

            class OuterBadBp(Blueprint[OuterBadConfig]):
                def render(self, config):
                    pass

    def test_error_message_includes_class_and_field(self):
        class BadFieldConfig(BaseModel):
            good: str
            bad: set[int]

        with pytest.raises(TypeError, match=r"(?s)BadBp.*BadFieldConfig.*bad"):

            class BadBp(Blueprint[BadFieldConfig]):
                def render(self, config):
                    pass

    def test_multiple_errors_collected(self):
        class MultiBadConfig(BaseModel):
            a: bytes
            b: set[str]

        with pytest.raises(TypeError, match=r"(?s).*a:.*b:"):

            class MultiBadBp(Blueprint[MultiBadConfig]):
                def render(self, config):
                    pass

    def test_invalid_bare_dict(self):
        class BareDictConfig(BaseModel):
            metadata: dict = Field(default_factory=dict)

        with pytest.raises(TypeError, match="non-YAML-compatible"):

            class BareDictBp(Blueprint[BareDictConfig]):
                def render(self, config):
                    pass

    def test_invalid_bare_list(self):
        class BareListConfig(BaseModel):
            items: list = Field(default_factory=list)

        with pytest.raises(TypeError, match="non-YAML-compatible"):

            class BareListBp(Blueprint[BareListConfig]):
                def render(self, config):
                    pass

    def test_valid_circular_model_reference(self):
        class TreeNode(BaseModel):
            value: str
            children: list["TreeNode"] = Field(default_factory=list)

        TreeNode.model_rebuild()

        class TreeConfig(BaseModel):
            root: TreeNode

        class TreeBp(Blueprint[TreeConfig]):
            def render(self, config):
                pass

        assert TreeBp.get_config_type() is TreeConfig

    def test_invalid_union_with_bad_member(self):
        class BadUnionConfig(BaseModel):
            x: str | bytes

        with pytest.raises(TypeError, match="non-YAML-compatible"):

            class BadUnionBp(Blueprint[BadUnionConfig]):
                def render(self, config):
                    pass


class TestResolveRefs:
    """Test that $ref/$defs resolution inlines definitions."""

    def test_no_defs_unchanged(self):
        schema = {
            "type": "object",
            "properties": {"name": {"type": "string"}},
        }
        result = _resolve_refs(schema)
        assert result == schema
        assert "$defs" not in result

    def test_simple_ref_inlined(self):
        schema = {
            "$defs": {
                "Inner": {
                    "type": "object",
                    "properties": {"value": {"type": "string"}},
                }
            },
            "type": "object",
            "properties": {
                "child": {"$ref": "#/$defs/Inner"},
            },
        }
        result = _resolve_refs(schema)
        assert "$defs" not in result
        assert "$ref" not in str(result)
        assert result["properties"]["child"] == {
            "type": "object",
            "properties": {"value": {"type": "string"}},
        }

    def test_ref_in_array_items(self):
        schema = {
            "$defs": {
                "Item": {
                    "type": "object",
                    "properties": {"x": {"type": "integer"}},
                }
            },
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {"$ref": "#/$defs/Item"},
                }
            },
        }
        result = _resolve_refs(schema)
        assert "$defs" not in result
        assert result["properties"]["items"]["items"] == {
            "type": "object",
            "properties": {"x": {"type": "integer"}},
        }

    def test_nested_model_schema_flattened(self):
        class Inner(BaseModel):
            value: str

        class Outer(BaseModel):
            inner: Inner
            inners: list[Inner]

        class TestBp(Blueprint[Outer]):
            def render(self, config):
                pass

        schema = TestBp.get_schema()
        assert "$defs" not in schema
        assert "$ref" not in str(schema)
        assert "value" in schema["properties"]["inner"]["properties"]
        assert "value" in schema["properties"]["inners"]["items"]["properties"]

    def test_deeply_nested_model(self):
        class Level2(BaseModel):
            z: int

        class Level1(BaseModel):
            nested: Level2

        class TopConfig(BaseModel):
            items: list[Level1]

        class DeepBp(Blueprint[TopConfig]):
            def render(self, config):
                pass

        schema = DeepBp.get_schema()
        assert "$defs" not in schema
        assert "$ref" not in str(schema)
        item_schema = schema["properties"]["items"]["items"]
        assert "nested" in item_schema["properties"]
        assert "z" in item_schema["properties"]["nested"]["properties"]

    def test_circular_ref_preserved(self):
        schema = {
            "$defs": {
                "Node": {
                    "type": "object",
                    "properties": {
                        "value": {"type": "string"},
                        "child": {"$ref": "#/$defs/Node"},
                    },
                }
            },
            "type": "object",
            "properties": {
                "root": {"$ref": "#/$defs/Node"},
            },
        }
        result = _resolve_refs(schema)
        assert "$defs" not in result
        root = result["properties"]["root"]
        assert root["properties"]["value"] == {"type": "string"}
        assert "$ref" in root["properties"]["child"]

    def test_non_defs_ref_preserved(self):
        schema = {
            "$defs": {
                "Inner": {
                    "type": "object",
                    "properties": {"x": {"type": "string"}},
                }
            },
            "type": "object",
            "properties": {
                "local": {"$ref": "#/$defs/Inner"},
                "ext": {"$ref": "https://example.com/schema.json"},
            },
        }
        result = _resolve_refs(schema)
        assert "$defs" not in result
        assert result["properties"]["local"]["type"] == "object"
        assert result["properties"]["ext"] == {"$ref": "https://example.com/schema.json"}
