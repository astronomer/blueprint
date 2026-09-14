"""Tests for declarative field conditions (``applies_when`` / ``mandatory``)."""

import itertools
from typing import Literal

import pytest
from pydantic import BaseModel
from pydantic import ValidationError as PydanticValidationError

from blueprint import AllOf, AnyOf, Condition, Field, Not
from blueprint.builder import Builder, DAGConfig, StepConfig
from blueprint.conditions import (
    APPLIES_WHEN_KEY,
    MANDATORY_KEY,
    check,
    declaration_errors,
    evaluate,
    from_json,
)
from blueprint.core import Blueprint, BlueprintDagArgs, TaskOrGroup
from blueprint.registry import BlueprintRegistry


class ScheduleConfig(BaseModel):
    schedule_type: Literal["time", "asset"] = "time"
    size: int = 8
    region: str = "us"
    cron: str | None = Field(
        None, applies_when=Condition("schedule_type", "==", "time"), mandatory=True
    )
    assets: list[str] | None = Field(
        None, applies_when=Condition("schedule_type", "==", "asset"), mandatory=True
    )
    retries: int | None = Field(
        None,
        applies_when=AnyOf(Condition("size", ">=", 5), Condition("schedule_type", "==", "asset")),
    )
    archive: str | None = Field(None, applies_when=Not(Condition("region", "in", ["us", "eu"])))


class Schedule(Blueprint[ScheduleConfig]):
    """Schedule a DAG by time or by asset."""

    def render(self, config: ScheduleConfig) -> TaskOrGroup:
        from airflow.operators.bash import BashOperator

        return BashOperator(task_id=self.step_id, bash_command=f"echo {config.schedule_type}")


class TestConditionTree:
    def test_atom_json(self):
        assert Condition("mode", "==", "batch").to_json() == {
            "field": "mode",
            "op": "==",
            "value": "batch",
        }

    def test_presence_atom_omits_value(self):
        assert Condition("archive", "set").to_json() == {"field": "archive", "op": "set"}

    def test_group_json(self):
        condition = AllOf(Condition("a", "==", 1), Condition("b", ">=", 2))
        assert condition.to_json() == {
            "allOf": [
                {"field": "a", "op": "==", "value": 1},
                {"field": "b", "op": ">=", "value": 2},
            ]
        }

    def test_group_accepts_an_iterable(self):
        atoms = [Condition("a", "==", 1), Condition("b", "==", 2)]
        assert AnyOf(atoms).to_json() == AnyOf(*atoms).to_json()

    def test_not_json(self):
        assert Not(Condition("a", "==", 1)).to_json() == {
            "not": {"field": "a", "op": "==", "value": 1}
        }

    @pytest.mark.parametrize(
        "condition",
        [
            Condition("a", "==", 1),
            Condition("a", "set"),
            AllOf(Condition("a", "==", 1), Not(Condition("b", "in", [1, 2]))),
            AnyOf(Condition("a", "matches", "^x"), Condition("b", "<", 3)),
        ],
    )
    def test_json_round_trip(self, condition):
        assert from_json(condition.to_json()).to_json() == condition.to_json()

    def test_describe(self):
        condition = AnyOf(Condition("size", ">=", 5), Not(Condition("mode", "==", "batch")))
        assert condition.describe() == "(size >= 5 or not mode == 'batch')"

    def test_describe_presence(self):
        assert Condition("archive", "unset").describe() == "archive is unset"

    def test_from_json_rejects_non_mapping(self):
        with pytest.raises(TypeError, match="must be a mapping"):
            from_json([{"field": "a", "op": "=="}])

    def test_from_json_rejects_empty_group(self):
        with pytest.raises(ValueError, match="at least one condition"):
            from_json({"allOf": []})

    def test_from_json_rejects_incomplete_atom(self):
        with pytest.raises(ValueError, match="needs 'field' and 'op'"):
            from_json({"field": "a"})


class TestEvaluate:
    @pytest.mark.parametrize(
        ("condition", "expected"),
        [
            (Condition("size", ">=", 5), True),
            (Condition("size", ">", 8), False),
            (Condition("size", "<=", 8), True),
            (Condition("schedule_type", "==", "time"), True),
            (Condition("schedule_type", "!=", "time"), False),
            (Condition("region", "in", ["us", "eu"]), True),
            (Condition("region", "not in", ["us", "eu"]), False),
            (Condition("region", "matches", "^u"), True),
            (Condition("cron", "set"), False),
            (Condition("cron", "unset"), True),
        ],
    )
    def test_operators(self, condition, expected):
        assert evaluate(condition, ScheduleConfig()) is expected

    def test_combinators(self):
        config = ScheduleConfig(size=2)
        assert evaluate(AnyOf(Condition("size", ">=", 5), Condition("region", "==", "us")), config)
        assert not evaluate(
            AllOf(Condition("size", ">=", 5), Condition("region", "==", "us")), config
        )
        assert evaluate(Not(Condition("size", ">=", 5)), config)

    def test_evaluates_against_a_mapping(self):
        assert evaluate(Condition("size", ">=", 5), {"size": 9})
        assert not evaluate(Condition("size", ">=", 5), {})

    def test_incomparable_values_do_not_hold(self):
        assert not evaluate(Condition("cron", ">=", 5), ScheduleConfig())


class TestCheck:
    def test_applicable_field_passes(self):
        check(ScheduleConfig(schedule_type="time", cron="@daily"))

    def test_inapplicable_field_rejected(self):
        with pytest.raises(PydanticValidationError) as exc:
            check(ScheduleConfig(schedule_type="time", cron="@daily", assets=["a"]))

        errors = exc.value.errors()
        assert [e["loc"] for e in errors] == [("assets",)]
        assert errors[0]["msg"] == "only applicable when schedule_type == 'asset'"
        assert errors[0]["type"] == "blueprint_applies_when"

    def test_missing_mandatory_field_rejected(self):
        with pytest.raises(PydanticValidationError, match="required when schedule_type == 'asset'"):
            check(ScheduleConfig(schedule_type="asset"))

    def test_mandatory_field_not_required_when_inapplicable(self):
        check(ScheduleConfig(schedule_type="asset", assets=["a"]))

    def test_explicit_null_counts_as_absent(self):
        check(ScheduleConfig(schedule_type="time", cron="@daily", assets=None))

    def test_combinator_condition_enforced(self):
        check(ScheduleConfig(cron="@daily", size=9, retries=2))
        with pytest.raises(PydanticValidationError, match="only applicable when \\(size >= 5 or"):
            check(ScheduleConfig(cron="@daily", size=2, retries=2))

    def test_negated_condition_enforced(self):
        check(ScheduleConfig(cron="@daily", region="ap", archive="s3://x"))
        with pytest.raises(PydanticValidationError, match="not region in"):
            check(ScheduleConfig(cron="@daily", region="us", archive="s3://x"))

    def test_all_violations_reported_together(self):
        with pytest.raises(PydanticValidationError) as exc:
            check(ScheduleConfig(schedule_type="asset", cron="@daily", retries=1, size=2))

        assert {e["loc"] for e in exc.value.errors()} == {("cron",), ("assets",)}

    def test_nested_model_checked(self):
        class Inner(BaseModel):
            mode: Literal["batch", "stream"] = "batch"
            window: str | None = Field(None, applies_when=Condition("mode", "==", "stream"))

        class Outer(BaseModel):
            inner: Inner

        with pytest.raises(PydanticValidationError) as exc:
            check(Outer(inner=Inner(mode="batch", window="5m")))

        assert [e["loc"] for e in exc.value.errors()] == [("inner", "window")]

    def test_list_of_models_checked(self):
        class Watcher(BaseModel):
            kind: Literal["file", "table"] = "file"
            path: str | None = Field(None, applies_when=Condition("kind", "==", "file"))

        class Outer(BaseModel):
            watchers: list[Watcher]

        with pytest.raises(PydanticValidationError) as exc:
            check(Outer(watchers=[Watcher(), Watcher(kind="table", path="/tmp/x")]))

        assert [e["loc"] for e in exc.value.errors()] == [("watchers", 1, "path")]

    def test_dict_of_models_checked(self):
        class Sink(BaseModel):
            kind: Literal["s3", "gcs"] = "s3"
            bucket: str | None = Field(None, applies_when=Condition("kind", "==", "gcs"))

        class Outer(BaseModel):
            sinks: dict[str, Sink]

        with pytest.raises(PydanticValidationError) as exc:
            check(Outer(sinks={"main": Sink(kind="s3", bucket="b")}))

        assert [e["loc"] for e in exc.value.errors()] == [("sinks", "main", "bucket")]


class TestDeclarationErrors:
    def test_valid_model_has_no_errors(self):
        assert declaration_errors(ScheduleConfig) == []

    def test_unknown_field(self):
        class Config(BaseModel):
            a: int = 1
            b: str | None = Field(None, applies_when=Condition("nope", "==", 1))

        assert "unknown field 'nope'" in declaration_errors(Config)[0]

    def test_unknown_operator(self):
        class Config(BaseModel):
            a: int = 1
            b: str | None = Field(None, applies_when=Condition("a", "~=", 1))  # type: ignore[arg-type]

        assert "unknown operator '~='" in declaration_errors(Config)[0]

    def test_self_reference(self):
        class Config(BaseModel):
            b: str | None = Field(None, applies_when=Condition("b", "set"))

        assert "cannot reference the field it is declared on" in declaration_errors(Config)[0]

    def test_field_must_be_optional(self):
        class Config(BaseModel):
            a: int = 1
            b: str = Field("x", applies_when=Condition("a", "==", 1))

        assert "must be optional and default to None" in declaration_errors(Config)[0]

    def test_required_field_rejected(self):
        class Config(BaseModel):
            a: int = 1
            b: str = Field(applies_when=Condition("a", "==", 1))

        assert "must be optional and default to None" in declaration_errors(Config)[0]

    def test_literal_value_must_be_a_member(self):
        class Config(BaseModel):
            mode: Literal["batch", "stream"] = "batch"
            b: str | None = Field(None, applies_when=Condition("mode", "==", "micro"))

        assert (
            "'micro' is not a valid one of 'batch', 'stream' value" in declaration_errors(Config)[0]
        )

    def test_ordering_needs_a_numeric_field(self):
        class Config(BaseModel):
            name: str = "x"
            b: str | None = Field(None, applies_when=Condition("name", ">=", 5))

        assert "'>=' needs a numeric field, got str" in declaration_errors(Config)[0]

    def test_ordering_needs_a_numeric_value(self):
        class Config(BaseModel):
            size: int = 1
            b: str | None = Field(None, applies_when=Condition("size", ">=", "big"))

        assert "'>=' needs a numeric value" in declaration_errors(Config)[0]

    def test_membership_needs_a_list(self):
        class Config(BaseModel):
            region: str = "us"
            b: str | None = Field(None, applies_when=Condition("region", "in", "us"))

        assert "needs a non-empty list" in declaration_errors(Config)[0]

    def test_membership_members_are_checked(self):
        class Config(BaseModel):
            size: int = 1
            b: str | None = Field(None, applies_when=Condition("size", "in", [1, "two"]))

        assert "'two' is not a valid int value" in declaration_errors(Config)[0]

    def test_pattern_must_compile(self):
        class Config(BaseModel):
            name: str = "x"
            b: str | None = Field(None, applies_when=Condition("name", "matches", "("))

        assert "invalid regular expression" in declaration_errors(Config)[0]

    def test_presence_operator_takes_no_value(self):
        class Config(BaseModel):
            a: int = 1
            b: str | None = Field(None, applies_when=Condition("a", "set", 1))

        assert "takes no value" in declaration_errors(Config)[0]

    def test_comparison_needs_a_value(self):
        class Config(BaseModel):
            a: int = 1
            b: str | None = Field(None, applies_when=Condition("a", "=="))

        assert "needs a value" in declaration_errors(Config)[0]

    def test_none_value_points_at_presence_operators(self):
        class Config(BaseModel):
            a: str | None = None
            b: str | None = Field(None, applies_when=Condition("a", "==", None))

        assert "use the 'set' / 'unset' operators" in declaration_errors(Config)[0]

    def test_non_scalar_reference_allows_only_presence(self):
        class Config(BaseModel):
            tags: list[str] | None = None
            b: str | None = Field(None, applies_when=Condition("tags", "==", ["a"]))
            c: str | None = Field(None, applies_when=Condition("tags", "set"))

        errors = declaration_errors(Config)
        assert len(errors) == 1
        assert "not a scalar field" in errors[0]

    def test_mandatory_needs_a_condition(self):
        class Config(BaseModel):
            b: str | None = Field(None, json_schema_extra={MANDATORY_KEY: True})

        assert "no meaning without 'applies_when'" in declaration_errors(Config)[0]

    def test_nested_model_errors_are_prefixed(self):
        class Inner(BaseModel):
            a: int = 1
            b: str | None = Field(None, applies_when=Condition("nope", "set"))

        class Outer(BaseModel):
            inner: Inner

        assert declaration_errors(Outer)[0].startswith("inner.b:")

    def test_blueprint_definition_rejects_bad_condition(self):
        class BadConfig(BaseModel):
            a: int = 1
            b: str | None = Field(None, applies_when=Condition("nope", "set"))

        with pytest.raises(TypeError, match="invalid field conditions"):

            class Bad(Blueprint[BadConfig]):
                def render(self, config: BadConfig) -> TaskOrGroup:
                    raise NotImplementedError

    def test_dag_args_definition_rejects_bad_condition(self):
        class BadArgsConfig(BaseModel):
            a: int = 1
            b: str | None = Field(None, applies_when=Condition("nope", "set"))

        with pytest.raises(TypeError, match="invalid field conditions"):

            class BadArgs(BlueprintDagArgs[BadArgsConfig]):
                def render(self, config: BadArgsConfig) -> dict:
                    return {"description": str(config.a)}


class TestFieldDeclaration:
    def test_mandatory_without_condition_rejected(self):
        with pytest.raises(ValueError, match="no meaning without 'applies_when'"):
            Field(None, mandatory=True)

    def test_condition_must_be_a_condition(self):
        with pytest.raises(TypeError, match="must be a Condition"):
            Field(None, applies_when=("a", "==", 1))

    def test_declared_schema_extra_is_kept(self):
        class Config(BaseModel):
            a: int = 1
            b: str | None = Field(
                None,
                applies_when=Condition("a", "==", 1),
                json_schema_extra={"x-custom": "kept"},
            )

        extra = Config.model_fields["b"].json_schema_extra
        assert extra["x-custom"] == "kept"
        assert APPLIES_WHEN_KEY in extra

    def test_callable_schema_extra_rejected(self):
        with pytest.raises(TypeError, match="must be a dict"):
            Field(None, applies_when=Condition("a", "set"), json_schema_extra=lambda _schema: None)


class TestSchema:
    def test_condition_published_on_the_property(self):
        schema = Schedule.get_schema()
        assert schema["properties"]["cron"][APPLIES_WHEN_KEY] == {
            "field": "schedule_type",
            "op": "==",
            "value": "time",
        }
        assert schema["properties"]["cron"][MANDATORY_KEY] is True

    def test_conditional_field_keeps_a_single_string_type(self):
        schema = Schedule.get_schema()
        assert schema["properties"]["cron"]["type"] == "string"

    def test_mandatory_field_is_not_unconditionally_required(self):
        assert "cron" not in Schedule.get_schema().get("required", [])

    def test_clause_carries_requiredness_and_applicability(self):
        clauses = Schedule.get_schema()["allOf"]
        assets = next(c for c in clauses if c["else"]["not"]["required"] == ["assets"])
        assert assets["if"] == {
            "properties": {"schedule_type": {"const": "asset"}},
            "required": ["schedule_type"],
        }
        assert assets["then"] == {"required": ["assets"]}

    def test_non_mandatory_clause_has_no_then(self):
        clauses = Schedule.get_schema()["allOf"]
        retries = next(c for c in clauses if c["else"]["not"]["required"] == ["retries"])
        assert "then" not in retries

    def test_satisfying_default_is_folded_into_the_clause(self):
        clauses = Schedule.get_schema()["allOf"]
        cron = next(c for c in clauses if c["else"]["not"]["required"] == ["cron"])
        assert cron["if"]["anyOf"][1] == {"not": {"required": ["schedule_type"]}}

    def test_nested_model_clauses_are_inlined(self):
        class Inner(BaseModel):
            mode: Literal["batch", "stream"] = "batch"
            window: str | None = Field(None, applies_when=Condition("mode", "==", "stream"))

        class OuterConfig(BaseModel):
            inner: Inner

        class Outer(Blueprint[OuterConfig]):
            """Outer."""

            def render(self, config: OuterConfig) -> TaskOrGroup:
                raise NotImplementedError

        inner = Outer.get_schema()["properties"]["inner"]
        assert inner["allOf"][0]["else"] == {"not": {"required": ["window"]}}

    def test_unconditional_model_has_no_clauses(self):
        class PlainConfig(BaseModel):
            a: int = 1

        class Plain(Blueprint[PlainConfig]):
            """Plain."""

            def render(self, config: PlainConfig) -> TaskOrGroup:
                raise NotImplementedError

        assert "allOf" not in Plain.get_schema()


class TestSchemaAgreesWithPython:
    """The published clauses must accept exactly what ``check()`` accepts."""

    def test_every_combination_agrees(self):
        jsonschema = pytest.importorskip("jsonschema")
        validator = jsonschema.Draft7Validator(Schedule.get_schema())

        axes = {
            "schedule_type": [None, "time", "asset"],
            "size": [None, 2, 9],
            "region": [None, "us", "ap"],
            "cron": [None, "@daily"],
            "assets": [None, ["a"]],
            "retries": [None, 3],
            "archive": [None, "s3://x"],
        }

        for combo in itertools.product(*axes.values()):
            payload = {k: v for k, v in zip(axes, combo, strict=True) if v is not None}
            try:
                check(ScheduleConfig(**payload))
                python_ok = True
            except PydanticValidationError:
                python_ok = False

            assert python_ok is validator.is_valid(payload), payload


class TestBuilderEnforcement:
    @pytest.fixture
    def builder(self):
        reg = BlueprintRegistry()
        reg._blueprints = {"schedule": {1: Schedule}}
        reg._blueprint_locations = {"schedule": {1: "test.py"}}
        reg._discovered = True
        return Builder(bp_registry=reg)

    def test_valid_step_builds(self, builder):
        config = DAGConfig(
            dag_id="ok_dag",
            steps={"s": StepConfig(blueprint="schedule", schedule_type="time", cron="@daily")},
        )
        assert builder.build(config).dag_id == "ok_dag"

    def test_inapplicable_field_fails_the_build(self, builder):
        from blueprint.errors import ConfigurationError

        config = DAGConfig(
            dag_id="bad_dag",
            steps={
                "s": StepConfig(
                    blueprint="schedule", schedule_type="time", cron="@daily", assets=["a"]
                )
            },
        )
        with pytest.raises(ConfigurationError, match="only applicable when"):
            builder.build(config)

    def test_missing_mandatory_field_fails_the_build(self, builder):
        from blueprint.errors import ConfigurationError

        config = DAGConfig(
            dag_id="bad_dag",
            steps={"s": StepConfig(blueprint="schedule", schedule_type="asset")},
        )
        with pytest.raises(ConfigurationError, match="required when"):
            builder.build(config)

    def test_dag_args_conditions_enforced(self):
        class ArgsConfig(BaseModel):
            schedule_type: Literal["time", "asset"] = "time"
            cron: str | None = Field(
                None, applies_when=Condition("schedule_type", "==", "time"), mandatory=True
            )

        class Args(BlueprintDagArgs[ArgsConfig]):
            def render(self, config: ArgsConfig) -> dict:
                return {"description": config.schedule_type}

        from blueprint.errors import ConfigurationError

        reg = BlueprintRegistry()
        reg._blueprints = {"schedule": {1: Schedule}}
        reg._blueprint_locations = {"schedule": {1: "test.py"}}
        reg._dag_args = {"args": Args}
        reg._dag_args_locations = {"args": "test.py"}
        reg._discovered = True

        config = DAGConfig(
            dag_id="args_dag",
            schedule_type="asset",
            cron="@daily",
            steps={"s": StepConfig(blueprint="schedule", schedule_type="asset", assets=["a"])},
        )
        with pytest.raises(ConfigurationError, match="only applicable when"):
            Builder(bp_registry=reg).validate_dag_args(config)


class TestInteractivePrompting:
    @pytest.fixture
    def info(self):
        reg = BlueprintRegistry()
        reg._blueprints = {"schedule": {1: Schedule}}
        reg._blueprint_locations = {"schedule": {1: "test.py"}}
        reg._discovered = True
        return reg.get_blueprint_info("schedule")

    def _collect(self, monkeypatch, info, answers):
        from blueprint import cli

        asked: list[str] = []
        pending = list(answers)

        def fake_input(prompt: str = "") -> str:
            asked.append(prompt.split(" ")[0].rstrip(":"))
            return pending.pop(0)

        monkeypatch.setattr(cli.console, "input", fake_input)
        return cli._collect_parameters(info), asked

    def test_inapplicable_fields_are_skipped(self, monkeypatch, info):
        config, asked = self._collect(monkeypatch, info, ["", "", "", "@daily", ""])

        assert asked == ["schedule_type", "size", "region", "cron", "retries"]
        assert config == {"schedule_type": "time", "size": 8, "region": "us", "cron": "@daily"}

    def test_answers_steer_which_fields_are_asked(self, monkeypatch, info):
        config, asked = self._collect(monkeypatch, info, ["asset", "", "", "a,b", ""])

        assert asked == ["schedule_type", "size", "region", "assets", "retries"]
        assert config["assets"] == ["a", "b"]
        assert "cron" not in config

    def test_collected_config_passes_validation(self, monkeypatch, info):
        config, _asked = self._collect(monkeypatch, info, ["", "", "", "@daily", ""])
        check(ScheduleConfig(**config))


class TestResolveConfig:
    def test_param_override_breaking_a_condition_is_rejected(self):
        blueprint = Schedule()
        blueprint.step_id = "s"
        config = ScheduleConfig(schedule_type="asset", assets=["a"])

        with pytest.raises(PydanticValidationError, match="only applicable when"):
            blueprint.resolve_config(config, {"params": {"s__cron": "@daily"}})
