"""Tests for declarative variable resolution."""

import pytest
import yaml

from blueprint import vars as bp_vars
from blueprint.errors import (
    CompositionDepthError,
    CyclicVariableError,
    IncompleteVariableError,
    InvalidVariableNameError,
    InvalidVariableValueError,
    ProfileError,
    UndefinedVariableError,
)


def write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(data if isinstance(data, str) else yaml.safe_dump(data, sort_keys=False))
    return path


def dag_file(tmp_path, body, name="x.dag.yaml"):
    return write(tmp_path / name, body)


def resolve(path, profile=None, search_root=None):
    config = yaml.safe_load(path.read_text())
    return bp_vars.resolve(config, path, profile=profile, search_root=search_root)


class TestBasicSubstitution:
    def test_substitutes_into_step_config(self, tmp_path):
        p = dag_file(
            tmp_path,
            "dag_id: d\nvars:\n  db: analytics\nsteps:\n  s:\n    blueprint: b\n    t: ${db}.tbl\n",
        )

        out, _ = resolve(p)

        assert out["steps"]["s"]["t"] == "analytics.tbl"

    def test_vars_key_is_stripped(self, tmp_path):
        p = dag_file(tmp_path, "dag_id: d\nvars:\n  a: 1\nsteps:\n  s:\n    blueprint: b\n")

        out, _ = resolve(p)

        assert "vars" not in out

    def test_substitutes_into_dag_level_args(self, tmp_path):
        p = dag_file(
            tmp_path,
            "dag_id: d\nschedule: ${sched}\nvars:\n  sched: '@daily'\nsteps:\n  s:\n    blueprint: b\n",
        )

        out, _ = resolve(p)

        assert out["schedule"] == "@daily"

    def test_multiple_references_in_one_string(self, tmp_path):
        p = dag_file(
            tmp_path,
            "dag_id: d\nvars:\n  a: x\n  b: y\nsteps:\n  s:\n    blueprint: b\n    t: ${a}.${b}.z\n",
        )

        out, _ = resolve(p)

        assert out["steps"]["s"]["t"] == "x.y.z"

    def test_substitutes_inside_lists(self, tmp_path):
        p = dag_file(
            tmp_path,
            "dag_id: d\nvars:\n  t: team\nsteps:\n  s:\n    blueprint: b\n"
            "    tags:\n      - ${t}\n      - fixed\n",
        )

        out, _ = resolve(p)

        assert out["steps"]["s"]["tags"] == ["team", "fixed"]

    def test_undefined_variable_raises_with_suggestion(self, tmp_path):
        p = dag_file(
            tmp_path,
            "dag_id: d\nvars:\n  database: x\nsteps:\n  s:\n    blueprint: b\n    t: ${databse}\n",
        )

        with pytest.raises(UndefinedVariableError, match="Did you mean 'database'"):
            resolve(p)

    def test_dotted_reference_is_rejected(self, tmp_path):
        p = dag_file(
            tmp_path,
            "dag_id: d\nvars:\n  a: x\nsteps:\n  s:\n    blueprint: b\n    t: ${env.FOO}\n",
        )

        with pytest.raises(UndefinedVariableError, match="reserved for future namespaces"):
            resolve(p)


class TestTypePreservation:
    @pytest.mark.parametrize(
        ("literal", "expected"),
        [("90", 90), ("true", True), ("false", False), ("1.5", 1.5), ("null", None)],
    )
    def test_whole_value_reference_keeps_type(self, tmp_path, literal, expected):
        p = dag_file(
            tmp_path,
            f"dag_id: d\nvars:\n  v: {literal}\nsteps:\n  s:\n    blueprint: b\n    n: ${{v}}\n",
        )

        out, _ = resolve(p)

        assert out["steps"]["s"]["n"] is expected or out["steps"]["s"]["n"] == expected

    def test_embedded_reference_becomes_string(self, tmp_path):
        p = dag_file(
            tmp_path,
            "dag_id: d\nvars:\n  v: 90\nsteps:\n  s:\n    blueprint: b\n    n: days-${v}\n",
        )

        out, _ = resolve(p)

        assert out["steps"]["s"]["n"] == "days-90"

    def test_padded_reference_stays_a_string(self, tmp_path):
        p = dag_file(
            tmp_path,
            'dag_id: d\nvars:\n  v: 90\nsteps:\n  s:\n    blueprint: b\n    n: "  ${v}  "\n',
        )

        out, _ = resolve(p)

        assert out["steps"]["s"]["n"] == "  90  "

    def test_list_value_preserved(self, tmp_path):
        p = dag_file(
            tmp_path,
            "dag_id: d\nvars:\n  v: [a, b]\nsteps:\n  s:\n    blueprint: b\n    n: ${v}\n",
        )

        out, _ = resolve(p)

        assert out["steps"]["s"]["n"] == ["a", "b"]


class TestProfilesAreOptional:
    """Everything except per-profile values works with no `profiles:` declared."""

    def test_vars_file_without_profiles(self, tmp_path):
        write(tmp_path / bp_vars.VARS_FILENAME, "vars:\n  db: analytics\n")
        p = dag_file(tmp_path, "dag_id: d\nsteps:\n  s:\n    blueprint: b\n    t: ${db}.x\n")

        assert resolve(p)[0]["steps"]["s"]["t"] == "analytics.x"

    def test_dag_vars_with_no_vars_file(self, tmp_path):
        p = dag_file(
            tmp_path,
            "dag_id: d\nvars:\n  db: analytics\nsteps:\n  s:\n    blueprint: b\n    t: ${db}.x\n",
        )

        assert resolve(p)[0]["steps"]["s"]["t"] == "analytics.x"

    def test_lists_and_composition_without_profiles(self, tmp_path):
        write(tmp_path / bp_vars.VARS_FILENAME, "vars:\n  a: x\n  db: ${a}-y\n  l: [p, q]\n")
        p = dag_file(
            tmp_path,
            "dag_id: d\nsteps:\n  s:\n    blueprint: b\n    t: ${db}\n    n: ${l}\n",
        )

        out, _ = resolve(p)

        assert out["steps"]["s"]["t"] == "x-y"
        assert out["steps"]["s"]["n"] == ["p", "q"]


class TestProfiles:
    def _project(self, tmp_path):
        write(
            tmp_path / bp_vars.VARS_FILENAME,
            "profiles: [prod, dev]\nvars:\n  db:\n    prod: production_db\n    dev: dev_db\n",
        )
        return dag_file(tmp_path, "dag_id: d\nsteps:\n  s:\n    blueprint: b\n    t: ${db}.tbl\n")

    def test_selects_active_profile(self, tmp_path):
        p = self._project(tmp_path)

        assert resolve(p, "prod")[0]["steps"]["s"]["t"] == "production_db.tbl"
        assert resolve(p, "dev")[0]["steps"]["s"]["t"] == "dev_db.tbl"

    def test_no_profile_selected_raises_when_referenced(self, tmp_path):
        p = self._project(tmp_path)

        with pytest.raises(ProfileError, match="no profile was selected"):
            resolve(p)

    def test_no_profile_needed_when_varying_var_is_unreferenced(self, tmp_path):
        write(
            tmp_path / bp_vars.VARS_FILENAME,
            "profiles: [prod, dev]\nvars:\n"
            "  db:\n    prod: production_db\n    dev: dev_db\n"
            "  fixed: everywhere\n",
        )
        p = dag_file(tmp_path, "dag_id: d\nsteps:\n  s:\n    blueprint: b\n    t: ${fixed}.tbl\n")

        out, resolved = resolve(p)

        assert out["steps"]["s"]["t"] == "everywhere.tbl"
        assert resolved.referenced == {"fixed"}

    def test_unreferenced_incomplete_var_does_not_raise(self, tmp_path):
        write(
            tmp_path / bp_vars.VARS_FILENAME,
            "profiles: [prod, dev]\nvars:\n  db:\n    prod: only_prod\n  fixed: ok\n",
        )
        p = dag_file(tmp_path, "dag_id: d\nsteps:\n  s:\n    blueprint: b\n    t: ${fixed}\n")

        assert resolve(p, "dev")[0]["steps"]["s"]["t"] == "ok"

    def test_composition_through_unreferenced_varying_var_is_lazy(self, tmp_path):
        write(
            tmp_path / bp_vars.VARS_FILENAME,
            "profiles: [prod, dev]\nvars:\n"
            "  db:\n    prod: p\n    dev: d\n"
            "  wrapper: ${db}.suffix\n"
            "  fixed: plain\n",
        )
        p = dag_file(tmp_path, "dag_id: d\nsteps:\n  s:\n    blueprint: b\n    t: ${fixed}\n")

        # `wrapper` depends on a profile-varying var but nothing references it.
        assert resolve(p)[0]["steps"]["s"]["t"] == "plain"

    def test_unknown_profile_raises(self, tmp_path):
        p = self._project(tmp_path)

        with pytest.raises(ProfileError, match="Unknown profile 'staging'"):
            resolve(p, "staging")

    def test_missing_profile_value_raises(self, tmp_path):
        write(
            tmp_path / bp_vars.VARS_FILENAME,
            "profiles: [prod, dev]\nvars:\n  db:\n    prod: only_prod\n",
        )
        p = dag_file(tmp_path, "dag_id: d\nsteps:\n  s:\n    blueprint: b\n    t: ${db}\n")

        with pytest.raises(IncompleteVariableError, match="no value under profile 'dev'"):
            resolve(p, "dev")

    def test_map_with_unknown_keys_is_rejected(self, tmp_path):
        write(tmp_path / bp_vars.VARS_FILENAME, "profiles: [prod, dev]\nvars:\n  cfg:\n    a: 1\n")
        p = dag_file(tmp_path, "dag_id: d\nsteps:\n  s:\n    blueprint: b\n    c: ${cfg}\n")

        with pytest.raises(InvalidVariableValueError, match="unknown: 'a'"):
            resolve(p, "prod")

    def test_typo_in_profile_key_is_rejected(self, tmp_path):
        write(
            tmp_path / bp_vars.VARS_FILENAME,
            "profiles: [prod, dev]\nvars:\n  db:\n    prod: analytics\n    dve: sandbox\n",
        )
        p = dag_file(tmp_path, "dag_id: d\nsteps:\n  s:\n    blueprint: b\n    t: ${db}\n")

        with pytest.raises(InvalidVariableValueError, match="unknown: 'dve'"):
            resolve(p, "prod")

    def test_map_without_declared_profiles_is_rejected(self, tmp_path):
        p = dag_file(
            tmp_path,
            "dag_id: d\nvars:\n  cfg:\n    a: 1\nsteps:\n  s:\n    blueprint: b\n    c: ${cfg}\n",
        )

        with pytest.raises(InvalidVariableValueError, match="no profiles are declared"):
            resolve(p)

    def test_profile_name_as_var(self, tmp_path):
        write(
            tmp_path / bp_vars.VARS_FILENAME,
            "profiles: [prod, dev]\nvars:\n  suffix:\n    prod: ''\n    dev: _dev\n",
        )
        p = dag_file(tmp_path, "dag_id: d\nsteps:\n  s:\n    blueprint: b\n    t: tbl${suffix}\n")

        assert resolve(p, "prod")[0]["steps"]["s"]["t"] == "tbl"
        assert resolve(p, "dev")[0]["steps"]["s"]["t"] == "tbl_dev"


class TestScopeChain:
    def test_dag_overrides_project(self, tmp_path):
        write(tmp_path / bp_vars.VARS_FILENAME, "vars:\n  db: shared\n")
        p = dag_file(
            tmp_path,
            "dag_id: d\nvars:\n  db: local\nsteps:\n  s:\n    blueprint: b\n    t: ${db}\n",
        )

        assert resolve(p)[0]["steps"]["s"]["t"] == "local"

    def test_nearer_directory_wins(self, tmp_path):
        write(tmp_path / bp_vars.VARS_FILENAME, "vars:\n  db: root\n  other: kept\n")
        write(tmp_path / "team" / bp_vars.VARS_FILENAME, "vars:\n  db: team\n")
        p = dag_file(
            tmp_path / "team",
            "dag_id: d\nsteps:\n  s:\n    blueprint: b\n    t: ${db}-${other}\n",
        )

        assert resolve(p, search_root=tmp_path)[0]["steps"]["s"]["t"] == "team-kept"

    def test_partial_profile_override_inherits_rest(self, tmp_path):
        write(
            tmp_path / bp_vars.VARS_FILENAME,
            "profiles: [prod, dev]\nvars:\n  db:\n    prod: shared_prod\n    dev: shared_dev\n",
        )
        p = dag_file(
            tmp_path,
            "dag_id: d\nvars:\n  db:\n    dev: local_dev\nsteps:\n  s:\n    blueprint: b\n    t: ${db}\n",
        )

        assert resolve(p, "prod")[0]["steps"]["s"]["t"] == "shared_prod"
        assert resolve(p, "dev")[0]["steps"]["s"]["t"] == "local_dev"

    def test_search_root_limits_discovery(self, tmp_path):
        write(tmp_path / bp_vars.VARS_FILENAME, "vars:\n  db: outer\n")
        inner = tmp_path / "inner"
        write(inner / bp_vars.VARS_FILENAME, "vars:\n  other: x\n")
        p = dag_file(inner, "dag_id: d\nsteps:\n  s:\n    blueprint: b\n    t: ${other}\n")

        config = yaml.safe_load(p.read_text())
        out, resolved = bp_vars.resolve(config, p, search_root=inner)

        assert out["steps"]["s"]["t"] == "x"
        assert "db" not in resolved.values


class TestSearchRoot:
    def test_defaults_to_the_dag_directory(self, tmp_path):
        """Without an explicit root the walk never climbs above the DAG."""
        write(tmp_path / bp_vars.VARS_FILENAME, "vars:\n  db: outer\n")
        nested = tmp_path / "team"
        p = dag_file(nested, "dag_id: d\nsteps:\n  s:\n    blueprint: b\n    t: x\n")

        _out, resolved = resolve(p)

        assert resolved.available == {}

    def test_climbs_to_an_explicit_root(self, tmp_path):
        write(tmp_path / bp_vars.VARS_FILENAME, "vars:\n  db: outer\n")
        nested = tmp_path / "team"
        p = dag_file(nested, "dag_id: d\nsteps:\n  s:\n    blueprint: b\n    t: ${db}\n")

        assert resolve(p, search_root=tmp_path)[0]["steps"]["s"]["t"] == "outer"

    def test_airflowignore_does_not_hide_a_vars_file(self, tmp_path):
        write(tmp_path / bp_vars.VARS_FILENAME, "vars:\n  db: root\n")
        (tmp_path / ".airflowignore").write_text("drafts\n")
        drafts = tmp_path / "drafts"
        write(drafts / bp_vars.VARS_FILENAME, "vars:\n  db: nearer\n")
        p = dag_file(drafts, "dag_id: d\nsteps:\n  s:\n    blueprint: b\n    t: ${db}\n")

        assert resolve(p, search_root=tmp_path)[0]["steps"]["s"]["t"] == "nearer"


class TestProfilesDeclaration:
    def test_profiles_may_only_be_declared_once(self, tmp_path):
        write(tmp_path / bp_vars.VARS_FILENAME, "profiles: [prod, dev]\nvars:\n  a: 1\n")
        nested = tmp_path / "team"
        write(nested / bp_vars.VARS_FILENAME, "profiles: [staging]\nvars:\n  b: 2\n")
        p = dag_file(nested, "dag_id: d\nsteps:\n  s:\n    blueprint: b\n    t: ${a}\n")

        with pytest.raises(ProfileError, match="already declared"):
            resolve(p, search_root=tmp_path)


class TestValueShape:
    def test_list_of_scalars_allowed(self, tmp_path):
        p = dag_file(
            tmp_path,
            "dag_id: d\nvars:\n  v: [a, b]\nsteps:\n  s:\n    blueprint: b\n    n: ${v}\n",
        )

        assert resolve(p)[0]["steps"]["s"]["n"] == ["a", "b"]

    def test_references_inside_lists_are_composed(self, tmp_path):
        p = dag_file(
            tmp_path,
            "dag_id: d\nvars:\n  bucket: s3://x\n  srcs:\n    - ${bucket}/a\n    - ${bucket}/b\n"
            "steps:\n  s:\n    blueprint: b\n    n: ${srcs}\n",
        )

        assert resolve(p)[0]["steps"]["s"]["n"] == ["s3://x/a", "s3://x/b"]

    def test_map_inside_a_list_is_rejected(self, tmp_path):
        p = dag_file(
            tmp_path,
            "dag_id: d\nvars:\n  v:\n    - a: 1\nsteps:\n  s:\n    blueprint: b\n    n: ${v}\n",
        )

        with pytest.raises(InvalidVariableValueError, match="maps are not allowed inside list"):
            resolve(p)

    def test_profile_map_of_lists_allowed(self, tmp_path):
        write(
            tmp_path / bp_vars.VARS_FILENAME,
            "profiles: [prod, dev]\nvars:\n  v:\n    prod: [a]\n    dev: [b, c]\n",
        )
        p = dag_file(tmp_path, "dag_id: d\nsteps:\n  s:\n    blueprint: b\n    n: ${v}\n")

        assert resolve(p, "dev")[0]["steps"]["s"]["n"] == ["b", "c"]


class TestComposition:
    def test_var_referencing_var(self, tmp_path):
        p = dag_file(
            tmp_path,
            "dag_id: d\nvars:\n  db: a\n  schema: b\n  base: ${db}.${schema}\n"
            "steps:\n  s:\n    blueprint: b\n    t: ${base}.tbl\n",
        )

        assert resolve(p)[0]["steps"]["s"]["t"] == "a.b.tbl"

    def test_chained_composition(self, tmp_path):
        p = dag_file(
            tmp_path,
            "dag_id: d\nvars:\n  a: 1\n  b: ${a}2\n  c: ${b}3\nsteps:\n  s:\n    blueprint: b\n    t: ${c}\n",
        )

        assert resolve(p)[0]["steps"]["s"]["t"] == "123"

    def test_composition_across_scopes(self, tmp_path):
        write(tmp_path / bp_vars.VARS_FILENAME, "vars:\n  db: shared\n")
        p = dag_file(
            tmp_path,
            "dag_id: d\nvars:\n  full: ${db}.local\nsteps:\n  s:\n    blueprint: b\n    t: ${full}\n",
        )

        assert resolve(p)[0]["steps"]["s"]["t"] == "shared.local"

    def test_direct_cycle_detected(self, tmp_path):
        p = dag_file(
            tmp_path,
            "dag_id: d\nvars:\n  a: ${b}\n  b: ${a}\nsteps:\n  s:\n    blueprint: b\n    t: ${a}\n",
        )

        with pytest.raises(CyclicVariableError, match="Cyclic variable reference"):
            resolve(p)

    def test_deep_acyclic_chain_is_not_reported_as_a_cycle(self, tmp_path):
        chain = "".join(f"  v{i}: ${{v{i + 1}}}\n" for i in range(60))
        p = dag_file(
            tmp_path,
            f"dag_id: d\nvars:\n{chain}  v60: end\n"
            "steps:\n  s:\n    blueprint: b\n    t: ${v0}\n",
        )

        with pytest.raises(CompositionDepthError, match="not a cycle"):
            resolve(p)

    def test_self_reference_detected(self, tmp_path):
        p = dag_file(
            tmp_path, "dag_id: d\nvars:\n  a: ${a}\nsteps:\n  s:\n    blueprint: b\n    t: ${a}\n"
        )

        with pytest.raises(CyclicVariableError):
            resolve(p)


class TestNames:
    @pytest.mark.parametrize("name", ["with.period", "1leading", "has space", "has$sign"])
    def test_invalid_names_rejected(self, tmp_path, name):
        p = dag_file(
            tmp_path,
            f"dag_id: d\nvars:\n  {name!r}: x\nsteps:\n  s:\n    blueprint: b\n",
        )

        with pytest.raises(InvalidVariableNameError):
            resolve(p)

    @pytest.mark.parametrize("name", ["snake_case", "_leading", "with-hyphen", "Mixed9"])
    def test_valid_names_accepted(self, tmp_path, name):
        p = dag_file(
            tmp_path,
            f"dag_id: d\nvars:\n  {name}: x\nsteps:\n  s:\n    blueprint: b\n    t: ${{{name}}}\n",
        )

        assert resolve(p)[0]["steps"]["s"]["t"] == "x"


class TestIntrospection:
    def test_declared_profiles(self, tmp_path):
        write(tmp_path / bp_vars.VARS_FILENAME, "profiles: [prod, dev, staging]\nvars: {}\n")
        p = dag_file(tmp_path, "dag_id: d\nsteps:\n  s:\n    blueprint: b\n")

        assert bp_vars.declared_profiles(p) == ["prod", "dev", "staging"]

    def test_sources_recorded(self, tmp_path):
        vars_file = write(tmp_path / bp_vars.VARS_FILENAME, "vars:\n  shared: x\n")
        p = dag_file(
            tmp_path,
            "dag_id: d\nvars:\n  local: y\nsteps:\n  s:\n    blueprint: b\n    t: ${local}${shared}\n",
        )

        _, resolved = resolve(p)

        assert resolved.sources["shared"] == vars_file
        assert resolved.sources["local"] == p

    def test_partial_override_reports_per_profile_source(self, tmp_path):
        vars_file = write(
            tmp_path / bp_vars.VARS_FILENAME,
            "profiles: [prod, dev]\nvars:\n  db:\n    prod: shared_prod\n    dev: shared_dev\n",
        )
        p = dag_file(
            tmp_path,
            "dag_id: d\nvars:\n  db:\n    dev: local_dev\n"
            "steps:\n  s:\n    blueprint: b\n    t: ${db}\n",
        )

        assert resolve(p, "prod")[1].sources["db"] == vars_file
        assert resolve(p, "dev")[1].sources["db"] == p

    def test_available_lists_everything_in_scope(self, tmp_path):
        write(tmp_path / bp_vars.VARS_FILENAME, "vars:\n  shared: x\n  extra: y\n")
        p = dag_file(
            tmp_path,
            "dag_id: d\nvars:\n  local: z\nsteps:\n  s:\n    blueprint: b\n    t: ${local}\n",
        )

        _out, resolved = resolve(p)

        assert set(resolved.available) == {"shared", "extra", "local"}
        assert set(resolved.values) == {"local"}

    def test_unused_variables(self, tmp_path):
        p = dag_file(
            tmp_path,
            "dag_id: d\nvars:\n  used: a\n  never: b\n  via_other: c\n  wrapper: ${via_other}\n"
            "steps:\n  s:\n    blueprint: b\n    t: ${used}${wrapper}\n",
        )

        _out, resolved = resolve(p)

        assert bp_vars.unused_variables(resolved) == ["never"]


class TestJinjaProfile:
    """The active profile is available to Jinja2, which runs before variables."""

    def test_profile_available_in_jinja(self, tmp_path):
        from blueprint.loaders import render_yaml_template

        p = dag_file(
            tmp_path,
            'dag_id: d\ndescription: "{{ profile }} run"\nsteps:\n  s:\n    blueprint: b\n',
        )

        config, _ = render_yaml_template(p, context={"profile": "prod"}, use_airflow_context=False)

        assert config["description"] == "prod run"


class TestNoVars:
    def test_config_without_vars_is_untouched(self, tmp_path):
        p = dag_file(tmp_path, "dag_id: d\nschedule: '@daily'\nsteps:\n  s:\n    blueprint: b\n")

        out, resolved = resolve(p)

        assert out == {"dag_id": "d", "schedule": "@daily", "steps": {"s": {"blueprint": "b"}}}
        assert resolved.values == {}

    def test_reference_errors_even_when_no_vars_declared(self, tmp_path):
        """`${...}` always means a variable, so the meaning never depends on
        whether some other file in the project declares one."""
        p = dag_file(
            tmp_path,
            "dag_id: d\nsteps:\n  s:\n    blueprint: b\n    bash_command: echo ${HOME}/data\n",
        )

        with pytest.raises(UndefinedVariableError, match=r"meant the literal text"):
            resolve(p)

    def test_escape_works_with_no_vars_declared(self, tmp_path):
        p = dag_file(
            tmp_path,
            "dag_id: d\nsteps:\n  s:\n    blueprint: b\n    bash_command: echo $${HOME}/data\n",
        )

        out, _ = resolve(p)

        assert out["steps"]["s"]["bash_command"] == "echo ${HOME}/data"

    def test_config_with_no_references_is_unchanged(self, tmp_path):
        p = dag_file(
            tmp_path,
            "dag_id: d\nsteps:\n  s:\n    blueprint: b\n    bash_command: echo $$ && ls\n",
        )

        out, _ = resolve(p)

        assert out["steps"]["s"]["bash_command"] == "echo $$ && ls"

    def test_undefined_reference_errors_once_vars_are_declared(self, tmp_path):
        p = dag_file(
            tmp_path, "dag_id: d\nvars:\n  a: x\nsteps:\n  s:\n    blueprint: b\n    t: ${nope}\n"
        )

        with pytest.raises(UndefinedVariableError):
            resolve(p)


class TestEscaping:
    def test_double_dollar_escapes_a_reference(self, tmp_path):
        p = dag_file(
            tmp_path,
            "dag_id: d\nvars:\n  d: /opt\nsteps:\n  s:\n    blueprint: b\n"
            "    bash_command: echo $${HOME}/${d}\n",
        )

        out, _ = resolve(p)

        assert out["steps"]["s"]["bash_command"] == "echo ${HOME}//opt"

    def test_bare_double_dollar_is_left_alone(self, tmp_path):
        """`$$` is the shell PID; only `$${` escapes a reference."""
        p = dag_file(
            tmp_path,
            "dag_id: d\nvars:\n  a: x\nsteps:\n  s:\n    blueprint: b\n"
            '    bash_command: "echo $$ && echo ${a}"\n',
        )

        out, _ = resolve(p)

        assert out["steps"]["s"]["bash_command"] == "echo $$ && echo x"
