"""Tests for shared blueprint utilities."""

from pathlib import Path

from blueprint.utils import display_path


class TestDisplayPath:
    """Test the cwd-relative path renderer used for blueprint locations."""

    def test_path_under_cwd_is_relative(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        nested = tmp_path / "dags" / "bp.py"
        nested.parent.mkdir()
        nested.write_text("")
        assert display_path(nested) == str(Path("dags") / "bp.py")

    def test_path_outside_cwd_is_absolute(self, tmp_path, monkeypatch):
        cwd = tmp_path / "project"
        cwd.mkdir()
        monkeypatch.chdir(cwd)
        sibling = (tmp_path / "elsewhere" / "bp.py").resolve()
        assert display_path(sibling) == str(sibling)

    def test_explicit_base_overrides_cwd(self, tmp_path, monkeypatch):
        other = tmp_path / "other"
        other.mkdir()
        monkeypatch.chdir(other)
        nested = tmp_path / "dags" / "bp.py"
        assert display_path(nested, base=tmp_path) == str(Path("dags") / "bp.py")
