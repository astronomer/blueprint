"""Test-only blueprint package, installed and discovered via its entry point.

Exists solely so the integration suite can prove that BlueprintRegistry discovers
blueprints from an installed package end to end, without depending on anything
under ``examples/``.
"""
