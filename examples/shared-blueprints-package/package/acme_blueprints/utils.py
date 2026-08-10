"""Helper code that is not a blueprint.

This module lives outside the entry point target (`acme_blueprints.blueprints`),
so the registry never imports it while scanning. Blueprints may still import
from it -- scoping the entry point limits discovery, not ordinary imports.
"""

SLA_BY_TIER = {"gold": 60, "silver": 240, "bronze": 1440}


def sla_minutes(tier: str) -> int:
    """Minutes within which a tier is expected to complete."""
    return SLA_BY_TIER.get(tier, SLA_BY_TIER["bronze"])
