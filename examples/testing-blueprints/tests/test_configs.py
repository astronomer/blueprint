"""Config tests: the cheapest and most valuable tests you can write.

No Airflow, no DAG context -- these are plain Pydantic models, so they run in
milliseconds and pin down the contract with DAG authors.
"""

import pytest
from blueprints import ExtractConfig, LoadConfig
from pydantic import ValidationError


class TestExtractConfig:
    def test_defaults(self):
        config = ExtractConfig(source="crm", datasets=["customers"])
        assert config.validate_first is True

    def test_rejects_unknown_field(self):
        # extra="forbid" is what turns a typo into an error rather than a
        # silently ignored key. Worth a test so nobody removes it casually.
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            ExtractConfig(source="crm", datasets=["customers"], dataset="typo")

    def test_requires_at_least_one_dataset(self):
        with pytest.raises(ValidationError, match="at least 1 item"):
            ExtractConfig(source="crm", datasets=[])


class TestLoadConfig:
    def test_upsert_requires_dedupe_keys(self):
        with pytest.raises(ValidationError, match="requires at least one entry"):
            LoadConfig(target="warehouse.customers", mode="upsert")

    def test_upsert_with_keys_is_valid(self):
        config = LoadConfig(
            target="warehouse.customers", mode="upsert", dedupe_keys=["customer_id"]
        )
        assert config.dedupe_keys == ["customer_id"]

    def test_rejects_unknown_mode(self):
        with pytest.raises(ValidationError):
            LoadConfig(target="warehouse.customers", mode="merge")
