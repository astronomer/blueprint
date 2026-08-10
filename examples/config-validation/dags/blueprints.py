"""Two blueprints with deliberately different strictness.

Extract keeps its config permissive. Load locks its config down: unknown fields
are rejected, values are range- and pattern-checked, and two custom validators
enforce rules the type system cannot express.
"""

from typing import Literal

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import TaskGroup

# PrivateAttr is not re-exported by blueprint; import anything else you need
# straight from pydantic.
from pydantic import PrivateAttr

from blueprint import (
    BaseModel,
    Blueprint,
    ConfigDict,
    Field,
    TaskOrGroup,
    field_validator,
    model_validator,
)


class ExtractConfig(BaseModel):
    """A permissive config. Every field is optional bar one, nothing is checked
    beyond its type, and unknown fields are ignored rather than rejected."""

    source: str = Field(description="Source system to read from")
    since: str | None = Field(default=None, description="Only read rows newer than this")


class Extract(Blueprint[ExtractConfig]):
    """Read from a source system."""

    def render(self, config: ExtractConfig) -> TaskOrGroup:
        window = f" since {config.since}" if config.since else ""
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Extracting {config.source}{window}'",
        )


class Warehouse(BaseModel):
    """Nested models validate too, and nest in the generated JSON schema."""

    database: str = Field(description="Target database")
    schema_name: str = Field(
        pattern=r"^[a-z][a-z0-9_]*$",
        description="Target schema; lowercase letters, digits and underscores only",
    )
    table: str = Field(pattern=r"^[a-z][a-z0-9_]*$", description="Target table")


class LoadConfig(BaseModel):
    """A strict config. See the README for what each mechanism buys you."""

    # Reject fields the model does not define, rather than silently dropping
    # them. Without this, `batchsize: 5000` is ignored and the default is used.
    model_config = ConfigDict(extra="forbid")

    destination: Warehouse
    mode: Literal["append", "overwrite", "upsert"] = Field(
        default="append", description="How rows are written"
    )
    batch_size: int = Field(
        default=1000, ge=1, le=100_000, description="Rows per write batch"
    )
    dedupe_keys: list[str] = Field(
        default_factory=list, description="Columns forming the natural key, for upserts"
    )

    # Internal tuning knob. A private attribute is not a model field: it never
    # appears in the JSON schema and cannot be set from YAML.
    _shard_factor: int = PrivateAttr(default=4)

    @property
    def shards(self) -> int:
        """Derived values belong on the config, not in render()."""
        return max(1, self.batch_size // 250) * self._shard_factor

    @field_validator("dedupe_keys")
    @classmethod
    def keys_must_be_unique(cls, value: list[str]) -> list[str]:
        """Field validators see one field, after its type has been checked."""
        if len(set(value)) != len(value):
            msg = f"dedupe_keys contains duplicates: {value}"
            raise ValueError(msg)
        return value

    @model_validator(mode="after")
    def upsert_requires_keys(self) -> "LoadConfig":
        """Model validators see the whole config, so they can relate fields."""
        if self.mode == "upsert" and not self.dedupe_keys:
            msg = "mode 'upsert' requires at least one entry in dedupe_keys"
            raise ValueError(msg)
        return self


class Load(Blueprint[LoadConfig]):
    """Write rows into a warehouse table."""

    def render(self, config: LoadConfig) -> TaskOrGroup:
        target = (
            f"{config.destination.database}."
            f"{config.destination.schema_name}."
            f"{config.destination.table}"
        )
        with TaskGroup(group_id=self.step_id) as group:
            write = BashOperator(
                task_id="write",
                bash_command=(
                    f"echo 'Writing to {target} mode={config.mode} "
                    f"batch={config.batch_size} shards={config.shards}'"
                ),
            )
            if config.mode == "upsert":
                # config.dedupe_keys is guaranteed non-empty here -- the model
                # validator already rejected the alternative.
                keys = ",".join(config.dedupe_keys)
                dedupe = BashOperator(
                    task_id="dedupe",
                    bash_command=f"echo 'Deduplicating {target} on {keys}'",
                )
                write >> dedupe
        return group
