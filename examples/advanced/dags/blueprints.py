"""Blueprint definitions for a space mission operations pipeline.

Demonstrates versioning, runtime params, nested configs, validators,
explicit name/version, ConfigDict, Field constraints, and Literal types.
"""

from typing import Literal

from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from blueprint import (
    BaseModel,
    Blueprint,
    ConfigDict,
    Field,
    TaskOrGroup,
    field_validator,
    model_validator,
)


# --- Scan v1: single-band sensor scan (returns a single operator) ---


class ScanConfig(BaseModel):
    target: str
    band: str = "radio"


class Scan(Blueprint[ScanConfig]):
    """Point a sensor at a celestial target on a single frequency band."""

    def render(self, config: ScanConfig) -> TaskOrGroup:
        return BashOperator(
            task_id=self.step_id,
            bash_command=f"echo 'Scanning {config.target} on {config.band}'",
        )


# --- Scan v2: multi-band with nested config (returns a TaskGroup) ---


class FrequencyBand(BaseModel):
    name: str
    ghz: float = Field(ge=0.01, le=300.0)


class ScanV2Config(BaseModel):
    target: str
    bands: list[FrequencyBand]


class ScanV2(Blueprint[ScanV2Config]):
    """Multi-band scan with structured frequency configuration."""

    def render(self, config: ScanV2Config) -> TaskOrGroup:
        with TaskGroup(group_id=self.step_id) as group:
            for band in config.bands:
                BashOperator(
                    task_id=f"scan_{band.name}",
                    bash_command=(
                        f"echo 'Scanning {config.target} on {band.name} @ {band.ghz} GHz'"
                    ),
                )
        return group


# --- Transmit: data downlink with runtime parameter support ---


class TransmitConfig(BaseModel):
    destination: str
    protocol: Literal["tcp", "udp"] = "tcp"
    compression: bool = True


class Transmit(Blueprint[TransmitConfig]):
    """Transmit data to a ground station. Supports runtime parameter overrides."""

    supports_params = True

    def render(self, config: TransmitConfig) -> TaskOrGroup:
        with TaskGroup(group_id=self.step_id) as group:
            downlink = BashOperator(
                task_id="downlink",
                bash_command=(
                    f"echo 'Transmitting to {self.param('destination')} "
                    f"via {self.param('protocol')}'"
                ),
            )

            @task(task_id="log_transmission")
            def log_transmission(**context):
                resolved = self.resolve_config(config, context)
                print(
                    f"Logged: dest={resolved.destination}, "
                    f"compression={resolved.compression}"
                )

            downlink >> log_transmission()
        return group


# --- Analyze: signal processing with strict validation ---


VALID_ALGORITHMS = {"fft", "wavelet", "matched_filter", "bayesian", "correlation"}


class AnalyzeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    algorithms: list[str]
    confidence_threshold: float = Field(default=0.95, ge=0.0, le=1.0)

    @field_validator("algorithms")
    @classmethod
    def check_algorithms(cls, v: list[str]) -> list[str]:
        for algo in v:
            if algo not in VALID_ALGORITHMS:
                msg = f"Unknown algorithm '{algo}'. Valid: {sorted(VALID_ALGORITHMS)}"
                raise ValueError(msg)
        return v


class Analyze(Blueprint[AnalyzeConfig]):
    """Run signal analysis algorithms sequentially on scan data."""

    def render(self, config: AnalyzeConfig) -> TaskOrGroup:
        with TaskGroup(group_id=self.step_id) as group:
            prev = None
            for algo in config.algorithms:
                t = BashOperator(
                    task_id=algo,
                    bash_command=(
                        f"echo 'Running {algo} (threshold={config.confidence_threshold})'"
                    ),
                )
                if prev:
                    prev >> t
                prev = t
        return group


# --- Orbit: navigation with explicit name/version and model validator ---


class OrbitalParams(BaseModel):
    altitude_km: float = Field(ge=160, le=36000)
    inclination_deg: float = Field(ge=0, le=180)


class OrbitConfig(BaseModel):
    satellite_id: str = Field(pattern=r"^SAT-\d{3,}$")
    params: OrbitalParams
    maneuver: Literal["raise", "lower", "adjust"] = "adjust"

    @model_validator(mode="after")
    def validate_maneuver(self) -> "OrbitConfig":
        if self.maneuver == "raise" and self.params.altitude_km > 35000:
            msg = "Cannot raise orbit above 35,000 km"
            raise ValueError(msg)
        if self.maneuver == "lower" and self.params.altitude_km < 200:
            msg = "Cannot lower orbit below 200 km"
            raise ValueError(msg)
        return self


class Orbit(Blueprint[OrbitConfig]):
    """Calculate and execute orbital maneuvers."""

    name = "orbit"
    version = 1

    def render(self, config: OrbitConfig) -> TaskOrGroup:
        with TaskGroup(group_id=self.step_id) as group:
            calculate = BashOperator(
                task_id="calculate",
                bash_command=(
                    f"echo 'Calculating {config.maneuver} to "
                    f"{config.params.altitude_km}km at "
                    f"{config.params.inclination_deg} deg for {config.satellite_id}'"
                ),
            )
            execute = BashOperator(
                task_id="execute",
                bash_command=f"echo 'Executing maneuver for {config.satellite_id}'",
            )
            calculate >> execute
        return group
