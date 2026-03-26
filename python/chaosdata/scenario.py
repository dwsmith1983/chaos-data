"""Scenario configuration dataclasses for chaos-data mutations.

Each mutation type has a corresponding frozen dataclass with sensible defaults
matching the Go implementation's parameter conventions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TypeAlias

from chaosdata.types import Safety, Target


# --- Mutation parameter dataclasses ---


@dataclass(frozen=True)
class Delay:
    """Parameters for the delay mutation: simulates late-arriving data."""

    duration: str = "30m"
    jitter: str = "5m"
    release: bool = True

    def to_params(self) -> dict[str, str]:
        return {
            "duration": self.duration,
            "jitter": self.jitter,
            "release": str(self.release).lower(),
        }


@dataclass(frozen=True)
class Corrupt:
    """Parameters for the corrupt mutation: modifies JSONL records."""

    affected_pct: int = 10
    corruption_type: str = "null"

    def __post_init__(self) -> None:
        if self.affected_pct < 0 or self.affected_pct > 100:
            raise ValueError(
                f"affected_pct must be 0-100, got {self.affected_pct}"
            )

    def to_params(self) -> dict[str, str]:
        return {
            "affected_pct": str(self.affected_pct),
            "corruption_type": self.corruption_type,
        }


@dataclass(frozen=True)
class Drop:
    """Parameters for the drop mutation: silently drops data objects."""

    scope: str = "object"

    def to_params(self) -> dict[str, str]:
        return {"scope": self.scope}


@dataclass(frozen=True)
class Duplicate:
    """Parameters for the duplicate mutation: writes data twice."""

    dup_pct: int = 100
    exact: bool = True

    def to_params(self) -> dict[str, str]:
        return {
            "dup_pct": str(self.dup_pct),
            "exact": str(self.exact).lower(),
        }


@dataclass(frozen=True)
class SchemaDrift:
    """Parameters for the schema-drift mutation: modifies JSONL schema."""

    add_columns: str = ""
    remove_columns: str = ""
    change_types: str = ""

    def to_params(self) -> dict[str, str]:
        params: dict[str, str] = {}
        if self.add_columns:
            params["add_columns"] = self.add_columns
        if self.remove_columns:
            params["remove_columns"] = self.remove_columns
        if self.change_types:
            params["change_types"] = self.change_types
        return params


@dataclass(frozen=True)
class StaleReplay:
    """Parameters for the stale-replay mutation: replays data from a past date."""

    replay_date: str = ""
    prefix: str = ""

    def to_params(self) -> dict[str, str]:
        params: dict[str, str] = {"replay_date": self.replay_date}
        if self.prefix:
            params["prefix"] = self.prefix
        return params


@dataclass(frozen=True)
class MultiDay:
    """Parameters for the multi-day mutation: data delivered across multiple days."""

    days: str = ""
    prefix: str = ""

    def to_params(self) -> dict[str, str]:
        params: dict[str, str] = {"days": self.days}
        if self.prefix:
            params["prefix"] = self.prefix
        return params


@dataclass(frozen=True)
class Partial:
    """Parameters for the partial mutation: truncated data delivery."""

    delivery_pct: int = 50

    def __post_init__(self) -> None:
        if self.delivery_pct < 0 or self.delivery_pct > 100:
            raise ValueError(
                f"delivery_pct must be 0-100, got {self.delivery_pct}"
            )

    def to_params(self) -> dict[str, str]:
        return {"delivery_pct": str(self.delivery_pct)}


@dataclass(frozen=True)
class Empty:
    """Parameters for the empty mutation: replaces content with empty data."""

    preserve_header: bool = False

    def to_params(self) -> dict[str, str]:
        return {"preserve_header": str(self.preserve_header).lower()}


@dataclass(frozen=True)
class CascadeDelay:
    """Parameters for the cascade-delay mutation: simulates cascading upstream delay."""

    upstream_pipeline: str = ""
    delay_duration: str = ""
    sensor_key: str = "arrival"

    def to_params(self) -> dict[str, str]:
        return {
            "upstream_pipeline": self.upstream_pipeline,
            "delay_duration": self.delay_duration,
            "sensor_key": self.sensor_key,
        }


@dataclass(frozen=True)
class OutOfOrder:
    """Parameters for the out-of-order mutation: delivers data in wrong partition order."""

    delay_older_by: str = ""
    partition_field: str = ""
    older_value: str = ""
    newer_value: str = ""

    def to_params(self) -> dict[str, str]:
        return {
            "delay_older_by": self.delay_older_by,
            "partition_field": self.partition_field,
            "older_value": self.older_value,
            "newer_value": self.newer_value,
        }


@dataclass(frozen=True)
class StreamingLag:
    """Parameters for the streaming-lag mutation: simulates consumer group lag."""

    lag_duration: str = ""
    consumer_group: str = ""

    def to_params(self) -> dict[str, str]:
        return {
            "lag_duration": self.lag_duration,
            "consumer_group": self.consumer_group,
        }


@dataclass(frozen=True)
class PostRunDrift:
    """Parameters for the post-run-drift mutation: late-arriving partition data."""

    partition_key: str = ""
    partition_value: str = ""
    drift_delay: str = ""
    late_pct: int = 20

    def __post_init__(self) -> None:
        if self.late_pct < 1 or self.late_pct > 100:
            raise ValueError(
                f"late_pct must be 1-100, got {self.late_pct}"
            )

    def to_params(self) -> dict[str, str]:
        return {
            "partition_key": self.partition_key,
            "partition_value": self.partition_value,
            "drift_delay": self.drift_delay,
            "late_pct": str(self.late_pct),
        }


@dataclass(frozen=True)
class SlowWrite:
    """Parameters for the slow-write mutation: adds latency to write operations."""

    latency: str = ""
    jitter: str = ""

    def to_params(self) -> dict[str, str]:
        return {
            "latency": self.latency,
            "jitter": self.jitter,
        }


@dataclass(frozen=True)
class RollingDegradation:
    """Parameters for the rolling-degradation mutation: gradual quality degradation."""

    start_pct: int = 0
    end_pct: int = 100
    ramp_duration: str = ""

    def __post_init__(self) -> None:
        if self.start_pct < 0 or self.start_pct > 100:
            raise ValueError(
                f"start_pct must be 0-100, got {self.start_pct}"
            )
        if self.end_pct < 0 or self.end_pct > 100:
            raise ValueError(
                f"end_pct must be 0-100, got {self.end_pct}"
            )

    def to_params(self) -> dict[str, str]:
        return {
            "start_pct": str(self.start_pct),
            "end_pct": str(self.end_pct),
            "ramp_duration": self.ramp_duration,
        }


@dataclass(frozen=True)
class StaleSensor:
    """Parameters for the stale-sensor mutation: marks a sensor as stale."""

    sensor_key: str = ""
    pipeline: str = ""
    last_update_age: str = ""

    def to_params(self) -> dict[str, str]:
        return {
            "sensor_key": self.sensor_key,
            "pipeline": self.pipeline,
            "last_update_age": self.last_update_age,
        }


@dataclass(frozen=True)
class PhantomSensor:
    """Parameters for the phantom-sensor mutation: writes a fake sensor record."""

    pipeline: str = ""
    sensor_key: str = ""
    status: str = "ready"

    def to_params(self) -> dict[str, str]:
        return {
            "pipeline": self.pipeline,
            "sensor_key": self.sensor_key,
            "status": self.status,
        }


@dataclass(frozen=True)
class SensorFlapping:
    """Parameters for the sensor-flapping mutation: rapidly alternates sensor status."""

    sensor_key: str = ""
    pipeline: str = ""
    flap_count: int = 5
    start_status: str = "ready"
    alternate_status: str = "pending"

    def __post_init__(self) -> None:
        if self.flap_count < 1:
            raise ValueError(
                f"flap_count must be >= 1, got {self.flap_count}"
            )

    def to_params(self) -> dict[str, str]:
        return {
            "sensor_key": self.sensor_key,
            "pipeline": self.pipeline,
            "flap_count": str(self.flap_count),
            "start_status": self.start_status,
            "alternate_status": self.alternate_status,
        }


@dataclass(frozen=True)
class SplitSensor:
    """Parameters for the split-sensor mutation: writes conflicting sensor values."""

    sensor_key: str = ""
    pipeline: str = ""
    conflicting_values: str = "ready,stale"

    def to_params(self) -> dict[str, str]:
        return {
            "sensor_key": self.sensor_key,
            "pipeline": self.pipeline,
            "conflicting_values": self.conflicting_values,
        }


@dataclass(frozen=True)
class TimestampForgery:
    """Parameters for the timestamp-forgery mutation: falsifies sensor timestamps."""

    sensor_key: str = ""
    pipeline: str = ""
    last_updated_offset: str = ""
    payload_timestamp_offset: str = ""

    def to_params(self) -> dict[str, str]:
        return {
            "sensor_key": self.sensor_key,
            "pipeline": self.pipeline,
            "last_updated_offset": self.last_updated_offset,
            "payload_timestamp_offset": self.payload_timestamp_offset,
        }


@dataclass(frozen=True)
class FalseSuccess:
    """Parameters for the false-success mutation: reports success for failed jobs."""

    pipeline: str = ""
    schedule: str = ""
    date: str = ""
    job_type: str = "glue"
    missing_output: str = ""

    def to_params(self) -> dict[str, str]:
        return {
            "pipeline": self.pipeline,
            "schedule": self.schedule,
            "date": self.date,
            "job_type": self.job_type,
            "missing_output": self.missing_output,
        }


@dataclass(frozen=True)
class JobKill:
    """Parameters for the job-kill mutation: terminates a pipeline job mid-execution."""

    pipeline: str = ""
    schedule: str = ""
    date: str = ""
    kill_after_pct: int = 50
    job_type: str = "glue"

    def __post_init__(self) -> None:
        if self.kill_after_pct < 0 or self.kill_after_pct > 100:
            raise ValueError(
                f"kill_after_pct must be 0-100, got {self.kill_after_pct}"
            )

    def to_params(self) -> dict[str, str]:
        return {
            "pipeline": self.pipeline,
            "schedule": self.schedule,
            "date": self.date,
            "kill_after_pct": str(self.kill_after_pct),
            "job_type": self.job_type,
        }


@dataclass(frozen=True)
class PhantomTrigger:
    """Parameters for the phantom-trigger mutation: creates a fake trigger record."""

    pipeline: str = ""
    schedule: str = ""
    date: str = ""
    trigger_type: str = "scheduled"

    def to_params(self) -> dict[str, str]:
        return {
            "pipeline": self.pipeline,
            "schedule": self.schedule,
            "date": self.date,
            "trigger_type": self.trigger_type,
        }


@dataclass(frozen=True)
class TriggerTimeout:
    """Parameters for the trigger-timeout mutation: simulates a trigger that times out."""

    pipeline: str = ""
    schedule: str = ""
    date: str = ""
    timeout_duration: str = "30m"

    def to_params(self) -> dict[str, str]:
        return {
            "pipeline": self.pipeline,
            "schedule": self.schedule,
            "date": self.date,
            "timeout_duration": self.timeout_duration,
        }


# Union of all mutation parameter types.
MutationType: TypeAlias = (
    Delay | Corrupt | Drop | Duplicate | SchemaDrift
    | StaleReplay | MultiDay | Partial | Empty
    | CascadeDelay | OutOfOrder | StreamingLag | PostRunDrift
    | SlowWrite | RollingDegradation | StaleSensor | PhantomSensor
    | SensorFlapping | SplitSensor | TimestampForgery | FalseSuccess
    | JobKill | PhantomTrigger | TriggerTimeout
)

# Map from mutation dataclass to the Go mutation type string.
_MUTATION_TYPE_NAMES: dict[type, str] = {
    Delay: "delay",
    Corrupt: "corrupt",
    Drop: "drop",
    Duplicate: "duplicate",
    SchemaDrift: "schema-drift",
    StaleReplay: "stale-replay",
    MultiDay: "multi-day",
    Partial: "partial",
    Empty: "empty",
    CascadeDelay: "cascade-delay",
    OutOfOrder: "out-of-order",
    StreamingLag: "streaming-lag",
    PostRunDrift: "post-run-drift",
    SlowWrite: "slow-write",
    RollingDegradation: "rolling-degradation",
    StaleSensor: "stale-sensor",
    PhantomSensor: "phantom-sensor",
    SensorFlapping: "sensor-flapping",
    SplitSensor: "split-sensor",
    TimestampForgery: "timestamp-forgery",
    FalseSuccess: "false-success",
    JobKill: "job-kill",
    PhantomTrigger: "phantom-trigger",
    TriggerTimeout: "trigger-timeout",
}

VALID_CATEGORIES = frozenset({
    "data-arrival",
    "data-quality",
    "state-consistency",
    "infrastructure",
    "orchestrator",
    "compound",
})

VALID_SEVERITIES = frozenset({"low", "moderate", "severe", "critical"})


@dataclass(frozen=True)
class Scenario:
    """A complete chaos scenario configuration.

    Mirrors the Go scenario.Scenario struct with Python-idiomatic defaults.
    """

    name: str = ""
    target: Target = field(default_factory=Target)
    mutation: MutationType = field(default_factory=Delay)
    safety: Safety = field(default_factory=Safety)
    description: str = ""
    category: str = "data-arrival"
    severity: str = "low"
    probability: float = 0.3

    def __post_init__(self) -> None:
        if self.category and self.category not in VALID_CATEGORIES:
            raise ValueError(
                f"invalid category {self.category!r}: "
                f"must be one of {sorted(VALID_CATEGORIES)}"
            )
        if self.severity and self.severity not in VALID_SEVERITIES:
            raise ValueError(
                f"invalid severity {self.severity!r}: "
                f"must be one of {sorted(VALID_SEVERITIES)}"
            )
        if self.probability < 0.0 or self.probability > 1.0:
            raise ValueError(
                f"probability must be in [0.0, 1.0], got {self.probability}"
            )

    @property
    def mutation_type(self) -> str:
        """Return the Go mutation type string for the configured mutation."""
        return _MUTATION_TYPE_NAMES.get(type(self.mutation), "unknown")
