"""Core data types mirroring the Go chaos-data type system.

All types are frozen dataclasses to enforce immutability.
"""

from __future__ import annotations

import types as _types
from collections.abc import Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True)
class ObjectFilter:
    """Narrows which objects within a target are affected.

    Both prefix and match must be satisfied when set (logical AND).
    An empty filter matches every object.
    """

    prefix: str = ""
    match: str = ""


@dataclass(frozen=True)
class Target:
    """Identifies a chaos injection target within an architectural layer.

    Valid layers: "data", "state", "orchestrator".
    """

    layer: str = "data"
    transport: str = ""
    filter: ObjectFilter = field(default_factory=ObjectFilter)

    def __post_init__(self) -> None:
        valid_layers = {"data", "state", "orchestrator"}
        if self.layer and self.layer not in valid_layers:
            raise ValueError(
                f"invalid layer {self.layer!r}: must be one of {sorted(valid_layers)}"
            )


@dataclass(frozen=True)
class Safety:
    """Safety constraints for a chaos scenario."""

    max_affected_pct: int = 25
    cooldown: str = "5m"
    sla_aware: bool = True

    def __post_init__(self) -> None:
        if self.max_affected_pct < 0 or self.max_affected_pct > 100:
            raise ValueError(
                f"max_affected_pct must be 0-100, got {self.max_affected_pct}"
            )


@dataclass(frozen=True)
class MutationRecord:
    """Tracks the result of applying a single mutation to an object."""

    object_key: str = ""
    mutation: str = ""
    params: Mapping[str, str] = field(default_factory=dict)
    applied: bool = False
    error: str = ""
    timestamp: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.params, _types.MappingProxyType):
            object.__setattr__(
                self, "params", _types.MappingProxyType(dict(self.params))
            )


@dataclass(frozen=True)
class ExperimentResult:
    """Aggregated result of a chaos experiment run."""

    experiment_id: str = ""
    records: tuple[MutationRecord, ...] = field(default_factory=tuple)
    state: str = ""


@dataclass(frozen=True)
class ChaosEvent:
    """A single chaos injection event within an experiment."""

    id: str = ""
    experiment_id: str = ""
    scenario: str = ""
    category: str = ""
    severity: str = ""
    target: str = ""
    mutation: str = ""
    params: Mapping[str, str] = field(default_factory=dict)
    timestamp: str = ""
    mode: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.params, _types.MappingProxyType):
            object.__setattr__(
                self, "params", _types.MappingProxyType(dict(self.params))
            )


@dataclass(frozen=True)
class ExperimentStats:
    """Aggregate statistics for a chaos experiment run."""

    experiment_id: str = ""
    total_events: int = 0
    affected_targets: int = 0
    affected_pipelines: int = 0
    affected_pct: float = 0.0
    start_time: str = ""
    end_time: str = ""
