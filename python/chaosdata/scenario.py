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


# Union of all mutation parameter types.
MutationType: TypeAlias = (
    Delay | Corrupt | Drop | Duplicate | SchemaDrift
    | StaleReplay | MultiDay | Partial | Empty
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
