"""chaosdata: Python SDK for chaos-data, chaos testing for data pipelines.

Provides frozen dataclasses mirroring the Go type system and a subprocess
client for driving the chaos-data CLI binary.
"""

from __future__ import annotations

from chaosdata.client import ChaosDataClient, ChaosDataError
from chaosdata.scenario import (
    Corrupt,
    Delay,
    Drop,
    Duplicate,
    Empty,
    MultiDay,
    Partial,
    Scenario,
    SchemaDrift,
    StaleReplay,
)
from chaosdata.types import (
    ChaosEvent,
    ExperimentResult,
    ExperimentStats,
    MutationRecord,
    ObjectFilter,
    Safety,
    Target,
)

__all__ = [
    # Types
    "ChaosEvent",
    "ExperimentResult",
    "ExperimentStats",
    "MutationRecord",
    "ObjectFilter",
    "Safety",
    "Target",
    # Scenario + mutations
    "Corrupt",
    "Delay",
    "Drop",
    "Duplicate",
    "Empty",
    "MultiDay",
    "Partial",
    "Scenario",
    "SchemaDrift",
    "StaleReplay",
    # Client
    "ChaosDataClient",
    "ChaosDataError",
    # Convenience
    "catalog",
    "run",
]

_default_client: ChaosDataClient | None = None


def _get_client() -> ChaosDataClient:
    """Return the module-level default client, creating it on first use."""
    global _default_client  # noqa: PLW0603
    if _default_client is None:
        _default_client = ChaosDataClient()
    return _default_client


def run(scenario: str, input_dir: str, output_dir: str) -> str:
    """Run a chaos scenario using the default client.

    Args:
        scenario: Scenario name (from catalog) or path to a YAML file.
        input_dir: Input staging directory.
        output_dir: Output directory.

    Returns:
        Raw text output from the CLI.
    """
    return _get_client().run(scenario, input_dir, output_dir)


def catalog() -> str:
    """List available scenarios using the default client.

    Returns:
        Raw text output listing built-in scenarios.
    """
    return _get_client().catalog()
