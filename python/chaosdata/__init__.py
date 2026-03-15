"""chaosdata: Python SDK for chaos-data, chaos testing for data pipelines.

Provides frozen dataclasses mirroring the Go type system and a subprocess
client for driving the chaos-data CLI binary.
"""

from __future__ import annotations

import threading

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
    ExperimentStatus,
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
    "ExperimentStatus",
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
    "inject",
    "replay",
    "run",
    "start_experiment",
    "status",
]

_default_client: ChaosDataClient | None = None
_default_client_lock = threading.Lock()


def _get_client() -> ChaosDataClient:
    """Return the module-level default client, creating it on first use."""
    global _default_client  # noqa: PLW0603
    with _default_client_lock:
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


def start_experiment(
    scenarios: list[str],
    input_dir: str,
    output_dir: str,
    duration: str = "5m",
    mode: str = "deterministic",
    dry_run: bool = False,
) -> dict | list | None:
    """Start a chaos experiment using the default client."""
    return _get_client().start_experiment(
        scenarios, input_dir, output_dir, duration, mode, dry_run
    )


def replay(manifest_path: str) -> dict | list | None:
    """Replay mutations from a JSONL manifest file."""
    return _get_client().replay(manifest_path)


def status() -> dict | list | None:
    """Check engine status using the default client."""
    return _get_client().status()


def inject(scenario: str, target: str, input_dir: str, output_dir: str) -> dict | list | None:
    """Inject a single mutation using the default client."""
    return _get_client().inject(scenario, target, input_dir, output_dir)
