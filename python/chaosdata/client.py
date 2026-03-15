"""Subprocess client for the chaos-data Go CLI binary.

Wraps the CLI via subprocess with JSON stdin/stdout communication.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path


def _check_null_bytes(value: str, name: str) -> None:
    """Raise ValueError if *value* contains embedded null bytes."""
    if "\x00" in value:
        raise ValueError(f"{name} must not contain null bytes")


class ChaosDataError(Exception):
    """Raised when the chaos-data binary returns a non-zero exit code."""

    def __init__(self, returncode: int, stderr: str) -> None:
        self.returncode = returncode
        self.stderr = stderr
        super().__init__(f"chaos-data exited with code {returncode}: {stderr}")


class ChaosDataClient:
    """Client that wraps the chaos-data CLI binary via subprocess."""

    def __init__(
        self,
        binary_path: str = "chaos-data",
        timeout: int = 300,
    ) -> None:
        _check_null_bytes(binary_path, "binary_path")

        # If the caller supplied something that looks like a filesystem path
        # (contains a separator or starts with "."), resolve and verify it.
        if os.sep in binary_path or binary_path.startswith("."):
            resolved = Path(binary_path).resolve()
            if not resolved.exists():
                raise FileNotFoundError(
                    f"binary_path does not exist: {resolved}"
                )
            binary_path = str(resolved)

        self._binary_path = binary_path
        self._timeout = timeout

    @property
    def binary_path(self) -> str:
        return self._binary_path

    @property
    def timeout(self) -> int:
        return self._timeout

    def catalog(self) -> str:
        """List available scenarios from the built-in catalog.

        Returns the raw text output from the CLI. The CLI currently outputs
        a tab-separated table (not JSON), so we return it as-is.
        """
        return self._run(["catalog"])

    def run(
        self,
        scenario: str,
        input_dir: str,
        output_dir: str,
    ) -> str:
        """Run a chaos scenario against a local directory.

        Args:
            scenario: Scenario name (from catalog) or path to a YAML file.
            input_dir: Input staging directory.
            output_dir: Output directory.

        Returns:
            Raw text output from the CLI.
        """
        _check_null_bytes(input_dir, "input_dir")
        _check_null_bytes(output_dir, "output_dir")
        return self._run([
            "run",
            "--scenario", scenario,
            "--input", input_dir,
            "--output", output_dir,
        ])

    def _api_call(
        self, action: str, params: dict[str, str] | None = None
    ) -> dict | list | None:
        """Execute a JSON API call to the chaos-data binary.

        Args:
            action: The API action (e.g., "catalog", "run").
            params: Optional parameters for the action.

        Returns:
            The parsed response data on success.

        Raises:
            ChaosDataError: If the API returns success=false or non-zero exit.
        """
        request = json.dumps({"action": action, "params": params or {}})
        proc = subprocess.run(
            [self._binary_path, "api"],
            input=request,
            capture_output=True,
            text=True,
            timeout=self._timeout,
        )
        if proc.returncode != 0:
            raise ChaosDataError(proc.returncode, proc.stderr.strip())

        try:
            response = json.loads(proc.stdout)
        except json.JSONDecodeError as exc:
            raise ChaosDataError(
                1, f"invalid JSON from binary: {exc}"
            ) from exc

        if not isinstance(response, dict):
            raise ChaosDataError(
                1, f"unexpected API response type: {type(response).__name__}"
            )

        if not response.get("success"):
            raise ChaosDataError(1, response.get("error", "unknown error"))

        return response.get("data")

    def start_experiment(
        self,
        scenarios: list[str],
        input_dir: str,
        output_dir: str,
        duration: str = "5m",
        mode: str = "deterministic",
        dry_run: bool = False,
    ) -> dict | list | None:
        """Start a chaos experiment.

        This is a blocking call — the experiment runs for the specified
        duration and returns results when complete.
        """
        for s in scenarios:
            _check_null_bytes(s, "scenario")
        for val, name in [(input_dir, "input_dir"), (output_dir, "output_dir")]:
            _check_null_bytes(val, name)

        params = {
            "scenario": ",".join(scenarios) if scenarios else "",
            "input": input_dir,
            "output": output_dir,
            "duration": duration,
            "mode": mode,
        }
        if dry_run:
            params["dry_run"] = "true"

        return self._api_call("run", params)

    def replay(self, manifest_path: str) -> dict | list | None:
        """Replay mutations from a JSONL manifest file."""
        _check_null_bytes(manifest_path, "manifest_path")
        return self._api_call("replay", {"manifest": manifest_path})

    def status(self) -> dict | list | None:
        """Check the status of the chaos engine."""
        return self._api_call("status")

    def inject(
        self,
        scenario: str,
        target: str,
        input_dir: str,
        output_dir: str,
    ) -> dict | list | None:
        """Inject a single mutation against a specific target."""
        _check_null_bytes(scenario, "scenario")
        _check_null_bytes(target, "target")
        for val, name in [(input_dir, "input_dir"), (output_dir, "output_dir")]:
            _check_null_bytes(val, name)
        return self._api_call("run", {
            "scenario": scenario,
            "target": target,
            "input": input_dir,
            "output": output_dir,
        })

    def _run(self, args: list[str]) -> str:
        """Execute the chaos-data binary with arguments.

        Args:
            args: CLI arguments to pass after the binary name.

        Returns:
            The stdout output as a string.

        Raises:
            ChaosDataError: If the process exits with a non-zero return code.
            subprocess.TimeoutExpired: If the process exceeds the timeout.
            FileNotFoundError: If the binary is not found on the system PATH.
        """
        proc = subprocess.run(
            [self._binary_path, *args],
            capture_output=True,
            text=True,
            timeout=self._timeout,
        )
        if proc.returncode != 0:
            raise ChaosDataError(proc.returncode, proc.stderr.strip())
        return proc.stdout
