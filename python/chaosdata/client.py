"""Subprocess client for the chaos-data Go CLI binary.

Wraps the CLI via subprocess with JSON stdin/stdout communication.
"""

from __future__ import annotations

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
