"""Tests for chaosdata.client — subprocess wrapper for the chaos-data CLI."""

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from chaosdata.client import ChaosDataClient, ChaosDataError


class TestChaosDataClient:
    def test_default_binary_path(self) -> None:
        client = ChaosDataClient()
        assert client.binary_path == "chaos-data"

    def test_custom_binary_path(self, tmp_path: "Path") -> None:
        binary = tmp_path / "chaos-data"
        binary.touch(mode=0o755)
        client = ChaosDataClient(binary_path=str(binary))
        assert client.binary_path == str(binary.resolve())

    def test_default_timeout(self) -> None:
        client = ChaosDataClient()
        assert client.timeout == 300

    def test_custom_timeout(self) -> None:
        client = ChaosDataClient(timeout=60)
        assert client.timeout == 60


class TestCatalog:
    @patch("chaosdata.client.subprocess.run")
    def test_catalog_calls_subprocess(self, mock_run: MagicMock) -> None:
        mock_run.return_value = subprocess.CompletedProcess(
            args=["chaos-data", "catalog"],
            returncode=0,
            stdout="NAME\tCATEGORY\n",
            stderr="",
        )
        client = ChaosDataClient()
        result = client.catalog()

        mock_run.assert_called_once_with(
            ["chaos-data", "catalog"],
            capture_output=True,
            text=True,
            timeout=300,
        )
        assert result == "NAME\tCATEGORY\n"

    @patch("chaosdata.client.subprocess.run")
    def test_catalog_error(self, mock_run: MagicMock) -> None:
        mock_run.return_value = subprocess.CompletedProcess(
            args=["chaos-data", "catalog"],
            returncode=1,
            stdout="",
            stderr="load catalog: no scenarios found",
        )
        client = ChaosDataClient()
        with pytest.raises(ChaosDataError) as exc_info:
            client.catalog()
        assert exc_info.value.returncode == 1
        assert "no scenarios found" in exc_info.value.stderr


class TestRun:
    @patch("chaosdata.client.subprocess.run")
    def test_run_calls_subprocess(self, mock_run: MagicMock) -> None:
        mock_run.return_value = subprocess.CompletedProcess(
            args=[
                "chaos-data", "run",
                "--scenario", "late-arriving-data",
                "--input", "/tmp/input",
                "--output", "/tmp/output",
            ],
            returncode=0,
            stdout="1 mutation(s) applied:\n",
            stderr="",
        )
        client = ChaosDataClient()
        result = client.run("late-arriving-data", "/tmp/input", "/tmp/output")

        mock_run.assert_called_once_with(
            [
                "chaos-data", "run",
                "--scenario", "late-arriving-data",
                "--input", "/tmp/input",
                "--output", "/tmp/output",
            ],
            capture_output=True,
            text=True,
            timeout=300,
        )
        assert "mutation" in result

    @patch("chaosdata.client.subprocess.run")
    def test_run_error(self, mock_run: MagicMock) -> None:
        mock_run.return_value = subprocess.CompletedProcess(
            args=["chaos-data", "run"],
            returncode=1,
            stdout="",
            stderr="scenario not found",
        )
        client = ChaosDataClient()
        with pytest.raises(ChaosDataError) as exc_info:
            client.run("nonexistent", "/tmp/in", "/tmp/out")
        assert exc_info.value.returncode == 1

    @patch("chaosdata.client.subprocess.run")
    def test_run_timeout(self, mock_run: MagicMock) -> None:
        mock_run.side_effect = subprocess.TimeoutExpired(
            cmd="chaos-data", timeout=300
        )
        client = ChaosDataClient()
        with pytest.raises(subprocess.TimeoutExpired):
            client.run("test", "/tmp/in", "/tmp/out")

    @patch("chaosdata.client.subprocess.run")
    def test_binary_not_found(self, mock_run: MagicMock) -> None:
        mock_run.side_effect = FileNotFoundError(
            "[Errno 2] No such file or directory: 'chaos-data'"
        )
        client = ChaosDataClient()
        with pytest.raises(FileNotFoundError):
            client.catalog()


class TestCustomBinary:
    @patch("chaosdata.client.subprocess.run")
    def test_custom_binary_used_in_commands(
        self, mock_run: MagicMock, tmp_path: Path
    ) -> None:
        binary = tmp_path / "chaos-data"
        binary.touch(mode=0o755)
        resolved = str(binary.resolve())
        mock_run.return_value = subprocess.CompletedProcess(
            args=[resolved, "catalog"],
            returncode=0,
            stdout="output",
            stderr="",
        )
        client = ChaosDataClient(binary_path=str(binary))
        client.catalog()

        call_args = mock_run.call_args[0][0]
        assert call_args[0] == resolved

    @patch("chaosdata.client.subprocess.run")
    def test_custom_timeout_used(self, mock_run: MagicMock) -> None:
        mock_run.return_value = subprocess.CompletedProcess(
            args=["chaos-data", "catalog"],
            returncode=0,
            stdout="output",
            stderr="",
        )
        client = ChaosDataClient(timeout=60)
        client.catalog()

        call_kwargs = mock_run.call_args[1]
        assert call_kwargs["timeout"] == 60


class TestChaosDataError:
    def test_error_message(self) -> None:
        err = ChaosDataError(returncode=1, stderr="something went wrong")
        assert "1" in str(err)
        assert "something went wrong" in str(err)
        assert err.returncode == 1
        assert err.stderr == "something went wrong"
