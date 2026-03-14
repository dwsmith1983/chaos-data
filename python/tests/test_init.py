"""Tests for chaosdata.__init__ — public API surface and convenience functions."""

import subprocess
from unittest.mock import MagicMock, patch

import chaosdata


class TestPublicAPI:
    """Verify that all expected symbols are importable from the top-level package."""

    def test_types_exported(self) -> None:
        assert hasattr(chaosdata, "Target")
        assert hasattr(chaosdata, "Safety")
        assert hasattr(chaosdata, "MutationRecord")
        assert hasattr(chaosdata, "ExperimentResult")
        assert hasattr(chaosdata, "ObjectFilter")
        assert hasattr(chaosdata, "ChaosEvent")
        assert hasattr(chaosdata, "ExperimentStats")

    def test_scenarios_exported(self) -> None:
        assert hasattr(chaosdata, "Scenario")
        assert hasattr(chaosdata, "Delay")
        assert hasattr(chaosdata, "Corrupt")
        assert hasattr(chaosdata, "Drop")
        assert hasattr(chaosdata, "Duplicate")
        assert hasattr(chaosdata, "SchemaDrift")
        assert hasattr(chaosdata, "StaleReplay")
        assert hasattr(chaosdata, "MultiDay")
        assert hasattr(chaosdata, "Partial")
        assert hasattr(chaosdata, "Empty")

    def test_client_exported(self) -> None:
        assert hasattr(chaosdata, "ChaosDataClient")
        assert hasattr(chaosdata, "ChaosDataError")

    def test_convenience_functions_exported(self) -> None:
        assert callable(chaosdata.run)
        assert callable(chaosdata.catalog)


class TestConvenienceFunctions:
    def setup_method(self) -> None:
        # Reset the module-level default client between tests.
        chaosdata._default_client = None

    @patch("chaosdata.client.subprocess.run")
    def test_catalog_convenience(self, mock_run: MagicMock) -> None:
        mock_run.return_value = subprocess.CompletedProcess(
            args=["chaos-data", "catalog"],
            returncode=0,
            stdout="NAME\tCATEGORY\n",
            stderr="",
        )
        result = chaosdata.catalog()
        assert result == "NAME\tCATEGORY\n"

    @patch("chaosdata.client.subprocess.run")
    def test_run_convenience(self, mock_run: MagicMock) -> None:
        mock_run.return_value = subprocess.CompletedProcess(
            args=["chaos-data", "run"],
            returncode=0,
            stdout="done",
            stderr="",
        )
        result = chaosdata.run("test-scenario", "/tmp/in", "/tmp/out")
        assert result == "done"

    @patch("chaosdata.client.subprocess.run")
    def test_default_client_reused(self, mock_run: MagicMock) -> None:
        mock_run.return_value = subprocess.CompletedProcess(
            args=["chaos-data", "catalog"],
            returncode=0,
            stdout="output",
            stderr="",
        )
        chaosdata.catalog()
        client1 = chaosdata._default_client
        chaosdata.catalog()
        client2 = chaosdata._default_client
        assert client1 is client2
