"""Tests for chaosdata.types — frozen dataclasses mirroring Go types."""

import dataclasses

import pytest

from chaosdata.types import (
    ChaosEvent,
    ExperimentResult,
    ExperimentStats,
    MutationRecord,
    ObjectFilter,
    Safety,
    Target,
)


class TestObjectFilter:
    def test_defaults(self) -> None:
        f = ObjectFilter()
        assert f.prefix == ""
        assert f.match == ""

    def test_frozen(self) -> None:
        f = ObjectFilter(prefix="raw/")
        with pytest.raises(dataclasses.FrozenInstanceError):
            f.prefix = "other/"  # type: ignore[misc]

    def test_custom_values(self) -> None:
        f = ObjectFilter(prefix="raw/", match="*.csv")
        assert f.prefix == "raw/"
        assert f.match == "*.csv"


class TestTarget:
    def test_defaults(self) -> None:
        t = Target()
        assert t.layer == "data"
        assert t.transport == ""
        assert t.filter == ObjectFilter()

    def test_valid_layers(self) -> None:
        for layer in ("data", "state", "orchestrator"):
            t = Target(layer=layer)
            assert t.layer == layer

    def test_invalid_layer(self) -> None:
        with pytest.raises(ValueError, match="invalid layer"):
            Target(layer="invalid")

    def test_frozen(self) -> None:
        t = Target()
        with pytest.raises(dataclasses.FrozenInstanceError):
            t.layer = "state"  # type: ignore[misc]

    def test_with_filter(self) -> None:
        f = ObjectFilter(prefix="raw/", match="*.csv")
        t = Target(layer="data", transport="s3", filter=f)
        assert t.filter.prefix == "raw/"
        assert t.transport == "s3"


class TestSafety:
    def test_defaults(self) -> None:
        s = Safety()
        assert s.max_affected_pct == 25
        assert s.cooldown == "5m"
        assert s.sla_aware is True

    def test_frozen(self) -> None:
        s = Safety()
        with pytest.raises(dataclasses.FrozenInstanceError):
            s.max_affected_pct = 50  # type: ignore[misc]

    def test_custom_values(self) -> None:
        s = Safety(max_affected_pct=50, cooldown="10m", sla_aware=False)
        assert s.max_affected_pct == 50
        assert s.cooldown == "10m"
        assert s.sla_aware is False

    def test_invalid_max_affected_pct_low(self) -> None:
        with pytest.raises(ValueError, match="max_affected_pct must be 0-100"):
            Safety(max_affected_pct=-1)

    def test_invalid_max_affected_pct_high(self) -> None:
        with pytest.raises(ValueError, match="max_affected_pct must be 0-100"):
            Safety(max_affected_pct=101)

    def test_boundary_values(self) -> None:
        s0 = Safety(max_affected_pct=0)
        assert s0.max_affected_pct == 0
        s100 = Safety(max_affected_pct=100)
        assert s100.max_affected_pct == 100


class TestMutationRecord:
    def test_defaults(self) -> None:
        r = MutationRecord()
        assert r.object_key == ""
        assert r.mutation == ""
        assert r.params == {}
        assert r.applied is False
        assert r.error == ""
        assert r.timestamp == ""

    def test_frozen(self) -> None:
        r = MutationRecord(object_key="test.csv")
        with pytest.raises(dataclasses.FrozenInstanceError):
            r.applied = True  # type: ignore[misc]

    def test_custom_values(self) -> None:
        r = MutationRecord(
            object_key="data/file.csv",
            mutation="delay",
            params={"duration": "30m"},
            applied=True,
            timestamp="2024-01-15T10:00:00Z",
        )
        assert r.object_key == "data/file.csv"
        assert r.mutation == "delay"
        assert r.params == {"duration": "30m"}
        assert r.applied is True


class TestExperimentResult:
    def test_defaults(self) -> None:
        r = ExperimentResult()
        assert r.experiment_id == ""
        assert r.records == ()
        assert r.state == ""

    def test_with_records(self) -> None:
        rec = MutationRecord(object_key="a.csv", applied=True)
        result = ExperimentResult(
            experiment_id="exp-1",
            records=(rec,),
            state="completed",
        )
        assert len(result.records) == 1
        assert result.records[0].object_key == "a.csv"

    def test_frozen(self) -> None:
        r = ExperimentResult()
        with pytest.raises(dataclasses.FrozenInstanceError):
            r.state = "running"  # type: ignore[misc]


class TestChaosEvent:
    def test_defaults(self) -> None:
        e = ChaosEvent()
        assert e.id == ""
        assert e.experiment_id == ""
        assert e.params == {}

    def test_frozen(self) -> None:
        e = ChaosEvent(id="ev-1")
        with pytest.raises(dataclasses.FrozenInstanceError):
            e.id = "ev-2"  # type: ignore[misc]


class TestExperimentStats:
    def test_defaults(self) -> None:
        s = ExperimentStats()
        assert s.total_events == 0
        assert s.affected_pct == 0.0

    def test_frozen(self) -> None:
        s = ExperimentStats(total_events=5)
        with pytest.raises(dataclasses.FrozenInstanceError):
            s.total_events = 10  # type: ignore[misc]
