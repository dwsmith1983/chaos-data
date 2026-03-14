"""Tests for chaosdata.scenario — mutation parameter dataclasses and Scenario."""

import dataclasses

import pytest

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
from chaosdata.types import ObjectFilter, Safety, Target


class TestDelay:
    def test_defaults(self) -> None:
        d = Delay()
        assert d.duration == "30m"
        assert d.jitter == "5m"
        assert d.release is True

    def test_to_params(self) -> None:
        d = Delay(duration="1h", jitter="10m", release=False)
        params = d.to_params()
        assert params == {
            "duration": "1h",
            "jitter": "10m",
            "release": "false",
        }

    def test_frozen(self) -> None:
        d = Delay()
        with pytest.raises(dataclasses.FrozenInstanceError):
            d.duration = "1h"  # type: ignore[misc]


class TestCorrupt:
    def test_defaults(self) -> None:
        c = Corrupt()
        assert c.affected_pct == 10
        assert c.corruption_type == "null"

    def test_to_params(self) -> None:
        c = Corrupt(affected_pct=25, corruption_type="null")
        assert c.to_params() == {
            "affected_pct": "25",
            "corruption_type": "null",
        }

    def test_invalid_affected_pct_low(self) -> None:
        with pytest.raises(ValueError, match="affected_pct must be 0-100"):
            Corrupt(affected_pct=-1)

    def test_invalid_affected_pct_high(self) -> None:
        with pytest.raises(ValueError, match="affected_pct must be 0-100"):
            Corrupt(affected_pct=101)

    def test_frozen(self) -> None:
        c = Corrupt()
        with pytest.raises(dataclasses.FrozenInstanceError):
            c.affected_pct = 50  # type: ignore[misc]


class TestDrop:
    def test_defaults(self) -> None:
        d = Drop()
        assert d.scope == "object"

    def test_to_params(self) -> None:
        d = Drop(scope="partition")
        assert d.to_params() == {"scope": "partition"}

    def test_frozen(self) -> None:
        d = Drop()
        with pytest.raises(dataclasses.FrozenInstanceError):
            d.scope = "partition"  # type: ignore[misc]


class TestDuplicate:
    def test_defaults(self) -> None:
        d = Duplicate()
        assert d.dup_pct == 100
        assert d.exact is True

    def test_to_params(self) -> None:
        d = Duplicate(dup_pct=50, exact=False)
        assert d.to_params() == {"dup_pct": "50", "exact": "false"}


class TestSchemaDrift:
    def test_defaults(self) -> None:
        s = SchemaDrift()
        assert s.add_columns == ""
        assert s.remove_columns == ""
        assert s.change_types == ""

    def test_to_params_empty(self) -> None:
        s = SchemaDrift()
        assert s.to_params() == {}

    def test_to_params_with_values(self) -> None:
        s = SchemaDrift(
            add_columns="col_a,col_b",
            remove_columns="old_col",
            change_types="price:string",
        )
        params = s.to_params()
        assert params == {
            "add_columns": "col_a,col_b",
            "remove_columns": "old_col",
            "change_types": "price:string",
        }


class TestStaleReplay:
    def test_defaults(self) -> None:
        s = StaleReplay()
        assert s.replay_date == ""
        assert s.prefix == ""

    def test_to_params(self) -> None:
        s = StaleReplay(replay_date="2024-01-15", prefix="archive")
        assert s.to_params() == {
            "replay_date": "2024-01-15",
            "prefix": "archive",
        }

    def test_to_params_without_prefix(self) -> None:
        s = StaleReplay(replay_date="2024-01-15")
        assert s.to_params() == {"replay_date": "2024-01-15"}


class TestMultiDay:
    def test_defaults(self) -> None:
        m = MultiDay()
        assert m.days == ""
        assert m.prefix == ""

    def test_to_params(self) -> None:
        m = MultiDay(days="2024-01-15,2024-01-16", prefix="data")
        assert m.to_params() == {
            "days": "2024-01-15,2024-01-16",
            "prefix": "data",
        }


class TestPartial:
    def test_defaults(self) -> None:
        p = Partial()
        assert p.delivery_pct == 50

    def test_to_params(self) -> None:
        p = Partial(delivery_pct=75)
        assert p.to_params() == {"delivery_pct": "75"}

    def test_invalid_delivery_pct_low(self) -> None:
        with pytest.raises(ValueError, match="delivery_pct must be 0-100"):
            Partial(delivery_pct=-1)

    def test_invalid_delivery_pct_high(self) -> None:
        with pytest.raises(ValueError, match="delivery_pct must be 0-100"):
            Partial(delivery_pct=101)


class TestEmpty:
    def test_defaults(self) -> None:
        e = Empty()
        assert e.preserve_header is False

    def test_to_params(self) -> None:
        e = Empty(preserve_header=True)
        assert e.to_params() == {"preserve_header": "true"}


class TestScenario:
    def test_defaults(self) -> None:
        s = Scenario()
        assert s.name == ""
        assert s.category == "data-arrival"
        assert s.severity == "low"
        assert s.probability == 0.3
        assert isinstance(s.mutation, Delay)
        assert isinstance(s.target, Target)
        assert isinstance(s.safety, Safety)

    def test_construction(self) -> None:
        s = Scenario(
            name="test-delay",
            target=Target(layer="data", filter=ObjectFilter(prefix="raw/")),
            mutation=Delay(duration="1h"),
            description="Test scenario for late data",
        )
        assert s.name == "test-delay"
        assert s.target.filter.prefix == "raw/"
        assert s.description == "Test scenario for late data"

    def test_mutation_type_property(self) -> None:
        assert Scenario(mutation=Delay()).mutation_type == "delay"
        assert Scenario(mutation=Corrupt()).mutation_type == "corrupt"
        assert Scenario(mutation=Drop()).mutation_type == "drop"
        assert Scenario(mutation=Duplicate()).mutation_type == "duplicate"
        assert Scenario(mutation=SchemaDrift()).mutation_type == "schema-drift"
        assert Scenario(mutation=StaleReplay()).mutation_type == "stale-replay"
        assert Scenario(mutation=MultiDay()).mutation_type == "multi-day"
        assert Scenario(mutation=Partial()).mutation_type == "partial"
        assert Scenario(mutation=Empty()).mutation_type == "empty"

    def test_frozen(self) -> None:
        s = Scenario(name="test")
        with pytest.raises(dataclasses.FrozenInstanceError):
            s.name = "other"  # type: ignore[misc]

    def test_invalid_category(self) -> None:
        with pytest.raises(ValueError, match="invalid category"):
            Scenario(category="bad-category")

    def test_invalid_severity(self) -> None:
        with pytest.raises(ValueError, match="invalid severity"):
            Scenario(severity="extreme")

    def test_valid_severities(self) -> None:
        for sev in ("low", "moderate", "severe", "critical"):
            s = Scenario(severity=sev)
            assert s.severity == sev

    def test_valid_categories(self) -> None:
        for cat in (
            "data-arrival", "data-quality", "state-consistency",
            "infrastructure", "orchestrator", "compound",
        ):
            s = Scenario(category=cat)
            assert s.category == cat

    def test_invalid_probability_low(self) -> None:
        with pytest.raises(ValueError, match="probability must be in"):
            Scenario(probability=-0.1)

    def test_invalid_probability_high(self) -> None:
        with pytest.raises(ValueError, match="probability must be in"):
            Scenario(probability=1.1)

    def test_probability_boundaries(self) -> None:
        s0 = Scenario(probability=0.0)
        assert s0.probability == 0.0
        s1 = Scenario(probability=1.0)
        assert s1.probability == 1.0

    def test_with_corrupt_mutation(self) -> None:
        s = Scenario(
            name="corrupt-test",
            mutation=Corrupt(affected_pct=20),
            category="data-quality",
            severity="moderate",
        )
        assert s.mutation_type == "corrupt"
        assert s.mutation.affected_pct == 20
