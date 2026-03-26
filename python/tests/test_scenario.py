"""Tests for chaosdata.scenario — mutation parameter dataclasses and Scenario."""

import dataclasses

import pytest

from chaosdata.scenario import (
    CascadeDelay,
    Corrupt,
    Delay,
    Drop,
    Duplicate,
    Empty,
    FalseSuccess,
    JobKill,
    MultiDay,
    OutOfOrder,
    Partial,
    PhantomSensor,
    PhantomTrigger,
    PostRunDrift,
    RollingDegradation,
    Scenario,
    SchemaDrift,
    SensorFlapping,
    SlowWrite,
    SplitSensor,
    StaleReplay,
    StaleSensor,
    StreamingLag,
    TimestampForgery,
    TriggerTimeout,
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


class TestCascadeDelay:
    def test_defaults(self) -> None:
        c = CascadeDelay()
        assert c.upstream_pipeline == ""
        assert c.delay_duration == ""
        assert c.sensor_key == "arrival"

    def test_to_params(self) -> None:
        c = CascadeDelay(
            upstream_pipeline="ingest", delay_duration="15m", sensor_key="status"
        )
        assert c.to_params() == {
            "upstream_pipeline": "ingest",
            "delay_duration": "15m",
            "sensor_key": "status",
        }

    def test_frozen(self) -> None:
        c = CascadeDelay()
        with pytest.raises(dataclasses.FrozenInstanceError):
            c.upstream_pipeline = "x"  # type: ignore[misc]


class TestOutOfOrder:
    def test_defaults(self) -> None:
        o = OutOfOrder()
        assert o.delay_older_by == ""
        assert o.partition_field == ""
        assert o.older_value == ""
        assert o.newer_value == ""

    def test_to_params(self) -> None:
        o = OutOfOrder(
            delay_older_by="1h",
            partition_field="date",
            older_value="2024-01-01",
            newer_value="2024-01-02",
        )
        assert o.to_params() == {
            "delay_older_by": "1h",
            "partition_field": "date",
            "older_value": "2024-01-01",
            "newer_value": "2024-01-02",
        }

    def test_frozen(self) -> None:
        o = OutOfOrder()
        with pytest.raises(dataclasses.FrozenInstanceError):
            o.partition_field = "x"  # type: ignore[misc]


class TestStreamingLag:
    def test_defaults(self) -> None:
        s = StreamingLag()
        assert s.lag_duration == ""
        assert s.consumer_group == ""

    def test_to_params(self) -> None:
        s = StreamingLag(lag_duration="5m", consumer_group="analytics")
        assert s.to_params() == {
            "lag_duration": "5m",
            "consumer_group": "analytics",
        }

    def test_frozen(self) -> None:
        s = StreamingLag()
        with pytest.raises(dataclasses.FrozenInstanceError):
            s.lag_duration = "x"  # type: ignore[misc]


class TestPostRunDrift:
    def test_defaults(self) -> None:
        p = PostRunDrift()
        assert p.partition_key == ""
        assert p.partition_value == ""
        assert p.drift_delay == ""
        assert p.late_pct == 20

    def test_to_params(self) -> None:
        p = PostRunDrift(
            partition_key="region",
            partition_value="us-east-1",
            drift_delay="30m",
            late_pct=40,
        )
        assert p.to_params() == {
            "partition_key": "region",
            "partition_value": "us-east-1",
            "drift_delay": "30m",
            "late_pct": "40",
        }

    def test_frozen(self) -> None:
        p = PostRunDrift()
        with pytest.raises(dataclasses.FrozenInstanceError):
            p.partition_key = "x"  # type: ignore[misc]

    def test_invalid_late_pct_low(self) -> None:
        with pytest.raises(ValueError, match="late_pct must be 1-100"):
            PostRunDrift(late_pct=0)

    def test_invalid_late_pct_high(self) -> None:
        with pytest.raises(ValueError, match="late_pct must be 1-100"):
            PostRunDrift(late_pct=101)


class TestSlowWrite:
    def test_defaults(self) -> None:
        s = SlowWrite()
        assert s.latency == ""
        assert s.jitter == ""

    def test_to_params(self) -> None:
        s = SlowWrite(latency="500ms", jitter="100ms")
        assert s.to_params() == {
            "latency": "500ms",
            "jitter": "100ms",
        }

    def test_frozen(self) -> None:
        s = SlowWrite()
        with pytest.raises(dataclasses.FrozenInstanceError):
            s.latency = "x"  # type: ignore[misc]


class TestRollingDegradation:
    def test_defaults(self) -> None:
        r = RollingDegradation()
        assert r.start_pct == 0
        assert r.end_pct == 100
        assert r.ramp_duration == ""

    def test_to_params(self) -> None:
        r = RollingDegradation(start_pct=10, end_pct=90, ramp_duration="1h")
        assert r.to_params() == {
            "start_pct": "10",
            "end_pct": "90",
            "ramp_duration": "1h",
        }

    def test_frozen(self) -> None:
        r = RollingDegradation()
        with pytest.raises(dataclasses.FrozenInstanceError):
            r.start_pct = 5  # type: ignore[misc]

    def test_invalid_start_pct_low(self) -> None:
        with pytest.raises(ValueError, match="start_pct must be 0-100"):
            RollingDegradation(start_pct=-1)

    def test_invalid_start_pct_high(self) -> None:
        with pytest.raises(ValueError, match="start_pct must be 0-100"):
            RollingDegradation(start_pct=101)

    def test_invalid_end_pct_low(self) -> None:
        with pytest.raises(ValueError, match="end_pct must be 0-100"):
            RollingDegradation(end_pct=-1)

    def test_invalid_end_pct_high(self) -> None:
        with pytest.raises(ValueError, match="end_pct must be 0-100"):
            RollingDegradation(end_pct=101)


class TestStaleSensor:
    def test_defaults(self) -> None:
        s = StaleSensor()
        assert s.sensor_key == ""
        assert s.pipeline == ""
        assert s.last_update_age == ""

    def test_to_params(self) -> None:
        s = StaleSensor(sensor_key="arrival", pipeline="ingest", last_update_age="2h")
        assert s.to_params() == {
            "sensor_key": "arrival",
            "pipeline": "ingest",
            "last_update_age": "2h",
        }

    def test_frozen(self) -> None:
        s = StaleSensor()
        with pytest.raises(dataclasses.FrozenInstanceError):
            s.sensor_key = "x"  # type: ignore[misc]


class TestPhantomSensor:
    def test_defaults(self) -> None:
        p = PhantomSensor()
        assert p.pipeline == ""
        assert p.sensor_key == ""
        assert p.status == "ready"

    def test_to_params(self) -> None:
        p = PhantomSensor(pipeline="ingest", sensor_key="arrival", status="stale")
        assert p.to_params() == {
            "pipeline": "ingest",
            "sensor_key": "arrival",
            "status": "stale",
        }

    def test_frozen(self) -> None:
        p = PhantomSensor()
        with pytest.raises(dataclasses.FrozenInstanceError):
            p.pipeline = "x"  # type: ignore[misc]


class TestSensorFlapping:
    def test_defaults(self) -> None:
        s = SensorFlapping()
        assert s.sensor_key == ""
        assert s.pipeline == ""
        assert s.flap_count == 5
        assert s.start_status == "ready"
        assert s.alternate_status == "pending"

    def test_to_params(self) -> None:
        s = SensorFlapping(
            sensor_key="arrival",
            pipeline="ingest",
            flap_count=10,
            start_status="active",
            alternate_status="inactive",
        )
        assert s.to_params() == {
            "sensor_key": "arrival",
            "pipeline": "ingest",
            "flap_count": "10",
            "start_status": "active",
            "alternate_status": "inactive",
        }

    def test_frozen(self) -> None:
        s = SensorFlapping()
        with pytest.raises(dataclasses.FrozenInstanceError):
            s.flap_count = 3  # type: ignore[misc]

    def test_invalid_flap_count(self) -> None:
        with pytest.raises(ValueError, match="flap_count must be >= 1"):
            SensorFlapping(flap_count=0)


class TestSplitSensor:
    def test_defaults(self) -> None:
        s = SplitSensor()
        assert s.sensor_key == ""
        assert s.pipeline == ""
        assert s.conflicting_values == "ready,stale"

    def test_to_params(self) -> None:
        s = SplitSensor(
            sensor_key="arrival", pipeline="ingest", conflicting_values="ok,error"
        )
        assert s.to_params() == {
            "sensor_key": "arrival",
            "pipeline": "ingest",
            "conflicting_values": "ok,error",
        }

    def test_frozen(self) -> None:
        s = SplitSensor()
        with pytest.raises(dataclasses.FrozenInstanceError):
            s.sensor_key = "x"  # type: ignore[misc]


class TestTimestampForgery:
    def test_defaults(self) -> None:
        t = TimestampForgery()
        assert t.sensor_key == ""
        assert t.pipeline == ""
        assert t.last_updated_offset == ""
        assert t.payload_timestamp_offset == ""

    def test_to_params(self) -> None:
        t = TimestampForgery(
            sensor_key="arrival",
            pipeline="ingest",
            last_updated_offset="-2h",
            payload_timestamp_offset="-1h",
        )
        assert t.to_params() == {
            "sensor_key": "arrival",
            "pipeline": "ingest",
            "last_updated_offset": "-2h",
            "payload_timestamp_offset": "-1h",
        }

    def test_frozen(self) -> None:
        t = TimestampForgery()
        with pytest.raises(dataclasses.FrozenInstanceError):
            t.sensor_key = "x"  # type: ignore[misc]


class TestFalseSuccess:
    def test_defaults(self) -> None:
        f = FalseSuccess()
        assert f.pipeline == ""
        assert f.schedule == ""
        assert f.date == ""
        assert f.job_type == "glue"
        assert f.missing_output == ""

    def test_to_params(self) -> None:
        f = FalseSuccess(
            pipeline="etl",
            schedule="daily",
            date="2024-01-15",
            job_type="spark",
            missing_output="s3://bucket/output",
        )
        assert f.to_params() == {
            "pipeline": "etl",
            "schedule": "daily",
            "date": "2024-01-15",
            "job_type": "spark",
            "missing_output": "s3://bucket/output",
        }

    def test_frozen(self) -> None:
        f = FalseSuccess()
        with pytest.raises(dataclasses.FrozenInstanceError):
            f.pipeline = "x"  # type: ignore[misc]


class TestJobKill:
    def test_defaults(self) -> None:
        j = JobKill()
        assert j.pipeline == ""
        assert j.schedule == ""
        assert j.date == ""
        assert j.kill_after_pct == 50
        assert j.job_type == "glue"

    def test_to_params(self) -> None:
        j = JobKill(
            pipeline="etl",
            schedule="daily",
            date="2024-01-15",
            kill_after_pct=75,
            job_type="spark",
        )
        assert j.to_params() == {
            "pipeline": "etl",
            "schedule": "daily",
            "date": "2024-01-15",
            "kill_after_pct": "75",
            "job_type": "spark",
        }

    def test_frozen(self) -> None:
        j = JobKill()
        with pytest.raises(dataclasses.FrozenInstanceError):
            j.pipeline = "x"  # type: ignore[misc]

    def test_invalid_kill_after_pct_low(self) -> None:
        with pytest.raises(ValueError, match="kill_after_pct must be 0-100"):
            JobKill(kill_after_pct=-1)

    def test_invalid_kill_after_pct_high(self) -> None:
        with pytest.raises(ValueError, match="kill_after_pct must be 0-100"):
            JobKill(kill_after_pct=101)


class TestPhantomTrigger:
    def test_defaults(self) -> None:
        p = PhantomTrigger()
        assert p.pipeline == ""
        assert p.schedule == ""
        assert p.date == ""
        assert p.trigger_type == "scheduled"

    def test_to_params(self) -> None:
        p = PhantomTrigger(
            pipeline="etl",
            schedule="daily",
            date="2024-01-15",
            trigger_type="manual",
        )
        assert p.to_params() == {
            "pipeline": "etl",
            "schedule": "daily",
            "date": "2024-01-15",
            "trigger_type": "manual",
        }

    def test_frozen(self) -> None:
        p = PhantomTrigger()
        with pytest.raises(dataclasses.FrozenInstanceError):
            p.pipeline = "x"  # type: ignore[misc]


class TestTriggerTimeout:
    def test_defaults(self) -> None:
        t = TriggerTimeout()
        assert t.pipeline == ""
        assert t.schedule == ""
        assert t.date == ""
        assert t.timeout_duration == "30m"

    def test_to_params(self) -> None:
        t = TriggerTimeout(
            pipeline="etl",
            schedule="daily",
            date="2024-01-15",
            timeout_duration="1h",
        )
        assert t.to_params() == {
            "pipeline": "etl",
            "schedule": "daily",
            "date": "2024-01-15",
            "timeout_duration": "1h",
        }

    def test_frozen(self) -> None:
        t = TriggerTimeout()
        with pytest.raises(dataclasses.FrozenInstanceError):
            t.pipeline = "x"  # type: ignore[misc]


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
        assert Scenario(mutation=CascadeDelay()).mutation_type == "cascade-delay"
        assert Scenario(mutation=OutOfOrder()).mutation_type == "out-of-order"
        assert Scenario(mutation=StreamingLag()).mutation_type == "streaming-lag"
        assert Scenario(mutation=PostRunDrift()).mutation_type == "post-run-drift"
        assert Scenario(mutation=SlowWrite()).mutation_type == "slow-write"
        assert Scenario(mutation=RollingDegradation()).mutation_type == "rolling-degradation"
        assert Scenario(mutation=StaleSensor()).mutation_type == "stale-sensor"
        assert Scenario(mutation=PhantomSensor()).mutation_type == "phantom-sensor"
        assert Scenario(mutation=SensorFlapping()).mutation_type == "sensor-flapping"
        assert Scenario(mutation=SplitSensor()).mutation_type == "split-sensor"
        assert Scenario(mutation=TimestampForgery()).mutation_type == "timestamp-forgery"
        assert Scenario(mutation=FalseSuccess()).mutation_type == "false-success"
        assert Scenario(mutation=JobKill()).mutation_type == "job-kill"
        assert Scenario(mutation=PhantomTrigger()).mutation_type == "phantom-trigger"
        assert Scenario(mutation=TriggerTimeout()).mutation_type == "trigger-timeout"

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
