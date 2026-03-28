package types

import "testing"

func TestAssertionType_IsValid(t *testing.T) {
	tests := []struct {
		name  string
		at    AssertionType
		valid bool
	}{
		{"sensor_state", AssertSensorState, true},
		{"trigger_state", AssertTriggerState, true},
		{"event_emitted", AssertEventEmitted, true},
		{"job_state", AssertJobState, true},
		{"data_state", AssertDataState, true},
		{"unknown", AssertionType("bogus"), false},
		{"empty", AssertionType(""), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.at.IsValid(); got != tt.valid {
				t.Errorf("AssertionType(%q).IsValid() = %v, want %v", tt.at, got, tt.valid)
			}
		})
	}
}

func TestCondition_IsValid(t *testing.T) {
	tests := []struct {
		name  string
		c     Condition
		valid bool
	}{
		{"is_stale", CondIsStale, true},
		{"is_ready", CondIsReady, true},
		{"is_pending", CondIsPending, true},
		{"exists", CondExists, true},
		{"not_exists", CondNotExists, true},
		{"is_held", CondIsHeld, true},
		{"status:failed", CondStatusFailed, true},
		{"status:running", CondStatusRunning, true},
		{"status:success", CondStatusSuccess, true},
		{"status:killed", CondStatusKilled, true},
		{"status:timeout", CondStatusTimeout, true},
		{"was_triggered", CondWasTriggered, true},
		{"status:stopped", CondStatusStopped, true}, // NEW: Dagster sensor/schedule stopped state
		{"unknown", Condition("bogus"), false},
		{"empty", Condition(""), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.c.IsValid(); got != tt.valid {
				t.Errorf("Condition(%q).IsValid() = %v, want %v", tt.c, got, tt.valid)
			}
		})
	}
}

func TestCondition_ValidFor(t *testing.T) {
	tests := []struct {
		cond  Condition
		atype AssertionType
		valid bool
	}{
		{CondIsStale, AssertSensorState, true},
		{CondIsStale, AssertTriggerState, true},
		{CondIsStale, AssertDataState, false},
		{CondExists, AssertSensorState, true},
		{CondExists, AssertEventEmitted, true},
		{CondExists, AssertDataState, true},
		{CondNotExists, AssertDataState, true},
		{CondNotExists, AssertSensorState, false},
		{CondIsHeld, AssertDataState, true},
		{CondIsHeld, AssertSensorState, false},
		{CondStatusFailed, AssertTriggerState, true},
		{CondStatusFailed, AssertJobState, true},
		{CondStatusFailed, AssertSensorState, false},
		{CondStatusRunning, AssertJobState, true},
		// NEW: status:running is now also valid for sensor_state and trigger_state
		{CondStatusRunning, AssertSensorState, true},
		{CondStatusRunning, AssertTriggerState, true},
		{CondWasTriggered, AssertTriggerState, true},
		{CondWasTriggered, AssertSensorState, false},
		// NEW: status:stopped valid for sensor_state and trigger_state, not job_state
		{CondStatusStopped, AssertSensorState, true},
		{CondStatusStopped, AssertTriggerState, true},
		{CondStatusStopped, AssertJobState, false},
		// NEW: is_pending and status:killed now valid for job_state
		{CondIsPending, AssertJobState, true},
		{CondStatusKilled, AssertJobState, true},
	}
	for _, tt := range tests {
		name := string(tt.cond) + "/" + string(tt.atype)
		t.Run(name, func(t *testing.T) {
			if got := tt.cond.ValidFor(tt.atype); got != tt.valid {
				t.Errorf("Condition(%q).ValidFor(%q) = %v, want %v",
					tt.cond, tt.atype, got, tt.valid)
			}
		})
	}
}

func TestAssertInterlockEvent_IsValid(t *testing.T) {
	if !AssertInterlockEvent.IsValid() {
		t.Error("AssertInterlockEvent should be valid")
	}
}

func TestAssertRerunState_IsValid(t *testing.T) {
	if !AssertRerunState.IsValid() {
		t.Error("AssertRerunState should be valid")
	}
}

func TestCondNotExists_ValidForEventAssertions(t *testing.T) {
	tests := []struct {
		at   AssertionType
		want bool
	}{
		{AssertEventEmitted, true},
		{AssertInterlockEvent, true},
		{AssertRerunState, true},
		{AssertSensorState, false},
		{AssertTriggerState, false},
	}
	for _, tt := range tests {
		t.Run(string(tt.at), func(t *testing.T) {
			if got := CondNotExists.ValidFor(tt.at); got != tt.want {
				t.Errorf("CondNotExists.ValidFor(%q) = %v, want %v", tt.at, got, tt.want)
			}
		})
	}
}

func TestAssertion_Validate(t *testing.T) {
	tests := []struct {
		name    string
		a       Assertion
		wantErr bool
	}{
		{"valid sensor 2 segments", Assertion{AssertSensorState, "pipe/key", CondIsStale}, false},
		{"valid trigger 3 segments", Assertion{AssertTriggerState, "p/s/d", CondStatusFailed}, false},
		{"valid event 2 segments", Assertion{AssertEventEmitted, "sc/mut", CondExists}, false},
		{"valid job 1 segment", Assertion{AssertJobState, "job-1", CondStatusFailed}, false},
		{"valid data 1 segment", Assertion{AssertDataState, "file.jsonl", CondExists}, false},
		{"unknown type", Assertion{AssertionType("nope"), "x", CondExists}, true},
		{"unknown condition", Assertion{AssertSensorState, "p/k", Condition("nope")}, true},
		{"condition invalid for type", Assertion{AssertDataState, "f", CondIsStale}, true},
		{"empty target", Assertion{AssertSensorState, "", CondIsStale}, true},
		// NEW: Validate() no longer rejects based on segment count — routing is
		// delegated to adapter ValidateTarget. These previously-failing cases
		// must now PASS validation.
		{"sensor 1-segment target passes validate", Assertion{AssertSensorState, "only-one", CondIsStale}, false},
		{"sensor 3-segment target passes validate", Assertion{AssertSensorState, "a/b/c", CondIsStale}, false},
		{"trigger 2-segment target passes validate", Assertion{AssertTriggerState, "only/two", CondStatusFailed}, false},
		{"trigger 1-segment target passes validate", Assertion{AssertTriggerState, "just-one", CondStatusFailed}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.a.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Assertion.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
