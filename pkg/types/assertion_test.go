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
		{CondStatusRunning, AssertTriggerState, false},
		{CondWasTriggered, AssertTriggerState, true},
		{CondWasTriggered, AssertSensorState, false},
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
		{"sensor wrong segments", Assertion{AssertSensorState, "only-one", CondIsStale}, true},
		{"trigger wrong segments", Assertion{AssertTriggerState, "only/two", CondStatusFailed}, true},
		{"empty target", Assertion{AssertSensorState, "", CondIsStale}, true},
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
