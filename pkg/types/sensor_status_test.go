package types

import "testing"

func TestSensorStatus_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		status SensorStatus
		valid  bool
	}{
		{SensorStatusReady, true},
		{SensorStatusPending, true},
		{SensorStatusStale, true},
		{SensorStatusUnknown, true},
		{SensorStatusComplete, true},
		{SensorStatus("bogus"), false},
		{SensorStatus(""), false},
	}

	for _, tc := range tests {
		t.Run(string(tc.status), func(t *testing.T) {
			t.Parallel()
			if got := tc.status.IsValid(); got != tc.valid {
				t.Errorf("SensorStatus(%q).IsValid() = %v, want %v",
					tc.status, got, tc.valid)
			}
		})
	}
}
