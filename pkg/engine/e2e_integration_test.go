package engine_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dwsmith1983/chaos-data/adapters/local"
	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/engine"
	"github.com/dwsmith1983/chaos-data/pkg/mutation"
	"github.com/dwsmith1983/chaos-data/pkg/scenario"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

const testJSONL = `{"user":"alice","action":"login","ts":"2024-01-01T00:00:00Z"}
{"user":"bob","action":"purchase","ts":"2024-01-01T01:00:00Z"}
{"user":"carol","action":"logout","ts":"2024-01-01T02:00:00Z"}
`

// fullRegistry registers all 21 mutations (13 data + 8 state/compound).
// The interlock adapter wraps the same 7 state mutations with pipeline-prefix
// enrichment — it doesn't add new mutation types.
func fullRegistry(t *testing.T, store adapter.StateStore) *mutation.Registry {
	t.Helper()
	reg := mutation.NewRegistry()
	dataMutations := []mutation.Mutation{
		&mutation.DelayMutation{},
		&mutation.DropMutation{},
		&mutation.CorruptMutation{},
		&mutation.DuplicateMutation{},
		&mutation.EmptyMutation{},
		&mutation.SchemaDriftMutation{},
		&mutation.StaleReplayMutation{},
		&mutation.MultiDayMutation{},
		&mutation.PartialMutation{},
		&mutation.SlowWriteMutation{},
		&mutation.StreamingLagMutation{},
		&mutation.RollingDegradationMutation{},
		&mutation.OutOfOrderMutation{},
	}
	for _, m := range dataMutations {
		if err := reg.Register(m); err != nil {
			t.Fatalf("register %s: %v", m.Type(), err)
		}
	}
	stateMutations := []mutation.Mutation{
		mutation.NewStaleSensorMutation(store),
		mutation.NewPhantomSensorMutation(store),
		mutation.NewSplitSensorMutation(store),
		mutation.NewPhantomTriggerMutation(store),
		mutation.NewJobKillMutation(store),
		mutation.NewTriggerTimeoutMutation(store),
		mutation.NewFalseSuccessMutation(store),
		mutation.NewCascadeDelayMutation(store),
	}
	for _, m := range stateMutations {
		if err := reg.Register(m); err != nil {
			t.Fatalf("register %s: %v", m.Type(), err)
		}
	}
	return reg
}

func writeTestFiles(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for name, content := range files {
		fpath := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(fpath), 0o755); err != nil {
			t.Fatalf("mkdir for %s: %v", name, err)
		}
		if err := os.WriteFile(fpath, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
}

func testScenario(name, mutationType string, sev types.Severity, params map[string]string) scenario.Scenario {
	return scenario.Scenario{
		Name:        name,
		Description: "e2e test scenario",
		Category:    "data-arrival",
		Severity:    sev,
		Version:     1,
		Target: scenario.TargetSpec{
			Layer: "data",
		},
		Mutation: scenario.MutationSpec{
			Type:   mutationType,
			Params: params,
		},
		Probability: 1.0,
	}
}

func testScenarioWithPrefix(name, mutationType, prefix string, sev types.Severity, params map[string]string) scenario.Scenario {
	s := testScenario(name, mutationType, sev, params)
	s.Target.Filter = scenario.FilterSpec{Prefix: prefix}
	return s
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func fileExists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	t.Fatalf("stat %s: unexpected error: %v", path, err)
	return false
}

func jsonlLineCount(data string) int {
	count := 0
	for _, line := range strings.Split(data, "\n") {
		if strings.TrimSpace(line) != "" {
			count++
		}
	}
	return count
}

func e2eConfig() types.EngineConfig {
	return types.EngineConfig{
		Mode: "deterministic",
		Safety: types.SafetyConfig{
			MaxSeverity:       types.SeverityCritical,
			MaxAffectedPct:    100,
			MaxPipelines:      100,
			KillSwitchEnabled: true,
		},
	}
}

func newSQLiteState(t *testing.T) *local.SQLiteState {
	t.Helper()
	store, err := local.NewSQLiteState(":memory:")
	if err != nil {
		t.Fatalf("new sqlite state: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

// --- Data Mutation Tests ---

func TestE2E_DataMutations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		factory func() mutation.Mutation
		params  map[string]string
		assert  func(t *testing.T, rec types.MutationRecord, stagingDir, outputDir string)
	}{
		{
			name:    "delay_hold",
			factory: func() mutation.Mutation { return &mutation.DelayMutation{} },
			params:  map[string]string{"duration": "30m"},
			assert: func(t *testing.T, rec types.MutationRecord, stagingDir, _ string) {
				t.Helper()
				if !rec.Applied {
					t.Fatal("expected Applied=true")
				}
				holdDir := filepath.Join(stagingDir, ".chaos-hold")
				if !fileExists(t, filepath.Join(holdDir, "test.jsonl")) {
					t.Error("file not in .chaos-hold/")
				}
				if !fileExists(t, filepath.Join(holdDir, "test.jsonl.meta")) {
					t.Error(".meta sidecar missing")
				}
			},
		},
		{
			name:    "delay_delete",
			factory: func() mutation.Mutation { return &mutation.DelayMutation{} },
			params:  map[string]string{"duration": "30m", "release": "false"},
			assert: func(t *testing.T, rec types.MutationRecord, stagingDir, _ string) {
				t.Helper()
				if !rec.Applied {
					t.Fatal("expected Applied=true")
				}
				if fileExists(t, filepath.Join(stagingDir, "test.jsonl")) {
					t.Error("file should be deleted from staging")
				}
			},
		},
		{
			name:    "drop",
			factory: func() mutation.Mutation { return &mutation.DropMutation{} },
			params:  map[string]string{},
			assert: func(t *testing.T, rec types.MutationRecord, stagingDir, outputDir string) {
				t.Helper()
				if !rec.Applied {
					t.Fatal("expected Applied=true")
				}
				if rec.Mutation != "drop" {
					t.Errorf("expected Mutation=drop, got %q", rec.Mutation)
				}
				// Drop doesn't touch the staging file.
				if !fileExists(t, filepath.Join(stagingDir, "test.jsonl")) {
					t.Error("file should still exist in staging")
				}
				// Drop prevents forwarding — output must NOT contain the file.
				if fileExists(t, filepath.Join(outputDir, "test.jsonl")) {
					t.Error("output should not contain file after drop")
				}
			},
		},
		{
			name:    "corrupt",
			factory: func() mutation.Mutation { return &mutation.CorruptMutation{} },
			params:  map[string]string{"affected_pct": "100"},
			assert: func(t *testing.T, rec types.MutationRecord, _, outputDir string) {
				t.Helper()
				if !rec.Applied {
					t.Fatal("expected Applied=true")
				}
				content := readFile(t, filepath.Join(outputDir, "test.jsonl"))
				if !strings.Contains(content, "null") {
					t.Error("expected null fields in corrupted output")
				}
			},
		},
		{
			name:    "duplicate",
			factory: func() mutation.Mutation { return &mutation.DuplicateMutation{} },
			params:  map[string]string{},
			assert: func(t *testing.T, rec types.MutationRecord, _, outputDir string) {
				t.Helper()
				if !rec.Applied {
					t.Fatal("expected Applied=true")
				}
				if !fileExists(t, filepath.Join(outputDir, "test.jsonl")) {
					t.Error("original missing in output")
				}
				if !fileExists(t, filepath.Join(outputDir, "test.jsonl.dup")) {
					t.Error("duplicate missing in output")
				}
			},
		},
		{
			name:    "empty",
			factory: func() mutation.Mutation { return &mutation.EmptyMutation{} },
			params:  map[string]string{},
			assert: func(t *testing.T, rec types.MutationRecord, _, outputDir string) {
				t.Helper()
				if !rec.Applied {
					t.Fatal("expected Applied=true")
				}
				content := readFile(t, filepath.Join(outputDir, "test.jsonl"))
				if len(content) != 0 {
					t.Errorf("expected 0 bytes, got %d", len(content))
				}
			},
		},
		{
			name:    "empty_header",
			factory: func() mutation.Mutation { return &mutation.EmptyMutation{} },
			params:  map[string]string{"preserve_header": "true"},
			assert: func(t *testing.T, rec types.MutationRecord, _, outputDir string) {
				t.Helper()
				if !rec.Applied {
					t.Fatal("expected Applied=true")
				}
				content := readFile(t, filepath.Join(outputDir, "test.jsonl"))
				if jsonlLineCount(content) != 1 {
					t.Errorf("expected 1 line (header), got %d", jsonlLineCount(content))
				}
			},
		},
		{
			name:    "schema_drift_add",
			factory: func() mutation.Mutation { return &mutation.SchemaDriftMutation{} },
			params:  map[string]string{"add_columns": "extra_col"},
			assert: func(t *testing.T, rec types.MutationRecord, _, outputDir string) {
				t.Helper()
				if !rec.Applied {
					t.Fatal("expected Applied=true")
				}
				content := readFile(t, filepath.Join(outputDir, "test.jsonl"))
				if !strings.Contains(content, "extra_col") {
					t.Error("expected extra_col in output")
				}
			},
		},
		{
			name:    "stale_replay",
			factory: func() mutation.Mutation { return &mutation.StaleReplayMutation{} },
			params:  map[string]string{"replay_date": "2024-01-15"},
			assert: func(t *testing.T, rec types.MutationRecord, _, outputDir string) {
				t.Helper()
				if !rec.Applied {
					t.Fatal("expected Applied=true")
				}
				replayPath := filepath.Join(outputDir, "date=2024-01-15", "test.jsonl")
				if !fileExists(t, replayPath) {
					t.Error("replayed file not at expected date-prefixed path")
				}
			},
		},
		{
			name:    "multi_day",
			factory: func() mutation.Mutation { return &mutation.MultiDayMutation{} },
			params:  map[string]string{"days": "2024-01-15,2024-01-16"},
			assert: func(t *testing.T, rec types.MutationRecord, _, outputDir string) {
				t.Helper()
				if !rec.Applied {
					t.Fatal("expected Applied=true")
				}
				for _, day := range []string{"2024-01-15", "2024-01-16"} {
					p := filepath.Join(outputDir, "date="+day, "test.jsonl")
					if !fileExists(t, p) {
						t.Errorf("file not at date=%s/ prefix", day)
					}
				}
			},
		},
		{
			name:    "partial",
			factory: func() mutation.Mutation { return &mutation.PartialMutation{} },
			params:  map[string]string{"delivery_pct": "50"},
			assert: func(t *testing.T, rec types.MutationRecord, _, outputDir string) {
				t.Helper()
				if !rec.Applied {
					t.Fatal("expected Applied=true")
				}
				content := readFile(t, filepath.Join(outputDir, "test.jsonl"))
				originalLen := len(testJSONL)
				// PartialMutation computes: deliverBytes = int64(pct) * int64(len) / 100
				expected := int64(50) * int64(originalLen) / 100
				if int64(len(content)) != expected {
					t.Errorf("expected exactly %d bytes (50%% of %d), got %d", expected, originalLen, len(content))
				}
			},
		},
		{
			name:    "slow_write",
			factory: func() mutation.Mutation { return &mutation.SlowWriteMutation{} },
			params:  map[string]string{"latency": "1ms"},
			assert: func(t *testing.T, rec types.MutationRecord, _, outputDir string) {
				t.Helper()
				if !rec.Applied {
					t.Fatal("expected Applied=true")
				}
				if !fileExists(t, filepath.Join(outputDir, "test.jsonl")) {
					t.Error("output file missing")
				}
			},
		},
		{
			name:    "streaming_lag",
			factory: func() mutation.Mutation { return &mutation.StreamingLagMutation{} },
			params:  map[string]string{"lag_duration": "1h"},
			assert: func(t *testing.T, rec types.MutationRecord, stagingDir, _ string) {
				t.Helper()
				if !rec.Applied {
					t.Fatal("expected Applied=true")
				}
				holdDir := filepath.Join(stagingDir, ".chaos-hold")
				if !fileExists(t, filepath.Join(holdDir, "test.jsonl")) {
					t.Error("file not in .chaos-hold/")
				}
			},
		},
		{
			name:    "rolling_degradation",
			factory: func() mutation.Mutation { return &mutation.RollingDegradationMutation{} },
			params:  map[string]string{"start_pct": "0", "end_pct": "100", "ramp_duration": "1h"},
			assert: func(t *testing.T, rec types.MutationRecord, _, outputDir string) {
				t.Helper()
				if !rec.Applied {
					t.Fatal("expected Applied=true")
				}
				content := readFile(t, filepath.Join(outputDir, "test.jsonl"))
				if !strings.Contains(content, "null") {
					t.Error("expected null fields in degraded output")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stagingDir := t.TempDir()
			outputDir := t.TempDir()

			writeTestFiles(t, stagingDir, map[string]string{"test.jsonl": testJSONL})

			transport := local.NewFSTransport(stagingDir, outputDir)

			mut := tt.factory()
			reg := mutation.NewRegistry()
			if err := reg.Register(mut); err != nil {
				t.Fatalf("register: %v", err)
			}

			sc := testScenario(tt.name, mut.Type(), types.SeverityLow, tt.params)
			eng := engine.New(e2eConfig(), transport, reg, []scenario.Scenario{sc})

			records, err := eng.ProcessObject(context.Background(), types.DataObject{Key: "test.jsonl"})
			if err != nil {
				t.Fatalf("ProcessObject: %v", err)
			}
			if len(records) != 1 {
				t.Fatalf("expected 1 record, got %d", len(records))
			}

			tt.assert(t, records[0], stagingDir, outputDir)
		})
	}
}

// --- Out-of-Order Mutation Test ---

// TestE2E_OutOfOrder uses two partition files to verify that the older partition
// is placed in .chaos-hold/ while the newer partition passes through unaffected.
func TestE2E_OutOfOrder(t *testing.T) {
	t.Parallel()

	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	// Write two partition files: the older one should be held, the newer passes through.
	writeTestFiles(t, stagingDir, map[string]string{
		"par_hour=14_data.jsonl": testJSONL,
		"par_hour=15_data.jsonl": testJSONL,
	})

	transport := local.NewFSTransport(stagingDir, outputDir)
	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.OutOfOrderMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	sc := testScenarioWithPrefix("out-of-order-test", "out-of-order", "par_hour=", types.SeverityLow,
		map[string]string{
			"delay_older_by":  "1h",
			"partition_field": "par_hour",
			"older_value":     "14",
			"newer_value":     "15",
		})

	eng := engine.New(e2eConfig(), transport, reg, []scenario.Scenario{sc})

	records, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records (one per file), got %d", len(records))
	}

	applied := map[string]bool{}
	for _, rec := range records {
		applied[rec.ObjectKey] = rec.Applied
	}

	// Older partition must be held (Applied=true).
	if !applied["par_hour=14_data.jsonl"] {
		t.Error("expected par_hour=14_data.jsonl to be Applied=true (held)")
	}

	// Newer partition passes through (Applied=false, no error).
	if applied["par_hour=15_data.jsonl"] {
		t.Error("expected par_hour=15_data.jsonl to be Applied=false (passthrough)")
	}

	// Verify physical hold: older file should be in .chaos-hold/.
	holdDir := filepath.Join(stagingDir, ".chaos-hold")
	if !fileExists(t, filepath.Join(holdDir, "par_hour=14_data.jsonl")) {
		t.Error("par_hour=14_data.jsonl not found in .chaos-hold/")
	}
	if !fileExists(t, filepath.Join(holdDir, "par_hour=14_data.jsonl.meta")) {
		t.Error("par_hour=14_data.jsonl.meta sidecar missing in .chaos-hold/")
	}

	// Newer file must NOT be in hold.
	if fileExists(t, filepath.Join(holdDir, "par_hour=15_data.jsonl")) {
		t.Error("par_hour=15_data.jsonl should not be in .chaos-hold/")
	}
}

// --- State Mutation Tests ---

// cascade-delay is tested separately in TestE2E_CascadeDelay because its
// compound effects require both transport and state assertions.
func TestE2E_StateMutations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		factory func(store adapter.StateStore) mutation.Mutation
		params  map[string]string
		seed    func(t *testing.T, store *local.SQLiteState)
		assert  func(t *testing.T, store *local.SQLiteState)
	}{
		{
			name:    "stale_sensor",
			factory: func(store adapter.StateStore) mutation.Mutation { return mutation.NewStaleSensorMutation(store) },
			params:  map[string]string{"sensor_key": "arrival", "pipeline": "ingest", "last_update_age": "24h"},
			seed: func(t *testing.T, store *local.SQLiteState) {
				t.Helper()
				err := store.WriteSensor(context.Background(), "ingest", "arrival", adapter.SensorData{
					Pipeline:    "ingest",
					Key:         "arrival",
					Status:      types.SensorStatusReady,
					LastUpdated: time.Now(),
				})
				if err != nil {
					t.Fatalf("seed sensor: %v", err)
				}
			},
			assert: func(t *testing.T, store *local.SQLiteState) {
				t.Helper()
				sensor, err := store.ReadSensor(context.Background(), "ingest", "arrival")
				if err != nil {
					t.Fatalf("read sensor: %v", err)
				}
				age := time.Since(sensor.LastUpdated)
				if age < 23*time.Hour {
					t.Errorf("expected LastUpdated ~24h ago, got age %v", age)
				}
			},
		},
		{
			name:    "phantom_sensor",
			factory: func(store adapter.StateStore) mutation.Mutation { return mutation.NewPhantomSensorMutation(store) },
			params:  map[string]string{"pipeline": "phantom-pipe", "sensor_key": "health"},
			assert: func(t *testing.T, store *local.SQLiteState) {
				t.Helper()
				sensor, err := store.ReadSensor(context.Background(), "phantom-pipe", "health")
				if err != nil {
					t.Fatalf("read sensor: %v", err)
				}
				if sensor.Status != types.SensorStatusReady {
					t.Errorf("expected status=ready, got %q", sensor.Status)
				}
			},
		},
		{
			name:    "split_sensor",
			factory: func(store adapter.StateStore) mutation.Mutation { return mutation.NewSplitSensorMutation(store) },
			params:  map[string]string{"sensor_key": "load", "pipeline": "etl", "conflicting_values": "ready,pending,stale"},
			assert: func(t *testing.T, store *local.SQLiteState) {
				t.Helper()
				sensor, err := store.ReadSensor(context.Background(), "etl", "load")
				if err != nil {
					t.Fatalf("read sensor: %v", err)
				}
				// Final value should be the last in the sequence.
				if sensor.Status != types.SensorStatusStale {
					t.Errorf("expected final status=stale, got %q", sensor.Status)
				}
			},
		},
		{
			name:    "phantom_trigger",
			factory: func(store adapter.StateStore) mutation.Mutation { return mutation.NewPhantomTriggerMutation(store) },
			params:  map[string]string{"pipeline": "nightly", "schedule": "daily", "date": "2024-01-15"},
			assert: func(t *testing.T, store *local.SQLiteState) {
				t.Helper()
				status, err := store.ReadTriggerStatus(context.Background(), adapter.TriggerKey{
					Pipeline: "nightly", Schedule: "daily", Date: "2024-01-15",
				})
				if err != nil {
					t.Fatalf("read trigger: %v", err)
				}
				if status != "triggered" {
					t.Errorf("expected triggered, got %q", status)
				}
			},
		},
		{
			name:    "job_kill",
			factory: func(store adapter.StateStore) mutation.Mutation { return mutation.NewJobKillMutation(store) },
			params:  map[string]string{"pipeline": "batch", "schedule": "hourly", "date": "2024-01-15"},
			assert: func(t *testing.T, store *local.SQLiteState) {
				t.Helper()
				status, err := store.ReadTriggerStatus(context.Background(), adapter.TriggerKey{
					Pipeline: "batch", Schedule: "hourly", Date: "2024-01-15",
				})
				if err != nil {
					t.Fatalf("read trigger: %v", err)
				}
				if status != "killed" {
					t.Errorf("expected killed, got %q", status)
				}
			},
		},
		{
			name:    "trigger_timeout",
			factory: func(store adapter.StateStore) mutation.Mutation { return mutation.NewTriggerTimeoutMutation(store) },
			params:  map[string]string{"pipeline": "sync", "schedule": "daily", "date": "2024-01-15"},
			assert: func(t *testing.T, store *local.SQLiteState) {
				t.Helper()
				status, err := store.ReadTriggerStatus(context.Background(), adapter.TriggerKey{
					Pipeline: "sync", Schedule: "daily", Date: "2024-01-15",
				})
				if err != nil {
					t.Fatalf("read trigger: %v", err)
				}
				if status != "timeout" {
					t.Errorf("expected timeout, got %q", status)
				}
			},
		},
		{
			name:    "false_success",
			factory: func(store adapter.StateStore) mutation.Mutation { return mutation.NewFalseSuccessMutation(store) },
			params:  map[string]string{"pipeline": "export", "schedule": "weekly", "date": "2024-01-15"},
			assert: func(t *testing.T, store *local.SQLiteState) {
				t.Helper()
				status, err := store.ReadTriggerStatus(context.Background(), adapter.TriggerKey{
					Pipeline: "export", Schedule: "weekly", Date: "2024-01-15",
				})
				if err != nil {
					t.Fatalf("read trigger: %v", err)
				}
				if status != "succeeded" {
					t.Errorf("expected succeeded, got %q", status)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := newSQLiteState(t)
			stagingDir := t.TempDir()
			outputDir := t.TempDir()
			writeTestFiles(t, stagingDir, map[string]string{"test.jsonl": testJSONL})
			transport := local.NewFSTransport(stagingDir, outputDir)

			if tt.seed != nil {
				tt.seed(t, store)
			}

			mut := tt.factory(store)
			reg := mutation.NewRegistry()
			if err := reg.Register(mut); err != nil {
				t.Fatalf("register: %v", err)
			}

			sc := testScenario(tt.name, mut.Type(), types.SeverityLow, tt.params)
			sc.Category = "state-consistency"
			eng := engine.New(e2eConfig(), transport, reg, []scenario.Scenario{sc})

			records, err := eng.ProcessObject(context.Background(), types.DataObject{Key: "test.jsonl"})
			if err != nil {
				t.Fatalf("ProcessObject: %v", err)
			}
			if len(records) != 1 {
				t.Fatalf("expected 1 record, got %d", len(records))
			}
			if !records[0].Applied {
				t.Fatalf("expected Applied=true, got error: %s", records[0].Error)
			}

			tt.assert(t, store)
		})
	}
}

// --- Compound Mutation Tests ---

func TestE2E_CascadeDelay(t *testing.T) {
	t.Parallel()

	store := newSQLiteState(t)
	stagingDir := t.TempDir()
	outputDir := t.TempDir()
	writeTestFiles(t, stagingDir, map[string]string{"test.jsonl": testJSONL})

	transport := local.NewFSTransport(stagingDir, outputDir)

	reg := mutation.NewRegistry()
	if err := reg.Register(mutation.NewCascadeDelayMutation(store)); err != nil {
		t.Fatalf("register: %v", err)
	}

	sc := testScenario("cascade-test", "cascade-delay", types.SeverityLow, map[string]string{
		"upstream_pipeline": "upstream",
		"delay_duration":    "1h",
		"sensor_key":        "arrival",
	})
	sc.Category = "compound"

	eng := engine.New(e2eConfig(), transport, reg, []scenario.Scenario{sc})
	records, err := eng.ProcessObject(context.Background(), types.DataObject{Key: "test.jsonl"})
	if err != nil {
		t.Fatalf("ProcessObject: %v", err)
	}
	if len(records) != 1 || !records[0].Applied {
		t.Fatalf("expected 1 applied record, got %d", len(records))
	}

	// Transport effect: file in .chaos-hold/.
	holdDir := filepath.Join(stagingDir, ".chaos-hold")
	if !fileExists(t, filepath.Join(holdDir, "test.jsonl")) {
		t.Error("file not in .chaos-hold/")
	}

	// State effect: sensor marked stale.
	sensor, err := store.ReadSensor(context.Background(), "upstream", "arrival")
	if err != nil {
		t.Fatalf("read sensor: %v", err)
	}
	if sensor.Status != types.SensorStatusStale {
		t.Errorf("expected sensor status=stale, got %q", sensor.Status)
	}
}

// --- Safety Tests ---

func TestE2E_Safety_CooldownEnforcement(t *testing.T) {
	t.Parallel()

	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	transport := local.NewFSTransport(stagingDir, outputDir)
	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DropMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Use a controllable clock from the start to avoid mutating a live field.
	now := time.Now()
	clock := func() time.Time { return now }

	safety := local.NewConfigSafety(types.SafetyConfig{
		MaxSeverity:       types.SeverityCritical,
		MaxAffectedPct:    100,
		MaxPipelines:      100,
		CooldownDuration:  types.Duration{Duration: 10 * time.Minute},
		KillSwitchEnabled: true,
	})
	safety.Now = clock

	sc := testScenario("cooldown-test", "drop", types.SeverityLow, map[string]string{})
	eng := engine.New(e2eConfig(), transport, reg, []scenario.Scenario{sc}, engine.WithSafety(safety))

	obj := types.DataObject{Key: "test.jsonl"}

	// First call: should apply.
	records, err := eng.ProcessObject(context.Background(), obj)
	if err != nil {
		t.Fatalf("first ProcessObject: %v", err)
	}
	if len(records) != 1 || !records[0].Applied {
		t.Fatal("first call should apply")
	}

	// Second call: should be skipped (cooldown active).
	records, err = eng.ProcessObject(context.Background(), obj)
	if err != nil {
		t.Fatalf("second ProcessObject: %v", err)
	}
	if len(records) != 0 {
		t.Errorf("expected 0 records (cooldown skip), got %d", len(records))
	}

	// Advance the clock past cooldown. The clock closure captures now by reference.
	now = now.Add(11 * time.Minute)

	// Third call: should apply again.
	records, err = eng.ProcessObject(context.Background(), obj)
	if err != nil {
		t.Fatalf("third ProcessObject: %v", err)
	}
	if len(records) != 1 || !records[0].Applied {
		t.Fatal("third call should apply (cooldown expired)")
	}
}

// --- Dry-Run Tests ---

func TestE2E_DryRun_NoSideEffects(t *testing.T) {
	t.Parallel()

	stagingDir := t.TempDir()
	outputDir := t.TempDir()
	writeTestFiles(t, stagingDir, map[string]string{"test.jsonl": testJSONL})

	transport := local.NewFSTransport(stagingDir, outputDir)
	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	emitter := &mockEmitter{}
	sc := testScenario("dryrun-test", "delay", types.SeverityLow, map[string]string{"duration": "30m"})

	cfg := e2eConfig()
	cfg.DryRun = true
	eng := engine.New(cfg, transport, reg, []scenario.Scenario{sc}, engine.WithEmitter(emitter))

	records, err := eng.ProcessObject(context.Background(), types.DataObject{Key: "test.jsonl"})
	if err != nil {
		t.Fatalf("ProcessObject: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	// Dry-run record: Applied=false, Error="dry-run".
	if records[0].Applied {
		t.Error("expected Applied=false in dry-run")
	}
	if records[0].Error != "dry-run" {
		t.Errorf("expected Error=dry-run, got %q", records[0].Error)
	}

	// Files unchanged.
	if !fileExists(t, filepath.Join(stagingDir, "test.jsonl")) {
		t.Error("file should still be in staging")
	}
	holdDir := filepath.Join(stagingDir, ".chaos-hold")
	if fileExists(t, holdDir) {
		t.Error(".chaos-hold/ should not exist")
	}

	// Emitter still receives events.
	events := emitter.getEvents()
	if len(events) != 1 {
		t.Errorf("expected 1 event emitted, got %d", len(events))
	}
}

func TestE2E_DryRun_NoCooldownRecorded(t *testing.T) {
	t.Parallel()

	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	transport := local.NewFSTransport(stagingDir, outputDir)
	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DropMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	safety := local.NewConfigSafety(types.SafetyConfig{
		MaxSeverity:       types.SeverityCritical,
		MaxAffectedPct:    100,
		MaxPipelines:      100,
		CooldownDuration:  types.Duration{Duration: 10 * time.Minute},
		KillSwitchEnabled: true,
	})

	sc := testScenario("dryrun-cooldown-test", "drop", types.SeverityLow, map[string]string{})
	cfg := e2eConfig()
	cfg.DryRun = true
	eng := engine.New(cfg, transport, reg, []scenario.Scenario{sc}, engine.WithSafety(safety))

	obj := types.DataObject{Key: "test.jsonl"}

	// First dry-run: should produce a record.
	records, err := eng.ProcessObject(context.Background(), obj)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	// Second dry-run: should NOT be blocked by cooldown (dry-run skips RecordInjection).
	records, err = eng.ProcessObject(context.Background(), obj)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if len(records) != 1 {
		t.Errorf("expected 1 record (no cooldown in dry-run), got %d", len(records))
	}
}

// --- ListHeld Test ---

func TestE2E_ListHeld(t *testing.T) {
	t.Parallel()

	stagingDir := t.TempDir()
	outputDir := t.TempDir()
	writeTestFiles(t, stagingDir, map[string]string{"test.jsonl": testJSONL})

	transport := local.NewFSTransport(stagingDir, outputDir)
	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DelayMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	sc := testScenario("hold-test", "delay", types.SeverityLow, map[string]string{"duration": "1h"})
	eng := engine.New(e2eConfig(), transport, reg, []scenario.Scenario{sc})

	_, err := eng.ProcessObject(context.Background(), types.DataObject{Key: "test.jsonl"})
	if err != nil {
		t.Fatalf("ProcessObject: %v", err)
	}

	held, err := transport.ListHeld(context.Background())
	if err != nil {
		t.Fatalf("ListHeld: %v", err)
	}
	if len(held) != 1 {
		t.Fatalf("expected 1 held file, got %d", len(held))
	}
	if held[0].Key != "test.jsonl" {
		t.Errorf("expected held key=test.jsonl, got %q", held[0].Key)
	}
}

// --- Multi-Scenario Test ---

func TestE2E_MultiScenario_Pipeline(t *testing.T) {
	t.Parallel()

	stagingDir := t.TempDir()
	outputDir := t.TempDir()
	writeTestFiles(t, stagingDir, map[string]string{"test.jsonl": testJSONL})

	transport := local.NewFSTransport(stagingDir, outputDir)
	reg := mutation.NewRegistry()
	for _, m := range []mutation.Mutation{
		&mutation.DropMutation{},
		&mutation.CorruptMutation{},
		&mutation.DuplicateMutation{},
	} {
		if err := reg.Register(m); err != nil {
			t.Fatalf("register %s: %v", m.Type(), err)
		}
	}

	scenarios := []scenario.Scenario{
		testScenario("s1-drop", "drop", types.SeverityLow, map[string]string{}),
		testScenario("s2-corrupt", "corrupt", types.SeverityLow, map[string]string{"affected_pct": "100"}),
		testScenario("s3-duplicate", "duplicate", types.SeverityLow, map[string]string{}),
	}

	eng := engine.New(e2eConfig(), transport, reg, scenarios)
	records, err := eng.ProcessObject(context.Background(), types.DataObject{Key: "test.jsonl"})
	if err != nil {
		t.Fatalf("ProcessObject: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}

	mutTypes := map[string]bool{}
	for _, rec := range records {
		mutTypes[rec.Mutation] = true
		if !rec.Applied {
			t.Errorf("record %s not applied", rec.Mutation)
		}
	}
	for _, expected := range []string{"drop", "corrupt", "duplicate"} {
		if !mutTypes[expected] {
			t.Errorf("missing mutation type: %s", expected)
		}
	}
}

// --- Prefix Filter Test ---

func TestE2E_PrefixFilter(t *testing.T) {
	t.Parallel()

	stagingDir := t.TempDir()
	outputDir := t.TempDir()
	writeTestFiles(t, stagingDir, map[string]string{
		"events-001.jsonl": testJSONL,
		"logs-001.jsonl":   testJSONL,
	})

	transport := local.NewFSTransport(stagingDir, outputDir)
	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DropMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	sc := testScenarioWithPrefix("prefix-test", "drop", "events-", types.SeverityLow, map[string]string{})
	eng := engine.New(e2eConfig(), transport, reg, []scenario.Scenario{sc})

	records, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(records) != 1 {
		t.Fatalf("expected 1 record (only events- prefix), got %d", len(records))
	}
	if records[0].ObjectKey != "events-001.jsonl" {
		t.Errorf("expected events-001.jsonl, got %q", records[0].ObjectKey)
	}
}

// --- Experiment Lifecycle Test ---

func TestE2E_ExperimentLifecycle(t *testing.T) {
	t.Parallel()

	stagingDir := t.TempDir()
	outputDir := t.TempDir()
	writeTestFiles(t, stagingDir, map[string]string{
		"test-001.jsonl": testJSONL,
		"test-002.jsonl": testJSONL,
	})

	transport := local.NewFSTransport(stagingDir, outputDir)
	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DropMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	emitter := &mockEmitter{}
	sc := testScenario("exp-test", "drop", types.SeverityLow, map[string]string{})
	eng := engine.New(e2eConfig(), transport, reg, []scenario.Scenario{sc}, engine.WithEmitter(emitter))

	expCfg := types.ExperimentConfig{
		Duration: types.Duration{Duration: 100 * time.Millisecond},
		Mode:     "deterministic",
	}

	exp, err := eng.StartExperiment(context.Background(), expCfg)
	if err != nil {
		t.Fatalf("StartExperiment: %v", err)
	}

	exp.Wait()

	if exp.Err() != nil {
		t.Fatalf("experiment error: %v", exp.Err())
	}
	if exp.State() != types.ExperimentCompleted {
		t.Errorf("expected state=completed, got %q", exp.State())
	}

	records := exp.Records()
	if len(records) != 2 {
		t.Errorf("expected 2 records, got %d", len(records))
	}

	events := exp.Events()
	if len(events) != 2 {
		t.Errorf("expected 2 events, got %d", len(events))
	}

	// Manifest is valid JSONL.
	manifest, err := exp.Manifest()
	if err != nil {
		t.Fatalf("Manifest: %v", err)
	}
	lineCount := 0
	for _, line := range bytes.Split(manifest, []byte("\n")) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var ev types.ChaosEvent
		if err := json.Unmarshal(line, &ev); err != nil {
			t.Errorf("invalid JSONL line: %v", err)
		}
		lineCount++
	}
	if lineCount != 2 {
		t.Errorf("expected 2 manifest lines, got %d", lineCount)
	}

	// Stats.
	stats := exp.Stats()
	if stats.TotalEvents != 2 {
		t.Errorf("expected TotalEvents=2, got %d", stats.TotalEvents)
	}
}

// --- Manifest Replay Test ---

func TestE2E_ManifestReplay(t *testing.T) {
	t.Parallel()

	stagingDir := t.TempDir()
	outputDir := t.TempDir()
	writeTestFiles(t, stagingDir, map[string]string{
		"test-001.jsonl": testJSONL,
		"test-002.jsonl": testJSONL,
	})

	transport := local.NewFSTransport(stagingDir, outputDir)
	reg := mutation.NewRegistry()
	if err := reg.Register(&mutation.DropMutation{}); err != nil {
		t.Fatalf("register: %v", err)
	}

	emitter := &mockEmitter{}
	sc := testScenario("replay-test", "drop", types.SeverityLow, map[string]string{})
	eng := engine.New(e2eConfig(), transport, reg, []scenario.Scenario{sc}, engine.WithEmitter(emitter))

	// Run experiment.
	expCfg := types.ExperimentConfig{
		Duration: types.Duration{Duration: 100 * time.Millisecond},
		Mode:     "deterministic",
	}
	exp, err := eng.StartExperiment(context.Background(), expCfg)
	if err != nil {
		t.Fatalf("StartExperiment: %v", err)
	}
	exp.Wait()
	if exp.Err() != nil {
		t.Fatalf("experiment error: %v", exp.Err())
	}

	manifest, err := exp.Manifest()
	if err != nil {
		t.Fatalf("Manifest: %v", err)
	}

	// Set up fresh staging for replay.
	replayStagingDir := t.TempDir()
	replayOutputDir := t.TempDir()
	writeTestFiles(t, replayStagingDir, map[string]string{
		"test-001.jsonl": testJSONL,
		"test-002.jsonl": testJSONL,
	})
	replayTransport := local.NewFSTransport(replayStagingDir, replayOutputDir)
	replayEng := engine.New(e2eConfig(), replayTransport, reg, nil)

	replayRecords, err := replayEng.ReplayFromManifest(context.Background(), manifest)
	if err != nil {
		t.Fatalf("ReplayFromManifest: %v", err)
	}

	if len(replayRecords) != len(exp.Records()) {
		t.Errorf("replay produced %d records, original had %d", len(replayRecords), len(exp.Records()))
	}

	// Verify same mutation types and all Applied.
	origTypes := map[string]int{}
	for _, r := range exp.Records() {
		origTypes[r.Mutation]++
	}
	replayTypes := map[string]int{}
	for _, r := range replayRecords {
		if !r.Applied {
			t.Errorf("replay record %s not applied: %s", r.Mutation, r.Error)
		}
		replayTypes[r.Mutation]++
	}
	for mt, count := range origTypes {
		if replayTypes[mt] != count {
			t.Errorf("mutation %s: original=%d, replay=%d", mt, count, replayTypes[mt])
		}
	}
}

// --- Capstone: All Mutations in One Run ---

func TestE2E_AllMutations_SingleRun(t *testing.T) {
	t.Parallel()

	store := newSQLiteState(t)
	stagingDir := t.TempDir()
	outputDir := t.TempDir()

	// Seed sensor for stale-sensor (requires existing row).
	err := store.WriteSensor(context.Background(), "ingest", "arrival", adapter.SensorData{
		Pipeline:    "ingest",
		Key:         "arrival",
		Status:      types.SensorStatusReady,
		LastUpdated: time.Now(),
	})
	if err != nil {
		t.Fatalf("seed sensor: %v", err)
	}

	// All mutation types and their required params.
	type mutDef struct {
		mutType  string
		category string
		params   map[string]string
	}
	allMuts := []mutDef{
		{"delay", "data-arrival", map[string]string{"duration": "30m"}},
		{"drop", "data-arrival", map[string]string{}},
		{"corrupt", "data-quality", map[string]string{"affected_pct": "100"}},
		{"duplicate", "data-arrival", map[string]string{}},
		{"empty", "data-quality", map[string]string{}},
		{"schema-drift", "data-quality", map[string]string{"add_columns": "extra"}},
		{"stale-replay", "data-arrival", map[string]string{"replay_date": "2024-01-15"}},
		{"multi-day", "data-arrival", map[string]string{"days": "2024-01-15,2024-01-16"}},
		{"partial", "data-quality", map[string]string{"delivery_pct": "50"}},
		{"slow-write", "infrastructure", map[string]string{"latency": "1ms"}},
		{"streaming-lag", "data-arrival", map[string]string{"lag_duration": "1h"}},
		{"rolling-degradation", "data-quality", map[string]string{"start_pct": "0", "end_pct": "100", "ramp_duration": "1h"}},
		{"stale-sensor", "state-consistency", map[string]string{"sensor_key": "arrival", "pipeline": "ingest", "last_update_age": "24h"}},
		{"phantom-sensor", "state-consistency", map[string]string{"pipeline": "phantom", "sensor_key": "health"}},
		{"split-sensor", "state-consistency", map[string]string{"sensor_key": "load", "pipeline": "etl", "conflicting_values": "ready,stale"}},
		{"phantom-trigger", "orchestrator", map[string]string{"pipeline": "nightly", "schedule": "daily", "date": "2024-01-15"}},
		{"job-kill", "orchestrator", map[string]string{"pipeline": "batch", "schedule": "hourly", "date": "2024-01-15"}},
		{"trigger-timeout", "orchestrator", map[string]string{"pipeline": "sync", "schedule": "daily", "date": "2024-01-15"}},
		{"false-success", "orchestrator", map[string]string{"pipeline": "export", "schedule": "weekly", "date": "2024-01-15"}},
		{"cascade-delay", "compound", map[string]string{"upstream_pipeline": "upstream", "delay_duration": "1h", "sensor_key": "arrival"}},
	}

	// Write a file per mutation, each with a unique prefix.
	files := map[string]string{}
	var scenarios []scenario.Scenario
	for _, md := range allMuts {
		prefix := md.mutType + "-"
		fileName := prefix + "001.jsonl"
		files[fileName] = testJSONL

		sc := testScenarioWithPrefix(
			md.mutType+"-scenario",
			md.mutType,
			prefix,
			types.SeverityLow,
			md.params,
		)
		sc.Category = md.category
		scenarios = append(scenarios, sc)
	}

	writeTestFiles(t, stagingDir, files)

	transport := local.NewFSTransport(stagingDir, outputDir)
	reg := fullRegistry(t, store)
	eng := engine.New(e2eConfig(), transport, reg, scenarios)

	records, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(records) != len(allMuts) {
		t.Fatalf("expected %d records, got %d", len(allMuts), len(records))
	}

	// Verify all mutation types are present.
	appliedTypes := map[string]bool{}
	for _, rec := range records {
		if !rec.Applied {
			t.Errorf("record %s/%s not applied: %s", rec.ObjectKey, rec.Mutation, rec.Error)
		}
		appliedTypes[rec.Mutation] = true
	}
	for _, md := range allMuts {
		if !appliedTypes[md.mutType] {
			t.Errorf("missing mutation type: %s", md.mutType)
		}
	}
}
