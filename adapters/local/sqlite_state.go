package local

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"

	"github.com/dwsmith1983/chaos-data/pkg/adapter"
	"github.com/dwsmith1983/chaos-data/pkg/types"
)

// Compile-time interface assertion.
var _ adapter.StateStore = (*SQLiteState)(nil)

// SQLiteState implements adapter.StateStore backed by a SQLite database.
type SQLiteState struct {
	db *sql.DB
}

// NewSQLiteState opens (or creates) a SQLite database at dsn and initialises
// the schema. Use ":memory:" for an ephemeral in-memory database.
func NewSQLiteState(dsn string) (*SQLiteState, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	if err := createTables(db); err != nil {
		db.Close()
		return nil, err
	}
	return &SQLiteState{db: db}, nil
}

// Close releases the underlying database connection.
func (s *SQLiteState) Close() error {
	return s.db.Close()
}

func createTables(db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS sensors (
			pipeline TEXT NOT NULL,
			key TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT '',
			last_updated TEXT NOT NULL DEFAULT '',
			metadata TEXT NOT NULL DEFAULT '{}',
			PRIMARY KEY (pipeline, key)
		)`,
		`CREATE TABLE IF NOT EXISTS triggers (
			pipeline TEXT NOT NULL,
			schedule TEXT NOT NULL,
			date TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (pipeline, schedule, date)
		)`,
		`CREATE TABLE IF NOT EXISTS chaos_events (
			id TEXT NOT NULL PRIMARY KEY,
			experiment_id TEXT NOT NULL DEFAULT '',
			scenario TEXT NOT NULL DEFAULT '',
			category TEXT NOT NULL DEFAULT '',
			severity INTEGER NOT NULL DEFAULT 0,
			target TEXT NOT NULL DEFAULT '',
			mutation TEXT NOT NULL DEFAULT '',
			params TEXT NOT NULL DEFAULT '{}',
			timestamp TEXT NOT NULL DEFAULT '',
			mode TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS idx_events_experiment ON chaos_events(experiment_id)`,
		`CREATE TABLE IF NOT EXISTS pipeline_configs (
			pipeline TEXT NOT NULL PRIMARY KEY,
			config TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS reruns (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			pipeline TEXT NOT NULL,
			schedule TEXT NOT NULL,
			date TEXT NOT NULL,
			reason TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS job_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			pipeline TEXT NOT NULL,
			schedule TEXT NOT NULL,
			date TEXT NOT NULL,
			event TEXT NOT NULL,
			run_id TEXT NOT NULL DEFAULT '',
			timestamp TEXT NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("create tables: %w", err)
		}
	}
	return nil
}

// ReadSensor returns the sensor data for the given pipeline and key.
// If no row exists, a zero-value SensorData is returned without error.
func (s *SQLiteState) ReadSensor(ctx context.Context, pipeline, key string) (adapter.SensorData, error) {
	var (
		status      string
		lastUpdated string
		metaJSON    string
	)
	row := s.db.QueryRowContext(ctx,
		`SELECT status, last_updated, metadata FROM sensors WHERE pipeline = ? AND key = ?`,
		pipeline, key,
	)
	if err := row.Scan(&status, &lastUpdated, &metaJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return adapter.SensorData{}, nil
		}
		return adapter.SensorData{}, fmt.Errorf("read sensor: %w", err)
	}

	var t time.Time
	if lastUpdated != "" {
		var err error
		t, err = time.Parse(time.RFC3339Nano, lastUpdated)
		if err != nil {
			return adapter.SensorData{}, fmt.Errorf("read sensor: parse last_updated: %w", err)
		}
	}

	var metadata map[string]string
	if metaJSON != "" && metaJSON != "{}" {
		if err := json.Unmarshal([]byte(metaJSON), &metadata); err != nil {
			return adapter.SensorData{}, fmt.Errorf("read sensor: parse metadata: %w", err)
		}
	}

	return adapter.SensorData{
		Pipeline:    pipeline,
		Key:         key,
		Status:      types.SensorStatus(status),
		LastUpdated: t,
		Metadata:    metadata,
	}, nil
}

// WriteSensor upserts sensor data for the given pipeline and key.
func (s *SQLiteState) WriteSensor(ctx context.Context, pipeline, key string, data adapter.SensorData) error {
	metaBytes, err := json.Marshal(data.Metadata)
	if err != nil {
		return fmt.Errorf("write sensor: marshal metadata: %w", err)
	}
	// nil metadata marshals to "null"; normalise to "{}".
	metaJSON := string(metaBytes)
	if metaJSON == "null" {
		metaJSON = "{}"
	}

	_, err = s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO sensors (pipeline, key, status, last_updated, metadata) VALUES (?, ?, ?, ?, ?)`,
		pipeline, key, string(data.Status), data.LastUpdated.Format(time.RFC3339Nano), metaJSON,
	)
	if err != nil {
		return fmt.Errorf("write sensor: %w", err)
	}
	return nil
}

// DeleteSensor removes the sensor row for the given pipeline and key.
func (s *SQLiteState) DeleteSensor(ctx context.Context, pipeline, key string) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM sensors WHERE pipeline = ? AND key = ?`,
		pipeline, key,
	)
	if err != nil {
		return fmt.Errorf("delete sensor: %w", err)
	}
	return nil
}

// ReadTriggerStatus returns the status string for the given trigger key.
// If no row exists, an empty string is returned without error.
func (s *SQLiteState) ReadTriggerStatus(ctx context.Context, key adapter.TriggerKey) (string, error) {
	var status string
	row := s.db.QueryRowContext(ctx,
		`SELECT status FROM triggers WHERE pipeline = ? AND schedule = ? AND date = ?`,
		key.Pipeline, key.Schedule, key.Date,
	)
	if err := row.Scan(&status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("read trigger status: %w", err)
	}
	return status, nil
}

// WriteTriggerStatus upserts the status for the given trigger key.
func (s *SQLiteState) WriteTriggerStatus(ctx context.Context, key adapter.TriggerKey, status string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO triggers (pipeline, schedule, date, status) VALUES (?, ?, ?, ?)`,
		key.Pipeline, key.Schedule, key.Date, status,
	)
	if err != nil {
		return fmt.Errorf("write trigger status: %w", err)
	}
	return nil
}

// WriteEvent inserts a chaos event into the database.
func (s *SQLiteState) WriteEvent(ctx context.Context, event types.ChaosEvent) error {
	paramsBytes, err := json.Marshal(event.Params)
	if err != nil {
		return fmt.Errorf("write event: marshal params: %w", err)
	}
	paramsJSON := string(paramsBytes)
	if paramsJSON == "null" {
		paramsJSON = "{}"
	}

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO chaos_events (id, experiment_id, scenario, category, severity, target, mutation, params, timestamp, mode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		event.ID, event.ExperimentID, event.Scenario, event.Category,
		int(event.Severity), event.Target, event.Mutation, paramsJSON,
		event.Timestamp.Format(time.RFC3339Nano), event.Mode,
	)
	if err != nil {
		return fmt.Errorf("write event: %w", err)
	}
	return nil
}

// ReadChaosEvents returns all chaos events for the given experiment ID,
// ordered by timestamp. If no events exist, a non-nil empty slice is returned.
func (s *SQLiteState) ReadChaosEvents(ctx context.Context, experimentID string) ([]types.ChaosEvent, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, experiment_id, scenario, category, severity, target, mutation, params, timestamp, mode FROM chaos_events WHERE experiment_id = ? ORDER BY timestamp`,
		experimentID,
	)
	if err != nil {
		return nil, fmt.Errorf("read chaos events: %w", err)
	}
	defer rows.Close()

	events := make([]types.ChaosEvent, 0)
	for rows.Next() {
		var (
			e         types.ChaosEvent
			severity  int
			paramsJSON string
			ts        string
		)
		if err := rows.Scan(
			&e.ID, &e.ExperimentID, &e.Scenario, &e.Category,
			&severity, &e.Target, &e.Mutation, &paramsJSON, &ts, &e.Mode,
		); err != nil {
			return nil, fmt.Errorf("read chaos events: scan: %w", err)
		}

		e.Severity = types.Severity(severity)

		if ts != "" {
			t, err := time.Parse(time.RFC3339Nano, ts)
			if err != nil {
				return nil, fmt.Errorf("read chaos events: parse timestamp: %w", err)
			}
			e.Timestamp = t
		}

		if paramsJSON != "" && paramsJSON != "{}" {
			if err := json.Unmarshal([]byte(paramsJSON), &e.Params); err != nil {
				return nil, fmt.Errorf("read chaos events: parse params: %w", err)
			}
		}

		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read chaos events: iterate: %w", err)
	}

	return events, nil
}

// WritePipelineConfig stores a pipeline configuration blob.
func (s *SQLiteState) WritePipelineConfig(ctx context.Context, pipeline string, config []byte) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO pipeline_configs (pipeline, config) VALUES (?, ?)`,
		pipeline, string(config),
	)
	if err != nil {
		return fmt.Errorf("write pipeline config: %w", err)
	}
	return nil
}

// ReadPipelineConfig retrieves a pipeline configuration blob.
// If no row exists for the given pipeline, nil is returned without error.
func (s *SQLiteState) ReadPipelineConfig(ctx context.Context, pipeline string) ([]byte, error) {
	var config string
	row := s.db.QueryRowContext(ctx,
		`SELECT config FROM pipeline_configs WHERE pipeline = ?`,
		pipeline,
	)
	if err := row.Scan(&config); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("read pipeline config: %w", err)
	}
	return []byte(config), nil
}

// DeleteByPrefix removes all state entries whose pipeline matches the given
// prefix. The operation is wrapped in a transaction for atomicity. Tables
// without a pipeline column (e.g. chaos_events) are skipped.
func (s *SQLiteState) DeleteByPrefix(ctx context.Context, prefix string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("delete by prefix: begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // rollback after commit is a no-op

	stmts := []string{
		`DELETE FROM sensors WHERE pipeline LIKE ? || '%'`,
		`DELETE FROM triggers WHERE pipeline LIKE ? || '%'`,
		`DELETE FROM pipeline_configs WHERE pipeline LIKE ? || '%'`,
		`DELETE FROM reruns WHERE pipeline LIKE ? || '%'`,
		`DELETE FROM job_events WHERE pipeline LIKE ? || '%'`,
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt, prefix); err != nil {
			return fmt.Errorf("delete by prefix: %w", err)
		}
	}

	return tx.Commit()
}

// CountReruns returns the number of reruns for a pipeline/schedule/date.
func (s *SQLiteState) CountReruns(ctx context.Context, pipeline, schedule, date string) (int, error) {
	var count int
	row := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM reruns WHERE pipeline = ? AND schedule = ? AND date = ?`,
		pipeline, schedule, date,
	)
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("count reruns: %w", err)
	}
	return count, nil
}

// WriteRerun records a rerun event.
func (s *SQLiteState) WriteRerun(ctx context.Context, pipeline, schedule, date, reason string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO reruns (pipeline, schedule, date, reason, created_at) VALUES (?, ?, ?, ?, ?)`,
		pipeline, schedule, date, reason, time.Now().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("write rerun: %w", err)
	}
	return nil
}

// ReadJobEvents returns job events for a pipeline/schedule/date, ordered by
// timestamp descending (most recent first). If no events exist, a non-nil
// empty slice is returned.
func (s *SQLiteState) ReadJobEvents(ctx context.Context, pipeline, schedule, date string) ([]adapter.JobEvent, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT pipeline, schedule, date, event, run_id, timestamp FROM job_events WHERE pipeline = ? AND schedule = ? AND date = ? ORDER BY timestamp DESC`,
		pipeline, schedule, date,
	)
	if err != nil {
		return nil, fmt.Errorf("read job events: %w", err)
	}
	defer rows.Close()

	events := make([]adapter.JobEvent, 0)
	for rows.Next() {
		var (
			je adapter.JobEvent
			ts string
		)
		if err := rows.Scan(&je.Pipeline, &je.Schedule, &je.Date, &je.Event, &je.RunID, &ts); err != nil {
			return nil, fmt.Errorf("read job events: scan: %w", err)
		}
		if ts != "" {
			t, err := time.Parse(time.RFC3339Nano, ts)
			if err != nil {
				return nil, fmt.Errorf("read job events: parse timestamp: %w", err)
			}
			je.Timestamp = t
		}
		events = append(events, je)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read job events: iterate: %w", err)
	}
	return events, nil
}
