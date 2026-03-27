package local

// InsertJobEventForTest inserts a job event row directly into the database.
// It is exported only for tests (this file is compiled only during testing).
func (s *SQLiteState) InsertJobEventForTest(pipeline, schedule, date, event, runID, timestamp string) error {
	_, err := s.db.Exec(
		`INSERT INTO job_events (pipeline, schedule, date, event, run_id, timestamp) VALUES (?, ?, ?, ?, ?, ?)`,
		pipeline, schedule, date, event, runID, timestamp,
	)
	return err
}
