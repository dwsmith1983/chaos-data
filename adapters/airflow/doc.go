// Package airflow provides a chaos-data Asserter implementation that evaluates
// assertions against Airflow's REST API v1. It supports DAG run state
// (job_state), DAG paused/active state (trigger_state), and task instance
// state including sensors (sensor_state) without mutating Airflow's state.
package airflow
