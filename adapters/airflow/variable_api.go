package airflow

import (
	"context"
	"errors"
)

// AirflowVariableAPI abstracts the Airflow Variables REST API for testability.
// It is intentionally separate from AirflowAPI (interface segregation): the
// asserter reads orchestration state, the state store persists chaos bookkeeping.
type AirflowVariableAPI interface {
	// GetVariable returns a single Airflow Variable by key.
	// Returns ErrVariableNotFound when the key does not exist.
	GetVariable(ctx context.Context, key string) (Variable, error)

	// SetVariable creates or updates an Airflow Variable.
	SetVariable(ctx context.Context, v Variable) error

	// DeleteVariable removes an Airflow Variable by key.
	// Returns nil (not an error) when the key does not exist (idempotent).
	DeleteVariable(ctx context.Context, key string) error

	// ListVariables returns all Airflow Variables. The client handles
	// pagination internally.
	ListVariables(ctx context.Context) ([]Variable, error)
}

// Variable represents a single Airflow Variable.
type Variable struct {
	Key         string `json:"key"`
	Value       string `json:"value,omitempty"`
	Description string `json:"description,omitempty"`
}

// ErrVariableNotFound is returned when a requested Airflow Variable does not exist.
var ErrVariableNotFound = errors.New("airflow: variable not found")
