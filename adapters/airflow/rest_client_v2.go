package airflow

import (
	"context"
	"net/http"
	"strings"
	"time"
)

type restClientV2 struct {
	baseURL  string
	headers  map[string]string
	username string
	password string
	client   *http.Client
}

// NewRESTClientV2 creates an AirflowAPI backed by Airflow 3.x REST API v2.
func NewRESTClientV2(cfg Config) AirflowAPI {
	return &restClientV2{
		baseURL:  strings.TrimRight(cfg.URL, "/"),
		headers:  cfg.Headers,
		username: cfg.Username,
		password: cfg.Password,
		client:   &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *restClientV2) GetDAGRun(_ context.Context, _ string) (DAGRunState, error) {
	return DAGRunState{}, nil
}

func (c *restClientV2) GetDAG(_ context.Context, _ string) (DAGState, error) {
	return DAGState{}, nil
}

func (c *restClientV2) GetTaskInstance(_ context.Context, _, _ string) (TaskInstanceState, error) {
	return TaskInstanceState{}, nil
}
