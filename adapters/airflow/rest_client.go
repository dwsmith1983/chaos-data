package airflow

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type restClient struct {
	baseURL string
	headers map[string]string
	client  *http.Client
}

// NewRESTClient creates an AirflowAPI backed by HTTP calls to the Airflow REST API v1.
func NewRESTClient(cfg Config) AirflowAPI {
	return &restClient{
		baseURL: strings.TrimRight(cfg.URL, "/"),
		headers: cfg.Headers,
		client:  &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *restClient) do(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("airflow: GET %s: %w", path, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("airflow: read response: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrAirflowNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("airflow: GET %s: HTTP %d: %s", path, resp.StatusCode, string(body))
	}

	return body, nil
}

func (c *restClient) GetDAGRun(ctx context.Context, dagID string) (DAGRunState, error) {
	path := fmt.Sprintf("/dags/%s/dagRuns?order_by=-execution_date&limit=1", dagID)

	body, err := c.do(ctx, path)
	if err != nil {
		return DAGRunState{}, err
	}

	var resp struct {
		DagRuns []struct {
			DagID    string `json:"dag_id"`
			DagRunID string `json:"dag_run_id"`
			State    string `json:"state"`
		} `json:"dag_runs"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return DAGRunState{}, fmt.Errorf("airflow: parse dag runs: %w", err)
	}

	if len(resp.DagRuns) == 0 {
		return DAGRunState{}, ErrAirflowNotFound
	}

	run := resp.DagRuns[0]
	return DAGRunState{
		DagID:  run.DagID,
		RunID:  run.DagRunID,
		Status: DAGRunStatus(run.State),
	}, nil
}

func (c *restClient) GetDAG(ctx context.Context, dagID string) (DAGState, error) {
	path := fmt.Sprintf("/dags/%s", dagID)

	body, err := c.do(ctx, path)
	if err != nil {
		return DAGState{}, err
	}

	var resp struct {
		DagID    string `json:"dag_id"`
		IsPaused bool   `json:"is_paused"`
		IsActive bool   `json:"is_active"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return DAGState{}, fmt.Errorf("airflow: parse dag: %w", err)
	}

	return DAGState{
		DagID:    resp.DagID,
		IsPaused: resp.IsPaused,
		IsActive: resp.IsActive,
	}, nil
}

func (c *restClient) GetTaskInstance(ctx context.Context, dagID, taskID string) (TaskInstanceState, error) {
	run, err := c.GetDAGRun(ctx, dagID)
	if err != nil {
		return TaskInstanceState{}, err
	}

	path := fmt.Sprintf("/dags/%s/dagRuns/%s/taskInstances/%s", dagID, run.RunID, taskID)

	body, err := c.do(ctx, path)
	if err != nil {
		return TaskInstanceState{}, err
	}

	var resp struct {
		DagID    string `json:"dag_id"`
		TaskID   string `json:"task_id"`
		DagRunID string `json:"dag_run_id"`
		State    string `json:"state"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return TaskInstanceState{}, fmt.Errorf("airflow: parse task instance: %w", err)
	}

	return TaskInstanceState{
		DagID:  resp.DagID,
		TaskID: resp.TaskID,
		RunID:  resp.DagRunID,
		Status: TaskInstanceStatus(resp.State),
	}, nil
}
