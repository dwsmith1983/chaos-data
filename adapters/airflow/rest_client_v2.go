package airflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type restClientV2 struct {
	baseURL  string
	headers  map[string]string
	username string
	password string
	token    string
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

func (c *restClientV2) fetchToken(ctx context.Context) error {
	body, err := json.Marshal(map[string]string{
		"username": c.username,
		"password": c.password,
	})
	if err != nil {
		return fmt.Errorf("airflow: marshal auth request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/auth/token", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("airflow: create auth request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("airflow: auth request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("airflow: read auth response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("airflow: auth failed: HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(respBody, &tokenResp); err != nil {
		return fmt.Errorf("airflow: parse auth response: %w", err)
	}
	if tokenResp.AccessToken == "" {
		return fmt.Errorf("airflow: empty access_token in auth response")
	}

	c.token = tokenResp.AccessToken
	return nil
}

func (c *restClientV2) ensureToken(ctx context.Context) error {
	if c.token != "" {
		return nil
	}
	return c.fetchToken(ctx)
}

// doOnce makes a single GET request without retry logic.
func (c *restClientV2) doOnce(ctx context.Context, path string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.baseURL+"/api/v2"+path, nil)
	if err != nil {
		return nil, 0, err
	}

	// Set JWT token if available.
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	// Apply configured headers (may override Authorization for custom auth).
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("airflow: GET %s: %w", path, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("airflow: read response: %w", err)
	}

	return body, resp.StatusCode, nil
}

// do makes a GET request with JWT auth. On 401, re-fetches token and retries once.
func (c *restClientV2) do(ctx context.Context, path string) ([]byte, error) {
	// Ensure we have a token (lazy fetch).
	if c.username != "" {
		if err := c.ensureToken(ctx); err != nil {
			return nil, err
		}
	}

	body, status, err := c.doOnce(ctx, path)
	if err != nil {
		return nil, err
	}

	// On 401, try re-fetching the token once and retry.
	if status == http.StatusUnauthorized && c.username != "" {
		if fetchErr := c.fetchToken(ctx); fetchErr != nil {
			return nil, fetchErr
		}
		body, status, err = c.doOnce(ctx, path)
		if err != nil {
			return nil, err
		}
	}

	if status == http.StatusNotFound {
		return nil, ErrAirflowNotFound
	}
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("airflow: GET %s: HTTP %d: %s", path, status, string(body))
	}
	return body, nil
}

// --- Response types (unexported, v2-specific) ---

type v2DagRunListResponse struct {
	DagRuns []struct {
		DagID    string `json:"dag_id"`
		DagRunID string `json:"dag_run_id"`
		State    string `json:"state"`
	} `json:"dag_runs"`
}

type v2DagResponse struct {
	DagID    string `json:"dag_id"`
	IsPaused bool   `json:"is_paused"`
	IsStale  bool   `json:"is_stale"`
}

type v2TaskInstanceResponse struct {
	DagID    string  `json:"dag_id"`
	TaskID   string  `json:"task_id"`
	DagRunID string  `json:"dag_run_id"`
	State    *string `json:"state"` // nullable in v2
}

// --- AirflowAPI implementation ---

func (c *restClientV2) GetDAGRun(ctx context.Context, dagID string) (DAGRunState, error) {
	path := fmt.Sprintf("/dags/%s/dagRuns?order_by=-logical_date&limit=1", dagID)
	body, err := c.do(ctx, path)
	if err != nil {
		return DAGRunState{}, err
	}

	var resp v2DagRunListResponse
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

func (c *restClientV2) GetDAG(ctx context.Context, dagID string) (DAGState, error) {
	path := fmt.Sprintf("/dags/%s", dagID)
	body, err := c.do(ctx, path)
	if err != nil {
		return DAGState{}, err
	}

	var resp v2DagResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return DAGState{}, fmt.Errorf("airflow: parse dag: %w", err)
	}
	return DAGState{
		DagID:    resp.DagID,
		IsPaused: resp.IsPaused,
		IsActive: !resp.IsStale, // v2: is_stale is inverted is_active
	}, nil
}

func (c *restClientV2) GetTaskInstance(ctx context.Context, dagID, taskID string) (TaskInstanceState, error) {
	run, err := c.GetDAGRun(ctx, dagID)
	if err != nil {
		return TaskInstanceState{}, err
	}

	path := fmt.Sprintf("/dags/%s/dagRuns/%s/taskInstances/%s", dagID, run.RunID, taskID)
	body, err := c.do(ctx, path)
	if err != nil {
		return TaskInstanceState{}, err
	}

	var resp v2TaskInstanceResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return TaskInstanceState{}, fmt.Errorf("airflow: parse task instance: %w", err)
	}

	status := TaskInstanceStatus("")
	if resp.State != nil {
		status = TaskInstanceStatus(*resp.State)
	}

	return TaskInstanceState{
		DagID:  resp.DagID,
		TaskID: resp.TaskID,
		RunID:  resp.DagRunID,
		Status: status,
	}, nil
}
