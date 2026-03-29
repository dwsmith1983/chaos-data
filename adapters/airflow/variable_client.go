package airflow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// variableClient implements AirflowVariableAPI via the Airflow REST API.
type variableClient struct {
	baseURL string
	headers map[string]string
	client  *http.Client
}

// NewVariableClient creates an AirflowVariableAPI backed by HTTP calls to the
// Airflow REST API. It reuses the same Config (URL, Headers, Version) as
// NewRESTClient.
func NewVariableClient(cfg Config) AirflowVariableAPI {
	return &variableClient{
		baseURL: strings.TrimRight(cfg.URL, "/"),
		headers: cfg.Headers,
		client:  &http.Client{Timeout: 10 * time.Second},
	}
}

// do executes an HTTP request and returns the response body. It handles
// header injection, error wrapping, and 404 detection.
func (c *variableClient) do(ctx context.Context, method, path string, body io.Reader) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return nil, 0, fmt.Errorf("airflow: create request: %w", err)
	}
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("airflow: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("airflow: read response: %w", err)
	}

	return respBody, resp.StatusCode, nil
}

// GetVariable returns a single Airflow Variable by key.
func (c *variableClient) GetVariable(ctx context.Context, key string) (Variable, error) {
	body, status, err := c.do(ctx, http.MethodGet, "/variables/"+url.PathEscape(key), nil)
	if err != nil {
		return Variable{}, err
	}
	if status == http.StatusNotFound {
		return Variable{}, ErrVariableNotFound
	}
	if status < 200 || status >= 300 {
		return Variable{}, fmt.Errorf("airflow: GET /variables/%s: HTTP %d: %s", key, status, string(body))
	}

	var v Variable
	if err := json.Unmarshal(body, &v); err != nil {
		return Variable{}, fmt.Errorf("airflow: parse variable: %w", err)
	}
	return v, nil
}

// SetVariable creates or updates an Airflow Variable. It attempts a POST
// (create) first; on conflict (409) it falls back to PATCH (update).
func (c *variableClient) SetVariable(ctx context.Context, v Variable) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("airflow: marshal variable: %w", err)
	}

	body, status, err := c.do(ctx, http.MethodPost, "/variables", bytes.NewReader(payload))
	if err != nil {
		return err
	}

	// 409 Conflict means the variable already exists — update via PATCH.
	if status == http.StatusConflict {
		body, status, err = c.do(ctx, http.MethodPatch, "/variables/"+url.PathEscape(v.Key), bytes.NewReader(payload))
		if err != nil {
			return err
		}
	}

	if status < 200 || status >= 300 {
		return fmt.Errorf("airflow: set variable %q: HTTP %d: %s", v.Key, status, string(body))
	}
	return nil
}

// DeleteVariable removes an Airflow Variable by key. Returns nil when the key
// does not exist (idempotent delete).
func (c *variableClient) DeleteVariable(ctx context.Context, key string) error {
	body, status, err := c.do(ctx, http.MethodDelete, "/variables/"+url.PathEscape(key), nil)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		return nil // idempotent
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("airflow: DELETE /variables/%s: HTTP %d: %s", key, status, string(body))
	}
	return nil
}

// listPage is the Airflow REST API response shape for GET /variables.
type listPage struct {
	Variables  []Variable `json:"variables"`
	TotalCount int        `json:"total_entries"`
}

// ListVariables returns all Airflow Variables, paginating internally with
// limit=100 per page.
func (c *variableClient) ListVariables(ctx context.Context) ([]Variable, error) {
	const pageSize = 100
	var all []Variable

	for offset := 0; ; offset += pageSize {
		path := fmt.Sprintf("/variables?limit=%d&offset=%d", pageSize, offset)
		body, status, err := c.do(ctx, http.MethodGet, path, nil)
		if err != nil {
			return nil, err
		}
		if status < 200 || status >= 300 {
			return nil, fmt.Errorf("airflow: GET %s: HTTP %d: %s", path, status, string(body))
		}

		var page listPage
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, fmt.Errorf("airflow: parse variables list: %w", err)
		}

		all = append(all, page.Variables...)

		if len(all) >= page.TotalCount || len(page.Variables) == 0 {
			break
		}
	}

	return all, nil
}
