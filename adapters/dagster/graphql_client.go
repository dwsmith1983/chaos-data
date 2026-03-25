package dagster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	queryGetSensor = `query GetSensor($selector: SensorSelector!) {
  sensorOrError(sensorSelector: $selector) {
    __typename
    ... on Sensor {
      name
      sensorState {
        status
        ticks(limit: 1) {
          status
        }
      }
    }
    ... on SensorNotFoundError {
      sensorName
    }
  }
}`

	queryGetSchedule = `query GetSchedule($selector: ScheduleSelector!) {
  scheduleOrError(scheduleSelector: $selector) {
    __typename
    ... on Schedule {
      name
      scheduleState {
        status
        ticks(limit: 1) {
          status
        }
      }
    }
    ... on ScheduleNotFoundError {
      scheduleName
    }
  }
}`

	queryGetRun = `query GetRuns($jobName: String!) {
  runsOrError(filter: {pipelineName: $jobName}, limit: 1) {
    __typename
    ... on Runs {
      results {
        runId
        status
      }
    }
  }
}`
)

type graphqlClient struct {
	url              string
	headers          map[string]string
	repoLocationName string
	repoName         string
	client           *http.Client
}

// NewGraphQLClient constructs a DagsterAPI backed by the dagit GraphQL HTTP
// endpoint described in cfg.
func NewGraphQLClient(cfg Config) DagsterAPI {
	return &graphqlClient{
		url:              cfg.URL,
		headers:          cfg.Headers,
		repoLocationName: cfg.RepositoryLocationName,
		repoName:         cfg.RepositoryName,
		client:           &http.Client{Timeout: 10 * time.Second},
	}
}

// do POSTs a GraphQL request and returns the raw response body bytes.
func (g *graphqlClient) do(ctx context.Context, query string, vars map[string]any) ([]byte, error) {
	payload, err := json.Marshal(map[string]any{"query": query, "variables": vars})
	if err != nil {
		return nil, fmt.Errorf("dagster: marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.url, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("dagster: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range g.headers {
		req.Header.Set(k, v)
	}

	resp, err := g.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("dagster: http request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("dagster: read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("dagster: unexpected status %d: %s", resp.StatusCode, body)
	}

	return body, nil
}

// sensorSelector builds the selector variable map for a sensor query, including
// optional repository scoping when configured.
func (g *graphqlClient) sensorSelector(name string) map[string]any {
	sel := map[string]any{"sensorName": name}
	if g.repoLocationName != "" {
		sel["repositoryLocationName"] = g.repoLocationName
	}
	if g.repoName != "" {
		sel["repositoryName"] = g.repoName
	}
	return sel
}

// scheduleSelector builds the selector variable map for a schedule query, including
// optional repository scoping when configured.
func (g *graphqlClient) scheduleSelector(name string) map[string]any {
	sel := map[string]any{"scheduleName": name}
	if g.repoLocationName != "" {
		sel["repositoryLocationName"] = g.repoLocationName
	}
	if g.repoName != "" {
		sel["repositoryName"] = g.repoName
	}
	return sel
}

// graphqlErrors is the top-level errors array returned by dagit on failure.
type graphqlErrors []struct {
	Message string `json:"message"`
}

// checkErrors returns a non-nil error if the response body contains a
// top-level GraphQL errors array.
func checkErrors(body []byte) error {
	var envelope struct {
		Errors graphqlErrors `json:"errors"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil // not parseable as an errors envelope; handled downstream
	}
	if len(envelope.Errors) > 0 {
		return fmt.Errorf("dagster: graphql error: %s", envelope.Errors[0].Message)
	}
	return nil
}

// GetSensor implements DagsterAPI.
func (g *graphqlClient) GetSensor(ctx context.Context, name string) (SensorState, error) {
	vars := map[string]any{"selector": g.sensorSelector(name)}

	body, err := g.do(ctx, queryGetSensor, vars)
	if err != nil {
		return SensorState{}, err
	}
	if err := checkErrors(body); err != nil {
		return SensorState{}, err
	}

	var resp struct {
		Data struct {
			SensorOrError struct {
				Typename    string `json:"__typename"`
				Name        string `json:"name"`
				SensorState struct {
					Status string `json:"status"`
					Ticks  []struct {
						Status string `json:"status"`
					} `json:"ticks"`
				} `json:"sensorState"`
			} `json:"sensorOrError"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return SensorState{}, fmt.Errorf("dagster: parse sensor response: %w", err)
	}

	node := resp.Data.SensorOrError
	if node.Typename == "SensorNotFoundError" {
		return SensorState{}, ErrDagsterNotFound
	}
	if node.Typename != "Sensor" {
		return SensorState{}, fmt.Errorf("dagster: unexpected sensorOrError type %q", node.Typename)
	}

	state := SensorState{
		Name:              node.Name,
		InstigationStatus: InstigationStatus(node.SensorState.Status),
	}
	if len(node.SensorState.Ticks) > 0 {
		tick := TickStatus(node.SensorState.Ticks[0].Status)
		state.LatestTick = &tick
	}
	return state, nil
}

// GetSchedule implements DagsterAPI.
func (g *graphqlClient) GetSchedule(ctx context.Context, name string) (ScheduleState, error) {
	vars := map[string]any{"selector": g.scheduleSelector(name)}

	body, err := g.do(ctx, queryGetSchedule, vars)
	if err != nil {
		return ScheduleState{}, err
	}
	if err := checkErrors(body); err != nil {
		return ScheduleState{}, err
	}

	var resp struct {
		Data struct {
			ScheduleOrError struct {
				Typename      string `json:"__typename"`
				Name          string `json:"name"`
				ScheduleState struct {
					Status string `json:"status"`
					Ticks  []struct {
						Status string `json:"status"`
					} `json:"ticks"`
				} `json:"scheduleState"`
			} `json:"scheduleOrError"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return ScheduleState{}, fmt.Errorf("dagster: parse schedule response: %w", err)
	}

	node := resp.Data.ScheduleOrError
	if node.Typename == "ScheduleNotFoundError" {
		return ScheduleState{}, ErrDagsterNotFound
	}
	if node.Typename != "Schedule" {
		return ScheduleState{}, fmt.Errorf("dagster: unexpected scheduleOrError type %q", node.Typename)
	}

	state := ScheduleState{
		Name:              node.Name,
		InstigationStatus: InstigationStatus(node.ScheduleState.Status),
	}
	if len(node.ScheduleState.Ticks) > 0 {
		tick := TickStatus(node.ScheduleState.Ticks[0].Status)
		state.LatestTick = &tick
	}
	return state, nil
}

// GetRun implements DagsterAPI.
func (g *graphqlClient) GetRun(ctx context.Context, jobName string) (RunState, error) {
	vars := map[string]any{"jobName": jobName}

	body, err := g.do(ctx, queryGetRun, vars)
	if err != nil {
		return RunState{}, err
	}
	if err := checkErrors(body); err != nil {
		return RunState{}, err
	}

	var resp struct {
		Data struct {
			RunsOrError struct {
				Typename string `json:"__typename"`
				Results  []struct {
					RunID  string `json:"runId"`
					Status string `json:"status"`
				} `json:"results"`
			} `json:"runsOrError"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return RunState{}, fmt.Errorf("dagster: parse runs response: %w", err)
	}

	node := resp.Data.RunsOrError
	if node.Typename != "Runs" {
		return RunState{}, fmt.Errorf("dagster: unexpected runsOrError type %q", node.Typename)
	}
	if len(node.Results) == 0 {
		return RunState{}, ErrDagsterNotFound
	}

	run := node.Results[0]
	return RunState{
		RunID:  run.RunID,
		Status: RunStatus(run.Status),
	}, nil
}
