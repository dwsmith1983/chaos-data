package dagster_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dwsmith1983/chaos-data/adapters/dagster"
)

// sensorResponse builds a JSON body that the fake dagit server returns for
// a sensor query. status is the sensorState.status value and tickStatus is
// the single tick's status (empty string means no ticks returned).
func sensorJSON(typename, sensorName, instigationStatus, tickStatus string) string {
	ticks := "[]"
	if tickStatus != "" {
		ticks = `[{"status":"` + tickStatus + `"}]`
	}
	switch typename {
	case "Sensor":
		return `{"data":{"sensorOrError":{"__typename":"Sensor","name":"` + sensorName +
			`","sensorState":{"status":"` + instigationStatus + `","ticks":` + ticks + `}}}}`
	case "SensorNotFoundError":
		return `{"data":{"sensorOrError":{"__typename":"SensorNotFoundError","sensorName":"` + sensorName + `"}}}`
	default:
		return `{"data":{"sensorOrError":{"__typename":"` + typename + `"}}}`
	}
}

func scheduleJSON(typename, scheduleName, instigationStatus, tickStatus string) string {
	ticks := "[]"
	if tickStatus != "" {
		ticks = `[{"status":"` + tickStatus + `"}]`
	}
	switch typename {
	case "Schedule":
		return `{"data":{"scheduleOrError":{"__typename":"Schedule","name":"` + scheduleName +
			`","scheduleState":{"status":"` + instigationStatus + `","ticks":` + ticks + `}}}}`
	case "ScheduleNotFoundError":
		return `{"data":{"scheduleOrError":{"__typename":"ScheduleNotFoundError","scheduleName":"` + scheduleName + `"}}}`
	default:
		return `{"data":{"scheduleOrError":{"__typename":"` + typename + `"}}}`
	}
}

func runJSON(typename string, runID, status string) string {
	switch typename {
	case "Runs":
		if runID == "" {
			return `{"data":{"runsOrError":{"__typename":"Runs","results":[]}}}`
		}
		return `{"data":{"runsOrError":{"__typename":"Runs","results":[{"runId":"` + runID + `","status":"` + status + `"}]}}}`
	default:
		return `{"data":{"runsOrError":{"__typename":"` + typename + `"}}}`
	}
}

func graphqlErrorJSON(msg string) string {
	return `{"errors":[{"message":"` + msg + `"}]}`
}

// decodeRequestBody decodes the GraphQL POST body into query + variables.
func decodeRequestBody(t *testing.T, r *http.Request) (query string, vars map[string]any) {
	t.Helper()
	var body struct {
		Query     string         `json:"query"`
		Variables map[string]any `json:"variables"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		t.Fatalf("decodeRequestBody: %v", err)
	}
	return body.Query, body.Variables
}

// --- GetSensor tests ---

func TestGetSensor_ValidResponse(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(sensorJSON("Sensor", "my_sensor", "RUNNING", "SUCCESS")))
	}))
	defer ts.Close()

	client := dagster.NewGraphQLClient(dagster.Config{URL: ts.URL})
	got, err := client.GetSensor(context.Background(), "my_sensor")

	if err != nil {
		t.Fatalf("GetSensor() error = %v, want nil", err)
	}
	if got.Name != "my_sensor" {
		t.Errorf("GetSensor().Name = %q, want %q", got.Name, "my_sensor")
	}
	if got.InstigationStatus != dagster.InstigationRunning {
		t.Errorf("GetSensor().InstigationStatus = %q, want %q", got.InstigationStatus, dagster.InstigationRunning)
	}
	if got.LatestTick == nil {
		t.Fatal("GetSensor().LatestTick = nil, want non-nil")
	}
	if *got.LatestTick != dagster.TickSuccess {
		t.Errorf("GetSensor().LatestTick = %q, want %q", *got.LatestTick, dagster.TickSuccess)
	}
}

func TestGetSensor_NoTicks(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(sensorJSON("Sensor", "my_sensor", "STOPPED", "")))
	}))
	defer ts.Close()

	client := dagster.NewGraphQLClient(dagster.Config{URL: ts.URL})
	got, err := client.GetSensor(context.Background(), "my_sensor")

	if err != nil {
		t.Fatalf("GetSensor() error = %v, want nil", err)
	}
	if got.LatestTick != nil {
		t.Errorf("GetSensor().LatestTick = %v, want nil (no ticks)", got.LatestTick)
	}
	if got.InstigationStatus != dagster.InstigationStopped {
		t.Errorf("GetSensor().InstigationStatus = %q, want %q", got.InstigationStatus, dagster.InstigationStopped)
	}
}

func TestGetSensor_NotFound(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(sensorJSON("SensorNotFoundError", "missing_sensor", "", "")))
	}))
	defer ts.Close()

	client := dagster.NewGraphQLClient(dagster.Config{URL: ts.URL})
	_, err := client.GetSensor(context.Background(), "missing_sensor")

	if !errors.Is(err, dagster.ErrDagsterNotFound) {
		t.Errorf("GetSensor() error = %v, want %v", err, dagster.ErrDagsterNotFound)
	}
}

func TestGetSensor_AuthHeaderSent(t *testing.T) {
	t.Parallel()

	var gotToken string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotToken = r.Header.Get("Dagster-Cloud-Api-Token")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(sensorJSON("Sensor", "s", "RUNNING", "")))
	}))
	defer ts.Close()

	client := dagster.NewGraphQLClient(dagster.Config{
		URL:     ts.URL,
		Headers: map[string]string{"Dagster-Cloud-Api-Token": "secret-token-123"},
	})
	_, _ = client.GetSensor(context.Background(), "s")

	if gotToken != "secret-token-123" {
		t.Errorf("auth header Dagster-Cloud-Api-Token = %q, want %q", gotToken, "secret-token-123")
	}
}

func TestGetSensor_RepoScoping_IncludesFieldsWhenConfigured(t *testing.T) {
	t.Parallel()

	var capturedVars map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, vars := decodeRequestBody(t, r)
		capturedVars = vars
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(sensorJSON("Sensor", "s", "RUNNING", "")))
	}))
	defer ts.Close()

	client := dagster.NewGraphQLClient(dagster.Config{
		URL:                    ts.URL,
		RepositoryLocationName: "my_location",
		RepositoryName:         "my_repo",
	})
	_, _ = client.GetSensor(context.Background(), "s")

	selector, ok := capturedVars["selector"].(map[string]any)
	if !ok {
		t.Fatalf("variables[selector] is not a map: %v", capturedVars["selector"])
	}
	if selector["repositoryLocationName"] != "my_location" {
		t.Errorf("selector[repositoryLocationName] = %v, want %q", selector["repositoryLocationName"], "my_location")
	}
	if selector["repositoryName"] != "my_repo" {
		t.Errorf("selector[repositoryName] = %v, want %q", selector["repositoryName"], "my_repo")
	}
}

func TestGetSensor_RepoScoping_OmitsFieldsWhenNotConfigured(t *testing.T) {
	t.Parallel()

	var capturedVars map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, vars := decodeRequestBody(t, r)
		capturedVars = vars
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(sensorJSON("Sensor", "s", "RUNNING", "")))
	}))
	defer ts.Close()

	client := dagster.NewGraphQLClient(dagster.Config{URL: ts.URL})
	_, _ = client.GetSensor(context.Background(), "s")

	selector, ok := capturedVars["selector"].(map[string]any)
	if !ok {
		t.Fatalf("variables[selector] is not a map: %v", capturedVars["selector"])
	}
	if _, found := selector["repositoryLocationName"]; found {
		t.Error("selector should not contain repositoryLocationName when not configured")
	}
	if _, found := selector["repositoryName"]; found {
		t.Error("selector should not contain repositoryName when not configured")
	}
}

// --- GetSchedule tests ---

func TestGetSchedule_ValidResponse(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(scheduleJSON("Schedule", "my_schedule", "RUNNING", "SKIPPED")))
	}))
	defer ts.Close()

	client := dagster.NewGraphQLClient(dagster.Config{URL: ts.URL})
	got, err := client.GetSchedule(context.Background(), "my_schedule")

	if err != nil {
		t.Fatalf("GetSchedule() error = %v, want nil", err)
	}
	if got.Name != "my_schedule" {
		t.Errorf("GetSchedule().Name = %q, want %q", got.Name, "my_schedule")
	}
	if got.InstigationStatus != dagster.InstigationRunning {
		t.Errorf("GetSchedule().InstigationStatus = %q, want %q", got.InstigationStatus, dagster.InstigationRunning)
	}
	if got.LatestTick == nil {
		t.Fatal("GetSchedule().LatestTick = nil, want non-nil")
	}
	if *got.LatestTick != dagster.TickSkipped {
		t.Errorf("GetSchedule().LatestTick = %q, want %q", *got.LatestTick, dagster.TickSkipped)
	}
}

func TestGetSchedule_NotFound(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(scheduleJSON("ScheduleNotFoundError", "missing", "", "")))
	}))
	defer ts.Close()

	client := dagster.NewGraphQLClient(dagster.Config{URL: ts.URL})
	_, err := client.GetSchedule(context.Background(), "missing")

	if !errors.Is(err, dagster.ErrDagsterNotFound) {
		t.Errorf("GetSchedule() error = %v, want %v", err, dagster.ErrDagsterNotFound)
	}
}

func TestGetSchedule_RepoScoping_IncludesFieldsWhenConfigured(t *testing.T) {
	t.Parallel()

	var capturedVars map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, vars := decodeRequestBody(t, r)
		capturedVars = vars
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(scheduleJSON("Schedule", "s", "RUNNING", "")))
	}))
	defer ts.Close()

	client := dagster.NewGraphQLClient(dagster.Config{
		URL:                    ts.URL,
		RepositoryLocationName: "prod_location",
		RepositoryName:         "prod_repo",
	})
	_, _ = client.GetSchedule(context.Background(), "s")

	selector, ok := capturedVars["selector"].(map[string]any)
	if !ok {
		t.Fatalf("variables[selector] is not a map: %v", capturedVars["selector"])
	}
	if selector["repositoryLocationName"] != "prod_location" {
		t.Errorf("selector[repositoryLocationName] = %v, want %q", selector["repositoryLocationName"], "prod_location")
	}
	if selector["repositoryName"] != "prod_repo" {
		t.Errorf("selector[repositoryName] = %v, want %q", selector["repositoryName"], "prod_repo")
	}
}

// --- GetRun tests ---

func TestGetRun_ValidResponse(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(runJSON("Runs", "run-abc-123", "SUCCESS")))
	}))
	defer ts.Close()

	client := dagster.NewGraphQLClient(dagster.Config{URL: ts.URL})
	got, err := client.GetRun(context.Background(), "my_job")

	if err != nil {
		t.Fatalf("GetRun() error = %v, want nil", err)
	}
	if got.RunID != "run-abc-123" {
		t.Errorf("GetRun().RunID = %q, want %q", got.RunID, "run-abc-123")
	}
	if got.Status != dagster.RunSuccess {
		t.Errorf("GetRun().Status = %q, want %q", got.Status, dagster.RunSuccess)
	}
}

func TestGetRun_NoRuns_ReturnsNotFound(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// Runs typename but with empty results slice
		_, _ = w.Write([]byte(runJSON("Runs", "", "")))
	}))
	defer ts.Close()

	client := dagster.NewGraphQLClient(dagster.Config{URL: ts.URL})
	_, err := client.GetRun(context.Background(), "nonexistent_job")

	if !errors.Is(err, dagster.ErrDagsterNotFound) {
		t.Errorf("GetRun() error = %v, want %v", err, dagster.ErrDagsterNotFound)
	}
}

func TestGetRun_JobNameSentAsVariable(t *testing.T) {
	t.Parallel()

	var capturedVars map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, vars := decodeRequestBody(t, r)
		capturedVars = vars
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(runJSON("Runs", "r1", "STARTED")))
	}))
	defer ts.Close()

	client := dagster.NewGraphQLClient(dagster.Config{URL: ts.URL})
	_, _ = client.GetRun(context.Background(), "target_job")

	if capturedVars["jobName"] != "target_job" {
		t.Errorf("variables[jobName] = %v, want %q", capturedVars["jobName"], "target_job")
	}
}

// --- Error handling tests ---

func TestGraphQLClient_GraphQLErrorResponse(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(graphqlErrorJSON("sensor selector is invalid")))
	}))
	defer ts.Close()

	client := dagster.NewGraphQLClient(dagster.Config{URL: ts.URL})
	_, err := client.GetSensor(context.Background(), "s")

	if err == nil {
		t.Fatal("GetSensor() = nil error, want error for GraphQL errors response")
	}
	if !strings.Contains(err.Error(), "sensor selector is invalid") {
		t.Errorf("GetSensor() error = %q, want it to contain the GraphQL error message", err.Error())
	}
}

func TestGraphQLClient_HTTPError(t *testing.T) {
	t.Parallel()

	// Use a server that immediately closes to simulate connection refused.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	ts.Close() // Close immediately so the URL is unreachable.

	client := dagster.NewGraphQLClient(dagster.Config{URL: ts.URL})
	_, err := client.GetSensor(context.Background(), "s")

	if err == nil {
		t.Fatal("GetSensor() = nil error, want error for unreachable server")
	}
}

func TestGraphQLClient_NonOKHTTPStatus(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte("unauthorized"))
	}))
	defer ts.Close()

	client := dagster.NewGraphQLClient(dagster.Config{URL: ts.URL})
	_, err := client.GetSensor(context.Background(), "s")

	if err == nil {
		t.Fatal("GetSensor() = nil error, want error for 401 response")
	}
}

func TestGetRun_FailureStatus(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(runJSON("Runs", "run-xyz", "FAILURE")))
	}))
	defer ts.Close()

	client := dagster.NewGraphQLClient(dagster.Config{URL: ts.URL})
	got, err := client.GetRun(context.Background(), "failing_job")

	if err != nil {
		t.Fatalf("GetRun() error = %v, want nil", err)
	}
	if got.Status != dagster.RunFailure {
		t.Errorf("GetRun().Status = %q, want %q", got.Status, dagster.RunFailure)
	}
}

func TestGetSensor_TickFailure(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(sensorJSON("Sensor", "s", "RUNNING", "FAILURE")))
	}))
	defer ts.Close()

	client := dagster.NewGraphQLClient(dagster.Config{URL: ts.URL})
	got, err := client.GetSensor(context.Background(), "s")

	if err != nil {
		t.Fatalf("GetSensor() error = %v, want nil", err)
	}
	if got.LatestTick == nil {
		t.Fatal("GetSensor().LatestTick = nil, want non-nil")
	}
	if *got.LatestTick != dagster.TickFailure {
		t.Errorf("GetSensor().LatestTick = %q, want %q", *got.LatestTick, dagster.TickFailure)
	}
}

func TestGetSchedule_NoTicks(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(scheduleJSON("Schedule", "s", "STOPPED", "")))
	}))
	defer ts.Close()

	client := dagster.NewGraphQLClient(dagster.Config{URL: ts.URL})
	got, err := client.GetSchedule(context.Background(), "s")

	if err != nil {
		t.Fatalf("GetSchedule() error = %v, want nil", err)
	}
	if got.LatestTick != nil {
		t.Errorf("GetSchedule().LatestTick = %v, want nil (no ticks)", got.LatestTick)
	}
}

func TestGetSensor_ContentTypeHeader(t *testing.T) {
	t.Parallel()

	var gotContentType string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotContentType = r.Header.Get("Content-Type")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(sensorJSON("Sensor", "s", "RUNNING", "")))
	}))
	defer ts.Close()

	client := dagster.NewGraphQLClient(dagster.Config{URL: ts.URL})
	_, _ = client.GetSensor(context.Background(), "s")

	if gotContentType != "application/json" {
		t.Errorf("Content-Type = %q, want %q", gotContentType, "application/json")
	}
}

func TestGetSensor_SelectorContainsSensorName(t *testing.T) {
	t.Parallel()

	var capturedVars map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, vars := decodeRequestBody(t, r)
		capturedVars = vars
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(sensorJSON("Sensor", "target_sensor", "RUNNING", "")))
	}))
	defer ts.Close()

	client := dagster.NewGraphQLClient(dagster.Config{URL: ts.URL})
	_, _ = client.GetSensor(context.Background(), "target_sensor")

	selector, ok := capturedVars["selector"].(map[string]any)
	if !ok {
		t.Fatalf("variables[selector] is not a map: %v", capturedVars["selector"])
	}
	if selector["sensorName"] != "target_sensor" {
		t.Errorf("selector[sensorName] = %v, want %q", selector["sensorName"], "target_sensor")
	}
}

func TestGetSchedule_SelectorContainsScheduleName(t *testing.T) {
	t.Parallel()

	var capturedVars map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, vars := decodeRequestBody(t, r)
		capturedVars = vars
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(scheduleJSON("Schedule", "target_schedule", "RUNNING", "")))
	}))
	defer ts.Close()

	client := dagster.NewGraphQLClient(dagster.Config{URL: ts.URL})
	_, _ = client.GetSchedule(context.Background(), "target_schedule")

	selector, ok := capturedVars["selector"].(map[string]any)
	if !ok {
		t.Fatalf("variables[selector] is not a map: %v", capturedVars["selector"])
	}
	if selector["scheduleName"] != "target_schedule" {
		t.Errorf("selector[scheduleName] = %v, want %q", selector["scheduleName"], "target_schedule")
	}
}

func TestGetRun_StartingStatus(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(runJSON("Runs", "run-starting", "STARTING")))
	}))
	defer ts.Close()

	client := dagster.NewGraphQLClient(dagster.Config{URL: ts.URL})
	got, err := client.GetRun(context.Background(), "my_job")

	if err != nil {
		t.Fatalf("GetRun() error = %v, want nil", err)
	}
	if got.Status != dagster.RunStarting {
		t.Errorf("GetRun().Status = %q, want %q", got.Status, dagster.RunStarting)
	}
}
