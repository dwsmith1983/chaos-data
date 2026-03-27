package airflow_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/dwsmith1983/chaos-data/adapters/airflow"
)

// ---------------------------------------------------------------------------
// JWT tests
// ---------------------------------------------------------------------------

func TestV2_JWT_LazyFetch(t *testing.T) {
	t.Parallel()

	var authCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPost && r.URL.Path == "/auth/token" {
			authCalls.Add(1)
			fmt.Fprint(w, `{"access_token":"tok_123"}`)
			return
		}
		// API response
		fmt.Fprint(w, `{"dag_runs":[{"dag_id":"d","dag_run_id":"r","state":"success"}]}`)
	}))
	defer ts.Close()

	client := airflow.NewRESTClientV2(airflow.Config{
		URL:      ts.URL,
		Version:  "v2",
		Username: "admin",
		Password: "admin",
	})

	// First call should trigger auth
	_, err := client.GetDAGRun(context.Background(), "d")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := authCalls.Load(); got != 1 {
		t.Errorf("auth calls after first request = %d, want 1", got)
	}

	// Second call should reuse token (no additional auth call)
	_, err = client.GetDAGRun(context.Background(), "d")
	if err != nil {
		t.Fatalf("unexpected error on second call: %v", err)
	}
	if got := authCalls.Load(); got != 1 {
		t.Errorf("auth calls after second request = %d, want 1 (should reuse token)", got)
	}
}

func TestV2_JWT_401_RefetchAndRetry(t *testing.T) {
	t.Parallel()

	var authCalls atomic.Int32
	var apiCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPost && r.URL.Path == "/auth/token" {
			authCalls.Add(1)
			fmt.Fprint(w, `{"access_token":"tok_fresh"}`)
			return
		}

		call := apiCalls.Add(1)
		if call == 1 {
			// First API attempt returns 401
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `{"detail":"token expired"}`)
			return
		}
		// Retry succeeds
		fmt.Fprint(w, `{"dag_runs":[{"dag_id":"d","dag_run_id":"r","state":"success"}]}`)
	}))
	defer ts.Close()

	client := airflow.NewRESTClientV2(airflow.Config{
		URL:      ts.URL,
		Version:  "v2",
		Username: "admin",
		Password: "admin",
	})

	run, err := client.GetDAGRun(context.Background(), "d")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if run.DagID != "d" {
		t.Errorf("DagID = %q, want %q", run.DagID, "d")
	}
	// Should have fetched token twice: initial lazy fetch + re-fetch after 401
	if got := authCalls.Load(); got != 2 {
		t.Errorf("auth calls = %d, want 2", got)
	}
}

func TestV2_JWT_AuthHeaderSent(t *testing.T) {
	t.Parallel()

	var gotAuth string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPost && r.URL.Path == "/auth/token" {
			fmt.Fprint(w, `{"access_token":"my_jwt_token"}`)
			return
		}
		gotAuth = r.Header.Get("Authorization")
		fmt.Fprint(w, `{"dag_runs":[{"dag_id":"d","dag_run_id":"r","state":"success"}]}`)
	}))
	defer ts.Close()

	client := airflow.NewRESTClientV2(airflow.Config{
		URL:      ts.URL,
		Version:  "v2",
		Username: "admin",
		Password: "admin",
	})

	_, err := client.GetDAGRun(context.Background(), "d")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotAuth != "Bearer my_jwt_token" {
		t.Errorf("Authorization = %q, want %q", gotAuth, "Bearer my_jwt_token")
	}
}

func TestV2_JWT_CustomHeadersOverride(t *testing.T) {
	t.Parallel()

	var authCalls atomic.Int32
	var gotAuth string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPost && r.URL.Path == "/auth/token" {
			authCalls.Add(1)
			fmt.Fprint(w, `{"access_token":"should_not_be_used"}`)
			return
		}
		gotAuth = r.Header.Get("Authorization")
		fmt.Fprint(w, `{"dag_runs":[{"dag_id":"d","dag_run_id":"r","state":"success"}]}`)
	}))
	defer ts.Close()

	client := airflow.NewRESTClientV2(airflow.Config{
		URL:     ts.URL,
		Version: "v2",
		Headers: map[string]string{"Authorization": "Bearer external-token"},
	})

	_, err := client.GetDAGRun(context.Background(), "d")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := authCalls.Load(); got != 0 {
		t.Errorf("auth calls = %d, want 0 (custom header should bypass JWT)", got)
	}
	if gotAuth != "Bearer external-token" {
		t.Errorf("Authorization = %q, want %q", gotAuth, "Bearer external-token")
	}
}

// ---------------------------------------------------------------------------
// GetDAGRun tests
// ---------------------------------------------------------------------------

func TestV2_GetDAGRun_ValidResponse(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPost && r.URL.Path == "/auth/token" {
			fmt.Fprint(w, `{"access_token":"tok"}`)
			return
		}
		fmt.Fprint(w, `{"dag_runs":[{"dag_id":"d","dag_run_id":"r","state":"success"}]}`)
	}))
	defer ts.Close()

	client := airflow.NewRESTClientV2(airflow.Config{
		URL:      ts.URL,
		Version:  "v2",
		Username: "admin",
		Password: "admin",
	})

	run, err := client.GetDAGRun(context.Background(), "d")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if run.DagID != "d" {
		t.Errorf("DagID = %q, want %q", run.DagID, "d")
	}
	if run.RunID != "r" {
		t.Errorf("RunID = %q, want %q", run.RunID, "r")
	}
	if run.Status != airflow.DAGRunSuccess {
		t.Errorf("Status = %q, want %q", run.Status, airflow.DAGRunSuccess)
	}
}

func TestV2_GetDAGRun_URLPath(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPost && r.URL.Path == "/auth/token" {
			fmt.Fprint(w, `{"access_token":"tok"}`)
			return
		}
		if r.URL.Path != "/api/v2/dags/my_dag/dagRuns" {
			t.Errorf("URL.Path = %q, want %q", r.URL.Path, "/api/v2/dags/my_dag/dagRuns")
		}
		if got := r.URL.Query().Get("order_by"); got != "-logical_date" {
			t.Errorf("order_by = %q, want %q", got, "-logical_date")
		}
		if got := r.URL.Query().Get("limit"); got != "1" {
			t.Errorf("limit = %q, want %q", got, "1")
		}
		fmt.Fprint(w, `{"dag_runs":[{"dag_id":"my_dag","dag_run_id":"run_1","state":"success"}]}`)
	}))
	defer ts.Close()

	client := airflow.NewRESTClientV2(airflow.Config{
		URL:      ts.URL,
		Version:  "v2",
		Username: "admin",
		Password: "admin",
	})

	_, err := client.GetDAGRun(context.Background(), "my_dag")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestV2_GetDAGRun_EmptyRuns_NotFound(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPost && r.URL.Path == "/auth/token" {
			fmt.Fprint(w, `{"access_token":"tok"}`)
			return
		}
		fmt.Fprint(w, `{"dag_runs":[]}`)
	}))
	defer ts.Close()

	client := airflow.NewRESTClientV2(airflow.Config{
		URL:      ts.URL,
		Version:  "v2",
		Username: "admin",
		Password: "admin",
	})

	_, err := client.GetDAGRun(context.Background(), "d")
	if !errors.Is(err, airflow.ErrAirflowNotFound) {
		t.Fatalf("err = %v, want ErrAirflowNotFound", err)
	}
}

// ---------------------------------------------------------------------------
// GetDAG tests
// ---------------------------------------------------------------------------

func TestV2_GetDAG_IsStale_False(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPost && r.URL.Path == "/auth/token" {
			fmt.Fprint(w, `{"access_token":"tok"}`)
			return
		}
		fmt.Fprint(w, `{"dag_id":"d","is_paused":false,"is_stale":false}`)
	}))
	defer ts.Close()

	client := airflow.NewRESTClientV2(airflow.Config{
		URL:      ts.URL,
		Version:  "v2",
		Username: "admin",
		Password: "admin",
	})

	dag, err := client.GetDAG(context.Background(), "d")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dag.IsActive {
		t.Error("IsActive = false, want true (is_stale=false)")
	}
	if dag.IsPaused {
		t.Error("IsPaused = true, want false")
	}
}

func TestV2_GetDAG_IsStale_True(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPost && r.URL.Path == "/auth/token" {
			fmt.Fprint(w, `{"access_token":"tok"}`)
			return
		}
		fmt.Fprint(w, `{"dag_id":"d","is_paused":false,"is_stale":true}`)
	}))
	defer ts.Close()

	client := airflow.NewRESTClientV2(airflow.Config{
		URL:      ts.URL,
		Version:  "v2",
		Username: "admin",
		Password: "admin",
	})

	dag, err := client.GetDAG(context.Background(), "d")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dag.IsActive {
		t.Error("IsActive = true, want false (is_stale=true)")
	}
}

func TestV2_GetDAG_IsPaused(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPost && r.URL.Path == "/auth/token" {
			fmt.Fprint(w, `{"access_token":"tok"}`)
			return
		}
		fmt.Fprint(w, `{"dag_id":"d","is_paused":true,"is_stale":false}`)
	}))
	defer ts.Close()

	client := airflow.NewRESTClientV2(airflow.Config{
		URL:      ts.URL,
		Version:  "v2",
		Username: "admin",
		Password: "admin",
	})

	dag, err := client.GetDAG(context.Background(), "d")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dag.IsPaused {
		t.Error("IsPaused = false, want true")
	}
}

// ---------------------------------------------------------------------------
// GetTaskInstance tests
// ---------------------------------------------------------------------------

func TestV2_GetTaskInstance_ValidResponse(t *testing.T) {
	t.Parallel()

	var apiCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPost && r.URL.Path == "/auth/token" {
			fmt.Fprint(w, `{"access_token":"tok"}`)
			return
		}

		call := apiCalls.Add(1)
		switch call {
		case 1: // GetDAGRun
			fmt.Fprint(w, `{"dag_runs":[{"dag_id":"my_dag","dag_run_id":"run_1","state":"success"}]}`)
		case 2: // task instance
			fmt.Fprint(w, `{"dag_id":"my_dag","task_id":"my_task","dag_run_id":"run_1","state":"success"}`)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	client := airflow.NewRESTClientV2(airflow.Config{
		URL:      ts.URL,
		Version:  "v2",
		Username: "admin",
		Password: "admin",
	})

	ti, err := client.GetTaskInstance(context.Background(), "my_dag", "my_task")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ti.DagID != "my_dag" {
		t.Errorf("DagID = %q, want %q", ti.DagID, "my_dag")
	}
	if ti.TaskID != "my_task" {
		t.Errorf("TaskID = %q, want %q", ti.TaskID, "my_task")
	}
	if ti.RunID != "run_1" {
		t.Errorf("RunID = %q, want %q", ti.RunID, "run_1")
	}
	if ti.Status != airflow.TaskSuccess {
		t.Errorf("Status = %q, want %q", ti.Status, airflow.TaskSuccess)
	}
}

func TestV2_GetTaskInstance_NullState(t *testing.T) {
	t.Parallel()

	var apiCalls atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPost && r.URL.Path == "/auth/token" {
			fmt.Fprint(w, `{"access_token":"tok"}`)
			return
		}

		call := apiCalls.Add(1)
		switch call {
		case 1: // GetDAGRun
			fmt.Fprint(w, `{"dag_runs":[{"dag_id":"my_dag","dag_run_id":"run_1","state":"running"}]}`)
		case 2: // task instance with null state
			fmt.Fprint(w, `{"dag_id":"my_dag","task_id":"my_task","dag_run_id":"run_1","state":null}`)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	client := airflow.NewRESTClientV2(airflow.Config{
		URL:      ts.URL,
		Version:  "v2",
		Username: "admin",
		Password: "admin",
	})

	ti, err := client.GetTaskInstance(context.Background(), "my_dag", "my_task")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ti.Status != "" {
		t.Errorf("Status = %q, want empty string for null state", ti.Status)
	}
}

// ---------------------------------------------------------------------------
// Error tests
// ---------------------------------------------------------------------------

func TestV2_404_ReturnsNotFound(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPost && r.URL.Path == "/auth/token" {
			fmt.Fprint(w, `{"access_token":"tok"}`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	client := airflow.NewRESTClientV2(airflow.Config{
		URL:      ts.URL,
		Version:  "v2",
		Username: "admin",
		Password: "admin",
	})

	_, err := client.GetDAGRun(context.Background(), "d")
	if !errors.Is(err, airflow.ErrAirflowNotFound) {
		t.Fatalf("err = %v, want ErrAirflowNotFound", err)
	}
}

func TestV2_422_ReturnsError(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPost && r.URL.Path == "/auth/token" {
			fmt.Fprint(w, `{"access_token":"tok"}`)
			return
		}
		w.WriteHeader(http.StatusUnprocessableEntity)
		fmt.Fprint(w, `{"detail":"validation error"}`)
	}))
	defer ts.Close()

	client := airflow.NewRESTClientV2(airflow.Config{
		URL:      ts.URL,
		Version:  "v2",
		Username: "admin",
		Password: "admin",
	})

	_, err := client.GetDAGRun(context.Background(), "d")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if errors.Is(err, airflow.ErrAirflowNotFound) {
		t.Fatal("422 should not return ErrAirflowNotFound")
	}
	if got := err.Error(); !strings.Contains(got, "422") {
		t.Errorf("error = %q, want it to contain %q", got, "422")
	}
}

func TestV2_NonOK_ReturnsError(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPost && r.URL.Path == "/auth/token" {
			fmt.Fprint(w, `{"access_token":"tok"}`)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `{"detail":"internal error"}`)
	}))
	defer ts.Close()

	client := airflow.NewRESTClientV2(airflow.Config{
		URL:      ts.URL,
		Version:  "v2",
		Username: "admin",
		Password: "admin",
	})

	_, err := client.GetDAGRun(context.Background(), "d")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if got := err.Error(); !strings.Contains(got, "500") {
		t.Errorf("error = %q, want it to contain %q", got, "500")
	}
}
