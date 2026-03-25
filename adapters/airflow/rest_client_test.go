package airflow_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dwsmith1983/chaos-data/adapters/airflow"
)

// ---------------------------------------------------------------------------
// GetDAGRun tests
// ---------------------------------------------------------------------------

func TestGetDAGRun_ValidResponse(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"dag_runs":[{"dag_id":"my_dag","dag_run_id":"run_1","state":"success"}]}`)
	}))
	defer ts.Close()

	client := airflow.NewRESTClient(airflow.Config{URL: ts.URL})

	run, err := client.GetDAGRun(context.Background(), "my_dag")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if run.DagID != "my_dag" {
		t.Errorf("DagID = %q, want %q", run.DagID, "my_dag")
	}
	if run.RunID != "run_1" {
		t.Errorf("RunID = %q, want %q", run.RunID, "run_1")
	}
	if run.Status != airflow.DAGRunSuccess {
		t.Errorf("Status = %q, want %q", run.Status, airflow.DAGRunSuccess)
	}
}

func TestGetDAGRun_EmptyRuns_ReturnsNotFound(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"dag_runs":[]}`)
	}))
	defer ts.Close()

	client := airflow.NewRESTClient(airflow.Config{URL: ts.URL})

	_, err := client.GetDAGRun(context.Background(), "my_dag")
	if !errors.Is(err, airflow.ErrAirflowNotFound) {
		t.Fatalf("err = %v, want ErrAirflowNotFound", err)
	}
}

func TestGetDAGRun_404_ReturnsNotFound(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	client := airflow.NewRESTClient(airflow.Config{URL: ts.URL})

	_, err := client.GetDAGRun(context.Background(), "my_dag")
	if !errors.Is(err, airflow.ErrAirflowNotFound) {
		t.Fatalf("err = %v, want ErrAirflowNotFound", err)
	}
}

func TestGetDAGRun_AuthHeaderSent(t *testing.T) {
	t.Parallel()

	const wantAuth = "Basic dGVzdA=="

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := r.Header.Get("Authorization")
		if got != wantAuth {
			t.Errorf("Authorization header = %q, want %q", got, wantAuth)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"dag_runs":[{"dag_id":"my_dag","dag_run_id":"run_1","state":"success"}]}`)
	}))
	defer ts.Close()

	client := airflow.NewRESTClient(airflow.Config{
		URL:     ts.URL,
		Headers: map[string]string{"Authorization": wantAuth},
	})

	_, err := client.GetDAGRun(context.Background(), "my_dag")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGetDAGRun_URLPath(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/dags/my_dag/dagRuns" {
			t.Errorf("URL.Path = %q, want %q", r.URL.Path, "/dags/my_dag/dagRuns")
		}
		if got := r.URL.Query().Get("order_by"); got != "-execution_date" {
			t.Errorf("order_by = %q, want %q", got, "-execution_date")
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"dag_runs":[{"dag_id":"my_dag","dag_run_id":"run_1","state":"success"}]}`)
	}))
	defer ts.Close()

	client := airflow.NewRESTClient(airflow.Config{URL: ts.URL})

	_, err := client.GetDAGRun(context.Background(), "my_dag")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// GetDAG tests
// ---------------------------------------------------------------------------

func TestGetDAG_ValidResponse(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"dag_id":"my_dag","is_paused":false,"is_active":true}`)
	}))
	defer ts.Close()

	client := airflow.NewRESTClient(airflow.Config{URL: ts.URL})

	dag, err := client.GetDAG(context.Background(), "my_dag")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dag.DagID != "my_dag" {
		t.Errorf("DagID = %q, want %q", dag.DagID, "my_dag")
	}
	if dag.IsPaused {
		t.Error("IsPaused = true, want false")
	}
	if !dag.IsActive {
		t.Error("IsActive = false, want true")
	}
}

func TestGetDAG_Paused(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"dag_id":"my_dag","is_paused":true,"is_active":true}`)
	}))
	defer ts.Close()

	client := airflow.NewRESTClient(airflow.Config{URL: ts.URL})

	dag, err := client.GetDAG(context.Background(), "my_dag")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dag.IsPaused {
		t.Error("IsPaused = false, want true")
	}
}

func TestGetDAG_404_ReturnsNotFound(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	client := airflow.NewRESTClient(airflow.Config{URL: ts.URL})

	_, err := client.GetDAG(context.Background(), "my_dag")
	if !errors.Is(err, airflow.ErrAirflowNotFound) {
		t.Fatalf("err = %v, want ErrAirflowNotFound", err)
	}
}

// ---------------------------------------------------------------------------
// GetTaskInstance tests
// ---------------------------------------------------------------------------

func TestGetTaskInstance_ValidResponse(t *testing.T) {
	t.Parallel()

	var reqCount int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCount++
		w.Header().Set("Content-Type", "application/json")
		switch {
		case reqCount == 1: // GetDAGRun call
			fmt.Fprint(w, `{"dag_runs":[{"dag_id":"my_dag","dag_run_id":"run_1","state":"success"}]}`)
		case reqCount == 2: // task instance call
			fmt.Fprint(w, `{"dag_id":"my_dag","task_id":"my_task","dag_run_id":"run_1","state":"success"}`)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	client := airflow.NewRESTClient(airflow.Config{URL: ts.URL})

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

func TestGetTaskInstance_NoRuns_ReturnsNotFound(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"dag_runs":[]}`)
	}))
	defer ts.Close()

	client := airflow.NewRESTClient(airflow.Config{URL: ts.URL})

	_, err := client.GetTaskInstance(context.Background(), "my_dag", "my_task")
	if !errors.Is(err, airflow.ErrAirflowNotFound) {
		t.Fatalf("err = %v, want ErrAirflowNotFound", err)
	}
}

func TestGetTaskInstance_Task404_ReturnsNotFound(t *testing.T) {
	t.Parallel()

	var reqCount int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCount++
		w.Header().Set("Content-Type", "application/json")
		switch {
		case reqCount == 1: // GetDAGRun call succeeds
			fmt.Fprint(w, `{"dag_runs":[{"dag_id":"my_dag","dag_run_id":"run_1","state":"success"}]}`)
		case reqCount == 2: // task instance returns 404
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	client := airflow.NewRESTClient(airflow.Config{URL: ts.URL})

	_, err := client.GetTaskInstance(context.Background(), "my_dag", "my_task")
	if !errors.Is(err, airflow.ErrAirflowNotFound) {
		t.Fatalf("err = %v, want ErrAirflowNotFound", err)
	}
}

// ---------------------------------------------------------------------------
// Error tests
// ---------------------------------------------------------------------------

func TestRESTClient_NonOKHTTPStatus(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"detail":"unauthorized"}`)
	}))
	defer ts.Close()

	client := airflow.NewRESTClient(airflow.Config{URL: ts.URL})

	_, err := client.GetDAGRun(context.Background(), "my_dag")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if got := err.Error(); !strings.Contains(got, "401") {
		t.Errorf("error = %q, want it to contain %q", got, "401")
	}
}

func TestRESTClient_HTTPError(t *testing.T) {
	t.Parallel()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	ts.Close() // close immediately to force connection error

	client := airflow.NewRESTClient(airflow.Config{URL: ts.URL})

	_, err := client.GetDAGRun(context.Background(), "my_dag")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

