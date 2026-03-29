package airflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/dwsmith1983/chaos-data/adapters/airflow"
)

func TestVariableClient_GetVariable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		handler    http.HandlerFunc
		wantKey    string
		wantValue  string
		wantErr    error
		wantErrMsg string
	}{
		{
			name: "success",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("unexpected method %s", r.Method)
				}
				if r.URL.Path != "/variables/my-key" {
					t.Errorf("unexpected path %s", r.URL.Path)
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(airflow.Variable{
					Key:   "my-key",
					Value: "my-value",
				})
			},
			wantKey:   "my-key",
			wantValue: "my-value",
		},
		{
			name: "not found",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`{"detail":"not found"}`))
			},
			wantErr: airflow.ErrVariableNotFound,
		},
		{
			name: "server error",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(`internal error`))
			},
			wantErrMsg: "HTTP 500",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(tt.handler)
			defer srv.Close()

			client := airflow.NewVariableClient(airflow.Config{URL: srv.URL})
			got, err := client.GetVariable(context.Background(), "my-key")

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("GetVariable() error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if tt.wantErrMsg != "" {
				if err == nil {
					t.Fatal("GetVariable() expected error, got nil")
				}
				if !strings.Contains(err.Error(), tt.wantErrMsg) {
					t.Fatalf("GetVariable() error = %q, want substring %q", err.Error(), tt.wantErrMsg)
				}
				return
			}
			if err != nil {
				t.Fatalf("GetVariable() unexpected error: %v", err)
			}
			if got.Key != tt.wantKey {
				t.Errorf("Key = %q, want %q", got.Key, tt.wantKey)
			}
			if got.Value != tt.wantValue {
				t.Errorf("Value = %q, want %q", got.Value, tt.wantValue)
			}
		})
	}
}

func TestVariableClient_SetVariable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		handler    http.HandlerFunc
		wantErrMsg string
	}{
		{
			name: "create success",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost {
					t.Errorf("unexpected method %s", r.Method)
				}
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{}`))
			},
		},
		{
			name: "conflict then patch",
			handler: func() http.HandlerFunc {
				var mu sync.Mutex
				calls := 0
				return func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					calls++
					call := calls
					mu.Unlock()

					if call == 1 {
						if r.Method != http.MethodPost {
							t.Errorf("first call: unexpected method %s", r.Method)
						}
						w.WriteHeader(http.StatusConflict)
						w.Write([]byte(`{"detail":"conflict"}`))
						return
					}
					if r.Method != http.MethodPatch {
						t.Errorf("second call: unexpected method %s, want PATCH", r.Method)
					}
					body, _ := io.ReadAll(r.Body)
					var v airflow.Variable
					if err := json.Unmarshal(body, &v); err != nil {
						t.Errorf("unmarshal PATCH body: %v", err)
					}
					if v.Key != "my-key" {
						t.Errorf("PATCH body key = %q, want %q", v.Key, "my-key")
					}
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(`{}`))
				}
			}(),
		},
		{
			name: "server error",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(`fail`))
			},
			wantErrMsg: "HTTP 500",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(tt.handler)
			defer srv.Close()

			client := airflow.NewVariableClient(airflow.Config{URL: srv.URL})
			err := client.SetVariable(context.Background(), airflow.Variable{
				Key:   "my-key",
				Value: "my-value",
			})

			if tt.wantErrMsg != "" {
				if err == nil {
					t.Fatal("SetVariable() expected error, got nil")
				}
				if !strings.Contains(err.Error(), tt.wantErrMsg) {
					t.Fatalf("SetVariable() error = %q, want substring %q", err.Error(), tt.wantErrMsg)
				}
				return
			}
			if err != nil {
				t.Fatalf("SetVariable() unexpected error: %v", err)
			}
		})
	}
}

func TestVariableClient_DeleteVariable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		handler    http.HandlerFunc
		wantErr    bool
		wantErrMsg string
	}{
		{
			name: "success",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodDelete {
					t.Errorf("unexpected method %s", r.Method)
				}
				w.WriteHeader(http.StatusNoContent)
			},
		},
		{
			name: "not found is idempotent",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`{"detail":"not found"}`))
			},
			wantErr: false,
		},
		{
			name: "server error",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(`fail`))
			},
			wantErr:    true,
			wantErrMsg: "HTTP 500",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(tt.handler)
			defer srv.Close()

			client := airflow.NewVariableClient(airflow.Config{URL: srv.URL})
			err := client.DeleteVariable(context.Background(), "my-key")

			if tt.wantErr {
				if err == nil {
					t.Fatal("DeleteVariable() expected error, got nil")
				}
				if tt.wantErrMsg != "" && !strings.Contains(err.Error(), tt.wantErrMsg) {
					t.Fatalf("DeleteVariable() error = %q, want substring %q", err.Error(), tt.wantErrMsg)
				}
				return
			}
			if err != nil {
				t.Fatalf("DeleteVariable() unexpected error: %v", err)
			}
		})
	}
}

func TestVariableClient_ListVariables(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		handler    http.HandlerFunc
		wantCount  int
		wantErrMsg string
	}{
		{
			name: "single page",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				resp := map[string]any{
					"variables": []airflow.Variable{
						{Key: "k1", Value: "v1"},
						{Key: "k2", Value: "v2"},
					},
					"total_entries": 2,
				}
				json.NewEncoder(w).Encode(resp)
			},
			wantCount: 2,
		},
		{
			name: "pagination across two pages",
			handler: func() http.HandlerFunc {
				var mu sync.Mutex
				calls := 0
				return func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					calls++
					call := calls
					mu.Unlock()

					if call == 1 {
						// First page: offset=0
						vars := make([]airflow.Variable, 100)
						for i := range vars {
							vars[i] = airflow.Variable{Key: "k", Value: "v"}
						}
						resp := map[string]any{
							"variables":     vars,
							"total_entries": 150,
						}
						json.NewEncoder(w).Encode(resp)
						return
					}
					// Second page: offset=100
					vars := make([]airflow.Variable, 50)
					for i := range vars {
						vars[i] = airflow.Variable{Key: "k", Value: "v"}
					}
					resp := map[string]any{
						"variables":     vars,
						"total_entries": 150,
					}
					json.NewEncoder(w).Encode(resp)
				}
			}(),
			wantCount: 150,
		},
		{
			name: "empty list",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				resp := map[string]any{
					"variables":     []airflow.Variable{},
					"total_entries": 0,
				}
				json.NewEncoder(w).Encode(resp)
			},
			wantCount: 0,
		},
		{
			name: "server error",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(`fail`))
			},
			wantErrMsg: "HTTP 500",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(tt.handler)
			defer srv.Close()

			client := airflow.NewVariableClient(airflow.Config{URL: srv.URL})
			got, err := client.ListVariables(context.Background())

			if tt.wantErrMsg != "" {
				if err == nil {
					t.Fatal("ListVariables() expected error, got nil")
				}
				if !strings.Contains(err.Error(), tt.wantErrMsg) {
					t.Fatalf("ListVariables() error = %q, want substring %q", err.Error(), tt.wantErrMsg)
				}
				return
			}
			if err != nil {
				t.Fatalf("ListVariables() unexpected error: %v", err)
			}
			if len(got) != tt.wantCount {
				t.Errorf("ListVariables() returned %d variables, want %d", len(got), tt.wantCount)
			}
		})
	}
}

func TestVariableClient_Headers(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer tok" {
			t.Errorf("Authorization header = %q, want %q", got, "Bearer tok")
		}
		json.NewEncoder(w).Encode(airflow.Variable{Key: "k", Value: "v"})
	}))
	defer srv.Close()

	client := airflow.NewVariableClient(airflow.Config{
		URL:     srv.URL,
		Headers: map[string]string{"Authorization": "Bearer tok"},
	})
	_, err := client.GetVariable(context.Background(), "k")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

