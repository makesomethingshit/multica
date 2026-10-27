package daemon

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestTerminalReportGenerationReplay(t *testing.T) {
	for _, kind := range []terminalTaskReportKind{terminalTaskReportComplete, terminalTaskReportFail} {
		for _, generation := range []string{"", "2026-10-27T09:00:00.123456+09:00"} {
			for _, code := range []string{"task_claim_generation_mismatch", "other_conflict"} {
				name, _ := terminalReportKindName(kind)
				t.Run(name+"/"+generation+"/"+code, func(t *testing.T) {
					calls := 0
					srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						calls++
						if r.URL.Path != "/api/daemon/tasks/task-replay/"+name {
							t.Errorf("path = %s", r.URL.Path)
						}
						var body map[string]any
						if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
							t.Error(err)
						}
						if generation == "" {
							if _, present := body["expected_dispatched_at"]; present {
								t.Error("legacy report included generation")
							}
						} else if body["expected_dispatched_at"] != generation {
							t.Errorf("generation = %v, want %s", body["expected_dispatched_at"], generation)
						}
						w.WriteHeader(http.StatusConflict)
						json.NewEncoder(w).Encode(map[string]string{"code": code})
					}))
					defer srv.Close()
					cfg := Config{ServerBaseURL: srv.URL, WorkspacesRoot: t.TempDir(), DaemonID: "daemon-replay"}
					logger := slog.New(slog.NewTextHandler(io.Discard, nil))
					before := New(cfg, logger)
					report := terminalTaskReport{kind: kind, taskID: "task-replay", claimDispatchedAt: generation, output: "original", errorMessage: "original failure"}
					if err := before.terminalReports.enqueue(report); err != nil {
						t.Fatal(err)
					}
					body, err := os.ReadFile(filepath.Join(before.terminalReports.dir, terminalReportFileName(report.taskID)))
					if err != nil {
						t.Fatal(err)
					}
					var record map[string]any
					if err := json.Unmarshal(body, &record); err != nil {
						t.Fatal(err)
					}
					if record["version"] != float64(1) {
						t.Fatalf("version = %v", record["version"])
					}
					if generation == "" {
						if _, present := record["claim_dispatched_at"]; present {
							t.Fatal("legacy record included generation")
						}
					} else if record["claim_dispatched_at"] != generation {
						t.Fatal("record changed claim generation")
					}
					after := New(cfg, logger)
					pending, delivered := after.replayPendingTerminalReports(context.Background())
					wantPending := 1
					if code == "task_claim_generation_mismatch" {
						wantPending = 0
					}
					if pending != wantPending || delivered != 0 {
						t.Fatalf("pending/delivered = %d/%d", pending, delivered)
					}
					items, err := after.terminalReports.list()
					if err != nil || len(items) != wantPending {
						t.Fatalf("queue = %v, %v", items, err)
					}
					if calls != 1 {
						t.Fatalf("requests = %d; stale completion must not compensate", calls)
					}
				})
			}
		}
	}
}
