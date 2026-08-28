package daemon

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// TestClient_ClaimTasks_PostsRuntimeSetAndParsesTasks verifies the machine-level
// batch claim (MUL-4257): the client POSTs to /api/daemon/tasks/claim with the full
// runtime_id set + max_tasks, and parses the {"tasks":[...]} envelope, keeping
// each task's runtime_id so the daemon can route it locally.
func TestClient_ClaimTasks_PostsRuntimeSetAndParsesTasks(t *testing.T) {
	var gotPath string
	var gotBody struct {
		DaemonID   string   `json:"daemon_id"`
		RuntimeIDs []string `json:"runtime_ids"`
		MaxTasks   int      `json:"max_tasks"`
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"tasks":[
			{"id":"t1","runtime_id":"rt-a","agent":{"name":"a"}},
			{"id":"t2","runtime_id":"rt-b","agent":{"name":"b"}}
		]}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	c.SetToken("tok")

	tasks, err := c.ClaimTasks(context.Background(), "daemon-x", []string{"rt-a", "rt-b", "rt-c"}, 3)
	if err != nil {
		t.Fatalf("ClaimTasks: %v", err)
	}

	if gotPath != "/api/daemon/tasks/claim" {
		t.Errorf("path = %q, want /api/daemon/tasks/claim", gotPath)
	}
	if gotBody.DaemonID != "daemon-x" {
		t.Errorf("posted daemon_id = %q, want daemon-x", gotBody.DaemonID)
	}
	if len(gotBody.RuntimeIDs) != 3 || gotBody.RuntimeIDs[0] != "rt-a" || gotBody.MaxTasks != 3 {
		t.Errorf("posted body = %+v, want runtime_ids=[rt-a rt-b rt-c] max_tasks=3", gotBody)
	}
	if len(tasks) != 2 {
		t.Fatalf("got %d tasks, want 2", len(tasks))
	}
	if tasks[0].ID != "t1" || tasks[0].RuntimeID != "rt-a" {
		t.Errorf("task[0] = %+v, want id=t1 runtime_id=rt-a", tasks[0])
	}
	if tasks[1].ID != "t2" || tasks[1].RuntimeID != "rt-b" {
		t.Errorf("task[1] = %+v, want id=t2 runtime_id=rt-b", tasks[1])
	}
}

// TestClient_ClaimTasks_EmptyResult confirms an empty batch (idle daemon) is
// returned as a nil/empty slice, not an error.
func TestClient_ClaimTasks_EmptyResult(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"tasks":[]}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	c.SetToken("tok")

	tasks, err := c.ClaimTasks(context.Background(), "daemon-x", []string{"rt-a"}, 1)
	if err != nil {
		t.Fatalf("ClaimTasks: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("got %d tasks, want 0", len(tasks))
	}
}

// TestClient_ClaimTasks_ChunksLargeRuntimeSet verifies the PUCK-58 chunking
// fix: a set larger than batchClaimMaxRuntimeIDs is split across multiple
// requests so a runtime at position 257+ is still claimable instead of being
// permanently truncated by the server.
func TestClient_ClaimTasks_ChunksLargeRuntimeSet(t *testing.T) {
	var mu sync.Mutex
	var gotBodies [][]string
	var requestCount int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			RuntimeIDs []string `json:"runtime_ids"`
		}
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &body)
		mu.Lock()
		requestCount++
		gotBodies = append(gotBodies, body.RuntimeIDs)
		mu.Unlock()
		// Only the tail chunk contains rt-tail; return a task for it.
		for _, id := range body.RuntimeIDs {
			if id == "rt-tail" {
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`{"tasks":[{"id":"tail-task","runtime_id":"rt-tail"}]}`))
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"tasks":[]}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	c.SetToken("tok")

	// 300 IDs: first 256 are filler, tail at position 299
	ids := make([]string, 300)
	for i := 0; i < 299; i++ {
		ids[i] = "rt-filler"
	}
	ids[299] = "rt-tail"

	tasks, err := c.ClaimTasks(context.Background(), "daemon-x", ids, 5)
	if err != nil {
		t.Fatalf("ClaimTasks chunked: %v", err)
	}
	if len(tasks) != 1 || tasks[0].ID != "tail-task" {
		t.Fatalf("chunked claim got %d tasks %+v, want tail-task", len(tasks), tasks)
	}
	mu.Lock()
	rc := requestCount
	mu.Unlock()
	if rc != 2 {
		t.Fatalf("chunked claim made %d requests, want 2 (256 + 44)", rc)
	}
	mu.Lock()
	foundTail := false
	for _, body := range gotBodies {
		for _, id := range body {
			if id == "rt-tail" {
				foundTail = true
			}
		}
		if len(body) > batchClaimMaxRuntimeIDs {
			t.Fatalf("chunk size %d exceeds limit %d", len(body), batchClaimMaxRuntimeIDs)
		}
	}
	mu.Unlock()
	if !foundTail {
		t.Fatalf("tail runtime not found in any chunk")
	}
}

// TestClient_ClaimTasks_RotatesWhenHeadSaturated verifies the rotation fix:
// when the head chunk keeps filling maxTasks, the tail is not permanently
// starved — the starting chunk rotates each poll so the tail is eventually
// claimed.
func TestClient_ClaimTasks_RotatesWhenHeadSaturated(t *testing.T) {
	var mu sync.Mutex
	var gotBodies [][]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			RuntimeIDs []string `json:"runtime_ids"`
			MaxTasks   int      `json:"max_tasks"`
		}
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &body)
		mu.Lock()
		gotBodies = append(gotBodies, body.RuntimeIDs)
		mu.Unlock()
		// Head chunk always has tasks that fill maxTasks; tail chunk has the
		// desired tail task. The client should rotate so the second poll
		// starts with the tail chunk.
		hasHead := false
		hasTail := false
		for _, id := range body.RuntimeIDs {
			if id == "rt-head" {
				hasHead = true
			}
			if id == "rt-tail" {
				hasTail = true
			}
		}
		w.Header().Set("Content-Type", "application/json")
		if hasHead {
			// Fill maxTasks so remaining becomes 0 and the tail chunk in the
			// same poll is not visited — rotation is needed for the next poll.
			w.Write([]byte(`{"tasks":[{"id":"head-task","runtime_id":"rt-head"}]}`))
			return
		}
		if hasTail {
			w.Write([]byte(`{"tasks":[{"id":"tail-task","runtime_id":"rt-tail"}]}`))
			return
		}
		w.Write([]byte(`{"tasks":[]}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	c.SetToken("tok")

	// 300 IDs: head at position 0, tail at 299, rest filler
	ids := make([]string, 300)
	ids[0] = "rt-head"
	for i := 1; i < 299; i++ {
		ids[i] = "rt-filler"
	}
	ids[299] = "rt-tail"

	// First poll: offset 0 -> head chunk first, fills maxTasks=1, tail not visited
	tasks1, err := c.ClaimTasks(context.Background(), "daemon-x", ids, 1)
	if err != nil {
		t.Fatalf("first poll ClaimTasks: %v", err)
	}
	if len(tasks1) != 1 || tasks1[0].ID != "head-task" {
		t.Fatalf("first poll got %+v, want head-task", tasks1)
	}
	mu.Lock()
	if len(gotBodies) != 1 || gotBodies[0][0] != "rt-head" {
		t.Fatalf("first poll first chunk = %v, want rt-head first", gotBodies[0])
	}
	mu.Unlock()

	// Second poll: offset 1 -> tail chunk first, should claim tail
	mu.Lock()
	gotBodies = nil
	mu.Unlock()
	tasks2, err := c.ClaimTasks(context.Background(), "daemon-x", ids, 1)
	if err != nil {
		t.Fatalf("second poll ClaimTasks: %v", err)
	}
	if len(tasks2) != 1 || tasks2[0].ID != "tail-task" {
		t.Fatalf("second poll got %+v, want tail-task (rotation)", tasks2)
	}
	mu.Lock()
	if len(gotBodies) == 0 || gotBodies[0][0] == "rt-head" {
		t.Fatalf("second poll first chunk = %v, want tail chunk first due to rotation", gotBodies[0])
	}
	foundTailFirst := false
	for _, id := range gotBodies[0] {
		if id == "rt-tail" {
			foundTailFirst = true
			break
		}
	}
	mu.Unlock()
	if !foundTailFirst {
		t.Fatalf("second poll first chunk did not contain rt-tail, rotation failed")
	}
}
