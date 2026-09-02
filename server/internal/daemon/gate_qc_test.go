package daemon

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/multica-ai/multica/server/internal/daemon/execenv"
)

func TestGateResumeToReachableSession_QuickCreateBypass(t *testing.T) {
	// Fresh Issue env: prior workdir != env workdir, but qc store has rollout
	root := t.TempDir()
	sharedHome := filepath.Join(root, "shared")
	t.Setenv("CODEX_HOME", sharedHome)
	const (
		agentID = "agent-qc-gate"
		scope   = "qc_019f59d9-a6aa-7a53-b173-1eccc4b4c874"
		session = "019f59d9-a6aa-7a53-b173-1eccc4b4c875"
	)
	// Seed store directly via exported path helper
	storeDir := execenv.CodexSessionStorePath("", execenv.TaskContextForEnv{AgentID: agentID, SessionStoreScope: scope})
	// CodexSessionStorePath resolves via CODEX_HOME, but we have set CODEX_HOME to sharedHome
	// For determinism, also ensure directory exists via same logic as codexSessionStoreDir
	if storeDir == "" {
		// fallback manual for default profile
		storeDir = filepath.Join(sharedHome, "multica-sessions", "default", agentID, scope)
	}
	if err := os.MkdirAll(filepath.Join(storeDir, "2026", "08", "05"), 0o755); err != nil {
		t.Fatal(err)
	}
	seed := filepath.Join(storeDir, "2026", "08", "05", "rollout-2026-08-05T00-00-00-"+session+".jsonl")
	if err := os.WriteFile(seed, []byte("test"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Ensure it's regular file
	if fi, err := os.Lstat(seed); err != nil || !fi.Mode().IsRegular() {
		t.Fatalf("seed not regular: %v %v", fi, err)
	}
	task := Task{
		AgentID:           agentID,
		PriorSessionID:    session,
		PriorWorkDir:      "/tmp/prior-not-exist", // different from env
		SessionStoreScope: scope,
	}
	taskCtx := execenv.TaskContextForEnv{PriorSessionResumed: true, AgentID: agentID, SessionStoreScope: scope}
	envDir := filepath.Join(root, "fresh-env")
	if err := os.MkdirAll(envDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// sessionHomeReachable false, workdir mismatch -> normally would drop, but qc store has rollout so should keep
	reachable := gateResumeToReachableSession(&task, &taskCtx, "codex", envDir, "", false, slog.Default())
	if !reachable {
		t.Fatal("qc store rollout should make session reachable across fresh workdir")
	}
	if task.PriorSessionID != session {
		t.Fatalf("qc gate should not drop PriorSessionID, got %q", task.PriorSessionID)
	}
	// Negative: without rollout, should drop
	task2 := Task{
		AgentID:           agentID,
		PriorSessionID:    "missing-session",
		PriorWorkDir:      "/tmp/prior-not-exist",
		SessionStoreScope: scope,
	}
	taskCtx2 := execenv.TaskContextForEnv{PriorSessionResumed: true, AgentID: agentID, SessionStoreScope: scope}
	reachable2 := gateResumeToReachableSession(&task2, &taskCtx2, "codex", envDir, "", false, slog.Default())
	if reachable2 {
		t.Fatal("missing rollout should not be reachable")
	}
	if task2.PriorSessionID != "" {
		t.Fatalf("should drop missing session, got %q", task2.PriorSessionID)
	}
	// Negative: malformed scope should not bypass (falls back to workdir check)
	task3 := Task{
		AgentID:           agentID,
		PriorSessionID:    session,
		PriorWorkDir:      "/tmp/prior-not-exist",
		SessionStoreScope: "qc_bad/scope",
	}
	taskCtx3 := execenv.TaskContextForEnv{PriorSessionResumed: true, AgentID: agentID, SessionStoreScope: "qc_bad/scope"}
	reachable3 := gateResumeToReachableSession(&task3, &taskCtx3, "codex", envDir, "", false, slog.Default())
	if reachable3 {
		t.Fatal("malformed qc scope should not bypass workdir check")
	}
	// Negative: different agent should not see
	task4 := Task{
		AgentID:           "other-agent",
		PriorSessionID:    session,
		PriorWorkDir:      "/tmp/prior-not-exist",
		SessionStoreScope: scope,
	}
	taskCtx4 := execenv.TaskContextForEnv{PriorSessionResumed: true, AgentID: "other-agent", SessionStoreScope: scope}
	reachable4 := gateResumeToReachableSession(&task4, &taskCtx4, "codex", envDir, "", false, slog.Default())
	if reachable4 {
		t.Fatal("other agent should not see qc store")
	}
	// Negative: profile isolation
	task5 := Task{
		AgentID:           agentID,
		PriorSessionID:    session,
		PriorWorkDir:      "/tmp/prior-not-exist",
		SessionStoreScope: scope,
	}
	taskCtx5 := execenv.TaskContextForEnv{PriorSessionResumed: true, AgentID: agentID, SessionStoreScope: scope}
	reachable5 := gateResumeToReachableSession(&task5, &taskCtx5, "codex", envDir, "other-profile", false, slog.Default())
	if reachable5 {
		t.Fatal("other profile should not see store")
	}
}
