package daemon

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/daemon/execenv"
)

// Cross-process lifecycle safety (GH #8280 Blocker A).
//
// Two daemon PROCESSES now share one work-state scope, so the in-process guard
// maps cannot be the boundary that protects a live tree. These tests model the
// two processes with two independent Daemon guard states - never two goroutines
// on one Daemon - sharing only the scope directory on disk, and assert the
// invariant: while any daemon in the scope is using an env root or a persistent
// store, no other daemon may reclaim or mutate it.

// newScopeGuardDaemon builds a Daemon with its own guard state, pointed at one
// scope lock directory and one workspaces root. Two of these are two processes:
// nothing is shared but the files on disk.
func newScopeGuardDaemon(t *testing.T, lockDir, workspacesRoot string) *Daemon {
	t.Helper()
	d := &Daemon{
		cfg: Config{
			WorkspacesRoot:     workspacesRoot,
			GCArtifactPatterns: DefaultGCArtifactPatterns,
		},
		logger:            quietTaskLog(),
		scopeLocks:        execenv.NewScopeLocks(lockDir),
		storeClaimTimeout: 250 * time.Millisecond,
		activeEnvRoots:    map[string]int{},
		deletingEnvRoots:  map[string]bool{},
		activeStores:      map[string]int{},
		deletingStores:    map[string]bool{},
	}
	d.activeEnvRootsCond = sync.NewCond(&d.activeEnvRootsMu)
	d.activeStoresCond = sync.NewCond(&d.activeStoresMu)
	return d
}

func scopeLockDirForTest(t *testing.T, key string) string {
	t.Helper()
	return filepath.Join(t.TempDir(), workStateRootDirName, key, workStateLockDirName)
}

// TestGCScope_EnvRootActiveInAnotherDaemon is case A: a task running in one
// daemon owns its env root through .task_lock, so a GC in another daemon of the
// same scope must skip every mutation of that root - full removal and the
// artifact paths, which share the reservation.
func TestGCScope_EnvRootActiveInAnotherDaemon(t *testing.T) {
	lockDir := scopeLockDirForTest(t, "key-a")
	daemonA := newScopeGuardDaemon(t, lockDir, "")
	daemonB := newScopeGuardDaemon(t, lockDir, "")

	wsRoot := t.TempDir()
	daemonA.cfg.WorkspacesRoot = wsRoot
	daemonB.cfg.WorkspacesRoot = wsRoot
	claim, err := execenv.ClaimEnvRoot(execenv.RootDirParams{
		WorkspacesRoot: wsRoot,
		WorkspaceID:    "11111111-1111-1111-1111-111111111111",
		TaskID:         "22222222-2222-2222-2222-222222222222",
	})
	if err != nil {
		t.Fatalf("daemon B claim env root: %v", err)
	}
	root := claim.RootDir()
	mustWriteFile(t, filepath.Join(root, "workdir", "main.go"), "package main")
	artifact := filepath.Join(root, "workdir", "node_modules", "pkg", "index.js")
	mustWriteFile(t, artifact, "module.exports = 1")

	// Daemon A would never know about B own task from its own maps; the env root
	// lock is what has to stop it.
	if _, ok := daemonA.reserveEnvRootForGC(root); ok {
		t.Fatal("GC took the env root of a task running in another daemon")
	}
	stats := &gcStats{byPattern: map[string]int{}}
	if cleaned := daemonA.applyGCAction(root, gcActionCleanArtifacts, stats); cleaned != 0 {
		t.Fatalf("artifact cleanup mutated a live env root (%d dirs)", cleaned)
	}
	if _, err := os.Stat(artifact); err != nil {
		t.Fatalf("artifact cleanup removed content from a live env root: %v", err)
	}

	// B finishes its task; now A may do the cleanup it was asked for.
	claim.Release()
	release, ok := daemonA.reserveEnvRootForGC(root)
	if !ok {
		t.Fatal("GC still refused an env root no process owns")
	}
	release()
	if _, removed := daemonA.cleanTaskDir(root); !removed {
		t.Fatal("eligible cleanup did not remove the released env root")
	}
}

// TestGCScope_CodexStoreActiveInAnotherDaemon is case B.
func TestGCScope_CodexStoreActiveInAnotherDaemon(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CODEX_HOME", filepath.Join(home, ".codex"))
	lockDir := scopeLockDirForTest(t, "key-b")
	daemonA := newScopeGuardDaemon(t, lockDir, "")
	daemonB := newScopeGuardDaemon(t, lockDir, "")

	const namespace = "w_scopetest"
	store := execenv.CodexSessionStorePath(namespace, execenv.TaskContextForEnv{AgentID: "agent-1", IssueID: "issue-1"})
	rollout := filepath.Join(store, "sessions", "rollout.jsonl")
	mustWriteFile(t, rollout, "{}")
	ageWorkStateTree(t, store, time.Now().Add(-30*24*time.Hour))

	holder, err := daemonB.holdStoreForTask(store)
	if err != nil {
		t.Fatalf("task store claim in daemon B: %v", err)
	}
	if removed, _ := execenv.PruneCodexSessionStores(namespace, 14*24*time.Hour, time.Now(), daemonA.reserveStoreForDeletion, quietTaskLog()); removed != 0 {
		t.Fatalf("GC reclaimed a store a task is using in another daemon (removed=%d)", removed)
	}
	if _, err := os.Stat(rollout); err != nil {
		t.Fatalf("store was mutated while another daemon held it: %v", err)
	}

	holder()
	if removed, _ := execenv.PruneCodexSessionStores(namespace, 14*24*time.Hour, time.Now(), daemonA.reserveStoreForDeletion, quietTaskLog()); removed != 1 {
		t.Fatalf("GC refused the released store (removed=%d)", removed)
	}
}

// TestGCScope_HermesMemoryStoreKeepsConcurrentUsers is case C, and the one that
// must not regress into serialisation: two tasks in two daemons share one
// agent memory store by design (last-writer-wins), while the GC still cannot
// reclaim it until both are done.
func TestGCScope_HermesMemoryStoreKeepsConcurrentUsers(t *testing.T) {
	lockDir := scopeLockDirForTest(t, "key-c")
	daemonA := newScopeGuardDaemon(t, lockDir, "")
	daemonB := newScopeGuardDaemon(t, lockDir, "")
	daemonC := newScopeGuardDaemon(t, lockDir, "")

	stateRoot := t.TempDir()
	store := execenv.HermesMemoryStorePath(stateRoot, "agent-1", "")
	mustWriteFile(t, filepath.Join(store, "MEMORY.md"), "remembered")
	ageWorkStateTree(t, store, time.Now().Add(-100*24*time.Hour))

	releaseB, err := daemonB.holdStoreForTask(store)
	if err != nil {
		t.Fatalf("first task could not use the memory store: %v", err)
	}
	releaseC, err := daemonC.holdStoreForTask(store)
	if err != nil {
		t.Fatalf("a second legitimate task was serialised out of the memory store: %v", err)
	}

	if removed, _ := execenv.PruneHermesMemoryStores(stateRoot, 90*24*time.Hour, time.Now(), daemonA.reserveStoreForDeletion, quietTaskLog()); removed != 0 {
		t.Fatalf("GC reclaimed a memory store with live tasks (removed=%d)", removed)
	}

	releaseB()
	releaseC()
	if removed, _ := execenv.PruneHermesMemoryStores(stateRoot, 90*24*time.Hour, time.Now(), daemonA.reserveStoreForDeletion, quietTaskLog()); removed != 1 {
		t.Fatalf("GC refused the released memory store (removed=%d)", removed)
	}
}

// TestGCScope_HermesSessionStoreActiveInAnotherDaemon is case D.
func TestGCScope_HermesSessionStoreActiveInAnotherDaemon(t *testing.T) {
	lockDir := scopeLockDirForTest(t, "key-d")
	daemonA := newScopeGuardDaemon(t, lockDir, "")
	daemonB := newScopeGuardDaemon(t, lockDir, "")

	stateRoot := t.TempDir()
	task := execenv.TaskContextForEnv{AgentID: "agent-1", IssueID: "issue-1"}
	store := execenv.HermesSessionStorePath(stateRoot, "agent-1", "", task)
	mustWriteFile(t, filepath.Join(store, "state.db"), "transcript")
	ageWorkStateTree(t, store, time.Now().Add(-30*24*time.Hour))

	holder, err := daemonB.holdStoreForTask(store)
	if err != nil {
		t.Fatalf("session store claim in daemon B: %v", err)
	}
	if removed, _ := execenv.PruneHermesSessionStores(stateRoot, 14*24*time.Hour, time.Now(), daemonA.reserveStoreForDeletion, quietTaskLog()); removed != 0 {
		t.Fatalf("GC reclaimed a session store a conversation is using elsewhere (removed=%d)", removed)
	}

	holder()
	if removed, _ := execenv.PruneHermesSessionStores(stateRoot, 14*24*time.Hour, time.Now(), daemonA.reserveStoreForDeletion, quietTaskLog()); removed != 1 {
		t.Fatalf("GC refused the released session store (removed=%d)", removed)
	}
}

// TestGCScope_DeletionStartRace is case E, both orderings. The point is that a
// task never mounts a store another daemon has begun deleting, and a GC never
// claims a store another daemon is already using.
func TestGCScope_DeletionStartRace(t *testing.T) {
	lockDir := scopeLockDirForTest(t, "key-e")
	daemonA := newScopeGuardDaemon(t, lockDir, "")
	daemonB := newScopeGuardDaemon(t, lockDir, "")

	store := filepath.Join(t.TempDir(), "store")
	if err := os.MkdirAll(store, 0o755); err != nil {
		t.Fatalf("mkdir store: %v", err)
	}

	// A owns the deletion first: B must fail safe rather than mount halfway.
	commitA, ok := daemonA.reserveStoreForDeletion(store)
	if !ok {
		t.Fatal("daemon A could not claim an unowned store")
	}
	if _, err := daemonB.holdStoreForTask(store); err == nil {
		t.Fatal("a task mounted a store another daemon is deleting")
	}
	commitA()
	released, err := daemonB.holdStoreForTask(store)
	if err != nil {
		t.Fatalf("a task was still refused after the deletion finished: %v", err)
	}
	released()

	// B owns the store first: A must skip.
	holder, err := daemonB.holdStoreForTask(store)
	if err != nil {
		t.Fatalf("task claim: %v", err)
	}
	if _, ok := daemonA.reserveStoreForDeletion(store); ok {
		t.Fatal("GC claimed a store a task is using in another daemon")
	}
	holder()
	commitA, ok = daemonA.reserveStoreForDeletion(store)
	if !ok {
		t.Fatal("GC refused a store no task holds any more")
	}
	commitA()
}

// TestScopeLifecycle_ResumeAcrossDaemonsWithConcurrentGC is the end-to-end
// regression for the issue lifecycle: profile A creates a task and leaves a
// workdir plus a provider session behind, profile B starts independently, finds
// them through the persisted mapping, and a GC running in the same scope while B
// works cannot reclaim what B is using.
func TestScopeLifecycle_ResumeAcrossDaemonsWithConcurrentGC(t *testing.T) {
	home := stageWorkStateHome(t)
	writeProfileConfig(t, "", testBackendA, "")
	writeProfileConfig(t, testDesktop, testBackendA, "")

	// Profile A (default) is the first daemon here: it establishes the mapping and
	// creates a task, exactly as it would have before B ever existed.
	seedWorkspaceState(t, filepath.Join(home, workspacesRootDirName))
	profileA := resolveScope(t, testBackendA, "", "")
	task := execenv.TaskContextForEnv{AgentID: testAgentID, IssueID: testIssueID}
	workdir := filepath.Join(profileA.WorkspacesRoot, "0f0f0f0f-1111-2222-3333-444444444444", "0c0c0c0c", "workdir")
	mustWriteFile(t, filepath.Join(workdir, "checkout.txt"), "turn 1")
	storeA := execenv.CodexSessionStorePath(profileA.CodexNamespace, task)
	rollout := filepath.Join(storeA, "sessions", "rollout-2026-09-18T00-00-00-session.jsonl")
	mustWriteFile(t, rollout, "{}")

	// Profile B starts independently and resolves the persisted mapping.
	profileB := resolveScope(t, testBackendA, testDesktop, "")
	if profileB.CodexNamespace != profileA.CodexNamespace || profileB.WorkspacesRoot != profileA.WorkspacesRoot {
		t.Fatalf("profile B resolved %+v, want profile A scope %+v", profileB, profileA)
	}
	if _, err := os.Stat(rollout); err != nil {
		t.Fatalf("profile B cannot reach the provider session profile A created: %v", err)
	}
	rel, err := filepath.Rel(profileB.WorkspacesRoot, workdir)
	if err != nil || strings.HasPrefix(rel, "..") {
		t.Fatalf("profile A workdir %q is outside profile B root %q", workdir, profileB.WorkspacesRoot)
	}

	// A GC from another daemon in the same scope runs while B is using the store.
	lockDir := filepath.Join(profileB.ScopeDir, workStateLockDirName)
	daemonB := newScopeGuardDaemon(t, lockDir, profileB.WorkspacesRoot)
	daemonGC := newScopeGuardDaemon(t, lockDir, profileB.WorkspacesRoot)
	ageWorkStateTree(t, storeA, time.Now().Add(-30*24*time.Hour))
	session, err := daemonB.holdStoreForTask(storeA)
	if err != nil {
		t.Fatalf("profile B could not take the store: %v", err)
	}
	if removed, _ := execenv.PruneCodexSessionStores(profileB.CodexNamespace, 14*24*time.Hour, time.Now(), daemonGC.reserveStoreForDeletion, quietTaskLog()); removed != 0 {
		t.Fatalf("GC reclaimed the state profile B is resuming (removed=%d)", removed)
	}

	// B finishes; the state is idle now, so a later GC reclaims it normally.
	session()
	if removed, _ := execenv.PruneCodexSessionStores(profileB.CodexNamespace, 14*24*time.Hour, time.Now(), daemonGC.reserveStoreForDeletion, quietTaskLog()); removed != 1 {
		t.Fatalf("GC refused the idle store after the resume finished (removed=%d)", removed)
	}
}
