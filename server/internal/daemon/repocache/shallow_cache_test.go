package repocache

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestCreateIsolatedCheckoutFromShallowCacheResolvesFetchedTip reproduces
// multica-ai/multica#8011: an isolated checkout created from a shallow shared
// cache loses the commit CreateWorktree just resolved as its base.
//
// The shared cache below has the reported topology: it is shallow, its stale
// refs/heads/master still points at commit A, and the freshly fetched tip B is
// reachable only through refs/remotes/origin/master. `git clone --local` from
// a shallow source is silently downgraded to a transport-based clone, which
// transfers only the objects reachable from the heads it fetches
// (refs/heads/*) — so B is absent from the fresh checkout and the subsequent
// `git checkout --detach B` fails (reported as "reference is not a tree";
// "unable to read tree" on newer Git).
func TestCreateIsolatedCheckoutFromShallowCacheResolvesFetchedTip(t *testing.T) {
	t.Parallel()

	// A bare origin plus a working clone: commit A, then advance origin/master
	// with commit B after the shallow cache snapshot was taken.
	originDir := filepath.Join(t.TempDir(), "origin.git")
	workDir := filepath.Join(t.TempDir(), "work")
	seed := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=test", "GIT_AUTHOR_EMAIL=test@test.com",
			"GIT_COMMITTER_NAME=test", "GIT_COMMITTER_EMAIL=test@test.com",
		)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git setup %v failed: %s: %v", args, out, err)
		}
	}
	seed("init", workDir)
	// `* -text` pins byte-exact working-tree content regardless of the local
	// core.autocrlf, so the assertions below hold on every platform.
	if err := os.WriteFile(filepath.Join(workDir, ".gitattributes"), []byte("* -text\n"), 0o644); err != nil {
		t.Fatalf("write .gitattributes: %v", err)
	}
	if err := os.WriteFile(filepath.Join(workDir, "a.txt"), []byte("contents of a.txt\n"), 0o644); err != nil {
		t.Fatalf("write a.txt: %v", err)
	}
	seed("-C", workDir, "add", ".gitattributes", "a.txt")
	seed("-C", workDir, "commit", "-m", "commit A")
	seed("-C", workDir, "branch", "-M", "master")
	// The bare origin is a plain-path full clone — only the cache seed below
	// needs a shallow fetch. `--depth` is honored only over the pack protocol
	// (a plain path takes the local hardlink shortcut and drops the depth
	// silently), so that one clone has to go through file:// with an
	// all-forward-slash URL.
	originURL := "file://" + filepath.ToSlash(originDir)
	if !strings.HasPrefix(filepath.ToSlash(originDir), "/") {
		originURL = "file:///" + filepath.ToSlash(originDir)
	}
	seed("clone", "--bare", "--", workDir, originDir)

	// The workspace registers the repository by a canonical remote URL instead
	// of the local origin path: a file:// URL carrying a Windows drive letter
	// would give bareDirName a "C:" path segment, which no filesystem accepts
	// as a directory name. The canonical URL is never contacted — every fetch
	// in this test flows through the daemon-owned cache or the local paths.
	repoURL := "https://shallow-test.local/origin.git"

	// Seed the shared bare cache as a depth-1 shallow clone, then convert it to
	// the modern remote-tracking layout — the two steps Cache.Sync performs on
	// a cold miss.
	cache := New(t.TempDir(), testLogger())
	barePath := filepath.Join(cache.root, "ws-1", bareDirName(repoURL))
	if err := os.MkdirAll(filepath.Dir(barePath), 0o755); err != nil {
		t.Fatalf("create workspace cache dir: %v", err)
	}
	seed("clone", "--bare", "--depth=1", "--", originURL, barePath)
	if err := ensureRemoteTrackingLayout(barePath); err != nil {
		t.Fatalf("ensure refspec: %v", err)
	}

	// Advance origin/master with commit B and fetch it through the same
	// cache-fetch path CreateWorktree uses.
	commitA := gitHead(t, workDir)
	if err := os.WriteFile(filepath.Join(workDir, "b.txt"), []byte("contents of b.txt\n"), 0o644); err != nil {
		t.Fatalf("write b.txt: %v", err)
	}
	runGitAuthored(t, workDir, "add", "b.txt")
	runGitAuthored(t, workDir, "commit", "-m", "commit B")
	commitB := gitHead(t, workDir)
	runGitAuthored(t, workDir, "push", originDir, "master:master")
	if err := cache.Fetch(barePath); err != nil {
		t.Fatalf("cache fetch: %v", err)
	}

	// Mandatory preconditions: the assertions below must exercise the exact
	// #8011 topology, not an accidental generic checkout setup.
	if shallow, err := runGitOutput("-C", barePath, "rev-parse", "--is-shallow-repository"); err != nil || strings.TrimSpace(string(shallow)) != "true" {
		t.Fatalf("precondition: shared cache must be shallow, got %q err=%v", shallow, err)
	}
	if got := gitRefCommit(t, barePath, "refs/heads/master"); got != commitA {
		t.Fatalf("precondition: refs/heads/master = %s, want stale head %s", got, commitA)
	}
	if got := gitRefCommit(t, barePath, "refs/remotes/origin/master"); got != commitB {
		t.Fatalf("precondition: refs/remotes/origin/master = %s, want fetched tip %s", got, commitB)
	}
	if commitA == commitB {
		t.Fatal("precondition: stale head and fetched tip must be distinct commits")
	}
	if err := runGit("-C", barePath, "rev-parse", "--verify", commitB+"^{commit}"); err != nil {
		t.Fatalf("precondition: fetched tip %s must resolve in the bare cache: %v", commitB, err)
	}

	// The production path used by Linux/Windows Codex isolated checkouts.
	result, err := cache.CreateWorktree(WorktreeParams{
		WorkspaceID:         "ws-1",
		RepoURL:             repoURL,
		WorkDir:             t.TempDir(),
		AgentName:           "Linux Codex",
		TaskID:              "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
		IsolatedGitMetadata: true,
	})
	if err != nil {
		t.Fatalf("CreateWorktree failed: %v", err)
	}

	// The resolved base commit must have survived the clone.
	if got := gitHead(t, result.Path); got != commitB {
		t.Fatalf("isolated checkout HEAD = %s, want fetched tip %s", got, commitB)
	}
	content, err := os.ReadFile(filepath.Join(result.Path, "b.txt"))
	if err != nil {
		t.Fatalf("read b.txt from isolated checkout: %v", err)
	}
	if got, want := string(content), "contents of b.txt\n"; got != want {
		t.Fatalf("b.txt = %q, want %q", got, want)
	}

	// The recovery pulls the object from the local cache without rewriting the
	// shared cache's stale head.
	if got := gitRefCommit(t, barePath, "refs/heads/master"); got != commitA {
		t.Fatalf("shared cache refs/heads/master was rewritten by the recovery: got %s, want %s", got, commitA)
	}
	origin, err := runGitOutput("-C", result.Path, "remote", "get-url", "origin")
	if err != nil {
		t.Fatalf("get origin URL: %v", err)
	}
	if got, want := strings.TrimSpace(string(origin)), repoURL; got != want {
		t.Fatalf("origin URL = %q, want %q", got, want)
	}
}
