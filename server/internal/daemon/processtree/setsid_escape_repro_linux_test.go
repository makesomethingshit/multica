//go:build linux

package processtree

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

const (
	setsidHelperEnv     = "MULTICA_TEST_SETSID_CHILD"
	setsidHelperPIDFile = "MULTICA_TEST_SETSID_CHILD_PIDFILE"
)

func init() {
	if os.Getenv(setsidHelperEnv) != "1" {
		return
	}
	pidFile := os.Getenv(setsidHelperPIDFile)
	if pidFile == "" {
		os.Exit(2)
	}
	if _, err := unix.Setsid(); err != nil {
		os.Exit(3)
	}
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		os.Exit(4)
	}
	for {
		time.Sleep(time.Second)
	}
}

func readPIDFile(path string) int {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		return 0
	}
	return pid
}

func waitForPIDFile(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if pid := readPIDFile(path); pid > 0 {
			return pid
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("helper never wrote pid to %s", path)
	return 0
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

// TestProcesstreeRunKillsSetsidEscape verifies that Linux cancellation retains
// and terminates a descendant even after the child creates its own session and
// leaves the leader's process group.
func TestProcesstreeRunKillsSetsidEscape(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	leaderPIDFile := filepath.Join(dir, "leader.pid")
	childPIDFile := filepath.Join(dir, "setsid-child.pid")
	script := filepath.Join(dir, "leader.sh")
	bin, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve test executable: %v", err)
	}
	content := "#!/bin/sh\n" +
		"set -eu\n" +
		"echo $$ > " + shellQuote(leaderPIDFile) + "\n" +
		setsidHelperEnv + "=1 " + setsidHelperPIDFile + "=" + shellQuote(childPIDFile) + " " + shellQuote(bin) + " -test.run=^$ &\n" +
		"sleep 300\n"
	if err := os.WriteFile(script, []byte(content), 0o755); err != nil {
		t.Fatalf("write leader fixture: %v", err)
	}

	var leaderPID, leaderPGID, childPID int
	var childRef ownedDescendant
	t.Cleanup(func() {
		if childPID == 0 {
			childPID = readPIDFile(childPIDFile)
		}
		if childPID > 0 {
			_ = syscall.Kill(childPID, syscall.SIGKILL)
		}
		if leaderPGID > 0 {
			_ = syscall.Kill(-leaderPGID, syscall.SIGKILL)
		} else if leaderPID > 0 {
			_ = syscall.Kill(leaderPID, syscall.SIGKILL)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd := exec.Command(script)
	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(ctx, cmd, 2*time.Second)
	}()

	leaderPID = waitForPIDFile(t, leaderPIDFile)
	leaderPGID, err = unix.Getpgid(leaderPID)
	if err != nil {
		t.Fatalf("get leader pgid: %v", err)
	}
	leaderSID, err := unix.Getsid(leaderPID)
	if err != nil {
		t.Fatalf("get leader sid: %v", err)
	}

	childPID = waitForPIDFile(t, childPIDFile)
	childPGID, err := unix.Getpgid(childPID)
	if err != nil {
		t.Fatalf("get child pgid: %v", err)
	}
	childSID, err := unix.Getsid(childPID)
	if err != nil {
		t.Fatalf("get child sid: %v", err)
	}
	childStat, err := readLinuxProcessStat(childPID)
	if err != nil {
		t.Fatalf("read child process identity: %v", err)
	}
	childRef = ownedDescendant{pid: childPID, identity: childStat.startTime}

	t.Logf("leader pid=%d pgid=%d sid=%d", leaderPID, leaderPGID, leaderSID)
	t.Logf("setsid child pid=%d pgid=%d sid=%d", childPID, childPGID, childSID)
	if childPGID == leaderPGID || childSID == leaderSID {
		t.Fatalf(
			"fixture did not escape leader group/session: leader=(pgid=%d sid=%d), child=(pgid=%d sid=%d)",
			leaderPGID,
			leaderSID,
			childPGID,
			childSID,
		)
	}

	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	if ownedDescendantAlive(childRef) {
		t.Fatalf("setsid descendant %d survived Run cancellation", childPID)
	}
}
