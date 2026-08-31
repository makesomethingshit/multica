//go:build linux

package processtree

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func init() {
	if os.Getenv("PUCK_SETSID_HELPER2") == "1" {
		pidFile := os.Getenv("PUCK_SETSID_HELPER2_PIDFILE")
		if pidFile == "" {
			os.Exit(2)
		}
		if _, err := syscall.Setsid(); err != nil {
			if _, err2 := unix.Setsid(); err2 != nil {
				os.Exit(3)
			}
		}
		if err := os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
			os.Exit(4)
		}
		for {
			time.Sleep(time.Second)
		}
	}
}

func waitForPid(path string) int {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(path)
		if err == nil {
			n := 0
			for _, c := range b {
				if c >= '0' && c <= '9' {
					n = n*10 + int(c-'0')
				}
			}
			if n > 0 {
				return n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	panic("helper never wrote pid to " + path)
}

// TestProcesstreeRunSetsidEscape reproduces the setsid escape via the public
// Run path (run.go:65-85 Wait then finish with lifecycle error propagation).
// Build tag linux only, uses Go re-exec helper without external setsid.
func TestProcesstreeRunSetsidEscape(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	leaderPidFile := filepath.Join(dir, "leader.pid")
	childPidFile := filepath.Join(dir, "setsid-child.pid")
	script := filepath.Join(dir, "leader.sh")
	bin, _ := os.Executable()
	content := "#!/bin/sh\necho $$ > \"" + leaderPidFile + "\"\nPUCK_SETSID_HELPER2=1 PUCK_SETSID_HELPER2_PIDFILE=\"" + childPidFile + "\" \"" + bin + "\" -test.run=^$ &\nsleep 300\n"
	if err := os.WriteFile(script, []byte(content), 0755); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd := exec.Command(script)
	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(ctx, cmd, 2*time.Second)
	}()

	childPID := waitForPid(childPidFile)
	leaderPID := waitForPid(leaderPidFile)
	leaderPGID, _ := unix.Getpgid(leaderPID)
	childPGID, _ := unix.Getpgid(childPID)
	leaderSID, _ := unix.Getsid(leaderPID)
	childSID, _ := unix.Getsid(childPID)
	t.Logf("leader pid=%d pgid=%d sid=%d", leaderPID, leaderPGID, leaderSID)
	t.Logf("setsid child pid=%d pgid=%d sid=%d", childPID, childPGID, childSID)
	if childPGID == leaderPGID {
		t.Fatalf("child PGID %d == leader PGID %d — Setsid escape failed", childPGID, leaderPGID)
	}

	t.Cleanup(func() {
		if leaderPID != 0 {
			_ = syscall.Kill(-leaderPID, syscall.SIGKILL)
			_ = syscall.Kill(leaderPID, syscall.SIGKILL)
		}
		_ = syscall.Kill(childPID, syscall.SIGKILL)
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if syscall.Kill(childPID, 0) != nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if syscall.Kill(childPID, 0) == nil {
			t.Logf("cleanup warning: child pid %d still alive after SIGKILL", childPID)
		}
		if leaderPID != 0 && syscall.Kill(-leaderPID, 0) == nil {
			t.Logf("cleanup warning: leader pgid %d still alive", leaderPID)
		}
	})

	cancel()
	select {
	case err := <-errCh:
		t.Logf("Run returned err=%v", err)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context.Canceled; lifecycle errors must not be misread as setsid reproduction", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("Run did not return after cancel (wait+finish hung)")
	}

	alive := syscall.Kill(childPID, 0) == nil
	t.Logf("after Run cancel+finish: child pid=%d alive=%v", childPID, alive)
	if !alive {
		t.Fatalf("child already gone — not reproducing escape")
	}
	t.Logf("REPRODUCED processtree Run: setsid child pid=%d pgid=%d sid=%d survived Run cancel+finish (leader pid=%d pgid=%d) — expected on main 11861145a", childPID, childPGID, childSID, leaderPID, leaderPGID)
}
