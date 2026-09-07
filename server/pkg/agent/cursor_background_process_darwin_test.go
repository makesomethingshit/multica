//go:build darwin

package agent

import (
	"os"
	"os/exec"
	"syscall"
	"testing"
)

func TestCaptureCursorBackgroundRejectsDetachedSession(t *testing.T) {
	child := exec.Command(os.Args[0])
	child.Env = append(os.Environ(), cursorFakeModeEnv+"=leaf", "CURSOR_FAKE_DURATION=30s")
	child.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
	self, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	defer self.Release()
	if owned, err := captureCursorBackgroundProcess(&exec.Cmd{Process: self}, child.Process.Pid); err == nil {
		owned.Close()
		t.Fatal("claimed a detached session without durable ownership")
	}
	if err := child.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("failed capture signalled the detached shell: %v", err)
	}
}
