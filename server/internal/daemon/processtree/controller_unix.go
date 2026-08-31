//go:build !windows

package processtree

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
)

const processTreeFinishTimeout = 5 * time.Second

type ownedDescendant struct {
	pid      int
	identity uint64
}

type controller struct {
	descendants map[int]ownedDescendant
}

func newController(cmd *exec.Cmd) (*controller, error) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
	return &controller{descendants: make(map[int]ownedDescendant)}, nil
}

func (*controller) attach(_ *exec.Cmd) error { return nil }

func (c *controller) rememberDescendants(rootPID int) {
	refs, err := discoverOwnedDescendants(rootPID)
	if err != nil {
		// Process groups remain the portable Unix fallback. Descendant discovery
		// is an additional Linux containment layer and must not make cancellation
		// fail on hosts where procfs is unavailable or restricted.
		return
	}
	for _, ref := range refs {
		c.descendants[ref.pid] = ref
	}
}

func signalProcessGroup(pid int, sig syscall.Signal) error {
	if err := syscall.Kill(-pid, sig); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return err
	}
	return nil
}

func (c *controller) signalDescendants(sig syscall.Signal) error {
	var result error
	for _, ref := range c.descendants {
		result = errors.Join(result, signalOwnedDescendant(ref, sig))
	}
	return result
}

func (c *controller) activeDescendantPIDs() []int {
	active := make([]int, 0, len(c.descendants))
	for _, ref := range c.descendants {
		if ownedDescendantAlive(ref) {
			active = append(active, ref.pid)
		}
	}
	return active
}

func (c *controller) interrupt(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return os.ErrProcessDone
	}
	pid := cmd.Process.Pid
	// Snapshot descendants while the leader is still alive. A child may call
	// setsid and leave the process group without changing its parent; retaining
	// its process identity lets stop/finish reach it after the leader exits and
	// it is reparented.
	c.rememberDescendants(pid)
	return errors.Join(
		c.signalDescendants(syscall.SIGTERM),
		signalProcessGroup(pid, syscall.SIGTERM),
	)
}

func (c *controller) stop(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return os.ErrProcessDone
	}
	pid := cmd.Process.Pid
	// Capture children created during the graceful-stop window before the
	// process group is force-killed.
	c.rememberDescendants(pid)
	groupErr := signalProcessGroup(pid, syscall.SIGKILL)
	if groupErr != nil {
		if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			groupErr = errors.Join(groupErr, err)
		}
	}
	return errors.Join(c.signalDescendants(syscall.SIGKILL), groupErr)
}

func (c *controller) finish(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	pid := cmd.Process.Pid
	// The normal-exit path may still have group-bound descendants. The
	// cancellation path also has remembered descendants that may now be in a
	// different process group or reparented to init.
	c.rememberDescendants(pid)
	groupErr := signalProcessGroup(pid, syscall.SIGKILL)
	descendantErr := c.signalDescendants(syscall.SIGKILL)
	if err := errors.Join(groupErr, descendantErr); err != nil {
		return err
	}

	deadline := time.Now().Add(processTreeFinishTimeout)
	for time.Now().Before(deadline) {
		groupActive := syscall.Kill(-pid, 0) == nil
		activeDescendants := c.activeDescendantPIDs()
		if !groupActive && len(activeDescendants) == 0 {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf(
		"process tree rooted at %d still active after %s (descendants=%v)",
		pid,
		processTreeFinishTimeout,
		c.activeDescendantPIDs(),
	)
}

func (*controller) close() {}
