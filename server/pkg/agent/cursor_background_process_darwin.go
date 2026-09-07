//go:build darwin

package agent

import (
	"os"
	"os/exec"
	"syscall"

	"golang.org/x/sys/unix"
)

type cursorUnixGroupHandle struct {
	anchor *exec.Cmd
	input  *os.File
	pgid   int
	start  uint64
}

func captureCursorUnixGroup(pgid int) (*cursorUnixGroupHandle, error) {
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	// A private pipe keeps this native utility blocked without a timer. Join
	// before revalidating the shell identity. Cross-session groups cannot be
	// joined, so capture fails closed rather than claiming a numeric PGID.
	anchor := exec.Command("/bin/cat")
	anchor.Env = []string{}
	anchor.Stdin = reader
	anchor.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pgid: pgid}
	if err := anchor.Start(); err != nil {
		_ = writer.Close()
		return nil, err
	}
	g := &cursorUnixGroupHandle{anchor: anchor, input: writer, pgid: pgid}
	info, err := readCursorUnixProcessInfo(anchor.Process.Pid)
	if err != nil || info.pgid != pgid {
		g.close()
		return nil, errCursorBackgroundProcessIdentity
	}
	g.start = info.start
	return g, nil
}

func (g *cursorUnixGroupHandle) signal(sig syscall.Signal) error {
	// We exclusively own Wait. Even if killed, the unreaped direct child
	// retains its PID/group lifetime until close, preventing PGID reuse in
	// the interval between this check and the group signal.
	// getpgid excludes zombies on Darwin; sysctl includes our unreaped child
	// after a group kill, while its start identity and group remain retained.
	info, err := readCursorUnixProcessInfo(g.anchor.Process.Pid)
	if err != nil || info.pgid != g.pgid || info.start != g.start {
		return errCursorBackgroundProcessIdentity
	}
	if sig == 0 {
		// The anchor already proves identity. Darwin's killpg(0) reports
		// EPERM when only zombies remain; that is not an ownership failure.
		return nil
	}
	return syscall.Kill(-g.pgid, sig)
}
func (g *cursorUnixGroupHandle) anchorPID() int { return g.anchor.Process.Pid }
func (g *cursorUnixGroupHandle) close() {
	_ = g.input.Close()
	// Only our unreaped direct child is targeted here, including on capture
	// failure; closing a failed claim must never signal the shell's group.
	_ = g.anchor.Process.Kill()
	_ = g.anchor.Wait()
}

// Darwin's proc state constants are defined in <sys/proc.h>; x/sys/unix does
// not export SZOMB on all supported Darwin architectures.
const cursorDarwinZombieState = 5

func readCursorUnixProcessInfo(pid int) (cursorUnixProcessInfo, error) {
	proc, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return cursorUnixProcessInfo{}, err
	}
	return cursorUnixProcessInfo{
		pid:    int(proc.Proc.P_pid),
		ppid:   int(proc.Eproc.Ppid),
		pgid:   int(proc.Eproc.Pgid),
		start:  uint64(proc.Proc.P_starttime.Sec)*1_000_000 + uint64(proc.Proc.P_starttime.Usec),
		zombie: proc.Proc.P_stat == cursorDarwinZombieState,
	}, nil
}

func listCursorUnixProcessInfos() ([]cursorUnixProcessInfo, error) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		return nil, err
	}
	infos := make([]cursorUnixProcessInfo, 0, len(procs))
	for _, proc := range procs {
		infos = append(infos, cursorUnixProcessInfo{
			pid:    int(proc.Proc.P_pid),
			ppid:   int(proc.Eproc.Ppid),
			pgid:   int(proc.Eproc.Pgid),
			start:  uint64(proc.Proc.P_starttime.Sec)*1_000_000 + uint64(proc.Proc.P_starttime.Usec),
			zombie: proc.Proc.P_stat == cursorDarwinZombieState,
		})
	}
	return infos, nil
}
