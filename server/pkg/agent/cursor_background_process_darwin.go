//go:build darwin

package agent

import "golang.org/x/sys/unix"

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
