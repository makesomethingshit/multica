//go:build linux

package processtree

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"syscall"
)

type linuxProcessStat struct {
	state     byte
	ppid      int
	startTime uint64
}

func readLinuxProcessStat(pid int) (linuxProcessStat, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return linuxProcessStat{}, err
	}
	// comm is enclosed in parentheses and may contain spaces or ')'. Split
	// after the final ')' so field indexes from state onward remain stable.
	closeParen := strings.LastIndexByte(string(data), ')')
	if closeParen < 0 || closeParen+1 >= len(data) {
		return linuxProcessStat{}, fmt.Errorf("malformed /proc/%d/stat", pid)
	}
	fields := strings.Fields(string(data[closeParen+1:]))
	// fields[0] is state (field 3), fields[1] is ppid (field 4), and
	// fields[19] is starttime (field 22).
	if len(fields) <= 19 || len(fields[0]) != 1 {
		return linuxProcessStat{}, fmt.Errorf("short /proc/%d/stat", pid)
	}
	ppid, err := strconv.Atoi(fields[1])
	if err != nil {
		return linuxProcessStat{}, fmt.Errorf("parse /proc/%d/stat ppid: %w", pid, err)
	}
	startTime, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return linuxProcessStat{}, fmt.Errorf("parse /proc/%d/stat starttime: %w", pid, err)
	}
	return linuxProcessStat{state: fields[0][0], ppid: ppid, startTime: startTime}, nil
}

func discoverOwnedDescendants(rootPID int) ([]ownedDescendant, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}

	children := make(map[int][]ownedDescendant)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 || pid == rootPID {
			continue
		}
		stat, err := readLinuxProcessStat(pid)
		if err != nil {
			// Processes can exit while /proc is scanned and hidepid mounts may
			// deny unrelated entries. Both are expected; retain readable children.
			continue
		}
		children[stat.ppid] = append(children[stat.ppid], ownedDescendant{
			pid:      pid,
			identity: stat.startTime,
		})
	}

	seen := map[int]struct{}{rootPID: {}}
	queue := []int{rootPID}
	result := make([]ownedDescendant, 0)
	for len(queue) > 0 {
		parent := queue[0]
		queue = queue[1:]
		for _, child := range children[parent] {
			if _, ok := seen[child.pid]; ok {
				continue
			}
			seen[child.pid] = struct{}{}
			result = append(result, child)
			queue = append(queue, child.pid)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].pid < result[j].pid })
	return result, nil
}

func signalOwnedDescendant(ref ownedDescendant, sig syscall.Signal) error {
	stat, err := readLinuxProcessStat(ref.pid)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		// Do not signal an unverified PID: it may have been recycled for an
		// unrelated process after the original descendant exited.
		return nil
	}
	if stat.startTime != ref.identity || stat.state == 'Z' {
		return nil
	}
	if err := syscall.Kill(ref.pid, sig); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}

func ownedDescendantAlive(ref ownedDescendant) bool {
	stat, err := readLinuxProcessStat(ref.pid)
	return err == nil && stat.startTime == ref.identity && stat.state != 'Z'
}
