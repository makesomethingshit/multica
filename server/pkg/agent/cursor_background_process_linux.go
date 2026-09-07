//go:build linux

package agent

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func readCursorUnixProcessInfo(pid int) (cursorUnixProcessInfo, error) {
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return cursorUnixProcessInfo{}, err
	}
	raw := string(data)
	openParen := strings.IndexByte(raw, '(')
	closeParen := strings.LastIndex(raw, ") ")
	if openParen <= 0 || closeParen < openParen {
		return cursorUnixProcessInfo{}, fmt.Errorf("malformed /proc/%d/stat", pid)
	}
	parsedPID, err := strconv.Atoi(strings.TrimSpace(raw[:openParen]))
	if err != nil || parsedPID != pid {
		return cursorUnixProcessInfo{}, fmt.Errorf("unexpected pid in /proc/%d/stat", pid)
	}
	fields := strings.Fields(raw[closeParen+2:])
	if len(fields) <= 19 {
		return cursorUnixProcessInfo{}, fmt.Errorf("short /proc/%d/stat", pid)
	}
	ppid, err := strconv.Atoi(fields[1])
	if err != nil {
		return cursorUnixProcessInfo{}, err
	}
	pgid, err := strconv.Atoi(fields[2])
	if err != nil {
		return cursorUnixProcessInfo{}, err
	}
	start, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return cursorUnixProcessInfo{}, err
	}
	return cursorUnixProcessInfo{pid: pid, ppid: ppid, pgid: pgid, start: start, zombie: fields[0] == "Z"}, nil
}

func listCursorUnixProcessInfos() ([]cursorUnixProcessInfo, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	infos := make([]cursorUnixProcessInfo, 0, len(entries))
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		info, err := readCursorUnixProcessInfo(pid)
		if err == nil && !info.zombie {
			infos = append(infos, info)
		}
	}
	return infos, nil
}
