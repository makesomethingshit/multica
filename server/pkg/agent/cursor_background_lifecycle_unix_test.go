//go:build linux || darwin

package agent

import (
	"testing"
	"time"
)

func assertCursorTestProcessGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		info, err := readCursorUnixProcessInfo(pid)
		if err != nil || info.zombie {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("background process %d survived", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
