//go:build !windows && !linux && !darwin

package agent

import "testing"

func assertCursorTestProcessGone(t *testing.T, _ int) {
	t.Helper()
	t.Skip("background process ownership is unavailable on this platform")
}
