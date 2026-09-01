//go:build windows

package agent

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	openclawShimHelperEnv      = "MULTICA_OPENCLAW_SHIM_HELPER"
	openclawShimHelperArgvFile = "MULTICA_OPENCLAW_SHIM_ARGV_FILE"
	openclawShimHelperMsgFile  = "MULTICA_OPENCLAW_SHIM_MSG_FILE"
	openclawShimHelperMode     = "MULTICA_OPENCLAW_SHIM_HELPER_MODE" // "supported" (default/unset) | "unsupported"
)

// TestOpenclawWindowsShimHelperProcess is not a test. Re-executed by the
// fake openclaw.cmd below, it stands in for `node openclaw.mjs` as the real
// native child. Inert unless the shim env var is set.
func TestOpenclawWindowsShimHelperProcess(t *testing.T) {
	if os.Getenv(openclawShimHelperEnv) != "1" {
		t.Skip("helper process; only runs when re-executed by the shim")
	}
	// Direct invocation by the shim: os.Args is the shim's own argv forwarding
	// (openclaw agent --json ... --message-file ...). Also handle `--` sentinel
	// for compatibility with any future wrapper that inserts it.
	forwarded := os.Args[1:]
	for i, a := range os.Args {
		if a == "--" {
			forwarded = os.Args[i+1:]
			break
		}
	}
	if len(forwarded) == 0 {
		fmt.Fprintf(os.Stderr, "helper: no forwarded args; os.Args=%q\n", os.Args)
		os.Exit(1)
	}
	// --version probe
	if len(forwarded) == 1 && forwarded[0] == "--version" {
		fmt.Println("openclaw 2026.7.1")
		os.Exit(0)
	}
	if len(forwarded) >= 2 && forwarded[0] == "agent" && forwarded[1] == "--help" {
		if os.Getenv(openclawShimHelperMode) == "unsupported" {
			fmt.Println("Usage: openclaw agent")
			fmt.Println("  --message <text>")
			os.Exit(0)
		}
		fmt.Println("Usage: openclaw agent")
		fmt.Println("  --message <text>")
		fmt.Println("  --message-file <path>")
		os.Exit(0)
	}
	// Actual agent invocation: record argv and message-file content.
	if argvPath := os.Getenv(openclawShimHelperArgvFile); argvPath != "" {
		if err := os.WriteFile(argvPath, []byte(strings.Join(forwarded, "\n")), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "helper: write argv: %v\n", err)
			os.Exit(1)
		}
	}
	var mf string
	for i, a := range forwarded {
		if a == "--message-file" && i+1 < len(forwarded) {
			mf = forwarded[i+1]
			break
		}
	}
	if mf != "" {
		data, err := os.ReadFile(mf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "helper: read message-file %q: %v\n", mf, err)
			os.Exit(1)
		}
		if dst := os.Getenv(openclawShimHelperMsgFile); dst != "" {
			if err := os.WriteFile(dst, data, 0o644); err != nil {
				fmt.Fprintf(os.Stderr, "helper: write msg: %v\n", err)
				os.Exit(1)
			}
		}
		// Ensure --message is not also present (capability-aware transport uses file only).
		for _, a := range forwarded {
			if a == "--message" {
				fmt.Fprintf(os.Stderr, "helper: unexpected --message alongside --message-file\n")
				os.Exit(1)
			}
		}
	} else {
		// For long-prompt this test expects --message-file; fail if missing.
		fmt.Fprintf(os.Stderr, "helper: missing --message-file in argv %q\n", strings.Join(forwarded, " "))
		os.Exit(1)
	}
	fmt.Println(`{"payloads":[{"text":"ok"}],"meta":{}}`)
	os.Exit(0)
}

// TestOpenclawExecuteLongPromptViaMessageFileOnWindowsShim is the Windows
// half of the openclaw --message-file regression. It crosses the boundary
// the bug lived on:
//
//	Go os/exec -> cmd.exe -> openclaw.cmd -> native child (test binary)
//
// The .cmd shim is the 8191-char cmd.exe limit site and the CreateProcess
// 32767 limit site. A 6KB+ UTF-8 prompt inlined on argv would overflow
// the shim before the OS reports "command line too long". Delivering it
// via --message-file keeps argv short, preserves UTF-8 byte-for-byte,
// and cleans up the temp file on all exit paths.
//
// Runs only on windows-latest via ci.yml windows-execenv, which scopes
// -run to windows-tagged tests. -v makes a silent skip visible.
func TestOpenclawExecuteLongPromptViaMessageFileOnWindowsShim(t *testing.T) {
	// No Parallel: sets process-wide env via t.Setenv.
	self, err := os.Executable()
	if err != nil {
		t.Fatalf("locate test binary: %v", err)
	}
	dir := t.TempDir()
	argvPath := filepath.Join(dir, "argv.txt")
	msgContentPath := filepath.Join(dir, "msg.txt")

	cmdPath := filepath.Join(dir, "openclaw.cmd")
	// The .cmd forwards to the helper test above. %* is the shim's own argv
	// forwarding. Avoid ^/$ regex anchors to keep cmd.exe from stripping them.
	body := fmt.Sprintf("@echo off\r\n\"%s\" -test.run=TestOpenclawWindowsShimHelperProcess -- %%*\r\n", self)
	if err := os.WriteFile(cmdPath, []byte(body), 0o644); err != nil {
		t.Fatalf("write shim: %v", err)
	}

	// Probes (version/help) inherit the parent env; the task itself gets Config.Env via buildEnv
	// (which filters MULTICA_ from os.Environ, so we must also pass via Config.Env).
	t.Setenv(openclawShimHelperEnv, "1")
	t.Setenv(openclawShimHelperArgvFile, argvPath)
	t.Setenv(openclawShimHelperMsgFile, msgContentPath)
	helperEnv := map[string]string{
		openclawShimHelperEnv:      "1",
		openclawShimHelperArgvFile: argvPath,
		openclawShimHelperMsgFile:  msgContentPath,
	}

	openclawMessageFileSupportCache = sync.Map{}

	// 6KB+ UTF-8 that exceeds the 6000-byte shim threshold
	// (openclawShimThreshold) even after combining with any system prompt.
	// Multi-byte ensures the file is UTF-8 without BOM.
	prompt := strings.Repeat("한글🌟a", 900) + "\n— gateway dispatch test —\n" + strings.Repeat("Ω≈ç√∫˜µ≤≥÷", 200)
	if len(prompt) <= openclawShimThreshold {
		t.Fatalf("prompt must exceed shim threshold %d, got %d", openclawShimThreshold, len(prompt))
	}

	backend, err := New("openclaw", Config{ExecutablePath: cmdPath, Logger: slog.Default(), Env: helperEnv})
	if err != nil {
		t.Fatalf("New(openclaw): %v", err)
	}
	session, err := backend.Execute(t.Context(), prompt, ExecOptions{Timeout: 60 * time.Second})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	go func() {
		for range session.Messages {
		}
	}()
	result := <-session.Result
	if result.Status != "completed" {
		t.Fatalf("status = %q, want completed; error=%q", result.Status, result.Error)
	}

	argvRaw, err := os.ReadFile(argvPath)
	if err != nil {
		t.Fatalf("native child never recorded argv (did shim reach helper?): %v; result=%+v", err, result)
	}
	gotArgv := string(argvRaw)
	if !strings.Contains(gotArgv, "--message-file") {
		t.Errorf("expected --message-file in argv; got %q", gotArgv)
	}
	if strings.Contains(gotArgv, "--message\n") || strings.Contains(gotArgv, "--message ") && strings.Contains(gotArgv, "한글") {
		t.Errorf("prompt must not be inlined on argv; argv=%q", gotArgv)
	}
	for _, needle := range []string{"한글", "🌟", "Ω≈", "gateway dispatch"} {
		if strings.Contains(gotArgv, needle) {
			t.Errorf("prompt fragment %q leaked into argv %q", needle, gotArgv)
		}
	}
	for _, want := range []string{"agent", "--json", "--session-id", "--message-file"} {
		if !strings.Contains(gotArgv, want) {
			t.Errorf("expected %q to reach native child; argv=%q", want, gotArgv)
		}
	}
	if len(gotArgv) > 8000 {
		t.Errorf("argv is %d chars, should be short via --message-file", len(gotArgv))
	}

	stdinRaw, err := os.ReadFile(msgContentPath)
	if err != nil {
		t.Fatalf("helper never recorded message-file content: %v", err)
	}
	if string(stdinRaw) != prompt {
		t.Errorf("UTF-8 prompt did not survive file transport: got %d bytes want %d", len(stdinRaw), len(prompt))
	}
	// No BOM.
	if len(stdinRaw) >= 3 && stdinRaw[0] == 0xEF && stdinRaw[1] == 0xBB && stdinRaw[2] == 0xBF {
		t.Error("message file must not contain UTF-8 BOM")
	}

	// Cleanup: the temp message file the backend created must be removed
	// after the run (defer cleanupOpenclawMessageFile in the result goroutine).
	var mfPath string
	for i, line := range strings.Split(strings.TrimSuffix(gotArgv, "\n"), "\n") {
		if line == "--message-file" && i+1 < len(strings.Split(strings.TrimSuffix(gotArgv, "\n"), "\n")) {
			parts := strings.Split(strings.TrimSuffix(gotArgv, "\n"), "\n")
			mfPath = parts[i+1]
			break
		}
	}
	// Fallback parse by scanning original argv lines.
	if mfPath == "" {
		lines := strings.Split(strings.TrimSuffix(gotArgv, "\n"), "\n")
		for i, a := range lines {
			if a == "--message-file" && i+1 < len(lines) {
				mfPath = strings.TrimSpace(lines[i+1])
				break
			}
		}
	}
	if mfPath == "" {
		t.Fatalf("could not extract --message-file path from argv %q", gotArgv)
	}
	if _, err := os.Stat(mfPath); err == nil {
		t.Errorf("message file %q still exists after session; cleanup missing", mfPath)
	}
	if _, err := os.Stat(filepath.Dir(mfPath)); err == nil {
		t.Errorf("message temp dir %q still exists after session; cleanup missing", filepath.Dir(mfPath))
	}
}

func TestIsOpenclawShimPath(t *testing.T) {
	for _, tc := range []struct {
		path string
		want bool
	}{
		{`C:\tools\openclaw.cmd`, true},
		{`C:\tools\openclaw.CMD`, true},
		{`C:\tools\openclaw.bat`, true},
		{`C:\tools\openclaw.BAT`, true},
		{`openclaw.cmd`, true},
		{`openclaw.bat`, true},
		{" openclaw.cmd ", true},
		{`/usr/local/bin/openclaw`, false},
		{`C:\tools\openclaw.exe`, false},
		{`openclaw`, false},
		{`openclaw.sh`, false},
	} {
		if got := isOpenclawShimPath(tc.path); got != tc.want {
			t.Errorf("isOpenclawShimPath(%q)=%v want %v", tc.path, got, tc.want)
		}
	}
}

func TestOpenclawThresholds(t *testing.T) {
	if openclawShimThreshold != 6000 {
		t.Errorf("openclawShimThreshold=%d want 6000 (must fire before 6193 repro)", openclawShimThreshold)
	}
	if openclawNativeThreshold != 30000 {
		t.Errorf("openclawNativeThreshold=%d want 30000", openclawNativeThreshold)
	}
	if openclawMessageFileThreshold != openclawShimThreshold {
		t.Errorf("openclawMessageFileThreshold alias %d != shim %d", openclawMessageFileThreshold, openclawShimThreshold)
	}
	if openclawShimThreshold >= 6193 {
		t.Errorf("shim threshold %d must be < 6193 to block before repro", openclawShimThreshold)
	}
}

func TestOpenclawExecuteLongPromptRejectedWithoutMessageFileOnWindowsShimAtReproBoundary(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatalf("locate test binary: %v", err)
	}
	dir := t.TempDir()
	argvPath := filepath.Join(dir, "argv.txt")
	msgContentPath := filepath.Join(dir, "msg.txt")
	cmdPath := filepath.Join(dir, "openclaw.cmd")
	body := fmt.Sprintf("@echo off\r\n\"%s\" -test.run=TestOpenclawWindowsShimHelperProcess -- %%*\r\n", self)
	if err := os.WriteFile(cmdPath, []byte(body), 0o644); err != nil {
		t.Fatalf("write shim: %v", err)
	}
	t.Setenv(openclawShimHelperEnv, "1")
	t.Setenv(openclawShimHelperMode, "unsupported")
	t.Setenv(openclawShimHelperArgvFile, argvPath)
	t.Setenv(openclawShimHelperMsgFile, msgContentPath)
	helperEnv := map[string]string{
		openclawShimHelperEnv:      "1",
		openclawShimHelperMode:     "unsupported",
		openclawShimHelperArgvFile: argvPath,
		openclawShimHelperMsgFile:  msgContentPath,
	}
	openclawMessageFileSupportCache = sync.Map{}
	reproPrompt := strings.Repeat("x", 6200)
	if len(reproPrompt) <= openclawShimThreshold {
		t.Fatalf("repro prompt must exceed shim threshold %d", openclawShimThreshold)
	}
	backend, err := New("openclaw", Config{ExecutablePath: cmdPath, Logger: slog.Default(), Env: helperEnv})
	if err != nil {
		t.Fatalf("New(openclaw): %v", err)
	}
	_, execErr := backend.Execute(t.Context(), reproPrompt, ExecOptions{Timeout: 10 * time.Second})
	if execErr == nil {
		t.Fatal("expected Execute to reject 6200-byte prompt on unsupported .cmd shim, got nil")
	}
	msg := execErr.Error()
	if strings.Contains(strings.ToLower(msg), "command line too long") {
		t.Errorf("must not surface raw OS message: %q", msg)
	}
	for _, want := range []string{"command-line limit", "--message-file", "openclaw update"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error missing %q: %q", want, msg)
		}
	}
	// 5999 on the same unsupported shim must NOT be rejected (just below 6000)
	openclawMessageFileSupportCache = sync.Map{}
	short := strings.Repeat("x", 5999)
	if err := os.Remove(argvPath); err != nil && !os.IsNotExist(err) {
		t.Fatalf("cleanup argv: %v", err)
	}
	session, err := backend.Execute(t.Context(), short, ExecOptions{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("short 5999-byte prompt should not be rejected on unsupported shim: %v", err)
	}
	go func() { for range session.Messages {} }()
	<-session.Result
}

func TestOpenclawExecuteLongPromptViaMessageFileOnWindowsShimAtReproBoundary(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatalf("locate test binary: %v", err)
	}
	dir := t.TempDir()
	argvPath := filepath.Join(dir, "argv.txt")
	msgContentPath := filepath.Join(dir, "msg.txt")
	cmdPath := filepath.Join(dir, "openclaw.cmd")
	body := fmt.Sprintf("@echo off\r\n\"%s\" -test.run=TestOpenclawWindowsShimHelperProcess -- %%*\r\n", self)
	if err := os.WriteFile(cmdPath, []byte(body), 0o644); err != nil {
		t.Fatalf("write shim: %v", err)
	}
	t.Setenv(openclawShimHelperEnv, "1")
	t.Setenv(openclawShimHelperMode, "supported")
	t.Setenv(openclawShimHelperArgvFile, argvPath)
	t.Setenv(openclawShimHelperMsgFile, msgContentPath)
	helperEnv := map[string]string{
		openclawShimHelperEnv:      "1",
		openclawShimHelperMode:     "supported",
		openclawShimHelperArgvFile: argvPath,
		openclawShimHelperMsgFile:  msgContentPath,
	}
	openclawMessageFileSupportCache = sync.Map{}
	prompt := strings.Repeat("한글🌟", 800) + strings.Repeat("x", 900)
	if len(prompt) <= openclawShimThreshold {
		t.Fatalf("prompt must exceed shim threshold %d, got %d", openclawShimThreshold, len(prompt))
	}
	if len(prompt) < 6200 {
		prompt += strings.Repeat("a", 6200-len(prompt))
	}
	backend, err := New("openclaw", Config{ExecutablePath: cmdPath, Logger: slog.Default(), Env: helperEnv})
	if err != nil {
		t.Fatalf("New(openclaw): %v", err)
	}
	session, err := backend.Execute(t.Context(), prompt, ExecOptions{Timeout: 60 * time.Second})
	if err != nil {
		t.Fatalf("Execute at repro boundary: %v", err)
	}
	go func() { for range session.Messages {} }()
	result := <-session.Result
	if result.Status != "completed" {
		t.Fatalf("status=%q want completed; error=%q", result.Status, result.Error)
	}
	argvRaw, err := os.ReadFile(argvPath)
	if err != nil {
		t.Fatalf("helper never recorded argv: %v", err)
	}
	gotArgv := string(argvRaw)
	if !strings.Contains(gotArgv, "--message-file") {
		t.Errorf("expected --message-file at 6200 boundary; argv=%q", gotArgv)
	}
	for _, needle := range []string{"한글", "🌟"} {
		if strings.Contains(gotArgv, needle) {
			t.Errorf("prompt fragment %q leaked into argv at boundary", needle)
		}
	}
	data, err := os.ReadFile(msgContentPath)
	if err != nil {
		t.Fatalf("helper never recorded msg: %v", err)
	}
	if string(data) != prompt {
		t.Errorf("UTF-8 not preserved byte-for-byte at boundary: got %d want %d", len(data), len(prompt))
	}
}


