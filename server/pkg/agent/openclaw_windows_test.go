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

	// 6KB+ UTF-8 that exceeds the 8000-byte threshold even after combining
	// with any system prompt. Multi-byte ensures the file is UTF-8 without BOM.
	prompt := strings.Repeat("한글🌟a", 900) + "\n— gateway dispatch test —\n" + strings.Repeat("Ω≈ç√∫˜µ≤≥÷", 200)
	if len(prompt) <= openclawMessageFileThreshold {
		t.Fatalf("prompt must exceed threshold %d, got %d", openclawMessageFileThreshold, len(prompt))
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


