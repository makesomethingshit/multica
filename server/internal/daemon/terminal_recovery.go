package daemon

import (
	"context"
	"fmt"
	"time"
)

// Live-daemon terminal report recovery (GitHub #8157).
//
// CompleteTask/FailTask retry transient failures over a bounded schedule
// (defaultTerminalRetrySchedule, ~124s worst case). When every attempt fails
// while the server row is still running, the already-produced terminal result
// used to become ownerless: the task goroutine returned, the server row stayed
// running, and nothing inside the still-alive daemon was responsible for
// settling the result anymore.
//
// The store below keeps such reports in memory for the daemon process
// lifetime, and a single recovery loop replays them until the server settles
// them authoritatively. The mechanism is deliberately NOT durable: a daemon
// restart discards pending reports — durable replay would cross the server's
// parent-failure / retry-child authority boundary (#4579) and stays out of
// scope. No new watchdog or wall-clock task timeout is involved either: the
// loop is a plain goroutine bound to the daemon root context that becomes a
// no-op whenever the store is empty.

// terminalRecoveryInterval is how often the recovery loop re-attempts pending
// terminal reports on its own. Each pass makes a single delivery attempt per
// report; the loop itself is the retry mechanism, so no nested backoff
// schedule is stacked here. Var (not const) so tests can shrink it.
var terminalRecoveryInterval = 30 * time.Second

// String renders the report kind for recovery logs.
func (k terminalTaskReportKind) String() string {
	switch k {
	case terminalTaskReportComplete:
		return "complete"
	case terminalTaskReportFail:
		return "fail"
	default:
		return fmt.Sprintf("kind(%d)", uint8(k))
	}
}

// initPendingTerminalReports lazily allocates the pending terminal report
// store. Production daemons get it from New; focused tests build &Daemon{}
// literals, so every entry point into the store initializes on demand. The
// wake channel has capacity 1: enqueue never blocks and signals coalesce,
// so a burst of enqueues costs one recovery pass, not one per report.
func (d *Daemon) initPendingTerminalReports() {
	d.terminalReportsMu.Lock()
	defer d.terminalReportsMu.Unlock()
	if d.pendingTerminalReports == nil {
		d.pendingTerminalReports = make(map[string]terminalTaskReport)
	}
	if d.terminalReportsWake == nil {
		d.terminalReportsWake = make(chan struct{}, 1)
	}
}

// enqueuePendingTerminalReport records a terminal report whose bounded
// transient-retry schedule was exhausted while the server row was still
// running, transferring settlement ownership to the recovery loop. Keyed by
// taskID: re-enqueueing a task replaces its report (one pending terminal
// report per task, enqueue is idempotent) and the wake signal coalesces.
func (d *Daemon) enqueuePendingTerminalReport(report terminalTaskReport) {
	d.initPendingTerminalReports()
	d.terminalReportsMu.Lock()
	d.pendingTerminalReports[report.taskID] = report
	d.terminalReportsMu.Unlock()
	select {
	case d.terminalReportsWake <- struct{}{}:
	default:
	}
}

// removePendingTerminalReport drops a settled report — delivered, or
// discarded because the server state already won — but only when it is still
// the one currently stored for the task. A recovery pass works on a snapshot:
// if the same task was re-enqueued with a newer report while the pass was in
// flight (latest-wins enqueue), deleting by taskID alone would discard the
// newer report the daemon now owns. terminalTaskReport is comparable (plain
// string/bool fields), so an equality check against the store's current value
// guarantees latest-wins without a generation counter. A newer report that
// survives a drop here is re-evaluated — and dropped authoritatively if the
// server state is terminal — by the next recovery pass.
func (d *Daemon) removePendingTerminalReport(taskID string, report terminalTaskReport) {
	d.terminalReportsMu.Lock()
	defer d.terminalReportsMu.Unlock()
	if current, ok := d.pendingTerminalReports[taskID]; ok && current == report {
		delete(d.pendingTerminalReports, taskID)
	}
}

// pendingTerminalReportSnapshot returns a copy of the store. Test seam and
// debug aid: HTTP attempts must run outside the store lock, so the recovery
// pass works on a snapshot too.
func (d *Daemon) pendingTerminalReportSnapshot() map[string]terminalTaskReport {
	d.terminalReportsMu.Lock()
	defer d.terminalReportsMu.Unlock()
	if len(d.pendingTerminalReports) == 0 {
		return nil
	}
	out := make(map[string]terminalTaskReport, len(d.pendingTerminalReports))
	for k, v := range d.pendingTerminalReports {
		out[k] = v
	}
	return out
}

// terminalRecoveryLoop is the single daemon-level recovery goroutine started
// from Run. It wakes on a coalesced enqueue signal or on its periodic timer,
// and stops with the daemon root context. No goroutine per pending task: one
// loop drains every report serially.
func (d *Daemon) terminalRecoveryLoop(ctx context.Context) {
	d.logger.Info("terminal recovery loop started")
	d.initPendingTerminalReports()
	d.terminalReportsMu.Lock()
	wake := d.terminalReportsWake
	d.terminalReportsMu.Unlock()

	ticker := time.NewTicker(terminalRecoveryInterval)
	defer ticker.Stop()
	defer d.logger.Debug("terminal recovery loop stopped")

	for {
		select {
		case <-ctx.Done():
			return
		case <-wake:
		case <-ticker.C:
		}
		d.recoverPendingTerminalReports(ctx)
	}
}

// recoverPendingTerminalReports performs one pass over the pending store:
// one authoritative status read plus at most one delivery attempt per report.
// A transient failure anywhere keeps the report; the next pass retries.
func (d *Daemon) recoverPendingTerminalReports(ctx context.Context) {
	reports := d.pendingTerminalReportSnapshot()
	for _, report := range reports {
		if ctx.Err() != nil {
			return
		}
		d.recoverOneTerminalReport(ctx, report)
	}
}

// terminalReportCanReplay reports whether a pending terminal report may be
// replayed against the authoritative server status (fail-recovery scope
// expansion, #8157 follow-up). The kind asymmetry is deliberate and is the
// core invariant of this fix:
//
//	FailTask may settle a task before provider execution reaches running.
//	CompleteTask may only be recovered after execution reached running.
//
// Pre-execution failures (runtime-untracked claims, local-directory
// validation or lock failures) legitimately settle the task while the server
// row is still dispatched or waiting_local_directory, so fail reports replay
// there. Replaying a completion before the row ran would settle work the
// server may still dispatch, so complete reports replay only while running.
func terminalReportCanReplay(status string, kind terminalTaskReportKind) bool {
	if status == "running" {
		return true
	}
	if kind == terminalTaskReportFail {
		return status == "dispatched" || status == "waiting_local_directory"
	}
	return false
}

// recoverOneTerminalReport settles one pending report against the
// authoritative server state. The state machine is intentionally total:
//
//	running + any kind              → one delivery attempt; success removes the
//	                               report, transient failure keeps it, permanent
//	                               rejection drops it
//	dispatched /                    → one delivery attempt (fail reports only)
//	waiting_local_directory + fail
//	dispatched /                    → keep; never replay a completion before the
//	waiting_local_directory +
//	complete                       row ran
//	completed / failed /
//	cancelled                       → drop; never overwrite a terminal state
//	404 task not found              → drop; nothing left to settle
//	transient lookup error          → keep; retry on a later pass
//	permanent (4xx) lookup error    → drop + error log; never poll a dead row
//	                                forever
//	other non-terminal states       → keep; log the state, never mutate it
func (d *Daemon) recoverOneTerminalReport(ctx context.Context, report terminalTaskReport) {
	taskLog := d.logger.With("task_id", report.taskID, "terminal_kind", report.kind.String())

	status, err := d.client.GetTaskStatus(ctx, report.taskID)
	if err != nil {
		if isTaskNotFoundError(err) {
			d.removePendingTerminalReport(report.taskID, report)
			taskLog.Info("dropping pending terminal report; server task not found")
			return
		}
		if !isTransientError(err) {
			// Permanent lookup rejection (e.g. 400/401/403): the status
			// endpoint refuses this row for good. Drop the report with an
			// error log instead of polling a dead row every 30 seconds
			// forever. 404s are handled above; anything reaching here is a
			// non-404 permanent 4xx.
			d.removePendingTerminalReport(report.taskID, report)
			taskLog.Error("terminal recovery status lookup permanently rejected; dropping pending report", "error", err)
			return
		}
		taskLog.Warn("terminal recovery status lookup failed; keeping pending report", "error", err)
		return
	}

	switch {
	case terminalReportCanReplay(status, report.kind):
		// Single delivery attempt — the loop is the retry mechanism. Do not
		// stack defaultTerminalRetrySchedule inside background passes; that
		// would serialize a 124-second stall into every tick.
		if err := d.sendTerminalTask(ctx, report); err != nil {
			if isTransientError(err) {
				taskLog.Warn("terminal recovery delivery failed; keeping pending report", "server_status", status, "outcome", "retry_later", "error", err)
				return
			}
			d.removePendingTerminalReport(report.taskID, report)
			taskLog.Error("terminal recovery delivery permanently rejected; dropping pending report", "server_status", status, "outcome", "dropped_permanent_rejection", "error", err)
			return
		}
		d.removePendingTerminalReport(report.taskID, report)
		taskLog.Info("terminal report recovery delivered", "server_status", status, "outcome", "delivered")

	case isAgentTaskTerminal(status):
		// Server authority: never overwrite a terminal state. A pending
		// completion meeting cancelled/failed/completed is dropped, so user
		// cancellation wins and a failed parent is never resurrected. The
		// value guard keeps a report re-enqueued mid-pass for the next pass,
		// which re-reads the (still terminal) state and drops it there.
		d.removePendingTerminalReport(report.taskID, report)
		taskLog.Info("dropping pending terminal report; server task is already terminal", "server_status", status, "outcome", "dropped_server_terminal")

	default:
		// queued, dispatched, deferred, waiting_local_directory, or a future
		// state: do not invent transition semantics here — keep the report
		// and surface the unexpected state.
		taskLog.Warn("terminal recovery found unexpected task status; keeping pending report", "server_status", status, "outcome", "kept_unexpected_status")
	}
}

// sendTerminalTask is the low-level terminal delivery used by the recovery
// loop: exactly one HTTP attempt per call, no retry schedule. Delivery and
// recovery ownership stay split (#8157): the normal task path keeps
// reportTerminalTask with its bounded schedule, and this loop — not a nested
// backoff — is the retry mechanism.
func (d *Daemon) sendTerminalTask(ctx context.Context, report terminalTaskReport) error {
	switch report.kind {
	case terminalTaskReportComplete:
		return d.client.CompleteTaskOnce(ctx, report.taskID, report.output, report.branchName, report.sessionID, report.workDir, report.sessionRolloutMissing, report.retiredSessionID, report.durableWorkDir)
	case terminalTaskReportFail:
		return d.client.FailTaskOnce(ctx, report.taskID, report.errorMessage, report.sessionID, report.workDir, report.branchName, report.failureReason, report.sessionRolloutMissing, report.retiredSessionID, report.durableWorkDir)
	default:
		return fmt.Errorf("unsupported terminal task report kind %d", report.kind)
	}
}
