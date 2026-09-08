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

// removePendingTerminalReport drops a report once it is settled — delivered,
// or discarded because the server state already won.
func (d *Daemon) removePendingTerminalReport(taskID string) {
	d.terminalReportsMu.Lock()
	delete(d.pendingTerminalReports, taskID)
	d.terminalReportsMu.Unlock()
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

// recoverOneTerminalReport settles one pending report against the
// authoritative server state. The state machine is intentionally total:
//
//	running                  → one delivery attempt; success removes the
//	                           report, transient failure keeps it, permanent
//	                           rejection drops it
//	completed                → drop (another delivery path won, or a lost
//	                           success response already committed)
//	failed                   → drop; never reclaim, never rewrite the parent
//	                           (#4579 split-brain stays closed)
//	cancelled                → drop; user cancellation is authoritative
//	404 task not found       → drop; nothing left to settle
//	transient lookup error   → keep; retry on a later pass
//	unexpected non-terminal  → keep; log the state, never mutate it
func (d *Daemon) recoverOneTerminalReport(ctx context.Context, report terminalTaskReport) {
	taskLog := d.logger.With("task_id", report.taskID, "terminal_kind", report.kind.String())

	status, err := d.client.GetTaskStatus(ctx, report.taskID)
	if err != nil {
		if isTaskNotFoundError(err) {
			d.removePendingTerminalReport(report.taskID)
			taskLog.Info("dropping pending terminal report; server task not found")
			return
		}
		taskLog.Warn("terminal recovery status lookup failed; keeping pending report", "error", err)
		return
	}

	switch {
	case status == "running":
		// Single delivery attempt — the loop is the retry mechanism. Do not
		// stack defaultTerminalRetrySchedule inside background passes; that
		// would serialize a 124-second stall into every tick.
		if err := d.sendTerminalTask(ctx, report); err != nil {
			if isTransientError(err) {
				taskLog.Warn("terminal recovery delivery failed; keeping pending report", "server_status", status, "outcome", "retry_later", "error", err)
				return
			}
			d.removePendingTerminalReport(report.taskID)
			taskLog.Error("terminal recovery delivery permanently rejected; dropping pending report", "server_status", status, "outcome", "dropped_permanent_rejection", "error", err)
			return
		}
		d.removePendingTerminalReport(report.taskID)
		taskLog.Info("terminal report recovery delivered", "server_status", status, "outcome", "delivered")

	case isAgentTaskTerminal(status):
		// Server authority: never overwrite a terminal state. A pending
		// completion meeting cancelled/failed/completed is dropped, so user
		// cancellation wins and a failed parent is never resurrected.
		d.removePendingTerminalReport(report.taskID)
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
