package agent

import (
	"context"
	"log/slog"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"
)

type cursorBackgroundTool struct {
	call    cursorToolCall
	process *cursorBackgroundProcess
}

// The daemon owns the only budget timer. This tracker owns process cleanup and
// the matching tool result, so expiration cannot race a second run-cancel timer.
type cursorBackgroundTools struct {
	ctx              context.Context
	cmd              *exec.Cmd
	messages         chan<- Message
	logger           *slog.Logger
	mu               sync.Mutex
	tools            []cursorBackgroundTool
	closed           bool
	stop             chan struct{}
	done             chan struct{}
	once             sync.Once
	inFlight         atomic.Int32
	lastToolActivity atomic.Int64
}

func newCursorBackgroundTools(ctx context.Context, cmd *exec.Cmd, messages chan<- Message, logger *slog.Logger) *cursorBackgroundTools {
	b := &cursorBackgroundTools{ctx: ctx, cmd: cmd, messages: messages, logger: logger, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(b.done)
		// This only observes natural process exit; it never measures inactivity.
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				b.Reap()
			case <-b.stop:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	return b
}

func (b *cursorBackgroundTools) Activity() (int32, time.Time) {
	return b.inFlight.Load(), time.Unix(0, b.lastToolActivity.Load())
}

func (b *cursorBackgroundTools) Send(msg Message) {
	if msg.Type == MessageToolUse || msg.Type == MessageToolResult {
		now := time.Now().UnixNano()
		for {
			previous := b.lastToolActivity.Load()
			if now <= previous || b.lastToolActivity.CompareAndSwap(previous, now) {
				break
			}
		}
	}
	if msg.Type == MessageToolUse {
		b.inFlight.Add(1)
	}
	// Publish completion before switching to the idle budget. Otherwise the
	// watchdog can see zero tools with the old timestamp and an empty queue.
	trySend(b.messages, msg)
	if msg.Type == MessageToolResult {
		for {
			count := b.inFlight.Load()
			if count <= 0 || b.inFlight.CompareAndSwap(count, count-1) {
				break
			}
		}
	}
}

func (b *cursorBackgroundTools) SendResult(call cursorToolCall) {
	b.Send(Message{Type: MessageToolResult, Tool: call.Name, CallID: call.CallID, Output: call.Result})
}

func (b *cursorBackgroundTools) Add(call cursorToolCall) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	for _, tool := range b.tools {
		if call.CallID != "" && tool.call.CallID == call.CallID {
			return
		}
	}
	p, err := captureCursorBackgroundProcess(b.cmd, call.PID)
	if err != nil {
		// Keep the tool in flight even if the payload cannot be safely acted on.
		// At timeout the ordinary watchdog remains the fallback; never signal an
		// unverified PID or treat unverifiable cleanup as success.
		b.logger.Warn("cannot own Cursor background shell; tool watchdog will use normal cancellation", "error", err)
	}
	b.tools = append(b.tools, cursorBackgroundTool{call: call, process: p})
}

func (b *cursorBackgroundTools) Reap() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.closed {
		b.finish(false)
	}
}

// Interrupt is called synchronously by the daemon's tool watchdog. Each tool
// can grant a recovery window only once: it is removed with its tool result.
func (b *cursorBackgroundTools) Interrupt() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.ctx.Err() != nil {
		return false
	}
	return b.finish(true)
}

// finish is called with mu held. A failed ownership check or cleanup never
// decrements the tool count; only confirmed process exit releases its result.
func (b *cursorBackgroundTools) finish(interrupt bool) bool {
	completed := false
	remaining := b.tools[:0]
	for _, tool := range b.tools {
		if tool.process == nil {
			remaining = append(remaining, tool)
			continue
		}
		alive, err := tool.process.Alive()
		if err == nil && alive && interrupt {
			err = tool.process.Terminate()
			if err == nil {
				alive, err = tool.process.Alive()
			}
		}
		if err != nil || alive {
			if err != nil && interrupt {
				b.logger.Warn("could not stop Cursor background shell", "error", err)
			}
			remaining = append(remaining, tool)
			continue
		}
		b.SendResult(tool.call)
		tool.process.Close()
		completed = true
	}
	b.tools = remaining
	return completed
}

func (b *cursorBackgroundTools) Close() {
	b.once.Do(func() {
		b.mu.Lock()
		b.closed = true
		b.finish(true)
		for _, tool := range b.tools {
			// The stream is closing, so preserve even unverifiable launch
			// payloads in the transcript without granting watchdog recovery.
			b.SendResult(tool.call)
			tool.process.Close()
		}
		b.tools = nil
		b.mu.Unlock()
		close(b.stop)
		<-b.done
	})
}
