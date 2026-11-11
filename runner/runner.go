// Package runner adapts installed agent CLIs to a common streaming turn.
// Hosts authorize CWD, control the environment and supervise the process tree.
// This library contains no scheduler, listener or executable.
package runner

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os/exec"
	"sync"
	"time"
)

const (
	Completed = "completed"
	Failed    = "failed"
	Cancelled = "cancelled"
	Unknown   = "unknown"
)

type Event struct {
	Type, Text, SessionID, RequestID, Tool string
	Input                                  json.RawMessage
}
type ApprovalRequest struct {
	ID, Tool string
	Input    json.RawMessage
}
type Approver func(context.Context, ApprovalRequest) (bool, error)
type Emitter func(Event) error

// SessionID must come from the same service/account/conversation/agent/workspace.
type Request struct {
	Agent, Prompt, CWD, SessionID string
	Env                           map[string]string
	// CommandHook installs process-tree cancellation in cmd.Cancel before Start.
	CommandHook func(*exec.Cmd) error
	// StopProcess must stop and confirm the whole owned process tree.
	StopProcess func(*exec.Cmd) error
}
type Result struct {
	State, SessionID string
	Stopped          bool
}

// Executor never downloads a runtime.
type Executor struct{ CodexPath, ClaudePath string }

func (e *Executor) Run(ctx context.Context, req Request, emit Emitter, approve Approver) (Result, error) {
	if req.CommandHook == nil || req.StopProcess == nil || req.Env == nil || req.CWD == "" || req.Prompt == "" || emit == nil || approve == nil {
		return Result{State: Failed}, errors.New("runner requires local authorization, controlled environment, process supervision and approval callbacks")
	}
	if len(req.Prompt) > 1<<20 {
		return Result{State: Failed}, errors.New("prompt too large")
	}
	switch req.Agent {
	case "codex":
		if e.CodexPath == "" {
			return Result{State: Failed}, errors.New("codex CLI not configured")
		}
		return e.runCodex(ctx, req, emit, approve)
	case "claude":
		if e.ClaudePath == "" {
			return Result{State: Failed}, errors.New("claude CLI not configured")
		}
		return e.runClaude(ctx, req, emit, approve)
	default:
		return Result{State: Failed}, errors.New("unsupported local agent")
	}
}

type supervisedProcess struct {
	mu  sync.Mutex
	cmd *exec.Cmd
}

func (p *supervisedProcess) hook(next func(*exec.Cmd) error) func(*exec.Cmd) error {
	return func(cmd *exec.Cmd) error {
		if err := next(cmd); err != nil {
			return err
		}
		if cmd.Cancel == nil {
			return errors.New("process tree cancellation is required")
		}
		p.mu.Lock()
		p.cmd = cmd
		p.mu.Unlock()
		return nil
	}
}
func (p *supervisedProcess) stop(stop func(*exec.Cmd) error) error {
	p.mu.Lock()
	cmd := p.cmd
	p.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	return stop(cmd)
}

// A provider may ask twice for the same native item. Every callback needs a
// fresh approval nonce; an old allow must never approve a later callback.
func approvalID(_ string) string { return "req_" + rand.Text() }
func emitText(emit Emitter, text string) error {
	for len(text) > 0 {
		n := len(text)
		if n > 8192 {
			n = 8192
		}
		for n < len(text) && n > 0 && text[n]&0xc0 == 0x80 {
			n--
		}
		if err := emit(Event{Type: "text", Text: text[:n]}); err != nil {
			return err
		}
		text = text[n:]
	}
	return nil
}
func cleanupContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}
