package goim

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/godeps/cc-connect/core"
)

// Agent implements core.Agent by wrapping a Runtime.
type Agent struct {
	runtime Runtime
	name    string

	mu       sync.Mutex
	sessions map[string]*Session // sessionID -> active session
}

// NewAgent creates a new Agent adapter around an existing Runtime.
func NewAgent(rt Runtime, name string) *Agent {
	if name == "" {
		name = "goim"
	}
	return &Agent{
		runtime:  rt,
		name:     name,
		sessions: make(map[string]*Session),
	}
}

func (a *Agent) Name() string { return a.name }

func (a *Agent) StartSession(ctx context.Context, sessionID string) (core.AgentSession, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Reuse existing session if alive.
	if s, ok := a.sessions[sessionID]; ok && s.Alive() {
		return s, nil
	}

	s := newSession(a.runtime, sessionID)
	a.sessions[sessionID] = s
	return s, nil
}

func (a *Agent) ListSessions(_ context.Context) ([]core.AgentSessionInfo, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var infos []core.AgentSessionInfo
	for id, s := range a.sessions {
		if s.Alive() {
			infos = append(infos, core.AgentSessionInfo{ID: id})
		}
	}
	return infos, nil
}

func (a *Agent) Stop() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for id, s := range a.sessions {
		s.Close()
		delete(a.sessions, id)
	}
	return nil
}

// Session implements core.AgentSession by calling Runtime.RunStream for each
// user message and converting StreamEvent to core.Event.
type Session struct {
	runtime   Runtime
	sessionID string
	events    chan core.Event
	alive     atomic.Bool
	cancel    context.CancelFunc
	mu        sync.Mutex
	eventMu   sync.RWMutex
	done      chan struct{}
}

func newSession(rt Runtime, sessionID string) *Session {
	s := &Session{
		runtime:   rt,
		sessionID: sessionID,
		events:    make(chan core.Event, 128),
		done:      make(chan struct{}),
	}
	s.alive.Store(true)
	return s
}

func (s *Session) Send(prompt string, messageID string, images []core.ImageAttachment, files []core.FileAttachment) error {
	if !s.Alive() {
		return fmt.Errorf("session %s is closed", s.sessionID)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	if !s.Alive() {
		s.mu.Unlock()
		cancel()
		return fmt.Errorf("session %s is closed", s.sessionID)
	}
	// Cancel any previous in-flight stream for this session.
	if s.cancel != nil {
		s.cancel()
	}
	s.cancel = cancel
	s.mu.Unlock()

	req := Request{
		Prompt:    prompt,
		SessionID: s.sessionID,
		MessageID: messageID,
	}

	// Convert images to multimodal content blocks (base64-encoded).
	for _, img := range images {
		req.ContentBlocks = append(req.ContentBlocks, ContentBlock{
			Type:      "image",
			MediaType: img.MimeType,
			Data:      base64.StdEncoding.EncodeToString(img.Data),
		})
	}

	// Convert document files to "document" content blocks so runtimes can
	// receive attachments alongside the prompt (images stay "image" blocks).
	for _, f := range files {
		req.ContentBlocks = append(req.ContentBlocks, ContentBlock{
			Type:      "document",
			MediaType: f.MimeType,
			FileName:  f.FileName,
			Data:      base64.StdEncoding.EncodeToString(f.Data),
		})
	}

	stream, err := s.runtime.RunStream(ctx, req)
	if err != nil {
		cancel()
		return fmt.Errorf("start stream: %w", err)
	}

	// Consume stream events in background, convert to core.Event.
	go s.consumeStream(ctx, stream)
	return nil
}

func (s *Session) consumeStream(ctx context.Context, stream <-chan StreamEvent) {
	// textBuf holds the assistant segment being accumulated; a message_stop
	// closes one segment. turnBuf holds every segment of the turn, because the
	// terminal result has to carry the whole turn's text — cc-connect renders
	// the final reply from it, and the last segment is only part of the answer.
	var textBuf strings.Builder
	var turnBuf strings.Builder
	var lastSessionID string

	for {
		select {
		case <-ctx.Done():
			// The stream was cancelled mid-turn — a later Send superseded it,
			// or the session closed. Emit the terminal result anyway so a
			// foreground turn still reading this session is released instead of
			// waiting out its idle timeout. Best-effort: when the session is
			// closing there is nobody left to read it.
			s.emitEvent(core.Event{
				Type:      core.EventResult,
				Content:   turnBuf.String(),
				SessionID: lastSessionID,
				Done:      true,
			})
			return
		case evt, ok := <-stream:
			if !ok {
				// Stream ended — this is the turn boundary, and the only place
				// a terminal result is emitted. It goes out even when the turn
				// produced no text (tool calls only): cc-connect's turn loop
				// exits solely on Done=true, so suppressing it would leave the
				// turn open until it exhausts its idle timeout.
				s.emitTerminal(ctx, core.Event{
					Type:      core.EventResult,
					Content:   turnBuf.String(),
					SessionID: lastSessionID,
					Done:      true,
				})
				return
			}

			if evt.SessionID != "" {
				lastSessionID = evt.SessionID
			}

			switch evt.Type {
			case EventContentBlockDelta:
				if evt.Delta != nil && evt.Delta.Text != "" {
					textBuf.WriteString(evt.Delta.Text)
					turnBuf.WriteString(evt.Delta.Text)
					s.emitEvent(core.Event{
						Type:      core.EventText,
						Content:   evt.Delta.Text,
						SessionID: lastSessionID,
					})
				}

			case EventToolExecutionStart:
				s.emitEvent(core.Event{
					Type:      core.EventToolUse,
					ToolName:  evt.Name,
					SessionID: lastSessionID,
				})

			case EventToolExecutionResult:
				output := ""
				if evt.Output != nil {
					output = fmt.Sprintf("%v", evt.Output)
				}
				s.emitEvent(core.Event{
					Type:       core.EventToolResult,
					ToolName:   evt.Name,
					ToolResult: output,
					SessionID:  lastSessionID,
				})

			case EventError:
				errMsg := ""
				if evt.Output != nil {
					errMsg = fmt.Sprintf("%v", evt.Output)
				}
				s.emitEvent(core.Event{
					Type:      core.EventError,
					Content:   errMsg,
					Error:     fmt.Errorf("%s", errMsg),
					SessionID: lastSessionID,
				})

			case EventMessageStop:
				// Not a turn boundary. saker's kernels emit message_stop at the
				// end of each assistant message and only then run that
				// iteration's tool calls, so reporting Done=true here ended
				// cc-connect's turn mid-flight: the user received the
				// intermediate segment as their answer, and every tool approval
				// request that followed landed in the unsolicited reader, which
				// has no user turn to consult and denies them outright.
				// Done=false is cc-connect's non-terminal result — its
				// EventResult case continues reading the same turn — so the
				// turn stays open until the stream closes.
				s.emitEvent(core.Event{
					Type:      core.EventResult,
					Content:   textBuf.String(),
					SessionID: lastSessionID,
					Done:      false,
				})
				textBuf.Reset()

			case EventToolExecutionOutput:
				output := ""
				if evt.Output != nil {
					output = fmt.Sprintf("%v", evt.Output)
				}
				if output != "" {
					s.emitEvent(core.Event{
						Type:       core.EventToolResult,
						ToolName:   evt.Name,
						ToolResult: output,
						SessionID:  lastSessionID,
					})
				}

			case EventPermissionRequest:
				var rawInput map[string]any
				if m, ok := evt.ToolInputRaw.(map[string]any); ok {
					rawInput = m
				}
				s.emitPermission(ctx, core.Event{
					Type:         core.EventPermissionRequest,
					ToolName:     evt.Name,
					ToolInputRaw: rawInput,
					RequestID:    evt.RequestID,
					SessionID:    lastSessionID,
				})
			}
		}
	}
}

func (s *Session) emitEvent(evt core.Event) {
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	if !s.Alive() {
		return
	}
	select {
	case s.events <- evt:
	default:
		slog.Warn("goim: event channel full, dropping event", "type", evt.Type)
	}
}

// emitPermission applies backpressure because dropping approval requests leaves
// the runtime waiting for a decision the user cannot make.
func (s *Session) emitPermission(ctx context.Context, evt core.Event) {
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	if !s.Alive() {
		return
	}
	select {
	case s.events <- evt:
	case <-ctx.Done():
	case <-s.done:
	}
}

// terminalDeliveryWait bounds how long the turn's terminal result waits for a
// reader. Delivery is immediate whenever a turn is reading the session, so
// reaching this deadline means there is no turn left to close.
const terminalDeliveryWait = 10 * time.Second

// emitTerminal delivers the result that ends a turn. Unlike emitEvent it does
// not drop the event when the buffer is full: this is the only signal
// cc-connect's turn loop exits on, so losing it strands the turn until its idle
// timeout expires. The wait is bounded because a buffer that stays full means
// no turn is reading — a send with no receiver to wait for.
func (s *Session) emitTerminal(ctx context.Context, evt core.Event) {
	s.eventMu.RLock()
	defer s.eventMu.RUnlock()
	if !s.Alive() {
		return
	}
	timer := time.NewTimer(terminalDeliveryWait)
	defer timer.Stop()
	select {
	case s.events <- evt:
	case <-s.done:
	case <-ctx.Done():
	case <-timer.C:
		slog.Error("goim: terminal result undelivered; the turn stays open until its idle timeout",
			"session", s.sessionID, "waited", terminalDeliveryWait)
	}
}

// PermissionResolver is optionally implemented by a Runtime to receive
// user permission decisions (Allow/Deny) from the IM platform.
type PermissionResolver interface {
	ResolvePermission(reqID string, result core.PermissionResult) error
}

func (s *Session) RespondPermission(reqID string, result core.PermissionResult) error {
	if rp, ok := s.runtime.(PermissionResolver); ok {
		return rp.ResolvePermission(reqID, result)
	}
	return nil
}

func (s *Session) Events() <-chan core.Event {
	return s.events
}

func (s *Session) CurrentSessionID() string {
	return s.sessionID
}

func (s *Session) Alive() bool {
	return s.alive.Load()
}

func (s *Session) Close() error {
	if !s.alive.CompareAndSwap(true, false) {
		return nil
	}
	close(s.done)
	s.mu.Lock()
	if s.cancel != nil {
		s.cancel()
	}
	s.mu.Unlock()
	s.eventMu.Lock()
	close(s.events)
	s.eventMu.Unlock()
	return nil
}
