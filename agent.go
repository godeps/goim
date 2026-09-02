package goim

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"

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
}

func newSession(rt Runtime, sessionID string) *Session {
	s := &Session{
		runtime:   rt,
		sessionID: sessionID,
		events:    make(chan core.Event, 128),
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
	var textBuf strings.Builder
	var lastSessionID string

	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-stream:
			if !ok {
				// Stream ended — emit final result.
				text := textBuf.String()
				if text != "" {
					s.emitEvent(core.Event{
						Type:      core.EventResult,
						Content:   text,
						SessionID: lastSessionID,
						Done:      true,
					})
				}
				return
			}

			if evt.SessionID != "" {
				lastSessionID = evt.SessionID
			}

			switch evt.Type {
			case EventContentBlockDelta:
				if evt.Delta != nil && evt.Delta.Text != "" {
					textBuf.WriteString(evt.Delta.Text)
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
				text := textBuf.String()
				textBuf.Reset()
				s.emitEvent(core.Event{
					Type:      core.EventResult,
					Content:   text,
					SessionID: lastSessionID,
					Done:      true,
				})

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
			}
		}
	}
}

func (s *Session) emitEvent(evt core.Event) {
	select {
	case s.events <- evt:
	default:
		slog.Warn("goim: event channel full, dropping event", "type", evt.Type)
	}
}

func (s *Session) RespondPermission(_ string, _ core.PermissionResult) error {
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
	s.mu.Lock()
	if s.cancel != nil {
		s.cancel()
	}
	s.mu.Unlock()
	close(s.events)
	return nil
}
