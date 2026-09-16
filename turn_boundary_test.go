package goim

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/godeps/cc-connect/core"
)

// turnScriptRuntime replays a fixed event script for one message and records
// the permission decisions the engine reaches on its own.
type turnScriptRuntime struct {
	events []StreamEvent

	mu       sync.Mutex
	resolved []core.PermissionResult
}

func (r *turnScriptRuntime) RunStream(_ context.Context, _ Request) (<-chan StreamEvent, error) {
	ch := make(chan StreamEvent, len(r.events))
	for _, evt := range r.events {
		ch <- evt
	}
	close(ch)
	return ch, nil
}

// ResolvePermission makes this runtime a PermissionResolver, which makes a
// decision the engine reached without asking anyone observable. An automatic
// deny is exactly what the unsolicited reader sends.
func (r *turnScriptRuntime) ResolvePermission(_ string, result core.PermissionResult) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resolved = append(r.resolved, result)
	return nil
}

func (r *turnScriptRuntime) decisions() []core.PermissionResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]core.PermissionResult(nil), r.resolved...)
}

// capturePlatform keeps the message handler the engine installs, so a test can
// deliver a message the way a real platform does.
type capturePlatform struct {
	*sendStubPlatform
	handler core.MessageHandler
}

func (p *capturePlatform) Start(h core.MessageHandler) error {
	p.handler = h
	return nil
}

// TestTurnStaysOpenForToolPermission is the regression test for the gateway's
// "denied: no active user turn" failures.
//
// saker closes each assistant message with a message_stop and only then runs
// that iteration's tool calls. A turn that read message_stop as terminal ended
// before its own tool approval request arrived, so the request fell through to
// the unsolicited reader — which has no user turn to consult and denies it
// outright. With the boundary moved to the closed stream, the foreground turn
// is still running when the request arrives and must ask the user instead of
// deciding for them.
func TestTurnStaysOpenForToolPermission(t *testing.T) {
	const tool = "image_generate"
	runtime := &turnScriptRuntime{events: []StreamEvent{
		{Type: EventContentBlockDelta, Delta: &Delta{Text: "working on it"}, SessionID: testSessionKey},
		{Type: EventMessageStop, SessionID: testSessionKey},
		{Type: EventToolExecutionStart, Name: tool, SessionID: testSessionKey},
		{Type: EventPermissionRequest, Name: tool, RequestID: "req-1", SessionID: testSessionKey},
		{Type: EventToolExecutionResult, Name: tool, Output: "ok", SessionID: testSessionKey},
		{Type: EventContentBlockDelta, Delta: &Delta{Text: "all done"}, SessionID: testSessionKey},
		{Type: EventMessageStop, SessionID: testSessionKey},
	}}

	platform := &capturePlatform{sendStubPlatform: newSendStubPlatform("stub")}
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Project.Name = "test"
	engine := NewEngine(NewAgent(runtime, "test"), []core.Platform{platform}, cfg)
	defer func() { _ = engine.Stop() }()

	if err := engine.Start(); err != nil {
		t.Fatalf("start engine: %v", err)
	}
	if platform.handler == nil {
		t.Fatal("engine did not install a message handler")
	}
	platform.handler(platform, &core.Message{
		SessionKey: testSessionKey,
		Platform:   "stub",
		MessageID:  "m1",
		UserID:     "user",
		UserName:   "user",
		Content:    "generate an image",
		ReplyCtx:   testSessionKey,
	})

	// The prompt reaches the user from the turn's goroutine, so wait for it
	// rather than assuming it is already there.
	deadline := time.After(5 * time.Second)
	for {
		texts, _, _, _, _ := platform.snapshot()
		if len(texts) > 0 {
			if !strings.Contains(strings.Join(texts, "\n"), tool) {
				t.Errorf("prompt does not name the tool %q: %q", tool, texts)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("the tool permission request never reached the user: the turn had already closed when it arrived")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// The decision belongs to the user. Sending one back unasked is precisely
	// what the unsolicited reader does, and what this fix exists to prevent.
	if decisions := runtime.decisions(); len(decisions) != 0 {
		t.Errorf("engine decided on the user's behalf: %+v", decisions)
	}
}