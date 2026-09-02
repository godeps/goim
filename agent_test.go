package goim

import (
	"context"
	"testing"
	"time"

	"github.com/godeps/cc-connect/core"
)

func TestAgent_Name(t *testing.T) {
	a := NewAgent(nil, "")
	if a.Name() != "goim" {
		t.Errorf("expected name 'goim', got %q", a.Name())
	}

	a2 := NewAgent(nil, "mybot")
	if a2.Name() != "mybot" {
		t.Errorf("expected name 'mybot', got %q", a2.Name())
	}
}

func TestAgent_StartSession(t *testing.T) {
	a := NewAgent(nil, "")
	s, err := a.StartSession(context.Background(), "test-session")
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if s.CurrentSessionID() != "test-session" {
		t.Errorf("expected session ID 'test-session', got %q", s.CurrentSessionID())
	}
	if !s.Alive() {
		t.Error("expected session to be alive")
	}

	// StartSession with same ID returns same session.
	s2, err := a.StartSession(context.Background(), "test-session")
	if err != nil {
		t.Fatalf("StartSession (reuse): %v", err)
	}
	if s2 != s {
		t.Error("expected same session instance for same ID")
	}

	// Close and verify.
	s.Close()
	if s.Alive() {
		t.Error("expected session to be dead after Close")
	}
}

func TestAgent_ListSessions(t *testing.T) {
	a := NewAgent(nil, "")
	_, _ = a.StartSession(context.Background(), "s1")
	_, _ = a.StartSession(context.Background(), "s2")

	infos, err := a.ListSessions(context.Background())
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(infos) != 2 {
		t.Errorf("expected 2 sessions, got %d", len(infos))
	}
}

func TestAgent_Stop(t *testing.T) {
	a := NewAgent(nil, "")
	s, _ := a.StartSession(context.Background(), "s1")
	if err := a.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if s.Alive() {
		t.Error("expected session to be dead after Stop")
	}
}

func TestSession_ConsumeStream_TextEvents(t *testing.T) {
	ch := make(chan StreamEvent, 3)
	ch <- StreamEvent{Type: EventContentBlockDelta, Delta: &Delta{Text: "Hello "}, SessionID: "s1"}
	ch <- StreamEvent{Type: EventContentBlockDelta, Delta: &Delta{Text: "world"}, SessionID: "s1"}
	ch <- StreamEvent{Type: EventMessageStop, SessionID: "s1"}
	close(ch)

	s := newSession(nil, "s1")
	go s.consumeStream(context.Background(), ch)

	var events []core.Event
	timeout := time.After(2 * time.Second)
	for {
		select {
		case evt, ok := <-s.events:
			if !ok {
				goto done
			}
			events = append(events, evt)
			if evt.Done {
				goto done
			}
		case <-timeout:
			t.Fatal("timeout waiting for events")
		}
	}
done:

	if len(events) < 3 {
		t.Fatalf("expected at least 3 events, got %d", len(events))
	}

	if events[0].Type != core.EventText || events[0].Content != "Hello " {
		t.Errorf("event[0]: expected text 'Hello ', got %q %q", events[0].Type, events[0].Content)
	}
	if events[1].Type != core.EventText || events[1].Content != "world" {
		t.Errorf("event[1]: expected text 'world', got %q %q", events[1].Type, events[1].Content)
	}

	last := events[len(events)-1]
	if last.Type != core.EventResult || !last.Done {
		t.Errorf("last event: expected result with done=true, got type=%q done=%v", last.Type, last.Done)
	}
	if last.Content != "Hello world" {
		t.Errorf("last event content: expected 'Hello world', got %q", last.Content)
	}
}

func TestSession_ConsumeStream_ToolEvents(t *testing.T) {
	ch := make(chan StreamEvent, 4)
	ch <- StreamEvent{Type: EventToolExecutionStart, Name: "bash", SessionID: "s1"}
	ch <- StreamEvent{Type: EventToolExecutionResult, Name: "bash", Output: "ok", SessionID: "s1"}
	ch <- StreamEvent{Type: EventMessageStop, SessionID: "s1"}
	close(ch)

	s := newSession(nil, "s1")
	go s.consumeStream(context.Background(), ch)

	var events []core.Event
	timeout := time.After(2 * time.Second)
	for {
		select {
		case evt, ok := <-s.events:
			if !ok {
				goto done
			}
			events = append(events, evt)
			if evt.Done {
				goto done
			}
		case <-timeout:
			t.Fatal("timeout waiting for events")
		}
	}
done:

	if len(events) < 3 {
		t.Fatalf("expected at least 3 events, got %d", len(events))
	}
	if events[0].Type != core.EventToolUse || events[0].ToolName != "bash" {
		t.Errorf("event[0]: expected tool_use 'bash', got %q %q", events[0].Type, events[0].ToolName)
	}
	if events[1].Type != core.EventToolResult || events[1].ToolName != "bash" {
		t.Errorf("event[1]: expected tool_result 'bash', got %q %q", events[1].Type, events[1].ToolName)
	}
}

func TestSession_ConsumeStream_ErrorEvent(t *testing.T) {
	ch := make(chan StreamEvent, 2)
	ch <- StreamEvent{
		Type:      EventError,
		Output:    "something broke",
		SessionID: "s1",
	}
	close(ch)

	s := newSession(nil, "s1")
	go s.consumeStream(context.Background(), ch)

	timeout := time.After(2 * time.Second)
	select {
	case evt := <-s.events:
		if evt.Type != core.EventError {
			t.Errorf("expected error event, got %q", evt.Type)
		}
		if evt.Content != "something broke" {
			t.Errorf("expected error content 'something broke', got %q", evt.Content)
		}
	case <-timeout:
		t.Fatal("timeout waiting for error event")
	}
}

func TestSession_Close_Idempotent(t *testing.T) {
	s := newSession(nil, "s1")
	if err := s.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close should be no-op: %v", err)
	}
}

func TestSession_RespondPermission_NoOp(t *testing.T) {
	s := newSession(nil, "s1")
	defer s.Close()
	err := s.RespondPermission("req-1", core.PermissionResult{Behavior: "allow"})
	if err != nil {
		t.Errorf("RespondPermission should be no-op, got: %v", err)
	}
}
