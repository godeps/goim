package goim

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

type identityRuntime func(context.Context, Request) (<-chan StreamEvent, error)

func (f identityRuntime) RunStream(ctx context.Context, req Request) (<-chan StreamEvent, error) {
	return f(ctx, req)
}

func awaitIdentityTurn(t *testing.T, s core.AgentSession) []core.Event {
	t.Helper()
	timeout := time.NewTimer(2 * time.Second)
	defer timeout.Stop()
	var events []core.Event
	for {
		select {
		case e, ok := <-s.Events():
			if !ok {
				t.Fatal("session closed before terminal")
			}
			events = append(events, e)
			if e.Done {
				return events
			}
		case <-timeout.C:
			t.Fatal("terminal not received")
		}
	}
}

func TestEmptySessionIDsCreateIndependentConversations(t *testing.T) {
	a := NewAgent(nil, "")
	defer a.Stop()
	const n = 32
	sessions := make(chan core.AgentSession, n)
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := a.StartSession(context.Background(), "")
			if err != nil {
				t.Error(err)
				return
			}
			sessions <- s
		}()
	}
	wg.Wait()
	close(sessions)
	seen := map[core.AgentSession]bool{}
	for s := range sessions {
		if seen[s] {
			t.Fatal("two new conversations share a session")
		}
		seen[s] = true
	}
	if len(seen) != n {
		t.Fatalf("got %d sessions, want %d", len(seen), n)
	}
	infos, err := a.ListSessions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 0 {
		t.Fatal("unassigned native sessions must not be advertised as resumable")
	}
}

func TestNativeSessionIDIsUsedForNextTurnAndResume(t *testing.T) {
	var mu sync.Mutex
	var requests []Request
	rt := identityRuntime(func(_ context.Context, r Request) (<-chan StreamEvent, error) {
		mu.Lock()
		requests = append(requests, r)
		mu.Unlock()
		id := r.SessionID
		if id == "" {
			id = "native-" + r.Prompt
		}
		out := make(chan StreamEvent, 1)
		out <- StreamEvent{Type: EventMessageStop, SessionID: id}
		close(out)
		return out, nil
	})
	a := NewAgent(rt, "")
	defer a.Stop()
	first, _ := a.StartSession(context.Background(), "")
	second, _ := a.StartSession(context.Background(), "")
	for _, step := range []struct {
		s      core.AgentSession
		prompt string
	}{{first, "first"}, {second, "second"}, {first, "again"}} {
		if err := step.s.Send(step.prompt, "", nil, nil); err != nil {
			t.Fatal(err)
		}
		awaitIdentityTurn(t, step.s)
	}
	if first.CurrentSessionID() != "native-first" || second.CurrentSessionID() != "native-second" {
		t.Fatal("native identities were not isolated")
	}
	resumed, err := a.StartSession(context.Background(), "native-first")
	if err != nil || resumed != first {
		t.Fatalf("resume created a second session: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if requests[2].SessionID != "native-first" {
		t.Fatalf("next request lost native ID: %#v", requests[2])
	}
}

func TestNativeSessionCollisionCannotReplaceLiveOwner(t *testing.T) {
	rt := identityRuntime(func(context.Context, Request) (<-chan StreamEvent, error) {
		out := make(chan StreamEvent, 1)
		out <- StreamEvent{Type: EventMessageStop, SessionID: "owned"}
		close(out)
		return out, nil
	})
	a := NewAgent(rt, "")
	defer a.Stop()
	owner, _ := a.StartSession(context.Background(), "owned")
	other, _ := a.StartSession(context.Background(), "")
	if err := other.Send("new", "", nil, nil); err != nil {
		t.Fatal(err)
	}
	events := awaitIdentityTurn(t, other)
	found := false
	for _, e := range events {
		if errors.Is(e.Error, ErrNativeSessionConflict) {
			found = true
		}
	}
	if !found {
		t.Fatal("missing native ID conflict")
	}
	if other.CurrentSessionID() != "" {
		t.Fatal("conflicting ID was adopted")
	}
	resumed, _ := a.StartSession(context.Background(), "owned")
	if resumed != owner {
		t.Fatal("existing live owner was replaced")
	}
}

func TestCanceledStreamCannotChangeNativeIdentity(t *testing.T) {
	s := newSession(nil, "current")
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.adoptSessionID(ctx, "stale"); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if s.CurrentSessionID() != "current" {
		t.Fatal("canceled stream changed current native ID")
	}
}

func TestNativeIdentityReadAndAdoptionAreSynchronized(t *testing.T) {
	s := newSession(nil, "a")
	defer s.Close()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 1000 {
			_ = s.CurrentSessionID()
		}
	}()
	go func() {
		defer wg.Done()
		for range 1000 {
			_ = s.adoptSessionID(context.Background(), "b")
		}
	}()
	wg.Wait()
}
