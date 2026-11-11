package remote

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestFrozenBudgetAllowsUnknownThenKnownStopAtCapacity(t *testing.T) {
	f := newFixture(t)
	c, creds, _ := f.enroll("alice")
	f.service.options.Limits.MaxEventsPerRun = 2
	f.service.options.Limits.MaxRunEventBytes = 1024
	run := f.create("alice", creds, "frozen-budget", "")
	if run.Limits.MaxEventsPerRun != 2 || run.Limits.MaxRunEventBytes != 1024 || run.Limits.MaxFinalEvents != 2 {
		t.Fatalf("budget not negotiated: %+v", run.Limits)
	}
	// Changing the service's future defaults must not change the existing run.
	f.service.options.Limits.MaxEventsPerRun = 1
	_ = mustPoll(t, c, "budget-claim")
	appendBatch(t, c, run, Event{Seq: 1, Type: EventStarted}, Event{Seq: 2, Type: EventText, Text: "last ordinary event"})
	_, err := c.AppendEvents(context.Background(), run.ID, EventBatch{AttemptID: run.AttemptID, Fence: run.Fence, Events: []Event{{Seq: 3, Type: EventText, Text: "over limit"}}})
	wantCode(t, err, "event_capacity")
	receipt := appendBatch(t, c, run, Event{Seq: 3, Type: EventUnknown, Text: "termination could not yet be confirmed"})
	if receipt.State != StateUnknown {
		t.Fatal("unknown incorrectly terminal")
	}
	receipt = appendBatch(t, c, run, Event{Seq: 4, Type: EventCancelled, Text: "original process now confirmed reaped"})
	if receipt.State != StateCancelled || receipt.AckSeq != 4 {
		t.Fatalf("known-stop reserve unavailable: %+v", receipt)
	}
}

func TestCancellationAcceptsHistoricalStartAndApproval(t *testing.T) {
	f := newFixture(t)
	c, creds, _ := f.enroll("alice")
	run := f.create("alice", creds, "late-start", "")
	_ = mustPoll(t, c, "late-claim")
	if _, err := f.service.CancelRun(context.Background(), Principal{"alice"}, run.ID); err != nil {
		t.Fatal(err)
	}
	appendBatch(t, c, run,
		Event{Seq: 1, Type: EventStarted},
		Event{Seq: 2, Type: EventApproval, RequestID: "historic-request", Tool: "shell", Input: json.RawMessage(`{"command":"go test"}`)})
	ctl, err := c.Heartbeat(context.Background(), run.ID, HeartbeatRequest{AttemptID: run.AttemptID, Fence: run.Fence})
	if err != nil || !ctl.CancelRequested || len(ctl.Approvals) != 1 {
		t.Fatalf("historical controls lost: %+v %v", ctl, err)
	}
	approval := ctl.Approvals[0]
	_, err = f.service.Answer(context.Background(), Principal{"alice"}, run.ID, approval.RequestID, AnswerRequest{AttemptID: run.AttemptID, Digest: approval.Digest, Decision: "allow"})
	wantCode(t, err, "request_inactive")
	if got := appendBatch(t, c, run, Event{Seq: 3, Type: EventCancelled}); got.State != StateCancelled {
		t.Fatalf("outbox blocked: %+v", got)
	}
}

func TestOptionalHostRunAuthorization(t *testing.T) {
	f := newFixture(t)
	_, creds, _ := f.enroll("alice")
	f.service.options.AuthorizeRun = func(context.Context, Principal, CreateRunRequest) error { return errors.New("resource grant removed") }
	_, err := f.service.CreateRun(context.Background(), Principal{"alice"}, CreateRunRequest{IdempotencyKey: testKey("host-deny"), DeviceID: creds.DeviceID, Agent: "codex", Workspace: "repo", Prompt: "denied"})
	wantCode(t, err, "run_not_authorized")
	runs, err := f.service.Runs(context.Background(), Principal{"alice"})
	if err != nil || len(runs) != 0 {
		t.Fatalf("denied run persisted: %+v %v", runs, err)
	}
}

func TestCorruptStructuredStateFailsClosed(t *testing.T) {
	f := newFixture(t)
	_, _, _ = f.enroll("alice")
	key := f.service.key(hash("alice"))
	snapshot, err := f.service.store.Load(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	var st state
	if err = json.Unmarshal(snapshot.Data, &st); err != nil {
		t.Fatal(err)
	}
	st.Runs["bad"] = nil
	raw, _ := json.Marshal(st)
	if ok, err := f.service.store.CompareAndSwap(context.Background(), key, snapshot.Version, raw); err != nil || !ok {
		t.Fatalf("fixture: %v %v", ok, err)
	}
	_, err = f.service.Runs(context.Background(), Principal{"alice"})
	if err == nil {
		t.Fatal("invalid structured state accepted")
	}
}
