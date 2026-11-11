package remote

import (
	"context"
	"testing"
	"time"
)

func TestRetentionRejectsOldIntentAfterSafeCleanup(t *testing.T) {
	f := newFixture(t)
	c, creds, _ := f.enroll("alice")
	ctx := context.Background()
	owner := Principal{"alice"}
	run := f.create("alice", creds, "expires", "")
	_ = mustPoll(t, c, "old-claim")
	appendBatch(t, c, run, Event{Seq: 1, Type: EventStarted, SessionID: "native-resume"}, Event{Seq: 2, Type: EventCompleted})
	f.clock.add(25 * time.Hour)
	policy, err := f.service.Policy(ctx, owner)
	if err != nil || policy.Runs != 0 || policy.Sessions != 1 {
		t.Fatalf("unsafe or missing pruning: %+v %v", policy, err)
	}
	_, err = f.service.CreateRun(ctx, owner, CreateRunRequest{IdempotencyKey: testKey("expires"), DeviceID: creds.DeviceID, Agent: "codex", Workspace: "repo", Prompt: "Inspect the tests"})
	wantCode(t, err, "request_expired")
	_, err = c.Poll(ctx, PollRequest{ID: testPoll("old-claim")})
	wantCode(t, err, "claim_expired")
	key, err := newTimedID("rk1", f.clock.now())
	if err != nil {
		t.Fatal(err)
	}
	follow, err := f.service.CreateRun(ctx, owner, CreateRunRequest{IdempotencyKey: key, DeviceID: creds.DeviceID, Agent: "codex", Workspace: "repo", Prompt: "A new explicit turn", SessionID: run.SessionID})
	if err != nil || follow.NativeSessionID != "native-resume" {
		t.Fatalf("retained session unusable: %+v %v", follow, err)
	}
	if _, err = f.service.CancelRun(ctx, owner, follow.ID); err != nil {
		t.Fatal(err)
	}
	f.clock.add(8 * 24 * time.Hour)
	policy, err = f.service.Policy(ctx, owner)
	if err != nil || policy.Sessions != 0 {
		t.Fatalf("idle session retention: %+v %v", policy, err)
	}
	key, _ = newTimedID("rk1", f.clock.now())
	_, err = f.service.CreateRun(ctx, owner, CreateRunRequest{IdempotencyKey: key, DeviceID: creds.DeviceID, Agent: "codex", Workspace: "repo", Prompt: "Should not silently start fresh", SessionID: run.SessionID})
	wantCode(t, err, "session_expired")
}

func TestActiveAndUnknownNeverPrunedAndFutureKeysRejected(t *testing.T) {
	f := newFixture(t)
	c, creds, _ := f.enroll("alice")
	ctx := context.Background()
	owner := Principal{"alice"}
	run := f.create("alice", creds, "long-lived", "")
	_ = mustPoll(t, c, "long-lived-claim")
	f.clock.add(40 * 24 * time.Hour)
	policy, err := f.service.Policy(ctx, owner)
	if err != nil || policy.Runs != 1 || policy.Sessions != 1 {
		t.Fatalf("unknown was pruned: %+v %v", policy, err)
	}
	old, err := c.Poll(ctx, PollRequest{ID: testPoll("long-lived-claim")})
	if err != nil || old.Assignment == nil || old.Assignment.Run.ID != run.ID || old.Assignment.Run.State != StateUnknown {
		t.Fatalf("old exact claim not recoverable: %+v %v", old, err)
	}
	req := CreateRunRequest{IdempotencyKey: testKey("long-lived"), DeviceID: creds.DeviceID, Agent: "codex", Workspace: "repo", Prompt: "Inspect the tests"}
	retry, err := f.service.CreateRun(ctx, owner, req)
	if err != nil || retry.ID != run.ID {
		t.Fatalf("active idempotency expired: %+v %v", retry, err)
	}
	req.Prompt = "Changed intent"
	_, err = f.service.CreateRun(ctx, owner, req)
	wantCode(t, err, "idempotency_conflict")
	key, _ := newTimedID("rk1", f.clock.now().Add(6*time.Minute))
	req.IdempotencyKey = key
	_, err = f.service.CreateRun(ctx, owner, req)
	wantCode(t, err, "request_from_future")
	poll, _ := newTimedID("pc1", f.clock.now().Add(6*time.Minute))
	_, err = c.Poll(ctx, PollRequest{ID: poll})
	wantCode(t, err, "request_from_future")
}
