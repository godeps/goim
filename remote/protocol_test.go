package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var testEpoch = time.Now().Unix()

func testKey(seed string) string {
	return "rk1." + strconv.FormatInt(testEpoch, 10) + "." + hash(seed)[:32]
}
func testPoll(seed string) string {
	return "pc1." + strconv.FormatInt(testEpoch, 10) + "." + hash(seed)[:32]
}

type testClock struct{ nanos atomic.Int64 }

func newClock() *testClock               { c := &testClock{}; c.nanos.Store(time.Now().UTC().UnixNano()); return c }
func (c *testClock) now() time.Time      { return time.Unix(0, c.nanos.Load()).UTC() }
func (c *testClock) add(d time.Duration) { c.nanos.Add(int64(d)) }

type fixture struct {
	t        *testing.T
	dir      string
	service  *Service
	server   *httptest.Server
	clock    *testClock
	disabled *atomic.Bool
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{t: t, dir: t.TempDir(), clock: newClock(), disabled: &atomic.Bool{}}
	if err := os.Chmod(f.dir, 0700); err != nil {
		t.Fatal(err)
	}
	f.open()
	t.Cleanup(func() { f.server.Close() })
	return f
}
func (f *fixture) open() {
	f.t.Helper()
	store, err := NewFileStore(f.dir)
	if err != nil {
		f.t.Fatal(err)
	}
	f.service, err = NewService(store, Options{Now: f.clock.now, LeaseDuration: 10 * time.Second, ValidatePrincipal: func(_ context.Context, p Principal) error {
		if f.disabled.Load() {
			return errors.New("host account disabled")
		}
		return nil
	}})
	if err != nil {
		f.t.Fatal(err)
	}
	f.server = httptest.NewTLSServer(http.StripPrefix("/api/agents/v1", NewHandler(f.service, HTTPOptions{Authenticate: func(r *http.Request) (Principal, error) {
		if r.Header.Get("X-Test-User") == "" {
			return Principal{}, errors.New("no session")
		}
		return Principal{ID: r.Header.Get("X-Test-User")}, nil
	}})))
}
func (f *fixture) request(method, route, principal string, body any) (int, []byte) {
	f.t.Helper()
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		if err != nil {
			f.t.Fatal(err)
		}
	}
	req, err := http.NewRequest(method, f.server.URL+"/api/agents/v1"+route, bytes.NewReader(raw))
	if err != nil {
		f.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Test-User", principal)
	res, err := f.server.Client().Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		f.t.Fatal(err)
	}
	return res.StatusCode, data
}
func (f *fixture) enroll(principal string) (*Client, Credentials, EnrollmentRequest) {
	f.t.Helper()
	status, raw := f.request("POST", "/pairings", principal, PairingRequest{Name: "laptop"})
	if status != 200 {
		f.t.Fatalf("pairing: %d %s", status, raw)
	}
	var pair Pairing
	if err := json.Unmarshal(raw, &pair); err != nil {
		f.t.Fatal(err)
	}
	req, creds, err := NewEnrollment(pair.Code, "local laptop", []Agent{{Name: "codex", Capabilities: []string{"chat", "approvals"}}}, []Workspace{{ID: "repo", Name: "My repository"}})
	if err != nil {
		f.t.Fatal(err)
	}
	req.ExpectedPrincipal = principal
	client, err := NewClient(f.server.URL+"/api/agents/v1", creds, ClientOptions{HTTPClient: f.server.Client()})
	if err != nil {
		f.t.Fatal(err)
	}
	d, err := client.Enroll(context.Background(), req)
	if err != nil {
		f.t.Fatal(err)
	}
	if d.PrincipalID != principal || d.ID != creds.DeviceID {
		f.t.Fatalf("bad enrollment: %+v", d)
	}
	return client, creds, req
}
func (f *fixture) create(principal string, creds Credentials, key string, session string) Run {
	f.t.Helper()
	status, raw := f.request("POST", "/runs", principal, CreateRunRequest{IdempotencyKey: testKey(key), DeviceID: creds.DeviceID, Agent: "codex", Workspace: "repo", Prompt: "Inspect the tests", SessionID: session})
	if status != 200 {
		f.t.Fatalf("create: %d %s", status, raw)
	}
	var r Run
	if err := json.Unmarshal(raw, &r); err != nil {
		f.t.Fatal(err)
	}
	return r
}
func mustPoll(t *testing.T, c *Client, id string) Run {
	t.Helper()
	p, err := c.Poll(context.Background(), PollRequest{ID: testPoll(id)})
	if err != nil || p.Assignment == nil {
		t.Fatalf("poll: %+v %v", p, err)
	}
	return p.Assignment.Run
}
func appendBatch(t *testing.T, c *Client, r Run, events ...Event) Receipt {
	t.Helper()
	receipt, err := c.AppendEvents(context.Background(), r.ID, EventBatch{AttemptID: r.AttemptID, Fence: r.Fence, Events: events})
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}
func wantCode(t *testing.T, err error, code string) {
	t.Helper()
	if ErrorCode(err) != code {
		t.Fatalf("got error %v (%s), want %s", err, ErrorCode(err), code)
	}
}

type loseResponse struct {
	base  http.RoundTripper
	route string
	once  atomic.Bool
}

func (l *loseResponse) RoundTrip(r *http.Request) (*http.Response, error) {
	response, err := l.base.RoundTrip(r)
	if err == nil && r.URL.Path == l.route && l.once.CompareAndSwap(false, true) {
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
		return nil, errors.New("simulated lost response after commit")
	}
	return response, err
}
func droppingClient(t *testing.T, f *fixture, creds Credentials, route string) *Client {
	t.Helper()
	hc := *f.server.Client()
	hc.Transport = &loseResponse{base: hc.Transport, route: "/api/agents/v1" + route}
	c, err := NewClient(f.server.URL+"/api/agents/v1", creds, ClientOptions{HTTPClient: &hc})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestHTTPDurabilityLostResponsesAndRestart(t *testing.T) {
	f := newFixture(t)
	status, raw := f.request("POST", "/pairings", "alice", PairingRequest{})
	if status != 200 {
		t.Fatal(string(raw))
	}
	var pair Pairing
	json.Unmarshal(raw, &pair)
	enrollment, creds, err := NewEnrollment(pair.Code, "laptop", []Agent{{Name: "codex"}}, []Workspace{{ID: "repo", Name: "Repository"}})
	if err != nil {
		t.Fatal(err)
	}
	enrollment.ExpectedPrincipal = "alice"
	// The local enrollment is committed before the first network side effect.
	journal, err := json.Marshal(struct {
		Enrollment  EnrollmentRequest
		Credentials Credentials
	}{enrollment, creds})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(f.dir, "enrollment-private"), journal, 0600); err != nil {
		t.Fatal(err)
	}
	c := droppingClient(t, f, creds, "/devices/enroll")
	if _, err = c.Enroll(context.Background(), enrollment); err == nil {
		t.Fatal("expected lost enrollment response")
	}
	d, err := c.Enroll(context.Background(), enrollment)
	if err != nil {
		t.Fatal(err)
	}
	if d.ID != creds.DeviceID {
		t.Fatal("enrollment changed identity")
	}
	replacement, _, err := NewEnrollment(pair.Code, "replacement", []Agent{{Name: "codex"}}, []Workspace{{ID: "repo", Name: "Repository"}})
	if err != nil {
		t.Fatal(err)
	}
	replacement.ExpectedPrincipal = "alice"
	_, err = f.service.Enroll(context.Background(), replacement)
	wantCode(t, err, "pairing_consumed")
	run := f.create("alice", creds, "create-one", "")
	same := f.create("alice", creds, "create-one", "")
	if same.ID != run.ID {
		t.Fatal("create retry was not idempotent")
	}
	c = droppingClient(t, f, creds, "/device/poll")
	if _, err = c.Poll(context.Background(), PollRequest{ID: testPoll("claim-one")}); err == nil {
		t.Fatal("expected lost claim response")
	}
	claimed := mustPoll(t, c, "claim-one")
	if claimed.ID != run.ID {
		t.Fatal("claim changed")
	}
	c = droppingClient(t, f, creds, "/device/runs/"+run.ID+"/events")
	batch := EventBatch{AttemptID: run.AttemptID, Fence: run.Fence, Events: []Event{{Seq: 1, Type: EventStarted, SessionID: "native-thread-7"}, {Seq: 2, Type: EventText, Text: "Exact original output"}}}
	if _, err = c.AppendEvents(context.Background(), run.ID, batch); err == nil {
		t.Fatal("expected lost event response")
	}
	receipt, err := c.AppendEvents(context.Background(), run.ID, batch)
	if err != nil || receipt.AckSeq != 2 {
		t.Fatalf("ack: %+v %v", receipt, err)
	}
	f.server.Close()
	f.open()
	c, err = NewClient(f.server.URL+"/api/agents/v1", creds, ClientOptions{HTTPClient: f.server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	resumed := mustPoll(t, c, "claim-after-restart")
	if resumed.ID != run.ID || resumed.LastSeq != 2 || resumed.NativeSessionID != "native-thread-7" {
		t.Fatalf("lost durable state: %+v", resumed)
	}
	got, err := f.service.Events(context.Background(), Principal{"alice"}, run.ID, 0)
	if err != nil || len(got.Events) != 2 || got.Events[1].Text != "Exact original output" {
		t.Fatalf("events: %+v %v", got, err)
	}
	receipt = appendBatch(t, c, run, Event{Seq: 3, Type: EventCompleted, Text: "done"})
	if receipt.State != StateSucceeded {
		t.Fatalf("not terminal: %+v", receipt)
	}
	follow := f.create("alice", creds, "create-two", run.SessionID)
	if follow.NativeSessionID != "native-thread-7" {
		t.Fatalf("native session lost: %+v", follow)
	}
	old := mustPoll(t, c, "claim-one")
	if old.ID != run.ID || old.State != StateSucceeded {
		t.Fatalf("old claim replay reassigned: %+v", old)
	}
	next := mustPoll(t, c, "claim-two")
	if next.ID != follow.ID {
		t.Fatal("follow-up not claimed")
	}
}

func TestIsolationRevocationAndHostAccountValidation(t *testing.T) {
	f := newFixture(t)
	a, ac, _ := f.enroll("alice")
	b, bc, _ := f.enroll("bob")
	run := f.create("alice", ac, "alice-run", "")
	_ = mustPoll(t, a, "alice-claim")
	status, raw := f.request("GET", "/runs/"+run.ID, "bob", nil)
	if status != 404 {
		t.Fatalf("cross-account run exposed: %d %s", status, raw)
	}
	_, err := b.Heartbeat(context.Background(), run.ID, HeartbeatRequest{AttemptID: run.AttemptID, Fence: run.Fence})
	wantCode(t, err, "not_found")
	status, raw = f.request("GET", "/devices", "alice", nil)
	if status != 200 || bytes.Contains(raw, []byte(bc.DeviceID)) {
		t.Fatalf("device isolation failed: %s", raw)
	}
	_, err = f.service.CreateRun(context.Background(), Principal{"bob"}, CreateRunRequest{IdempotencyKey: testKey("bad-target"), DeviceID: ac.DeviceID, Agent: "codex", Workspace: "repo", Prompt: "bad"})
	wantCode(t, err, "device_unavailable")
	f.disabled.Store(true)
	_, err = a.Poll(context.Background(), PollRequest{ID: testPoll("disabled-poll")})
	wantCode(t, err, "principal_unavailable")
	_, err = a.AppendEvents(context.Background(), run.ID, EventBatch{AttemptID: run.AttemptID, Fence: run.Fence, Events: []Event{{Seq: 1, Type: EventStarted}}})
	wantCode(t, err, "principal_unavailable")
	f.disabled.Store(false)
	if err = f.service.RevokeDevice(context.Background(), Principal{"alice"}, ac.DeviceID); err != nil {
		t.Fatal(err)
	}
	_, err = a.Poll(context.Background(), PollRequest{ID: testPoll("revoked-poll")})
	wantCode(t, err, "unauthorized")
	revoked, err := f.service.Run(context.Background(), Principal{"alice"}, run.ID)
	if err != nil || revoked.State != StateUnknown || !revoked.CancelRequested {
		t.Fatalf("revocation falsely stopped process: %+v %v", revoked, err)
	}
}

func TestImmutableSequenceApprovalAndCancellation(t *testing.T) {
	f := newFixture(t)
	c, creds, _ := f.enroll("alice")
	run := f.create("alice", creds, "events", "")
	_ = mustPoll(t, c, "claim")
	first := []Event{{Seq: 1, Type: EventStarted}, {Seq: 2, Type: EventApproval, RequestID: "request-9", Tool: "shell", Input: json.RawMessage(`{"command":"go test ./..."}`)}}
	appendBatch(t, c, run, first...)
	ctl, err := c.Heartbeat(context.Background(), run.ID, HeartbeatRequest{AttemptID: run.AttemptID, Fence: run.Fence})
	if err != nil || len(ctl.Approvals) != 1 {
		t.Fatalf("approval absent: %+v %v", ctl, err)
	}
	approval := ctl.Approvals[0]
	_, err = f.service.Answer(context.Background(), Principal{"alice"}, run.ID, approval.RequestID, AnswerRequest{AttemptID: run.AttemptID, Digest: strings.Repeat("0", 64), Decision: "allow"})
	wantCode(t, err, "stale_request")
	answer := AnswerRequest{AttemptID: run.AttemptID, Digest: approval.Digest, Decision: "allow"}
	if _, err = f.service.Answer(context.Background(), Principal{"alice"}, run.ID, approval.RequestID, answer); err != nil {
		t.Fatal(err)
	}
	if _, err = f.service.Answer(context.Background(), Principal{"alice"}, run.ID, approval.RequestID, answer); err != nil {
		t.Fatal(err)
	}
	answer.Decision = "deny"
	_, err = f.service.Answer(context.Background(), Principal{"alice"}, run.ID, approval.RequestID, answer)
	wantCode(t, err, "answer_conflict")
	changed := append([]Event(nil), first...)
	changed[1].Tool = "different"
	_, err = c.AppendEvents(context.Background(), run.ID, EventBatch{AttemptID: run.AttemptID, Fence: run.Fence, Events: changed})
	wantCode(t, err, "event_conflict")
	_, err = c.AppendEvents(context.Background(), run.ID, EventBatch{AttemptID: run.AttemptID, Fence: run.Fence, Events: []Event{{Seq: 4, Type: EventText, Text: "gap"}}})
	wantCode(t, err, "sequence_gap")
	cancelled, err := f.service.CancelRun(context.Background(), Principal{"alice"}, run.ID)
	if err != nil || cancelled.State != StateRunning || !cancelled.CancelRequested {
		t.Fatalf("request mistaken for stop: %+v %v", cancelled, err)
	}
	receipt := appendBatch(t, c, run, Event{Seq: 3, Type: EventCancelled, Text: "process reaped"})
	if receipt.State != StateCancelled {
		t.Fatal("stop confirmation missing")
	}
	_, err = c.AppendEvents(context.Background(), run.ID, EventBatch{AttemptID: run.AttemptID, Fence: run.Fence, Events: []Event{{Seq: 4, Type: EventCompleted}}})
	wantCode(t, err, "run_terminal")
	// Repeating originals after completion acknowledges their persisted prefix.
	if r := appendBatch(t, c, run, first...); r.AckSeq != 3 {
		t.Fatal("terminal replay lost ACK")
	}
}

func TestUnknownNeverReassignedAndLateOutboxReconciles(t *testing.T) {
	f := newFixture(t)
	c, creds, _ := f.enroll("alice")
	run := f.create("alice", creds, "unknown", "")
	_ = mustPoll(t, c, "claim")
	f.clock.add(11 * time.Second)
	got := mustPoll(t, c, "different-claim")
	if got.ID != run.ID || got.State != StateUnknown || !got.CancelRequested {
		t.Fatalf("expired claim reassigned: %+v", got)
	}
	ctl, err := c.Heartbeat(context.Background(), run.ID, HeartbeatRequest{AttemptID: run.AttemptID, Fence: run.Fence})
	if err != nil || ctl.Run.State != StateUnknown {
		t.Fatalf("heartbeat revived unknown: %+v %v", ctl, err)
	}
	_, err = f.service.CreateRun(context.Background(), Principal{"alice"}, CreateRunRequest{IdempotencyKey: testKey("unsafe-retry"), DeviceID: creds.DeviceID, Agent: "codex", Workspace: "repo", Prompt: "rerun"})
	wantCode(t, err, "device_requires_reconciliation")
	// These are previously persisted local events, not permission to start again.
	receipt := appendBatch(t, c, run, Event{Seq: 1, Type: EventStarted}, Event{Seq: 2, Type: EventText, Text: "output before disconnect"})
	if receipt.State != StateUnknown {
		t.Fatal("historical events revived unknown")
	}
	receipt = appendBatch(t, c, run, Event{Seq: 3, Type: EventFailed, Text: "original process confirmed stopped"})
	if receipt.State != StateFailed {
		t.Fatal("known stopped outcome not reconciled")
	}
	next := f.create("alice", creds, "safe-new", run.SessionID)
	if next.ID == run.ID {
		t.Fatal("new intent reused old run")
	}
}

func TestConcurrentFileStoreServiceInstances(t *testing.T) {
	f := newFixture(t)
	_, creds, _ := f.enroll("alice")
	otherStore, err := NewFileStore(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewService(otherStore, Options{Now: f.clock.now, LeaseDuration: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	req := CreateRunRequest{IdempotencyKey: testKey("concurrent-idempotency"), DeviceID: creds.DeviceID, Agent: "codex", Workspace: "repo", Prompt: "one intent"}
	var wg sync.WaitGroup
	ids := make(chan string, 24)
	failures := make(chan error, 24)
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := f.service
			if i%2 != 0 {
				s = other
			}
			r, e := s.CreateRun(ctx, Principal{"alice"}, req)
			if e != nil {
				failures <- e
			} else {
				ids <- r.ID
			}
		}(i)
	}
	wg.Wait()
	close(ids)
	close(failures)
	for e := range failures {
		t.Fatal(e)
	}
	id := ""
	for got := range ids {
		if id == "" {
			id = got
		}
		if got != id {
			t.Fatal("concurrent idempotency created multiple runs")
		}
	}
	var newClaims atomic.Int32
	failures = make(chan error, 24)
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := f.service
			if i%2 != 0 {
				s = other
			}
			p, e := s.Poll(ctx, creds.Token, PollRequest{ID: testPoll("poll-" + string(rune('a'+i)))})
			if e != nil {
				failures <- e
				return
			}
			if p.Assignment == nil || p.Assignment.Run.ID != id {
				failures <- errors.New("assignment mismatch")
				return
			}
			if p.Assignment.New {
				newClaims.Add(1)
			}
		}(i)
	}
	wg.Wait()
	close(failures)
	for e := range failures {
		t.Fatal(e)
	}
	if newClaims.Load() != 1 {
		t.Fatalf("got %d first claims", newClaims.Load())
	}
	changed := req
	changed.Prompt = "another action"
	_, err = other.CreateRun(ctx, Principal{"alice"}, changed)
	wantCode(t, err, "idempotency_conflict")
}

func TestCapacityReservesKnownStoppedTerminal(t *testing.T) {
	f := newFixture(t)
	c, creds, _ := f.enroll("alice")
	f.service.options.Limits.MaxEventsPerRun = 2
	f.service.options.Limits.MaxRunEventBytes = 1024
	run := f.create("alice", creds, "capacity", "")
	_ = mustPoll(t, c, "claim")
	appendBatch(t, c, run, Event{Seq: 1, Type: EventStarted}, Event{Seq: 2, Type: EventText, Text: "bounded output"})
	_, err := c.AppendEvents(context.Background(), run.ID, EventBatch{AttemptID: run.AttemptID, Fence: run.Fence, Events: []Event{{Seq: 3, Type: EventText, Text: "over capacity"}}})
	wantCode(t, err, "event_capacity")
	receipt := appendBatch(t, c, run, Event{Seq: 3, Type: EventFailed, Text: "process stopped after explicit capacity rejection"})
	if receipt.State != StateFailed || receipt.AckSeq != 3 {
		t.Fatalf("terminal reserve unusable: %+v", receipt)
	}
}
