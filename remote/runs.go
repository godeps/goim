package remote

import (
	"context"
	"encoding/json"
	"sort"
	"time"
)

func releaseRun(st *state, r *runRecord) {
	if d := st.Devices[r.Run.DeviceID]; d != nil && d.ActiveRun == r.Run.ID {
		d.ActiveRun = ""
	}
	if session := st.Sessions[r.Run.SessionID]; session != nil && session.ActiveRun == r.Run.ID {
		session.ActiveRun = ""
		session.LastUsed = r.Run.UpdatedAt
	}
}
func cloneEvent(e Event) Event          { e.Input = append(json.RawMessage(nil), e.Input...); return e }
func cloneApproval(a Approval) Approval { a.Input = append(json.RawMessage(nil), a.Input...); return a }
func approvals(r *runRecord) []Approval {
	out := make([]Approval, 0, len(r.Approvals))
	for _, a := range r.Approvals {
		out = append(out, cloneApproval(*a))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RequestID < out[j].RequestID })
	return out
}
func ownedRun(st *state, id string) (*runRecord, error) {
	r := st.Runs[id]
	if r == nil {
		return nil, problem("not_found", 404)
	}
	return r, nil
}
func assignedRun(st *state, deviceID, runID, attemptID string, fence uint64) (*runRecord, error) {
	r, err := ownedRun(st, runID)
	if err != nil || r.Run.DeviceID != deviceID {
		return nil, problem("not_found", 404)
	}
	if r.Run.AttemptID != attemptID || r.Run.Fence != fence {
		return nil, problem("stale_attempt", 409)
	}
	if r.Run.State == StateQueued {
		return nil, problem("not_claimed", 409)
	}
	return r, nil
}

// CreateRun atomically creates an idempotent queued prompt. A logical session
// permits only one outstanding run, and its device/agent/workspace never change.
// Random IDs are generated before the CAS loop and have no retry side effects.
func (s *Service) CreateRun(ctx context.Context, p Principal, req CreateRunRequest) (Run, error) {
	var out Run
	if err := validatePrincipal(p); err != nil {
		return out, err
	}
	if !bounded(req.IdempotencyKey, 128) || !opaqueID.MatchString(req.DeviceID) || !alias.MatchString(req.Agent) || !alias.MatchString(req.Workspace) || !bounded(req.Prompt, s.options.Limits.MaxPromptBytes) || (req.SessionID != "" && !alias.MatchString(req.SessionID)) {
		return out, problem("invalid_run", 400)
	}
	keyTime, err := timedID(req.IdempotencyKey, "rk1")
	if err != nil {
		return out, err
	}
	digest, err := digestValue(req)
	if err != nil {
		return out, err
	}
	runID, err := randomID()
	if err != nil {
		return out, err
	}
	attemptID, err := randomID()
	if err != nil {
		return out, err
	}
	sessionID := req.SessionID
	if sessionID == "" {
		sessionID, err = randomID()
		if err != nil {
			return out, err
		}
	}
	err = s.mutate(ctx, hash(p.ID), p.ID, func(st *state, now time.Time) error {
		if s.options.AuthorizeRun != nil {
			if err := s.options.AuthorizeRun(ctx, p, req); err != nil {
				return problem("run_not_authorized", 403)
			}
		}
		if id := st.Idempotency[req.IdempotencyKey]; id != "" {
			prior := st.Runs[id]
			if prior == nil {
				return problem("state_corrupt", 500)
			}
			if !equal(prior.RequestDigest, digest) {
				return problem("idempotency_conflict", 409)
			}
			out = prior.Run
			return nil
		}
		if err := s.checkKeyTime(keyTime, now, "request_expired"); err != nil {
			return err
		}
		d := st.Devices[req.DeviceID]
		if d == nil || d.Device.Revoked {
			return problem("device_unavailable", 409)
		}
		if !supported(d.Device, req.Agent, req.Workspace) {
			return problem("unsupported_target", 400)
		}
		if len(st.Runs) >= s.options.Limits.MaxRuns {
			return problem("run_capacity", 409)
		}
		session := st.Sessions[sessionID]
		if session == nil {
			if req.SessionID != "" {
				return problem("session_expired", 410)
			}
			if len(st.Sessions) >= s.options.Limits.MaxSessions {
				return problem("session_capacity", 409)
			}
			session = &sessionRecord{ID: sessionID, DeviceID: req.DeviceID, Agent: req.Agent, Workspace: req.Workspace, LastUsed: now}
			st.Sessions[sessionID] = session
		} else if session.DeviceID != req.DeviceID || session.Agent != req.Agent || session.Workspace != req.Workspace {
			return problem("session_scope_conflict", 409)
		}
		if session.ActiveRun != "" {
			return problem("session_busy", 409)
		}
		// A prior uncertain process blocks all work on its device until the original
		// attempt reports a known stopped outcome. Another prompt cannot bypass it.
		if d.ActiveRun != "" {
			if r := st.Runs[d.ActiveRun]; r != nil && r.Run.State == StateUnknown {
				return problem("device_requires_reconciliation", 409)
			}
		}
		out = Run{Limits: s.eventLimits(), ID: runID, DeviceID: req.DeviceID, Agent: req.Agent, Workspace: req.Workspace, SessionID: sessionID, NativeSessionID: session.NativeID, Prompt: req.Prompt, State: StateQueued, AttemptID: attemptID, Fence: 1, CreatedAt: now, UpdatedAt: now}
		st.Runs[runID] = &runRecord{Run: out, RequestDigest: digest, Key: req.IdempotencyKey, KeyIssuedAt: keyTime, Events: []Event{}, EventDigests: []string{}, Approvals: map[string]*Approval{}}
		st.Idempotency[req.IdempotencyKey] = runID
		session.ActiveRun = runID
		session.LastUsed = now
		return nil
	})
	return out, err
}

// Runs returns only runs owned by this principal, oldest first.
func (s *Service) Runs(ctx context.Context, p Principal) ([]Run, error) {
	out := []Run{}
	if err := validatePrincipal(p); err != nil {
		return nil, err
	}
	err := s.mutate(ctx, hash(p.ID), p.ID, func(st *state, _ time.Time) error {
		out = out[:0]
		for _, r := range st.Runs {
			out = append(out, r.Run)
		}
		sort.Slice(out, func(i, j int) bool {
			if out[i].CreatedAt.Equal(out[j].CreatedAt) {
				return out[i].ID < out[j].ID
			}
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		})
		return nil
	})
	return out, err
}

// Run returns an owned run and reconciles any elapsed lease to unknown.
func (s *Service) Run(ctx context.Context, p Principal, id string) (Run, error) {
	var out Run
	if err := validatePrincipal(p); err != nil {
		return out, err
	}
	err := s.mutate(ctx, hash(p.ID), p.ID, func(st *state, _ time.Time) error {
		r, e := ownedRun(st, id)
		if e != nil {
			return e
		}
		out = r.Run
		return nil
	})
	return out, err
}

// Events returns at most 256 original persisted events, strictly after the
// supplied sequence. Next is the last returned sequence, not the current head.
func (s *Service) Events(ctx context.Context, p Principal, id string, after uint64) (EventsResponse, error) {
	var out EventsResponse
	if err := validatePrincipal(p); err != nil {
		return out, err
	}
	err := s.mutate(ctx, hash(p.ID), p.ID, func(st *state, _ time.Time) error {
		r, e := ownedRun(st, id)
		if e != nil {
			return e
		}
		if after > r.Run.LastSeq {
			return problem("invalid_cursor", 400)
		}
		out = EventsResponse{Run: r.Run, Events: []Event{}, Next: after, Approvals: approvals(r)}
		for i := after; i < uint64(len(r.Events)) && len(out.Events) < 256; i++ {
			out.Events = append(out.Events, cloneEvent(r.Events[i]))
			out.Next = r.Events[i].Seq
		}
		return nil
	})
	return out, err
}

// CancelRun records a cancellation request. Only an unclaimed run is immediately
// cancelled; a claimed process must acknowledge that it has actually stopped.
func (s *Service) CancelRun(ctx context.Context, p Principal, id string) (Run, error) {
	var out Run
	if err := validatePrincipal(p); err != nil {
		return out, err
	}
	err := s.mutate(ctx, hash(p.ID), p.ID, func(st *state, now time.Time) error {
		r, e := ownedRun(st, id)
		if e != nil {
			return e
		}
		if !r.Run.Terminal() {
			r.Run.CancelRequested = true
			r.Run.UpdatedAt = now
			if r.Run.State == StateQueued {
				r.Run.State = StateCancelled
				releaseRun(st, r)
			}
		}
		out = r.Run
		return nil
	})
	return out, err
}

// Answer binds the decision to the exact immutable tool request and attempt.
// Cancellation, lease loss or completion never creates an implicit approval.
func (s *Service) Answer(ctx context.Context, p Principal, runID, requestID string, req AnswerRequest) (Approval, error) {
	var out Approval
	if err := validatePrincipal(p); err != nil {
		return out, err
	}
	if req.Decision != "allow" && req.Decision != "deny" {
		return out, problem("invalid_decision", 400)
	}
	err := s.mutate(ctx, hash(p.ID), p.ID, func(st *state, now time.Time) error {
		r, e := ownedRun(st, runID)
		if e != nil {
			return e
		}
		a := r.Approvals[requestID]
		if a == nil {
			return problem("not_found", 404)
		}
		if a.AttemptID != req.AttemptID || !equal(a.Digest, req.Digest) {
			return problem("stale_request", 409)
		}
		if a.Decision != "" {
			if a.Decision != req.Decision {
				return problem("answer_conflict", 409)
			}
			out = cloneApproval(*a)
			return nil
		}
		if r.Run.Terminal() || r.Run.State == StateUnknown || r.Run.CancelRequested {
			return problem("request_inactive", 409)
		}
		a.Decision = req.Decision
		a.AnsweredAt = now
		out = cloneApproval(*a)
		return nil
	})
	return out, err
}

// Poll claims one queued run, or returns the device's existing assignment.
// A claim ID is retained with the run: retrying a lost response cannot claim a
// different run even after completion or subsequent polls. Empty polls are not
// claims and can be retried while waiting for work.
func (s *Service) Poll(ctx context.Context, token string, req PollRequest) (PollResponse, error) {
	out := PollResponse{RetryAfterSeconds: 2}
	issued, err := timedID(req.ID, "pc1")
	if err != nil {
		return out, problem("invalid_poll_id", 400)
	}
	h, id, err := tokenParts(token, "gd1")
	if err != nil {
		return out, err
	}
	err = s.mutate(ctx, h, "", func(st *state, now time.Time) error {
		d, e := checkDevice(st, id, token)
		if e != nil {
			return e
		}
		out = PollResponse{RetryAfterSeconds: 2}
		d.Device.LastSeen = now
		for _, r := range st.Runs {
			if r.Run.DeviceID == id && r.ClaimID == req.ID {
				out.Assignment = &Assignment{Run: r.Run, New: true}
				return nil
			}
		}
		if err := s.checkKeyTime(issued, now, "claim_expired"); err != nil {
			return err
		}
		if d.ActiveRun != "" {
			r := st.Runs[d.ActiveRun]
			if r == nil {
				return problem("state_corrupt", 500)
			}
			out.Assignment = &Assignment{Run: r.Run}
			return nil
		}
		var chosen *runRecord
		for _, r := range st.Runs {
			if r.Run.DeviceID == id && r.Run.State == StateQueued && (chosen == nil || r.Run.CreatedAt.Before(chosen.Run.CreatedAt) || (r.Run.CreatedAt.Equal(chosen.Run.CreatedAt) && r.Run.ID < chosen.Run.ID)) {
				chosen = r
			}
		}
		if chosen == nil {
			return nil
		}
		chosen.ClaimID = req.ID
		chosen.Run.State = StateClaimed
		chosen.Run.LeaseUntil = now.Add(s.options.LeaseDuration)
		chosen.Run.UpdatedAt = now
		d.ActiveRun = chosen.Run.ID
		out.Assignment = &Assignment{Run: chosen.Run, New: true}
		return nil
	})
	return out, err
}

// Heartbeat renews a live execution lease and returns decisions/cancellation.
// An unknown execution is never revived by heartbeat; it must be reconciled.
func (s *Service) Heartbeat(ctx context.Context, token, runID string, req HeartbeatRequest) (Control, error) {
	var out Control
	h, id, err := tokenParts(token, "gd1")
	if err != nil {
		return out, err
	}
	err = s.mutate(ctx, h, "", func(st *state, now time.Time) error {
		d, e := checkDevice(st, id, token)
		if e != nil {
			return e
		}
		r, e := assignedRun(st, id, runID, req.AttemptID, req.Fence)
		if e != nil {
			return e
		}
		d.Device.LastSeen = now
		if r.Run.State == StateClaimed || r.Run.State == StateRunning {
			r.Run.LeaseUntil = now.Add(s.options.LeaseDuration)
			r.Run.UpdatedAt = now
		}
		out = Control{Run: r.Run, CancelRequested: r.Run.CancelRequested, Approvals: approvals(r)}
		return nil
	})
	return out, err
}

func terminalEvent(kind string) bool {
	return kind == EventCompleted || kind == EventFailed || kind == EventCancelled
}
func outcomeEvent(kind string) bool { return terminalEvent(kind) || kind == EventUnknown }
func (s *Service) eventLimits() EventLimits {
	return EventLimits{MaxEventsPerRun: s.options.Limits.MaxEventsPerRun, MaxRunEventBytes: s.options.Limits.MaxRunEventBytes, MaxEventTextBytes: 16 << 10, MaxEventInputBytes: 16 << 10, MaxEventBytes: 24 << 10, MaxTerminalBytes: 2048, MaxFinalEvents: 2}
}
func validateEvent(e Event) ([]byte, error) {
	if e.Seq == 0 {
		return nil, problem("invalid_event", 400)
	}
	if e.Text != "" && !bounded(e.Text, 16<<10) {
		return nil, problem("invalid_event", 400)
	}
	if e.SessionID != "" && !bounded(e.SessionID, 256) {
		return nil, problem("invalid_event", 400)
	}
	if len(e.Input) > 16<<10 || (len(e.Input) != 0 && !json.Valid(e.Input)) {
		return nil, problem("invalid_event", 400)
	}
	switch e.Type {
	case EventStarted, EventText, EventTool, EventToolResult, EventApproval, EventCompleted, EventFailed, EventCancelled, EventUnknown:
	default:
		return nil, problem("invalid_event_type", 400)
	}
	if e.Type == EventApproval {
		if !alias.MatchString(e.RequestID) || !bounded(e.Tool, 128) || len(e.Input) == 0 {
			return nil, problem("invalid_request", 400)
		}
	} else if e.RequestID != "" && !alias.MatchString(e.RequestID) {
		return nil, problem("invalid_request", 400)
	}
	if e.Tool != "" && !bounded(e.Tool, 128) {
		return nil, problem("invalid_event", 400)
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return nil, problem("invalid_event", 400)
	}
	if outcomeEvent(e.Type) && len(raw) > 2048 {
		return nil, problem("terminal_event_too_large", 400)
	}
	if len(raw) > 24<<10 {
		return nil, problem("event_too_large", 400)
	}
	return raw, nil
}

// AppendEvents atomically commits a contiguous original outbox prefix. Identical
// retries are acknowledged; changed, skipped, or post-terminal events conflict.
// Nonterminal capacity has a reserved final event slot so a full stream can
// still report a known stopped failure without silently dropping prior events.
func (s *Service) AppendEvents(ctx context.Context, token, runID string, batch EventBatch) (Receipt, error) {
	var out Receipt
	if len(batch.Events) < 1 || len(batch.Events) > 64 {
		return out, problem("invalid_batch", 400)
	}
	h, id, err := tokenParts(token, "gd1")
	if err != nil {
		return out, err
	}
	sizes := make([]int, len(batch.Events))
	digests := make([]string, len(batch.Events))
	for i, e := range batch.Events {
		raw, err := validateEvent(e)
		if err != nil {
			return out, err
		}
		if i > 0 && e.Seq != batch.Events[i-1].Seq+1 {
			return out, problem("sequence_gap", 409)
		}
		sizes[i] = len(raw)
		digests[i] = hash(string(raw))
	}
	err = s.mutate(ctx, h, "", func(st *state, now time.Time) error {
		d, e := checkDevice(st, id, token)
		if e != nil {
			return e
		}
		r, e := assignedRun(st, id, runID, batch.AttemptID, batch.Fence)
		if e != nil {
			return e
		}
		for i, event := range batch.Events {
			if event.Seq <= r.Run.LastSeq {
				if event.Seq > uint64(len(r.EventDigests)) || !equal(r.EventDigests[event.Seq-1], digests[i]) {
					return problem("event_conflict", 409)
				}
				continue
			}
			if event.Seq != r.Run.LastSeq+1 {
				return problem("sequence_gap", 409)
			}
			if r.Run.Terminal() {
				return problem("run_terminal", 409)
			}
			final := outcomeEvent(event.Type)
			limits := r.Run.Limits
			if limits.MaxEventsPerRun < 1 || limits.MaxRunEventBytes < 1024 || limits.MaxFinalEvents != 2 {
				return problem("state_corrupt", 500)
			}
			if !final && (len(r.Events)-r.FinalCount >= limits.MaxEventsPerRun || r.EventBytes-r.FinalBytes+sizes[i] > limits.MaxRunEventBytes) {
				return problem("event_capacity", 409)
			}
			if final && (r.FinalCount >= limits.MaxFinalEvents || sizes[i] > limits.MaxTerminalBytes) {
				return problem("event_capacity", 409)
			}
			if event.Type == EventUnknown && r.UnknownReported {
				return problem("outcome_reported", 409)
			}
			if event.Type == EventStarted {
				if r.Started || (r.Run.State != StateClaimed && r.Run.State != StateUnknown) {
					return problem("start_not_allowed", 409)
				}
				r.Started = true
				if r.Run.State != StateUnknown {
					r.Run.State = StateRunning
				}
			} else if !final && event.Type != EventUnknown && r.Run.State != StateRunning && r.Run.State != StateUnknown {
				return problem("not_started", 409)
			}
			if event.Type == EventApproval {
				if r.Run.State != StateRunning && r.Run.State != StateUnknown {
					return problem("request_inactive", 409)
				}
				if r.Approvals[event.RequestID] != nil {
					return problem("request_conflict", 409)
				}
				digest, _ := digestValue(struct {
					RunID, AttemptID string
					Fence            uint64
					Event            Event
				}{r.Run.ID, r.Run.AttemptID, r.Run.Fence, event})
				r.Approvals[event.RequestID] = &Approval{RequestID: event.RequestID, AttemptID: r.Run.AttemptID, Digest: digest, Tool: event.Tool, Input: append(json.RawMessage(nil), event.Input...), CreatedAt: now}
			}
			if event.SessionID != "" {
				r.Run.NativeSessionID = event.SessionID
				if session := st.Sessions[r.Run.SessionID]; session != nil {
					session.NativeID = event.SessionID
					session.LastUsed = now
				}
			}
			switch event.Type {
			case EventCompleted:
				r.Run.State = StateSucceeded
			case EventFailed:
				r.Run.State = StateFailed
			case EventCancelled:
				r.Run.State = StateCancelled
			case EventUnknown:
				r.Run.State = StateUnknown
				r.Run.CancelRequested = true
			}
			if final {
				r.FinalCount++
				r.FinalBytes += sizes[i]
			}
			if event.Type == EventUnknown {
				r.UnknownReported = true
			}
			r.Events = append(r.Events, cloneEvent(event))
			r.EventDigests = append(r.EventDigests, digests[i])
			r.EventBytes += sizes[i]
			r.Run.LastSeq = event.Seq
			r.Run.UpdatedAt = now
			if r.Run.Terminal() {
				releaseRun(st, r)
			}
		}
		d.Device.LastSeen = now
		out = Receipt{RunID: r.Run.ID, AttemptID: r.Run.AttemptID, AckSeq: r.Run.LastSeq, State: r.Run.State}
		return nil
	})
	return out, err
}
