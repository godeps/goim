package remote

import (
	"context"
	"strconv"
	"strings"
	"time"
)

// NewRunKey creates a timestamped idempotency key. Save the key with the original
// request before sending it. A key beyond the retention window cannot create a
// new execution even after the corresponding terminal history was removed.
func NewRunKey() (string, error) { return newTimedID("rk1", time.Now()) }

// NewPollID creates a timestamped durable claim ID. An assigned claim is retained
// with its run; an expired ID never silently becomes a fresh claim after cleanup.
func NewPollID() (string, error) { return newTimedID("pc1", time.Now()) }
func newTimedID(prefix string, now time.Time) (string, error) {
	nonce, err := randomID()
	if err != nil {
		return "", err
	}
	return prefix + "." + strconv.FormatInt(now.Unix(), 10) + "." + nonce, nil
}
func timedID(value, prefix string) (time.Time, error) {
	p := strings.Split(value, ".")
	if len(p) != 3 || p[0] != prefix || !opaqueID.MatchString(p[2]) {
		return time.Time{}, problem("invalid_request_key", 400)
	}
	seconds, err := strconv.ParseInt(p[1], 10, 64)
	if err != nil || seconds < 0 || strconv.FormatInt(seconds, 10) != p[1] {
		return time.Time{}, problem("invalid_request_key", 400)
	}
	// Restrict to ordinary RFC3339-compatible time values before any arithmetic.
	if seconds > 253402300799 {
		return time.Time{}, problem("invalid_request_key", 400)
	}
	return time.Unix(seconds, 0).UTC(), nil
}
func (s *Service) checkKeyTime(issued, now time.Time, expired string) error {
	if issued.After(now.Add(5 * time.Minute)) {
		return problem("request_from_future", 400)
	}
	if !now.Before(issued.Add(s.options.RetentionDuration)) {
		return problem(expired, 410)
	}
	return nil
}
func (s *Service) prune(st *state, now time.Time) {
	for id, r := range st.Runs {
		if r.Run.Terminal() && !now.Before(r.Run.UpdatedAt.Add(s.options.RetentionDuration)) && !now.Before(r.KeyIssuedAt.Add(s.options.RetentionDuration)) {
			delete(st.Runs, id)
			if st.Idempotency[r.Key] == id {
				delete(st.Idempotency, r.Key)
			}
		}
	}
	for id, session := range st.Sessions {
		if session.ActiveRun == "" && !now.Before(session.LastUsed.Add(s.options.SessionRetention)) {
			delete(st.Sessions, id)
		}
	}
	for id, pair := range st.Pairings {
		// A consumed enrollment receipt lasts as long as its device identity. This
		// permits exact enrollment recovery after a lost response without new secrets.
		if pair.DeviceID == "" && !now.Before(pair.ExpiresAt) {
			delete(st.Pairings, id)
		}
	}
}

// Policy describes the host-configured retention and remaining indexed state.
// Known terminal history is retained at least RetentionSeconds after completion;
// active or unknown execution is never expired by this policy.
type Policy struct {
	Protocol                string `json:"protocol"`
	RetentionSeconds        int64  `json:"retention_seconds"`
	SessionRetentionSeconds int64  `json:"session_retention_seconds"`
	Limits                  Limits `json:"limits"`
	Runs                    int    `json:"runs"`
	Sessions                int    `json:"sessions"`
}

// Policy returns retention/limits after safe maintenance for this principal.
func (s *Service) Policy(ctx context.Context, p Principal) (Policy, error) {
	var out Policy
	if err := validatePrincipal(p); err != nil {
		return out, err
	}
	err := s.mutate(ctx, hash(p.ID), p.ID, func(st *state, _ time.Time) error {
		out = Policy{Protocol: Protocol, RetentionSeconds: int64(s.options.RetentionDuration / time.Second), SessionRetentionSeconds: int64(s.options.SessionRetention / time.Second), Limits: s.options.Limits, Runs: len(st.Runs), Sessions: len(st.Sessions)}
		return nil
	})
	return out, err
}
