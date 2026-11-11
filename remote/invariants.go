package remote

import "errors"

// Stored state is a closed schema. A malformed durable snapshot must fail
// closed, never panic into partial protocol handling or reset execution intent.
func validateStoredState(st *state) error {
	invalid := errors.New("corrupt remote protocol state")
	for id, d := range st.Devices {
		if d == nil || id != d.Device.ID || !opaqueID.MatchString(id) || d.Device.PrincipalID != st.Principal || !ownerHash.MatchString(d.CredentialHash) {
			return invalid
		}
		if d.ActiveRun != "" {
			r := st.Runs[d.ActiveRun]
			if r == nil || r.Run.DeviceID != id || r.Run.Terminal() || r.Run.State == StateQueued {
				return invalid
			}
		}
	}
	for id, p := range st.Pairings {
		if p == nil || !opaqueID.MatchString(id) || !ownerHash.MatchString(p.Hash) {
			return invalid
		}
		if p.DeviceID != "" && st.Devices[p.DeviceID] == nil {
			return invalid
		}
	}
	for id, r := range st.Runs {
		if r == nil || id != r.Run.ID || !opaqueID.MatchString(id) || !opaqueID.MatchString(r.Run.AttemptID) || r.Run.Fence == 0 || st.Devices[r.Run.DeviceID] == nil || st.Sessions[r.Run.SessionID] == nil || r.Approvals == nil {
			return invalid
		}
		if r.Run.LastSeq != uint64(len(r.Events)) || len(r.Events) != len(r.EventDigests) || r.FinalCount < 0 || r.FinalCount > 2 || r.FinalBytes < 0 || r.EventBytes < r.FinalBytes {
			return invalid
		}
		if r.Run.Limits.MaxEventsPerRun < 1 || r.Run.Limits.MaxRunEventBytes < 1024 || r.Run.Limits.MaxTerminalBytes != 2048 || r.Run.Limits.MaxFinalEvents != 2 {
			return invalid
		}
		switch r.Run.State {
		case StateQueued, StateClaimed, StateRunning, StateSucceeded, StateFailed, StateCancelled, StateUnknown:
		default:
			return invalid
		}
		for i, e := range r.Events {
			if e.Seq != uint64(i+1) || !ownerHash.MatchString(r.EventDigests[i]) {
				return invalid
			}
		}
		for id, a := range r.Approvals {
			if a == nil || a.RequestID != id || a.AttemptID != r.Run.AttemptID || !ownerHash.MatchString(a.Digest) {
				return invalid
			}
		}
		if st.Idempotency[r.Key] != id {
			return invalid
		}
	}
	for key, id := range st.Idempotency {
		r := st.Runs[id]
		if r == nil || r.Key != key {
			return invalid
		}
	}
	for id, s := range st.Sessions {
		if s == nil || id != s.ID || st.Devices[s.DeviceID] == nil {
			return invalid
		}
		if s.ActiveRun != "" {
			r := st.Runs[s.ActiveRun]
			if r == nil || r.Run.SessionID != id || r.Run.Terminal() {
				return invalid
			}
		}
	}
	return nil
}
