package remote

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

// Service owns protocol state, while the host owns authentication and policy.
// State is CAS-serialized per principal, including device, claim and event facts.
type Service struct {
	store   Store
	options Options
}
type state struct {
	Protocol    string                    `json:"protocol"`
	Principal   string                    `json:"principal"`
	Devices     map[string]*deviceRecord  `json:"devices"`
	Pairings    map[string]*pairRecord    `json:"pairings"`
	Runs        map[string]*runRecord     `json:"runs"`
	Sessions    map[string]*sessionRecord `json:"sessions"`
	Idempotency map[string]string         `json:"idempotency"`
}
type deviceRecord struct {
	Device         Device `json:"device"`
	CredentialHash string `json:"credential_hash"`
	ActiveRun      string `json:"active_run,omitempty"`
}
type pairRecord struct {
	Hash          string    `json:"hash"`
	ExpiresAt     time.Time `json:"expires_at"`
	DeviceID      string    `json:"device_id,omitempty"`
	RequestDigest string    `json:"request_digest,omitempty"`
}
type sessionRecord struct {
	ID        string    `json:"id"`
	DeviceID  string    `json:"device_id"`
	Agent     string    `json:"agent"`
	Workspace string    `json:"workspace"`
	NativeID  string    `json:"native_id,omitempty"`
	ActiveRun string    `json:"active_run,omitempty"`
	LastUsed  time.Time `json:"last_used"`
}
type runRecord struct {
	Run             Run                  `json:"run"`
	RequestDigest   string               `json:"request_digest"`
	ClaimID         string               `json:"claim_id,omitempty"`
	Started         bool                 `json:"started"`
	Events          []Event              `json:"events"`
	EventDigests    []string             `json:"event_digests"`
	EventBytes      int                  `json:"event_bytes"`
	Approvals       map[string]*Approval `json:"approvals"`
	Key             string               `json:"key"`
	KeyIssuedAt     time.Time            `json:"key_issued_at"`
	FinalCount      int                  `json:"final_count"`
	FinalBytes      int                  `json:"final_bytes"`
	UnknownReported bool                 `json:"unknown_reported"`
}

var opaqueID = regexp.MustCompile(`^[a-f0-9]{32}$`)
var ownerHash = regexp.MustCompile(`^[a-f0-9]{64}$`)
var alias = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}$`)

// NewService constructs a protocol service without side effects or migrations.
func NewService(store Store, options Options) (*Service, error) {
	if store == nil {
		return nil, errors.New("remote store is required")
	}
	if options.Namespace == "" {
		options.Namespace = "default"
	}
	if !alias.MatchString(options.Namespace) {
		return nil, errors.New("invalid remote namespace")
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.LeaseDuration == 0 {
		options.LeaseDuration = 45 * time.Second
	}
	if options.PairingDuration == 0 {
		options.PairingDuration = 5 * time.Minute
	}
	if options.LeaseDuration < 5*time.Second || options.LeaseDuration > 10*time.Minute || options.PairingDuration < time.Second || options.PairingDuration > time.Hour {
		return nil, errors.New("invalid remote duration")
	}
	if options.RetentionDuration == 0 {
		options.RetentionDuration = 24 * time.Hour
	}
	if options.SessionRetention == 0 {
		options.SessionRetention = 7 * 24 * time.Hour
		if options.SessionRetention < options.RetentionDuration {
			options.SessionRetention = options.RetentionDuration
		}
	}
	if options.RetentionDuration < time.Hour || options.RetentionDuration > 90*24*time.Hour || options.SessionRetention < options.RetentionDuration || options.SessionRetention > 365*24*time.Hour {
		return nil, errors.New("invalid remote retention")
	}
	l := &options.Limits
	if l.MaxSessions == 0 {
		l.MaxSessions = 256
	}
	if l.MaxDevices == 0 {
		l.MaxDevices = 32
	}
	if l.MaxRuns == 0 {
		l.MaxRuns = 256
	}
	if l.MaxEventsPerRun == 0 {
		l.MaxEventsPerRun = 2048
	}
	if l.MaxRunEventBytes == 0 {
		l.MaxRunEventBytes = 128 << 10
	}
	if l.MaxPromptBytes == 0 {
		l.MaxPromptBytes = 16 << 10
	}
	if l.MaxStateBytes == 0 {
		l.MaxStateBytes = 32 << 20
	}
	if l.MaxSessions < 1 || l.MaxSessions > 4096 || l.MaxDevices < 1 || l.MaxDevices > 1024 || l.MaxRuns < 1 || l.MaxRuns > 1024 || l.MaxEventsPerRun < 1 || l.MaxEventsPerRun > 65536 || l.MaxRunEventBytes < 1024 || l.MaxRunEventBytes > 4<<20 || l.MaxPromptBytes < 1 || l.MaxPromptBytes > 64<<10 || l.MaxStateBytes < 64<<10 || l.MaxStateBytes > 48<<20 {
		return nil, errors.New("invalid remote limits")
	}
	return &Service{store: store, options: options}, nil
}
func hash(s string) string   { v := sha256.Sum256([]byte(s)); return hex.EncodeToString(v[:]) }
func equal(a, b string) bool { return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 }
func randomID() (string, error) {
	b := make([]byte, 16)
	if _, e := rand.Read(b); e != nil {
		return "", e
	}
	return hex.EncodeToString(b), nil
}
func randomSecret() (string, error) {
	b := make([]byte, 32)
	if _, e := rand.Read(b); e != nil {
		return "", e
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
func tokenParts(token, prefix string) (string, string, error) {
	p := strings.Split(token, ".")
	if len(p) != 4 || p[0] != prefix || !ownerHash.MatchString(p[1]) || !opaqueID.MatchString(p[2]) {
		return "", "", problem("unauthorized", 401)
	}
	raw, e := base64.RawURLEncoding.DecodeString(p[3])
	if e != nil || len(raw) != 32 {
		return "", "", problem("unauthorized", 401)
	}
	return p[1], p[2], nil
}
func bounded(s string, n int) bool {
	return s != "" && len(s) <= n && utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}
func validatePrincipal(p Principal) error {
	if !bounded(p.ID, 256) {
		return problem("unauthorized", 401)
	}
	return nil
}
func newState(principal string) *state {
	return &state{Protocol: Protocol, Principal: principal, Devices: map[string]*deviceRecord{}, Pairings: map[string]*pairRecord{}, Runs: map[string]*runRecord{}, Sessions: map[string]*sessionRecord{}, Idempotency: map[string]string{}}
}
func (s *Service) key(h string) string { return s.options.Namespace + "/principal/" + h }
func (s *Service) now() time.Time      { return s.options.Now().UTC() }

func (s *Service) mutate(ctx context.Context, h, principal string, fn func(*state, time.Time) error) error {
	for retry := 0; retry < 64; retry++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		old, err := s.store.Load(ctx, s.key(h))
		if err != nil {
			return err
		}
		st := newState(principal)
		if old.Version != 0 {
			if len(old.Data) > s.options.Limits.MaxStateBytes || json.Unmarshal(old.Data, st) != nil || st.Protocol != Protocol || st.Principal == "" || hash(st.Principal) != h || st.Devices == nil || st.Pairings == nil || st.Runs == nil || st.Sessions == nil || st.Idempotency == nil {
				return errors.New("corrupt remote protocol state")
			}
		} else if principal == "" {
			return problem("unauthorized", 401)
		}
		if err := validateStoredState(st); err != nil {
			return err
		}
		if principal != "" && st.Principal != principal {
			return problem("unauthorized", 401)
		}
		if s.options.ValidatePrincipal != nil {
			if err = s.options.ValidatePrincipal(ctx, Principal{ID: st.Principal}); err != nil {
				return problem("principal_unavailable", 403)
			}
		}
		now := s.now()
		expireRuns(st, now)
		s.prune(st, now)
		if err = fn(st, now); err != nil {
			return err
		}
		data, err := json.Marshal(st)
		if err != nil {
			return err
		}
		reserved := 0
		for _, r := range st.Runs {
			if !r.Run.Terminal() {
				reserved += 8192
			}
		}
		if len(data)+reserved > s.options.Limits.MaxStateBytes {
			return problem("state_capacity", 409)
		}
		if old.Version != 0 && bytes.Equal(old.Data, data) {
			return nil
		}
		ok, err := s.store.CompareAndSwap(ctx, s.key(h), old.Version, data)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		timer := time.NewTimer(time.Duration(1+retry%8) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return problem("state_busy", 503)
}
func expireRuns(st *state, now time.Time) {
	for _, r := range st.Runs {
		if (r.Run.State == StateClaimed || r.Run.State == StateRunning) && !now.Before(r.Run.LeaseUntil) {
			r.Run.State = StateUnknown
			r.Run.CancelRequested = true
			r.Run.UpdatedAt = now
		}
	}
}
func checkDevice(st *state, id, token string) (*deviceRecord, error) {
	d := st.Devices[id]
	if d == nil || d.Device.Revoked || !equal(d.CredentialHash, hash(token)) {
		return nil, problem("unauthorized", 401)
	}
	return d, nil
}
func validateManifest(agents []Agent, workspaces []Workspace) error {
	if len(agents) < 1 || len(agents) > 16 || len(workspaces) < 1 || len(workspaces) > 32 {
		return problem("invalid_manifest", 400)
	}
	seen := map[string]bool{}
	for _, a := range agents {
		if !alias.MatchString(a.Name) || seen[a.Name] || len(a.Capabilities) > 16 {
			return problem("invalid_manifest", 400)
		}
		seen[a.Name] = true
		caps := map[string]bool{}
		for _, c := range a.Capabilities {
			if !alias.MatchString(c) || caps[c] {
				return problem("invalid_manifest", 400)
			}
			caps[c] = true
		}
	}
	seen = map[string]bool{}
	for _, w := range workspaces {
		if !alias.MatchString(w.ID) || !bounded(w.Name, 128) || seen[w.ID] {
			return problem("invalid_manifest", 400)
		}
		seen[w.ID] = true
	}
	return nil
}
func supported(d Device, agent, workspace string) bool {
	a, w := false, false
	for _, v := range d.Agents {
		if v.Name == agent {
			a = true
		}
	}
	for _, v := range d.Workspaces {
		if v.ID == workspace {
			w = true
		}
	}
	return a && w
}
func digestValue(v any) (string, error) {
	b, e := json.Marshal(v)
	if e != nil {
		return "", e
	}
	return hash(string(b)), nil
}
func cloneDevice(d Device) Device {
	d.Agents = append([]Agent(nil), d.Agents...)
	for i := range d.Agents {
		d.Agents[i].Capabilities = append([]string(nil), d.Agents[i].Capabilities...)
	}
	d.Workspaces = append([]Workspace(nil), d.Workspaces...)
	return d
}

// CreatePairing creates one short-lived invitation for an authenticated owner.
func (s *Service) CreatePairing(ctx context.Context, p Principal, req PairingRequest) (Pairing, error) {
	var out Pairing
	if err := validatePrincipal(p); err != nil {
		return out, err
	}
	if req.Name != "" && !bounded(req.Name, 128) {
		return out, problem("invalid_name", 400)
	}
	id, err := randomID()
	if err != nil {
		return out, err
	}
	secret, err := randomSecret()
	if err != nil {
		return out, err
	}
	h := hash(p.ID)
	code := "gp1." + h + "." + id + "." + secret
	err = s.mutate(ctx, h, p.ID, func(st *state, now time.Time) error {
		for k, v := range st.Pairings {
			if v.DeviceID == "" && !now.Before(v.ExpiresAt) {
				delete(st.Pairings, k)
			}
		}
		if len(st.Pairings) >= 128 {
			return problem("pairing_capacity", 409)
		}
		out = Pairing{Code: code, PrincipalID: p.ID, ExpiresAt: now.Add(s.options.PairingDuration)}
		st.Pairings[id] = &pairRecord{Hash: hash(code), ExpiresAt: out.ExpiresAt}
		return nil
	})
	return out, err
}

// NewEnrollment generates a replayable device identity. Set ExpectedPrincipal
// from the human-approved pairing screen before saving and sending this request.
func NewEnrollment(code, name string, agents []Agent, workspaces []Workspace) (EnrollmentRequest, Credentials, error) {
	var req EnrollmentRequest
	var credentials Credentials
	h, _, err := tokenParts(code, "gp1")
	if err != nil {
		return req, credentials, err
	}
	if !bounded(name, 128) {
		return req, credentials, problem("invalid_name", 400)
	}
	if err = validateManifest(agents, workspaces); err != nil {
		return req, credentials, err
	}
	id, err := randomID()
	if err != nil {
		return req, credentials, err
	}
	secret, err := randomSecret()
	if err != nil {
		return req, credentials, err
	}
	credentials = Credentials{DeviceID: id, Token: "gd1." + h + "." + id + "." + secret}
	req = EnrollmentRequest{Code: code, DeviceID: id, Credential: credentials.Token, Name: name, Agents: agents, Workspaces: workspaces}
	return req, credentials, nil
}

// Enroll consumes a pairing once. Identical enrollment retries return the same
// device; a different credential or payload cannot take over a consumed code.
func (s *Service) Enroll(ctx context.Context, req EnrollmentRequest) (Device, error) {
	var out Device
	h, pairID, err := tokenParts(req.Code, "gp1")
	if err != nil {
		return out, err
	}
	th, deviceID, err := tokenParts(req.Credential, "gd1")
	if err != nil || th != h || deviceID != req.DeviceID || !bounded(req.Name, 128) || !bounded(req.ExpectedPrincipal, 256) || hash(req.ExpectedPrincipal) != h {
		return out, problem("invalid_enrollment", 400)
	}
	if err = validateManifest(req.Agents, req.Workspaces); err != nil {
		return out, err
	}
	digest, err := digestValue(req)
	if err != nil {
		return out, err
	}
	err = s.mutate(ctx, h, "", func(st *state, now time.Time) error {
		pair := st.Pairings[pairID]
		if pair == nil || !equal(pair.Hash, hash(req.Code)) || st.Principal != req.ExpectedPrincipal {
			return problem("invalid_pairing", 403)
		}
		if pair.DeviceID != "" {
			if pair.DeviceID != deviceID || !equal(pair.RequestDigest, digest) {
				return problem("pairing_consumed", 409)
			}
			d, err := checkDevice(st, deviceID, req.Credential)
			if err != nil {
				return err
			}
			out = cloneDevice(d.Device)
			return nil
		}
		if !now.Before(pair.ExpiresAt) {
			return problem("pairing_expired", 410)
		}
		if len(st.Devices) >= s.options.Limits.MaxDevices {
			return problem("device_capacity", 409)
		}
		if st.Devices[deviceID] != nil {
			return problem("device_conflict", 409)
		}
		out = cloneDevice(Device{ID: deviceID, PrincipalID: st.Principal, Name: req.Name, Agents: req.Agents, Workspaces: req.Workspaces, CreatedAt: now, LastSeen: now})
		st.Devices[deviceID] = &deviceRecord{Device: out, CredentialHash: hash(req.Credential)}
		pair.DeviceID = deviceID
		pair.RequestDigest = digest
		return nil
	})
	return out, err
}

// Devices lists only devices belonging to the authenticated principal.
func (s *Service) Devices(ctx context.Context, p Principal) ([]Device, error) {
	out := []Device{}
	if err := validatePrincipal(p); err != nil {
		return nil, err
	}
	err := s.mutate(ctx, hash(p.ID), p.ID, func(st *state, _ time.Time) error {
		out = out[:0]
		for _, v := range st.Devices {
			out = append(out, cloneDevice(v.Device))
		}
		sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
		return nil
	})
	return out, err
}

// RevokeDevice immediately invalidates the credential and queued assignments.
// An active offline process becomes unknown, never falsely "stopped".
func (s *Service) RevokeDevice(ctx context.Context, p Principal, id string) error {
	if err := validatePrincipal(p); err != nil {
		return err
	}
	return s.mutate(ctx, hash(p.ID), p.ID, func(st *state, now time.Time) error {
		d := st.Devices[id]
		if d == nil {
			return problem("not_found", 404)
		}
		d.Device.Revoked = true
		for _, r := range st.Runs {
			if r.Run.DeviceID == id && !r.Run.Terminal() {
				r.Run.CancelRequested = true
				r.Run.UpdatedAt = now
				if r.Run.State == StateQueued {
					r.Run.State = StateCancelled
					releaseRun(st, r)
				} else {
					r.Run.State = StateUnknown
				}
			}
		}
		return nil
	})
}

// Device validates a device credential, account status and revocation state.
func (s *Service) Device(ctx context.Context, token string) (Device, error) {
	var out Device
	h, id, err := tokenParts(token, "gd1")
	if err != nil {
		return out, err
	}
	err = s.mutate(ctx, h, "", func(st *state, _ time.Time) error {
		d, e := checkDevice(st, id, token)
		if e != nil {
			return e
		}
		out = cloneDevice(d.Device)
		return nil
	})
	return out, err
}

// SafeError converts internal failures to a public code without exposing details.
func SafeError(err error) error {
	if err == nil {
		return nil
	}
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return problem("request_interrupted", http.StatusServiceUnavailable)
	}
	return problem("internal_error", 500)
}
