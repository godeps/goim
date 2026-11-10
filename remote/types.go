// Package remote provides an authenticated, durable remote-agent protocol.
// Hosts own account authentication and resource policy; clients own local process
// authorization, an execution-intent journal and a persistent event outbox.
package remote

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

// Protocol is the version of the independently negotiated remote-agent wire.
const Protocol = "goim-remote-1"

// Principal is an already authenticated host account, never a wire claim.
type Principal struct {
	ID string `json:"id"`
}

// Agent declares one explicitly configured local executor.
type Agent struct {
	Name         string   `json:"name"`
	Capabilities []string `json:"capabilities"`
}

// Workspace identifies a locally authorized directory without exposing its path.
type Workspace struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// Device is the public device projection; credential hashes are never returned.
type Device struct {
	ID          string      `json:"id"`
	PrincipalID string      `json:"principal_id"`
	Name        string      `json:"name"`
	Agents      []Agent     `json:"agents"`
	Workspaces  []Workspace `json:"workspaces"`
	CreatedAt   time.Time   `json:"created_at"`
	LastSeen    time.Time   `json:"last_seen"`
	Revoked     bool        `json:"revoked"`
}

// PairingRequest creates a short-lived single-device invitation.
type PairingRequest struct {
	Name string `json:"name"`
}

// Pairing is displayed to its authenticated owner. Code is a secret.
type Pairing struct {
	Code        string    `json:"code"`
	PrincipalID string    `json:"principal_id"`
	ExpiresAt   time.Time `json:"expires_at"`
}

// EnrollmentRequest must be saved privately with Credentials before enrollment.
// Repeating the exact request after a lost response preserves device identity.
type EnrollmentRequest struct {
	Code              string      `json:"code"`
	ExpectedPrincipal string      `json:"expected_principal"`
	DeviceID          string      `json:"device_id"`
	Credential        string      `json:"credential"`
	Name              string      `json:"name"`
	Agents            []Agent     `json:"agents"`
	Workspaces        []Workspace `json:"workspaces"`
}

// Credentials are a private client secret. Do not log or render Token.
type Credentials struct {
	DeviceID string `json:"device_id"`
	Token    string `json:"token"`
}

// CreateRunRequest binds an immutable prompt to one device, workspace and agent.
// SessionID is a goim logical session ID, never a provider-native thread ID.
type CreateRunRequest struct {
	IdempotencyKey string `json:"idempotency_key"`
	DeviceID       string `json:"device_id"`
	Agent          string `json:"agent"`
	Workspace      string `json:"workspace"`
	SessionID      string `json:"session_id,omitempty"`
	Prompt         string `json:"prompt"`
}

const (
	StateQueued     = "queued"
	StateClaimed    = "claimed"
	StateRunning    = "running"
	StateSucceeded  = "succeeded"
	StateFailed     = "failed"
	StateCancelled  = "cancelled"
	StateUnknown    = "unknown"
	EventStarted    = "started"
	EventText       = "text"
	EventTool       = "tool"
	EventToolResult = "tool_result"
	EventApproval   = "approval"
	EventCompleted  = "completed"
	EventFailed     = "failed"
	EventCancelled  = "cancelled"
	EventUnknown    = "unknown"
)

// EventLimits is the immutable per-run streaming budget. Ordinary event limits
// exclude outcome reports. At most one unknown report and one known-stopped
// terminal report each have a separately reserved MaxTerminalBytes slot.
type EventLimits struct {
	MaxEventsPerRun    int `json:"max_events_per_run"`
	MaxRunEventBytes   int `json:"max_run_event_bytes"`
	MaxEventTextBytes  int `json:"max_event_text_bytes"`
	MaxEventInputBytes int `json:"max_event_input_bytes"`
	MaxEventBytes      int `json:"max_event_bytes"`
	MaxTerminalBytes   int `json:"max_terminal_bytes"`
	MaxFinalEvents     int `json:"max_final_events"`
}

// Run records execution facts. A terminal success does not adopt host artifacts.
type Run struct {
	Limits          EventLimits `json:"limits"`
	ID              string      `json:"id"`
	DeviceID        string      `json:"device_id"`
	Agent           string      `json:"agent"`
	Workspace       string      `json:"workspace"`
	SessionID       string      `json:"session_id"`
	NativeSessionID string      `json:"native_session_id,omitempty"`
	Prompt          string      `json:"prompt"`
	State           string      `json:"state"`
	AttemptID       string      `json:"attempt_id"`
	Fence           uint64      `json:"fence"`
	LeaseUntil      time.Time   `json:"lease_until"`
	CancelRequested bool        `json:"cancel_requested"`
	LastSeq         uint64      `json:"last_seq"`
	CreatedAt       time.Time   `json:"created_at"`
	UpdatedAt       time.Time   `json:"updated_at"`
}

// Terminal reports confirmed terminal execution facts. Unknown is not terminal:
// the previous process must be reconciled before any replacement may run.
func (r Run) Terminal() bool {
	return r.State == StateSucceeded || r.State == StateFailed || r.State == StateCancelled
}

// PollRequest identifies a durable claim attempt. Reuse ID after a lost response.
type PollRequest struct {
	ID string `json:"id"`
}

// Assignment.New means this poll originally claimed the run; it does not
// authorize replay after the client's local execution intent became uncertain.
type Assignment struct {
	Run Run  `json:"run"`
	New bool `json:"new"`
}

// PollResponse can contain an existing assignment requiring reconciliation.
type PollResponse struct {
	Assignment        *Assignment `json:"assignment,omitempty"`
	RetryAfterSeconds int         `json:"retry_after_seconds"`
}

// HeartbeatRequest identifies the exact previously claimed execution.
type HeartbeatRequest struct {
	AttemptID string `json:"attempt_id"`
	Fence     uint64 `json:"fence"`
}

// Event is an immutable original outbox event. Seq starts at 1 per run.
// SessionID, when present, is the provider-native session/thread identifier.
type Event struct {
	Seq       uint64          `json:"seq"`
	Type      string          `json:"type"`
	Text      string          `json:"text,omitempty"`
	SessionID string          `json:"session_id,omitempty"`
	RequestID string          `json:"request_id,omitempty"`
	Tool      string          `json:"tool,omitempty"`
	Input     json.RawMessage `json:"input,omitempty"`
}

// EventBatch is retried with the same original events until Receipt confirms it.
type EventBatch struct {
	AttemptID string  `json:"attempt_id"`
	Fence     uint64  `json:"fence"`
	Events    []Event `json:"events"`
}

// Receipt confirms a persisted prefix; it is not a task-success signal.
type Receipt struct {
	RunID     string `json:"run_id"`
	AttemptID string `json:"attempt_id"`
	AckSeq    uint64 `json:"ack_seq"`
	State     string `json:"state"`
}

// Approval is bound to one attempt and immutable tool request digest.
type Approval struct {
	RequestID  string          `json:"request_id"`
	AttemptID  string          `json:"attempt_id"`
	Digest     string          `json:"digest"`
	Tool       string          `json:"tool"`
	Input      json.RawMessage `json:"input,omitempty"`
	Decision   string          `json:"decision,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	AnsweredAt time.Time       `json:"answered_at,omitempty"`
}

// AnswerRequest must echo the exact displayed request binding.
type AnswerRequest struct {
	AttemptID string `json:"attempt_id"`
	Digest    string `json:"digest"`
	Decision  string `json:"decision"`
}

// Control is the current authoritative instruction for the exact run.
type Control struct {
	Run             Run        `json:"run"`
	CancelRequested bool       `json:"cancel_requested"`
	Approvals       []Approval `json:"approvals"`
}

// EventsResponse contains a bounded persisted event page and a stable cursor.
type EventsResponse struct {
	Run       Run        `json:"run"`
	Events    []Event    `json:"events"`
	Next      uint64     `json:"next"`
	Approvals []Approval `json:"approvals"`
}

// Limits bound all persisted state. Capacity errors never discard originals.
type Limits struct {
	MaxDevices       int `json:"max_devices"`
	MaxRuns          int `json:"max_runs"`
	MaxSessions      int `json:"max_sessions"`
	MaxEventsPerRun  int `json:"max_events_per_run"`
	MaxRunEventBytes int `json:"max_run_event_bytes"`
	MaxPromptBytes   int `json:"max_prompt_bytes"`
	MaxStateBytes    int `json:"max_state_bytes"`
}

// Options configures a service. Namespace must be unique inside a shared Store.
// ValidatePrincipal must recheck host account status for both device and human
// operations; authentication alone must not keep a disabled account executable.
type Options struct {
	Namespace         string
	LeaseDuration     time.Duration
	PairingDuration   time.Duration
	RetentionDuration time.Duration
	SessionRetention  time.Duration
	Limits            Limits
	Now               func() time.Time
	ValidatePrincipal func(context.Context, Principal) error
	// AuthorizeRun applies optional host resource policy to each submitted intent.
	// Like ValidatePrincipal, it must be side-effect-free and safe to retry.
	AuthorizeRun func(context.Context, Principal, CreateRunRequest) error
}

// HTTPOptions authenticates human routes including host write CSRF policy.
// Device routes authenticate their own bearer credential and call the service's
// ValidatePrincipal. The caller mounts this handler below one fixed URL prefix.
type HTTPOptions struct {
	Authenticate func(*http.Request) (Principal, error)
}

// ClientOptions allows explicit loopback HTTP only in local integration tests.
type ClientOptions struct {
	HTTPClient *http.Client
	AllowHTTP  bool
}

// Error has a stable, public error code with no credentials or internal SQL.
type Error struct {
	Code   string `json:"code"`
	Status int    `json:"-"`
}

func (e *Error) Error() string { return e.Code }

// ErrorCode returns a stable service error code.
func ErrorCode(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return "internal_error"
}

// ErrorStatus returns the safe HTTP status for a service error.
func ErrorStatus(err error) int {
	var e *Error
	if errors.As(err, &e) {
		return e.Status
	}
	return http.StatusInternalServerError
}
func problem(code string, status int) error { return &Error{Code: code, Status: status} }
