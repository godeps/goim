package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

// Client speaks the versioned device protocol to one explicitly trusted base.
// It performs no implicit retries: the owner persists enrollment, claim intent
// and original outbox events before retrying the corresponding operation.
type Client struct {
	base        string
	credentials Credentials
	http        *http.Client
}

// NewClient requires the full mount URL, for example
// https://peek.example/api/agents/v1. Redirects are refused to keep credentials
// bound to that endpoint. Insecure HTTP is limited to explicit loopback tests.
func NewClient(baseURL string, credentials Credentials, options ClientOptions) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" || u.RawPath != "" {
		return nil, errors.New("invalid remote service URL")
	}
	if u.Scheme != "https" {
		host := u.Hostname()
		ip := net.ParseIP(host)
		if u.Scheme != "http" || !options.AllowHTTP || (host != "localhost" && (ip == nil || !ip.IsLoopback())) {
			return nil, errors.New("remote service requires HTTPS")
		}
	}
	if u.Path != "" && u.Path != "/" && path.Clean(u.Path) != strings.TrimSuffix(u.Path, "/") {
		return nil, errors.New("invalid remote service path")
	}
	_, id, err := tokenParts(credentials.Token, "gd1")
	if err != nil || credentials.DeviceID != id {
		return nil, errors.New("invalid remote device credentials")
	}
	client := http.Client{Timeout: 25 * time.Second}
	if options.HTTPClient != nil {
		client = *options.HTTPClient
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return errors.New("remote redirects are disabled") }
	client.Jar = nil
	return &Client{base: strings.TrimRight(u.String(), "/"), credentials: credentials, http: &client}, nil
}
func (c *Client) call(ctx context.Context, method, route string, body, out any, authenticated bool) error {
	var input io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return problem("invalid_request", 400)
		}
		input = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+route, input)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Goim-Protocol", Protocol)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if authenticated {
		req.Header.Set("Authorization", "Bearer "+c.credentials.Token)
	}
	response, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, (8<<20)+1))
	if err != nil {
		return err
	}
	if len(raw) > 8<<20 {
		return problem("invalid_response", 502)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var envelope struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if json.Unmarshal(raw, &envelope) != nil || !alias.MatchString(envelope.Error.Code) {
			return problem("remote_error", response.StatusCode)
		}
		return problem(envelope.Error.Code, response.StatusCode)
	}
	if response.Header.Get("X-Goim-Protocol") != Protocol {
		return problem("protocol_mismatch", 502)
	}
	if err = json.Unmarshal(raw, out); err != nil {
		return problem("invalid_response", 502)
	}
	return nil
}
func (c *Client) validateDevice(d Device, expected string) error {
	h, _, _ := tokenParts(c.credentials.Token, "gd1")
	if d.ID != c.credentials.DeviceID || hash(d.PrincipalID) != h || (expected != "" && d.PrincipalID != expected) || d.Revoked {
		return problem("invalid_device_binding", 502)
	}
	return nil
}

// Enroll retries only the caller's already persisted enrollment identity.
func (c *Client) Enroll(ctx context.Context, req EnrollmentRequest) (Device, error) {
	var out Device
	if req.DeviceID != c.credentials.DeviceID || !equal(req.Credential, c.credentials.Token) {
		return out, problem("invalid_enrollment_binding", 400)
	}
	if err := c.call(ctx, http.MethodPost, "/devices/enroll", req, &out, false); err != nil {
		return out, err
	}
	return out, c.validateDevice(out, req.ExpectedPrincipal)
}

// Device revalidates the stored device identity and the host account status.
func (c *Client) Device(ctx context.Context) (Device, error) {
	var out Device
	if err := c.call(ctx, http.MethodGet, "/device", nil, &out, true); err != nil {
		return out, err
	}
	return out, c.validateDevice(out, "")
}

// Poll retries the same persisted claim ID after a lost response. Assignment.New
// is not authority to launch again after an uncertain local execution intent.
func (c *Client) Poll(ctx context.Context, req PollRequest) (PollResponse, error) {
	var out PollResponse
	if err := c.call(ctx, http.MethodPost, "/device/poll", req, &out, true); err != nil {
		return out, err
	}
	if out.Assignment != nil {
		r := out.Assignment.Run
		if r.DeviceID != c.credentials.DeviceID || !opaqueID.MatchString(r.ID) || !opaqueID.MatchString(r.AttemptID) || r.Fence == 0 || !alias.MatchString(r.Agent) || !alias.MatchString(r.Workspace) || !alias.MatchString(r.SessionID) {
			return PollResponse{}, problem("invalid_assignment", 502)
		}
	}
	return out, nil
}

// Heartbeat obtains exact approval decisions and a stop request independently
// of event delivery; a blocked outbox must not suppress local cancellation.
func (c *Client) Heartbeat(ctx context.Context, runID string, req HeartbeatRequest) (Control, error) {
	var out Control
	if !opaqueID.MatchString(runID) {
		return out, problem("invalid_run", 400)
	}
	if err := c.call(ctx, http.MethodPost, "/device/runs/"+runID+"/heartbeat", req, &out, true); err != nil {
		return out, err
	}
	if out.Run.ID != runID || out.Run.DeviceID != c.credentials.DeviceID || out.Run.AttemptID != req.AttemptID || out.Run.Fence != req.Fence {
		return Control{}, problem("invalid_control_binding", 502)
	}
	return out, nil
}

// AppendEvents acknowledges the persisted original sequence prefix only.
func (c *Client) AppendEvents(ctx context.Context, runID string, req EventBatch) (Receipt, error) {
	var out Receipt
	if !opaqueID.MatchString(runID) || len(req.Events) == 0 {
		return out, problem("invalid_batch", 400)
	}
	if err := c.call(ctx, http.MethodPost, "/device/runs/"+runID+"/events", req, &out, true); err != nil {
		return out, err
	}
	if out.RunID != runID || out.AttemptID != req.AttemptID || out.AckSeq < req.Events[len(req.Events)-1].Seq {
		return Receipt{}, problem("invalid_receipt", 502)
	}
	return out, nil
}
