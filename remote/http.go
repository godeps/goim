package remote

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
)

const maxHTTPBody = 2 << 20

// NewHandler returns an independently mountable HTTP protocol handler. Human
// authentication (including browser CSRF for mutations) must be provided by the
// host. Device routes never consume host cookies or a self-asserted user header.
func NewHandler(service *Service, options HTTPOptions) http.Handler {
	mux := http.NewServeMux()
	if service == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { writeError(w, problem("service_unavailable", 503)) })
	}
	authenticate := func(r *http.Request) (Principal, error) {
		if options.Authenticate == nil {
			return Principal{}, problem("unauthorized", 401)
		}
		p, err := options.Authenticate(r)
		if err != nil {
			return Principal{}, problem("unauthorized", 401)
		}
		if err = validatePrincipal(p); err != nil {
			return Principal{}, err
		}
		return p, nil
	}
	humanRoute := func(pattern string, fn func(http.ResponseWriter, *http.Request, Principal)) {
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			p, err := authenticate(r)
			if err != nil {
				writeError(w, err)
				return
			}
			fn(w, r, p)
		})
	}
	mux.HandleFunc("GET /meta", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, struct {
			Protocol     string   `json:"protocol"`
			Capabilities []string `json:"capabilities"`
		}{Protocol, []string{"pairing", "durable-claims", "sequenced-events", "approvals", "cancellation", "native-session-resume"}})
	})
	humanRoute("GET /policy", func(w http.ResponseWriter, r *http.Request, p Principal) {
		v, e := service.Policy(r.Context(), p)
		respond(w, v, e)
	})
	humanRoute("POST /pairings", func(w http.ResponseWriter, r *http.Request, p Principal) {
		var req PairingRequest
		if !readJSON(w, r, &req) {
			return
		}
		v, err := service.CreatePairing(r.Context(), p, req)
		respond(w, v, err)
	})
	mux.HandleFunc("POST /devices/enroll", func(w http.ResponseWriter, r *http.Request) {
		var req EnrollmentRequest
		if !readJSON(w, r, &req) {
			return
		}
		v, err := service.Enroll(r.Context(), req)
		respond(w, v, err)
	})
	humanRoute("GET /devices", func(w http.ResponseWriter, r *http.Request, p Principal) {
		v, e := service.Devices(r.Context(), p)
		respond(w, v, e)
	})
	humanRoute("DELETE /devices/{id}", func(w http.ResponseWriter, r *http.Request, p Principal) {
		if e := service.RevokeDevice(r.Context(), p, r.PathValue("id")); e != nil {
			writeError(w, e)
			return
		}
		writeJSON(w, 200, struct {
			Revoked bool `json:"revoked"`
		}{true})
	})
	humanRoute("POST /runs", func(w http.ResponseWriter, r *http.Request, p Principal) {
		var req CreateRunRequest
		if !readJSON(w, r, &req) {
			return
		}
		v, e := service.CreateRun(r.Context(), p, req)
		respond(w, v, e)
	})
	humanRoute("GET /runs", func(w http.ResponseWriter, r *http.Request, p Principal) {
		v, e := service.Runs(r.Context(), p)
		respond(w, v, e)
	})
	humanRoute("GET /runs/{id}", func(w http.ResponseWriter, r *http.Request, p Principal) {
		v, e := service.Run(r.Context(), p, r.PathValue("id"))
		respond(w, v, e)
	})
	humanRoute("GET /runs/{id}/events", func(w http.ResponseWriter, r *http.Request, p Principal) {
		after := uint64(0)
		if raw := r.URL.Query().Get("after"); raw != "" {
			var err error
			after, err = strconv.ParseUint(raw, 10, 64)
			if err != nil {
				writeError(w, problem("invalid_cursor", 400))
				return
			}
		}
		v, e := service.Events(r.Context(), p, r.PathValue("id"), after)
		respond(w, v, e)
	})
	humanRoute("POST /runs/{id}/cancel", func(w http.ResponseWriter, r *http.Request, p Principal) {
		var req struct{}
		if !readJSON(w, r, &req) {
			return
		}
		v, e := service.CancelRun(r.Context(), p, r.PathValue("id"))
		respond(w, v, e)
	})
	humanRoute("POST /runs/{id}/requests/{request}/answer", func(w http.ResponseWriter, r *http.Request, p Principal) {
		var req AnswerRequest
		if !readJSON(w, r, &req) {
			return
		}
		v, e := service.Answer(r.Context(), p, r.PathValue("id"), r.PathValue("request"), req)
		respond(w, v, e)
	})
	deviceRoute := func(pattern string, fn func(http.ResponseWriter, *http.Request, string)) {
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			token, err := bearer(r)
			if err != nil {
				writeError(w, err)
				return
			}
			fn(w, r, token)
		})
	}
	deviceRoute("GET /device", func(w http.ResponseWriter, r *http.Request, token string) {
		v, e := service.Device(r.Context(), token)
		respond(w, v, e)
	})
	deviceRoute("POST /device/poll", func(w http.ResponseWriter, r *http.Request, token string) {
		var req PollRequest
		if !readJSON(w, r, &req) {
			return
		}
		v, e := service.Poll(r.Context(), token, req)
		respond(w, v, e)
	})
	deviceRoute("POST /device/runs/{id}/heartbeat", func(w http.ResponseWriter, r *http.Request, token string) {
		var req HeartbeatRequest
		if !readJSON(w, r, &req) {
			return
		}
		v, e := service.Heartbeat(r.Context(), token, r.PathValue("id"), req)
		respond(w, v, e)
	})
	deviceRoute("POST /device/runs/{id}/events", func(w http.ResponseWriter, r *http.Request, token string) {
		var req EventBatch
		if !readJSON(w, r, &req) {
			return
		}
		v, e := service.AppendEvents(r.Context(), token, r.PathValue("id"), req)
		respond(w, v, e)
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Goim-Protocol", Protocol)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if p := r.Header.Get("X-Goim-Protocol"); p != "" && p != Protocol {
			writeError(w, problem("protocol_mismatch", 400))
			return
		}
		// Query strings are never an authentication channel. Avoid accepting tokens
		// from URLs, where logs/history/referrers could persist the secret.
		for _, key := range []string{"token", "credential", "access_token"} {
			if r.URL.Query().Has(key) {
				writeError(w, problem("invalid_auth_transport", 400))
				return
			}
		}
		mux.ServeHTTP(w, r)
	})
}
func bearer(r *http.Request) (string, error) {
	values := r.Header.Values("Authorization")
	if len(values) != 1 {
		return "", problem("unauthorized", 401)
	}
	pieces := strings.Split(values[0], " ")
	if len(pieces) != 2 || pieces[0] != "Bearer" || pieces[1] == "" {
		return "", problem("unauthorized", 401)
	}
	return pieces[1], nil
}
func readJSON(w http.ResponseWriter, r *http.Request, out any) bool {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		writeError(w, problem("json_required", 415))
		return false
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxHTTPBody))
	if err != nil {
		var max *http.MaxBytesError
		if errors.As(err, &max) {
			writeError(w, problem("request_too_large", 413))
		} else {
			writeError(w, problem("invalid_json", 400))
		}
		return false
	}
	if err = checkJSON(raw); err != nil {
		writeError(w, problem("invalid_json", 400))
		return false
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err = dec.Decode(out); err != nil {
		writeError(w, problem("invalid_json", 400))
		return false
	}
	return true
}

// checkJSON rejects duplicate object fields and excessive nesting before normal
// typed decoding. Both request bindings and opaque tool arguments stay unambiguous.
func checkJSON(raw []byte) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var visit func(int) error
	visit = func(depth int) error {
		if depth > 64 {
			return errors.New("JSON depth")
		}
		token, err := dec.Token()
		if err != nil {
			return err
		}
		switch t := token.(type) {
		case json.Delim:
			switch t {
			case '{':
				keys := map[string]bool{}
				for dec.More() {
					name, err := dec.Token()
					if err != nil {
						return err
					}
					key, ok := name.(string)
					if !ok || keys[key] {
						return errors.New("duplicate JSON field")
					}
					keys[key] = true
					if err = visit(depth + 1); err != nil {
						return err
					}
				}
				close, err := dec.Token()
				if err != nil || close != json.Delim('}') {
					return errors.New("invalid JSON object")
				}
			case '[':
				for dec.More() {
					if err := visit(depth + 1); err != nil {
						return err
					}
				}
				close, err := dec.Token()
				if err != nil || close != json.Delim(']') {
					return errors.New("invalid JSON array")
				}
			default:
				return errors.New("invalid JSON delimiter")
			}
		}
		return nil
	}
	if len(bytes.TrimSpace(raw)) == 0 || bytes.TrimSpace(raw)[0] != '{' {
		return errors.New("JSON object required")
	}
	if err := visit(0); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}
func respond(w http.ResponseWriter, value any, err error) {
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, value)
}
func writeError(w http.ResponseWriter, err error) {
	safe := SafeError(err)
	writeJSON(w, ErrorStatus(safe), struct {
		Error *Error `json:"error"`
	}{&Error{Code: ErrorCode(safe)}})
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
