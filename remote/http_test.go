package remote

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPStrictBodiesAndAuthentication(t *testing.T) {
	f := newFixture(t)
	_, creds, _ := f.enroll("alice")
	cases := []struct {
		name, body string
		status     int
	}{
		{"duplicate", `{"name":"a","name":"b"}`, 400},
		{"unknown", `{"name":"a","user_id":"victim"}`, 400},
		{"trailing", `{"name":"a"}{"name":"b"}`, 400},
		{"null", `null`, 400},
		{"array", `[]`, 400},
		{"oversized", `{"name":"` + strings.Repeat("x", maxHTTPBody) + `"}`, 413},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest("POST", f.server.URL+"/api/agents/v1/pairings", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Test-User", "alice")
			response, err := f.server.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != tc.status {
				t.Fatalf("status=%d want=%d", response.StatusCode, tc.status)
			}
		})
	}
	req, _ := http.NewRequest("POST", f.server.URL+"/api/agents/v1/runs", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+creds.Token)
	response, err := f.server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 401 {
		t.Fatal("device credential authorized a human route")
	}
	req, _ = http.NewRequest("GET", f.server.URL+"/api/agents/v1/device?token=secret", nil)
	req.Header.Set("Authorization", "Bearer "+creds.Token)
	response, err = f.server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 400 {
		t.Fatal("credential query transport accepted")
	}
}

func TestClientRequiresTLSAndRefusesCredentialRedirect(t *testing.T) {
	f := newFixture(t)
	_, creds, _ := f.enroll("alice")
	for _, address := range []string{"http://example.org/api", "https://user:password@example.org/api", "https://example.org/api?token=x", "https://example.org/api/../target"} {
		if _, err := NewClient(address, creds, ClientOptions{AllowHTTP: true}); err == nil {
			t.Fatalf("unsafe endpoint accepted: %s", address)
		}
	}
	received := false
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { received = true; w.WriteHeader(200) }))
	defer target.Close()
	redirect := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	client, err := NewClient(redirect.URL, creds, ClientOptions{HTTPClient: redirect.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.Device(context.Background()); err == nil {
		t.Fatal("redirect followed")
	}
	if received {
		t.Fatal("credentials reached redirect target")
	}
}

type brokenStore struct{}

func (brokenStore) Load(context.Context, string) (Snapshot, error) {
	return Snapshot{}, errors.New("password=private SQL internal failure")
}
func (brokenStore) CompareAndSwap(context.Context, string, uint64, []byte) (bool, error) {
	return false, errors.New("private")
}
func TestHTTPDoesNotLeakInternalStorageError(t *testing.T) {
	service, err := NewService(brokenStore{}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	handler := NewHandler(service, HTTPOptions{Authenticate: func(*http.Request) (Principal, error) { return Principal{"alice"}, nil }})
	handler.ServeHTTP(recorder, httptest.NewRequest("GET", "/runs", nil))
	result := recorder.Result()
	defer result.Body.Close()
	raw, _ := io.ReadAll(result.Body)
	if result.StatusCode != 500 || strings.Contains(string(raw), "private") || !strings.Contains(string(raw), "internal_error") {
		t.Fatalf("unsafe error: %d %s", result.StatusCode, raw)
	}
}
