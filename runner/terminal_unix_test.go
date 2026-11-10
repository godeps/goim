//go:build linux || darwin

package runner

import (
	"context"
	"errors"
	"os/exec"
	"testing"
	"time"
)

func TestNativeEOFMustNotReportSuccess(t *testing.T) {
	for _, agent := range []string{"codex", "claude"} {
		t.Run(agent, func(t *testing.T) {
			executor, request := fixture(t, agent, "eof")
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			result, err := executor.Run(ctx, request, func(Event) error { return nil }, func(context.Context, ApprovalRequest) (bool, error) { return false, nil })
			if result.State != Unknown || err == nil {
				t.Fatalf("unconfirmed provider EOF: result=%+v err=%v", result, err)
			}
		})
	}
}
func TestNativeAllowIsOneExactRequest(t *testing.T) {
	for _, agent := range []string{"codex", "claude"} {
		t.Run(agent, func(t *testing.T) {
			executor, request := fixture(t, agent, "approval")
			calls := 0
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			result, err := executor.Run(ctx, request, func(Event) error { return nil }, func(_ context.Context, r ApprovalRequest) (bool, error) {
				calls++
				if r.ID == "" || r.ID == "tool-fixture" {
					t.Errorf("native request ID=%s", r.ID)
				}
				return true, nil
			})
			if result.State != Completed || err != nil || calls != 1 {
				t.Fatalf("approval result=%+v calls=%d err=%v", result, calls, err)
			}
		})
	}
}

func TestRepeatedNativeItemRequiresFreshApproval(t *testing.T) {
	for _, agent := range []string{"codex", "claude"} {
		t.Run(agent, func(t *testing.T) {
			executor, request := fixture(t, agent, "approval-twice")
			ids := []string{}
			text := ""
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			result, err := executor.Run(ctx, request, func(e Event) error {
				if e.Type == "text" {
					text += e.Text
				}
				return nil
			}, func(_ context.Context, r ApprovalRequest) (bool, error) {
				ids = append(ids, r.ID)
				return len(ids) == 1, nil
			})
			if err != nil || result.State != Completed || len(ids) != 2 || ids[0] == ids[1] || text != "denied" {
				t.Fatalf("reused approval: result=%+v ids=%v text=%q err=%v", result, ids, text, err)
			}
		})
	}
}
func TestStopFailureDoesNotBecomeCancelled(t *testing.T) {
	for _, agent := range []string{"codex", "claude"} {
		t.Run(agent, func(t *testing.T) {
			executor, request := fixture(t, agent, "cancel")
			original := request.StopProcess
			request.StopProcess = func(cmd *exec.Cmd) error {
				_ = original(cmd)
				return errors.New("synthetic group-stop confirmation failure")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			result, _ := executor.Run(ctx, request, func(e Event) error {
				if e.Type == "text" {
					cancel()
				}
				return nil
			}, func(context.Context, ApprovalRequest) (bool, error) { return false, nil })
			if result.State != Unknown || result.Stopped {
				t.Fatalf("unconfirmed group became terminal: %+v", result)
			}
		})
	}
}
