//go:build linux || darwin

package runner

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestNativeAgentPeer is a synthetic provider-native protocol peer running in a
// real OS child. It does not call a model or use developer credentials.
func TestNativeAgentPeer(t *testing.T) {
	agent := os.Getenv("RUNNER_TEST_AGENT")
	if agent == "" {
		return
	}
	mode := os.Getenv("RUNNER_TEST_MODE")
	enc := json.NewEncoder(os.Stdout)
	write := func(v any) {
		if enc.Encode(v) != nil {
			os.Exit(3)
		}
	}
	finish := func(text string) {
		if agent == "codex" {
			write(map[string]any{"method": "item/agentMessage/delta", "params": map[string]any{"threadId": "native-fixture", "turnId": "turn-fixture", "itemId": "message-fixture", "delta": text}})
			if mode == "eof" {
				os.Exit(0)
			}
			write(map[string]any{"method": "item/completed", "params": map[string]any{"threadId": "native-fixture", "turnId": "turn-fixture", "item": map[string]any{"id": "message-fixture", "type": "agentMessage", "text": text}}})
			status := "completed"
			if mode == "fail" {
				status = "failed"
			}
			write(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "native-fixture", "turn": map[string]any{"id": "turn-fixture", "status": status}}})
		} else {
			write(map[string]any{"type": "stream_event", "session_id": "native-fixture", "event": map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]any{"type": "text_delta", "text": text}}})
			if mode == "eof" {
				os.Exit(0)
			}
			write(map[string]any{"type": "assistant", "message": map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "text", "text": text}}}})
			write(map[string]any{"type": "result", "subtype": "success", "session_id": "native-fixture", "is_error": mode == "fail", "num_turns": 1})
		}
	}
	approvalResponses := 0
	input := bufio.NewScanner(os.Stdin)
	input.Buffer(make([]byte, 4096), 2<<20)
	for input.Scan() {
		var m map[string]any
		if json.Unmarshal(input.Bytes(), &m) != nil {
			os.Exit(4)
		}
		if agent == "codex" {
			params, _ := m["params"].(map[string]any)
			switch m["method"] {
			case "initialize":
				write(map[string]any{"id": m["id"], "result": map[string]any{"protocolVersion": "2026-08-03", "userAgent": "runner-fixture/1.0.0", "serverInfo": map[string]any{"name": "runner-fixture", "version": "1.0.0"}}})
			case "thread/start", "thread/resume":
				if m["method"] == "thread/resume" && params["threadId"] != "native-fixture" {
					os.Exit(5)
				}
				write(map[string]any{"id": m["id"], "result": map[string]any{"thread": map[string]any{"id": "native-fixture"}}})
			case "turn/start":
				write(map[string]any{"id": m["id"], "result": map[string]any{"turn": map[string]any{"id": "turn-fixture", "status": "in_progress"}}})
				if mode == "approval" || mode == "approval-twice" {
					write(map[string]any{"id": "approval-fixture", "method": "item/commandExecution/requestApproval", "params": map[string]any{"threadId": "native-fixture", "turnId": "turn-fixture", "itemId": "tool-fixture", "command": "pwd"}})
				} else if mode == "cancel" {
					write(map[string]any{"method": "item/agentMessage/delta", "params": map[string]any{"threadId": "native-fixture", "turnId": "turn-fixture", "itemId": "message-fixture", "delta": "waiting"}})
				} else {
					finish("hello")
				}
			case "turn/interrupt":
				write(map[string]any{"id": m["id"], "result": map[string]any{}})
			default:
				if m["id"] == "approval-fixture" {
					approvalResponses++
					if mode == "approval-twice" && approvalResponses == 1 {
						write(map[string]any{"id": "approval-fixture", "method": "item/commandExecution/requestApproval", "params": map[string]any{"threadId": "native-fixture", "turnId": "turn-fixture", "itemId": "tool-fixture", "command": "pwd"}})
						continue
					}
					result, _ := m["result"].(map[string]any)
					if result["decision"] == "accept" {
						finish("allowed")
					} else {
						finish("denied")
					}
				}
			}
		} else {
			switch m["type"] {
			case "control_request":
				request, _ := m["request"].(map[string]any)
				write(map[string]any{"type": "control_response", "response": map[string]any{"subtype": "success", "request_id": m["request_id"], "response": map[string]any{}}})
				if request["subtype"] == "interrupt" {
					continue
				}
			case "user":
				if mode == "approval" || mode == "approval-twice" {
					write(map[string]any{"type": "control_request", "request_id": "approval-fixture", "request": map[string]any{"subtype": "can_use_tool", "tool_name": "Bash", "tool_use_id": "tool-fixture", "input": map[string]any{"command": "pwd"}}})
				} else if mode == "cancel" {
					write(map[string]any{"type": "stream_event", "session_id": "native-fixture", "event": map[string]any{"type": "content_block_delta", "delta": map[string]any{"type": "text_delta", "text": "waiting"}}})
				} else {
					finish("hello")
				}
			case "control_response":
				approvalResponses++
				if mode == "approval-twice" && approvalResponses == 1 {
					write(map[string]any{"type": "control_request", "request_id": "approval-fixture", "request": map[string]any{"subtype": "can_use_tool", "tool_name": "Bash", "tool_use_id": "tool-fixture", "input": map[string]any{"command": "pwd"}}})
					continue
				}
				response, _ := m["response"].(map[string]any)
				result, _ := response["response"].(map[string]any)
				if result["behavior"] == "allow" {
					finish("allowed")
				} else {
					finish("denied")
				}
			}
		}
	}
	// Exercise real POSIX shutdown outcomes after an explicit native terminal
	// event. An EOF on stdin need not terminate a direct app-server executable.
	switch mode {
	case "shutdown-interrupt":
		time.Sleep(30 * time.Second)
		os.Exit(8)
	case "shutdown-exit17":
		os.Exit(17)
	}
	os.Exit(0)
}
func fixture(t *testing.T, agent, mode string) (*Executor, Request) {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "agent")
	escaped := strings.ReplaceAll(binary, "'", "'\\''")
	if err = os.WriteFile(script, []byte("#!/bin/sh\nexec '"+escaped+"' -test.run=TestNativeAgentPeer -- \"$@\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	req := Request{Agent: agent, Prompt: "fixture prompt", CWD: dir, Env: map[string]string{"PATH": os.Getenv("PATH"), "RUNNER_TEST_AGENT": agent, "RUNNER_TEST_MODE": mode}, CommandHook: func(cmd *exec.Cmd) error {
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		cmd.Cancel = func() error {
			if cmd.Process == nil {
				return nil
			}
			return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		return nil
	}}
	req.StopProcess = func(cmd *exec.Cmd) error {
		err := cmd.Cancel()
		if err == syscall.ESRCH {
			return nil
		}
		return err
	}
	return &Executor{CodexPath: script, ClaudePath: script}, req
}
func TestNativeStreamsResumeAndTerminalResults(t *testing.T) {
	for _, agent := range []string{"codex", "claude"} {
		t.Run(agent, func(t *testing.T) {
			e, req := fixture(t, agent, "normal")
			for turn := 0; turn < 2; turn++ {
				var text strings.Builder
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				result, err := e.Run(ctx, req, func(event Event) error {
					if event.Type == "text" {
						text.WriteString(event.Text)
					}
					return nil
				}, func(context.Context, ApprovalRequest) (bool, error) { return false, fmt.Errorf("unexpected approval") })
				cancel()
				if err != nil || result.State != Completed || result.SessionID != "native-fixture" || text.String() != "hello" {
					t.Fatalf("turn %d: result=%+v text=%q err=%v", turn, result, text.String(), err)
				}
				req.SessionID = result.SessionID
			}
		})
	}
}
func TestNativeApprovalAndCancellation(t *testing.T) {
	for _, agent := range []string{"codex", "claude"} {
		t.Run(agent+"-deny", func(t *testing.T) {
			e, req := fixture(t, agent, "approval")
			count := 0
			var text strings.Builder
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result, err := e.Run(ctx, req, func(event Event) error {
				if event.Type == "text" {
					text.WriteString(event.Text)
				}
				return nil
			}, func(_ context.Context, r ApprovalRequest) (bool, error) {
				count++
				if r.ID == "" || len(r.Input) == 0 {
					t.Error("missing native request binding")
				}
				return false, nil
			})
			if err != nil || result.State != Completed || count != 1 || text.String() != "denied" {
				t.Fatalf("result=%+v count=%d text=%q err=%v", result, count, text.String(), err)
			}
		})
		t.Run(agent+"-cancel", func(t *testing.T) {
			e, req := fixture(t, agent, "cancel")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result, err := e.Run(ctx, req, func(event Event) error {
				if event.Type == "text" {
					cancel()
				}
				return nil
			}, func(context.Context, ApprovalRequest) (bool, error) { return false, nil })
			if result.State != Cancelled || err == nil {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
}
func TestNativeFailureIsNotSuccess(t *testing.T) {
	for _, agent := range []string{"codex", "claude"} {
		t.Run(agent, func(t *testing.T) {
			e, req := fixture(t, agent, "fail")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result, err := e.Run(ctx, req, func(Event) error { return nil }, func(context.Context, ApprovalRequest) (bool, error) { return false, nil })
			if result.State != Failed || err == nil {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
}
