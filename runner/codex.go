package runner

import (
	"context"
	"encoding/json"
	"errors"
	codex "github.com/godeps/codex-sdk-go"
	"io"
)

func codexApproval(ctx context.Context, method string, params json.RawMessage, approve Approver) (json.RawMessage, error) {
	legacy := false
	switch method {
	case "item/commandExecution/requestApproval", "item/fileChange/requestApproval":
	case "execCommandApproval", "applyPatchApproval":
		legacy = true
	default:
		return nil, errors.New("unsupported native approval request")
	}
	var ids struct {
		ItemID string `json:"itemId"`
		CallID string `json:"callId"`
	}
	if err := json.Unmarshal(params, &ids); err != nil {
		return nil, err
	}
	id := ids.ItemID
	if id == "" {
		id = ids.CallID
	}
	allowed, err := approve(ctx, ApprovalRequest{ID: approvalID(id), Tool: method, Input: append(json.RawMessage(nil), params...)})
	if err != nil {
		return nil, err
	}
	decision := "decline"
	if allowed {
		decision = "accept"
	}
	if legacy {
		decision = "denied"
		if allowed {
			decision = "approved"
		}
	}
	return json.Marshal(map[string]string{"decision": decision})
}
func (e *Executor) runCodex(ctx context.Context, req Request, emit Emitter, approve Approver) (result Result, returnErr error) {
	result = Result{State: Unknown, SessionID: req.SessionID}
	p := &supervisedProcess{}
	client, err := codex.NewClient(ctx, codex.WithCodexPath(e.CodexPath), codex.WithEnv(req.Env), codex.WithCommandHook(p.hook(req.CommandHook)), codex.WithServerRequestHandler(func(c context.Context, m string, b json.RawMessage) (json.RawMessage, error) {
		return codexApproval(c, m, b, approve)
	}))
	if err != nil {
		stopErr := p.stop(req.StopProcess)
		result.Stopped = stopErr == nil
		if result.Stopped {
			result.State = Failed
		}
		return result, errors.New("codex CLI initialization failed")
	}
	defer func() {
		var stopErr error
		stopping := returnErr != nil || ctx.Err() != nil || result.State == Cancelled
		if stopping {
			stopErr = p.stop(req.StopProcess)
		}
		closeCtx, cancel := cleanupContext()
		closeErr := client.CloseContext(closeCtx)
		cancel()
		// A graceful parent exit can leave background descendants. Confirm the
		// entire owned group after close too, including successful turns.
		stopErr = errors.Join(stopErr, p.stop(req.StopProcess))
		waitCtx, done := cleanupContext()
		_ = client.WaitContext(waitCtx)
		result.Stopped = stopErr == nil && waitCtx.Err() == nil
		done()
		if stopErr != nil {
			returnErr = errors.Join(returnErr, stopErr)
		}
		if !result.Stopped {
			result.State = Unknown
			returnErr = errors.Join(returnErr, errors.New("codex process group shutdown was not confirmed"))
		} else if ctx.Err() != nil {
			result.State = Cancelled
		} else if closeErr != nil && result.State == Completed {
			result.State = Failed
			returnErr = errors.Join(returnErr, errors.New("codex process exited with a shutdown failure"))
		}
	}()
	options := codex.ThreadOptions{WorkingDirectory: req.CWD, SandboxMode: codex.SandboxWorkspaceWrite, ApprovalPolicy: codex.ApprovalOnRequest, WebSearchMode: codex.WebSearchDisabled}
	var thread *codex.Thread
	if req.SessionID != "" {
		thread, err = client.ResumeThread(ctx, req.SessionID, options)
	} else {
		thread, err = client.StartThread(ctx, options)
	}
	if err != nil {
		result.State = Failed
		return result, errors.New("codex thread initialization failed")
	}
	result.SessionID = thread.ID()
	if err = emit(Event{Type: "started", SessionID: thread.ID()}); err != nil {
		return result, err
	}
	turn, err := thread.StartTurnContext(ctx, codex.TextInput(req.Prompt), codex.TurnOptions{})
	if err != nil {
		return result, errors.New("codex turn start has uncertain outcome")
	}
	stream, err := turn.Stream()
	if err != nil {
		return result, err
	}
	defer stream.Close()
	streamed := map[string]bool{}
	for {
		event, nextErr := stream.Next(ctx)
		if nextErr != nil {
			if ctx.Err() != nil {
				stopCtx, done := cleanupContext()
				_ = turn.InterruptContext(stopCtx)
				done()
				return result, ctx.Err()
			}
			if errors.Is(nextErr, io.EOF) {
				break
			}
			return result, errors.New("codex stream ended without confirmed completion")
		}
		if event == nil {
			continue
		}
		switch event.Type {
		case "item.agent_message.delta":
			streamed[event.ItemID] = true
			if err = emitText(emit, event.Delta); err != nil {
				return result, err
			}
		case "item.started", "item.completed":
			if msg, ok := event.Item.(*codex.AgentMessageItem); ok {
				if event.Type == "item.completed" && !streamed[msg.ID] {
					if err = emitText(emit, msg.Text); err != nil {
						return result, err
					}
				}
			} else if event.Item != nil && (event.Item.ItemType() == "command_execution" || event.Item.ItemType() == "file_change" || event.Item.ItemType() == "mcp_tool_call") {
				payload, _ := json.Marshal(event.Item)
				typ := "tool"
				if event.Type == "item.completed" {
					typ = "tool_result"
				}
				if err = emit(Event{Type: typ, Tool: event.Item.ItemType(), Input: payload}); err != nil {
					return result, err
				}
			}
		case "turn.failed":
			result.State = Failed
			return result, errors.New("codex turn failed")
		case "turn.completed":
			if event.Turn == nil {
				return result, errors.New("codex terminal state missing")
			}
			switch event.Turn.Status {
			case codex.TurnStatusCompleted:
				result.State = Completed
			case codex.TurnStatusInterrupted:
				result.State = Cancelled
			case codex.TurnStatusFailed:
				result.State = Failed
			default:
				result.State = Unknown
			}
			return result, nil
		}
	}
	return result, errors.New("codex terminal event missing")
}
