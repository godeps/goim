package runner

import (
	"context"
	"encoding/json"
	"errors"
	claude "github.com/godeps/claude-agent-sdk-go"
	"github.com/godeps/claude-agent-sdk-go/types"
)

func (e *Executor) runClaude(ctx context.Context, req Request, emit Emitter, approve Approver) (result Result, returnErr error) {
	result = Result{State: Unknown, SessionID: req.SessionID}
	p := &supervisedProcess{}
	options := types.NewClaudeAgentOptions().WithCLIPath(e.ClaudePath).WithCWD(req.CWD).WithEnv(req.Env).WithReplaceEnv(true).WithIncludePartialMessages(true).WithPermissionMode(types.PermissionModeDefault).WithCommandHook(p.hook(req.CommandHook))
	if req.SessionID != "" {
		options.WithResume(req.SessionID)
	}
	options.WithCanUseTool(func(c context.Context, name string, input map[string]interface{}, pc types.ToolPermissionContext) (interface{}, error) {
		payload, err := json.Marshal(map[string]any{"tool_use_id": pc.ToolUseID, "input": input})
		if err != nil {
			return nil, err
		}
		allowed, err := approve(c, ApprovalRequest{ID: approvalID(pc.ToolUseID), Tool: name, Input: payload})
		if err != nil {
			return types.PermissionResultDeny{Behavior: "deny", Message: "Remote approval unavailable", Interrupt: true}, nil
		}
		if allowed {
			return types.PermissionResultAllow{Behavior: "allow"}, nil
		}
		return types.PermissionResultDeny{Behavior: "deny", Message: "Request declined"}, nil
	})
	client, err := claude.NewClient(ctx, options)
	if err != nil {
		result.State = Failed
		result.Stopped = true
		return result, errors.New("claude CLI initialization failed")
	}
	connected := false
	defer func() {
		if ctx.Err() != nil && connected {
			stopCtx, done := cleanupContext()
			_ = client.Interrupt(stopCtx)
			done()
		}
		var stopErr error
		if returnErr != nil || ctx.Err() != nil || result.State == Cancelled {
			stopErr = p.stop(req.StopProcess)
		}
		closeCtx, cancel := cleanupContext()
		closeErr := client.Close(closeCtx)
		result.Stopped = closeErr == nil && closeCtx.Err() == nil
		cancel()
		stopErr = errors.Join(stopErr, p.stop(req.StopProcess))
		result.Stopped = result.Stopped && stopErr == nil
		if stopErr != nil {
			returnErr = errors.Join(returnErr, stopErr)
		}
		if !result.Stopped {
			result.State = Unknown
			returnErr = errors.Join(returnErr, errors.New("claude process group shutdown was not confirmed"))
		} else if ctx.Err() != nil {
			result.State = Cancelled
		}
	}()
	if err = client.Connect(ctx); err != nil {
		result.State = Failed
		return result, errors.New("claude CLI connection failed")
	}
	connected = true
	if err = emit(Event{Type: "started", SessionID: req.SessionID}); err != nil {
		return result, err
	}
	if err = client.Query(ctx, req.Prompt); err != nil {
		return result, errors.New("claude query start has uncertain outcome")
	}
	partial := false
	for msg := range client.ReceiveResponse(ctx) {
		switch m := msg.(type) {
		case *types.StreamEvent:
			if m.SessionID != "" {
				result.SessionID = m.SessionID
			}
			if m.Event["type"] == "message_start" {
				partial = false
			}
			if m.Event["type"] == "content_block_delta" {
				delta, _ := m.Event["delta"].(map[string]interface{})
				if delta["type"] == "text_delta" {
					text, _ := delta["text"].(string)
					partial = true
					if err = emitText(emit, text); err != nil {
						return result, err
					}
				}
			}
		case *types.AssistantMessage:
			if m.Error != nil {
				result.State = Failed
				return result, errors.New("claude assistant error")
			}
			for _, b := range m.Content {
				switch block := b.(type) {
				case *types.TextBlock:
					if !partial {
						if err = emitText(emit, block.Text); err != nil {
							return result, err
						}
					}
				case types.TextBlock:
					if !partial {
						if err = emitText(emit, block.Text); err != nil {
							return result, err
						}
					}
				case *types.ToolUseBlock:
					payload, _ := json.Marshal(block.Input)
					if err = emit(Event{Type: "tool", RequestID: block.ID, Tool: block.Name, Input: payload}); err != nil {
						return result, err
					}
				case types.ToolUseBlock:
					payload, _ := json.Marshal(block.Input)
					if err = emit(Event{Type: "tool", RequestID: block.ID, Tool: block.Name, Input: payload}); err != nil {
						return result, err
					}
				}
			}
			partial = false
		case *types.UserMessage:
			if blocks, ok := m.Content.([]types.ContentBlock); ok {
				for _, b := range blocks {
					switch block := b.(type) {
					case *types.ToolResultBlock:
						payload, _ := json.Marshal(block)
						if err = emit(Event{Type: "tool_result", RequestID: block.ToolUseID, Input: payload}); err != nil {
							return result, err
						}
					case types.ToolResultBlock:
						payload, _ := json.Marshal(block)
						if err = emit(Event{Type: "tool_result", RequestID: block.ToolUseID, Input: payload}); err != nil {
							return result, err
						}
					}
				}
			}
		case *types.SystemMessage:
			if m.SessionID != "" {
				result.SessionID = m.SessionID
			}
		case *types.ResultMessage:
			if m.SessionID != "" {
				result.SessionID = m.SessionID
			}
			if m.IsError {
				result.State = Failed
				return result, errors.New("claude turn failed")
			}
			result.State = Completed
			return result, nil
		}
	}
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	return result, errors.New("claude result event missing")
}
