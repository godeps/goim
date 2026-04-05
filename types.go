// Package goim provides a platform-agnostic IM gateway that bridges
// any Runtime (agent backend) to multiple chat platforms via cc-connect.
package goim

import "context"

// Runtime is the interface that host applications must implement to integrate
// with goim. It receives user messages from IM platforms and streams back responses.
type Runtime interface {
	RunStream(ctx context.Context, req Request) (<-chan StreamEvent, error)
}

// Request represents a user message forwarded from an IM platform.
type Request struct {
	Prompt        string
	SessionID     string
	ContentBlocks []ContentBlock
}

// ContentBlock carries multimodal content (e.g., images).
type ContentBlock struct {
	Type      string // e.g. "image"
	MediaType string // e.g. "image/png"
	Data      string // base64-encoded data
}

// StreamEvent is a single event in a streaming response.
type StreamEvent struct {
	Type      string
	Name      string      // tool name (for tool events)
	Delta     *Delta      // text delta (for content events)
	Output    interface{} // tool output or error message
	SessionID string
}

// Delta carries incremental text content.
type Delta struct {
	Type string // e.g. "text_delta"
	Text string
}

// Stream event type constants.
const (
	EventContentBlockDelta   = "content_block_delta"
	EventToolExecutionStart  = "tool_execution_start"
	EventToolExecutionResult = "tool_execution_result"
	EventToolExecutionOutput = "tool_execution_output"
	EventError               = "error"
	EventMessageStop         = "message_stop"
)
