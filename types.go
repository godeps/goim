// Package goim provides a platform-agnostic IM gateway that bridges
// any Runtime (agent backend) to multiple chat platforms via cc-connect.
package goim

import (
	"context"

	"github.com/chenhg5/cc-connect/core"
)

// PermissionResult is the user's response to a permission request. Hosts can
// use this alias without importing the underlying cc-connect module.
type PermissionResult = core.PermissionResult

// Runtime is the interface that host applications must implement to integrate
// with goim. It receives user messages from IM platforms and streams back responses.
type Runtime interface {
	RunStream(ctx context.Context, req Request) (<-chan StreamEvent, error)
}

// UserInputSteerer is optionally implemented by a Runtime to deliver answers
// to non-blocking questions into an active turn. Return false when the session
// has no active turn so the engine can send the answer as a new message.
type UserInputSteerer interface {
	SteerUserInput(sessionID, prompt string) (bool, error)
}

// Request represents a user message forwarded from an IM platform.
type Request struct {
	Prompt    string
	SessionID string
	// MessageID is the originating platform message ID, useful for runtimes
	// that scope attachments or state per message. It may be empty for
	// synthesized messages.
	MessageID     string
	ContentBlocks []ContentBlock
}

// ContentBlock carries multimodal content (e.g., images, files).
type ContentBlock struct {
	Type      string // e.g. "image" or "file"
	MediaType string // e.g. "image/png"
	FileName  string // original file name (for "file" blocks)
	Data      string // base64-encoded data
}

// Attachment is a file sent from the host application to an IM platform. It
// mirrors ContentBlock for the outbound direction.
type Attachment struct {
	// Kind selects the transport. Empty means "infer from MimeType".
	Kind     string
	MimeType string
	FileName string
	Data     []byte
}

// Attachment kinds. Kind picks the platform sender: images get the platform's
// image upload path, audio and video their native media bubbles, and anything
// else is delivered as a document.
const (
	AttachmentKindImage = "image"
	AttachmentKindAudio = "audio"
	AttachmentKindVideo = "video"
	AttachmentKindFile  = "file"
)

// StreamEvent is a single event in a streaming response.
type StreamEvent struct {
	Type      string
	Name      string      // tool name (for tool events)
	Delta     *Delta      // text delta (for content events)
	Output    interface{} // tool output or error message
	SessionID string
	// Permission request fields (for EventPermissionRequest).
	RequestID    string      // unique ID for matching user response
	ToolInputRaw interface{} // raw tool input for display
	Questions    []Question  // structured questions (optional)
	ToolInput    string      // human-readable tool input or question context
}

// Question is a structured permission question (mirrors cc-connect core.UserQuestion).
type Question struct {
	Question    string
	Header      string
	Options     []QuestionOption
	MultiSelect bool
}

// QuestionOption is one choice in a Question.
type QuestionOption struct {
	Label       string
	Description string
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
	EventPermissionRequest   = "permission_request" // agent requests user permission for a tool
	EventUserInputRequest    = "user_input_request" // ask without blocking the active turn
	EventThinking            = "thinking"           // processing status, separate from answer text
)
