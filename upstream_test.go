package goim

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/chenhg5/cc-connect/core"
)

func TestUpstreamPlatformsRegistered(t *testing.T) {
	registered := core.ListRegisteredPlatforms()
	for _, name := range []string{"cloud_web", "dingtalk", "discord", "feishu", "googlechat", "line", "matrix", "max", "qq", "qqbot", "slack", "telegram", "tuitui", "webex", "wecom", "weibo", "weixin", "wps-agentspace", "wps-xiezuo", "yuanbao"} {
		t.Run(name, func(t *testing.T) {
			if !slices.Contains(registered, name) {
				t.Fatalf("platform %q is not registered", name)
			}
		})
	}
}

func TestStructuredQuestionsReachEngine(t *testing.T) {
	for _, eventType := range []string{EventPermissionRequest, EventUserInputRequest} {
		t.Run(eventType, func(t *testing.T) {
			stream := make(chan StreamEvent, 1)
			stream <- StreamEvent{Type: eventType, Name: "AskUserQuestion", RequestID: "q1", SessionID: "runtime-id", ToolInput: "Select targets", ToolInputRaw: map[string]any{"key": "value"}, Questions: []Question{{Question: "Where?", Header: "Targets", MultiSelect: true, Options: []QuestionOption{{Label: "A", Description: "First target"}, {Label: "B", Description: "Second target"}}}}}
			close(stream)
			s := newSession(nil, "s1")
			defer s.Close()
			s.consumeStream(t.Context(), stream)
			event := <-s.Events()
			if event.Type != core.EventType(eventType) || event.ToolName != "AskUserQuestion" || event.RequestID != "q1" || event.SessionID != "runtime-id" || event.ToolInput != "Select targets" || event.ToolInputRaw["key"] != "value" {
				t.Fatalf("question envelope lost: %+v", event)
			}
			want := []core.UserQuestion{{Question: "Where?", Header: "Targets", MultiSelect: true, Options: []core.UserQuestionOption{{Label: "A", Description: "First target"}, {Label: "B", Description: "Second target"}}}}
			if !reflect.DeepEqual(event.Questions, want) {
				t.Fatalf("questions = %+v, want %+v", event.Questions, want)
			}
			if terminal := <-s.Events(); !terminal.Done {
				t.Fatal("question must not suppress terminal result")
			}
		})
	}
}

func TestThinkingStaysOutOfAnswer(t *testing.T) {
	stream := make(chan StreamEvent, 3)
	stream <- StreamEvent{Type: EventThinking, Delta: &Delta{Text: "Working"}}
	stream <- StreamEvent{Type: EventThinking, Output: "Checking"}
	stream <- StreamEvent{Type: EventContentBlockDelta, Delta: &Delta{Text: "Answer"}}
	close(stream)
	s := newSession(nil, "s1")
	defer s.Close()
	s.consumeStream(t.Context(), stream)
	for _, want := range []string{"Working", "Checking"} {
		if evt := <-s.Events(); evt.Type != core.EventThinking || evt.Content != want {
			t.Fatalf("thinking = %+v", evt)
		}
	}
	<-s.Events() // answer delta
	if evt := <-s.Events(); !evt.Done || evt.Content != "Answer" {
		t.Fatalf("final answer = %+v", evt)
	}
}

type steeringRuntime struct {
	sessionID, prompt string
	err               error
}

func (r *steeringRuntime) RunStream(context.Context, Request) (<-chan StreamEvent, error) {
	return nil, nil
}
func (r *steeringRuntime) SteerUserInput(sessionID, prompt string) (bool, error) {
	r.sessionID, r.prompt = sessionID, prompt
	return true, r.err
}

func TestSessionSteerUserInput(t *testing.T) {
	rt := &steeringRuntime{}
	s := newSession(rt, "s1")
	defer s.Close()
	var steerer core.UserInputSteerer = s
	if handled, err := steerer.SteerUserInput("A"); !handled || err != nil || rt.sessionID != "s1" || rt.prompt != "A" {
		t.Fatalf("steering failed: handled=%v err=%v runtime=%+v", handled, err, rt)
	}
	rt.err = errors.New("steering failed")
	if _, err := steerer.SteerUserInput("B"); !errors.Is(err, rt.err) {
		t.Fatalf("error = %v", err)
	}
	s.Close()
	if handled, err := steerer.SteerUserInput("C"); handled || err != nil || rt.prompt != "B" {
		t.Fatal("closed session forwarded input")
	}
	unsupported := newSession(nil, "s2")
	defer unsupported.Close()
	if handled, err := unsupported.SteerUserInput("A"); handled || err != nil {
		t.Fatal("unsupported runtime must fall back to a normal message")
	}
}
