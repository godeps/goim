package goim

import (
	"context"
	"github.com/godeps/cc-connect/core"
	"testing"
	"time"
)

func TestPermissionDeliveryBackpressure(t *testing.T) {
	s := newSession(nil, "test")
	defer s.Close()
	for range cap(s.events) {
		s.events <- core.Event{}
	}
	done := make(chan struct{})
	go func() { s.emitPermission(context.Background(), core.Event{RequestID: "approval"}); close(done) }()
	for range cap(s.events) {
		<-s.events
	}
	select {
	case event := <-s.events:
		if event.RequestID != "approval" {
			t.Fatalf("unexpected event: %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("approval was dropped")
	}
	<-done
}

func TestPermissionDeliveryCancellation(t *testing.T) {
	for _, closeSession := range []bool{false, true} {
		s := newSession(nil, "test")
		for range cap(s.events) {
			s.events <- core.Event{}
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { s.emitPermission(ctx, core.Event{}); close(done) }()
		if closeSession {
			s.Close()
		} else {
			cancel()
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("blocked delivery did not stop")
		}
		cancel()
		s.Close()
	}
}
