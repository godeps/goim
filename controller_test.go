package goim

import (
	"path/filepath"
	"testing"
)

func TestIMController_InitialState(t *testing.T) {
	ctrl := NewIMController("")
	if ctrl.Running() {
		t.Error("expected not running initially")
	}
	if s := ctrl.Status(); s != "IM bridge: not running" {
		t.Errorf("unexpected status: %q", s)
	}
}

func TestIMController_StopWhenNotRunning(t *testing.T) {
	ctrl := NewIMController("")
	if err := ctrl.Stop(); err != nil {
		t.Errorf("Stop on idle controller should be no-op, got: %v", err)
	}
}

func TestIMController_StartMissingPlatform(t *testing.T) {
	ctrl := NewIMController("")
	err := ctrl.Start(nil, "", "", "token", "", "")
	if err == nil {
		t.Fatal("expected error for missing platform")
	}
}

func TestIMController_StartMissingToken(t *testing.T) {
	ctrl := NewIMController("")
	err := ctrl.Start(nil, "", "telegram", "", "", "")
	if err == nil {
		t.Fatal("expected error for missing token")
	}
}

func TestIMController_StartMissingConfigFile(t *testing.T) {
	ctrl := NewIMController("")
	err := ctrl.Start(nil, "", "", "", "", "/nonexistent/config.toml")
	if err == nil {
		t.Fatal("expected error for missing config file")
	}
}

func TestIMController_SaveChannel(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "channels.json")
	ctrl := NewIMController(path)

	err := ctrl.SaveChannel("telegram", map[string]any{"token": "abc"})
	if err != nil {
		t.Fatalf("SaveChannel: %v", err)
	}

	cfg, err := LoadChannelsJSON(path)
	if err != nil {
		t.Fatalf("LoadChannelsJSON: %v", err)
	}
	tg := cfg.Channels["telegram"]
	if tg == nil {
		t.Fatal("telegram not saved")
	}
	if tg["token"] != "abc" {
		t.Errorf("expected token 'abc', got %v", tg["token"])
	}
	if tg["enabled"] != true {
		t.Errorf("expected enabled=true, got %v", tg["enabled"])
	}
}

func TestIMController_SaveChannel_NoPath(t *testing.T) {
	ctrl := NewIMController("")
	if err := ctrl.SaveChannel("telegram", map[string]any{"token": "abc"}); err != nil {
		t.Errorf("expected no error, got: %v", err)
	}
}

func TestIMController_DeleteChannel(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "channels.json")
	ctrl := NewIMController(path)

	_ = ctrl.SaveChannel("telegram", map[string]any{"token": "abc"})

	if err := ctrl.DeleteChannel("telegram"); err != nil {
		t.Fatalf("DeleteChannel: %v", err)
	}

	cfg, _ := LoadChannelsJSON(path)
	if _, ok := cfg.Channels["telegram"]; ok {
		t.Error("telegram should have been deleted")
	}
}

func TestIMController_DeleteChannel_NotFound(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "channels.json")
	ctrl := NewIMController(path)

	if err := ctrl.DeleteChannel("nonexistent"); err == nil {
		t.Error("expected error for non-existent channel")
	}
}
