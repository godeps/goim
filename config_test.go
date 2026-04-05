package goim

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadChannelsJSON_NonExistent(t *testing.T) {
	cfg, err := LoadChannelsJSON("/nonexistent/channels.json")
	if err != nil {
		t.Fatalf("expected no error for missing file, got: %v", err)
	}
	if cfg.Channels == nil {
		t.Fatal("expected initialized channels map")
	}
	if len(cfg.Channels) != 0 {
		t.Errorf("expected empty channels, got %d", len(cfg.Channels))
	}
}

func TestLoadChannelsJSON_Valid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "channels.json")

	data := `{
		"channels": {
			"telegram": {"token": "123:ABC", "enabled": true},
			"feishu": {"app_id": "cli_x", "app_secret": "s", "enabled": false}
		},
		"language": "en"
	}`
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadChannelsJSON(path)
	if err != nil {
		t.Fatalf("LoadChannelsJSON: %v", err)
	}
	if len(cfg.Channels) != 2 {
		t.Errorf("expected 2 channels, got %d", len(cfg.Channels))
	}
	if cfg.Language != "en" {
		t.Errorf("expected language 'en', got %q", cfg.Language)
	}
}

func TestLoadChannelsJSON_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "channels.json")
	if err := os.WriteFile(path, []byte("{invalid"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadChannelsJSON(path)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestSaveChannelsJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "channels.json")

	cfg := ChannelsConfig{
		Channels: map[string]map[string]any{
			"telegram": {"token": "abc", "enabled": true},
		},
		Language: "zh",
	}

	if err := SaveChannelsJSON(path, cfg); err != nil {
		t.Fatalf("SaveChannelsJSON: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read saved file: %v", err)
	}
	var loaded ChannelsConfig
	if err := json.Unmarshal(data, &loaded); err != nil {
		t.Fatalf("unmarshal saved file: %v", err)
	}
	if loaded.Language != "zh" {
		t.Errorf("expected language 'zh', got %q", loaded.Language)
	}
	tg := loaded.Channels["telegram"]
	if tg == nil {
		t.Fatal("telegram channel missing after save")
	}
	if tg["token"] != "abc" {
		t.Errorf("expected token 'abc', got %v", tg["token"])
	}
}

func TestChannelsConfig_ToConfig(t *testing.T) {
	cfg := ChannelsConfig{
		Channels: map[string]map[string]any{
			"telegram": {"token": "abc", "enabled": true},
			"feishu":   {"app_id": "x", "app_secret": "y", "enabled": false},
			"discord":  {"token": "disc123"},
		},
		Language: "en",
	}

	result := cfg.ToConfig()

	if result.Language != "en" {
		t.Errorf("expected language 'en', got %q", result.Language)
	}

	if len(result.Project.Platforms) != 2 {
		t.Fatalf("expected 2 platforms (feishu disabled), got %d", len(result.Project.Platforms))
	}

	found := make(map[string]bool)
	for _, p := range result.Project.Platforms {
		found[p.Type] = true
		if _, ok := p.Options["enabled"]; ok {
			t.Errorf("platform %q options should not contain 'enabled'", p.Type)
		}
	}
	if !found["telegram"] {
		t.Error("telegram not found in platforms")
	}
	if !found["discord"] {
		t.Error("discord not found in platforms")
	}
	if found["feishu"] {
		t.Error("feishu should be excluded (disabled)")
	}
}

func TestChannelsConfig_LookupChannel(t *testing.T) {
	cfg := ChannelsConfig{
		Channels: map[string]map[string]any{
			"telegram": {"token": "abc", "enabled": true},
			"feishu":   {"app_id": "x", "enabled": false},
		},
	}

	opts := cfg.LookupChannel("telegram")
	if opts == nil {
		t.Fatal("expected telegram channel")
	}
	if opts["token"] != "abc" {
		t.Errorf("expected token 'abc', got %v", opts["token"])
	}

	if cfg.LookupChannel("feishu") != nil {
		t.Error("expected nil for disabled feishu")
	}

	if cfg.LookupChannel("nonexistent") != nil {
		t.Error("expected nil for nonexistent")
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Language != "zh" {
		t.Errorf("expected language 'zh', got %q", cfg.Language)
	}
	if cfg.Project.Name != "goim" {
		t.Errorf("expected project name 'goim', got %q", cfg.Project.Name)
	}
	if cfg.StreamPreview.Enabled == nil || !*cfg.StreamPreview.Enabled {
		t.Error("expected stream preview enabled by default")
	}
}

func TestConfigFromFlags(t *testing.T) {
	cfg := ConfigFromFlags("telegram", "my-token", "user1,user2")
	if len(cfg.Project.Platforms) != 1 {
		t.Fatalf("expected 1 platform, got %d", len(cfg.Project.Platforms))
	}
	p := cfg.Project.Platforms[0]
	if p.Type != "telegram" {
		t.Errorf("expected type 'telegram', got %q", p.Type)
	}
	if p.Options["token"] != "my-token" {
		t.Errorf("expected token 'my-token', got %v", p.Options["token"])
	}
}
