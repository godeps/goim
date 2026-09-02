package goim

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
	"github.com/godeps/cc-connect/core"
)

// Config is a simplified subset of cc-connect's config.toml focused on the
// fields needed to run an IM bridge.
type Config struct {
	DataDir       string              `toml:"data_dir"`
	Language      string              `toml:"language"`
	Log           LogConfig           `toml:"log"`
	StreamPreview StreamPreviewConfig `toml:"stream_preview"`
	Project       ProjectConfig       `toml:"project"`
}

type LogConfig struct {
	Level string `toml:"level"`
}

type StreamPreviewConfig struct {
	Enabled       *bool `toml:"enabled"`
	IntervalMs    *int  `toml:"interval_ms"`
	MinDeltaChars *int  `toml:"min_delta_chars"`
	MaxChars      *int  `toml:"max_chars"`
}

type ProjectConfig struct {
	Name      string           `toml:"name"`
	Platforms []PlatformConfig `toml:"platforms"`
}

type PlatformConfig struct {
	Type    string         `toml:"type"`
	Options map[string]any `toml:"options"`
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	t := true
	interval := 1500
	delta := 30
	maxc := 2000
	return Config{
		Language: "zh",
		Log:      LogConfig{Level: "info"},
		StreamPreview: StreamPreviewConfig{
			Enabled:       &t,
			IntervalMs:    &interval,
			MinDeltaChars: &delta,
			MaxChars:      &maxc,
		},
		Project: ProjectConfig{
			Name: "goim",
		},
	}
}

// LoadConfig reads a config file. Returns defaults if path is empty.
func LoadConfig(path string) (Config, error) {
	cfg := DefaultConfig()
	if path == "" {
		return cfg, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("read config: %w", err)
	}
	if err := toml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config: %w", err)
	}
	return cfg, nil
}

// ConfigFromFlags builds a minimal Config from CLI flags for the common case
// of a single platform without a config file.
func ConfigFromFlags(platform, token, allowFrom string) Config {
	cfg := DefaultConfig()
	cfg.Project.Platforms = []PlatformConfig{
		{
			Type: platform,
			Options: map[string]any{
				"token":      token,
				"allow_from": allowFrom,
			},
		},
	}
	return cfg
}

// CreatePlatforms instantiates cc-connect Platform objects from config.
func CreatePlatforms(cfg Config) ([]core.Platform, error) {
	var platforms []core.Platform
	for _, pc := range cfg.Project.Platforms {
		p, err := core.CreatePlatform(pc.Type, pc.Options)
		if err != nil {
			return nil, fmt.Errorf("create platform %q: %w", pc.Type, err)
		}
		platforms = append(platforms, p)
	}
	if len(platforms) == 0 {
		return nil, fmt.Errorf("no platforms configured")
	}
	return platforms, nil
}

// ChannelsConfig is the JSON-native config format stored in ~/.animus/channels.json (user-global).
// Each key in Channels is a platform name (e.g. "telegram", "feishu") and the
// value is passed directly to core.CreatePlatform as options.
type ChannelsConfig struct {
	Channels      map[string]map[string]any `json:"channels"`
	Language      string                    `json:"language,omitempty"`
	StreamPreview *StreamPreviewJSON        `json:"stream_preview,omitempty"`
}

// StreamPreviewJSON is the JSON variant of StreamPreviewConfig.
type StreamPreviewJSON struct {
	Enabled       *bool `json:"enabled,omitempty"`
	IntervalMs    *int  `json:"interval_ms,omitempty"`
	MinDeltaChars *int  `json:"min_delta_chars,omitempty"`
	MaxChars      *int  `json:"max_chars,omitempty"`
}

// LoadChannelsJSON reads a channels.json file.
// Returns an empty config (not an error) if the file does not exist.
func LoadChannelsJSON(path string) (ChannelsConfig, error) {
	var cfg ChannelsConfig
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return ChannelsConfig{Channels: make(map[string]map[string]any)}, nil
		}
		return cfg, fmt.Errorf("read channels config: %w", err)
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parse channels config: %w", err)
	}
	if cfg.Channels == nil {
		cfg.Channels = make(map[string]map[string]any)
	}
	return cfg, nil
}

// SaveChannelsJSON writes the channels config to a JSON file, creating
// parent directories as needed.
func SaveChannelsJSON(path string, cfg ChannelsConfig) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal channels config: %w", err)
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

// ToConfig converts a ChannelsConfig to the internal Config format used by
// CreatePlatforms. Channels with enabled=false are skipped.
func (c ChannelsConfig) ToConfig() Config {
	cfg := DefaultConfig()
	if c.Language != "" {
		cfg.Language = c.Language
	}
	if c.StreamPreview != nil {
		if c.StreamPreview.Enabled != nil {
			cfg.StreamPreview.Enabled = c.StreamPreview.Enabled
		}
		if c.StreamPreview.IntervalMs != nil {
			cfg.StreamPreview.IntervalMs = c.StreamPreview.IntervalMs
		}
		if c.StreamPreview.MinDeltaChars != nil {
			cfg.StreamPreview.MinDeltaChars = c.StreamPreview.MinDeltaChars
		}
		if c.StreamPreview.MaxChars != nil {
			cfg.StreamPreview.MaxChars = c.StreamPreview.MaxChars
		}
	}
	for name, opts := range c.Channels {
		// Skip disabled channels.
		if enabled, ok := opts["enabled"].(bool); ok && !enabled {
			continue
		}
		// Copy opts without the "enabled" key (not a platform option).
		platformOpts := make(map[string]any, len(opts))
		for k, v := range opts {
			if k != "enabled" {
				platformOpts[k] = v
			}
		}
		cfg.Project.Platforms = append(cfg.Project.Platforms, PlatformConfig{
			Type:    name,
			Options: platformOpts,
		})
	}
	return cfg
}

// LookupChannel returns the options for a specific channel from a ChannelsConfig.
// Returns nil if the channel is not found or is disabled.
func (c ChannelsConfig) LookupChannel(platform string) map[string]any {
	opts, ok := c.Channels[platform]
	if !ok {
		return nil
	}
	if enabled, ok := opts["enabled"].(bool); ok && !enabled {
		return nil
	}
	return opts
}

// ResolveDataDir returns the data directory, defaulting to .animus/connect.
func ResolveDataDir(cfg Config) string {
	if cfg.DataDir != "" {
		return cfg.DataDir
	}
	return ".animus/connect"
}

// ToStreamPreviewCfg converts to cc-connect's core.StreamPreviewCfg.
func (sp StreamPreviewConfig) ToStreamPreviewCfg() core.StreamPreviewCfg {
	cfg := core.DefaultStreamPreviewCfg()
	if sp.Enabled != nil {
		cfg.Enabled = *sp.Enabled
	}
	if sp.IntervalMs != nil {
		cfg.IntervalMs = *sp.IntervalMs
	}
	if sp.MinDeltaChars != nil {
		cfg.MinDeltaChars = *sp.MinDeltaChars
	}
	if sp.MaxChars != nil {
		cfg.MaxChars = *sp.MaxChars
	}
	return cfg
}

// ResolveLang converts a language string to a cc-connect Language constant.
func ResolveLang(lang string) core.Language {
	switch lang {
	case "zh":
		return core.LangChinese
	case "en":
		return core.LangEnglish
	default:
		return core.LangAuto
	}
}
