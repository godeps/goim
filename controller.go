package goim

import (
	"fmt"
	"log"
	"sync"
)

// IMController manages the lifecycle of an IM bridge Engine.
type IMController struct {
	mu           sync.Mutex
	engine       *Engine
	platform     string
	running      bool
	ChannelsPath string // path to ~/.animus/channels.json
	logCleanup   func() // restores slog handler and closes log file
}

// NewIMController creates a new controller with no active bridge.
// channelsPath is the path to ~/.animus/channels.json for auto-load/save.
func NewIMController(channelsPath string) *IMController {
	return &IMController{ChannelsPath: channelsPath}
}

// Start launches the IM bridge. If already running, returns an error.
//
// Priority for resolving configuration:
//  1. platform + token provided directly (dialog mode) -> use immediately
//  2. platform provided without token -> look up saved config in channels.json
//  3. configPath non-empty -> load TOML config (legacy compatibility)
//  4. no platform -> load all enabled channels from channels.json
func (c *IMController) Start(runtime Runtime, name, platform, token, allowFrom, configPath string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return fmt.Errorf("IM bridge already running (%s), stop it first", c.platform)
	}

	var cfg Config

	switch {
	case platform != "" && token != "":
		cfg = ConfigFromFlags(platform, token, allowFrom)

	case platform != "" && token == "":
		saved, err := c.lookupSavedChannel(platform)
		if err != nil {
			return err
		}
		cfg = saved

	case configPath != "":
		var err error
		cfg, err = LoadConfig(configPath)
		if err != nil {
			return fmt.Errorf("load config: %w", err)
		}

	default:
		if c.ChannelsPath == "" {
			return fmt.Errorf("platform is required (e.g. telegram, feishu, discord)")
		}
		chCfg, err := LoadChannelsJSON(c.ChannelsPath)
		if err != nil {
			return fmt.Errorf("load channels.json: %w", err)
		}
		cfg = chCfg.ToConfig()
		if len(cfg.Project.Platforms) == 0 {
			return fmt.Errorf("no enabled channels in %s", c.ChannelsPath)
		}
	}

	return c.startWithConfig(runtime, name, platform, cfg)
}

// StartWithOpts launches the IM bridge using raw platform options (from tool credentials).
func (c *IMController) StartWithOpts(runtime Runtime, name, platform string, opts map[string]any) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return fmt.Errorf("IM bridge already running (%s), stop it first", c.platform)
	}

	var cfg Config

	if platform != "" && len(opts) > 0 {
		cfg = DefaultConfig()
		cfg.Project.Platforms = []PlatformConfig{{Type: platform, Options: opts}}
	} else if platform != "" {
		var err error
		cfg, err = c.lookupSavedChannel(platform)
		if err != nil {
			return err
		}
	} else {
		if c.ChannelsPath == "" {
			return fmt.Errorf("platform is required (e.g. telegram, feishu, discord)")
		}
		chCfg, err := LoadChannelsJSON(c.ChannelsPath)
		if err != nil {
			return fmt.Errorf("load channels.json: %w", err)
		}
		cfg = chCfg.ToConfig()
		if len(cfg.Project.Platforms) == 0 {
			return fmt.Errorf("no enabled channels in %s", c.ChannelsPath)
		}
	}

	return c.startWithConfig(runtime, name, platform, cfg)
}

// startWithConfig is the shared startup logic.
func (c *IMController) startWithConfig(runtime Runtime, name, platform string, cfg Config) error {
	if name != "" {
		cfg.Project.Name = name
	}

	platforms, err := CreatePlatforms(cfg)
	if err != nil {
		return fmt.Errorf("create platforms: %w", err)
	}

	logCleanup, logErr := SetupIMLogger()
	if logErr != nil {
		log.Printf("warning: im log setup: %v", logErr)
	}

	agent := NewAgent(runtime, cfg.Project.Name)
	engine := NewEngine(agent, platforms, cfg)

	if err := engine.Start(); err != nil {
		if logCleanup != nil {
			logCleanup()
		}
		return fmt.Errorf("start engine: %w", err)
	}

	c.engine = engine
	c.logCleanup = logCleanup
	c.platform = platform
	if c.platform == "" && len(cfg.Project.Platforms) > 0 {
		c.platform = cfg.Project.Platforms[0].Type
	}
	c.running = true
	return nil
}

// lookupSavedChannel finds a platform's config from channels.json.
func (c *IMController) lookupSavedChannel(platform string) (Config, error) {
	if c.ChannelsPath == "" {
		return Config{}, fmt.Errorf("token is required for platform %q (no channels.json configured)", platform)
	}
	chCfg, err := LoadChannelsJSON(c.ChannelsPath)
	if err != nil {
		return Config{}, fmt.Errorf("load channels.json: %w", err)
	}
	opts := chCfg.LookupChannel(platform)
	if opts == nil {
		return Config{}, fmt.Errorf("no saved config for platform %q in %s; please provide a token", platform, c.ChannelsPath)
	}
	cfg := chCfg.ToConfig()
	cfg.Project.Platforms = nil
	platformOpts := make(map[string]any, len(opts))
	for k, v := range opts {
		if k != "enabled" {
			platformOpts[k] = v
		}
	}
	cfg.Project.Platforms = []PlatformConfig{{Type: platform, Options: platformOpts}}
	return cfg, nil
}

// SaveChannel persists a platform's credentials to channels.json.
func (c *IMController) SaveChannel(platform string, opts map[string]any) error {
	if c.ChannelsPath == "" {
		return nil
	}
	chCfg, err := LoadChannelsJSON(c.ChannelsPath)
	if err != nil {
		return fmt.Errorf("load channels.json for save: %w", err)
	}
	existing := chCfg.Channels[platform]
	if existing == nil {
		existing = make(map[string]any)
	}
	for k, v := range opts {
		if v != "" {
			existing[k] = v
		}
	}
	existing["enabled"] = true
	chCfg.Channels[platform] = existing
	return SaveChannelsJSON(c.ChannelsPath, chCfg)
}

// DeleteChannel removes a platform from channels.json.
func (c *IMController) DeleteChannel(platform string) error {
	if c.ChannelsPath == "" {
		return fmt.Errorf("no channels.json configured")
	}
	chCfg, err := LoadChannelsJSON(c.ChannelsPath)
	if err != nil {
		return fmt.Errorf("load channels.json: %w", err)
	}
	if _, ok := chCfg.Channels[platform]; !ok {
		return fmt.Errorf("channel %q not found", platform)
	}
	delete(chCfg.Channels, platform)
	return SaveChannelsJSON(c.ChannelsPath, chCfg)
}

// Stop shuts down the IM bridge. No-op if not running.
func (c *IMController) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.running {
		return nil
	}

	err := c.engine.Stop()
	if c.logCleanup != nil {
		c.logCleanup()
		c.logCleanup = nil
	}
	c.engine = nil
	c.running = false
	c.platform = ""
	return err
}

// Status returns a human-readable status string.
func (c *IMController) Status() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.running {
		return "IM bridge: not running"
	}
	return fmt.Sprintf("IM bridge: running (%s)", c.platform)
}

// Running reports whether the bridge is active.
func (c *IMController) Running() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.running
}
