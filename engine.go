package goim

import (
	"github.com/chenhg5/cc-connect/core"
)

// Engine wraps cc-connect's core.Engine with goim-specific configuration.
type Engine struct {
	inner *core.Engine
}

// NewEngine creates a cc-connect Engine binding the given Agent adapter and Platforms.
func NewEngine(agent *Agent, platforms []core.Platform, cfg Config) *Engine {
	lang := ResolveLang(cfg.Language)
	dataDir := ResolveDataDir(cfg)
	sessionStorePath := dataDir + "/sessions"

	engine := core.NewEngine(cfg.Project.Name, agent, platforms, sessionStorePath, lang)

	// Apply stream preview config.
	engine.SetStreamPreviewCfg(cfg.StreamPreview.ToStreamPreviewCfg())

	return &Engine{inner: engine}
}

// Start starts all platforms and begins routing messages.
func (e *Engine) Start() error {
	return e.inner.Start()
}

// Stop gracefully shuts down all platforms and the agent.
func (e *Engine) Stop() error {
	return e.inner.Stop()
}
