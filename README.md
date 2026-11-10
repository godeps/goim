# goim

A platform-agnostic IM gateway library for Go. Connect any AI agent backend to 20 chat platforms through a single `Runtime` interface.

[中文文档](#中文文档)

## Features

- **Single interface integration** — implement `Runtime` to bridge your agent to all supported platforms
- **20 IM platforms** — Telegram, Feishu (Lark), Discord, Slack, DingTalk, WeCom, QQ, QQBot, LINE, WeChat, Google Chat, Matrix, MAX, Tuitui, Webex, Weibo, WPS AgentSpace, WPS Xiezuo, Yuanbao, Cloud Web
- **Streaming support** — real-time text, tool use, and error events via channel-based API
- **Multi-channel config** — manage multiple platform credentials in a single `channels.json`
- **Session management** — per-user sessions with concurrent message handling
- **Rotating log** — automatic log rotation with size-based pruning

## Install

```bash
go get github.com/godeps/goim@latest
```

## Quick Start

### 1. Implement the Runtime interface

```go
package main

import (
    "context"
    "github.com/godeps/goim"
)

type MyRuntime struct{}

func (r *MyRuntime) RunStream(ctx context.Context, req goim.Request) (<-chan goim.StreamEvent, error) {
    ch := make(chan goim.StreamEvent, 16)
    go func() {
        defer close(ch)
        // Process req.Prompt, stream back events
        ch <- goim.StreamEvent{
            Type:  goim.EventContentBlockDelta,
            Delta: &goim.Delta{Text: "Hello from my agent!"},
        }
        ch <- goim.StreamEvent{Type: goim.EventMessageStop}
    }()
    return ch, nil
}
```

### 2. Start the gateway

```go
func main() {
    rt := &MyRuntime{}
    cfg := goim.ConfigFromFlags("telegram", "YOUR_BOT_TOKEN", "")

    platforms, _ := goim.CreatePlatforms(cfg)
    agent := goim.NewAgent(rt, "mybot")
    engine := goim.NewEngine(agent, platforms, cfg)

    engine.Start()
    // Block until interrupted...
}
```

### 3. Or use the controller for lifecycle management

```go
ctrl := goim.NewIMController("/path/to/channels.json")
err := ctrl.Start(rt, "mybot", "telegram", "YOUR_BOT_TOKEN", "", "")
// ...
ctrl.Stop()
```

## Configuration

### channels.json

Manage multiple platforms in a single file:

```json
{
  "channels": {
    "telegram": {
      "token": "123456:ABC-DEF",
      "enabled": true
    },
    "feishu": {
      "app_id": "cli_xxx",
      "app_secret": "secret",
      "enabled": true
    },
    "discord": {
      "token": "discord-bot-token",
      "enabled": false
    }
  },
  "language": "zh"
}
```

### Platform credentials

| Platform | Required fields |
|----------|----------------|
| Telegram | `token` |
| Feishu   | `app_id`, `app_secret` |
| Discord  | `token` |
| Slack    | `bot_token`, `app_token` |
| DingTalk | `client_id`, `client_secret` |
| WeCom    | `corp_id`, `corp_secret`, `agent_id` |
| QQ       | `ws_url` (optional) |
| QQBot    | `app_id`, `app_secret` |
| LINE     | `channel_secret`, `channel_token` |
| WeChat   | `token` |
| Google Chat | `subscription`; optional `credentials_file` (otherwise ADC) |
| Matrix | `homeserver`, `access_token`; optional `user_id` |
| MAX | `token` |
| Tuitui | `app_id`, `app_secret` |
| Webex | `token` |
| Weibo | `app_id`, `app_secret` |
| WPS AgentSpace (`wps-agentspace`) | `app_id`, `wps_sid` |
| WPS Xiezuo (`wps-xiezuo`) | `app_id`, `app_secret` |
| Yuanbao | `bot_token` |
| Cloud Web (`cloud_web`) | `token`, `ws_url` or `base_url` (WebSocket); other transports have separate options |

### TOML config (legacy)

```toml
[project]
name = "mybot"

[[project.platforms]]
type = "telegram"
[project.platforms.options]
token = "YOUR_BOT_TOKEN"
```

## Stream Events

| Constant | Description |
|----------|-------------|
| `EventContentBlockDelta` | Incremental text output |
| `EventToolExecutionStart` | Tool call started |
| `EventToolExecutionResult` | Tool call completed |
| `EventToolExecutionOutput` | Intermediate tool output |
| `EventError` | Error occurred |
| `EventMessageStop` | Assistant segment finished; close the stream to end the turn |
| `EventPermissionRequest` | Permission request with structured questions and options |
| `EventUserInputRequest` | Non-blocking question during an active turn |
| `EventThinking` | Processing status, separate from the final answer |

## Upstream source and migration

The gateway uses [cc-connect upstream commit `619f884c437c`](https://github.com/chenhg5/cc-connect/commit/619f884c437c3d44d0235c445c95a867073631eb) (2026-11-08), pinned in `go.mod` as `v1.5.2-beta.1.0.20261108144741-619f884c437c`. The former `godeps/cc-connect` fork's v1.7.1 is an older source snapshot despite its higher version number. Source integration uses a reproducible Go module dependency.

Hosts that directly import `github.com/godeps/cc-connect/core` must switch to `github.com/chenhg5/cc-connect/core`, including custom platforms and `PermissionResolver` implementations. Prefer `goim.PermissionResult` for permission callbacks:

```go
func (r *MyRuntime) ResolvePermission(reqID string, result goim.PermissionResult) error {
    // Deliver result to the pending request identified by reqID.
    return nil
}
```

`Runtime.RunStream`, configuration formats, and attachment APIs remain available. The upstream Slack adapter requires `slack-go v0.16.0`; hosts selecting a newer incompatible Slack SDK must resolve that dependency conflict. Registering Google Chat and Matrix also adds their SDK dependencies.

Emit `EventUserInputRequest` with a nonempty `RequestID` and `Questions` to ask while a turn continues. A runtime can optionally implement `goim.UserInputSteerer` (`SteerUserInput(sessionID, prompt string) (bool, error)`) to receive answers during the active turn; return false to route the answer as a normal message. Permission questions use `EventPermissionRequest` and `PermissionResolver` instead.

These integrations expose upstream core and platform functionality. Features requiring cc-connect's standalone CLI or its built-in agent backends need their own host integration.

## License

MIT

---

<a id="中文文档"></a>

# 中文文档

goim 是一个平台无关的 Go 语言 IM 网关库。通过实现一个 `Runtime` 接口，即可将任意 AI Agent 后端接入 20 个聊天平台。

## 特性

- **单接口集成** — 实现 `Runtime` 接口即可桥接所有支持的平台
- **20 个 IM 平台** — Telegram、飞书、Discord、Slack、钉钉、企业微信、QQ、QQ 机器人、LINE、微信、Google Chat、Matrix、MAX、推推、Webex、微博、WPS 智能体空间、WPS 协作、元宝、Cloud Web
- **流式支持** — 基于 channel 的实时文本、工具调用和错误事件流
- **多渠道配置** — 通过 `channels.json` 统一管理多平台凭据
- **会话管理** — 按用户隔离会话，支持并发消息处理
- **日志轮转** — 按大小自动轮转日志文件

## 安装

```bash
go get github.com/godeps/goim@latest
```

## 快速开始

### 1. 实现 Runtime 接口

```go
package main

import (
    "context"
    "github.com/godeps/goim"
)

type MyRuntime struct{}

func (r *MyRuntime) RunStream(ctx context.Context, req goim.Request) (<-chan goim.StreamEvent, error) {
    ch := make(chan goim.StreamEvent, 16)
    go func() {
        defer close(ch)
        // 处理 req.Prompt，流式返回事件
        ch <- goim.StreamEvent{
            Type:  goim.EventContentBlockDelta,
            Delta: &goim.Delta{Text: "你好！"},
        }
        ch <- goim.StreamEvent{Type: goim.EventMessageStop}
    }()
    return ch, nil
}
```

### 2. 启动网关

```go
func main() {
    rt := &MyRuntime{}
    cfg := goim.ConfigFromFlags("telegram", "你的机器人Token", "")

    platforms, _ := goim.CreatePlatforms(cfg)
    agent := goim.NewAgent(rt, "mybot")
    engine := goim.NewEngine(agent, platforms, cfg)

    engine.Start()
    // 阻塞等待中断信号...
}
```

### 3. 使用 Controller 管理生命周期

```go
ctrl := goim.NewIMController("/path/to/channels.json")
err := ctrl.Start(rt, "mybot", "telegram", "你的Token", "", "")
// ...
ctrl.Stop()
```

## 配置

### channels.json

通过一个文件管理多个平台：

```json
{
  "channels": {
    "telegram": {
      "token": "123456:ABC-DEF",
      "enabled": true
    },
    "feishu": {
      "app_id": "cli_xxx",
      "app_secret": "secret",
      "enabled": true
    }
  },
  "language": "zh"
}
```

### 各平台所需凭据

| 平台 | 必需字段 |
|------|---------|
| Telegram | `token` |
| 飞书     | `app_id`, `app_secret` |
| Discord  | `token` |
| Slack    | `bot_token`, `app_token` |
| 钉钉     | `client_id`, `client_secret` |
| 企业微信  | `corp_id`, `corp_secret`, `agent_id` |
| QQ       | `ws_url`（可选） |
| QQ 机器人 | `app_id`, `app_secret` |
| LINE     | `channel_secret`, `channel_token` |
| 微信     | `token` |
| Google Chat | `subscription`; optional `credentials_file` (otherwise ADC) |
| Matrix | `homeserver`, `access_token`; optional `user_id` |
| MAX | `token` |
| Tuitui | `app_id`, `app_secret` |
| Webex | `token` |
| Weibo | `app_id`, `app_secret` |
| WPS AgentSpace (`wps-agentspace`) | `app_id`, `wps_sid` |
| WPS Xiezuo (`wps-xiezuo`) | `app_id`, `app_secret` |
| Yuanbao | `bot_token` |
| Cloud Web (`cloud_web`) | `token`, `ws_url` or `base_url` (WebSocket); other transports have separate options |

## 流式事件类型

| 常量 | 说明 |
|------|------|
| `EventContentBlockDelta` | 增量文本输出 |
| `EventToolExecutionStart` | 工具调用开始 |
| `EventToolExecutionResult` | 工具调用完成 |
| `EventToolExecutionOutput` | 工具中间输出 |
| `EventError` | 发生错误 |
| `EventMessageStop` | 一个助手消息段结束；关闭流才表示整轮结束 |
| `EventPermissionRequest` | 权限请求，支持结构化问题和选项 |
| `EventUserInputRequest` | 执行过程中的非阻塞提问 |
| `EventThinking` | 处理状态，不计入最终回答 |

## 许可证

MIT

## 上游同步与迁移

本次通过 Go 模块依赖接入原始上游 `chenhg5/cc-connect` 的最新源码提交 `619f884c437c`（2026-11-08），固定版本以保证可重复构建。原来的 `godeps/cc-connect v1.7.1` 虽然版本号更高，源码快照更旧。

直接使用旧 `core` 包的宿主需要把导入路径改为 `github.com/chenhg5/cc-connect/core`；权限回调建议使用 `goim.PermissionResult`。Slack SDK 按上游要求从 v0.29.0 调整为 v0.16.0，宿主若依赖新版 SDK，需要处理版本兼容。Google Chat 和 Matrix 会增加对应 SDK 依赖。

非阻塞提问通过 `EventUserInputRequest` 传入 `RequestID` 和 `Questions`。宿主可实现 `goim.UserInputSteerer`，将回答送入指定会话的当前任务；返回 false 时由引擎按普通新消息处理。`EventThinking` 可展示处理状态，结构化问题的标题、选项和多选标记也会完整转交上游。独立 CLI 和内置 Agent 后端的功能仍需宿主另行集成。
