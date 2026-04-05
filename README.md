# goim

A platform-agnostic IM gateway library for Go. Connect any AI agent backend to 10+ chat platforms through a single `Runtime` interface.

[中文文档](#中文文档)

## Features

- **Single interface integration** — implement `Runtime` to bridge your agent to all supported platforms
- **10 IM platforms** — Telegram, Feishu (Lark), Discord, Slack, DingTalk, WeCom, QQ, QQBot, LINE, WeChat
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
| `EventMessageStop` | Message generation finished |

## License

MIT

---

<a id="中文文档"></a>

# 中文文档

goim 是一个平台无关的 Go 语言 IM 网关库。通过实现一个 `Runtime` 接口，即可将任意 AI Agent 后端接入 10+ 聊天平台。

## 特性

- **单接口集成** — 实现 `Runtime` 接口即可桥接所有支持的平台
- **10 个 IM 平台** — Telegram、飞书、Discord、Slack、钉钉、企业微信、QQ、QQ 机器人、LINE、微信
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

## 流式事件类型

| 常量 | 说明 |
|------|------|
| `EventContentBlockDelta` | 增量文本输出 |
| `EventToolExecutionStart` | 工具调用开始 |
| `EventToolExecutionResult` | 工具调用完成 |
| `EventToolExecutionOutput` | 工具中间输出 |
| `EventError` | 发生错误 |
| `EventMessageStop` | 消息生成完毕 |

## 许可证

MIT
