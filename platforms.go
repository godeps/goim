package goim

// Blank imports to register all cc-connect platform backends via their init() functions.
import (
	_ "github.com/godeps/cc-connect/platform/dingtalk"
	_ "github.com/godeps/cc-connect/platform/discord"
	_ "github.com/godeps/cc-connect/platform/feishu"
	_ "github.com/godeps/cc-connect/platform/line"
	_ "github.com/godeps/cc-connect/platform/qq"
	_ "github.com/godeps/cc-connect/platform/qqbot"
	_ "github.com/godeps/cc-connect/platform/slack"
	_ "github.com/godeps/cc-connect/platform/telegram"
	_ "github.com/godeps/cc-connect/platform/wecom"
	_ "github.com/godeps/cc-connect/platform/weixin"
)
