# Local agent adapters

This independent module runs installed Codex app-server and Claude Code
stream-json through the godeps SDKs. It is a library without a scheduler or CLI.

Hosts authorize canonical workspaces, filter process environments, supervise
process trees, journal execution intent, persist emitted events and route exact
approval requests. Unsupported requests fail closed. Native session IDs belong
to one service/account/conversation/agent/workspace binding.

Terminal SDK events, not process exit alone, determine success. Missing terminal
events are unknown. Cancellation requires native interruption and process stop.

Published SDK feature commits with command/approval hooks are pinned in go.mod.
The host supplies both CommandHook and StopProcess. StopProcess must confirm
that the whole owned process group has stopped; a parent exit is insufficient.
Every native permission callback receives a new approval nonce, even when the
provider reuses the same item/tool ID. Native IDs remain in request metadata.
