# goim remote

The `github.com/godeps/goim/remote` package connects a user's local agent
installation to an authenticated application. Its production code imports only
the Go standard library. Use Go 1.27.4 or later and pin the root
`github.com/godeps/goim` module when importing this package.

The application owns account authentication, resource authorization and its UI.
The local client owns workspace authorization, native process execution and a
durable execution journal. The sibling `github.com/godeps/goim/runner` package
adapts native agent SDKs; this package does not ship a separate executable client.
Lanner can use this protocol against Peek or another compatible host.

## Host integration

Create the storage table through the host's explicit migration system. The
constructor does not migrate or close a borrowed database:

```go
store, err := remote.NewSQLStore(sqlDB, "postgres", "app_remote_state")
if err != nil {
    return err
}
// Run store.SchemaSQL() through an explicit application migration.

service, err := remote.NewService(store, remote.Options{
    Namespace: "my-app",
    ValidatePrincipal: func(ctx context.Context, p remote.Principal) error {
        // Recheck disabled/deleted accounts for EVERY human and device operation.
        return accounts.RequireActive(ctx, p.ID)
    },
    AuthorizeRun: func(ctx context.Context, p remote.Principal, req remote.CreateRunRequest) error {
        // Optional host resource policy. Must be safe to call again on a CAS retry.
        return resources.AllowAgentRun(ctx, p.ID, req.DeviceID, req.Workspace)
    },
})
if err != nil {
    return err
}

handler := remote.NewHandler(service, remote.HTTPOptions{
    Authenticate: func(r *http.Request) (remote.Principal, error) {
        // Verify the existing application session. On mutations also enforce
        // the application's CSRF policy. Never trust a supplied user ID.
        account, err := sessions.AuthenticateWithWriteCSRF(r)
        if err != nil {
            return remote.Principal{}, err
        }
        return remote.Principal{ID: account.ID}, nil
    },
})
mux.Handle("/api/agents/v1/", http.StripPrefix("/api/agents/v1", handler))
```

The `accounts`, `resources` and `sessions` objects above belong to the host.
Pairing does not grant access to the host's documents, credentials or projects.
A workspace declaration contains an alias and display name; the actual local
path and its authorization remain on the local client.

All state is isolated by `Namespace` and the authenticated principal. Two
applications sharing one Store must choose distinct namespaces. Device routes
use their own bearer credentials and recheck `ValidatePrincipal`; a browser
cookie or user header cannot impersonate a device. Human routes do not accept a
device credential as account authentication.

## Pairing and persistent device identity

1. An authenticated human creates a short-lived pairing invitation.
2. The local client calls `NewEnrollment(code, name, agents, workspaces)`.
3. Set `EnrollmentRequest.ExpectedPrincipal` to the exact account shown on the
   approved pairing screen. Save the complete request and returned credentials
   privately, with a durable write, **before** making the enrollment request.
4. Construct `NewClient(fullBaseURL, credentials, ClientOptions{})`, then call
   `Enroll` with that same saved request. Verify the returned principal/device.
5. If the response is lost, retry the identical request. It returns the original
   identity, including after the invitation expires. A changed credential or
   manifest cannot consume an already used invitation.
6. Once enrollment is acknowledged locally, the one-use invitation secret can be
   removed from the client's journal.

The server stores hashes of invitation and device secrets, not the raw secrets.
The device token is only sent in the Authorization header. Device credentials
remain valid until explicitly revoked or the host account is unavailable.
Revocation prevents subsequent polls, heartbeats and uploads. It cancels queued
work; a potentially active offline process is marked `unknown`, not reported as
stopped. Revocation cannot by itself kill an unreachable local process.

A device manifest is immutable for this enrollment. Adding a different agent or
workspace requires a new explicitly approved enrollment in this version.

## HTTP surface

Mount the handler below a fixed versioned prefix such as `/api/agents/v1`.
The client base URL includes that complete prefix. The Go client requires HTTPS,
disables redirects and cookie jars, and allows HTTP only when explicitly enabled
for loopback integration tests.

| Method and relative path | Authentication | Purpose |
| --- | --- | --- |
| `GET /meta` | Public | Protocol version and supported features |
| `GET /policy` | Human | Retention, capacity limits and indexed usage |
| `POST /pairings` | Human | Create a single-device invitation |
| `POST /devices/enroll` | Invitation | Consume or replay an exact enrollment |
| `GET /devices` | Human | List owned devices |
| `DELETE /devices/{id}` | Human | Revoke an owned device |
| `POST /runs` | Human | Create an idempotent chat turn |
| `GET /runs` | Human | List retained owned runs |
| `GET /runs/{id}` | Human | Read current execution facts |
| `GET /runs/{id}/events?after=N` | Human | Read up to 256 original events |
| `POST /runs/{id}/cancel` | Human | Request cancellation; send JSON `{}` |
| `POST /runs/{id}/requests/{request}/answer` | Human | Answer an exact approval |
| `GET /device` | Device | Revalidate enrollment and account status |
| `POST /device/poll` | Device | Claim or recover one assignment |
| `POST /device/runs/{id}/heartbeat` | Device | Renew a live lease and obtain controls |
| `POST /device/runs/{id}/events` | Device | Commit an original event prefix |

Success responses are direct JSON structs; device/run lists are arrays.
The response header `X-Goim-Protocol: goim-remote-1` identifies this protocol.
Errors have the shape `{"error":{"code":"sequence_gap"}}` and an appropriate HTTP
status. SQL, filesystem and credential details are not returned in HTTP errors.
JSON bodies require `application/json`; unknown fields, duplicate object keys,
trailing JSON values, excessive nesting and oversized requests are rejected.

The host should supply its normal TLS, request timeout, per-account rate limit
and request-log redaction configuration. No global server or listener is started
by this library.

## Runs, claims and native sessions

Use `NewRunKey()` to create an idempotency key. Persist it with the unchanged
request before POSTing. The wire format is
`rk1.<UTC Unix seconds>.<32 lowercase hexadecimal nonce>`.
A browser can use the current Unix second and a UUID with hyphens removed.
Keys more than five minutes in the future are rejected.

The same retained key and unchanged request return the same run; a different
prompt, target or session with that key returns `idempotency_conflict`.
This is separate from native agent resumption:

- Empty `CreateRunRequest.SessionID` explicitly starts a new logical session.
- A nonempty value must name an existing session for the same device, agent and
  workspace. Missing sessions return `410 session_expired`; they are not silently
  recreated with a fresh native context.
- `Run.SessionID` is the logical conversation. `Run.NativeSessionID` is the native
  provider thread/session ID. A local executor can report the latter through
  `Event.SessionID`.
- A session has at most one outstanding run. A device executes at most one
  claimed run at a time. Further turns on other sessions can queue.
- The local client independently verifies the native ID against its own
  service/account/workspace/session mapping before resuming an agent.

Use `NewPollID()` for a claim ID:
`pc1.<UTC Unix seconds>.<32 lowercase hexadecimal nonce>`.
Save it before polling and reuse it when a response is lost. A successful claim
retains its ID with the run. Repeating that ID retrieves the same assignment even
after intervening polls or completion. Empty polls do not allocate a claim.

`Assignment.New` means that this poll originally claimed the run. It is **not**
permission to replay a local process after an uncertain start. A client must
durably record the assignment and then the execution intent before spawning.

Claimed/running leases default to 45 seconds. Heartbeat renews a live lease.
Once the lease expires, the run becomes `unknown` and requests cancellation.
It remains associated with the original device, attempt and fence. Heartbeat
cannot revive it; another poll cannot requeue or replace it.

## Events, approvals and stopping

Event sequence starts at 1 for each run. Each upload binds `AttemptID`, `Fence`
and up to 64 consecutive original events. Successful Receipt.AckSeq confirms
durable storage of that prefix. It does not imply agent success.

The server accepts identical retries, rejects changed events at an existing
sequence, rejects gaps, and commits each batch atomically. Persisted event data
is returned in sequence order. The event digest uses its canonical JSON encoding;
text and tool arguments are not synthesized from SDK channel closure.

Record `started` when the native execution begins. Explicit `completed`,
`failed` or `cancelled` events record known terminal outcomes. A pre-launch
authorization failure or cancellation may report failed/cancelled without a
fictional started event. An uncertain process reports `unknown`.

Previously generated started/output/approval events can arrive after lease loss.
They are historical facts and may be acknowledged without reviving the run.
An exact known-stopped terminal report can later reconcile the original attempt.
There is no automatic process retry on stream EOF, disconnect, expired lease or
unknown state.

An approval event includes a unique RequestID, tool name and input. The service
returns an immutable digest and attempt binding. An answer must echo that exact
AttemptID and Digest and choose `allow` or `deny`. Same-answer retries are
idempotent; contradictory answers conflict. Cancellation, completion and unknown
state reject new decisions. A previously answered decision remains historical
data; the client must handle cancellation before applying approval updates.

A cancellation request and a confirmed stop are different facts. A queued run
can be cancelled immediately. A claimed process stays active until the client
actually stops/reaps it and reports a terminal outcome. SDK output completion
must not be confused with a process whose termination is still uncertain.

The client should run control/heartbeat polling independently of event upload.
A stalled outbox must not prevent cancellation, approval delivery or enforcing
the last confirmed lease deadline.

## Frozen budgets and bounded retention

Each Run.Limits is fixed when the run is created and is included in assignments.
Changing service configuration does not change that run's accepted event budget.

| Default | Meaning |
| --- | --- |
| 32 devices | Registered identities per principal |
| 256 retained runs | Active, unknown and recent terminal runs |
| 256 logical sessions | Active and retained idle conversations |
| 32 MiB state | Maximum encoded principal snapshot, including reserved space |
| 16 KiB prompt | Maximum original prompt |
| 2,048 ordinary events/run | Does not count reserved outcome reports |
| 128 KiB ordinary event data/run | Sum of encoded original event sizes |
| 16 KiB text / 16 KiB input | Maximum fields in an ordinary event |
| 24 KiB single ordinary event | Maximum encoded size |
| 2 KiB single outcome report | Maximum encoded unknown/terminal report |
| 2 reserved outcome slots | At most one unknown and one known-stopped outcome |

The local client must use the smaller of its own limits and Run.Limits, measured
before assigning a sequence and persisting a new event. It should stop the native
worker before exhausting its ordinary stream budget. Already persisted,
unacknowledged events must never be rewritten to make a later event fit.
An unknown report has its own reserved slot so a later known-stopped result can
still reconcile the same attempt after the ordinary stream is full. The service
also reserves 8 KiB of snapshot capacity for each nonterminal run.

`Options.Limits` can change device/run/session/state and ordinary stream limits.
`Options.RetentionDuration` defaults to 24 hours; `SessionRetention` defaults to
seven days and must be at least the run retention window.

Terminal run history and events are removed only after both its request key and
its actual terminal update are older than the retention window. Active, queued,
cancel-requested and unknown runs are never removed by TTL. Idle logical sessions
retain their native mapping until SessionRetention expires. Maintenance happens
during ordinary successful service operations; a host can call Policy for owned
accounts as part of its maintenance schedule.

After retained history is removed, an expired request key returns
`410 request_expired` and an expired claim ID returns `410 claim_expired`.
They cannot create a new run/claim. Exact retained active claims and intents
remain recoverable even when their key is old.

Within the configured window, a full account returns a specific capacity error
instead of discarding original data. Operators can configure an appropriate
window/limit; hosts that need permanent transcripts should copy acknowledged
events to their own archival storage before protocol retention expires. Do not
reduce the maximum state size below already retained snapshots. A restore to an
older database backup is not a safe execution reset: fence old credentials and
reconcile potentially running clients before admitting replacement work.

## Durable storage contract

`Store` is deliberately small:

```go
type Store interface {
    Load(context.Context, string) (Snapshot, error)
    CompareAndSwap(context.Context, string, uint64, []byte) (bool, error)
}
```

Missing keys have Version 0. A compare-and-swap must be one atomic, durable
commit across all writers. A successful commit survives process restart.
Returned bytes must not alias mutable storage. An error may represent an
uncertain commit; callers recover using the same enrollment/claim/event key.

SQLStore uses one bounded principal snapshot per row and a version predicate.
A conditional INSERT handles the first writer; UPDATE requires the previously
read version. This works across service instances without an in-process lock.
An application transaction must not overwrite these rows outside the same CAS
contract. The table is:

```sql
CREATE TABLE IF NOT EXISTS peek_remote_state (
    "key" TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload TEXT NOT NULL
);
```

Choose SQLite or PostgreSQL. Supply the corresponding database/sql driver in the
host. Configure SQLite WAL, a busy timeout and durable synchronous writes for
every connection, or PostgreSQL's required durable commit policy. The store does
not override a shared application's database settings. It never closes the
borrowed *sql.DB. GORM hosts can obtain the underlying *sql.DB once and inject it.

Callbacks can execute again on CAS conflicts. Account/resource authorization
callbacks must be read-only and retry-safe; IDs and secrets are generated outside
mutation retries. No native process, outbound message or other external effect
is executed inside a CAS callback.

FileStore supports a private, local directory on Linux, macOS and the listed BSD
platforms. It uses an OS lock across processes, exact version comparison, a
0600 temporary file, file fsync, atomic rename and directory fsync. Load verifies
the envelope key, version and SHA-256 digest, preserving exact stored bytes.
It fails closed on corruption, unsafe permissions or invalid files instead of
resetting the state. It is not a shared/network-filesystem or multi-host backend.
Other OSes return an explicit unsupported-locking error; use a platform-backed
durable Store there.

The principal snapshot implementation favors a small auditable protocol core
and bounded deployments. It rewrites the bounded snapshot for mutations.
Applications requiring very large transcripts or high per-account write rates
should use host archival storage and measure this backend's serialization cost.

## Verification

From the repository root:

```sh
go test -race ./...
go vet ./...
```

Tests exercise TLS HTTP, lost enrollment/claim/event acknowledgements, service
restart, immutable event replay, account isolation, host disablement, revocation,
exact approvals, cancellation versus stop confirmation, unknown recovery,
safe retention, capacity reserves, strict JSON, redirect refusal and concurrent
FileStore writers in separate operating-system processes.

The `remote/integration` test package imports real SQL drivers. The root module
records their versions, but importing `remote` does not link them into a host:

```sh
go test -race ./remote/integration  # SQLite always runs; PostgreSQL requires a test DSN.
python3 remote/integration/run_disposable_postgres.py
```

The Python command uses an already available postgres:16 Docker image, creates a
new loopback-only database with a random password and tmpfs, runs both database
tests, and removes only that container. It does not attach an existing database
or business volume. Alternatively set GOIM_REMOTE_TEST_POSTGRES_DSN explicitly
to a disposable test database. Tests cover cross-connection CAS contention,
concurrent idempotent runs, one successful claim, durable ACK after reconnection,
principal isolation and immediate cross-instance revocation.

These are protocol and database tests. Native Codex/Claude SDK process behavior
is tested by the runner package and the embedding client.
