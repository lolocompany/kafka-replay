## kafka-replay CLI v2 Implementation Plan

This document is a **step‑by‑step guide** to evolve the current `kafka-replay` CLI into the v2 design, with **breaking changes allowed** and **no v1 compatibility mode**. It assumes the existing codebase as of early 2026 (urfave/cli v3, current `list` and `cat` commands, etc.).

The steps are ordered to minimize churn and keep the code in a working state between stages. A human or agent can follow each step sequentially.

---

## 0. Ground Rules & Invariants

1. **No v1 compatibility mode**
   - There will be **no** `--compat-v1` flag or environment variable. The code should always target the new behavior.
   - Any shims for old commands/flags should be minimal and short‑lived; prefer hard breaks with clear release notes.

2. **Global goals**
   - **Consistent field names** in JSON/JSONL output (`groupId`, `partition`, `replicas`, etc.).
   - **Consistent flag names** across commands (`--brokers`, `--output`, etc.).
   - **Global flags** work for all commands/subcommands where applicable (e.g. `--brokers`, `--output`, `--profile`).
   - **Clear stdout/stderr separation**: stdout is for machine‑readable or user‑visible results; stderr for logs/progress.

3. **Incremental verification**
   - After each major step, run existing tests (if any) and add/adjust tests to lock in the new behavior.
   - Prefer adding small integration tests that run the built binary with specific arguments and assert on output/exit code.

4. **Code organization**
   - Code that is **explicitly related to the CLI** (urfave/cli wiring, command definitions, flag parsing, main entrypoint) must live under the `cmd` directory.
   - Code that is **not explicitly CLI‑specific** (business logic, Kafka interactions, data models, helpers) must live under the `pkg` directory.
   - Subdirectories under `cmd` and `pkg` are allowed, and the **current directory structure should be followed and extended**, not replaced, wherever reasonable.

---

## 1. Introduce Centralized Configuration & Connection Options

**Goal:** Move from ad‑hoc `--broker` usage to a shared configuration model and global flags for brokers/profile/config.

### 1.1 Create a config package

1. Add a new package, e.g. `pkg/config`:
   - Types:
     - `type Profile struct { Brokers []string; /* future SASL/TLS fields */ }`
     - `type Config struct { Profiles map[string]Profile; DefaultProfile string }`
   - Functions:
     - `LoadConfig(path string) (Config, error)`
     - `ResolveProfile(cfg Config, profileName string) (Profile, error)`
2. Decide on config path resolution:
   - Default: `~/.kafka-replay/config.yaml`.
   - Overridable via a global `--config` flag (to be added later) and optionally `KAFKA_REPLAY_CONFIG`.

### 1.2 Implement broker resolution logic

1. Add a small resolver function (in a new package or in `pkg/config`):
   - Signature example:
     - `func ResolveBrokers(globalBrokers []string, profileName string, cfg Config, envBrokers []string) ([]string, error)`
   - Resolution precedence (hard rule for v2):
     1. **Global `--brokers` flag** (explicit).
     2. **Profile** from `--profile` or `default_profile` in config.
     3. **Environment variable fallback** (e.g. `KAFKA_BROKERS`) — optional and clearly documented as lowest priority.
   - If, after resolution, the result is empty, return an error like:  
     `"no brokers configured; set --brokers, a profile, or KAFKA_BROKERS"`.

2. Ensure the resolver does **not** silently read brokers directly inside individual commands; everything must go through this shared logic.

### 1.3 Wire global config resolution into the app initialization

1. Locate the main app initialization (likely in `cmd/kafka-replay/main.go` or similar):
   - Identify where `cli.App` is created and where root commands are registered.
2. Introduce global options in a structured context:
   - Create a struct like:
     - `type AppContext struct { Config config.Config; Profile string; Brokers []string; OutputFormat string; /* future flags */ }`
   - Initialize this context once, in `main`.
3. Add **global flags** (to be wired later) to the root `cli.App`:
   - `--config string` (path).
   - `--profile string`.
   - `--brokers string` (to be parsed as CSV or with repeated usage).
   - `--output, -o string` (e.g. `jsonl`, `json`, `table`, `text`).
4. For now, just parse and store these values into `AppContext` without changing existing commands yet.

**Checkpoint:**  
Binary builds and runs with new global flags present (even if they are not yet used by commands).

---

## 2. Normalize Broker Handling Across Commands

**Goal:** Ensure every Kafka‑talking command uses the same broker resolution logic and flag naming.

### 2.1 Replace per-command `--broker` flags with global `--brokers`

Commands currently defining:

- `list_brokers.go`
- `list_partitions.go`
- `list_consumer_groups.go`
- Any other commands with broker flags (e.g. `record.go`).

For each such command:

1. **Remove** the `cli.StringSliceFlag` or similar for `broker` from the command’s `Flags` list.
2. Adjust the command `Action` signature to accept and use the shared `AppContext`:
   - Option A: Store a pointer to `AppContext` in `cli.App.Metadata` and read it from each command.
   - Option B: Use closures when constructing commands (e.g. `func ListCommand(ctx *AppContext) *cli.Command`), capturing `*AppContext`.
3. Inside each `Action`, call the shared broker resolver with:
   - Global `--brokers` value from `AppContext`.
   - `--profile` / `default_profile`.
   - Config object.
   - Environment brokers if you decide to keep them.
4. Remove local `len(brokers) == 0` checks and error messages; replace with a single shared error from the resolver.

**Checkpoint:**  
`list brokers`, `list partitions`, `list consumer-groups`, `record`, and any other broker‑using commands all obtain brokers only via the common resolver, with `--brokers` as the canonical flag name.

---

## 3. Introduce and Standardize Output Format Handling

**Goal:** Ensure all commands honor a consistent `--output` flag and keep stdout clean for machine output.

### 3.1 Define output formats and helpers

1. Create a package (e.g. `pkg/output`) with:
   - Type:
     - `type Format string` (e.g. `FormatJSONL`, `FormatJSON`, `FormatTable`, `FormatText`).
   - Parser:
     - `func ParseFormat(s string, isTTY bool) (Format, error)`  
       - If `s` is empty:
         - If `isTTY`: default to `FormatTable`.
         - Else: default to `FormatJSONL`.
   - Helpers:
     - `func NewEncoder(format Format, w io.Writer) Encoder` with methods like:
       - `EncodeAny(v any) error` (for JSON/JSONL).
       - `EncodeSlice[T any](items []T) error` (for lists).
       - `EncodeTable(headers []string, rows [][]string) error` (table for human output).

2. Add an `OutputFormat` field to `AppContext` and populate it in `main`:
   - Use `ParseFormat` on `--output`.
   - Use `os.Stdout` TTY detection if available (or keep it simple and always default to `jsonl` for now if you prefer).

### 3.2 Update existing list commands to use output helpers

For each of:

- `list_brokers.go`
- `list_partitions.go`
- `list_consumer_groups.go`

1. Replace manual `json.NewEncoder(os.Stdout)` with the shared `output.Encoder`:
   - For JSON/JSONL formats, `EncodeSlice` should behave similarly to existing one‑object‑per‑line output.
   - For table format, define human‑friendly headers and stringified rows.
2. Ensure **all** user‑facing data goes to **stdout**, while incidental info (debug, progress) goes to **stderr** using `fmt.Fprintln(os.Stderr, ...)`.

3. Confirm behavior:
   - `--output=jsonl` → one JSON object per line.
   - `--output=json` → a JSON array.
   - `--output=table` (optional for now) → pretty table.

**Checkpoint:**  
All `list` commands honor `--output`, and no longer write log messages to stdout.

---

## 4. Standardize JSON Field Names and Schemas

**Goal:** Make JSON/JSONL output schemas consistent and well‑named, with clear contracts.

### 4.1 Partition output (`pkg/list_partitions.go`)

Current struct (simplified):

- `Topic string  json:"topic"`
- `Partition int json:"partitions"`
- `Leader string json:"leader"`
- `ReplicatedOnBrokers []string json:"replicatedOnBrokers,omitempty"`
- `Earliest *int64 json:"earliest,omitempty"`
- `Latest *int64 json:"latest,omitempty"`
- `Replicas []string json:"replicas,omitempty"`
- `InSyncReplicas []string json:"inSyncReplicas,omitempty"`

Changes:

1. Rename the struct fields/tags to:
   - `Topic string  json:"topic"`
   - `Partition int json:"partition"`
   - `Leader string json:"leader"`
   - `Followers []string json:"followers,omitempty"` (formerly `ReplicatedOnBrokers`).
   - `EarliestOffset *int64 json:"earliestOffset,omitempty"`
   - `LatestOffset *int64 json:"latestOffset,omitempty"`
   - `Replicas []string json:"replicas,omitempty"`
   - `InSyncReplicas []string json:"inSyncReplicas,omitempty"`
2. Update the code that sets these fields accordingly.
3. Update any tests and any internal consumers that reference these fields.

### 4.2 Broker output (`pkg/list_brokers.go`)

Current struct:

- `Address string json:"address"`
- `Reachable bool json:"reachable"`

Changes:

1. Add broker ID (if available) and optional rack:
   - `ID int json:"id"`
   - `Rack string json:"rack,omitempty"`
2. Fill in `ID` and `Rack` based on `kafka.Broker` information if the underlying API supports it.

### 4.3 Consumer group output (`pkg/list_consumer_groups.go`)

Current structs (simplified):

- `ConsumerGroupOutput` with fields (`Group`, `State`, `ProtocolType`, `Members`, `Offsets`).
- `ConsumerGroupMember` with fields (`MemberID`, `ClientID`, `ClientHost`, `AssignedPartitions`).
- `ConsumerGroupOffset` with fields (`Topic`, `Partition`, `Offset`, `Metadata`).

Changes:

1. Rename fields and tags:
   - `Group string json:"group"` → `GroupID string json:"groupId"`.
   - `State string json:"state,omitempty"` (keep).
   - `ProtocolType string json:"protocolType,omitempty"` (keep).
   - `Members []ConsumerGroupMember json:"members,omitempty"` (keep).
   - `Offsets []ConsumerGroupOffset json:"offsets,omitempty"` (keep).
2. In `ConsumerGroupMember`:
   - Keep `MemberID`, `ClientID`, `ClientHost`, but update tags to lowerCamelCase where needed:
     - `MemberID string json:"memberId"`.
     - `ClientID string json:"clientId"`.
     - `ClientHost string json:"clientHost"`.
     - `AssignedPartitions map[string][]int json:"assignedPartitions,omitempty"`.
3. In `ConsumerGroupOffset`:
   - Ensure tags are:
     - `Topic string json:"topic"`.
     - `Partition int json:"partition"`.
     - `Offset int64 json:"offset"`.
     - `Metadata string json:"metadata"`.
4. Update the construction logic in `ListConsumerGroups` to set the renamed fields (`GroupID` etc.).

**Checkpoint:**  
All JSON outputs for brokers, partitions, and consumer groups use the new standardized field names and casing.

---

## 5. Clarify and Standardize `cat` Behavior

**Goal:** Make `cat` output predictable for both humans and scripts, with clear output modes and no stray stdout messages.

### 5.1 Revisit `cat` flags and semantics

Current flags (from `cmd/kafka-replay/commands/cat.go`):

- `--input, -i` (required).
- `--raw, -r`.
- `--find, -f`.
- `--count`.

Changes:

1. Introduce `--output, -o` for `cat` as well, but scoped to file output formats:
   - Allowed values: `jsonl` (default), `raw`, `text`.
   - Deprecate `--raw` in favor of `--output=raw` (optional: remove `--raw` immediately, since v2 is allowed to break).
2. Map behavior:
   - `jsonl`: use existing `jsonFormatter`.
   - `raw`: use `rawFormatter`.
   - `text`: possibly a human‑friendly format (timestamp + key + data in a readable line).
3. Ensure:
   - All message or count output is on **stdout**.
   - Any informational messages such as `"Reading messages from: ..."` go to **stderr** or are removed entirely.

### 5.2 Tighten the `--find` semantics

1. Decide and document:
   - Is `--find` a literal substring in the decoded bytes?  
     - If yes, implement it as a simple `bytes.Contains`.
   - Is it case‑sensitive? (default: **yes**; document).
2. Consider adding `--find-regex` if regex is needed; that can be a non‑breaking enhancement.
3. Ensure that when `--count` is set:
   - Only the count (a single integer followed by newline) is printed to stdout.
   - No additional prefixes or human messages are emitted.

**Checkpoint:**  
Running `kafka-replay cat --input file --output jsonl` yields pure JSONL on stdout, with any progress logs either removed or on stderr.

---

## 6. Define and Enforce Exit Code Semantics

**Goal:** Provide predictable exit codes across commands for integration in scripts and automation.

### 6.1 Choose exit code mapping

Recommended mapping:

- `0`: Success.
- `1`: Usage/config error (invalid flags, missing required arguments, invalid config file).
- `2`: “Not found” / “no results” (e.g. requested resource absent when that is considered an error).
- `3`: Network/auth/Kafka connectivity error.
- `4+`: Reserved for future categories if needed.

### 6.2 Implement centralized error classification

1. Create a helper function in a small package (e.g. `pkg/clierror`):
   - `func ExitCodeForError(err error) int`.
2. Classify errors based on:
   - Sentinel error values (e.g. `var ErrNoResults = errors.New("no results")`).
   - Wrapping types (e.g. `type NotFoundError struct { ... }`).
   - Message inspection as a last resort, only if necessary.

3. In `main`, after running the app:
   - Capture the returned error, map it via `ExitCodeForError`, and call `os.Exit(code)`.
4. Update commands to return classification‑friendly errors (e.g. wrap with `fmt.Errorf("...: %w", clierror.ErrNoResults)` where appropriate).

**Checkpoint:**  
Each command’s failure path yields a consistent exit code based on error type, not arbitrary integer values.

---

## 7. Command Layout and Globals Wiring

**Goal:** Align the CLI tree with the new design and ensure global flags apply uniformly.

### 7.1 Restructure the command tree

1. In the main commands package (e.g. `cmd/kafka-replay/commands`):
   - Keep / introduce the following top‑level commands:
     - `list` (with subcommands).
     - `record`.
     - `replay` (if not already present, create it according to existing functionality or as a stub).
     - `cat`.
     - `config`.
     - `inspect`.
     - `debug`.
2. For `list`:
   - Ensure it has subcommands:
     - `brokers`.
     - `topics`.
     - `partitions`.
     - `consumer-groups`.
   - Preserve `ls` alias if you still find it useful, otherwise drop it to avoid confusion.

3. For `inspect`:
   - Create initial subcommands:
     - `inspect topic`.
     - `inspect consumer-group`.
   - These can internally reuse the same logic/structs as `list` but for a single resource.

4. For `config`:
   - Implement at least:
     - `config list` (list profiles from config).
     - `config show --profile NAME` (print resolved profile).

5. For `debug`:
   - Move any unstable/internal functionality here to clearly mark it as non‑stable.

### 7.2 Attach global flags at the root app

1. At root `cli.App`:
   - Add global flags:
     - `--config`.
     - `--profile`.
     - `--brokers`.
     - `--output`.
     - `--verbose` / `--quiet`.
     - `--no-color`.
2. Make sure that:
   - These flags are **not** redefined on subcommands.
   - Subcommands read the resolved values only via `AppContext` or a similar shared context object, **not** by re‑parsing flags locally.

**Checkpoint:**  
Running `kafka-replay --brokers ... list partitions` works, with all subcommands correctly inheriting `--brokers` and `--output` from the root.

---

## 8. Documentation and Release Prep

**Goal:** Clearly communicate breaking changes and provide migration guidance.

### 8.1 Update README

1. Add a **“Breaking Changes in v2”** section:
   - Summarize:
     - New global flags.
     - Renamed/removed flags (e.g. `--broker` → `--brokers`).
     - JSON schema changes for `list` commands.
     - `cat` output behavior changes.
   - Provide before/after snippets for common use cases:
     - Example: old `jq` filters vs new field names.

### 8.2 Add `UPGRADING-v2.md`

1. Create a document that:
   - Lists v1 commands/flags on the left and v2 equivalents on the right.
   - Includes explicit examples:
     - “v1: `kafka-replay list partitions --broker host:9092`”  
       “v2: `kafka-replay --brokers host:9092 list partitions`”
     - Old vs new JSON key names in sample outputs.

2. Note explicitly that there is **no compatibility mode** and that scripts must be updated.

---

## 9. Testing and Hardening

**Goal:** Ensure the redesign is reliable and stable before tagging the new major version.

### 9.1 Add integration tests

1. If not present, add a simple integration test harness:
   - Build the binary in a temporary directory.
   - Invoke it with various argument combinations.
   - Assert on stdout/stderr/exit code.
2. Cover at least:
   - `list brokers`, `list partitions`, `list consumer-groups` with `--output=jsonl` and `--output=json`.
   - `cat` with each `--output` mode and with `--count`.
   - Error conditions: missing `--input` for `cat`, missing brokers, invalid profile.

### 9.2 Final review of behavior contracts

1. Confirm:
   - All commands respect the same broker resolution rules.
   - All list/inspect commands honor `--output`.
   - JSON field names are consistent and match the documentation.
   - Exit codes are aligned with the classification helper.
2. Once stable, tag the release as the new major version (`v2.0.0` or equivalent).

---

## Summary

By following these steps in order—config centralization, broker normalization, output handling, schema standardization, `cat` cleanup, exit‑code unification, command tree restructuring, and documentation—you can reliably transform the existing CLI into the desired v2 design with consistent field and flag names and global flags, without carrying any v1 compatibility layer.

