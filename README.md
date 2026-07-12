# rakko

<img src="assets/otter.jpg" alt="rakko — a sea otter, the project's mascot" width="480" />

**rakko** (ラッコ, Japanese for "sea otter") is a fast, keyboard-driven terminal UI for Kafka — everything you need for day-to-day cluster work without waiting on a browser tab or a bloated desktop client. Built with [ratatui](https://ratatui.rs/) and [rdkafka](https://github.com/fede1024/rust-rdkafka).

Browse topics and messages (live tail + seek), inspect consumer groups and lag, reset offsets, produce/replay messages, and export/import JSONL — all from the terminal, all scriptable, all fast.

**Why rakko over the usual Kafka GUI:**

- **No message-size ceiling on replay.** Replay and export operate on raw wire bytes, never a decode-then-re-encode round trip — resend a message byte-identical, Avro schema ID and all, no matter how large. Most GUI tools quietly cap this around 1MB; rakko doesn't.
- **Avro just works.** Auto-detects Confluent-wire-format Avro, fetches and caches the schema from your registry, and decodes it inline for browsing and filtering — without ever mutating the bytes you'd actually resend.
- **JSONL export/import is a real backup format**, not a debugging dump — base64 raw bytes are the source of truth, so reimporting is byte-identical too.
- **Kubernetes-friendly by staying out of the way** — no baked-in `kubectl`/port-forward magic to trust or debug. Point it at your tunnel like any other TLS/mTLS client.
- **Airgap-ready.** Ships as a single statically-linked binary for RHEL 9 — no runtime deps to smuggle into a locked-down environment.
- **It's a TUI.** No Electron, no browser tab, no waiting for a page to load — `j`/`k` and it's already there.

## Prerequisites

- **Rust** (stable) — [rustup](https://rustup.rs/)
- **CMake** + a C/C++ toolchain — required to build `rdkafka` (`cmake-build`, vendored OpenSSL)
  - macOS: `xcode-select --install` and `brew install cmake`
  - Linux: `gcc`, `g++`, `make`, `cmake`, `perl`

## Quick start (local stack)

```bash
# 1. Start Kafka + Schema Registry
docker compose up -d

# 2a. First run with no config — the TUI opens a create-profile form
cargo run
#    (save a profile named "local" pointing at localhost:19092)

# 2b. Or copy the example config and skip the form
mkdir -p ~/.config/rakko
cp config.example.toml ~/.config/rakko/config.toml
cargo run -- --profile local
```

| Service          | Host address        |
|------------------|---------------------|
| Kafka            | `localhost:19092`   |
| Schema Registry  | `http://localhost:18081` |

Non-default host ports (19092/18081, not 9092/8081), deliberately chosen so this
stack doesn't collide with an unrelated Kafka deployment that may already be running
on your machine.

Stop the stack with `docker compose down`.

## Configuration

Config lives at **`~/.config/rakko/config.toml`** on both macOS and Linux (not `~/Library/Application Support`).

### First run / in-app profile creation

If the config file is missing or has **no profiles**, rakko opens a **create-profile form** on startup:

- **Name**, **bootstrap servers** (default `localhost:9092`), **Auth**, optional **Schema Registry URL**
- **Tab** / **Shift-Tab** move fields · **←**/**→**/**Home**/**End** edit within a field · **Delete** forward-delete · **Space**/**t** cycles **Auth** (plaintext → TLS, system trust store → TLS, private CA → mTLS) while it's focused · **Enter** saves · **Esc** quits (when no profiles exist yet)
- Picking **TLS, private CA** or **mTLS** reveals **CA path** (and, for mTLS, **client cert path** / **client key path**) fields
- Saves to `~/.config/rakko/config.toml` (creates the directory if needed)
- Editing an existing profile (**e**) prefills its current auth mode and cert/key/CA paths, and saving changes them

On the **profile picker** (after you have at least one profile): **n** opens the same form to add another. **Esc** from the topic list returns to the picker.

You can still hand-edit the TOML anytime and restart (or re-select the profile).

Override the directory:

```bash
cargo run -- --config-dir /path/to/dir
# reads /path/to/dir/config.toml
```

### Example profile (PLAINTEXT)

```toml
[[profiles]]
name = "local"
bootstrap_servers = "localhost:19092"
tls_enabled = false
schema_registry_url = "http://localhost:18081"

[profiles.auth]
type = "none"
```

### TLS with a private CA (no client cert)

```toml
[[profiles]]
name = "internal"
bootstrap_servers = "kafka.internal:9093"
tls_enabled = true

[profiles.auth]
type = "tls"
ca_path = "/path/to/private-ca.pem"
```

### mTLS profile

```toml
[[profiles]]
name = "prod"
bootstrap_servers = "kafka.example.com:9093"
tls_enabled = true
# optional: pin client message.max.bytes (skip broker auto-detect on connect).
# If omitted, rakko reads the broker's message.max.bytes and writes it here.
# message_max_bytes = 20000000

[profiles.auth]
type = "mtls"
cert_path = "/path/to/client.pem"
key_path = "/path/to/client.key"
ca_path = "/path/to/ca.pem"
```

Optional per-profile producer knobs:

```toml
[profiles.extra_producer_config]
"compression.type" = "zstd"
```

### Schema Registry (Avro)

Set `schema_registry_url` on the profile (see example above). When browsing messages:

1. Values with the Confluent wire format (`0x00` + 4-byte schema id) are tagged `avro:<id>` (or `avro:<id>?` until the schema is cached).
2. rakko fetches `GET {url}/schemas/ids/{id}` in the background and caches the Avro schema.
3. After the cache hit, the **Value** column shows decoded JSON; filter (`/`) also searches the decoded text.
4. Failed fetches are not retried until you reconnect the profile; status line shows the error.
5. **Replay / export always use raw bytes** — lookup is display/filter only.

No `schema_registry_url` → Avro is still detected, but not decoded (hex/raw fallback).

### Advanced query filter

The plain `/` filter is a substring search. **`?`** opens a second, independent filter
that queries structured fields inside JSON/Avro keys and values:

```
key.person.name = jxhui AND key.person.age = 20 AND value.house.owner = jxhui
```

- Paths start with `key.` or `value.`, then dot-separated field names — any depth.
- `=` / `!=`, string/number/`true`/`false` literals (quote strings with spaces:
  `value.title = "hello world"`); string comparison is case-insensitive.
- Chain conditions with `AND` (only `AND` — no `OR`/parens yet).
- **Arrays**: a path through an array matches if *any* element satisfies the rest of
  the path — same implicit behavior as MongoDB's dot-notation array queries, e.g.
  `value.items.sku = "ABC123"` matches if any element of `items` has that sku, no
  index needed, at any depth (arrays of arrays included).
- Needs the same Avro schema-cache as the substring filter — a message whose schema
  hasn't loaded yet won't match a `key.`/`value.` condition until it does.
- Independent from `/`: apply both and a message must satisfy each. **c** clears
  whichever filter(s) are applied.
- A parse error is shown in the status line and keeps the query editor open so you
  can fix it.

Logs are written to **`~/.config/rakko/rakko.log`** (never to the TTY while the UI is running). Control verbosity with `RUST_LOG` (e.g. `RUST_LOG=info`).

## Running

```bash
# Debug build
cargo run

# Skip profile picker
cargo run -- --profile local

# Release binary
cargo build --release
./target/release/rakko --profile local
```

## Keybinds

Global: **`q`** quit (confirms) · **Ctrl-c** force quit · **Esc** back · **j/k** or arrows move · **Enter** confirm · **`A`** toggle banner braille-stream animation.

On first launch a **splash** shows the stream otter:
- **Truecolor terminals** (`COLORTERM=truecolor`, etc.): half-block photo art (~72 columns)
- **Otherwise**: braille silhouette (smaller ears)
- Force with `RAKKO_TRUECOLOR=1` / `0`; respects `NO_COLOR`
- Press **Enter** / **Space** / **Esc** (or any key) to continue

On any of the list-level screens (Topics, Messages, Groups, Group detail, Brokers,
Broker detail) a **switcher bar** sits under the banner: `1 Topics   2 Groups   3
Brokers`, with the active view highlighted. **1**/**2**/**3** jump straight there —
the only way to move between top-level views (no per-screen `g`/`b` shortcuts, so it
means the same thing everywhere it appears). Producer/Export-import/Create-profile
don't show it — a stray digit there would clobber an in-progress draft.

Shortcuts are kept consistent app-wide: **e** always means *edit* (profile picker,
replay's "edit in producer"), never anything else. Export uses **x**/**X** instead.

| Screen | Keys |
|--------|------|
| **Profile picker** | **Enter** connect · **n** new profile · **e** edit profile · **q** quit |
| **Create profile** | **Tab** / **Shift-Tab** fields · **←**/**→**/**Home**/**End** cursor · **Delete** · **Space**/**t** cycle Auth · **Enter** save · **Esc** cancel/quit |
| **Topics** | **Enter** open topic · **r** refresh list · **/** filter by name · **c** clear filter · **1**/**2**/**3** switch view |
| **Messages** | **Enter** view full message · **Tab**/**s** tail ↔ seek · **o** sort newest/oldest · **n**/**p** or PgDn/PgUp page · **r** refresh page (seek) · **/** filter · **?** query filter · **c** clear filter(s) · **w** produce · **y** replay · **x** export selected · **X** export all visible · **i** import · **1**/**2**/**3** switch view |
| **Message view** | **j**/**k** or arrows scroll · **PgUp**/**PgDn** page · **Enter**/**Esc** close · **y** replay · **x** export this message |
| **Groups** | **Enter** detail · **r** refresh list · **/** filter by name · **c** clear filter · **1**/**2**/**3** switch view |
| **Group detail** | **z** reset offsets · **r** refresh lag (also auto every ~3s while open) · **1**/**2**/**3** switch view |
| **Brokers** | **Enter** view broker config · **r** refresh list · **1**/**2**/**3** switch view |
| **Broker detail** | **r** refresh config · **Esc** back · **1**/**2**/**3** switch view |
| **Producer** | **Tab** focus · **F3**/Ctrl-m mode (inline / file / `$EDITOR`) · **F2**/Ctrl-p send · **Esc** back |
| **Replay** | **y**/**Enter** raw replay (byte-identical) · **e** edit in producer · **n**/**Esc** cancel |
| **Export/import** | type path · **←**/**→**/**Home**/**End** cursor · **Delete** · **Tab** (import: path ↔ topic) · **Enter** run · **Esc** back |

Offset reset only works reliably when the group has **no active members** — the UI warns if members are present.

### What updates live

| Data | Live? |
|------|--------|
| Messages in **tail** mode | Yes — continuous consumer poll |
| Messages in **seek** mode | No — load pages with **n**/**p** |
| Topic list / group list / broker list / broker config | On open, or **r** refresh |
| Group lag / members | On open, **R**, or auto ~every 3s while detail is open |

## Features (v1)

- Multi-cluster profiles (PLAINTEXT / TLS / mTLS; SASL designed for later)
- In-TUI first-run / **n** profile create (writes `config.toml`); profile picker to switch
- Topics: partitions, RF, compression, approximate message counts
- Message browse: live tail (ring buffer) + seek by page; filter on raw/decoded text
- Auto-detect + decode: Confluent Avro (magic byte → `GET /schemas/ids/{id}` when `schema_registry_url` is set, cached), JSON, raw/hex
- Consumer groups: members, per-partition lag (manual + auto refresh), destructive offset reset
- Produce: inline editor, load file, or `$EDITOR`
- Single-message replay: original raw bytes → same topic
- Export/import: JSONL with base64 raw bytes as source of truth

## Features (v2)

- Brokers screen: broker id/host/port, leader/replica partition counts (load
  distribution across the cluster), and a cluster-health line (under-replicated /
  offline partition counts) — drill in (**Enter**) for that broker's non-default
  config values (`describe_configs`, sensitive entries redacted)
- Persistent view-switcher bar: **1**/**2**/**3** jump directly between Topics/Groups/
  Brokers from any list-level screen
- Topic/group list filtering (**/**, **c**), topics sorted by name
- Advanced structured query filter (**?**) on the message browser — field-path queries
  into JSON/Avro keys/values (`key.a.b = "x" AND value.c != 5`), array fields matched
  by any-element, independent of and composable with the plain substring filter

## Architecture

Elm-style: input and background I/O both flow into a single `App::update` reducer;
rendering is a pure function of `App` state. Background Kafka/HTTP calls never run on
the render loop — see [PLAN.md](./PLAN.md) for the full design rationale.

```mermaid
flowchart LR
    subgraph Terminal
        KB[Keyboard / crossterm EventStream]
    end

    KB -->|Event::Key| K2A[key_to_action]
    K2A -->|Action| UPD[App::update]

    subgraph "Elm-style core (app/)"
        UPD -->|mutates| STATE[(App state\nScreen, ProfileConfig,\nBrowseMode, Producer, ...)]
        EVT[App::apply_event] -->|mutates| STATE
        STATE --> UPD
        STATE --> EVT
        UPD -->|dispatches into| SCREENS["per-screen handlers\napp/{producer,topic_detail,\ngroup_detail,export_import,\nprofile_create}.rs"]
        SCREENS -->|mutates| STATE
    end

    UPD -->|Command| DISP[handle_command]
    EVT -->|Command| DISP

    DISP -->|tokio::spawn / spawn_blocking| BG[Background tasks]

    subgraph "kafka/ (spawn_blocking, sync rdkafka calls)"
        ADMIN[admin.rs\ntopics]
        CONS[consumer.rs\ntail + seek]
        PROD[producer.rs]
        GRP[group_offsets.rs\nlist / lag / reset]
        SR[schema_registry.rs\nAvro schema fetch]
    end

    BG --> ADMIN & CONS & PROD & GRP & SR
    ADMIN & CONS & PROD & GRP & SR -->|AppEvent via mpsc channel| EVT

    STATE --> DRAW[ui::draw]
    DRAW --> SCREEN[Terminal frame]

    CFG[(config.toml\n~/.config/rakko)] -.load/save.-> STATE
    RAW[[RawMessage\nbyte-preserving]] -.-> CONS
    RAW -.-> PROD
    RAW -.-> EXPORT[export.rs\nJSONL]
```

Everything that talks to Kafka or the network runs inside `kafka/` on a
`tokio::task::spawn_blocking` (rdkafka's client is sync), reporting results back as an
`AppEvent` over an `mpsc` channel; the render loop's `tokio::select!` merges that
channel with terminal input and a couple of timer ticks (group-lag auto-refresh,
banner animation). `RawMessage` (`src/raw_message.rs`) is the one byte-preserving type
threaded through browsing, replay, and export/import, so replayed/exported messages
are never a decode-then-re-encode round trip.

## Airgap / RHEL 9 binary

Build a glibc-compatible **linux/amd64** binary (Rocky 9 builder) with statically vendored librdkafka + OpenSSL:

```bash
./scripts/build-tui-rhel9.sh
# optional: DOCKER=container|docker|podman
# optional: --no-cache
```

Artifacts land in `dist/`:

- `rakko-linux-amd64` / `rakko`
- `rakko-linux-amd64.tar.gz`
- `SHA256SUMS`

Requires a working container runtime with **linux/amd64** support (on Apple Silicon: Rosetta or Apple Container). First build is slow (compiles librdkafka + OpenSSL).

## Development

```bash
cargo test                     # pure-logic tests (no broker)
docker compose up -d           # start the local Kafka + Schema Registry stack
cargo test -- --ignored        # integration tests against that stack
cargo run                      # UI against your config
```

Design notes and milestone plan: [PLAN.md](./PLAN.md).

## License

See repository owners for licensing.
