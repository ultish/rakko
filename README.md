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
#    (save a profile named "local" pointing at localhost:9092)

# 2b. Or copy the example config and skip the form
mkdir -p ~/.config/rakko
cp config.example.toml ~/.config/rakko/config.toml
cargo run -- --profile local
```

| Service          | Host address        |
|------------------|---------------------|
| Kafka            | `localhost:9092`    |
| Schema Registry  | `http://localhost:8081` |

Stop the stack with `docker compose down`.

## Configuration

Config lives at **`~/.config/rakko/config.toml`** on both macOS and Linux (not `~/Library/Application Support`).

### First run / in-app profile creation

If the config file is missing or has **no profiles**, rakko opens a **create-profile form** on startup:

- **Name**, **bootstrap servers** (default `localhost:9092`), **TLS** on/off, optional **Schema Registry URL**
- **Tab** / **Shift-Tab** move fields · **←**/**→**/**Home**/**End** edit within a field · **Delete** forward-delete · **Space**/**t** toggles TLS · **Enter** saves · **Esc** quits (when no profiles exist yet)
- Saves to `~/.config/rakko/config.toml` (creates the directory if needed)
- Auth is saved as `none`; for **mTLS** cert paths, edit the TOML after save (or write the file by hand)

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
bootstrap_servers = "localhost:9092"
tls_enabled = false
schema_registry_url = "http://localhost:8081"

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

| Screen | Keys |
|--------|------|
| **Profile picker** | **Enter** connect · **n** new profile · **e** edit profile · **q** quit |
| **Create profile** | **Tab** / **Shift-Tab** fields · **←**/**→**/**Home**/**End** cursor · **Delete** · **Space**/**t** TLS · **Enter** save · **Esc** cancel/quit |
| **Topics** | **Enter** open topic · **g** consumer groups · **r**/**R** refresh list |
| **Messages** | **Enter** view full message · **Tab**/**s** tail ↔ seek · **o** sort newest/oldest · **n**/**p** or PgDn/PgUp page · **r**/**R** refresh page (seek) · **/** filter · **c** clear filter · **w** produce · **y** replay · **e** export selected · **E** export all visible · **i** import |
| **Message view** | **j**/**k** or arrows scroll · **PgUp**/**PgDn** page · **Enter**/**Esc** close · **y** replay · **e** export this message |
| **Groups** | **Enter** detail · **r**/**R** refresh list |
| **Group detail** | **x** reset offsets · **r**/**R** refresh lag (also auto every ~3s while open) |
| **Producer** | **Tab** focus · **F3**/Ctrl-m mode (inline / file / `$EDITOR`) · **F2**/Ctrl-p send · **Esc** back |
| **Replay** | **y**/**Enter** raw replay (byte-identical) · **e** edit in producer · **n**/**Esc** cancel |
| **Export/import** | type path · **←**/**→**/**Home**/**End** cursor · **Delete** · **Tab** (import: path ↔ topic) · **Enter** run · **Esc** back |

Offset reset only works reliably when the group has **no active members** — the UI warns if members are present.

### What updates live

| Data | Live? |
|------|--------|
| Messages in **tail** mode | Yes — continuous consumer poll |
| Messages in **seek** mode | No — load pages with **n**/**p** |
| Topic list / group list | On open, or **r** refresh |
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
cargo test          # pure-logic tests (no broker)
cargo run           # UI against your config
```

Design notes and milestone plan: [PLAN.md](./PLAN.md).

## License

See repository owners for licensing.
