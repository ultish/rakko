# kaf-tui — Kafka monitoring/management TUI

## Context

The user wants a `ratatui`-based terminal UI for managing and inspecting Kafka clusters, including ones running in Kubernetes (reached via a manually-managed port-forward/tunnel — no k8s automation needed in the app itself). The target directory (`/Users/jxhui/Developer/kaf-tui`) is empty, so this is a from-scratch project. Requirements were gathered over an extended discovery conversation (client library, connectivity model, message browsing modes, auth, schema registry, secrets storage, producer UX, export/import, and single-message replay). The design below reflects all of those decisions and adds the concrete Rust architecture needed to build it: crate choices, module layout, and the trickiest integration points (async+ratatui, rdkafka's consumer-group admin gaps, byte-preserving replay/export).

Full v1 scope is intentionally in view now (per the user's preference), but building proceeds in independently-testable milestones.

## Confirmed requirements (recap)

- Multi-cluster: named connection profiles (bootstrap address, TLS on/off, auth = mTLS or none today, designed to add SASL later without a redesign).
- No k8s-specific connectivity code — plain external TLS/mTLS client, user handles tunneling.
- Config: plain TOML at `~/.config/kaf-tui/` on macOS + Linux (constructed manually, not via a crate's platform-native path, since macOS's "native" convention differs).
- Topics view: partitions, replication factor, compression type, message counts, size.
- Consumer groups: members, per-partition lag, and a confirmed, destructive offset-reset action (offset / timestamp / earliest / latest).
- Schema Registry: Confluent-compatible.
- Deserialization: auto-detect per message (Avro via magic-byte + schema registry, else JSON, else raw/hex). Compression is transparent via librdkafka.
- Browsing: live-tail ring buffer AND paginated seek-by-offset/timestamp, both with filter/search, sharing one underlying reader abstraction.
- Producing: inline editor pane, load-from-file, or `$EDITOR` shell-out; message size not capped at Kafka's 1MB default (per-profile configurable).
- Single-message replay: select a message, one keybind, instantly re-produces original raw bytes onto the **same topic**, no edit step (editing is a separate flow). Optional opt-in step to append headers.
- Export: JSONL, base64 raw bytes (source of truth) + decoded view, single-message and streaming bulk "export all".
- Import: replay a JSONL file back onto Kafka using the raw-bytes field, with a selectable target topic (distinct from single-message replay's same-topic-only rule).
- Platform: macOS + Linux from day one.

## Crate selection

```toml
[dependencies]
rdkafka = { version = "0.39", features = ["cmake-build", "ssl", "ssl-vendored", "zstd", "libz-static", "tokio"] }
ratatui = "0.30"
crossterm = { version = "0.29", features = ["event-stream"] }
tokio = { version = "1", features = ["full"] }
futures = "0.3"
schema_registry_converter = { version = "4.9", features = ["avro", "easy", "rustls_tls"], default-features = false }
apache-avro = "0.21"
reqwest = { version = "0.13", default-features = false, features = ["json", "rustls-tls"] }
serde = { version = "1", features = ["derive"] }
toml = "0.9"
dirs = "6"
base64 = "0.22"
serde_json = "1"
clap = { version = "4.6", features = ["derive"] }
anyhow = "1"
thiserror = "2"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
tracing-appender = "0.2"
```

Notable decisions from research (verify exact versions with `cargo add` at scaffold time — they drift):

- **No literal "vendored" feature exists.** Static librdkafka compilation from bundled source is the *default* since rdkafka 0.39 — no system librdkafka needed. What you must add explicitly is `ssl` + `ssl-vendored` (statically-linked OpenSSL, required for TLS/mTLS) and `cmake-build` (more reliable than the plain-make build on macOS). Build-time prerequisite to document: CMake + a C toolchain on the *build* machine only — the resulting binary is self-contained.
- **Schema Registry**: use `schema_registry_converter` (actively maintained, async/reqwest-based, depends on the maintained `apache-avro` fork) rather than hand-rolling a REST client. Wrap it with a thin local `HashMap<u32, apache_avro::Schema>` cache so decode failures can fall through to JSON/raw instead of erroring the app.
- **Use `rustls` for the Schema Registry's HTTP client**, not `native-tls`/OpenSSL — avoids a second, differently-versioned OpenSSL colliding with rdkafka's vendored one at link time.
- **Config path**: don't use a crate's "native config dir" helper (recent `dirs`/`directories` versions return `~/Library/Application Support` on macOS). Construct `~/.config/kaf-tui/` manually via `dirs::home_dir()`.
- **Logging**: `tracing` to a file under `~/.config/kaf-tui/` only — never stdout/stderr while the alternate screen is active. Install a panic hook that restores the terminal before the panic prints, or a mid-render panic leaves the terminal broken.

## Module layout

```
src/
  main.rs                    # entrypoint: CLI args, logging init, config load, run App
  cli.rs                     # clap: --profile, --config-dir override, one-shot export/import subcommands
  config/
    mod.rs                   # Config, Profile, load/save (toml)
    profile.rs                # Profile { name, bootstrap_servers, tls, auth }
    auth.rs                    # AuthMode enum: None | Mtls{cert,key,ca} | (future) Sasl{..}
  kafka/
    mod.rs                     # KafkaClient facade
    client_config.rs            # Profile -> rdkafka::ClientConfig (security.protocol, ssl.*, message.max.bytes)
    consumer.rs                  # PartitionReader wrapping BaseConsumer, shared by tail + seek modes
    producer.rs                   # FutureProducer wrapper, configurable message.max.bytes
    admin.rs                       # AdminClient: list topics/metadata, describe configs
    group_offsets.rs                # manual group listing/lag computation + offset-reset workaround
    schema_registry.rs               # SrSettings wrapper + schema-id cache
    raw_message.rs                    # RawMessage: byte-preserving type used everywhere (browse/replay/export)
  serde_detect.rs                     # magic-byte sniff -> Avro/JSON/raw; decode-only, never mutates RawMessage
  export.rs                            # streaming JSONL writer/reader
  ring_buffer.rs                        # bounded VecDeque<RawMessage>, tail-mode only
  events.rs                              # AppEvent (background -> UI), Action (UI -> app)
  app.rs                                  # App struct, Screen enum, update(Action) reducer
  ui/
    mod.rs                                 # draw(frame, &App) dispatch
    theme.rs
    widgets/
      table_nav.rs                          # reusable selectable table (topics/groups/messages)
      confirm_dialog.rs                      # yes/no modal (offset reset, bulk import)
      editor_pane.rs                          # inline multi-line editor (producer input mode 1)
    screens/
      profile_picker.rs
      topic_list.rs
      topic_detail.rs                          # message browser: tail/seek toggle, filter bar
      group_list.rs
      group_detail.rs                           # lag table, offset-reset entrypoint
      producer.rs                                # 3 input modes
      export_import.rs
  external_editor.rs                              # $EDITOR shell-out to tempfile (git-commit style)
  error.rs                                          # thiserror AppError
```

`raw_message.rs` is the single canonical byte-preserving type (ring buffer, pagination, export, replay all use it); `serde_detect.rs` only ever attaches a `DecodedValue` alongside it for display/filtering, never replaces it — this is what makes byte-identical replay/export possible.

## Core architectural decisions

**State management**: single `App` struct + `Screen` enum + Elm-style `Action` enum with a `update(&mut self, Action)` reducer. Fits the ~7-screen linear navigation (profile → topics → topic detail → producer/export) and makes destructive confirmations explicit two-step actions (`RequestOffsetReset` → `ConfirmOffsetReset`) instead of boolean flags.

**Async integration**: `crossterm::event::EventStream` + `tokio::select!` over three sources — input events, an `mpsc::UnboundedReceiver<AppEvent>` fed by spawned background tasks, and a tick interval for periodic refresh (lag recomputation, etc). Every Kafka/HTTP operation is `tokio::spawn`ed (or `spawn_blocking` for rdkafka's sync-style calls) and reports back via `AppEvent`, never called inline on the render loop.

**BaseConsumer for both tail and seek modes** (not `StreamConsumer`): seek mode needs imperative `assign()`/`seek()`/bounded-`poll()` control that doesn't compose with `StreamConsumer`'s continuous-stream abstraction. Tail = background task looping `poll()` from `Offset::End`, pushing into the ring buffer. Seek = one-shot bounded `poll()` bursts from a resolved offset/timestamp, paged via `fetch_watermarks()` to detect true end-of-data vs. transient empty poll. Switching modes tears down and recreates the consumer assignment — model as `BrowseMode::Tail(RingBuffer) | BrowseMode::Seek(SeekState)`, mutually exclusive, no shared-state conflicts with the filter layer (a pure predicate function applied to whichever store is active).

**Consumer-group admin gap (the trickiest part — budget extra time here)**: rdkafka's `AdminClient` does **not** expose consumer-group offset listing/altering, despite librdkafka itself supporting it. Workaround:
- List groups/members: `Consumer::fetch_group_list()` on a throwaway `BaseConsumer`.
- Compute lag: a `BaseConsumer` configured with the target `group.id`, `committed_offsets()` per partition minus `fetch_watermarks()` high offset. Read-only, safe even while the real group is active.
- Reset offsets: `assign()` a `TopicPartitionList` with resolved target offsets, then `commit(&tpl, CommitMode::Sync)` with `enable.auto.commit = false`. **This only works reliably if the group has no active members** — check `fetch_group_list()` first and surface a warning in the confirmation dialog if the group looks active, rather than silently failing or being clobbered by a live consumer's next commit.

**TLS/mTLS mapping** (`kafka/client_config.rs`): `AuthMode::None` + TLS off → `PLAINTEXT`; TLS on → `SSL` (+ `ssl.ca.location`/`ssl.certificate.location`/`ssl.key.location` for `Mtls`). `AuthMode` is a serde-tagged enum so a future `Sasl{mechanism, username, password}` variant is additive — no `Profile`/TOML redesign needed. Also carry a per-profile `extra_producer_config: HashMap<String,String>` so `message.max.bytes`/`compression.type` etc. don't need to be hardcoded fields.

## Milestones

1. **M1 — Skeleton + config + connect + topic list.** Config load/save, TLS/mTLS client-config mapping, topic listing via `fetch_metadata()`/`describe_configs()`, profile-picker → topic-list screens, event loop wired up.
2. **M2 — Message browsing (tail + seek + filter), raw-bytes retention.** `RawMessage`, ring buffer, `BaseConsumer`-backed `PartitionReader`, topic-detail screen with mode toggle. Filter works on raw/JSON only until M6 adds Avro decode.
3. **M3 — Consumer groups, lag, offset reset.** The admin-gap workaround above, confirmation dialog. Scoped early (before producer) to de-risk the hardest integration point while the codebase is still small.
4. **M4 — Producer: 3 input modes + message-size config.** `FutureProducer`, inline editor pane, file-path load, `$EDITOR` shell-out, per-profile size config from M1.
5. **M5 — Single-message replay.** Composes M2 (raw bytes) + M4 (producer) — the "instant replay, same topic" keybind plus opt-in header-append step. Never decodes; sends raw bytes straight through.
6. **M6 — Schema registry + Avro auto-detect.** Highest external-dependency milestone (needs a live registry + real Avro topics), sequenced after the self-contained milestones are stable. Can swap with M4 if Avro topics are readily available for earlier manual testing — no functional dependency forces this order (M5 doesn't need M6, since replay never decodes).
7. **M7 — Export/import JSONL.** Streaming writer (paged via M2's primitives, never buffering a full topic), reader with target-topic override reusing M4's producer path. Composes every prior primitive, naturally last.
8. **M8 — Airgap RHEL9 build.** A `Dockerfile.rhel9` + wrapper script, adapted from the sibling `harness` project's `tui/Dockerfile.rhel9` / `scripts/build-tui-rhel9.sh` (Rocky Linux 9 builder → glibc-compatible with RHEL9, avoids `GLIBC_2.xx not found` at runtime; `cargo build --release --target x86_64-unknown-linux-gnu`; extracted binary packaged as a versioned `.tar.gz` + `SHA256SUMS`; multi-runtime docker/podman/Apple-Container wrapper with `--platform linux/amd64` for Apple Silicon). **Delta from that reference**: kaf-tui's `rdkafka` (`cmake-build`, `ssl`, `ssl-vendored`) compiles librdkafka from C source and statically vendors OpenSSL, so the Rocky 9 builder image needs `cmake` and `perl` in addition to `gcc gcc-c++ make`. Verify with `ldd` on the built binary (same check the reference script runs) that only expected dynamic deps (glibc, libgcc_s) remain — no stray dynamic OpenSSL/librdkafka links defeating the point of vendoring. Sequenced last since it packages the finished v1 binary rather than gating any feature work.

## Verification

- **Local stack**: a dev-only `docker-compose.yml` at repo root with `confluentinc/cp-kafka` (KRaft, no ZooKeeper) + `confluentinc/cp-schema-registry`. Used for all manual milestone checkpoints, including generating a throwaway self-signed CA/cert to test mTLS, and running `kcat -C -G <group> <topic>` in a second terminal to create real consumer-group activity for the offset-reset warning path.
- **Pure-logic `cargo test` coverage** (no broker needed): ring-buffer eviction/capacity; `serde_detect` magic-byte sniffing on fixture bytes (including a JSON payload that happens to start with `0x00`, and an unresolvable schema ID falling through to raw rather than panicking); `Profile` → `ClientConfig` mapping per `AuthMode` variant; export/import round-trip (base64 byte-identity, no Kafka involved); config TOML round-trip including the tagged `AuthMode` enum shape; filter-predicate logic over fixture data.
- **Per-milestone manual checkpoints** against the docker-compose stack: M1 topic list matches `kcat -L`; M2 tail receives `kcat -P` output live, seek pages correctly at exact/boundary counts; M3 lag matches `kafka-consumer-groups.sh --describe`, offset reset tested both idle and with an active consumer; M4 a >1MB message is rejected without the profile override and succeeds with it; M5 a replayed Avro message is byte-identical to the original (not just semantically equal); M6 schema-ID cache survives a killed-mid-session Schema Registry container; M7 bulk export of a topic larger than one page keeps memory flat (spot-check with `/usr/bin/time -l`), and re-import into a different target topic produces a matching count.

### Critical files
- `src/kafka/client_config.rs` — Profile → librdkafka config mapping, extensible `AuthMode`
- `src/kafka/consumer.rs` — `BaseConsumer`-backed reader shared by tail/seek
- `src/kafka/group_offsets.rs` — the AdminClient-gap workaround for lag + offset reset
- `src/raw_message.rs` — canonical byte-preserving message type
- `src/serde_detect.rs` — Avro/JSON/raw auto-detect feeding browsing, filtering, and export
