# Changelog

All notable changes to **rakko** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

How agents and humans maintain this file is described in `AGENTS.md`
(section **Changelog**). Prefer user-facing bullets derived from git history
over dumping raw commit subjects.

## [Unreleased]

### Added

- New **Brokers** screen: id/host/port, leader/replica partition counts (load
  distribution across the cluster, from the same metadata call — no extra round
  trip), and a cluster-health line (under-replicated / offline partition counts).
  **Enter** drills into a broker's non-default config values (sensitive entries
  redacted).
- Persistent view-switcher bar under the banner on every list-level screen —
  **1**/**2**/**3** jump directly between Topics/Groups/Brokers, replacing the old
  per-screen **g**/**b** shortcuts (removed) with one consistent mechanism shown in
  the same place everywhere.
- Topic list: **/** filters topics by name (case-insensitive substring), **c**
  clears it — same pattern as the message browser's existing filter.

### Changed

- Keybind consistency pass: **e** is now reserved app-wide for "edit" (profile
  picker, replay's edit-in-producer) — it no longer doubles as "export" on the
  message browser. Export selected/all moved to **x**/**X**; group detail's
  offset-reset trigger moved from **x** to **z** so **x** has one meaning everywhere.

### Fixed

- Group detail's lag table no longer truncates the **Partition** column header to
  "Par…" regardless of available width — it shared a width cap with the message
  browser's single-letter **P** column, which was far too tight for the spelled-out
  header used here.

## [0.2.0] - 2026-07-12

### Added

- Create-profile / edit-profile wizard: an **Auth** field (Space/t to cycle
  plaintext / TLS / TLS with a private CA / mTLS) with CA-path, client-cert-path,
  and client-key-path inputs that appear when needed. Previously TLS-with-CA and
  mTLS profiles could only be configured by hand-editing `config.toml` after
  saving; editing an existing profile now also prefills and can change its auth
  mode, instead of always preserving whatever was already on disk.

### Fixed

- Consumer group listing, group detail, and offset reset no longer occasionally
  fail with a spurious "Group list fetch error: BrokerTransportFailure" right
  after connecting — a fresh connection is now warmed up with a metadata call
  first, since that path retries internally where the group-list call doesn't.

## [0.1.0] - 2026-07-11

### Added

- Kafka TUI (**rakko**): topics, live tail + seek browse, filters, consumer
  groups / lag, offset reset, produce, single-message replay, JSONL
  export/import, Schema Registry Avro decode.
- Connection profiles (PLAINTEXT / TLS / mTLS) at `~/.config/rakko/`.
- Startup otter splash (truecolor half-block + braille fallback) and animated
  banner stream with **ラッコ** branding.
- Message inspector (`Enter`): full key/value/headers; independent **KFmt** /
  **VFmt** columns; newest-first sort (`o`); seek page watermarks
  (`page lo–hi` vs log high).
- Seek page refresh (`r`/`R`); quit confirmation dialog on `q` (Ctrl-c force
  quits).
- Offset-reset entry on group detail via `x` (`r`/`R` reserved for refresh).
- Replay UX: confirm dialog + optional header form (key/value fields, Tab).
- Content-aware table column widths (value/name columns expand).
- RHEL 9 / airgap Linux amd64 release build
  (`Dockerfile.rhel9`, `scripts/build-tui-rhel9.sh`).
- Local docker-compose Kafka + Schema Registry stack and kcat test scripts.
- Project agent guidance (`AGENTS.md`; `CLAUDE.md` includes it via `@AGENTS.md`).
- Profile picker: **e** edits the selected profile in-place (preserves mTLS,
  `message_max_bytes`, and extra producer config not shown on the form).
- Auto-detect broker `message.max.bytes` on connect when the profile omits
  `message_max_bytes`, then save it to config (explicit values are never
  overwritten).
- Export: **e** exports the selected/open message; **E** exports all visible.
- Cursor-aware text fields (←/→/Home/End/Delete) for export/import, producer,
  filter, and offset-reset input.
- Replay: removed “add header”; **e** opens producer prefilled with decoded
  key/value for edit (raw **y**/Enter still byte-identical).
- Producer: **Key** field is now multi-line (previously value-only); a
  high-contrast block cursor auto-scrolls both key and value panes to keep
  the caret in view.

### Changed

- Project renamed from **kaf-tui** to **rakko** (binary, config dir, group ids,
  env `RAKKO_TRUECOLOR`, docs/scripts).
- Message list Value column truncates to terminal width (not a fixed 60-char cap).

### Fixed

- RHEL 9 / airgap release build (`Dockerfile.rhel9`) — patches a vendored
  librdkafka 2.12.1 bug that required `libcurl` even with curl support
  disabled, and copies `assets/` into the build context so the startup
  splash image compiles in.

<!--
When cutting a release, move bullets from [Unreleased] into a new section:

## [X.Y.Z] - YYYY-MM-DD

Then leave [Unreleased] empty (or with only in-progress notes).
-->
