# Changelog

All notable changes to **rakko** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

How agents and humans maintain this file is described in `AGENTS.md`
(section **Changelog**). Prefer user-facing bullets derived from git history
over dumping raw commit subjects.

## [Unreleased]

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
  filter, offset-reset input, and replay header form.

### Changed

- Project renamed from **kaf-tui** to **rakko** (binary, config dir, group ids,
  env `RAKKO_TRUECOLOR`, docs/scripts).
- Message list Value column truncates to terminal width (not a fixed 60-char cap).

<!--
When cutting a release, move bullets from [Unreleased] into a new section:

## [X.Y.Z] - YYYY-MM-DD

Then leave [Unreleased] empty (or with only in-progress notes).
-->
