# AGENTS.md

Project guidance for coding agents (Grok, Claude Code, etc.) working in this repo.
Claude Code loads this via `CLAUDE.md` (`@AGENTS.md`); edit **this file only**.

## What this is

**rakko** — a terminal UI (ratatui + rdkafka) for monitoring and managing Kafka
clusters: topics, live tail/seek message browsing with filter, consumer groups + lag
+ destructive offset reset, Avro/JSON auto-detect via Confluent Schema Registry, a
3-mode producer, single-message replay (raw bytes, same topic), and JSONL
export/import. Read `README.md` for user-facing usage and `PLAN.md` for the
architecture/milestone plan — `PLAN.md` is the design-of-record; update it when
architecture decisions change, don't let it silently drift from the code.

## Layout

- `src/config/` — `Profile`/`AuthMode` (PLAINTEXT / TLS+private-CA / mTLS; SASL
  designed for later without a redesign) and TOML load/save at
  `~/.config/rakko/config.toml` (constructed manually, not via a platform-native
  path).
- `src/kafka/` — `client_config.rs` (Profile → librdkafka `ClientConfig`),
  `admin.rs` (topic listing), `consumer.rs` (tail + seek, `BaseConsumer`-backed),
  `producer.rs`, `group_offsets.rs` (the `AdminClient` consumer-group-offset gap
  workaround — read PLAN.md before touching this), `schema_registry.rs`.
- `src/raw_message.rs` — the byte-preserving `RawMessage` type threaded through
  browsing, replay, and export/import. Replay and export must always use these raw
  bytes, never a decoded-then-re-encoded value — see PLAN.md.
- `src/serde_detect.rs` — Avro (magic byte + schema registry) / JSON / raw
  auto-detect. Decode-only, never mutates `RawMessage`.
- `src/app.rs` / `src/events.rs` — the Elm-style `App`/`Action`/`AppEvent`/`Command`
  reducer. Background Kafka/HTTP I/O is never called inline on the render loop — it's
  spawned and reports back via `AppEvent` (see `src/main.rs`'s event loop).
- `src/ui/` — ratatui screens/widgets.
- `Dockerfile.rhel9` + `scripts/build-tui-rhel9.sh` — airgap Linux/amd64 release
  build (Rocky 9 builder, vendored librdkafka + OpenSSL; needs `cmake`+`perl` beyond
  the harness reference this was adapted from). Output:
  `dist/rakko-linux-amd64.tar.gz`, `dist/rakko`, `dist/SHA256SUMS`, `dist/ldd.txt`.
- `scripts/produce-test-messages.sh` / `scripts/consume-test-messages.sh` —
  kcat-based dev helpers for exercising a real broker (continuous producer with
  random delay; fixed-group-id consumer for lag/resume testing).
- `docker-compose.yml` / `config.example.toml` — local Kafka + Schema Registry stack
  for manual testing.

## Hard constraints (don't break these)

- **Config path is `~/.config/rakko/`** on both macOS and Linux, not
  `~/Library/Application Support` — deliberate, don't "fix" it to be more
  macOS-native.
- **Replay and export use raw wire-format bytes, never a decoded/re-encoded value.**
  This is what keeps Avro schema IDs and encoding byte-identical on resend — a
  design constraint, not an oversight.
- **No Kubernetes-specific connectivity code.** rakko is a plain external TLS/mTLS
  client; the user handles port-forwarding/tunnels themselves.
- **Background I/O never blocks the render loop.** rdkafka's sync-style calls run
  via `spawn_blocking`; anything continuous (tail mode) needs cooperative
  cancellation via a `watch` channel, since `JoinHandle::abort()` does not stop an
  in-flight `spawn_blocking` closure — see `kafka/consumer.rs`.
- **`AdminClient` doesn't expose consumer-group offset APIs** (despite librdkafka
  supporting them) — `group_offsets.rs`'s assign+commit workaround is deliberate,
  not a stopgap to "clean up."

## Before you finish a change

- `cargo test` — pure-logic tests, no broker required (config round-trips, ring
  buffer, serde_detect, seek pagination math, etc.).
- `cargo build` / `cargo clippy` — should stay warning-clean modulo expected
  dead-code on not-yet-wired pieces.
- For anything touching connection/TLS/consumer/producer/group-offset logic:
  `docker compose up -d` then `cargo test -- --ignored` runs the docker-compose-gated
  integration tier (produce/consume round-trip incl. a 20MiB message, topic listing,
  consumer-group lag + offset reset idle/active-member paths, live Schema Registry
  fetch) — see `kafka::integration_support`'s doc comment for the pattern to extend.
  These are `#[ignore]`d so plain `cargo test` never touches a live broker; still worth
  a manual pass against `config.example.toml`'s `local` profile for anything the
  automated tier doesn't reach (TLS/mTLS handshakes, the actual TUI).
- **User-facing changes:** update `CHANGELOG.md` under `[Unreleased]` (see
  **Changelog** below). Skip pure refactors, test-only, and docs-only work unless
  the user-facing surface changed.

## Changelog

`CHANGELOG.md` is the human-readable history of **user-visible** changes. Format:
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) sections under SemVer
headings. Git is the source of *what happened*; the changelog is the curated
summary — **not** a paste of commit subjects.

### Day-to-day (feature / fix work)

1. **After** the change is implemented (or as part of the same commit), open
   `CHANGELOG.md` and add bullets under **`## [Unreleased]`**.
2. Pick a section (add the heading if missing):
   - `### Added` — new capability or UI surface
   - `### Changed` — behavior change users will notice
   - `### Fixed` — bug fix
   - `### Removed` — removed feature / keybind / config key
   - `### Deprecated` / `### Security` — as needed
3. Write **user-facing** bullets (what a user would care about), not implementation
   notes. Good: `Seek page refresh (r/R) reloads the current offset window.`
   Bad: `Wire Action::Refresh in refresh_topic_detail().`
4. Derive bullets from the work just done **and** from git when catching up:
   ```bash
   # Since last tagged release (or since main diverged):
   git log vX.Y.Z..HEAD --oneline
   # Or unreleased commits on this branch:
   git log --oneline -20
   ```
   Skim commits/diffs, then **rewrite** into short product language. Do not dump
   `git log` verbatim into the changelog.
5. Keep `[Unreleased]` ordered roughly newest-first within each subsection is fine;
   merge related bullets instead of one line per commit.
6. Prefer updating the changelog **in the same commit** as the feature/fix so
   `git log -p -- CHANGELOG.md` stays aligned with history. If you forget, a
   follow-up commit that only edits `CHANGELOG.md` is OK — mention the related
   change in the commit message.

### When cutting a release

1. Bump `version` in `Cargo.toml` (see release checklist).
2. In `CHANGELOG.md`:
   - Rename `## [Unreleased]` content into
     `## [X.Y.Z] - YYYY-MM-DD` (today’s date, ISO).
   - Leave a fresh empty `## [Unreleased]` at the top (no bullets yet).
3. Use the new section (or a short headline distilled from it) as
   `gh release create --notes`. Prefer the changelog body over inventing notes
   from scratch:
   ```bash
   # Example: notes from the version section (edit interactively if needed)
   gh release create vX.Y.Z \
     --title "vX.Y.Z — <headline>" \
     --notes-file <(sed -n '/## \[X.Y.Z\]/,/## \[/p' CHANGELOG.md | sed '$d') \
     dist/rakko-linux-amd64.tar.gz \
     dist/SHA256SUMS
   ```
4. Commit version bump + changelog together when possible
   (`Release vX.Y.Z` or similar).

### What not to changelog

- Internal refactors with no user-visible behavior change
- Test-only or CI-only changes
- Typo fixes in comments / agent docs (`AGENTS.md`) unless they document a
  product change

## Release checklist (when cutting a version)

**Trigger:** a version bump in `Cargo.toml`'s `git diff` — whether you wrote it or
not — means a release is being cut. Run this checklist before committing. Ownership
of the commit implies ownership of the checklist; "someone else bumped it" is not an
exception.

1. **Bump `version` in `Cargo.toml`.** SemVer: bug fixes → patch, backward-compatible
   features → minor, breaking changes to the config format / CLI / architecture →
   major. Run `cargo build` once after bumping so `Cargo.lock` picks it up.
2. **Update `CHANGELOG.md`:** move `[Unreleased]` bullets into
   `## [X.Y.Z] - YYYY-MM-DD` and reset `[Unreleased]` (see **Changelog** above).
3. **RHEL 9 / airgap Linux release asset (do not skip).** Air-gapped users install
   the prebuilt binary; a version cut without it leaves them stranded.
   ```bash
   ./scripts/build-tui-rhel9.sh
   ```
   Prefer `DOCKER=docker` when the daemon is up; otherwise `DOCKER=container` on
   Apple Silicon. First build is slow (compiles librdkafka + OpenSSL from source).
   - Confirm artifacts exist and look right:
     - `dist/rakko-linux-amd64.tar.gz` — **primary GitHub Release asset**
     - `dist/SHA256SUMS`
     - `dist/ldd.txt` — sanity-check no stray dynamic OpenSSL/librdkafka links crept
       in (should be glibc/libgcc_s only)
     - `file dist/rakko-linux-amd64` → ELF 64-bit **x86-64** (not arm64)
   - Do **not** commit `dist/` (gitignored) — it's attached to the Release only, in
     step 4.
4. **Commit, tag, and cut the GitHub Release** — no CI; releases are 100% manual.
   Committing and pushing does **not** create a release; a Release only exists once
   `gh release create` runs. Every version bump gets a matching git tag *and* a
   GitHub Release.
   - Commit the release (include `Cargo.toml`, `Cargo.lock` if changed,
     `CHANGELOG.md`), push to `main`.
   - Tag the release commit and push the tag:
     ```bash
     git tag -a v<X.Y.Z> -m "Release <X.Y.Z>: <headline>"
     git push origin v<X.Y.Z>
     ```
   - Create the Release, attaching the dist assets; **prefer notes from
     `CHANGELOG.md`** for that version (see Changelog → When cutting a release):
     ```bash
     gh release create v<X.Y.Z> \
       --title "v<X.Y.Z> — <headline>" \
       --notes "<from CHANGELOG.md [X.Y.Z] section>" \
       dist/rakko-linux-amd64.tar.gz \
       dist/SHA256SUMS
     ```
   - Verify: `gh release view v<X.Y.Z>` shows both assets and `isDraft: false`.
