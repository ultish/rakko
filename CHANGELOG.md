# Changelog

All notable changes to **rakko** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

How agents and humans maintain this file is described in `AGENTS.md`
(section **Changelog**). Prefer user-facing bullets derived from git history
over dumping raw commit subjects.

## [Unreleased]

## [0.10.1] - 2026-07-13

### Fixed

- Banner FPS readout no longer misleadingly pins at ~5 while idle. It measured
  the gap between redraws, and rakko only redraws on an event — at idle the
  only thing driving one is the 200ms banner tick, so the number always read
  "~5fps" regardless of actual performance, looking like a problem when it was
  the intended idle behavior. It now times the render call itself: idle draws
  are fast (a high, reassuring number), and a genuinely stalled render (the
  failure mode this exists to catch) still reads low.

## [0.10.0] - 2026-07-13

### Added

- Top banner's `A` key now cycles wave → FPS → off (previously animation on/off
  only). FPS mode shows a live braille graph plus numeric readout of actual
  render cadence — a built-in, always-on perf diagnostic: sit on a heavy
  screen and a stalled render loop shows up immediately as a flatlined or
  dropping graph, no targeted benchmark needed to notice.

### Changed

- Banner wave animation now flows as a real wave — peaks and troughs are
  interpolated between sparse random keyframes with a smoothstep ease, instead
  of each column being an independent random height lightly blended with just
  its next neighbor (which read as texture/noise rather than motion).

## [0.9.2] - 2026-07-13

### Fixed

- Avro list-row preview is now bounded by total content, not per-field: a schema
  with many fields, or several nested levels of sub-records/arrays, could each
  individually stay under the previous per-field cap while summing to enough
  work to slow rendering again. Truncation now stops as soon as a shared preview
  budget is spent, however wide or deeply nested the schema, so it stays fast
  regardless of schema shape rather than just single-field size. Query filter
  autocomplete is unaffected — it always decodes full, untruncated messages, on
  a separate path from the list preview.

## [0.9.1] - 2026-07-13

### Fixed

- Topic detail message list is now fast and stays responsive on topics with
  large (1-3MB+) messages — the row preview no longer fully decodes every
  visible message's key/value on every render (the list redraws continuously,
  e.g. every ~200ms for the banner animation, not just when data changes). Large
  Avro records previously made the whole UI grind to a halt (multi-second
  redraws blocking keyboard/mouse input); they now show a real, bounded preview
  of the decoded record instead of paying full decode-and-serialize cost per
  render. Large JSON/text messages are similarly bounded. Opening a single
  message (Enter) still shows its full, untruncated decoded content.

## [0.9.0] - 2026-07-13

### Changed

- Producer screen now shows Key and Value as side-by-side columns, matching the
  message inspector's layout.
- Replay confirmation dialog is now sized to its content (no more floating in a
  block of blank space), with the message's topic/partition/offset/key/headers
  shown as aligned fields and a single accurate footer for its three actions
  (replay raw / edit in producer / cancel) — previously a duplicate, and for this
  dialog inaccurate, "y: confirm  n/Esc: cancel" line was tacked on underneath.

### Fixed

- Producer screen: ↑/↓ and mouse wheel now move the cursor within multi-line
  Key/Value fields (previously did nothing), and PageUp/PageDown/mouse wheel now
  scroll the read-only value preview in file-path and external-editor mode
  (previously stuck showing only the top of a loaded file).

## [0.8.0] - 2026-07-13

### Added

- Brokers screen shows a per-broker load bar chart (leader/replica partition counts)
  next to the table, so an imbalanced cluster is visible at a glance.
- Group detail screen shows a total-lag trend sparkline once a couple of refreshes
  (manual or auto) have accumulated history, so whether a group is catching up or
  falling behind is visible without watching the raw number tick.

## [0.7.0] - 2026-07-12

### Changed

- Message inspector redesigned as a 2×2 grid: fixed **Attrs** (topic/partition/offset/
  timestamp/formats) and **Headers** on top, **Key**/**Value** below (40/60 — value
  gets more room as the typically-larger payload, but key isn't starved since it can
  be just as deeply nested). Key/Headers/Value each scroll independently — **Tab** or
  a click switches which one j/k/PgUp/PgDn control. On a very small terminal, Attrs
  keeps priority over the other panels (it has no scrollback of its own) instead of
  getting squeezed out. **←/→** resizes the focused panel's share of its row
  (Attrs↔Headers or Key↔Value), and the split persists while browsing a topic.

## [0.6.0] - 2026-07-12

### Added

- Tab-completion in the advanced query filter dialog: completes `key`/`value`, then
  cycles through field names actually present on the current page (`value.` shows
  every top-level field; keep tabbing to go deeper, e.g. `value.house.` → `owner` /
  `price` / `rooms`). Shows the full candidate list with the current pick highlighted.

## [0.5.0] - 2026-07-12

### Added

- Mouse support: scroll wheel navigates the current list (or scrolls the message
  inspector, when open); clicking a list row selects it, and double-clicking it opens
  it (same as Enter); hovering a row highlights it; clicking a producer or
  export/import field box focuses it directly instead of Tab-cycling to it; the
  Topics/Groups/Brokers switcher bar is clickable.

## [0.4.0] - 2026-07-12

### Added

- Advanced structured query filter on the message browser (**?**), opened as a dialog
  with room for a longer query and a built-in help panel (**Ctrl-h**): field-path queries
  into JSON/Avro keys and values, e.g.
  `key.person.name = jxhui AND key.person.age = 20 AND value.house.owner = jxhui`.
  `=`/`!=`/`>`/`<`/`>=`/`<=`, `AND`-chaining, arbitrary nesting depth, and array fields
  matched by "any element" (same implicit behavior as MongoDB's dot-notation array
  queries, including for the comparison operators) — no index syntax needed.
  Independent of and composable with the existing substring filter (**/**); both apply
  together when both are set. A parse error shows in the status line and keeps the
  query dialog open to fix it.

## [0.3.0] - 2026-07-12

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
- Topic list and consumer-group list both get **/** to filter by name
  (case-insensitive substring) and **c** to clear it — the same pattern, on both
  screens, as the message browser's existing filter.

### Changed

- Keybind consistency pass: **e** is now reserved app-wide for "edit" (profile
  picker, replay's edit-in-producer) — it no longer doubles as "export" on the
  message browser. Export selected/all moved to **x**/**X**; group detail's
  offset-reset trigger moved from **x** to **z** so **x** has one meaning everywhere.
- Dropped the redundant **R** binding — refresh was always identical behavior on
  **r** and **R**, so screens now list only **r**.
- Topic list is now sorted alphabetically by name instead of whatever order the
  broker's metadata response happens to return.

### Fixed

- Group detail's lag table no longer truncates the **Partition** column header to
  "Par…" regardless of available width — it shared a width cap with the message
  browser's single-letter **P** column, which was far too tight for the spelled-out
  header used here.
- The topic list and group list now open their filter bar in the same place as the
  message browser's (above the list, right under the banner) — it previously opened
  below the list, just above the footer, which put it in a different spot on every
  screen.
- The message browser's header line (topic, partitions, mode, sort, filter, schema
  registry) now spaces every segment evenly — sort/filter/SR were previously tacked
  on with a bare double-space instead of the `·`-separated style used for the rest,
  which read as cramped even with terminal width to spare.

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
