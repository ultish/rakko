# Changelog

All notable changes to **rakko** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

How agents and humans maintain this file is described in `AGENTS.md`
(section **Changelog**). Prefer user-facing bullets derived from git history
over dumping raw commit subjects.

## [Unreleased]

## [0.20.0] - 2026-10-21

### Added

- Web: with a filter in the topics sidebar, a bulk bar shows the number of matching topics with "Delete topics" and "Clear messages". Each opens a confirmation listing the affected topics and requiring you to type their count; results are shown per topic so one failure doesn't hide the others, and the topic list refreshes afterwards.
- Web: the live view has a "Keep existing messages" toggle (on by default). Switching a topic to live now keeps the messages already loaded and adds new ones above them; turn it off to start from an empty list as before.

- Web: a Consumers panel on each topic lists the consumer groups that have committed offsets on it, with state and lag on that topic; each name links to the group's page.

### Fixed

- Web: the message detail "copy" button now shows "Copied" for 1.5s, and "Copy failed" when the browser blocks or lacks clipboard access, instead of failing silently.

## [0.19.0] - 2026-10-16

### Added

- Partitions tab: "Estimate size" gives an approximate data size per partition and for the topic (sampled; Kafka does not report it to clients).

### Changed

- Topic list counts refresh every 5 seconds with a lighter request.
- Live tail no longer skips messages on a busy topic: messages travel in batches and the page re-renders once per batch.

### Fixed

- Produce dialog: the Avro subject picker is now a real dropdown (topic's subjects first, then all, or type a name) instead of a text box that filtered itself to the pre-selected value.

## [0.18.0] - 2026-10-15

### Changed
- **Web:** Lists refresh themselves — topics every 30 s, consumer groups, schema subjects/versions and partitions every 15 s, brokers every 30 s, ACLs every minute — and immediately when you return to the tab, so new topics, schemas and groups appear without F5. Message lists don't change under you; use Refresh, or Live for a stream.

### Added
- **Web:** **Delete group…** on a consumer group's page (typed confirmation; Kafka refuses while the group has active members, and the message says so).
- **Web:** **Clear all messages…** in a topic's ⋯ menu: removes every message but keeps the topic, its settings and its consumer groups (typed confirmation).

## [0.17.0] - 2026-10-15

### Added
- **Web:** Consumer-group **lag over time** — a chart of total and per-topic lag (15 min – 24 h) on the group page, sampled every 30 s while a group is open and kept in the server's memory.
- **Web:** **Partitions** tab on a topic: leader, replicas, in-sync replicas (under-replicated and offline partitions are flagged), oldest/next offset.
- **Web:** **ACLs** tab: read-only list of every ACL on the cluster, with a filter.
- **Web:** **Saved searches** per topic (text, query, range, sort), kept in the browser.
- **Web:** Oldest-first (time or offset ascending) now streams instead of reading the whole topic first, merging partitions into one exact order.
- **Web:** The topic-list filter and the open message are part of the URL.
- Schema Registry over **HTTPS with a private CA / client certificate** (web and TUI). Defaults to the cluster's own CA and client cert; override with `[profiles.schema_registry_tls]`.

### Changed
- **Web:** Kafka's terse errors (InvalidRecord, TopicAlreadyExists, authorization failures, unreachable brokers, …) now come with a plain-language explanation; the broker's original text is kept in brackets. Registry connection errors include the underlying TLS reason.
- **Web:** Opening messages is faster: consumers for single-message fetches are reused.

## [0.16.0] - 2026-10-15

### Added
- **Web:** True server-side sort — click a column header (time, partition, offset, key, value) and the whole topic is sorted, not just what is loaded. Non-default orders read the whole topic first (up to 200,000 matches; narrow the filter or range beyond that).
- **Web:** The current view lives in the address bar — Browse/Table/Live, text and query filters, range, sort, and the selected schema version — so reloading or sharing a link lands on the same screen.

### Changed
- **Web:** Avro producing works with any subject naming strategy. The producer offers the subjects the topic's own messages use (TopicNameStrategy `<topic>-value` or RecordNameStrategy record names such as `com.acme.Order`), and *Edit & produce* keeps the edited message's own subject.

## [0.15.0] - 2026-10-15

### Added
- **Web:** Avro producing — pick the **avro** mode for the key or value, choose a subject (defaults to `<topic>-key` / `<topic>-value`), and get an editable JSON template of its latest schema; the server encodes it in Confluent wire format.
- **Web:** Topic settings dialog now lists every setting (not just overrides), with filter, inline editing and per-setting reset to the broker default.
- **Web:** **Table** view for a topic — the latest value of every key (deleted keys removed), with JSON fields as columns.
- **Web:** Topics can be sorted by name, message count or partitions; the topic list can be resized by dragging its edge (both remembered).
- **Web:** Click a column header in the message list to sort the loaded messages.
- **Web:** Strimzi operator internal topics (`__strimzi*`) are tagged.

### Changed
- **Web:** *Edit & produce* now loads the full message when clicked (including Avro, which is re-encoded), instead of staying disabled until you had paged through it.
- **Web:** Topic message counts are labelled as an upper bound ("up to N messages"); once an unfiltered scan completes the real count is shown next to the offset count.

### Fixed
- **Web:** Switching between schemas no longer shows a "version not found" error left over from the previous schema.
- **Web:** The ⋯ and Download menus open towards the left when they would run off the screen, instead of forcing a horizontal scrollbar.

## [0.14.0] - 2026-10-15

### Added

- **`rakko-web`: a browser UI** (new `crates/rakko-web` + `web/` frontend), for teams
  that want rakko without a terminal. First slice: topic list, a message list that
  scans the **whole topic** newest-first and keeps loading as you scroll, text and
  structured-query filters with **server-side autocomplete**, and a detail panel that
  reads even a 20 MiB message in chunks. Each browser session has its own scans, so
  concurrent users never share a filter. Decodes Avro (including schema references
  and RecordNameStrategy topics) exactly like the TUI. Run with
  `rakko-web --config config.toml` after `npm --prefix crates/rakko-web/web run build`.

- **`rakko-web` is feature-complete with the terminal app**: live tail, produce (text /
  base64 / file, headers, tombstones), replay and edit-and-resend, JSONL export (current
  filter, whole topic) and import, consumer groups with lag and a confirmed offset
  reset, and a brokers page. It has no login of its own: put it behind your authenticating proxy.

- **`rakko-web` container image** (`Dockerfile.web`, `scripts/build-web-image.sh`,
  `.github/workflows/web-image.yml`): UBI 9 based, with an air-gapped `docker save`
  archive attached to releases and an example `deploy/kubernetes.yaml`.

- **Schema management in the web UI**: browse subjects and versions, view a schema,
  register a new subject or version (with a compatibility check and references),
  delete a version or subject (soft or permanent, typed confirmation), and set the
  compatibility level per subject or globally.

- **Scan ranges in the web UI**: limit a browse, filter or export to a time window
  (presets for the last 15 min / 1 h / 24 h / 7 d, or any from/to) or to one partition's
  offset window, instead of always scanning the whole topic. Time bounds use the
  broker's timestamp index, so a narrow window is found without reading the rest.

- **Topic management in the web UI**: create a topic (partitions, replication factor,
  config overrides with presets), view its non-default config, and delete it (typed
  confirmation; internal topics are protected).

- **Web query autocomplete now uses the Avro schema**, not just recent messages: it
  offers every field the topic's schema allows (union branches, optional and nested
  fields, referenced types), including on an empty topic. Schemas come from the ids in
  sampled messages and the latest `<topic>-key` / `<topic>-value` subjects; JSON topics
  still use sampled messages.

- **Delete and download in the web UI**: tombstone a message's key, or permanently
  delete everything before an offset in a partition (Kafka can't delete one message in
  place); download a message's key/value as raw bytes or decoded text, or the whole
  record as re-importable JSONL.

- Query autocomplete now offers `key` / `value` on an empty query and right after
  `AND` / `OR` / `(` (web: also when the box is focused), instead of only once you've
  started typing a root word.

- **Settings tab** (`4`, alongside the existing `1`/`2`/`3` switcher): edit
  `seek_page_size` and cycle theme/banner mode from a dedicated screen instead
  of only via the `T`/`A` keys.

### Fixed

- Avro schemas that use Schema Registry **references** (a schema pulling in named
  types from other subjects) now decode. Previously the schema failed to parse and
  those messages showed as raw hex. Transitive references are followed.
- A single `NaN` / `Infinity` in an Avro `float`/`double` field no longer makes the
  whole message undecodable; it shows as `"NaN"` / `"Infinity"` / `"-Infinity"`.
- Avro `bytes` / `fixed` fields now show as base64 strings instead of an array of
  integers (a large blob was ~4x its size as `[12,255,...]`).
- A Schema Registry blip (unreachable, 5xx, 429) no longer leaves messages undecoded
  for the rest of the session: the schema fetch is retried with backoff (1s, 2s, ...
  up to 60s). Genuine failures (404, bad schema) are still not retried.
- Protobuf / JSON-Schema registry entries now report "only Avro is decoded" rather
  than a misleading Avro parse error.

## [0.13.2] - 2026-07-17

### Fixed

- Cycling the banner with **A** no longer leaves a sticky `banner: …` toast on
  screen. The mode is already shown on the banner strip itself.

## [0.13.1] - 2026-07-16

### Changed

- Banner **`A`** cycle is now **wave → ms/frame → fps → off**. **`ms`** shows
  paint wall time; **`fps`** shows paint capacity (`1000 / ms`) — the familiar
  high number from earlier builds, still not screen refresh rate. Config:
  `banner_mode = "wave" | "ms" | "fps" | "off"` (existing `"fps"` still loads).

### Fixed

- Message list browsing with large payloads (especially Avro) no longer re-decodes
  every buffered row on every redraw. List-row key/value previews are cached and
  only computed for the on-screen viewport (plus a small overscan), so the banner
  tick and other idle redraws stay cheap while the Value column still shows real
  preview text.
- **K** / **V** / **Y** copy confirmations now show as a top-centered toast
  (`copied key! (N bytes)`) in the same greenish-yellow as the ms/frame strip,
  not a quiet footer line that was easy to miss (and previously only appeared in
  the empty list placeholder). Toasts auto-dismiss after ~2.5s.

## [0.13.0] - 2026-07-16

### Added

- **Help overlay (`?`)** — full keybind reference for the current screen (global +
  contextual sections). Esc or `?` closes it. Structured query filter moved from
  `?` to **`Q`** so help can own `?` app-wide.
- **Copy to clipboard** on the message browser (list selection or open inspector):
  **`V`** value, **`K`** key, **`Y`** `topic:partition@offset`. Uses the OS
  clipboard when available, with OSC 52 fallback for SSH/tmux. Status line
  confirms size or reports failure.
- **Paste from clipboard** — **Ctrl+V** / **Cmd+V** inserts host clipboard text
  into the focused producer field (and other text inputs: profile form, export
  path, filters). Bulk insert at the cursor, not one key per character.
- **Semantic themes** — dark (default) and light palettes with shared slots
  (title, selection, status, error, borders, …). Cycle with **`T`**.
- **`[ui]` config** — `theme` and `banner_mode` under `~/.config/rakko/config.toml`.
  Cycling banner (`A`) or theme (`T`) persists the choice.

### Changed

- Opening a topic starts in **page (seek) mode** on the latest messages, not live
  tail — so n/p paging works immediately. Toggle to live tail with **Tab**/**s**.
- Advanced query filter keybind: **`Q`** (was `?`).
- Dark theme restyled closer to Grok Build's GrokNight: near-black `#141414`
  background, painted full-frame surface (no longer leaves the terminal's
  default profile color showing through). Color roles: **purple** only for
  selection, focus, and brand “rakko”; **soft cyan** for titles/footers/hints;
  **grey/white** for borders and body text.
- Light theme background toned down from near-white to a softer paper grey.

### Fixed

- Splash screen no longer clears the theme background back to the terminal
  default; it uses the same near-black (or light) surface as the rest of the UI.

## [0.12.0] - 2026-07-14

### Added

- Profile deletion: **z** on the profile picker opens a confirm dialog, then
  removes the profile from `config.toml`. Deleting the last remaining profile
  reopens the create-profile form, same as first run.

## [0.11.0] - 2026-07-14

### Added

- `seek_page_size` config setting (top-level, not per-profile) controls how many
  messages a seek-mode page request loads at a time (n/p paging, the initial page
  on switching into seek mode, and mode-switch-to-latest). Defaults to 50 (was a
  fixed 100) if unset.

## [0.10.2] - 2026-07-14

### Fixed

- Message browsing no longer redoes expensive decode work on every redraw. The
  message inspector re-decoded and pretty-printed the full key/value from scratch
  on every render (the banner animation redraws every ~200ms even at idle), and a
  filtered message list re-scanned and re-decoded the whole buffer (up to 500
  messages) the same way — both are now cached and only recomputed when something
  that actually changes the result does (a new message, a filter/query-filter
  apply or clear, a mode switch, or a schema finishing its background load).

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
