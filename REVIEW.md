# rakko — Project Review

Scope: full-repo read-through (source, tests, docs, build config) as of commit
`8435745` (v0.1.0). No code was changed as part of this review, except adding an
architecture diagram to `README.md` at the user's request.

## Summary

rakko is a well-scoped, cleanly-architected Kafka TUI. For a ~10.7k-line, single-owner
Rust project it's in unusually good shape: the build is warning-clean, clippy is
silent, 178 tests pass, and the trickiest integration points (byte-preserving replay,
the `AdminClient` consumer-group-offset gap, cooperative task cancellation) are called
out explicitly in `PLAN.md` and implemented the way the docs say they are. The main
structural risk is that `src/app.rs` has grown into a 4,445-line monolith holding
nearly all reducer logic for seven screens; everything else is comfortably sized.

## What's strong

- **Design docs match the code.** `PLAN.md` isn't aspirational — the `BaseConsumer`
  choice, the `assign()`+`commit()` offset-reset workaround, the raw-bytes-only
  replay/export path, and the `spawn_blocking` + `watch`-channel cancellation model
  are all exactly as documented. This is rare and makes onboarding cheap.
- **Byte-preservation discipline is real.** `RawMessage` (`src/raw_message.rs`) is
  genuinely the only path through browsing, replay, and export — `serde_detect.rs` is
  decode-only and never touches it. `kafka/consumer.rs` builds `RawMessage` straight
  from `BorrowedMessage` with no decode step in between.
- **No blocking I/O on the render loop.** Every Kafka call runs inside
  `tokio::task::spawn_blocking`, reporting back over an `mpsc::UnboundedSender<AppEvent>`
  (see `src/main.rs:681` `run_loop`'s `tokio::select!`). The tail-mode consumer
  (`kafka/consumer.rs:56`) is cooperatively cancelled via a `watch::Receiver<bool>`
  checked after every bounded `poll()` — correctly sidesteps the fact that
  `JoinHandle::abort()` can't stop an in-flight blocking closure.
- **The seek-paging math is isolated and well-tested.** `resolve_seek_plan` in
  `kafka/consumer.rs:194` is a pure function separated from the actual polling loop,
  with 9 unit tests covering edge cases (empty topic, page size larger than the
  available range, already-at-beginning, before-offset below low watermark). This is
  exactly the kind of off-by-one-prone logic that benefits from being pulled out of
  I/O code, and it was.
- **Clean error handling.** Zero `unsafe`. Every `.unwrap()`/`.expect()` in the
  production code paths I checked (`app.rs`, `export.rs`, `consumer.rs`,
  `group_offsets.rs`) is confined to `#[cfg(test)]` modules — the reducer itself
  (`app.rs` lines 1–2740, before its test module starts) has zero `unwrap`/`expect`.
  `AppError` (`src/error.rs`) is a small, honest `thiserror` enum with no
  catch-all-swallow variant.
- **Destructive actions are explicit two-step actions**, not boolean flags — offset
  reset (`RequestOffsetReset`-style `StartOffsetReset` → `ConfirmOffsetReset`) and
  quit (`Quit` → `ConfirmQuit`) both go through a confirm dialog in the `Action` enum
  (`src/events.rs`), matching PLAN.md's stated intent.
- **The consumer-group offset workaround re-validates right before the destructive
  commit.** `reset_group_offsets_blocking` (`kafka/group_offsets.rs:272`) re-checks
  `fetch_group_list` for active members immediately before `commit()`, not just at
  dialog-open time — closes the TOCTOU window where a consumer joins between opening
  the confirm dialog and hitting confirm.
- **Build hygiene**: `cargo build`, `cargo clippy --all-targets`, and `cargo test`
  (178 tests) are all clean with no changes needed. `AGENTS.md`/`CLAUDE.md` codify a
  real changelog discipline and a release checklist that includes the airgap RHEL9
  build — not just "bump version and tag."

## Where it's weaker

1. **`src/app.rs` is a 4,445-line monolith.** ~2,740 lines of actual reducer logic
   (before its own test module) implements `App::update`/`App::apply_event` for all
   seven screens in one file — profile CRUD, topic browsing, filter state, producer
   input modes, replay, export/import, and offset-reset wizard state all live
   together. Nothing here is *wrong*, and PLAN.md's single-`App`-struct choice is
   reasonable for a 7-screen linear-navigation app, but at this size the file is past
   the point where `grep`/scroll is a comfortable way to find a given screen's logic.
   Splitting per-screen state+update logic into `app/topic_detail.rs`,
   `app/producer.rs`, etc. (mirroring the existing `ui/screens/` split) would pay off
   before this grows further — it's the one piece of the architecture that hasn't
   scaled with the feature count.
2. **Young git history.** 5 commits total, single contributor, first commit to
   current HEAD spans a single day (2026-07-11 per `git log`). That's not a defect,
   but it means there's no track record yet of how the codebase holds up under
   sustained multi-session/multi-contributor change — worth revisiting this
   assessment after a few more milestones of real use.
3. **No integration tests against a live broker in CI/`cargo test`.** This is called
   out honestly in both `AGENTS.md` and `PLAN.md` ("manual check against
   `docker compose up` is worth it — `cargo test` doesn't touch a live broker") rather
   than silently glossed over, which is good, but it does mean the trickiest paths —
   offset reset against an active group, Avro decode against a real Schema Registry,
   TLS/mTLS handshakes — have no automated regression coverage. A docker-compose-gated
   `#[ignore]`d integration test tier (opt-in via `cargo test -- --ignored` when the
   stack is up) would close this gap without slowing down the default test run.
4. **Secrets/config are plaintext on disk by design.** `config.toml` holds cert/key
   *paths*, not secrets directly, which is fine for TLS/mTLS — but `PLAN.md` flags
   that SASL is "designed for later" via the tagged `AuthMode` enum
   (`config/auth.rs`). When that lands, a `Sasl { username, password }` variant would
   put a plaintext password in `~/.config/rakko/config.toml` unless it's deliberately
   designed to reference an external secret (env var, keychain, etc.) instead of
   storing the value inline. Worth deciding before SASL is implemented, not after —
   changing the config shape later is a breaking change PLAN.md explicitly wants to
   avoid.
5. **`producer.rs`/`export.rs` message-size handling relies on profile config, not a
   live cap.** `message_max_bytes` is either auto-detected once at connect time or
   manually pinned (per README); if a broker's limit changes mid-session, the client
   config won't know until reconnect. Minor, and consistent with the "no k8s
   automation, no magic reconnect" philosophy, but worth a one-line note in the
   producer screen if a broker rejects a message post-connect due to size, so the
   error surfaces the actual cause rather than reading as an opaque `KafkaError`.

## Untested-by-me surface

This review was static (read + `cargo build`/`clippy`/`test`); I did not spin up
`docker-compose.yml` to exercise TLS/mTLS, Avro decode against a live Schema
Registry, or offset reset against an active consumer group. `PLAN.md`'s own
verification section names these as the manual checkpoints that matter most — if a
release is imminent, that manual pass is the highest-value next step, not further
static review.

## Bottom line

No correctness bugs found in this pass. The codebase does what its own design docs
say it does, which is the strongest signal a solo project of this size can give. The
one thing worth acting on before the next couple of milestones is breaking up
`app.rs` — everything else is refinement, not risk.
