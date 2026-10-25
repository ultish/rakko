---
name: verify-rakko
description: Launch and drive rakko (Kafka TUI + rakko-web JSON API/React UI) against the local docker-compose Kafka and Schema Registry to prove a change works. Use when asked to verify, run, or demo rakko behaviour — topics, message scan/filter, produce/replay, export/import, tail, consumer groups, brokers, schemas, the TUI.
---

# Verify rakko

rakko has two surfaces over one engine (`crates/rakko-core`):

- **rakko-web** — axum JSON API plus a React UI on one port. Primary drive surface: every feature is a `curl`-able endpoint.
- **rakko** — the ratatui TUI. Drive it in a private tmux session.

Both need the docker-compose stack (Kafka `127.0.0.1:19092`, Schema Registry `http://127.0.0.1:18081`). Run everything from the repo root.

## Launch

```bash
docker start rakko-kafka rakko-schema-registry 2>/dev/null || docker compose up -d   # first time: compose creates them
cargo build --release -p rakko -p rakko-web                                          # ~1.5 min cold
npm --prefix crates/rakko-web/web ci --ignore-scripts && npm --prefix crates/rakko-web/web run build   # only for the web UI; the API works without it
.claude/skills/verify-rakko/scripts/web-up.sh 18090      # prints http://127.0.0.1:18090
```

`web-up.sh` writes a scratch config (profile `local`) to `$TMPDIR/rakko-verify-<port>/`, starts `target/release/rakko-web` on the port you give, and waits until `/api/profiles` answers. Pick your own free port; 18090 is the example. Do not use 8080 (that is `scripts/dev-up.sh`'s port and may be someone's live instance).

Do **not** use `scripts/dev-up.sh` for verification: it binds 8080 and does not stop on its own.

The seeded topics (`demo-events`, `demo-orders-avro`, `demo-large`) may be empty: Kafka retention expires them. Every page here creates its own `verify-*` topic through the API instead, so an empty broker is fine. The one precondition is the Avro subject `demo-orders-avro-value` in the registry (`dev-up.sh` registers it; `curl -s http://127.0.0.1:18081/subjects` should list it) for the schema page.

## Healthy

```bash
.claude/skills/verify-rakko/scripts/doctor.sh 18090
```

Read-only. Prints `ok`/`FAIL` for: Kafka container running and answering, Schema Registry answering, `demo-events` exists, rakko-web answering on the port with profile `local`, the TUI binary built. Exit 0 means worth driving. Run it first whenever anything looks off.

Ready means `curl -s http://127.0.0.1:18090/api/profiles` returns `[{"name":"local","has_schema_registry":true}]`.

## Drive

Pick a page in [features/README.md](features/README.md). Each page gives the exact commands and the response to expect. Two scripts run whole journeys with assertions:

```bash
YAGURA_EVIDENCE=/path/to/proof .claude/skills/verify-rakko/scripts/drive-web.sh 18090   # API journey, 15 checks
YAGURA_EVIDENCE=/path/to/proof .claude/skills/verify-rakko/scripts/drive-tui.sh 18090   # TUI journey in tmux, 6 checks
```

API conventions that bite:

- Scan endpoints need a header `x-rakko-session: <any uuid>`; scans belong to that session.
- Destructive calls (delete topic/group/subject, reset, clear) need `confirm` equal to the target's name, else `400 {"error":"confirm must equal …"}`.
- Errors are `{"error":"…"}` with a 4xx status.

## Evidence

Write proof to `$YAGURA_EVIDENCE` (never into the checkout). Both drive scripts save raw responses (`*.json`, `export.jsonl`) or pane captures (`tui-*.txt`) there; default `./evidence-rakko` if unset (gitignore it or delete it, don't commit). A proof must show the action and the resulting state: the response of the call, and a read-back (a scan, an export, a group lag) that confirms the side effect in Kafka. Drive the real endpoints and keys; do not poke Kafka directly except to create a consumer group member (`kafka-console-consumer`, the way the groups page does).

## Isolation

Kafka, the registry, and topics are shared with anyone else using the compose stack. Use unique `verify-<pid>` topic and group names (the scripts do) and delete them. Never `docker compose down -v` or touch topics you did not create. Run your own rakko-web port and your own tmux session; the TUI scripts use `--config-dir` on a temp dir so `~/.config/rakko` is untouched.

## Cleanup

```bash
.claude/skills/verify-rakko/scripts/web-down.sh 18090    # kills the pid it recorded, removes its scratch dir
tmux kill-session -t <your-session> 2>/dev/null          # only if a drive left one; drive-tui.sh cleans up after itself
```

The containers stay up on purpose (`docker compose down` stops them; only do that if you started them). Evidence in `$YAGURA_EVIDENCE` is outside the scratch dir and survives cleanup.

## Helpers

| Script | Use |
|---|---|
| `scripts/web-up.sh [port]` | start rakko-web, print URL |
| `scripts/web-down.sh [port]` | stop it |
| `scripts/doctor.sh [port]` | read-only health check |
| `scripts/drive-web.sh [port]` | API journey with assertions |
| `scripts/drive-tui.sh [port]` | TUI journey with assertions |
