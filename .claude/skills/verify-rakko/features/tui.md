# Terminal UI (`rakko`)

## Sub-features

Profile picker → Topics (`1`) / Groups (`2`) / Brokers (`3`) / Settings (`4`); Messages (seek/tail, `/` text filter, `Q` query filter, `o` sort); message view; produce `w`, replay `y`, export `x`/`X`, import `i`; group detail and reset `z`.

## How to get to it (user POV)

`target/release/rakko --config-dir <dir>` (dir holds `config.toml` with profiles). First launch shows a welcome screen (`Enter` dismisses), then the profile picker.

## Driving it with tmux

```bash
.claude/skills/verify-rakko/scripts/drive-tui.sh 18090      # whole journey, asserts, saves pane captures
```

By hand: `tmux new-session -d -s S -x 140 -y 40 "target/release/rakko --config-dir $DIR"`, then `tmux send-keys -t S ...` and `tmux capture-pane -t S -p`. Sequence the script uses, with what each screen shows:

| Keys | Screen text to expect |
|---|---|
| `Enter` (welcome) | profile table with `local   127.0.0.1:19092   plain` |
| `Enter` | `1 Topics   2 Groups   3 Brokers   4 Settings`, then the topic table (status line `detected message.max.bytes=20971520 from broker; saved to profile 'local'`) |
| `/` `<topic>` `Enter`, `Enter` | `Topic: <topic> · 1 partitions · mode seek · sort newest↑`; rows `0   <offset>   raw   k3   json ...` newest first |
| `/` `paid` `Enter` | header gains `filter: "paid"`; only rows whose value contains `paid` remain |
| `Enter` on a row | `Message · partition 0 · offset N` with `topic:`, `partition:`, `offset:`, `timestamp:` and Key/Value panes |
| `2` | group table with `Name State Members Protocol` |

## Gotchas

- Always pass `--config-dir`; the app writes back to `config.toml` (detected `message.max.bytes`, theme) and a first run with no profiles opens the create-profile form where typed keys become field text.
- A `detected message.max.bytes…` popup overlays the top-right of the first row for a few seconds; assert on key/offset columns, not the value column of row one.
- The banner animation fills the top of the screen; filter it out of captures (`grep -v '▀'`).
- `q` asks for confirmation; `Ctrl-c` force quits. Kill the tmux session in cleanup.
