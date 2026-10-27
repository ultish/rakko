# rakko features

Pages cover what the app does today. Variables used on every page:

```bash
B=http://127.0.0.1:18090/api                       # your rakko-web port
H='x-rakko-session: 11111111-1111-1111-1111-111111111111'
J='content-type: application/json'
T=verify-$$                                         # scratch topic
```

| Page | Check |
|---|---|
| [topics-and-scan.md](topics-and-scan.md) | create/list/delete topics, full-topic scan with text and query filter, message detail, filter autocomplete |
| [produce-and-replay.md](produce-and-replay.md) | produce text and Avro, byte-identical replay |
| [export-import.md](export-import.md) | JSONL export (filtered) and import round trip |
| [live-tail.md](live-tail.md) | server-sent-event tail of new messages |
| [consumer-groups.md](consumer-groups.md) | group lag, confirmed offset reset, delete |
| [brokers-and-schemas.md](brokers-and-schemas.md) | broker list/config, ACLs, Schema Registry subjects, Avro decode |
| [release-version-check.md](release-version-check.md) | version in all manifests + lockfiles, dated changelog section, build and tests |
| [tui.md](tui.md) | the terminal UI: profile, topics, messages, filter, message view, groups |

`scripts/drive-web.sh` covers the first four web pages and the groups page in one run; `scripts/drive-tui.sh` covers the TUI page.
