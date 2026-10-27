# Release version check

Proves a release commit: every manifest carries the new version, the changelog has a dated section above a fresh empty `[Unreleased]`, and the workspace builds and passes tests.

```bash
.claude/skills/verify-rakko/scripts/check-release.sh 0.24.2
```

Returns `ok: release 0.24.2` and exit 0. Any mismatch prints `FAIL: …` and exits 1. No broker needed.
