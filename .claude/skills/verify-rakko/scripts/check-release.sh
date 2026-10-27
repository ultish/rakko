#!/usr/bin/env bash
# Usage: check-release.sh <version>   (run from repo root). Exits non-zero on any mismatch.
set -u
V=${1:?version}
fail=0
bad() { echo "FAIL: $*"; fail=1; }

for f in Cargo.toml crates/rakko-core/Cargo.toml crates/rakko-web/Cargo.toml; do
  got=$(sed -n 's/^version = "\(.*\)"/\1/p' "$f" | head -1)
  [ "$got" = "$V" ] || bad "$f version=$got want $V"
done
got=$(sed -n 's/^  "version": "\(.*\)",/\1/p' crates/rakko-web/web/package.json)
[ "$got" = "$V" ] || bad "package.json version=$got"
lock=crates/rakko-web/web/package-lock.json
[ "$(grep -c "\"version\": \"$V\"" <(sed -n '1,10p' $lock))" = 2 ] || bad "$lock top two version lines are not $V"
for p in rakko rakko-core rakko-web; do
  grep -A1 "^name = \"$p\"\$" Cargo.lock | grep -q "version = \"$V\"" || bad "Cargo.lock $p not $V"
done

unrel=$(awk '/^## \[Unreleased\]/{f=1;next} /^## \[/{f=0} f&&NF' CHANGELOG.md)
[ -z "$unrel" ] || bad "[Unreleased] is not empty"
line=$(grep -n '^## \[' CHANGELOG.md | sed -n '1,2p' | tr '\n' ' ')
case "$line" in *"[Unreleased]"*"[$V] - 20"*) ;; *) bad "headings out of order: $line";; esac
sec=$(awk -v v="$V" '$0 ~ "^## \\["v"\\]"{f=1;next} /^## \[/{f=0} f' CHANGELOG.md)
for w in "topic" "group" "schema" "new tab\|middle-click"; do
  echo "$sec" | grep -qi "$w" || bad "[$V] section lacks '$w'"
done

cargo build 2>&1 | tail -2 || bad "cargo build"
cargo build >/dev/null 2>&1 || bad "cargo build failed"
cargo test --workspace >/dev/null 2>&1 || bad "cargo test --workspace failed"
[ $fail = 0 ] && echo "ok: release $V"
exit $fail
