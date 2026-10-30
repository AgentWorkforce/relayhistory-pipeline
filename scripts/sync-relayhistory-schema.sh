#!/usr/bin/env bash
# Pin the RelayHistory (Neon) schema migrations from AgentWorkforce/relayhistory-cloud
# into vendor/relayhistory-cloud/migrations, for the schema-parity test.
# Usage: scripts/sync-relayhistory-schema.sh <path-to-relayhistory-cloud-checkout> <ref>
set -euo pipefail
src=${1:?relayhistory-cloud checkout}
ref=${2:?git ref}
dir=packages/relayhistory/db/migrations
dest=vendor/relayhistory-cloud/migrations
sha=$(git -C "$src" rev-parse --verify "$ref^{commit}")
rm -rf "$dest"
mkdir -p "$dest"
git -C "$src" ls-tree --name-only "$sha" "$dir/" | while read -r path; do
  case "$path" in *.sql) git -C "$src" show "$sha:$path" > "$dest/${path##*/}" ;; esac
done
printf 'AgentWorkforce/relayhistory-cloud %s:%s\n' "$sha" "$dir" > vendor/relayhistory-cloud/SOURCE
echo "pinned $(ls "$dest" | wc -l | tr -d ' ') migrations at $sha"
