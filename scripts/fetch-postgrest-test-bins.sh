#!/usr/bin/env bash
# Downloads pinned, checksummed Postgres 17 and PostgREST for
# integration/session-directory-postgrest into the directory given as $1, then prints
# the env the suite needs. Docker is not required.
set -euo pipefail
out=${1:?usage: fetch-postgrest-test-bins.sh <directory>}
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64)
    pg_pkg=linux-x64
    pg_sha=5296077a6773d77fa3003b004b7d59e9df2fed0985ecdd5fcf4fcc94c75d4f18
    rest_pkg=linux-static-x86-64
    rest_sha=5099e7ada38f9f6846cd87c14c5f158b7c165f1e9959665ad412e61f4dceba95
    ;;
  Darwin-arm64)
    pg_pkg=darwin-arm64
    pg_sha=d290b5793619fc909e1616fc4520c95fe6dff8a762aa6bd60f615bbdbbfb73a5
    rest_pkg=macos-aarch64
    rest_sha=d8095cb439de0a3151036df542139e7afca5cebb109ce7f1bae52b3a97e9846a
    ;;
  *) echo "unsupported platform: $(uname -s)-$(uname -m)" >&2; exit 1 ;;
esac
pg_version=17.11.0-beta.19
rest_version=v16.5
mkdir -p "$out/pg" "$out/postgrest"
check() { printf '%s  %s\n' "$1" "$2" | shasum -a 256 -c - >&2; }

pg_tgz="$out/pg.tgz"
curl -fsSL -o "$pg_tgz" "https://registry.npmjs.org/@embedded-postgres/$pg_pkg/-/$pg_pkg-$pg_version.tgz"
check "$pg_sha" "$pg_tgz"
tar -xzf "$pg_tgz" -C "$out/pg"
rest_txz="$out/postgrest.tar.xz"
curl -fsSL -o "$rest_txz" "https://github.com/PostgREST/postgrest/releases/download/$rest_version/postgrest-$rest_version-$rest_pkg.tar.xz"
check "$rest_sha" "$rest_txz"
tar -xJf "$rest_txz" -C "$out/postgrest"

# npm tarballs cannot hold symlinks, so the package lists them.
python3 -I - "$out/pg/package" <<'PY'
import json, os, sys
base = sys.argv[1]
for link in json.load(open(os.path.join(base, "native/pg-symlinks.json"))):
    path = os.path.join(base, link["source"])
    if not os.path.lexists(path):
        os.symlink(os.path.relpath(os.path.join(base, link["target"]), os.path.dirname(path)), path)
PY

abs=$(cd "$out" && pwd)
echo "PG_BIN_DIR=$abs/pg/package/native/bin"
echo "POSTGREST_BIN=$abs/postgrest/postgrest"
# The macOS PostgREST build links libpq; the Postgres package carries a copy.
echo "DYLD_FALLBACK_LIBRARY_PATH=$abs/pg/package/native/lib"
