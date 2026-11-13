#!/bin/sh
# Runs inside the throwaway Blaxel sandbox (blaxel/docker-in-sandbox: Alpine, Docker and Compose, no bun), from the
# unpacked checkout: installs CI's pinned bun, the locked dependencies, and runs the pipeline e2e on the sandbox's
# own Docker. e2e/pipeline/sandbox.ts uploads the checkout, starts this, and reads the report back.
#
#   sh e2e/pipeline/sandbox-run.sh [--steps id,id,...]
set -eu
cd "$(dirname "$0")/../.."
want="$(sed -n 's/^ *bun-version: *//p' .github/workflows/ci.yml | head -1)"
if ! command -v bun >/dev/null 2>&1 || [ "$(bun --version)" != "$want" ]; then
  # The musl build (the image is Alpine); bun's own installer picks it.
  command -v bash >/dev/null 2>&1 || apk add --no-cache bash >/dev/null
  apk add --no-cache libstdc++ libgcc >/dev/null 2>&1 || true
  curl -fsSL https://bun.sh/install | bash -s "bun-v$want" >/dev/null
  export PATH="$HOME/.bun/bin:$PATH"
fi
command -v openssl >/dev/null 2>&1 || apk add --no-cache openssl >/dev/null
echo "bun $(bun --version); $(docker compose version); storage $(docker info --format '{{.Driver}}'); $(df -h /var/lib/docker | tail -1)"
bun install --frozen-lockfile >/dev/null
# The sandbox's Docker gives build containers no DNS; the host network has it.
export E2E_DOCKER_BUILD_NETWORK=host
exec bun run e2e/pipeline/run.ts --target local "$@"
