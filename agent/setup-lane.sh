#!/bin/bash
# sessions-pipeline's lane setup, run as root at the end of the kit's setup.sh
# (first boot, and again by every hot update). Idempotent.
#  - Node >= 24.11.1 and bun 1.4.2, the versions sessions-pipeline's engines
#    and CI pin (the kit installs Node 22).
#  - git rewrites git@github.com: to https for the agent user, so the
#    vendor/relayhistory submodule fetches with the host's GitHub token.
#  - Both repos in ~/src (the lead's checkouts) and ~/build (clean reference
#    clones for the ledger and for `git clone --reference`). The kit's
#    load-secrets.sh clones them whenever a GitHub token is available; this
#    retries any that are missing. Dependencies are installed once, in a
#    checkout that has none.
#  - The lead's session kept shared to Cloud every 5 minutes
#    (share-lead-session.sh), since the host shares selected sessions only.
set -euo pipefail
# shellcheck source=/dev/null
. /opt/agent-host/env

node_ok() { command -v node >/dev/null && node -e 'const [a,b,c]=process.versions.node.split(".").map(Number); process.exit(a>24||(a===24&&(b>11||(b===11&&c>=1)))?0:1)'; }
if ! node_ok; then
  curl -fsSL https://deb.nodesource.com/setup_24.x | bash -
  apt-get install -y nodejs
  node_ok || { echo "error: node $(node --version) is older than 24.11.1 after the upgrade" >&2; exit 1; }
fi
if [ "$(bun --version 2>/dev/null || true)" != 1.4.2 ]; then
  npm install -g bun@1.4.2
fi

sudo -H -u agent git config --global url."https://github.com/".insteadOf git@github.com:

for repo in ${AGENT_REPOS:-}; do
  name=${repo##*/}
  if [ -s /run/agent-host/gh-token ]; then
    sudo -H -u agent bash -lc "mkdir -p ~/src ~/build
      [ -d ~/src/$name/.git ] || gh repo clone '$repo' ~/src/$name
      [ -d ~/build/$name/.git ] || gh repo clone '$repo' ~/build/$name" \
      || echo "warning: could not clone $repo; load-secrets.sh retries every 30 minutes" >&2
  fi
  # Once, in a checkout that has none (never under work the lead has in
  # progress). A failure only warns: the lead can run the install itself.
  sudo -H -u agent bash -lc "cd ~/src/$name 2>/dev/null && [ ! -d node_modules ] || exit 0
    if [ -f bun.lock ]; then bun install --frozen-lockfile
    elif [ -f package-lock.json ]; then npm ci --no-audit --no-fund
    fi" || echo "warning: dependency install failed in ~/src/$name" >&2
done

cat > /etc/systemd/system/agent-host-share-lead.service <<'UNIT'
[Unit]
Description=sessions-pipeline lead session kept shared to Agent Relay Cloud
After=agent-relay.service
[Service]
Type=oneshot
User=agent
Environment=HOME=/home/agent
ExecStart=/opt/agent-host/share-lead-session.sh
UNIT
cat > /etc/systemd/system/agent-host-share-lead.timer <<'UNIT'
[Unit]
Description=sessions-pipeline lead session share check every 5 minutes
[Timer]
OnCalendar=*:00/5
RandomizedDelaySec=30
Persistent=true
[Install]
WantedBy=timers.target
UNIT
systemctl daemon-reload
systemctl enable --now agent-host-share-lead.timer
