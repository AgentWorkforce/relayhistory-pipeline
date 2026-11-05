#!/bin/bash
# Keep the lead's own Claude session shared to Agent Relay Cloud. This host's
# sharing mode is "selected" (only sessions chosen by hand upload, so
# scratch sessions under ~/work never do), which means a new lead session,
# after a restart or a rebuild, uploads only once someone includes it. Any
# other mode needs nothing, and this exits quietly. This includes it: the newest session of the lead's checkout, every
# few minutes (agent-host-share-lead.timer), idempotently. A failure is
# loud in the journal; a missing session (first boot) is not a failure.
set -euo pipefail
PROBE=/usr/lib/agent-relay/agent-relay-probe
PROJECT="$HOME/.claude/projects/-home-agent-src-sessions-pipeline"
latest="$(find "$PROJECT" -maxdepth 1 -name '*.jsonl' -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | cut -d' ' -f2- || true)"
if [ -z "$latest" ]; then
  echo "share-lead-session: no lead session yet in $PROJECT"
  exit 0
fi
session="claude:$(basename "$latest" .jsonl)"
install="$("$PROBE" sharing status --json | python3 -c '
import json, sys
installs = json.load(sys.stdin).get("installs", [])
if len(installs) != 1:
    sys.exit("share-lead-session: expected one install, found %d" % len(installs))
i = installs[0]
print(i["site_url"], i["account_id"], i["workspace_id"], i["sharing_mode"])
')"
read -r site account workspace mode <<<"$install"
if [ "$mode" != "selected" ]; then
  echo "share-lead-session: sharing mode is $mode, nothing to select"
  exit 0
fi
if ! out="$("$PROBE" sessions include --site-url "$site" --account "$account" --workspace "$workspace" --session "$session" --json 2>&1)"; then
  echo "share-lead-session: FAILED to include $session: $out" >&2
  exit 1
fi
echo "share-lead-session: $session included ($out)"
