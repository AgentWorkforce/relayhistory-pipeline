#!/bin/bash
# Every minute (launchd): admit Neon orgs created after the 05:51Z cutover into Supabase ingest admission,
# copying the live lazy policy (same epoch/baseline/retention/generation). Idempotent; never changes existing rows.
# Credentials: neonctl owner URL piped over stdin only.
set -o pipefail
: "${NEON_ENDPOINT:?NEON_ENDPOINT required}" "${NEON_PROJECT_ID:?NEON_PROJECT_ID required}" "${NEON_BRANCH_ID:?NEON_BRANCH_ID required}"
export PATH=/opt/homebrew/bin:/usr/bin:/bin
URL=$(neonctl connection-string "$NEON_BRANCH_ID" --project-id "$NEON_PROJECT_ID" --role-name neondb_owner --database-name neondb 2>/dev/null | tail -1)
case "$URL" in postgresql://neondb_owner:*"$NEON_ENDPOINT"*) ;; *) echo "$(date -u +%FT%TZ) bad owner url"; exit 1;; esac
printf '{"neonOwnerUrl":"%s"}\n' "$URL" | ssh -o BatchMode=yes -o ConnectTimeout=10 sf-frame \
  "cd /home/khaliqgant/Projects/AgentWorkforce/sessions-pipeline-worktrees/private-ingest-operator-final-462-20261104 && NEON_ENDPOINT=$(printf %q "$NEON_ENDPOINT") NEON_PROJECT_ID=$(printf %q "$NEON_PROJECT_ID") NEON_BRANCH_ID=$(printf %q "$NEON_BRANCH_ID") /home/khaliqgant/.local/share/mise/installs/bun/1.4.2/bin/bun ~/relay-briefs/bulls-resume-20261104/cutover-ceremony.ts --admit-new-orgs 2>/dev/null" \
  | grep -E '"admit-new-orgs"|STOPPED' | tail -1
