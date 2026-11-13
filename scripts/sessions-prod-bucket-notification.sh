#!/usr/bin/env bash
# The transcripts bucket's S3 event notification for the session pipeline
# (stacks/aws/session-pipeline.ts bucketNotificationConfiguration). Alchemy's
# AWS provider declares notifications only through function bindings, so this
# is the reviewed, checked-in way to set it; Alchemy keeps targets it does not
# manage. Runbook: sessions-pipeline-cloud:hosted-ops/preserved/runbooks/sessions-prod-session-pipeline.md.
#
#   scripts/sessions-prod-bucket-notification.sh            # print the wanted configuration (no AWS call)
#   scripts/sessions-prod-bucket-notification.sh --check    # read-only: compare with the live bucket; exit 1 on drift
#   NINETY_SIX_BULLS_CONFIRM_STAGE=aws-sessions-prod \
#     scripts/sessions-prod-bucket-notification.sh --apply  # write it (the queue and its policy must exist)
#   NINETY_SIX_BULLS_CONFIRM_STAGE=aws-sessions-prod \
#     scripts/sessions-prod-bucket-notification.sh --remove # rollback: stop notifications (sets an empty configuration)
#
# Needs SESSIONS_PROD_ACCOUNT_ID (the sessions-prod account id; the script refuses
# without it) and AWS_PROFILE=sessions-prod (or exported credentials) for that account.
#
# HISTORY_UPLOAD_DIRECT_INFRA=on (default off; the same flag as the stack, upload-inbox.ts) adds
# the second rule, `96bulls-upload-inbox`, to the wanted configuration: this script owns both Ids
# and writes them in one put. With the flag off, --apply refuses while the live bucket still carries
# `96bulls-upload-inbox` (that apply would silently drop desktop-upload admission) unless
# NINETY_SIX_BULLS_CONFIRM_DROP_UPLOAD_INBOX=yes says the drop is the rollback being made.
set -euo pipefail
cd "$(dirname "$0")/.."

STAGE=aws-sessions-prod
ACCOUNT="${SESSIONS_PROD_ACCOUNT_ID:?SESSIONS_PROD_ACCOUNT_ID required (the sessions-prod AWS account id)}"
if ! [[ "$ACCOUNT" =~ ^[0-9]{12}$ ]]; then echo "refused: SESSIONS_PROD_ACCOUNT_ID must be a 12-digit AWS account id" >&2; exit 2; fi
REGION=us-east-1
BUCKET="96bulls-${STAGE}-transcripts-${ACCOUNT}"
UPLOAD_DIRECT="${HISTORY_UPLOAD_DIRECT_INFRA:-off}"
case "$UPLOAD_DIRECT" in
  on | off) ;;
  "") UPLOAD_DIRECT=off ;;
  *) echo "refused: HISTORY_UPLOAD_DIRECT_INFRA must be on or off (or unset)" >&2; exit 2 ;;
esac
mode="${1:---print}"

wanted() {
  bun -e '
    import { bucketNotificationConfiguration, sessionPipelineNames } from "./stacks/aws/session-pipeline.ts";
    import { uploadInboxNames } from "./stacks/aws/upload-inbox.ts";
    const n = sessionPipelineNames(process.argv[1], process.argv[2], process.argv[3]);
    const upload = process.argv[4] === "on" ? uploadInboxNames(process.argv[1], process.argv[2], process.argv[3]).queueArn : undefined;
    console.log(JSON.stringify(bucketNotificationConfiguration(n.queueArn, upload)));
  ' "$STAGE" "$ACCOUNT" "$REGION" "$UPLOAD_DIRECT"
}

account_guard() {
  local caller
  caller="$(aws sts get-caller-identity --query Account --output text)"
  if [ "$caller" != "$ACCOUNT" ]; then
    echo "refused: credentials are for account $caller, not $ACCOUNT" >&2
    exit 2
  fi
}

live() {
  local raw
  raw="$(aws s3api get-bucket-notification-configuration --bucket "$BUCKET" --region "$REGION" --output json)"
  # An unconfigured bucket answers with an empty body.
  [ -n "$raw" ] || raw='{}'
  printf '%s' "$raw" \
    | jq -c '{QueueConfigurations: (.QueueConfigurations // []), TopicConfigurations: (.TopicConfigurations // []), LambdaFunctionConfigurations: (.LambdaFunctionConfigurations // []), EventBridgeConfiguration: .EventBridgeConfiguration}'
}

normalize() {
  jq -cS '{QueueConfigurations: [(.QueueConfigurations // [])[] | {Id, QueueArn: (.QueueArn), Events: (.Events | sort), Filter: {Key: {FilterRules: [.Filter.Key.FilterRules[] | {Name: (.Name | ascii_downcase), Value}] | sort_by(.Name)}}}] | sort_by(.Id), others: ((.TopicConfigurations // []) + (.LambdaFunctionConfigurations // []) | length), eventBridge: (.EventBridgeConfiguration != null)}'
}

case "$mode" in
  --print)
    wanted | jq .
    ;;
  --check)
    account_guard
    want="$(wanted | normalize)"
    have="$(live | normalize)"
    if [ "$want" = "$have" ]; then
      echo "ok: $BUCKET notifies exactly the wanted queue rules (upload inbox: $UPLOAD_DIRECT)"
    else
      echo "drift on $BUCKET:" >&2
      diff <(echo "$want" | jq .) <(echo "$have" | jq .) >&2 || true
      exit 1
    fi
    ;;
  --apply | --remove)
    if [ "${NINETY_SIX_BULLS_CONFIRM_STAGE:-}" != "$STAGE" ]; then
      echo "refused: set NINETY_SIX_BULLS_CONFIRM_STAGE=$STAGE to change the bucket's notifications" >&2
      exit 2
    fi
    account_guard
    # The bucket must carry no notification we do not own: put replaces the whole configuration.
    foreign="$(live | jq '([(.QueueConfigurations[] | select(.Id != "96bulls-session-pipeline-manifests" and .Id != "96bulls-upload-inbox")), .TopicConfigurations[], .LambdaFunctionConfigurations[]] | length) + (if .EventBridgeConfiguration != null then 1 else 0 end)')"
    if [ "$foreign" != "0" ]; then
      echo "refused: $BUCKET has $foreign notification target(s) this script does not own; merge them by hand" >&2
      exit 2
    fi
    # Flag off but the live bucket still routes desktop uploads: dropping that rule strands every
    # upload in the inbox. Only an explicit rollback may do it.
    if [ "$mode" = "--apply" ] && [ "$UPLOAD_DIRECT" = "off" ] && [ "${NINETY_SIX_BULLS_CONFIRM_DROP_UPLOAD_INBOX:-}" != "yes" ]; then
      upload_live="$(live | jq '[.QueueConfigurations[] | select(.Id == "96bulls-upload-inbox")] | length')"
      if [ "$upload_live" != "0" ]; then
        echo "refused: $BUCKET carries the 96bulls-upload-inbox rule and HISTORY_UPLOAD_DIRECT_INFRA is off; set it to on, or NINETY_SIX_BULLS_CONFIRM_DROP_UPLOAD_INBOX=yes to drop the rule" >&2
        exit 2
      fi
    fi
    if [ "$mode" = "--apply" ]; then config="$(wanted)"; else config='{}'; fi
    aws s3api put-bucket-notification-configuration --bucket "$BUCKET" --region "$REGION" --notification-configuration "$config"
    echo "done: $mode on $BUCKET; verify with --check"
    ;;
  *)
    echo "usage: $0 [--print|--check|--apply|--remove]" >&2
    exit 2
    ;;
esac
