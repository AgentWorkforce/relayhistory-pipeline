# Per-message DLQ resend

`scripts/dlq-resend.ts` (library: `src/session-pipeline/dlq-resend.ts`) resends
chosen messages from the indexing dead-letter queue back to the indexing queue.
SQS `StartMessageMoveTask` moves a DLQ in queue order and cannot pick messages;
this tool selects by session identity, so identities that now index can be
resent while identities still failing with `pointer_move_pending` stay put.

Running it against a real queue is an operator action that needs Khaliq's
separate, explicit OK (AGENTS.md). No queue URL, account or bucket is written
in this repository: everything is supplied at run time.

## Identity and selection

The identity of a message is
`sha256_hex(utf8("<org>/<ws>/<m>/<h>/<s>"))`, taken from the S3 object key in
the body after URL-decoding it (keys arrive encoded, e.g. `ws%3D...`):

- the org is the segment after the key root (the last segment of the key's
  deployment prefix, the same rule as the pipeline's prefix org resolver);
- `ws=`, `m=`, `h=` and `s=` contribute their values without the labels;
  the `d=` date is not part of the identity.

Bodies are either S3 events (`Records[0].s3.object.key`; a multi-record event
must name one identity) or reconcile bodies (`{"reconcile":{"key":...}}`). The
sessionless origin bucket (`h=_none`) has no identity and is never resent.
Bodies with no session key are released, never resent.

Selection is one of:

- `--select <file>`: one bare identity hash per line (`#` comments allowed);
  optionally `--classes <file>` to label the output;
- `--select-class <name> --classes <file>`: the classes file is a JSON object
  mapping identity hash to class name; every hash of that class is selected.

## Usage

```sh
# 1. Dry run (the default). Credentials: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY /
#    AWS_SESSION_TOKEN in the environment, else AWS_PROFILE through
#    `aws configure export-credentials`. Region comes from the queue URL.
bun scripts/dlq-resend.ts --dlq-url "$DLQ_URL" --target-url "$TARGET_URL" \
  --select-class now_indexes --classes classes.json --max 10

# 2. Execute (same arguments plus --execute and the confirmation variable).
DLQ_RESEND_CONFIRM=resend bun scripts/dlq-resend.ts --dlq-url "$DLQ_URL" \
  --target-url "$TARGET_URL" --select-class now_indexes --classes classes.json \
  --max 10 --execute
```

| Flag | Default | Notes |
| --- | --- | --- |
| `--dlq-url`, `--target-url` | required | refused when equal (case and trailing slash ignored) or FIFO |
| `--select` / `--select-class` | one required | mutually exclusive; an empty selection is refused |
| `--max` | required | 1 to 100; counts resend attempts, including failed sends |
| `--rate` | 1 | sends per second, at most 5 |
| `--visibility-seconds` | 120 | the hold on each received message while it is decided (30 to 43200) |
| `--empty-receives` | 3 | stop after this many consecutive receives with nothing new |
| `--max-scan` | 2x size | messages seen before stopping; default twice the DLQ's `ApproximateNumberOfMessages` at start, at least 10 |
| `--wait-seconds` | 2 | ReceiveMessage long poll |
| `--execute` | off | also needs `DLQ_RESEND_CONFIRM=resend` |

What it does per received message:

- selected and under the cap, execute: `SendMessage` of the identical body and
  message attributes to the target (no `DelaySeconds`, so the target queue's
  delay applies); only after the send succeeds, `DeleteMessage` from the DLQ;
- anything else: `ChangeMessageVisibility` to 0 at once, so it is not held;
- dry run: never sends or deletes; every message goes back to visibility 0 and
  the output lists what would be resent.

A message is decided at most once per run (released messages come straight
back; repeats are released again and not counted as new). A receive that
returns nothing new counts as empty for the stop rule.

Exit status: 0 done, 1 finished with errors or a duplicate risk, 2 refused or
failed before or during the scan.

## Output

JSON lines on stdout, hashes only: never a key, queue URL, org, workspace or
session id.

- `start`: mode, cap, rate, hold, the DLQ's approximate size, the scan bound;
- `would_resend` / `resent`: `identity`, `bodySha256` (first 16 hex chars of
  the body's sha256), `class`; a resent line with `duplicateRisk: true` means
  the send succeeded but the delete failed;
- `send_failed` / `send_skipped`: same fields plus an AWS error code;
- `summary`: `scanned`, `unique`, `selected`, `resent`, `wouldResend`,
  `skipped` (with `skipReasons`), `errors` (with `errorCodes`),
  `duplicateRisk`, `perClass`, and the `stop` reason (`cap_reached`,
  `empty_receives`, `max_scan`, `receive_failed`).

## Safety

- A send failure leaves the message in the DLQ (no delete); it reappears when
  its hold ends and is counted as an error.
- A delete failure after a successful send is counted as `duplicateRisk`. The
  message is still in the DLQ, so a later run could resend it again. The
  pipeline is idempotent on the same key and revision, so a duplicate is
  harmless, but the count must be read.
- A send is not started when less than 10 seconds of the hold remain (a slow
  `--rate` with a short `--visibility-seconds`); that message stays in the
  DLQ and is counted as `hold_expiring`.
- Receiving increments each message's `ApproximateReceiveCount` on the DLQ,
  including in a dry run. The DLQ has no redrive policy of its own, so the
  count triggers nothing and is harmless. The resent copy is a new message on
  the target with a fresh receive count, so it gets the target's full
  `maxReceiveCount` again.

## Dry run first

Always run the exact command without `--execute` first and read the output:
the `would_resend` lines must match the hashes you intended, and the summary's
`selected` and `perClass` must match the class sizes you expect. Do not
execute when the dry run shows unexpected selections, `receive_failed`, or a
scan that stopped on `max_scan` before seeing the selected messages.

## Canary procedure

1. Dry run the selection (above).
2. Execute with a small `--max` (for example 5) and `--rate 1`.
3. Watch, for at least the target queue's full retry window (its visibility
   timeout times `maxReceiveCount`): the consumer's `pointer_move_pending`
   errors for the resent identities, and the DLQ's size. A resent identity
   that fails again returns to the DLQ.
4. Only if the canary identities indexed (no new `pointer_move_pending` for
   them and they did not return to the DLQ), repeat with a larger `--max`, in
   steps, up to 100 per run.
5. Stop at the first sign of a regression: resent messages returning to the
   DLQ, a rise in consumer errors, or any `duplicateRisk`.

## Rollback

Nothing is lost by a resend. A resent message that still fails is retried by
the indexing queue up to its `maxReceiveCount` and then dead-lettered back to
the DLQ, where it waits as before. Stopping the tool mid-run is safe: a held
message reappears in the DLQ when its hold ends. To roll back, stop resending;
there is no state to undo.
