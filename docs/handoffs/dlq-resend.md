# Per-message DLQ resend: usage

`scripts/dlq-resend.ts` (library `src/session-pipeline/dlq-resend.ts`, synthetic
tests `src/session-pipeline/__tests__/dlq-resend.test.ts`) resends only the
dead-lettered indexing messages whose session identity is selected. SQS
`StartMessageMoveTask` cannot select messages.

**The operator procedure (dry-run review, canary, monitoring, rollback) lives
in the private ops repository.** Running this against a real queue needs
Khaliq's separate, explicit OK (AGENTS.md). No queue, account or bucket
identifier belongs in this repository; all of them are run-time arguments.

```sh
bun scripts/dlq-resend.ts --dlq-url <dlq-url> --target-url <source-queue-url> --max <1..100> \
  (--select <hashes-file> [--classes <classes.json>] | --select-class <name> --classes <classes.json>) \
  [--rate 1] [--visibility-seconds 120] [--empty-receives 3] [--max-scan <n>] [--wait-seconds 2] \
  [--execute]   # also needs DLQ_RESEND_CONFIRM=resend; without --execute it is a dry run
```

- **Credentials:** all three of `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
  and `AWS_SESSION_TOKEN`, or none of them plus `AWS_PROFILE` (read through
  `aws configure export-credentials` and held in memory). Setting only some of
  the three is refused.
- **Queues:** only canonical `https://sqs.<region>.amazonaws.com/<account>/<name>`
  URLs. Before any receive, including a dry run, the tool checks that both
  QueueArns match their URLs and differ, that the target's `RedrivePolicy`
  dead-letters into this DLQ, and that the DLQ has no `RedrivePolicy` of its
  own.
- **Provenance:** a DLQ can serve several sources. A message is considered
  only if its `DeadLetterQueueSourceArn`, which the tool requests on every
  receive, is exactly the verified target's ARN. A message with no source
  (for example a direct insert) or another source is released at once and
  counted as `provenance_mismatch`. Such a message is never sent.
- **Attributes:** message attributes are carried over exactly, including names
  such as `__proto__`, with String, Number and Binary types and custom
  suffixes. If any attribute cannot be serialized and round-tripped unchanged
  (a list value, a missing or extra value, an unknown type, invalid base64), the
  message is not sent. It is released and counted as
  `attributes_unserializable`.
- **Identity:** `sha256_hex("<org>/<ws>/<m>/<h>/<s>")` of the manifest key the
  consumer itself reads from the body (`parseQueueBody`). Reconcile keys are
  used literally, and S3 keys are decoded as the consumer decodes them. A body
  is rejected, never selected, if it is a TestEvent, is malformed, has a record
  the consumer would drop, or mixes identities. Origin, retention and
  publication-hint keys, and keys that are not manifests, are also rejected.
- **An identity hash selects every message of that session**, not one body or
  revision. A dry-run sample is therefore not an exact execution roster.
- **Receiving messages**, in a dry run too, increments their receive count on
  the DLQ. The DLQ has no redrive policy (this is enforced), so the count
  triggers nothing.
- **Duplicate risk:** an unproven send halts the run, releases the rest of the
  batch and leaves the message in the DLQ. Unproven sends are a timeout, a lost
  response, an HTTP 5xx, or an acknowledgement without a `MessageId` or with an
  MD5 that does not match the body. A failed delete after a valid send also
  halts. So does a valid acknowledgement that arrives with less hold left than
  the delete timeout plus margin. In that case the tool does not delete,
  because the receipt may have lapsed and another receiver may hold the
  message. The summary's `reconcile` list names each such message by identity and
  body digest.
- **Output:** JSON lines with hashes and fixed codes only (class names are
  printed as `class_<hash>`). Exit 0 means done, 1 means errors or a duplicate
  risk, and 2 means refused or failed.
