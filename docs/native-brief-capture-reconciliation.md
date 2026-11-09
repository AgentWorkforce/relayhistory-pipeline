# Canonical brief capture reconciliation

The canonical brief adapter reconciles a raw `history` message with its projected
capture before normalizing the model input. It pairs matching speaker and recorded
text within two seconds, one to one, and keeps records with conflicting message,
user or tool-call identities. Source, session, tenant and subagent boundaries are
preserved. Repeated projected prompts and unmatched history records remain real
messages. Raw canonical objects and event projections are never rewritten.

Canonical generation records the input engine version
`session-summary-v3:96bulls-canonical-v2`. With canonical input and the normal
periodic sweep enabled, an existing ready native brief with an older input version
is eligible for one normal wakeup even when its pointer revisions have not changed.
The worker cannot reuse an old input version merely because its fingerprint happens
to match. Once the current version is published, the version sweep is a no-op.

Refresh uses the existing workspace selection, bounded sweep and worker batch,
wakeup/lease deduplication, source-revision publication fences, throttle, and
per-session, organization and global admission caps. It does not clear attempts,
change pointers, or issue a model request outside normal admission. A disabled
sweep does not refresh stored output automatically. A cap, retry delay or throttle
can defer refresh through the existing worker lifecycle.

The raw-transcript/hosted adapter retains its existing engine version and sweep
behavior. Non-native/backfilled and failed outputs are not rearmed solely for this
canonical version upgrade. This source change does not run a backfill, regenerate
stored text, change roles or apply a database migration.
