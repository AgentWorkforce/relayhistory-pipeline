# Dormant canonical replay pages

The Node session pipeline can materialize the complete canonical source union outside workerd. `SessionPipelineInputs.replayPages` and `SESSION_REPLAY_PAGES_ENABLED=1` select the optional stage; both default off. It follows pointer indexing in the existing event consumer and publishes through backend RPCs. A write, read-back, hydration or source-fence error fails the source event for normal queue retry. No live configuration, deployment, credential or grant is changed by this source implementation.

## Artifact v1

`src/session-read-api/canonical-artifact.ts` is the shared producer/consumer protocol. Hashes are lowercase SHA256 of the exact stored bytes, without decompression or reserialization. JSON objects are UTF8. Every root/node/page is at most256KiB, row pages contain at most1000 rows, tree fanout is128 and maximum depth is4. Half-open ordinal ranges cover rows, bytes or UTF16 code units, according to the node unit. All nodes/pages carry canonical identity and generation; row trees also carry table/view. The root lists each table/view's exact count and range-tree reference, including explicit zero counts. Hierarchy keeps the root bounded for hundreds of thousands of realistic rows rather than collecting every page descriptor there.

Raw, replay and bounds event views use the authoritative shared projection. Replay retains all flat fields and only the native message_id under record.payload; bounds retains the existing identity/time fields. Legacy tables retain complete payloads and raw primary keys, including BIGSERIAL strings. Events sort globally by exact microsecond timestamp, event_id, machine_id and kind bytes. Legacy link IDs compare as integers, not lexicographic strings. Legacy coverage stays null unless the actual source stream contains a verified explicit coverage record. SQL-oracle timing is materialized only when legacy coverage is known.

Oversized replay content is an out-of-band JSON-string segment tree; JSON preserves every UTF16 unit, including lone surrogates. Adaptive segments have at most64KiB encoded JSON bytes and65536 UTF16 units. A maxContent reader can hydrate only the required prefix plus one unit; a full read also verifies the whole string's JSON hash. Oversized arbitrary nested arrays/objects use exact JSON-byte segments and a SHA-bound byte tree. User payload fields are never interpreted as codec sentinels. Mandatory non-content fields remain complete. The consumer must refuse a row exceeding its explicit assembly bound; segmentation is not permission to lift Worker memory limits or silently truncate fields.

Keys are under the lexicographically first live physical manifest's prefix:

`<physical-prefix>/normalized/read-projection-v1/<anchor-raw-chain-sha>/<artifact-generation>/<object-sha>.json|bin`

The anchor raw hash is deliberately present: existing exporter orphan cleanup retains normalized keys containing its current raw hash and erases older raw-hash generations with the prefix. A source mutation invalidates publication before later old-prefix cleanup. Immutable unreferenced objects after failed CAS are safe orphans; cleanup/deletion permissions remain a separate operational responsibility. Same-anchor-hash generations can remain unreferenced until a later raw-hash change; a safe generation GC/retention plan is still an activation dependency.

## Source capture and publication

The raw Worker's16MiB/100k/64-pointer boundary is unchanged. A separate trusted Node snapshot uses256MiB raw input,1M actual parsed records and at most10000 complete pointers. Actual source reads enforce chunk counts/digests, bounded decompression, canonical identity, admitted user ownership, optional catalog ETag, accepted_seq, per-origin revisions, tombstones, separate legacy domains and the full physical/canonical alias union through the same shared projection/fold. Output rows are capped at2M and unique stored artifact bytes at2GiB; exceeding a bound refuses publication.

The raw cursor source-generation helper is unchanged and exported. Artifact generation is explicitly domain-separated as SHA256 of JSON.stringify(["canonical-read-artifact-v1",rawSourceGeneration,epoch,sourceFingerprint]). Canonical artifact semantic changes require a version/domain bump. Cursors also bind indexSHA so rebuilt content cannot be mixed across pages.

`20261103040000_session_replay_pages.sql` adds backend capture/publication RPCs and a compact claim-bound read RPC. Source fingerprint is SHA256 over PostgreSQL's exact UTF8 JSONB text of the captured source snapshot; callers carry this opaque fingerprint rather than reproducing PostgreSQL serialization. Snapshot includes all live pointer tuples, physical/canonical identities, revisions, raw-chain digests, owners, storage/bucket/format/ETag/count metadata, persistent pending epochs and directory epoch/tombstone. A NO CYCLE global sequence drives persistent per-canonical-scope epochs; erasure/recreation and return-to-identical values cannot recreate an old epoch. Source-field updates invalidate publication; classifier-only writes do not strand artifacts awaiting an S3 event they never produce.

The producer hydrates and rechecks the captured union, writes immutable objects create-only, checks every put acknowledgement, then reads back and hashes every object. Only then may publish_session_replay_pages CAS the exact source snapshot/fingerprint/epoch under the same row lock acquired by source invalidation triggers. It also enforces the exact current anchor prefix and index-SHA key. Stale, erased, directory-tombstoned, foreign-scope or incomplete work publishes nothing current.

session_directory_replay_pages(workspace,harness,session) derives org from the existing signed search-worker policy and grants that role only this RPC. There are no new table grants. It distinguishes authenticated absence (exists:false) from an existing source without current publication (exists:true,current:false). The latter must be503 in page mode. Current responses acknowledge exact identity, epoch, source fingerprint, generation, normalized prefix and index key/SHA/byte count. The Worker rechecks this acknowledgement after bounded hydration and has no raw/Neon fallback in selected page mode.

## Resource and rollout dependencies

Current checked-in AWS pipeline policy grants List/Get only; it explicitly does not grant Put. The new source code does not alter that policy or the transcript key policy. Before any enablement, the operator must review/capture scoped PutObject on the read-projection normalized namespace, GetObject for read-back, and the bucket's applicable SSE-KMS GenerateDataKey/Decrypt permissions. No Delete, old-version access, raw overwrite, broad bucket write or other secret is required by this producer. The Worker needs only authenticated claim RPC and scoped Get for these artifact objects; its current raw-read credential/binding must be proven to cover that namespace and encryption, not assumed.

Apply the reviewed additive migration and exact ACL/boundary/role-verifier/generation union before enabling the producer, then prove its backend role can call only the intended RPCs. Current Lambda is Node24/3008MiB; the synthetic native-Node proof records total RSS/time including PGlite overhead, and does not establish deployed memory/latency. Capacity exceeding the Node budget requires a genuinely streaming producer or separately reviewed resource plan, never larger workerd source limits. Existing ClickHouse indexing still has its own64MiB/100k source bound; this artifact stage does not make its needs_full gate green.

The consumer's explicit page mode, deployed producer/consumer versions, complete replay/coverage, future legacy writer fence/redirection, new eraser-before-exporter ordering, obsolete-object reconciliation, full public adapter parity and the complete migration/ACL/bootstrap union remain separate activation gates. Rollback disables the page consumer and producer flags; immutable artifacts/publication metadata need not be deleted to stop traffic. No enablement is part of this change.

## Offline proof

The proof uses synthetic PGlite and a new empty local fs directory, with no service credentials:

```
bun run --filter @relayhistory/object-store build
node --experimental-transform-types --import ./scripts/proof-typescript-loader.mjs \
  ./scripts/proof-canonical-replay-pages.ts --events 300001 --out <new-empty-local-directory>
```

It exports actual source records with content/native IDs/tool metadata/counters across machines, then invokes the actual producer and SQL publication. proof.json/claim.json contain only synthetic metadata. Real offline workerd reads must exercise these bounded page objects rather than embedding the whole fixture in the isolate. Focused source tests cover write/read-back failure, source races, owner/digest/revision/alias/deletion CAS, ABA, >64 pointers, workspace claims and absent-versus-unpublished semantics. Mutation controls and exact capacity results belong in the PR evidence; a green suite alone cannot establish omitted cases.
