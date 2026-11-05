# Dense event features for hybrid search

Dormant, rebuildable CH target; operator-reviewed schema/bootstrap/configuration is required. No runtime DDL or source DML is performed by the copier.

eventFeatureSchemaStatements() defines session_event_features and EVENT_FEATURE_PROJECTION_VERSION=1. The source API is generated from the reviewed RHC0076 journal. Every event is copied, including embedding=NULL; semantic queries filter vectors separately. Nullable project identity, exact source folds and UTC6 event/ingested timestamps are retained. The table has no TTL/window.

captureEventFeatureBaseline(db) captures only metadata. Its checkpoint must be durably CAS-persisted by the leased runtime BEFORE copyEventFeaturePage can write the first baseline page. NullH refuses. PK-keyset pages cover the full source; after baseline, journal catchup and final source CAS are required. Background source reads are readonly. Vector/model labels are copied, never regenerated or relabeled.

Every tombstone clears payload/vector fields, then deletes only exact old routing keys with version strictly below the tombstone. The current source reference is checked after acknowledged CH writes. A delayed baseline cannot overwrite a newer journal write; an old eraser cannot delete a later resurrection. Neither a helper returning nor an in-memory checkpoint authorizes public search: use the durable both-kind source/target/profile readiness runtime.

Target hash is the shared semantic-copy-target-v1 digest of normalized endpoint/database and both event/brief schema protocol hashes. Schema protocol hash covers [event-feature-projection-v1,...eventFeatureSchemaStatements()]. Source identity/profile changes require a fresh reviewed target/rebuild; runtime must not adopt nonempty unowned tables.

Acceptance evidence:

- Unit controls cover null vectors/null vs empty project, source identity/precision, BigInt revisions, durableH, source CAS, exact versioned erasure and failed writes.
- scripts/proof-event-features-real.ts accepts only owned loopback PG17/CH26 ports32770/32771 and its named synthetic database. It applies actual0076 SQL, runs the real copier and compares actual RHC legacy buildWhere SQL against the real CH visibility compiler. Includes1001 irrelevant briefs before the eligible old unembedded session, semantic full history, legacy keyword bounds, source mutations, erase/resurrection and moves.
- Run after supplying the isolated RHC checkout: bun scripts/proof-event-features-real.ts <RHC-checkout>3277032771 <relay-briefs/ch-event-feature-proof-20261103...>. The emitted proof records exact source/provider metadata and checks; it does not establish production coverage or throughput.

The28M-row full baseline and source journal catchup, current live writer coverage, role privileges, CH grants/schema, actual runtime load/latency and independent exact-head review/CI remain operator gates. The legacy source does not prove S3-only forwarded ingest; see RHC docs/ch-four-pool-search.md.
