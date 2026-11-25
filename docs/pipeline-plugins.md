# Pipeline plugins

Design proposal, 2026-11-25. Requested by Khaliq Gant. This document defines a
customer-usable plugin contract for the sessions pipeline. Relayfacts is the
first plugin that must implement it. The document ships no code, migrations,
flags, credentials, cloud resources or deployments, and enables nothing.

The current behavior this contract must preserve is documented in
[Relayfacts](relayfacts.md), [Relayfacts GitHub push](relayfacts-github-push.md),
[live coordination advisories](coordination-live-advisories.md) and the
[operator per-session erase contract](operator-session-erase-contract.md).

## Purpose and boundaries

A pipeline plugin turns a bounded selection of one workspace's session data
into versioned records that the same workspace can read or send to an optional
destination. A plugin is configuration over pipeline-owned isolation,
scrubbing, execution, storage, read and erase boundaries; it is not arbitrary
code loaded into a pipeline process.

The contract has five parts:

1. an input selector;
2. an extractor, either managed by the pipeline or hosted by the customer;
3. versioned output records;
4. generated HTTP and MCP reads; and
5. optional sinks, such as a GitHub repository.

The first implementation should support a small, reviewed subset of this
contract. In particular, accepting a manifest must not by itself activate a
plugin, authorize egress or spend model tokens. Every installation and each
optional capability stay default off until separately enabled for a canary
workspace.

This design does not make raw transcripts, credentials, cross-workspace data,
arbitrary SQL, arbitrary network access or arbitrary in-process code available
to a plugin.

## Terminology and identities

- A **plugin release** is an immutable manifest identified by `plugin.id` and
  `plugin.version`.
- An **installation** binds one plugin release to one workspace. Runtime
  controls, quotas, signing material and sink credentials belong to the
  installation and are not stored in the manifest.
- A **run** evaluates one installation over a bounded input revision.
- A **record identity** names one logical result within `(workspace, plugin,
  topic)`. The pipeline also scopes every internal read and write by the owning
  organization. Identity is distinct from a database row UUID, topic and
  revision.
- A **record revision** is one immutable representation of that identity. A
  current-view store may have no revision history at all.
- A **topic** is a plugin-defined, bounded slug used to group records. It is
  data, not an authorization boundary.
- A **plugin version** and **output-schema version** describe the code and
  shape that produced a record. Neither is a record identity or revision.

Plugin ids and topics are stable lowercase slugs. A released manifest is
immutable: a prompt, schema, selector or sink-policy change requires a new
plugin version. This keeps re-runs reproducible and prevents a manifest edit
from silently changing an existing installation.

## Declarative manifest

The portable unit is a versioned, declarative manifest. The example below is
illustrative; the first implementation must publish a JSON Schema and reject
unknown fields before persisting or running a manifest.

```yaml
apiVersion: sessions.agentrelay.dev/v1alpha1
kind: PipelinePlugin
plugin:
  id: relayfacts
  version: 1.0.0
  displayName: Relayfacts

input:
  sessions:
    state: [completed]
    sources: [codex, claude]
    lookback: P30D
  events:
    kinds: [analysis_snapshot]
  projection: session-analysis/1

extractor:
  mode: managed
  managedOutputRef: session-analysis/understanding.decisions/1
  outputSchemaRef: plugin://relayfacts/record/1
  modelClass: inherited-from-projection

output:
  schemaVersion: 1
  topic:
    source: extractor
    pattern: '^[a-z0-9][a-z0-9-]{0,63}$'
  records:
    maxPerRun: 100
    maxBytes: 262144
  retentionClass: derived-session-evidence

exposure:
  http:
    enabled: false
  mcp:
    enabled: false

sinks: []
```

The manifest contains no secret values, workspace ids or environment-specific
credentials. References such as `promptRef`, `outputSchemaRef` and a sink
configuration name resolve only to versioned resources the installation is
allowed to use. A manifest cannot choose its database role, network policy,
model credential or signing key.

### Input selector

The input section says which data is relevant; it never grants access. The
runtime intersects it with the installation's single organization and
workspace, the authenticated source catalog, erasure fences, retention policy
and a bounded run window.

A selector can constrain:

- session lifecycle state, source, project and bounded activity window;
- event kinds and roles from a pipeline-defined projection;
- whether the extractor is invoked per event, per session or for a bounded
  workspace window; and
- a stable input projection version.

Only named projections are selectable. A projection defines the fields,
bounds and ordering visible to an extractor. `raw_transcript` is not a
customer-selectable projection. Selectors have server-side maximum lookback,
session count, event count and byte bounds; a manifest may narrow those bounds
but cannot widen them.

Runs bind a concrete input cursor or source revision. New events after that
point belong to a later run. Deleted or fenced sessions are excluded both when
the run is planned and immediately before output commit.

### Extractors

Two extractor modes implement the same bounded request and response contract.

#### Managed extractor

`mode: managed` names an immutable managed prompt and/or a versioned structured
output schema. It may invoke the pipeline's approved model for this run, or
consume a named structured projection already produced by that model;
relayfacts uses the latter through the existing session-analysis snapshot. The
pipeline enforces structured output, token and cost ceilings where a call is
made, and records the prompt, schema, model and source-projection revisions as
provenance. A manifest cannot inject system instructions or choose an
unapproved endpoint. Whether customers may supply prompt text at all is an
open product decision; the safe first slice is pipeline-reviewed prompts and
customer-selected schemas from an allowlist.

The managed extractor sees only the scrubbed input projection. It returns the
same response envelope as a customer-hosted extractor and has no direct
database or sink access.

#### Customer-hosted webhook or worker

`mode: webhook` invokes an HTTPS endpoint; `mode: worker` delivers the same
request to a customer-owned asynchronous worker and later accepts its signed
response. Both are egress and therefore require a separately approved
installation endpoint, network policy and signing identity.

The request envelope contains only bounded, scrubbed data:

```json
{
  "contract": "pipeline-plugin-extract/1",
  "deliveryId": "<stable opaque id>",
  "installationId": "<opaque installation id>",
  "plugin": { "id": "relayfacts", "version": "1.0.0" },
  "input": { "projection": "session-analysis/1", "revision": "<opaque>", "items": [] },
  "limits": { "records": 100, "responseBytes": 262144 },
  "issuedAt": "<timestamp>",
  "expiresAt": "<timestamp>"
}
```

The transport signs the method or message kind, destination, timestamp,
nonce, body digest and key id. The response is also signed and must echo the
contract, delivery id, plugin release and input revision. It contains either a
bounded records array or a static error code, never instructions for the
pipeline to execute. A response received after expiry, with a reused nonce,
wrong digest, wrong installation identity, unknown field or invalid signature
is refused before validation or storage.

Signing material is provisioned out of band and lives in the platform secret
store or the customer's worker secret store, never in the manifest, record,
logs or repository. The concrete signature profile, rotation overlap and
synchronous timeout must be fixed before this mode ships. Redirects,
credential-bearing URLs, private-network destinations and response bodies in
logs are forbidden. Retries reuse `deliveryId` but use a fresh signed timestamp
and nonce for each attempt, so replay protection and customer-side idempotency
do not conflict.

### Output records

Every extractor returns the same outer record shape. Plugin-specific data is a
schema-validated payload.

```json
{
  "topic": "src-session-read-api",
  "identity": "<stable logical record identity>",
  "revision": null,
  "status": "current",
  "payload": {},
  "evidence": [
    { "source": "codex", "sessionId": "<id>", "start": 12, "end": 15 }
  ]
}
```

`revision` is null for a current-view adapter and an immutable identifier only
when the backing store provides real revision history.

The runtime adds, rather than trusts the extractor to supply:

- organization and workspace scope;
- plugin id and release;
- output-schema version;
- input revision and run id;
- creation and update times; and
- the payload and evidence digests used for idempotency and audit.

The current-view key is `(workspace, plugin, topic, identity)`; a store that
really retains immutable history additionally addresses a revision. Organization
scope is always present internally even though it is not part of the portable
identity. A plugin declares whether a new revision supersedes or appends to the
prior current revision. A repeated revision key with a different digest is
refused, and an extractor-proposed identity or revision cannot address another
plugin or workspace. Supersession never deletes evidence needed for an erase.
Payload fields, evidence entries, records per run, total bytes and topic
cardinality are all bounded by the runtime and may be further narrowed by the
manifest.

A current row is not revision history. An adapter must not synthesize immutable
revisions from a row UUID, topic, `updated_at` or successive observations of a
mutable row. It may offer a revision-addressed read only when the backing store
can reproduce that revision's exact bytes and erase invalidates every revision
that depends on erased evidence.

An extractor cannot set tenant scope, runtime controls, quota counters,
provenance, retention state or sink delivery state. Invalid output commits
nothing for that writer operation. Atomicity is per writer operation, not
across an analysis completion and every optional plugin view. A plugin that
writes independently gated views uses separate deliveries for them. Within one
writer operation, its validated record set commits atomically.

### Generated reads

When an installation's read exposure is enabled, the read API generates a
common surface:

```text
GET /v1/plugins/{plugin}
GET /v1/plugins/{plugin}/topics
GET /v1/plugins/{plugin}/topics/{topic}
GET /v1/plugins/{plugin}/topics/{topic}/records/{identity}
GET /v1/plugins/{plugin}/topics/{topic}/records/{identity}/revisions/{revision} # optional
```

The first route returns plugin release and schema metadata visible to the
caller. Topic and record routes are bounded and cursor-paginated. They return a
common envelope plus schema-validated plugin payloads. The authenticated
workspace must exactly match the installation, and the database reader must
enforce the same organization and workspace scope. A route that is disabled,
not installed or unavailable keeps those states distinct and does not reveal
whether another workspace has the plugin. The revision route is absent unless
the adapter meets the exact-bytes and erase-invalidation requirements above.

Generic MCP tools mirror the HTTP surface rather than introducing a second
storage contract:

- `pipeline_plugins_list` lists enabled installations visible to the caller;
- `pipeline_plugin_topics` lists one plugin's topics; and
- `pipeline_plugin_records_get` reads one topic, record identity or supported
  revision.

MCP tools use the caller's workspace identity, the same limits and cursors,
and the same returned bytes as their HTTP adapters. Plugin-specific convenience
tools may wrap them, but are compatibility contracts of their own and cannot be
silently replaced by the generic shape.

Plugin payloads are untrusted evidence, not instructions. Generic MCP tools do
not promote record text into a system or developer message, and generated
markdown or HTML views escape stored fields rather than rendering active links,
tags, directives or code supplied by an extractor.

### Optional sinks

A sink consumes committed records; it is never part of extraction and cannot
make an extractor commit succeed. Each sink is independently default off,
quota-bound, canaried and retried from a durable delivery id. Credentials and
destination bindings are installation secrets, not manifest fields.

Candidate sink types include GitHub contents, a signed customer webhook and a
customer worker queue. A sink definition must specify:

- which record views and versions may leave the pipeline;
- a deterministic destination key that cannot collide across workspaces;
- create, update, conflict and retry semantics;
- content and metadata that may appear in paths, commits and logs;
- delivery rate and byte bounds; and
- deletion, correction and retention behavior, including immutable history.

The existing relayfacts GitHub module demonstrates safe request bounds,
conflict handling and secret-free logs, but it is not wired. Git history keeps
old content, the current topic-only path can collide across workspaces, and the
operator erase cannot remove earlier revisions. A generic GitHub sink must
remain off until the destination isolation and erasure model are accepted. A
later update that merely hides a file is not proof that the earlier bytes were
erased.

## Runtime guarantees

These guarantees belong to the pipeline runtime and apply to platform and
customer plugins alike. A manifest cannot weaken them.

### Workspace isolation

- One installation belongs to exactly one organization and workspace.
- Input planning, projection reads, extractor requests, record writes, read
  routes, MCP tools, quota counters and sink deliveries all carry and verify
  both scopes.
- Customer-hosted extractors receive only their installation's workspace. A
  request is never batched across workspaces, even for the same plugin.
- Storage is inaccessible directly to extractor and sink identities. Reads and
  writes use bounded functions or equivalent service boundaries with no broad
  table privilege.
- Topic, project, source, plugin id and customer-supplied record fields are
  filters only. None can substitute for workspace authorization.

Isolation must have negative tests at every boundary: a forged scope, a record
id from another workspace, a cursor from another installation and a validly
signed response for the wrong delivery all return no data and make no write.

### Scrubbing and minimization

The selected projection is scrubbed before any extractor receives it,
including the pipeline-managed model. The exact same or stricter scrubbed
projection is used before egress to a customer webhook or worker. Extractor
output is schema-validated and scrubbed again before storage and before each
sink. Scrubbing is provided by the shared relayscrubber integration; plugins
cannot add replacement scrub logic.

An item the scrubber changes may be dropped rather than passed as redacted
instructions. The manifest declares the drop policy from the runtime's allowed
choices. Counts and static reason codes may be logged; selected data, model
input, webhook bodies, responses and record payloads may not.

Selectors and projections must minimize data before scrubbing. Scrubbing is a
backstop, not authorization to send transcripts or unrelated events.

### Default-off controls

Registration, installation, extraction, reads and every sink are separate
states. At minimum the runtime requires:

1. a global plugin-runtime gate;
2. a per-plugin capability gate;
3. a global and workspace installation control; and
4. a separate control for each external sink.

Each plugin has separate default-off extraction and read flags in addition to
those controls; each sink has its own default-off flag. One plugin's flag never
enables another plugin or a shared wildcard.

All default off and fail closed on a missing, malformed or unavailable control
read. Existing feature flags remain authoritative until their plugin adapters
reach parity. Turning generic reads on does not turn extraction or sinks on.
Turning extraction off leaves existing records subject to reads, retention and
erase unless those are separately disabled by their own contract.

### Quotas and model spend

Each installation has hard ceilings for selected sessions and events, input
bytes, extractor invocations, concurrency, response bytes, records and evidence
per run, stored bytes, backfill rate, HTTP calls and sink delivery rate.

Managed extraction additionally has per-run token bounds and daily and monthly
workspace/plugin spend limits. Admission reserves budget before the model call
and reconciles actual usage afterward. An unavailable counter or exhausted hard
limit refuses new paid work before the call. Customer-hosted extraction has no
pipeline model spend but still consumes egress and invocation quotas.

Quota failures are retryable only when retrying cannot repeatedly spend. A run
records bounded counts and static reasons, never selected content. Operators
must be able to stop one installation without stopping unrelated pipeline work.
An ambiguous managed-model call is not invoked again unless the provider
supports the same idempotency key or the runtime can prove the first call did
not consume budget; otherwise the run stops for reconciliation.

### Canary and rollout

Every new plugin release, extractor mode, output schema and sink begins in an
explicit workspace canary. Rollout states are:

- `off`: no planning or calls;
- `observe`: estimate selection and quota use without exposing data or calling
  an extractor;
- `shadow`: extract within budget but commit only a comparison receipt;
- `canary`: commit and expose for named workspaces; and
- `on`: eligible workspaces may opt in.

Progression requires bounded error, cost, latency, parse and scrub-drop metrics
and a parity review against the previous release. Rollback stops new runs and
sink deliveries first; it does not delete records or erase durable fences.

### Idempotent re-runs and backfill

A run id is derived from the installation, plugin release, input projection,
source revision or bounded cursor, and extractor contract revision. Replaying
the same run sends the same customer `deliveryId` and can create at most one
output revision per record identity in that writer operation. Output commit
uses a deterministic idempotency key and an atomic compare-and-commit; an
ambiguous response is reconciled before a new extractor invocation.

Backfill uses the same path as live execution, with a fixed manifest release,
fixed window, durable cursor, smaller quotas and an operator-visible receipt.
It can pause and resume without duplicating records or sink deliveries. It
never bypasses current workspace controls, retention, scrubber, spend limits or
erasure fences. A session fenced after planning but before commit invalidates
the affected output, and the run is retried from a projection that excludes it.

Changing a prompt, schema or selector produces a new plugin release or output
schema version. It must not mutate older records in place under the same
provenance.

### Erase and retention

Plugin data follows the operator per-session erase contract. The erase key
remains `(org, workspace, source, session)`, and a stable request remains
idempotent. The session fence is authoritative before physical cleanup. A
plugin writer must refuse a late record or evidence write for a fenced session,
including a merge that would otherwise add no new evidence row.

For each affected record the plugin erase step:

1. removes evidence and payload text derived only from the erased session and
   its canonical aliases;
2. deletes the record if no evidence remains;
3. otherwise recomputes every derived field from remaining evidence, without
   retaining a title, statement, scope, observed time or summary sourced only
   from the erased session; and
4. verifies absence before reporting the step done.

The existing receipt has an exact, versioned set of artifact keys. A plugin
implementation must account for its work under an existing appropriate
artifact step or explicitly version that receipt contract; it must not add an
unrecognized key to the v1 response. A `completed` erase still means unreadable
and every applicable byte deletion verified, not merely requested. Failures are
retryable and prevent completion. Durable erase fences outlive feature flags,
plugin uninstall and normal record retention.

Per-user erase applies the same evidence removal and late-write fencing across
all of that user's sessions. There is no workspace-wide relayfacts deletion
path today, so this contract does not promise one. A future workspace-delete
design must separately define records, evidence, cursors, quota state, pending
deliveries and the durable anti-resurrection state it keeps.

Disabling extraction or uninstalling a manifest must not disable erase for data
the plugin already wrote. The runtime keeps the erase adapter and fences active
until every retained record and pending delivery is gone. Where an existing
flag couples writes to erase integration, operators must keep the erase side
enabled while records exist; a generic uninstall cannot silently remove it.

Each plugin declares a retention class that the runtime maps to a platform
policy. A manifest may request a shorter window, not a longer one. A derived
record may outlive transcript storage only when its schema contains no
transcript text and the erase path can remove or reconstruct every
session-derived field, as relayfacts does. Superseded records may be retained
only within that policy.

External sinks are part of retention, not an exception to it. If an immutable
or third-party destination cannot meet the required deletion proof, the sink
cannot be enabled for that retention class without an explicit product and
legal decision that changes the contract. Disabling or uninstalling a plugin
stops future work but does not by itself satisfy erasure.

## Relayfacts as the first plugin

Relayfacts already supplies most of the first plugin's domain behavior:

- the structured session-analysis snapshot is its input projection;
- `understanding.decisions` is its selected event data;
- the decision-records extractor creates bounded, scrub-clean topic records
  with session evidence;
- migration `062080` provides workspace-scoped decision storage, readers and
  idempotent evidence merge; `062090` adds per-session erase; `062100` adds the
  owner-only per-user erase, permanent user-erase fences and writer locking;
  and `062150` adds separately gated intent and constraint claims while
  extending both erase paths; and
- the existing read API provides topic listing and markdown rendering.

The plugin adapter must preserve relayfacts' stricter current rules: decisions
need medium or high confidence and cited evidence, scrub-changed text is
dropped, records and evidence remain bounded, and erased session evidence
cannot be written back.

The current record synthesizer performs its scrub checks while building
records. To meet the generic runtime's stronger boundary, the adapter must
materialize and scrub the candidate decision projection before invoking the
generic extractor boundary, then preserve the current rule that any changed
decision is dropped. This is a boundary refactor, not permission to change the
records produced from scrub-clean input.

Relayfacts is a current-view adapter. Its generic record identity is the
existing organization, workspace, topic and normalized-title dedupe identity,
namespaced by the `relayfacts` plugin; the stored UUID identifies the current
row and is not a revision. Evidence merges update that row. The adapter
therefore exposes no synthetic revision history and no revision-addressed
read. A future history store must meet the exact-bytes and erase-invalidation
requirements before it can add one.

Decision and claim writes are separate deliveries. A completed analysis and a
successful decision write survive a claims control being off, a claim refusal
or a claim-writer failure. The plugin abstraction must not wrap both calls in a
transaction or report claim failure as analysis failure.

The existing `src/session-analysis/runner.ts` refusal
`azure_decision_records_unsupported` remains in force. A plugin adapter does
not relax it; Azure decision extraction stays off until its erase prerequisite
and the runner's refusal are deliberately replaced by a separately reviewed
implementation.

Current runtime controls map to plugin capabilities without changing their
meaning:

| Current control | Plugin capability |
|---|---|
| `SUMMARY_DECISION_RECORDS_ENABLED` | relayfacts decision extraction and its erase integration |
| `READ_API_RELAYFACTS_ENABLED` | legacy relayfacts reads |
| `SESSION_INTEL_CLAIMS_ENABLED` | optional intent and constraint extraction, inside decision extraction |
| `RELAYFACTS_GITHUB_PUSH_ENABLED` | optional GitHub sink; still unwired and off |

Generic installation controls would be additional AND gates during migration,
not substitutes that accidentally turn an existing path on. Until the adapter
is proven, the current flags are the source of truth.

### Compatibility contract

The following remain byte-compatible, including disabled responses, field
names, ordering, pagination, markdown escaping, bounds and error behavior:

- `GET /v1/relayfacts`;
- `GET /v1/relayfacts/{topic}`;
- MCP `relayfacts_list`; and
- MCP `relayfacts_get`.

The legacy HTTP adapter continues to read through the tenant-bound
`sessions.relayfacts_topics_v1` and `sessions.relayfacts_list_v1` functions as
`bulls96_read_api`. Their `session_directory_read_api_scope` check, the
workspace attestation and the shared `READ_API_DIRECTORY_WORKSPACES` relayfacts
canary remain mandatory. Generic plugin authorization cannot replace or widen
any of them.

The exact legacy behavior includes:

- disabled routes return `501 {"error":"relayfacts_disabled"}` before a read;
- topics remain lexicographically ordered and bounded at 1,000 by the existing
  reader;
- topic records remain oldest first by `(created_at, id)`, at most 200 per
  page;
- the `md1:` cursor keeps its UTC microsecond timestamp and UUID encoding;
- continued-page text, markdown escaping, evidence ordering and every response
  field remain unchanged; and
- bad topics, limits, cursors and unsupported parameters keep their current
  status and error shapes.

The legacy surfaces remain decisions-only. Generic plugin routes and tools are
additive and cannot add plugin metadata, claim kinds or generic envelopes to a
legacy response. HTTP golden tests in this repository are necessary but not
sufficient for MCP byte parity. That claim waits until the MCP owner supplies
exact `relayfacts_list` and `relayfacts_get` golden requests and response bytes,
and those goldens pass with the adapter off and on.

### Option A: migrate into generic plugin records

Option A creates a generic `plugin_records` and evidence model, backfills the
`062080` decision rows and the `062150` claims rows, then moves relayfacts
writes and reads to it. Compatibility views or adapters keep the old functions,
routes and MCP tools serving their original bytes. The backfill represents each
source row as current state and does not manufacture revisions from its UUID or
timestamps.

Advantages:

- one storage, idempotency, erase, retention and generic-read implementation;
- new generic features apply to relayfacts without a second adapter; and
- fewer long-term special cases if most plugins fit the same record model.

Costs and risks:

- a populated-table migration, backfill and cutover for an already versioned
  contract;
- dual-write or read-switch complexity while old and new paths coexist;
- greater chance of changing dedupe, ordering, timestamps, erase rewrites,
  SQL grants or markdown bytes; and
- a generic schema may prematurely encode relayfacts-specific evidence and
  supersession rules as universal behavior.

This option should be considered only after at least two plugins demonstrate a
stable common storage shape and after parity can be proved over real-sized,
synthetic data. It is not the first rollout step.

### Option B: adapt the existing tables

Option B keeps the `062080`, `062090`, `062100` and `062150` tables and
functions as the relayfacts storage and erase implementation. A relayfacts
plugin adapter maps the generic runtime request to the existing extractor and
writers, and maps existing rows to a current-view generic read envelope. The
legacy routes and MCP tools continue to call their current path.

Advantages:

- no data migration or dual write;
- exact legacy behavior remains owned by the existing schema and renderer;
- the proven fence and erase behavior stays in place; and
- the generic contract is tested against a non-generic implementation, which
  discourages leaking storage details into the plugin API.

Costs and risks:

- adapters may duplicate some pagination, provenance and retention plumbing;
- generic runtime guarantees must be mapped explicitly onto older functions;
  and
- future generic features need adapter work until or unless storage converges.

**Recommendation: Option B.** It is the smallest compatibility boundary and
keeps a later Option A possible. The adapter should be narrow enough that a
future storage move occurs behind it without changing either legacy or generic
read contracts.

### Sessions-intel claims compatibility

Migration `062150` deliberately adds `relayfacts_claims_v1` and
`relayfacts_claim_evidence_v1` beside the decision tables. It does not change
`relayfacts_v1`, `relayfacts_evidence_v1` or the `062080` read functions.
`SESSION_INTEL_CLAIMS_ENABLED` is separately default off and remains nested
inside `SUMMARY_DECISION_RECORDS_ENABLED`.

The claims shape adds:

- `kind`: `intent` or `constraint` (the unified internal reader also presents
  existing records as `decision`);
- `scope`: `session`, `project` or `repo`; and
- nullable `observed_at`.

The adapter should model this as an optional `claims/1` output view of the same
relayfacts plugin release, not add fields to the legacy decision view. The
generic route may expose a claims subresource only to an explicitly authorized
capability; the existing relayfacts routes and MCP tools remain decisions-only.
The current unified claims reader is granted to `service_role` for the worker
and advisor; it is not a public read boundary and must never be called through
`service_role` on behalf of a read-API or MCP caller. A public claims view would
need its own tenant-scoped SECURITY DEFINER reader, read role, workspace
attestation, canary and negative isolation tests. Until those exist, only the
coordination advisor may use the internal unified claims reader.

Claims retain their existing semantics: kind participates in deduplication,
scope widens only when new evidence is added, observed time moves earlier, and
erase resets a retained claim's scope to `session` and `observed_at` to null.
Generic record normalization must not turn nullable decision scope or observed
time into fabricated values. A backfill or rerun must preserve the current
`SESSION_INTEL_CLAIMS_ENABLED` gate and the fail-closed global and workspace
`claims` controls.

### Relayfacts adapter proof obligations

Before the adapter leaves shadow, tests must prove at least these failure and
replay cases against the real existing writers:

- with either claims control off, no claims read or write occurs, while the
  analysis completion and decision delivery are unchanged;
- a claims writer error or refusal leaves the analysis completed and every
  already-committed decision intact;
- replaying the same analysis is idempotent: it creates no duplicate record or
  evidence and preserves the current-view bytes; and
- an `RH016` fence refuses a duplicate input even when it would merge into an
  existing record and add no new evidence, with no partial update.

The last case is a commit-time fence requirement, not merely a row-trigger
test: a duplicate or capped evidence set can otherwise skip the evidence-row
write on which a trigger would fire.

## Likely second plugin: intent-versus-outcome advisories

An `intent-outcome-advisories` plugin is a useful second implementation because
it exercises different parts of the contract without requiring raw
transcripts.

- **Input:** current relayfacts intents and constraints from `062150`, plus
  bounded outcome/write facts and live session-directory candidates from the
  same workspace and project.
- **Extractor:** deterministic candidate and rule classification first;
  optionally a managed, budgeted structured judge after replay evaluation. A
  customer webhook is not required for the first slice.
- **Output:** versioned advisory records by topic, with kind, urgency, peer
  pointers, bounded evidence and lifecycle state. They map naturally to the
  existing coordination-advisory adapter rather than requiring immediate
  generic storage migration.
- **Exposure:** generic plugin reads, with the existing coordination pull and
  ack routes retained as compatibility surfaces.
- **Sinks:** a turn-end notice and, only for evaluated high-confidence rows, a
  Relay DM. Cooldowns, per-recipient caps, delivery state and ack are sink
  concerns layered on committed records.

This plugin stays off until replay evidence supports its alert classes and the
known live claim-timing window is closed. Shared claims, planned fleets,
truncated inputs and claims-reader failure keep the current conservative
behavior. Its value as the second plugin is architectural: it tests
multi-source selection, continuously refreshed records and delivery sinks,
while relayfacts tests model extraction, evidence merge and topic reads.

## Phased rollout

Every phase is a separate reviewed implementation and remains default off. Any
real migration, deployment, credential, IAM change, model spend or activation
still needs its existing approval.

1. **Resolve the contract.** Decide the open questions below, publish the
   manifest and extractor-envelope schemas, define version semantics and threat
   models, and add only offline validators and fixtures.
2. **Relayfacts adapter in shadow.** Implement Option B for an internal,
   compiled relayfacts manifest. Compare selected input, records, pagination,
   markdown and erase results without exposing generic routes. Prove complete
   legacy HTTP byte parity; do not claim MCP parity until the MCP owner supplies
   and approves exact goldens.
3. **Additive generic reads.** Canary generated relayfacts HTTP routes and
   generic MCP tools for synthetic and named workspaces. Keep the legacy reads
   authoritative. No customer manifests, model calls, webhooks or sinks yet.
4. **Managed execution.** Put the relayfacts adapter behind shared planning,
   quota, spend, canary, run-ledger and backfill machinery. Start with observe,
   then shadow, then a bounded canary. Keep the existing relayfacts writer and
   erase functions.
5. **Customer-hosted extraction.** After signing, SSRF, retry, rotation,
   timeout, audit and deletion tests pass, allow reviewed manifests for one
   canary workspace. Begin with worker or webhook response validation and no
   external sink.
6. **Optional sinks.** Enable only destinations whose workspace isolation and
   erase/retention contract is complete. GitHub remains off until path
   collision and immutable-history decisions are resolved.
7. **Second plugin.** Adapt intent-versus-outcome advisories, run the replay
   evaluation, then canary pull-only records before considering any
   notification sink.
8. **Revisit generic storage.** With two adapters and measured workloads,
   decide whether Option A reduces complexity enough to justify a migration.

## Open questions for Khaliq

1. Is the first customer surface curated, signed manifests only, or may a
   workspace author and upload a manifest? Who may publish a new immutable
   plugin release?
2. Should the first slice accept only pipeline-managed prompts, or may a
   customer supply prompt text? If customer prompts are allowed, what review
   and model classes apply?
3. Is Option B, the relayfacts adapter over existing storage, the desired first
   implementation, with Option A deferred until a second plugin exists?
4. Should generic relayfacts reads expose the optional `claims/1` view, and if
   so to which identities? The legacy routes and MCP tools would remain
   decisions-only.
5. Which signing profile and response mode should customer extractors use:
   synchronous webhook, asynchronous worker, or both in the first public
   contract?
6. Are plugin output schemas fully customer-defined within JSON Schema bounds,
   or selected from reviewed schemas? Which fields must remain common for
   erase and evidence reconstruction?
7. What are the default and maximum model-spend, storage, invocation and
   backfill quotas, and who can raise them?
8. May any external sink retain immutable history that the operator erase
   cannot prove deleted? If not, the current GitHub push remains unwired.
9. Does plugin installation use the existing session-intel controls store or a
   separate plugin control plane? How are global, workspace and sink controls
   audited?
10. Which compatibility owner approves the golden bytes for
    `relayfacts_list`, `relayfacts_get` and the two legacy HTTP routes before a
    canary?
