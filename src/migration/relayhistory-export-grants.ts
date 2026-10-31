/**
 * Auditable lifecycle for the export's RelayAuth migration grants: dry-run,
 * plan, apply, revoke. Stacked on the shard planner
 * (relayhistory-export-shards.ts): one grant per shard.
 *
 * NOTHING HERE IS ACTIVE. The CLI refuses `apply` and `revoke` even with
 * `--execute`, because no real RelayAuth client and no authoritative
 * membership reader are wired. The library functions run only against an
 * injected `GrantClient` and `MembershipSource` (fakes in tests). No minting,
 * no activation. Activation waits for (1) the authoritative Cloud membership
 * reader to be wired (see "Membership source" below), and (2) the E3 HOLDs to
 * close. Both need Khaliq's explicit OK.
 *
 * ## Approval
 *
 * `plan` produces an immutable aggregate digest over: the contract and planner
 * versions, the org, `exported_at`, the grant parameters (API key id,
 * audience, not-after), the pinned membership snapshot id, every shard's id
 * and canonical plan-body digest, and the exception list. A human approves
 * that digest together with the exact snapshot id, never the mutable grant
 * rows. `apply` recomputes everything and fails closed unless both match.
 *
 * ## apply fails closed on
 *
 * - a missing snapshot (`snapshot_missing`), another snapshot than approved
 *   (`snapshot_changed`), or one older than the allowed age (`snapshot_stale`);
 * - any pair or session exception (`pair_exceptions`);
 * - digest drift (`digest_drift`);
 * - partial shard state from an earlier run: an apply that failed partway
 *   (`previous_run_failed`), a ledger of another plan (`ledger_mismatch`), or
 *   a plan already rolled back (`plan_rolled_back`). A shard whose create
 *   started but was never recorded is not a blocker any more: it is resumed
 *   with the same Idempotency-Key, and RelayAuth returns the original grant;
 * - a target origin (audience) not in the configured allow-list
 *   (`unrecognized_origin`).
 *
 * Re-applying the same approved plan is idempotent: shards recorded as created
 * are skipped by shard id, so no duplicate grant. Each create carries an
 * `Idempotency-Key` (relayauth #112 @b4a4c99b, required) derived from the
 * shard id and the approved plan version (`shardIdempotencyKey`, never logged
 * or stored). A lost response is recovered by retrying with the same key: the
 * same key with the same request returns the original grant (201,
 * `Idempotent-Replayed: true`). A 409 `idempotency_key_conflict`, any other
 * refusal, a replayed grant that is already revoked, or retries exhausted
 * halt the run (compensating revoke, then `previous_run_failed`).
 *
 * If a create fails partway, apply revokes the grants it created in that same
 * run (compensating revoke) and returns the evidence. `revoke` without shard
 * ids is the rollback (revoke-all) path.
 *
 * ## Membership contract (helm-chart-chief audit)
 *
 * - **Ids:** legacy ids are not assumed to be Cloud ids. Only canonical
 *   lowercase UUIDs are Cloud ids. `relay_` stripping is never authority for
 *   membership. A legacy alias counts only through an explicit, reviewed,
 *   collision-free `IdMappingLedger` (empty by default; `validateIdMapping`).
 * - **One read:** `MembershipSource.read` must run ONE Cloud transaction,
 *   `REPEATABLE READ READ ONLY`, and require all of: `workspaces.id` exact,
 *   `workspaces.organization_id` = the org, `users.id` exact, and an active
 *   `organization_memberships(org, user)` row. The snapshot id hashes the
 *   exact rows read, with `read_at`.
 * - **Blockers:** any invalid, alias, default, inactive, cross-org, unknown
 *   or ambiguous id or pair is an exception that blocks its shard and the
 *   whole run.
 * - **Identity preflight:** every source user must be an active HUMAN
 *   RelayAuth identity in the same org (`IdentityPreflight`).
 *
 * ## Membership source (finding, AgentWorkforce/cloud @865ca405, read-only)
 *
 * - **System and tables:** Cloud's Postgres, `organization_memberships`
 *   (`organization_id`, `user_id`, `role`, `status`; primary key
 *   `(organization_id, user_id)`), `workspaces` (`id`, `organization_id`,
 *   `relay_workspace_id`) and `users` (`id`) (packages/core/src/db/schema.ts).
 * - **Tenancy predicate:** a user may act on a workspace exactly when they
 *   have a `status = 'active'` membership in the organization that owns the
 *   workspace; there is no per-workspace membership table
 *   (packages/web/lib/arelay/org-member.ts; lib/auth/store.ts `getAuthContext`).
 * - **Writers:** sign-up (lib/auth/store.ts), invite acceptance and
 *   reactivation (lib/invites/invite-store.ts), revocation to `revoked` with
 *   the member's API tokens (lib/members/member-store.ts).
 * - **Deleted or disabled:** memberships are soft-deleted (`revoked`) and
 *   every reader filters `active`; workspaces are hard-deleted
 *   (lib/workspace/app-workspace-deletion.ts).
 * - **Authority: NOT established for the export's ids.** These tables are
 *   authoritative for Cloud ids. Cloud's live RelayHistory assertions sign the
 *   Cloud user and app workspace ids (lib/relayhistory-session.ts), but the
 *   legacy Neon ids are unverified; any that are not UUIDs need the reviewed
 *   mapping ledger. The real reader and preflight stay unwired.
 */
import { createHash, randomUUID } from "node:crypto";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { join } from "node:path";
import {
  canonicalMigrationId,
  canonicalMigrationJson,
  deriveMigrationGrantPlan,
} from "./vendor/relayauth-migration-plan.ts";
import type { UploadInput } from "./relayhistory-export-upload.ts";
import { planShards, shardReport, PLAN_CONTRACT, SHARD_PLANNER_VERSION, type Shard, type ShardReport } from "./relayhistory-export-shards.ts";

export const LIFECYCLE_VERSION = 1;

const sha256Hex = (s: string) => createHash("sha256").update(s).digest("hex");
const cmp = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);

export class GrantLifecycleError extends Error {
  constructor(readonly code: string, message: string) {
    super(message);
    this.name = "GrantLifecycleError";
  }
}
const refuse = (code: string, message: string): never => {
  throw new GrantLifecycleError(code, message);
};

// ---------------------------------------------------------------- ids

/** Cloud ids are canonical lowercase UUIDs; nothing else is a Cloud id. */
const CLOUD_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
export const isCloudId = (id: string) => CLOUD_UUID.test(id);

export type IdKind = "org" | "workspace" | "user";

/**
 * The only way a legacy (non-UUID) id may stand for a Cloud id: an explicit,
 * reviewed, collision-free mapping. Empty by default. No heuristic mapping:
 * `relay_` stripping, case folding or name matching never apply.
 */
export interface IdMappingLedger {
  version: 1;
  reviewed_by: string;
  reviewed_at: string;
  entries: { kind: IdKind; legacy_id: string; cloud_id: string }[];
}

export const EMPTY_ID_MAPPING: IdMappingLedger = { version: 1, reviewed_by: "", reviewed_at: "", entries: [] };

/** Refuses a mapping that is unreviewed, malformed or not collision-free. */
export function validateIdMapping(ledger: IdMappingLedger): IdMappingLedger {
  const bad = (why: string): never => refuse("id_mapping_invalid", `id mapping ledger: ${why}`);
  if (ledger.version !== 1) bad("unsupported version");
  if (!ledger.entries.length) return ledger;
  if (!ledger.reviewed_by.trim() || !Number.isFinite(Date.parse(ledger.reviewed_at))) bad("entries need reviewed_by and reviewed_at");
  for (const kind of ["org", "workspace", "user"] as const) {
    const entries = ledger.entries.filter((e) => e.kind === kind);
    const legacy = new Set<string>();
    const cloud = new Set<string>();
    for (const e of entries) {
      if (!isCloudId(e.cloud_id)) bad(`${kind} ${e.legacy_id} maps to a non-UUID`);
      if (!e.legacy_id || isCloudId(e.legacy_id)) bad(`${kind} legacy id must be a non-UUID alias`);
      if (legacy.has(e.legacy_id)) bad(`${kind} legacy id ${e.legacy_id} is mapped twice`);
      if (cloud.has(e.cloud_id)) bad(`${kind} Cloud id is the target of two legacy ids`);
      legacy.add(e.legacy_id);
      cloud.add(e.cloud_id);
    }
  }
  for (const e of ledger.entries) if (!["org", "workspace", "user"].includes(e.kind)) bad("unknown kind");
  return ledger;
}

export type IdResolution =
  | { ok: true; cloud_id: string; via: "uuid" | "mapping" }
  | { ok: false; reason: "default_workspace" | "alias_id" | "invalid_id" };

/** A legacy id to its Cloud id: an exact UUID, or an explicit mapping entry. */
export function resolveCloudId(kind: IdKind, id: string, mapping: IdMappingLedger): IdResolution {
  const mapped = mapping.entries.find((e) => e.kind === kind && e.legacy_id === id);
  if (mapped) return { ok: true, cloud_id: mapped.cloud_id, via: "mapping" };
  if (isCloudId(id)) return { ok: true, cloud_id: id, via: "uuid" };
  if (kind === "workspace" && id === "default") return { ok: false, reason: "default_workspace" };
  if (id.startsWith("relay_")) return { ok: false, reason: "alias_id" };
  return { ok: false, reason: "invalid_id" };
}

// ---------------------------------------------------------------- membership

/**
 * The rows one membership read returned. The reader must run ONE Cloud
 * transaction, `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY`, selecting
 * exactly: `workspaces` by exact `id` (with `organization_id`), `users` by
 * exact `id`, and `organization_memberships` by exact `(organization_id,
 * user_id)`. The snapshot id hashes these rows; `read_at` is when it read.
 */
export interface MembershipRows {
  workspaces: { id: string; organization_id: string }[];
  users: { id: string }[];
  memberships: { organization_id: string; user_id: string; status: string }[];
}

export interface MembershipSnapshot {
  source: string;
  isolation: "REPEATABLE READ READ ONLY";
  read_at: string;
  rows: MembershipRows;
}

export interface MembershipSource {
  read(request: { organization_id: string; workspace_ids: string[]; user_ids: string[] }): Promise<MembershipSnapshot | undefined>;
}

export function membershipSnapshotId(s: MembershipSnapshot): string {
  const rows = {
    workspaces: [...s.rows.workspaces].sort((a, b) => cmp(`${a.id}\u0000${a.organization_id}`, `${b.id}\u0000${b.organization_id}`)),
    users: [...s.rows.users].sort((a, b) => cmp(a.id, b.id)),
    memberships: [...s.rows.memberships].sort((a, b) => cmp(`${a.organization_id}\u0000${a.user_id}\u0000${a.status}`, `${b.organization_id}\u0000${b.user_id}\u0000${b.status}`)),
  };
  return `${s.source}:${sha256Hex(canonicalMigrationJson({ isolation: s.isolation, read_at: s.read_at, rows }))}`;
}

export interface MembershipFixture {
  source: string;
  read_at: string;
  workspaces: { id: string; organization_id: string }[];
  users: { id: string }[];
  memberships: { organization_id: string; user_id: string; status: string }[];
  /** Test hook: report another isolation level. */
  isolation?: string;
}

/** Cloud's three tables as a fixture; returns only the rows the exact-id query would. */
export class FixtureMembershipSource implements MembershipSource {
  constructor(private readonly fixture: MembershipFixture | undefined) {}

  async read(request: { organization_id: string; workspace_ids: string[]; user_ids: string[] }): Promise<MembershipSnapshot | undefined> {
    const f = this.fixture;
    if (!f) return undefined;
    const ws = new Set(request.workspace_ids);
    const us = new Set(request.user_ids);
    return {
      source: f.source,
      isolation: (f.isolation ?? "REPEATABLE READ READ ONLY") as MembershipSnapshot["isolation"],
      read_at: f.read_at,
      rows: {
        workspaces: f.workspaces.filter((w) => ws.has(w.id)),
        users: f.users.filter((u) => us.has(u.id)),
        memberships: f.memberships.filter((m) => m.organization_id === request.organization_id && us.has(m.user_id)),
      },
    };
  }
}

export type MembershipVerdict =
  | "active_member"
  | "workspace_unknown"
  | "workspace_other_org"
  | "user_unknown"
  | "membership_inactive"
  | "not_member"
  | "ambiguous";

/** The Cloud predicate, all four required, on exact ids. */
export function membershipVerdict(rows: MembershipRows, org: string, workspace: string, user: string): MembershipVerdict {
  const w = rows.workspaces.filter((r) => r.id === workspace);
  const u = rows.users.filter((r) => r.id === user);
  const m = rows.memberships.filter((r) => r.organization_id === org && r.user_id === user);
  if (w.length > 1 || u.length > 1 || m.length > 1) return "ambiguous";
  if (!w.length) return "workspace_unknown";
  if (w[0]!.organization_id !== org) return "workspace_other_org";
  if (!u.length) return "user_unknown";
  if (!m.length) return "not_member";
  return m[0]!.status === "active" ? "active_member" : "membership_inactive";
}

// ---------------------------------------------------------------- identity preflight

export type IdentityVerdict = "active_human" | "inactive" | "not_human" | "other_org" | "unknown";

/** Every source user must be an active HUMAN RelayAuth identity in the grant's org. */
export interface IdentityPreflight {
  check(request: { organization_id: string; user_ids: string[] }): Promise<{ source: string; read_at: string; identities: { user_id: string; org_id: string; kind: string; status: string }[] } | undefined>;
}

export class FixtureIdentityPreflight implements IdentityPreflight {
  constructor(private readonly fixture: { source: string; read_at: string; identities: { user_id: string; org_id: string; kind: string; status: string }[] } | undefined) {}
  async check(request: { organization_id: string; user_ids: string[] }) {
    if (!this.fixture) return undefined;
    const us = new Set(request.user_ids);
    return { ...this.fixture, identities: this.fixture.identities.filter((i) => us.has(i.user_id)) };
  }
}

function identityVerdict(identities: { user_id: string; org_id: string; kind: string; status: string }[], org: string, user: string): IdentityVerdict {
  const rows = identities.filter((i) => i.user_id === user);
  if (rows.length !== 1) return "unknown";
  const i = rows[0]!;
  if (i.org_id !== org) return "other_org";
  if (i.kind !== "human") return "not_human";
  return i.status === "active" ? "active_human" : "inactive";
}

// ---------------------------------------------------------------- plan

export interface GrantParameters {
  orgId: string;
  apiKeyId: string;
  /** The History origin the grant's assertions are for. */
  audience: string;
  notAfter: string;
}

export type LifecycleException =
  | { kind: "org"; org_id: string; reason: string }
  | { kind: "id"; id_kind: IdKind; id: string; reason: "default_workspace" | "alias_id" | "invalid_id"; shard_ids: string[] }
  | { kind: "ambiguous_id"; id_kind: IdKind; cloud_id: string; legacy_ids: string[]; shard_ids: string[] }
  | { kind: "membership"; verdict: Exclude<MembershipVerdict, "active_member">; workspace_id: string; source_user_id: string; shard_ids: string[] }
  | { kind: "identity"; verdict: Exclude<IdentityVerdict, "active_human">; source_user_id: string; shard_ids: string[] }
  | { kind: "oversized_session"; org_id: string; session: string; manifests: number; chunks: number; source_users: number; oracle_code: string };

export interface LifecycleShard {
  shard_id: string;
  /** sha256 of the canonical plan body a grant is created with. */
  body_sha256: string;
  pairs: number;
  source_users: number;
  manifests: number;
  chunks: number;
  serialized_bytes: number;
  /** An exception names this shard: it, and so the run, is blocked. */
  blocked: boolean;
}

export interface LifecyclePlan {
  version: number;
  contract: string;
  planner_version: number;
  org_id: string;
  exported_at: string;
  grant: { api_key_id: string; audience: string; not_after: string };
  source_snapshot: { source: string; snapshot_id: string; read_at: string };
  identity_snapshot: { source: string; snapshot_id: string; read_at: string };
  id_mapping_sha256: string;
  shards: LifecycleShard[];
  pair_counts: { total: number; matched: number; exceptions: number };
  limits: ShardReport["max"];
  exceptions: LifecycleException[];
  run_blocked: boolean;
  aggregate_digest: string;
}

interface Built {
  plan: LifecyclePlan;
  shards: Shard[];
}

export interface PlanInput {
  uploads: readonly UploadInput[];
  exportedAt: string;
  grant: GrantParameters;
  membership: MembershipSource;
  identities: IdentityPreflight;
  /** Reviewed legacy-to-Cloud id mapping; empty by default. */
  idMapping?: IdMappingLedger;
}

/**
 * The uploads with every id the mapping (or an exact UUID) resolves rewritten
 * to its Cloud id: `org_id`, `workspace_id`, `source_user_id`, and the key's
 * `ws=` segment. Shards, the plan sent to RelayAuth and the export courier's
 * plan all use these, so a reviewed alias binds the signed plan and the
 * request, not only the membership check. An id that does not resolve stays
 * as it is; it is an exception and blocks its shard.
 */
export function canonicalizeUploads(uploads: readonly UploadInput[], mapping: IdMappingLedger = EMPTY_ID_MAPPING): UploadInput[] {
  const m = validateIdMapping(mapping);
  const to = (kind: IdKind, id: string) => {
    const r = resolveCloudId(kind, id, m);
    return r.ok ? r.cloud_id : id;
  };
  return uploads.map((u) => {
    const ws = to("workspace", u.workspace_id);
    const prefix = `v1/ws=${u.workspace_id}/`;
    return {
      ...u,
      org_id: to("org", u.org_id),
      workspace_id: ws,
      source_user_id: to("user", u.source_user_id),
      key: u.key.startsWith(prefix) ? `v1/ws=${ws}/${u.key.slice(prefix.length)}` : u.key,
    };
  });
}

async function build(input: PlanInput): Promise<Built> {
  const mapping = validateIdMapping(input.idMapping ?? EMPTY_ID_MAPPING);
  // Shard the canonical uploads; keep each one's raw ids to judge aliases.
  const canonicalUploads = canonicalizeUploads(input.uploads, mapping);
  const rawOf = new Map(canonicalUploads.map((c, i) => [c.key, input.uploads[i]!]));
  if (rawOf.size !== canonicalUploads.length) refuse("ambiguous_key", "two uploads map to the same canonical manifest key");
  const shardPlan = planShards({ exportedAt: input.exportedAt, uploads: canonicalUploads });
  const report = shardReport(shardPlan);
  const exceptions: LifecycleException[] = [];
  const blocked = new Set<string>();
  const block = (ids: Iterable<string>) => {
    for (const id of ids) blocked.add(id);
  };

  // The grant's org must itself be a Cloud id (or explicitly mapped).
  const orgRes = resolveCloudId("org", input.grant.orgId, mapping);
  const org = orgRes.ok ? orgRes.cloud_id : undefined;
  if (!orgRes.ok) exceptions.push({ kind: "org", org_id: input.grant.orgId, reason: orgRes.reason });
  for (const u of input.uploads)
    if (u.org_id !== input.grant.orgId) refuse("org_mismatch", "every upload must name the grant's organization id exactly (one org per lifecycle run)");

  // Resolve every legacy id exactly; record which shards each appears in.
  type Seen = { shards: Set<string>; legacy: Set<string> };
  const ids = { workspace: new Map<string, Seen>(), user: new Map<string, Seen>() };
  const bad = new Map<string, { kind: IdKind; id: string; reason: "default_workspace" | "alias_id" | "invalid_id"; shards: Set<string> }>();
  const pairs = new Map<string, { workspace: string; user: string; shards: Set<string> }>();
  for (const shard of shardPlan.shards)
    for (const c of shard.plan.manifests) {
      const m = rawOf.get(c.key)!;
      const resolved: Record<"workspace" | "user", string | undefined> = { workspace: undefined, user: undefined };
      for (const [kind, raw] of [["workspace", m.workspace_id], ["user", m.source_user_id]] as const) {
        const r = resolveCloudId(kind, raw, mapping);
        if (!r.ok) {
          const k = `${kind}\u0000${raw}`;
          const e = bad.get(k) ?? { kind, id: raw, reason: r.reason, shards: new Set<string>() };
          e.shards.add(shard.shard_id);
          bad.set(k, e);
          continue;
        }
        resolved[kind] = r.cloud_id;
        const seen = ids[kind].get(r.cloud_id) ?? { shards: new Set<string>(), legacy: new Set<string>() };
        seen.shards.add(shard.shard_id);
        seen.legacy.add(raw);
        ids[kind].set(r.cloud_id, seen);
      }
      if (resolved.workspace && resolved.user) {
        const k = `${resolved.workspace}\u0000${resolved.user}`;
        const p = pairs.get(k) ?? { workspace: resolved.workspace, user: resolved.user, shards: new Set<string>() };
        p.shards.add(shard.shard_id);
        pairs.set(k, p);
      }
    }
  for (const e of [...bad.values()].sort((a, b) => cmp(`${a.kind}${a.id}`, `${b.kind}${b.id}`))) {
    exceptions.push({ kind: "id", id_kind: e.kind, id: e.id, reason: e.reason, shard_ids: [...e.shards].sort() });
    block(e.shards);
  }
  // One Cloud id reached through two legacy forms is ambiguous.
  for (const kind of ["workspace", "user"] as const)
    for (const [cloudId, seen] of [...ids[kind]].sort(([a], [b]) => cmp(a, b)))
      if (seen.legacy.size > 1) {
        exceptions.push({ kind: "ambiguous_id", id_kind: kind, cloud_id: cloudId, legacy_ids: [...seen.legacy].sort(), shard_ids: [...seen.shards].sort() });
        block(seen.shards);
      }

  const orderedPairs = [...pairs.values()].sort((a, b) => cmp(`${a.workspace}\u0000${a.user}`, `${b.workspace}\u0000${b.user}`));
  const userIds = [...ids.user.keys()].sort();
  const snapshot = await input.membership.read({ organization_id: org ?? "", workspace_ids: [...ids.workspace.keys()].sort(), user_ids: userIds });
  if (!snapshot) refuse("snapshot_missing", "the membership source returned no snapshot");
  if (snapshot!.isolation !== "REPEATABLE READ READ ONLY") refuse("snapshot_isolation", "the membership read was not one REPEATABLE READ READ ONLY transaction");
  let matched = 0;
  for (const p of orderedPairs) {
    const verdict = org ? membershipVerdict(snapshot!.rows, org, p.workspace, p.user) : "workspace_other_org";
    if (verdict === "active_member") matched += 1;
    else {
      exceptions.push({ kind: "membership", verdict, workspace_id: p.workspace, source_user_id: p.user, shard_ids: [...p.shards].sort() });
      block(p.shards);
    }
  }
  const idCheck = await input.identities.check({ organization_id: org ?? "", user_ids: userIds });
  if (!idCheck) refuse("identity_preflight_missing", "the identity preflight returned nothing");
  for (const user of userIds) {
    const verdict = org ? identityVerdict(idCheck!.identities, org, user) : "other_org";
    if (verdict !== "active_human") {
      exceptions.push({ kind: "identity", verdict, source_user_id: user, shard_ids: [...ids.user.get(user)!.shards].sort() });
      block(ids.user.get(user)!.shards);
    }
  }
  for (const u of shardPlan.unshardable)
    exceptions.push({ kind: "oversized_session", org_id: u.org_id, session: u.session, manifests: u.manifests, chunks: u.chunks, source_users: u.source_users, oracle_code: u.oracle_code });

  const snapshotId = membershipSnapshotId(snapshot!);
  const identitySnapshotId = `${idCheck!.source}:${sha256Hex(canonicalMigrationJson({ read_at: idCheck!.read_at, identities: [...idCheck!.identities].sort((a, b) => cmp(a.user_id, b.user_id)) }))}`;
  const idMappingSha = sha256Hex(canonicalMigrationJson(mapping));
  const shards: LifecycleShard[] = shardPlan.shards.map((s) => ({
    shard_id: s.shard_id,
    body_sha256: sha256Hex(canonicalMigrationJson(s.plan)),
    pairs: new Set(s.plan.manifests.map((m) => `${m.workspace_id}\u0000${m.source_user_id}`)).size,
    source_users: s.source_users,
    manifests: s.manifests,
    chunks: s.chunks,
    serialized_bytes: s.serialized_bytes,
    blocked: blocked.has(s.shard_id) || !orgRes.ok,
  }));
  const grant = { api_key_id: input.grant.apiKeyId, audience: input.grant.audience, not_after: input.grant.notAfter };
  const aggregate_digest = sha256Hex(canonicalMigrationJson({
    v: LIFECYCLE_VERSION,
    contract: PLAN_CONTRACT,
    planner_version: SHARD_PLANNER_VERSION,
    org_id: org ?? input.grant.orgId,
    exported_at: input.exportedAt,
    grant,
    source_snapshot_id: snapshotId,
    identity_snapshot_id: identitySnapshotId,
    id_mapping_sha256: idMappingSha,
    shards: shards.map((s) => ({ shard_id: s.shard_id, body_sha256: s.body_sha256 })),
    exceptions,
  }));
  return {
    shards: shardPlan.shards,
    plan: {
      version: LIFECYCLE_VERSION,
      contract: PLAN_CONTRACT,
      planner_version: SHARD_PLANNER_VERSION,
      org_id: org ?? input.grant.orgId,
      exported_at: input.exportedAt,
      grant,
      source_snapshot: { source: snapshot!.source, snapshot_id: snapshotId, read_at: snapshot!.read_at },
      identity_snapshot: { source: idCheck!.source, snapshot_id: identitySnapshotId, read_at: idCheck!.read_at },
      id_mapping_sha256: idMappingSha,
      shards,
      pair_counts: { total: orderedPairs.length, matched, exceptions: orderedPairs.length - matched },
      limits: report.max,
      exceptions,
      run_blocked: exceptions.length > 0,
      aggregate_digest,
    },
  };
}

/** `plan`: the approvable document (ids, digests, counts, exceptions only). */
export async function lifecyclePlan(input: PlanInput): Promise<LifecyclePlan> {
  return (await build(input)).plan;
}

/** `dry-run`: the shard report and the plan summary; writes nothing. */
export async function lifecycleDryRun(input: PlanInput) {
  const { plan } = await build(input);
  return { dry_run: true as const, aggregate_digest: plan.aggregate_digest, source_snapshot: plan.source_snapshot, identity_snapshot: plan.identity_snapshot, shards: plan.shards.length, blocked_shards: plan.shards.filter((s) => s.blocked).length, run_blocked: plan.run_blocked, pair_counts: plan.pair_counts, limits: plan.limits, exceptions: plan.exceptions };
}

// ---------------------------------------------------------------- RelayAuth

/** The RelayAuth #111 grant routes this command uses (`POST /v1/migration-grants`, `POST /:id/revoke`). */
/**
 * A refusal RelayAuth answered (an HTTP status and error code). Anything else
 * a client throws (timeout, connection loss) is a lost response.
 */
export class GrantClientError extends Error {
  constructor(readonly code: string, readonly status: number) {
    super(`relayauth refused: ${code}`);
    this.name = "GrantClientError";
  }
  /** A 5xx or 429 may be retried with the same key; a 4xx answer is final. */
  get retryable(): boolean {
    return this.status >= 500 || this.status === 429;
  }
}

export interface GrantClient {
  /**
   * `POST /v1/migration-grants` with `idempotencyKey` in the `Idempotency-Key`
   * header (never log it). Throws GrantClientError for a refusal; `replayed`
   * is the `Idempotent-Replayed: true` response header.
   */
  createGrant(request: { apiKeyId: string; audience: string; notAfter: string; plan: { exported_at: string; manifests: UploadInput[] }; idempotencyKey: string }): Promise<{
    grantId: string;
    orgId: string;
    planDigest: string;
    allowedPairs: { sourceUserId: string; workspaceId: string }[];
    planEntries: number;
    createdAt: string;
    revokedAt: string | null;
    replayed: boolean;
  }>;
  revokeGrant(grantId: string): Promise<{ grantId: string; revokedAt: string }>;
}

// ---------------------------------------------------------------- ledger

export type ShardState =
  | { status: "pending"; run_id: string; started_at: string }
  | { status: "created"; run_id: string; grant_id: string; plan_digest: string; created_at: string }
  /** RelayAuth answered with a grant that does not hold this shard: it must be revoked. */
  | { status: "invalid_grant"; run_id: string; grant_id: string; seen_at: string }
  | { status: "revoked"; run_id: string; grant_id: string; plan_digest: string; revoked_at: string; reason: "rollback" | "compensating" | "revoke"; via: "revoke" | "replay" };

export interface Ledger {
  version: number;
  aggregate_digest: string;
  source_snapshot_id: string;
  shards: Record<string, ShardState>;
  /** Set when a run failed partway: apply refuses until a human clears it. */
  halted?: { run_id: string; shard_id: string; at: string };
}

const ledgerPath = (dir: string) => join(dir, "grant-ledger.json");
const GRANT_ID = /^mgr_[0-9a-f]{32}$/;
const isTime = (v: unknown) => typeof v === "string" && Number.isFinite(Date.parse(v));
const isStr = (v: unknown) => typeof v === "string" && v.length > 0;

/**
 * Loads the ledger and validates it against the plan being applied or revoked
 * before anything trusts it: the exact version and shape, the approved
 * aggregate digest, (when given) the exact source snapshot id, only shards of
 * this plan, and, for every recorded grant, a plan digest that RelayAuth's own
 * derivation gives for that grant id and this shard's plan. Anything else is
 * refused (`ledger_invalid` / `ledger_mismatch`), never silently trusted.
 */
async function loadLedger(
  dir: string,
  expect: { aggregateDigest: string; sourceSnapshotId?: string; orgId: string; shards: ReadonlyMap<string, Shard> },
): Promise<Ledger | undefined> {
  let text: string;
  try {
    text = await readFile(ledgerPath(dir), "utf8");
  } catch (e) {
    if ((e as { code?: string }).code === "ENOENT") return undefined;
    throw e;
  }
  const bad = (why: string): never => refuse("ledger_invalid", `grant ledger: ${why}`);
  let raw: unknown;
  try {
    raw = JSON.parse(text);
  } catch {
    bad("not JSON");
  }
  const l = raw as Record<string, unknown>;
  if (!l || typeof l !== "object" || Array.isArray(l)) bad("not an object");
  const allowed = new Set(["version", "aggregate_digest", "source_snapshot_id", "shards", "halted"]);
  if (Object.keys(l).some((k) => !allowed.has(k))) bad("unknown fields");
  if (l.version !== LIFECYCLE_VERSION) bad(`version ${String(l.version)} is not ${LIFECYCLE_VERSION}`);
  if (l.aggregate_digest !== expect.aggregateDigest) refuse("ledger_mismatch", "the state directory holds another plan's grants");
  if (!isStr(l.source_snapshot_id)) bad("source_snapshot_id missing");
  if (expect.sourceSnapshotId !== undefined && l.source_snapshot_id !== expect.sourceSnapshotId)
    refuse("ledger_mismatch", "the ledger was written under another membership snapshot");
  if (l.halted !== undefined) {
    const h = l.halted as Record<string, unknown>;
    if (!h || !isStr(h.run_id) || !isStr(h.shard_id) || !isTime(h.at)) bad("halted is malformed");
  }
  const shards = l.shards as Record<string, Record<string, unknown>>;
  if (!shards || typeof shards !== "object" || Array.isArray(shards)) bad("shards missing");
  for (const [id, st] of Object.entries(shards)) {
    const shard = expect.shards.get(id);
    if (!shard) bad(`shard ${id} is not in this plan`);
    if (!st || typeof st !== "object" || !isStr(st.run_id)) bad(`shard ${id} is malformed`);
    const fields = (...keys: string[]) => Object.keys(st).sort().join(",") === ["status", "run_id", ...keys].sort().join(",");
    const digestOk = () => {
      if (!GRANT_ID.test(String(st.grant_id))) bad(`shard ${id} grant id is malformed`);
      const want = deriveMigrationGrantPlan({ grantId: String(st.grant_id), orgId: expect.orgId, plan: shard!.plan }).planDigest;
      if (st.plan_digest !== want) bad(`shard ${id} plan digest is not this shard's plan under its grant`);
    };
    switch (st.status) {
      case "pending":
        if (!fields("started_at") || !isTime(st.started_at)) bad(`shard ${id} pending record is malformed`);
        break;
      case "created":
        if (!fields("grant_id", "plan_digest", "created_at") || !isTime(st.created_at)) bad(`shard ${id} created record is malformed`);
        digestOk();
        break;
      case "invalid_grant":
        if (!fields("grant_id", "seen_at") || !GRANT_ID.test(String(st.grant_id)) || !isTime(st.seen_at)) bad(`shard ${id} invalid_grant record is malformed`);
        break;
      case "revoked":
        if (!fields("grant_id", "plan_digest", "revoked_at", "reason", "via") || !isTime(st.revoked_at) ||
          !["rollback", "compensating", "revoke"].includes(String(st.reason)) || !["revoke", "replay"].includes(String(st.via)))
          bad(`shard ${id} revoked record is malformed`);
        if (st.plan_digest !== "") digestOk();
        break;
      default:
        bad(`shard ${id} has unknown status`);
    }
  }
  return l as unknown as Ledger;
}

async function saveLedger(dir: string, ledger: Ledger): Promise<void> {
  await mkdir(dir, { recursive: true });
  const path = ledgerPath(dir);
  await writeFile(`${path}.tmp`, JSON.stringify(ledger, null, 2), { mode: 0o600 });
  await rename(`${path}.tmp`, path);
}

export interface RevocationEvidence {
  aggregate_digest: string;
  reason: "rollback" | "compensating" | "revoke";
  /** `via`: "revoke" (this run's revoke answered) or "replay" (relayauth #112 409 migration_grant_revoked, revoked_at read back from the idempotent create replay). */
  revoked: { shard_id: string; grant_id: string; revoked_at: string; via: "revoke" | "replay" }[];
  already_revoked: { shard_id: string; grant_id: string; revoked_at: string }[];
  /** Shards whose create was started but not recorded: a human must check RelayAuth. */
  unresolved: { shard_id: string; started_at: string }[];
  failed: { shard_id: string; grant_id: string; error: string }[];
}

type CreateRequest = Parameters<GrantClient["createGrant"]>[0];

/**
 * Revoke each selected shard's grant, converging against relayauth #112: a
 * revoke whose response was lost is retried; RelayAuth then answers 409
 * `migration_grant_revoked` (no body with the time), so the revoked state and
 * its `revokedAt` are read back by replaying the shard's create with the same
 * Idempotency-Key, which returns the stored grant as it is, revocation state
 * included, without creating anything. A replay naming another grant, or not
 * revoked after the retries, is a failure in the evidence.
 */
async function revokeShards(
  dir: string,
  ledger: Ledger,
  client: GrantClient,
  ids: (id: string, s: ShardState) => boolean,
  reason: RevocationEvidence["reason"],
  requestFor: (shardId: string) => CreateRequest | undefined,
): Promise<RevocationEvidence> {
  const evidence: RevocationEvidence = { aggregate_digest: ledger.aggregate_digest, reason, revoked: [], already_revoked: [], unresolved: [], failed: [] };
  const replayRevokedAt = async (shardId: string, grantId: string): Promise<string | null> => {
    const request = requestFor(shardId);
    if (!request) throw new Error("no create request to replay for this shard");
    const out = await client.createGrant(request);
    if (out.grantId !== grantId) throw new Error("the replay named another grant");
    return out.revokedAt;
  };
  for (const [shardId, state] of Object.entries(ledger.shards).sort(([a], [b]) => cmp(a, b))) {
    if (!ids(shardId, state)) continue;
    if (state.status === "pending") {
      evidence.unresolved.push({ shard_id: shardId, started_at: state.started_at });
      continue;
    }
    if (state.status === "revoked") {
      evidence.already_revoked.push({ shard_id: shardId, grant_id: state.grant_id, revoked_at: state.revoked_at });
      continue;
    }
    const planDigest = state.status === "created" ? state.plan_digest : "";
    let revokedAt: string | undefined;
    let via: "revoke" | "replay" = "revoke";
    let error = "revoke failed";
    let final = false;
    for (let attempt = 0; attempt <= LOST_RESPONSE_RETRIES && revokedAt === undefined && !final; attempt++) {
      try {
        const out = await client.revokeGrant(state.grant_id);
        if (out.grantId !== state.grant_id) throw new Error("revoke answered for another grant");
        revokedAt = out.revokedAt;
      } catch (e) {
        if (e instanceof GrantClientError && e.code === "migration_grant_revoked" && e.status === 409) {
          try {
            const at = await replayRevokedAt(shardId, state.grant_id);
            if (at) {
              revokedAt = at;
              via = "replay";
            } else error = "RelayAuth says revoked but the replay shows the grant live";
          } catch (r) {
            error = r instanceof Error ? r.message.slice(0, 200) : "replay failed";
          }
          final = true;
          break;
        }
        error = e instanceof Error ? e.message.slice(0, 200) : "revoke failed";
        if (e instanceof GrantClientError && !e.retryable) final = true; // 404 and other answers are final
      }
    }
    if (revokedAt === undefined && !final) {
      // Every answer was lost: read the state back through the replay.
      try {
        const at = await replayRevokedAt(shardId, state.grant_id);
        if (at) {
          revokedAt = at;
          via = "replay";
        }
      } catch (r) {
        error = r instanceof Error ? r.message.slice(0, 200) : "replay failed";
      }
    }
    if (revokedAt === undefined) {
      evidence.failed.push({ shard_id: shardId, grant_id: state.grant_id, error });
      continue;
    }
    ledger.shards[shardId] = { status: "revoked", run_id: state.run_id, grant_id: state.grant_id, plan_digest: planDigest, revoked_at: revokedAt, reason, via };
    evidence.revoked.push({ shard_id: shardId, grant_id: state.grant_id, revoked_at: revokedAt, via });
    await saveLedger(dir, ledger);
  }
  await mkdir(dir, { recursive: true });
  const at = new Date().toISOString().replace(/[:.]/g, "-");
  await writeFile(join(dir, `revocation-${reason}-${at}-${randomUUID().slice(0, 8)}.json`), JSON.stringify(evidence, null, 2), { mode: 0o600 });
  return evidence;
}

// ---------------------------------------------------------------- apply / revoke

/**
 * The grant create's Idempotency-Key for a shard: derived from the shard id,
 * the approved plan version (its aggregate digest) and the lifecycle,
 * contract and planner versions. A retry of the same shard under the same
 * approved plan sends the same key; a re-approved plan (say, another
 * not-after or audience) gets new keys, so RelayAuth can never answer it with
 * a grant made under different parameters. Never logged or stored (not in
 * the ledger, the plan, results or evidence).
 */
export function shardIdempotencyKey(shardId: string, aggregateDigest: string): string {
  // base64url(sha256(...)): 43 chars of [A-Za-z0-9_-], as relayauth #112
  // requires (32-128 chars, at least 8 distinct). Re-derived deterministically
  // in the (astronomically unlikely) case of too few distinct characters.
  for (let n = 0; ; n++) {
    const key = createHash("sha256")
      .update(`relayhistory-export-grant\nlifecycle=${LIFECYCLE_VERSION}\ncontract=${PLAN_CONTRACT}\nplanner=${SHARD_PLANNER_VERSION}\nplan=${aggregateDigest}\nshard=${shardId}${n ? `\nn=${n}` : ""}`)
      .digest("base64url");
    if (isValidIdempotencyKey(key)) return key;
  }
}

/** relayauth #112's rule: 32-128 chars of A-Za-z0-9_- with at least 8 distinct. */
export const isValidIdempotencyKey = (key: string) => /^[A-Za-z0-9_-]{32,128}$/.test(key) && new Set(key).size >= 8;

/** Retries of a create whose response was lost, always with the same key. */
const LOST_RESPONSE_RETRIES = 3;

export interface ApplyConfig extends PlanInput {
  execute: boolean;
  approvedDigest: string;
  approvedSnapshotId: string;
  /** History origins grants may be created for (exact https origins). */
  allowedAudiences: readonly string[];
  maxSnapshotAgeMs: number;
  client: GrantClient;
  stateDir: string;
  now?: () => number;
  /** Stop after this many new shards (an interruption between shards). */
  maxNewShards?: number;
}

export interface ApplyResult {
  aggregate_digest: string;
  created: { shard_id: string; grant_id: string; plan_digest: string }[];
  skipped: string[];
  interrupted: boolean;
  failed?: { shard_id: string; error: string };
  compensation?: RevocationEvidence;
}

const sameJson = (a: unknown, b: unknown) => canonicalMigrationJson(a) === canonicalMigrationJson(b);

export async function applyGrants(cfg: ApplyConfig): Promise<ApplyResult> {
  if (cfg.execute !== true) refuse("execute_required", "apply refused without --execute");
  const origin = (() => {
    try {
      return new URL(cfg.grant.audience);
    } catch {
      return undefined;
    }
  })();
  if (!origin || origin.protocol !== "https:" || origin.origin !== cfg.grant.audience || !cfg.allowedAudiences.includes(cfg.grant.audience))
    refuse("unrecognized_origin", "the grant audience is not a recognized History origin");
  const { plan, shards } = await build(cfg);
  if (plan.source_snapshot.snapshot_id !== cfg.approvedSnapshotId) refuse("snapshot_changed", "the membership snapshot is not the approved one");
  const readAt = Date.parse(plan.source_snapshot.read_at);
  const now = (cfg.now ?? Date.now)();
  if (!Number.isFinite(readAt) || now - readAt > cfg.maxSnapshotAgeMs || readAt > now)
    refuse("snapshot_stale", "the membership snapshot is too old (or from the future)");
  const idAt = Date.parse(plan.identity_snapshot.read_at);
  if (!Number.isFinite(idAt) || now - idAt > cfg.maxSnapshotAgeMs || idAt > now)
    refuse("snapshot_stale", "the identity preflight is too old (or from the future)");
  if (plan.exceptions.length) refuse("pair_exceptions", `${plan.exceptions.length} exception(s) need human review; nothing applied`);
  if (plan.aggregate_digest !== cfg.approvedDigest) refuse("digest_drift", "the recomputed aggregate digest is not the approved one");

  const byId = new Map(shards.map((s) => [s.shard_id, s]));
  const prior = await loadLedger(cfg.stateDir, { aggregateDigest: plan.aggregate_digest, sourceSnapshotId: plan.source_snapshot.snapshot_id, orgId: plan.org_id, shards: byId });
  const ledger: Ledger = prior ?? { version: LIFECYCLE_VERSION, aggregate_digest: plan.aggregate_digest, source_snapshot_id: plan.source_snapshot.snapshot_id, shards: {} };
  if (ledger.halted) refuse("previous_run_failed", "an earlier apply failed partway; check its revocation evidence and RelayAuth before going on");
  const states = Object.values(ledger.shards);
  // A create that started but was never recorded is resumed below with the
  // same Idempotency-Key; RelayAuth returns the original grant if it exists.
  if (states.some((s) => s.status === "revoked")) refuse("plan_rolled_back", "this plan's grants were revoked; approve a new plan to start again");

  const requestFor = (shardId: string): CreateRequest | undefined => {
    const shard = byId.get(shardId);
    return shard && { apiKeyId: cfg.grant.apiKeyId, audience: cfg.grant.audience, notAfter: cfg.grant.notAfter, plan: shard.plan, idempotencyKey: shardIdempotencyKey(shardId, plan.aggregate_digest) };
  };
  const runId = randomUUID();
  const result: ApplyResult = { aggregate_digest: plan.aggregate_digest, created: [], skipped: [], interrupted: false };
  let fresh = 0;
  for (const shard of shards) {
    if (ledger.shards[shard.shard_id]?.status === "created") {
      result.skipped.push(shard.shard_id);
      continue;
    }
    if (cfg.maxNewShards !== undefined && fresh >= cfg.maxNewShards) {
      result.interrupted = true;
      break;
    }
    ledger.shards[shard.shard_id] = { status: "pending", run_id: runId, started_at: new Date(now).toISOString() };
    await saveLedger(cfg.stateDir, ledger);
    let grantId: string | undefined;
    try {
      const request = requestFor(shard.shard_id)!;
      // A lost response (or a 5xx) is retried with the same key; RelayAuth
      // answers a retry with the original grant. A 4xx answer (409
      // idempotency_key_conflict included) is final and halts the run.
      let out: Awaited<ReturnType<GrantClient["createGrant"]>> | undefined;
      for (let attempt = 0; !out; attempt++) {
        try {
          out = await cfg.client.createGrant(request);
        } catch (e) {
          if ((e instanceof GrantClientError && !e.retryable) || attempt >= LOST_RESPONSE_RETRIES) throw e;
        }
      }
      grantId = out.grantId;
      if (out.revokedAt !== null) throw new Error("RelayAuth replayed a grant that is already revoked");
      // The grant must hold exactly this shard: re-derive with its id.
      const derived = deriveMigrationGrantPlan({ grantId: out.grantId, orgId: plan.org_id, plan: shard.plan });
      if (out.planDigest !== derived.planDigest || !sameJson(out.allowedPairs, derived.allowedPairs) || out.planEntries !== derived.plan.manifests.length || out.orgId !== plan.org_id)
        throw new Error("RelayAuth's grant does not match the shard");
      ledger.shards[shard.shard_id] = { status: "created", run_id: runId, grant_id: out.grantId, plan_digest: out.planDigest, created_at: out.createdAt };
      await saveLedger(cfg.stateDir, ledger);
      result.created.push({ shard_id: shard.shard_id, grant_id: out.grantId, plan_digest: out.planDigest });
      fresh += 1;
    } catch (e) {
      // A grant that was created but is wrong is revoked with the rest; a
      // create with no answer stays pending for a human.
      if (grantId) ledger.shards[shard.shard_id] = { status: "invalid_grant", run_id: runId, grant_id: grantId, seen_at: new Date(now).toISOString() };
      ledger.halted = { run_id: runId, shard_id: shard.shard_id, at: new Date(now).toISOString() };
      await saveLedger(cfg.stateDir, ledger);
      result.failed = { shard_id: shard.shard_id, error: e instanceof Error ? e.message.slice(0, 200) : "create failed" };
      result.compensation = await revokeShards(cfg.stateDir, ledger, cfg.client, (_id, s) => s.run_id === runId, "compensating", requestFor);
      return result;
    }
  }
  return result;
}

export interface RevokeConfig {
  execute: boolean;
  stateDir: string;
  client: GrantClient;
  /** The approved plan's digest; the ledger must be that plan's. */
  aggregateDigest: string;
  /**
   * The approved plan's inputs (no membership read): to validate the ledger's
   * grants against each shard's plan, and to replay a shard's create when
   * RelayAuth answers 409 migration_grant_revoked.
   */
  plan: Pick<PlanInput, "uploads" | "exportedAt" | "grant" | "idMapping">;
  /** Omit for the rollback (revoke-all) path. */
  shardIds?: readonly string[];
}

export async function revokeGrants(cfg: RevokeConfig): Promise<RevocationEvidence> {
  if (cfg.execute !== true) refuse("execute_required", "revoke refused without --execute");
  const mapping = validateIdMapping(cfg.plan.idMapping ?? EMPTY_ID_MAPPING);
  const orgRes = resolveCloudId("org", cfg.plan.grant.orgId, mapping);
  const orgId = orgRes.ok ? orgRes.cloud_id : cfg.plan.grant.orgId;
  const shards = planShards({ exportedAt: cfg.plan.exportedAt, uploads: canonicalizeUploads(cfg.plan.uploads, mapping) }).shards;
  const byId = new Map(shards.map((s) => [s.shard_id, s]));
  const ledger = await loadLedger(cfg.stateDir, { aggregateDigest: cfg.aggregateDigest, orgId, shards: byId });
  if (!ledger) refuse("ledger_missing", "no grant ledger in the state directory");
  const requestFor = (shardId: string): CreateRequest | undefined => {
    const shard = byId.get(shardId);
    const g = cfg.plan.grant;
    return shard && { apiKeyId: g.apiKeyId, audience: g.audience, notAfter: g.notAfter, plan: shard.plan, idempotencyKey: shardIdempotencyKey(shardId, cfg.aggregateDigest) };
  };
  const only = cfg.shardIds ? new Set(cfg.shardIds) : undefined;
  return revokeShards(cfg.stateDir, ledger!, cfg.client, (id) => !only || only.has(id), only ? "revoke" : "rollback", requestFor);
}

// ---------------------------------------------------------------- CLI

function argument(name: string): string | undefined {
  return process.argv.find((v) => v.startsWith(`--${name}=`))?.slice(name.length + 3);
}

/**
 * `bun run src/migration/relayhistory-export-grants.ts <dry-run|plan|apply|revoke>`.
 * dry-run and plan read local files only (`--inputs` plan inputs JSON,
 * `--membership` and `--identities` fixture JSON, optional `--id-mapping`, `--org`, `--exported-at`, `--api-key-id`,
 * `--audience`, `--not-after`). apply and revoke refuse: no RelayAuth client
 * or authoritative membership reader is wired.
 */
export async function main(argv = process.argv.slice(2)) {
  const sub = argv[0];
  if (sub === "apply" || sub === "revoke") {
    if (!argv.includes("--execute")) refuse("execute_required", `${sub} refused without --execute`);
    refuse("not_wired", `${sub} is not available: no RelayAuth grant client and no authoritative membership reader are wired (activation waits for both and for the E3 HOLDs)`);
  }
  if (sub !== "dry-run" && sub !== "plan") refuse("usage", "usage: <dry-run|plan|apply|revoke> --inputs=... --membership=...");
  const read = async (name: string) => JSON.parse(await readFile(argument(name) ?? refuse("usage", `--${name} is required`), "utf8"));
  const input = {
    uploads: (await read("inputs")) as UploadInput[],
    exportedAt: argument("exported-at") ?? refuse("usage", "--exported-at is required"),
    grant: {
      orgId: argument("org") ?? refuse("usage", "--org is required"),
      apiKeyId: argument("api-key-id") ?? refuse("usage", "--api-key-id is required"),
      audience: argument("audience") ?? refuse("usage", "--audience is required"),
      notAfter: argument("not-after") ?? refuse("usage", "--not-after is required"),
    },
    membership: new FixtureMembershipSource((await read("membership")) as MembershipFixture),
    identities: new FixtureIdentityPreflight(await read("identities")),
    ...(argument("id-mapping") ? { idMapping: (await read("id-mapping")) as IdMappingLedger } : {}),
  };
  const out = sub === "plan" ? await lifecyclePlan(input) : await lifecycleDryRun(input);
  process.stdout.write(`${JSON.stringify(out, null, 2)}\n`);
}

if (import.meta.main)
  main().catch((error) => {
    console.error(`96bulls grants: ${error instanceof Error ? error.message : "failed"}`);
    process.exit(1);
  });
