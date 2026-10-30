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
 *   (`previous_run_failed`), a shard whose create was started but never
 *   recorded (`partial_shard_state`; its grant may exist, so a human
 *   reconciles it), a ledger of another plan (`ledger_mismatch`), or a plan
 *   already rolled back (`plan_rolled_back`);
 * - a target origin (audience) not in the configured allow-list
 *   (`unrecognized_origin`).
 *
 * Re-applying the same approved plan is idempotent: shards recorded as created
 * are skipped by shard id, so no duplicate grant. RelayAuth #111 has no
 * idempotency key or list endpoint, so the only unrecoverable window is a
 * create whose response is lost; that leaves the shard `pending` and apply
 * refuses until a human reconciles it.
 *
 * If a create fails partway, apply revokes the grants it created in that same
 * run (compensating revoke) and returns the evidence. `revoke` without shard
 * ids is the rollback (revoke-all) path.
 *
 * ## Membership source (finding, AgentWorkforce/cloud @865ca405, read-only)
 *
 * - **System and tables:** Cloud's Postgres, `organization_memberships`
 *   (`organization_id`, `user_id`, `role`, `status`; primary key
 *   `(organization_id, user_id)`) joined to `workspaces` (`id`,
 *   `organization_id`, `relay_workspace_id`) on `organization_id`
 *   (packages/core/src/db/schema.ts).
 * - **Tenancy predicate:** a user may act on a workspace exactly when they
 *   have a `status = 'active'` membership in the organization that owns the
 *   workspace. There is no per-workspace membership table
 *   (packages/web/lib/arelay/org-member.ts, `requireArelayOrgMemberForWorkspace`;
 *   the session's workspace list in lib/auth/store.ts `getAuthContext` is
 *   every workspace of every org with an active membership).
 * - **Writers:** sign-up creates the org, an `owner`/`active` membership and
 *   a default workspace in one transaction (lib/auth/store.ts); invite
 *   acceptance inserts or reactivates a membership (lib/invites/invite-store.ts);
 *   revocation sets `status = 'revoked'` and revokes the member's API token
 *   sessions in the same transaction (lib/members/member-store.ts).
 * - **Deleted or disabled:** memberships are soft-deleted (`revoked`); every
 *   reader filters `status = 'active'`, so anything else is "not a member".
 *   Workspaces are hard-deleted (lib/workspace/app-workspace-deletion.ts), so
 *   a deleted workspace is "unknown". Both are exceptions here.
 * - **Freshness, consistency, snapshot:** not established by Cloud. The
 *   reader this module expects must read all pairs in ONE `REPEATABLE READ,
 *   READ ONLY` transaction, record when it read (`read_at`, the freshness
 *   point), and pin the snapshot id as a digest of the exact rows it read
 *   (`source` + content hash); `FixtureMembershipSource` does exactly that.
 * - **Authority: NOT fully established.** The tables above are what Cloud's
 *   own access checks enforce, so they are the authoritative membership for
 *   Cloud ids. What is not established is that the export's ids are those
 *   ids: Cloud's live RelayHistory assertions sign `sponsorId` = Cloud user
 *   id and `wks` = the app workspace id (lib/relayhistory-session.ts), but
 *   whether the legacy Neon rows use the same ids (rather than, say,
 *   `relay_workspace_id` or another identity) is unverified. Until a human
 *   confirms that mapping, the real reader stays unwired.
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

// ---------------------------------------------------------------- membership

export interface MembershipPair {
  org_id: string;
  workspace_id: string;
  source_user_id: string;
}

export type MembershipVerdict =
  | "active_member"
  | "membership_inactive"
  | "not_member"
  | "workspace_unknown"
  | "workspace_other_org";

export interface MembershipSnapshot {
  source: string;
  snapshot_id: string;
  /** When the source was read: the freshness point. */
  read_at: string;
  verdict(pair: MembershipPair): MembershipVerdict;
}

/** One consistent, read-only read of the authoritative membership. */
export interface MembershipSource {
  read(pairs: readonly MembershipPair[]): Promise<MembershipSnapshot | undefined>;
}

export interface MembershipFixture {
  source: string;
  read_at: string;
  workspaces: { workspace_id: string; organization_id: string }[];
  memberships: { organization_id: string; user_id: string; status: string }[];
}

/**
 * A fixture shaped like Cloud's tables (organization_memberships joined to
 * workspaces on organization_id). Its snapshot id hashes the exact rows.
 */
export class FixtureMembershipSource implements MembershipSource {
  constructor(private readonly fixture: MembershipFixture | undefined) {}

  async read(): Promise<MembershipSnapshot | undefined> {
    const f = this.fixture;
    if (!f) return undefined;
    const workspaces = [...f.workspaces].sort((a, b) => cmp(a.workspace_id, b.workspace_id));
    const memberships = [...f.memberships].sort((a, b) => cmp(`${a.organization_id}\u0000${a.user_id}`, `${b.organization_id}\u0000${b.user_id}`));
    const snapshotId = `${f.source}:${sha256Hex(canonicalMigrationJson({ workspaces, memberships }))}`;
    const owner = new Map(workspaces.map((w) => [w.workspace_id, w.organization_id]));
    const status = new Map(memberships.map((m) => [`${m.organization_id}\u0000${m.user_id}`, m.status]));
    return {
      source: f.source,
      snapshot_id: snapshotId,
      read_at: f.read_at,
      verdict: (pair) => {
        const org = owner.get(pair.workspace_id);
        if (org === undefined) return "workspace_unknown";
        if (org !== pair.org_id) return "workspace_other_org";
        const s = status.get(`${org}\u0000${pair.source_user_id}`);
        if (s === undefined) return "not_member";
        return s === "active" ? "active_member" : "membership_inactive";
      },
    };
  }
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
  | { kind: "membership"; verdict: Exclude<MembershipVerdict, "active_member">; org_id: string; workspace_id: string; source_user_id: string; shard_ids: string[] }
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
}

export interface LifecyclePlan {
  version: number;
  contract: string;
  planner_version: number;
  org_id: string;
  exported_at: string;
  grant: { api_key_id: string; audience: string; not_after: string };
  source_snapshot: { source: string; snapshot_id: string; read_at: string };
  shards: LifecycleShard[];
  pair_counts: { total: number; matched: number; exceptions: number };
  limits: ShardReport["max"];
  exceptions: LifecycleException[];
  aggregate_digest: string;
}

const pairKey = (p: MembershipPair) => `${p.org_id}\u0000${p.workspace_id}\u0000${p.source_user_id}`;

interface Built {
  plan: LifecyclePlan;
  shards: Shard[];
}

async function build(input: { uploads: readonly UploadInput[]; exportedAt: string; grant: GrantParameters; membership: MembershipSource }): Promise<Built> {
  const orgId = canonicalMigrationId(input.grant.orgId, "organization id");
  for (const u of input.uploads)
    if (canonicalMigrationId(u.org_id, "organization id") !== orgId)
      refuse("org_mismatch", "every upload must belong to the grant's organization (one org per lifecycle run)");
  const shardPlan = planShards({ exportedAt: input.exportedAt, uploads: input.uploads });
  const report = shardReport(shardPlan);
  // Every (org, workspace, user) pair, canonical, with the shards it is in.
  const pairs = new Map<string, { pair: MembershipPair; shards: Set<string> }>();
  for (const shard of shardPlan.shards)
    for (const m of shard.plan.manifests) {
      const pair = {
        org_id: orgId,
        workspace_id: canonicalMigrationId(m.workspace_id, "workspace id"),
        source_user_id: canonicalMigrationId(m.source_user_id, "source user id"),
      };
      const entry = pairs.get(pairKey(pair)) ?? { pair, shards: new Set<string>() };
      entry.shards.add(shard.shard_id);
      pairs.set(pairKey(pair), entry);
    }
  const ordered = [...pairs.values()].sort((a, b) => cmp(pairKey(a.pair), pairKey(b.pair)));
  const snapshot = await input.membership.read(ordered.map((p) => p.pair));
  if (!snapshot) refuse("snapshot_missing", "the membership source returned no snapshot");
  const exceptions: LifecycleException[] = [];
  let matched = 0;
  for (const { pair, shards } of ordered) {
    const verdict = snapshot!.verdict(pair);
    if (verdict === "active_member") matched += 1;
    else exceptions.push({ kind: "membership", verdict, ...pair, shard_ids: [...shards].sort() });
  }
  for (const u of shardPlan.unshardable)
    exceptions.push({ kind: "oversized_session", org_id: u.org_id, session: u.session, manifests: u.manifests, chunks: u.chunks, source_users: u.source_users, oracle_code: u.oracle_code });
  const shards: LifecycleShard[] = shardPlan.shards.map((s) => ({
    shard_id: s.shard_id,
    body_sha256: sha256Hex(canonicalMigrationJson(s.plan)),
    pairs: new Set(s.plan.manifests.map((m) => `${m.workspace_id}\u0000${m.source_user_id}`)).size,
    source_users: s.source_users,
    manifests: s.manifests,
    chunks: s.chunks,
    serialized_bytes: s.serialized_bytes,
  }));
  const grant = { api_key_id: input.grant.apiKeyId, audience: input.grant.audience, not_after: input.grant.notAfter };
  const aggregate_digest = sha256Hex(canonicalMigrationJson({
    v: LIFECYCLE_VERSION,
    contract: PLAN_CONTRACT,
    planner_version: SHARD_PLANNER_VERSION,
    org_id: orgId,
    exported_at: input.exportedAt,
    grant,
    source_snapshot_id: snapshot!.snapshot_id,
    shards: shards.map((s) => ({ shard_id: s.shard_id, body_sha256: s.body_sha256 })),
    exceptions,
  }));
  return {
    shards: shardPlan.shards,
    plan: {
      version: LIFECYCLE_VERSION,
      contract: PLAN_CONTRACT,
      planner_version: SHARD_PLANNER_VERSION,
      org_id: orgId,
      exported_at: input.exportedAt,
      grant,
      source_snapshot: { source: snapshot!.source, snapshot_id: snapshot!.snapshot_id, read_at: snapshot!.read_at },
      shards,
      pair_counts: { total: ordered.length, matched, exceptions: ordered.length - matched },
      limits: report.max,
      exceptions,
      aggregate_digest,
    },
  };
}

/** `plan`: the approvable document (ids, digests, counts, exceptions only). */
export async function lifecyclePlan(input: Parameters<typeof build>[0]): Promise<LifecyclePlan> {
  return (await build(input)).plan;
}

/** `dry-run`: the shard report and the plan summary; writes nothing. */
export async function lifecycleDryRun(input: Parameters<typeof build>[0]) {
  const { plan } = await build(input);
  return { dry_run: true as const, aggregate_digest: plan.aggregate_digest, source_snapshot: plan.source_snapshot, shards: plan.shards.length, pair_counts: plan.pair_counts, limits: plan.limits, exceptions: plan.exceptions };
}

// ---------------------------------------------------------------- RelayAuth

/** The RelayAuth #111 grant routes this command uses (`POST /v1/migration-grants`, `POST /:id/revoke`). */
export interface GrantClient {
  createGrant(request: { apiKeyId: string; audience: string; notAfter: string; plan: { exported_at: string; manifests: UploadInput[] } }): Promise<{
    grantId: string;
    orgId: string;
    planDigest: string;
    allowedPairs: { sourceUserId: string; workspaceId: string }[];
    planEntries: number;
    createdAt: string;
  }>;
  revokeGrant(grantId: string): Promise<{ grantId: string; revokedAt: string }>;
}

// ---------------------------------------------------------------- ledger

export type ShardState =
  | { status: "pending"; run_id: string; started_at: string }
  | { status: "created"; run_id: string; grant_id: string; plan_digest: string; created_at: string }
  | { status: "revoked"; run_id: string; grant_id: string; plan_digest: string; created_at: string; revoked_at: string; reason: "rollback" | "compensating" | "revoke" };

export interface Ledger {
  version: number;
  aggregate_digest: string;
  source_snapshot_id: string;
  shards: Record<string, ShardState>;
  /** Set when a run failed partway: apply refuses until a human clears it. */
  halted?: { run_id: string; shard_id: string; at: string };
}

const ledgerPath = (dir: string) => join(dir, "grant-ledger.json");

async function loadLedger(dir: string): Promise<Ledger | undefined> {
  try {
    return JSON.parse(await readFile(ledgerPath(dir), "utf8")) as Ledger;
  } catch (e) {
    if ((e as { code?: string }).code === "ENOENT") return undefined;
    throw e;
  }
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
  revoked: { shard_id: string; grant_id: string; revoked_at: string }[];
  already_revoked: { shard_id: string; grant_id: string; revoked_at: string }[];
  /** Shards whose create was started but not recorded: a human must check RelayAuth. */
  unresolved: { shard_id: string; started_at: string }[];
  failed: { shard_id: string; grant_id: string; error: string }[];
}

async function revokeShards(dir: string, ledger: Ledger, client: GrantClient, ids: (id: string, s: ShardState) => boolean, reason: RevocationEvidence["reason"]): Promise<RevocationEvidence> {
  const evidence: RevocationEvidence = { aggregate_digest: ledger.aggregate_digest, reason, revoked: [], already_revoked: [], unresolved: [], failed: [] };
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
    try {
      const out = await client.revokeGrant(state.grant_id);
      if (out.grantId !== state.grant_id) throw new Error("revoke answered for another grant");
      ledger.shards[shardId] = { ...state, status: "revoked", revoked_at: out.revokedAt, reason };
      evidence.revoked.push({ shard_id: shardId, grant_id: state.grant_id, revoked_at: out.revokedAt });
      await saveLedger(dir, ledger);
    } catch (e) {
      evidence.failed.push({ shard_id: shardId, grant_id: state.grant_id, error: e instanceof Error ? e.message.slice(0, 200) : "revoke failed" });
    }
  }
  await mkdir(dir, { recursive: true });
  const at = new Date().toISOString().replace(/[:.]/g, "-");
  await writeFile(join(dir, `revocation-${reason}-${at}-${randomUUID().slice(0, 8)}.json`), JSON.stringify(evidence, null, 2), { mode: 0o600 });
  return evidence;
}

// ---------------------------------------------------------------- apply / revoke

export interface ApplyConfig {
  execute: boolean;
  uploads: readonly UploadInput[];
  exportedAt: string;
  grant: GrantParameters;
  membership: MembershipSource;
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
  if (plan.exceptions.length) refuse("pair_exceptions", `${plan.exceptions.length} exception(s) need human review; nothing applied`);
  if (plan.aggregate_digest !== cfg.approvedDigest) refuse("digest_drift", "the recomputed aggregate digest is not the approved one");

  const prior = await loadLedger(cfg.stateDir);
  if (prior && prior.aggregate_digest !== plan.aggregate_digest) refuse("ledger_mismatch", "the state directory holds another plan's grants");
  const ledger: Ledger = prior ?? { version: LIFECYCLE_VERSION, aggregate_digest: plan.aggregate_digest, source_snapshot_id: plan.source_snapshot.snapshot_id, shards: {} };
  if (ledger.halted) refuse("previous_run_failed", "an earlier apply failed partway; check its revocation evidence and RelayAuth before going on");
  const states = Object.values(ledger.shards);
  if (states.some((s) => s.status === "pending")) refuse("partial_shard_state", "a shard's create was started but never recorded; reconcile it in RelayAuth first");
  if (states.some((s) => s.status === "revoked")) refuse("plan_rolled_back", "this plan's grants were revoked; approve a new plan to start again");

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
      const out = await cfg.client.createGrant({ apiKeyId: cfg.grant.apiKeyId, audience: cfg.grant.audience, notAfter: cfg.grant.notAfter, plan: shard.plan });
      grantId = out.grantId;
      // The grant must hold exactly this shard: re-derive with its id.
      const derived = deriveMigrationGrantPlan({ grantId: out.grantId, orgId: plan.org_id, plan: shard.plan });
      if (out.planDigest !== derived.planDigest || !sameJson(out.allowedPairs, derived.allowedPairs) || out.planEntries !== derived.plan.manifests.length || canonicalMigrationId(out.orgId, "organization id") !== plan.org_id)
        throw new Error("RelayAuth's grant does not match the shard");
      ledger.shards[shard.shard_id] = { status: "created", run_id: runId, grant_id: out.grantId, plan_digest: out.planDigest, created_at: out.createdAt };
      await saveLedger(cfg.stateDir, ledger);
      result.created.push({ shard_id: shard.shard_id, grant_id: out.grantId, plan_digest: out.planDigest });
      fresh += 1;
    } catch (e) {
      // A grant that was created but is wrong is revoked with the rest; a
      // create with no answer stays pending for a human.
      if (grantId) ledger.shards[shard.shard_id] = { status: "created", run_id: runId, grant_id: grantId, plan_digest: "", created_at: new Date(now).toISOString() };
      ledger.halted = { run_id: runId, shard_id: shard.shard_id, at: new Date(now).toISOString() };
      await saveLedger(cfg.stateDir, ledger);
      result.failed = { shard_id: shard.shard_id, error: e instanceof Error ? e.message.slice(0, 200) : "create failed" };
      result.compensation = await revokeShards(cfg.stateDir, ledger, cfg.client, (_id, s) => s.run_id === runId, "compensating");
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
  /** Omit for the rollback (revoke-all) path. */
  shardIds?: readonly string[];
}

export async function revokeGrants(cfg: RevokeConfig): Promise<RevocationEvidence> {
  if (cfg.execute !== true) refuse("execute_required", "revoke refused without --execute");
  const ledger = await loadLedger(cfg.stateDir);
  if (!ledger) refuse("ledger_missing", "no grant ledger in the state directory");
  if (ledger!.aggregate_digest !== cfg.aggregateDigest) refuse("ledger_mismatch", "the state directory holds another plan's grants");
  const only = cfg.shardIds ? new Set(cfg.shardIds) : undefined;
  return revokeShards(cfg.stateDir, ledger!, cfg.client, (id) => !only || only.has(id), only ? "revoke" : "rollback");
}

// ---------------------------------------------------------------- CLI

function argument(name: string): string | undefined {
  return process.argv.find((v) => v.startsWith(`--${name}=`))?.slice(name.length + 3);
}

/**
 * `bun run src/migration/relayhistory-export-grants.ts <dry-run|plan|apply|revoke>`.
 * dry-run and plan read local files only (`--inputs` plan inputs JSON,
 * `--membership` fixture JSON, `--org`, `--exported-at`, `--api-key-id`,
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
  };
  const out = sub === "plan" ? await lifecyclePlan(input) : await lifecycleDryRun(input);
  process.stdout.write(`${JSON.stringify(out, null, 2)}\n`);
}

if (import.meta.main)
  main().catch((error) => {
    console.error(`96bulls grants: ${error instanceof Error ? error.message : "failed"}`);
    process.exit(1);
  });
