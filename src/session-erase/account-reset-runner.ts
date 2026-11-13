// Continuous pipeline service. The Cloud connection owns a session advisory lock;
// process loss releases it. The account transaction commits its cleanup targets
// together with revocation. External effects replay from those durable targets.
import pg from "pg";
import { rootCertificates } from "node:tls";
import { randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { ownerClientConfig } from "../shared/owner-pg-config.ts";
import {
  scrubAccount,
  privateOrganizations,
  relayTargets,
} from "./account-reset-sql.ts";
import {
  runUserEraseMain,
  databaseUrl,
  exitCodeForError,
  userEraseEnabled,
} from "./user-erase-main.ts";
import {
  sessionEraseConfig,
  sessionEraseCredentials,
} from "./selfhost-eraser-main.ts";
import { CONFIRMATION } from "./user-erase.ts";

interface HistoryTarget {
  orgId: string;
  workspaceId: string;
  requestId: string;
  legacy: boolean;
}
interface ResetJob {
  user_id: string;
  confirmation: string | null;
  scope_orgs: string[];
  legacy_relay_only: boolean;
  attempts: number;
  deferred_error: string | null;
  phase: "account" | "history" | "relaycast";
  history_targets: HistoryTarget[];
  relay_targets: { id: string; provider_id: string | null }[];
}
class Stalled extends Error {}

type Workspace = { org_id: string; workspace_id: string };
type FindWorkspaces = (orgs: string[], user: string) => Promise<Workspace[]>;

async function resetAccount(
  db: pg.Client,
  job: ResetJob,
  findWorkspaces: FindWorkspaces,
) {
  await db.query("BEGIN");
  try {
    await db.query("SET LOCAL lock_timeout='30s'");
    await db.query("SET LOCAL statement_timeout='15min'");
    if (!job.confirmation) throw new Stalled("confirmation_missing");
    const confirmed = await db.query(
      "SELECT id FROM users WHERE id=$1 AND primary_email=$2 FOR UPDATE",
      [job.user_id, job.confirmation],
    );
    if (!confirmed.rows.length) throw new Stalled("confirmation_changed");
    const scoped = await db.query(privateOrganizations, [
      job.user_id,
      job.scope_orgs,
    ]);
    const orgs: string[] = scoped.rows.map((r) => r.org_id);
    const targets = new Map<string, HistoryTarget>();
    const capture = (workspace: Workspace, legacy = false) => {
      if (!orgs.includes(workspace.org_id))
        throw new Stalled("workspace_scope_mismatch");
      const key = JSON.stringify([workspace.org_id, workspace.workspace_id]);
      const target = targets.get(key);
      if (target) {
        target.legacy ||= legacy;
        return;
      }
      targets.set(key, {
        orgId: workspace.org_id,
        workspaceId: workspace.workspace_id,
        requestId: randomUUID(),
        legacy,
      });
    };
    if (orgs.length) {
      for (const row of await findWorkspaces(orgs, job.user_id)) capture(row);
      const cloud = await db.query<Workspace>(
        "SELECT organization_id::text AS org_id,id::text AS workspace_id FROM workspaces WHERE organization_id::text=ANY($1::text[])",
        [orgs],
      );
      for (const row of cloud.rows) capture(row);
    }
    const catalog = await db.query<{ name: string }>(
      `SELECT relname AS name FROM pg_class
      WHERE relnamespace=to_regnamespace('sessions') AND relname=ANY($1::text[])`,
      [
        [
          "delivery_records",
          "uploaded_support_identities",
          "convergence_events",
          "session_outcomes",
          "auth_sessions",
          "probe_connections",
        ],
      ],
    );
    for (const { name } of catalog.rows) {
      // Names come only from the fixed catalog filter. Historical workspaces
      // without Cloud rows must still be captured before credentials are revoked.
      const rows = await db.query<Workspace>(
        `SELECT DISTINCT org_id,workspace_id FROM sessions.${name}
        WHERE user_id=$1 AND org_id=ANY($2::text[])`,
        [job.user_id, orgs],
      );
      for (const row of rows.rows)
        capture(row, name !== "auth_sessions" && name !== "probe_connections");
    }
    if (catalog.rows.some((r) => r.name === "auth_sessions"))
      await db.query(
        "UPDATE sessions.auth_sessions SET revoked_at=now(),updated_at=now(),revoked_reason='admin_user_reset' WHERE user_id=$1 AND revoked_at IS NULL",
        [job.user_id],
      );
    if (catalog.rows.some((r) => r.name === "probe_connections"))
      await db.query(
        "DELETE FROM sessions.probe_connections WHERE user_id=$1 AND org_id=ANY($2::text[])",
        [job.user_id, orgs],
      );
    // Short commit window: prevent a new membership or binding from slipping
    // between the recheck and snapshot. No network call occurs under these locks.
    await db.query(
      "LOCK TABLE organization_memberships IN SHARE ROW EXCLUSIVE MODE NOWAIT",
    );
    const rechecked = await db.query(privateOrganizations, [job.user_id, orgs]);
    if (rechecked.rows.length !== orgs.length)
      throw new Stalled("membership_changed");
    const relay = await db.query(relayTargets, [job.user_id, orgs]);
    const safe = job.relay_targets.filter((target) =>
      relay.rows.some(
        (row) => row.id === target.id && row.provider_id === target.provider_id,
      ),
    );
    if (
      !(await db.query(scrubAccount, [job.user_id, job.confirmation])).rows
        .length
    )
      throw new Stalled("confirmation_changed");
    await db.query(
      `UPDATE public.admin_account_resets SET phase='relaycast', confirmation=NULL,
      history_targets=$2, relay_targets=$3, scope_orgs=$4,attempts=0,error_code=NULL,updated_at=now(),run_after=now()
      WHERE user_id=$1`,
      [
        job.user_id,
        JSON.stringify([...targets.values()]),
        JSON.stringify(safe),
        orgs,
      ],
    );
    await db.query("COMMIT");
  } catch (error) {
    await db.query("ROLLBACK");
    throw error;
  }
}

async function deleteWorkspace(
  db: pg.Client,
  target: ResetJob["relay_targets"][number],
  origin: string,
  job: ResetJob,
  send: typeof fetch,
) {
  if (!target.provider_id || !/^[A-Za-z0-9_-]{1,128}$/.test(target.provider_id))
    throw new Stalled("relaycast_provider_id_missing_or_invalid");
  let key: string;
  await db.query("BEGIN");
  try {
    // Authorize against live bindings and ownership. Freeze a verified target
    // before the external effect. Future bindings cannot adopt a retired ID.
    await db.query(
      "LOCK TABLE workspaces,organization_memberships,organizations,relay_workspaces IN SHARE MODE NOWAIT",
    );
    const { rows } = await db.query(
      `SELECT rw.relaycast_api_key FROM relay_workspaces rw
      WHERE rw.id=$1 AND rw.relaycast_workspace_id=$2 AND rw.owner_user_id=$3 AND NOT EXISTS (
        SELECT 1 FROM workspaces w WHERE w.relay_workspace_id=rw.id AND (
          NOT (w.organization_id::text=ANY($4::text[])) OR NOT EXISTS (SELECT FROM organizations o WHERE o.id=w.organization_id AND o.created_by_user_id=$3) OR EXISTS (
          SELECT 1 FROM organization_memberships m WHERE m.organization_id=w.organization_id AND m.user_id<>$3)))`,
      [target.id, target.provider_id, job.user_id, job.scope_orgs],
    );
    if (!rows.length) throw new Stalled("relaycast_scope_or_identity_changed");
    if (!rows[0].relaycast_api_key) throw new Stalled("relaycast_key_missing");
    await db.query(
      `INSERT INTO public.admin_reset_retired_workspaces(registry_id,provider_id,user_id)
      VALUES($1,$2,$3) ON CONFLICT(registry_id) DO NOTHING`,
      [target.id, target.provider_id, job.user_id],
    );
    await db.query("UPDATE relay_workspaces SET updated_at=now() WHERE id=$1", [
      target.id,
    ]);
    await db.query(
      "UPDATE workspaces SET relay_workspace_id=NULL WHERE relay_workspace_id=$1",
      [target.id],
    );
    key = rows[0].relaycast_api_key;
    await db.query("COMMIT");
  } catch (error) {
    await db.query("ROLLBACK");
    throw error;
  }
  // The engine atomically verifies the explicit provider ID against the key.
  const response = await send(
    new URL("/v1/workspaces/" + encodeURIComponent(target.provider_id), origin),
    {
      method: "DELETE",
      redirect: "error",
      signal: AbortSignal.timeout(15000),
      headers: { authorization: "Bearer " + key },
    },
  );
  if (response.status === 401 || response.status === 404)
    throw new Stalled("relaycast_deletion_unverified");
  if (response.status !== 204) throw new Error("relaycast_unavailable");
}

// Older History resets can invalidate a whole workspace's derived daily digest.
// Keep such an aggregate intact if another contributor has any evidence there.
// The snapshot includes both the ownership check and the native reset.
async function resetLegacy(
  db: pg.Client,
  job: ResetJob,
  target: HistoryTarget,
) {
  await db.query("BEGIN ISOLATION LEVEL SERIALIZABLE");
  try {
    await db.query("SET LOCAL lock_timeout='30s'");
    await db.query("SET LOCAL statement_timeout='15min'");
    const privateOwner = await db.query(
      `SELECT 1 FROM organizations o WHERE o.id::text=$1 AND o.created_by_user_id=$2
      AND NOT EXISTS(SELECT FROM organization_memberships m WHERE m.organization_id=o.id AND m.user_id<>$2)`,
      [target.orgId, job.user_id],
    );
    if (!privateOwner.rows.length)
      throw new Stalled("organization_became_shared");
    const relations = await db.query<{ relname: string }>(
      `SELECT relname FROM pg_class
      WHERE relnamespace=to_regnamespace('sessions') AND relname=ANY($1::text[])`,
      [
        [
          "daily_digests",
          "delivery_records",
          "uploaded_support_identities",
          "auth_sessions",
          "probe_connections",
          "convergence_events",
          "session_outcomes",
          "conversation_turns",
        ],
      ],
    );
    const sources = relations.rows.map((r) => r.relname);
    if (sources.includes("daily_digests")) {
      const digest = await db.query(
        "SELECT 1 FROM sessions.daily_digests WHERE org_id=$1 AND workspace_id=$2 LIMIT 1",
        [target.orgId, target.workspaceId],
      );
      if (digest.rows.length) {
        for (const name of sources.filter((n) => n !== "daily_digests")) {
          // Older conversation_turns carries a capture source as session_owner,
          // so a turn cannot prove sole authorship of a shared digest.
          const identifiesUser = name !== "conversation_turns";
          const other = await db.query(
            `SELECT 1 FROM sessions.${name} WHERE org_id=$1 AND workspace_id=$2 ${identifiesUser ? "AND user_id IS DISTINCT FROM $3" : ""} LIMIT 1`,
            identifiesUser
              ? [target.orgId, target.workspaceId, job.user_id]
              : [target.orgId, target.workspaceId],
          );
          if (other.rows.length)
            throw new Stalled("legacy_digest_has_other_contributors");
        }
      }
    }
    await db.query("SELECT sessions.reset_uploaded_sessions($1,$2,$3,$4)", [
      target.orgId,
      target.workspaceId,
      job.user_id,
      target.requestId,
    ]);
    // Older independent ingest rows need their owning erasure path. A native
    // reset returning a count alone does not prove that these bytes are gone.
    for (const name of [
      "delivery_records",
      "convergence_events",
      "session_outcomes",
    ]) {
      if (!sources.includes(name)) continue;
      const remains = await db.query(
        `SELECT 1 FROM sessions.${name} WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3 LIMIT 1`,
        [target.orgId, target.workspaceId, job.user_id],
      );
      if (remains.rows.length)
        throw new Stalled("legacy_owned_history_remains");
    }
    await db.query("COMMIT");
  } catch (error) {
    await db.query("ROLLBACK");
    throw error;
  }
}

interface ResetPorts {
  findWorkspaces: FindWorkspaces;
  erase?: typeof runUserEraseMain;
  fetch?: typeof fetch;
}
export async function advanceReset(
  db: pg.Client,
  job: ResetJob,
  env: NodeJS.ProcessEnv,
  origin: string,
  ports: ResetPorts,
) {
  if (job.phase === "account")
    return resetAccount(db, job, ports.findWorkspaces);
  if (job.phase === "history") {
    const [target, ...rest] = job.history_targets;
    if (target) {
      if (!job.scope_orgs.includes(target.orgId))
        throw new Stalled("workspace_scope_mismatch");
      const sharing = await db.query(
        "SELECT 1 FROM organization_memberships WHERE organization_id::text=$1 AND user_id<>$2 LIMIT 1",
        [target.orgId, job.user_id],
      );
      if (sharing.rows.length) throw new Stalled("organization_became_shared");
      if (target.legacy) await resetLegacy(db, job, target);
      const result = await (ports.erase ?? runUserEraseMain)(env, () =>
        JSON.stringify({
          request_id: target.requestId,
          org_id: target.orgId,
          workspace_id: target.workspaceId,
          user_id: job.user_id,
          confirmation: CONFIRMATION,
        }),
      );
      if (!result.enabled || !result.verified)
        throw new Error("history_unverified");
    }
    await finishTarget(db, job, rest);
    return;
  }
  if (job.phase !== "relaycast") throw new Stalled("invalid_job_phase");
  const [target, ...rest] = job.relay_targets;
  if (target)
    await deleteWorkspace(db, target, origin, job, ports.fetch ?? fetch);
  await finishTarget(db, job, rest);
}

// The other cleanup can finish even when one provider needs operator action.
function otherPhase(job: ResetJob) {
  if (job.phase === "history" && job.relay_targets.length) return "relaycast";
  if (job.phase === "relaycast" && job.history_targets.length) return "history";
  return null;
}

async function finishTarget(db: pg.Client, job: ResetJob, rest: unknown[]) {
  const column = job.phase === "history" ? "history_targets" : "relay_targets";
  const next = rest.length ? job.phase : (otherPhase(job) ?? "complete");
  const restore =
    next !== job.phase && next !== "complete" ? job.deferred_error : null;
  await db.query(
    `UPDATE public.admin_account_resets SET ${column}=$2,phase=$3,
    stalled=$4,error_code=$5,deferred_error=$6,attempts=0,updated_at=now(),run_after=now() WHERE user_id=$1`,
    [
      job.user_id,
      JSON.stringify(rest),
      next,
      Boolean(restore),
      restore,
      rest.length ? job.deferred_error : null,
    ],
  );
}

export async function recordResetFailure(
  db: pg.Client,
  job: ResetJob,
  error: unknown,
) {
  const sqlstate = (error as { code?: string })?.code;
  const code =
    error instanceof Stalled
      ? error.message
      : sqlstate && /^[A-Z0-9]{5}$/.test(sqlstate)
        ? sqlstate
        : job.phase + "_failed";
  const terminal =
    error instanceof Stalled ||
    exitCodeForError(error) === 2 ||
    job.attempts >= 4;
  const other = terminal && !job.deferred_error ? otherPhase(job) : null;
  await db.query(
    `UPDATE public.admin_account_resets SET phase=$3,attempts=$4,error_code=$5,stalled=$6,
    deferred_error=$7,run_after=now()+interval '1 minute' * $8,updated_at=now()
    WHERE user_id=$1 AND phase=$2`,
    [
      job.user_id,
      job.phase,
      other ?? job.phase,
      other ? 0 : job.attempts + 1,
      other ? null : code,
      terminal && !other,
      other ? code : job.deferred_error,
      other ? 0 : job.attempts + 1,
    ],
  );
}

async function run(env: NodeJS.ProcessEnv) {
  if (!userEraseEnabled(env)) throw new Error("eraser_disabled");
  const url = new URL(env.RELAYCAST_URL ?? "");
  if (url.protocol !== "https:" || url.origin !== env.RELAYCAST_URL)
    throw new Error("relaycast_config_invalid");
  if (sessionEraseConfig(env).dryRun) throw new Error("dry_run_configured");
  sessionEraseCredentials(env);
  const history = new pg.Client(
    ownerClientConfig(databaseUrl(env), "account-reset-preflight"),
  );
  await history.connect();
  try {
    const check = await history.query(
      "SELECT has_function_privilege(current_user,'sessions.operator_user_erase_begin(text,text,text,text)','EXECUTE') AS ready",
    );
    if (check.rows[0]?.ready !== true) throw new Error("eraser_unavailable");
  } catch (error) {
    await history.end();
    throw error;
  }
  // Historical workspaces can outlive their Cloud row. Admission fence owners
  // cover buffered-only tenants and the catalog/frozen rows cover older imports.
  const findWorkspaces: FindWorkspaces = async (orgs, user) => {
    const tables = [
      "ingest_fence_owners",
      "session_index",
      "ingest_frozen_source_owned_sessions",
      "ingest_frozen_source_records",
      "auth_sessions",
      "read_api_tokens",
    ];
    const sql = tables
      .map(
        (t) =>
          `SELECT workspace_id,org_id FROM sessions.${t} WHERE org_id=ANY($1::text[]) AND user_id=$2`,
      )
      .join(" UNION ");
    return (await history.query<Workspace>(sql, [orgs, user])).rows;
  };
  const db = new pg.Client({
    ...ownerClientConfig(
      env.ACCOUNT_RESET_CLOUD_DATABASE_URL ?? "",
      "account-reset",
      rootCertificates.join("\n"),
    ),
    keepAlive: true,
  });
  try {
    await db.connect();
    // One dispatcher at a time. Never queue behind a long-running erase.
    if (
      !(await db.query("SELECT pg_try_advisory_lock(961996,58011) AS held"))
        .rows[0]?.held
    )
      return;
    for (;;) {
      const { rows } =
        await db.query<ResetJob>(`SELECT user_id,confirmation,phase,history_targets,relay_targets,scope_orgs,legacy_relay_only,attempts,deferred_error
        FROM public.admin_account_resets WHERE NOT stalled AND phase<>'complete' AND run_after<=now()
        ORDER BY run_after,user_id LIMIT 1`);
      const job = rows[0];
      if (!job) {
        await sleep(5000);
        continue;
      }
      try {
        await advanceReset(db, job, env, url.origin, { findWorkspaces });
      } catch (error) {
        await recordResetFailure(db, job, error);
      }
    }
  } finally {
    await Promise.all([db.end(), history.end()]);
  }
}
if (import.meta.main)
  run(process.env).catch(() => {
    console.error("account_reset_runner_failed");
    process.exitCode = 1;
  });
