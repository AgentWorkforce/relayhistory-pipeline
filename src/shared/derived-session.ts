/** Trust boundary for content-derived workers, independent of queue provenance. */
import { prefixAllowed } from "../../packages/pipeline/src/lib/pipeline/adapter.ts";
import { PipelineError, parseSessionKey, type ManifestV1, type StageMessage } from "./vendor.ts";
import type { HoopDb } from "./db.ts";

export function assertDerivedScope(message: StageMessage, m: ManifestV1, key: string) {
  // Native 040600 owns the authority predicate and qualified sidecar layout.
  // The legacy worker must not mutate managed-generation images before that
  // final-write/clear CAS contract is composed into this release.
  if (/(?:^|\/)upload-generations\/|\/publications\//.test(key))
    throw new PipelineError("derived_managed_authority_unavailable", "managed derived publication requires the current-image authority contract", false);
  const ctx = message.ctx;
  const s = m.session;
  const loc = parseSessionKey(key);
  const tenant = (m.extensions?.["96bulls"] as { tenant_id?: unknown } | undefined)?.tenant_id;
  const mismatch = !ctx.orgId ||
    (tenant !== undefined && tenant !== ctx.orgId) ||
    (process.env.PIPELINE_ORG_FROM_PREFIX === "true" && !prefixAllowed({ PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: process.env.PIPELINE_KEY_PREFIX }, loc?.prefix, ctx.orgId)) ||
    (process.env.TENANT_ORG_ID !== undefined && process.env.PIPELINE_ORG_FROM_PREFIX !== "true" && process.env.TENANT_ORG_ID !== ctx.orgId) ||
    [
      [ctx.workspaceId, s.workspace_id], [ctx.machineId, s.machine_id],
      [ctx.harness, s.harness], [ctx.sessionId, s.session_id],
      ...(loc ? [[loc.workspaceId, s.workspace_id], [loc.machineId, s.machine_id], [loc.harness, s.harness], [loc.sessionId, s.session_id]] : []),
    ].some(([claim, actual]) => claim !== undefined && claim !== actual);
  if (mismatch) throw new PipelineError("derived_scope_mismatch", "derived work does not match its tenant/session binding", false);
}

/** Called again under the publication transaction's row lock. Missing, erased,
 * superseded, or reset pointers can never be reconstructed by a queued job. */
export async function derivedCurrent(
  sql: Pick<HoopDb, "query">,
  message: StageMessage,
  m: ManifestV1,
  manifestKey: string,
  lock = false,
): Promise<boolean> {
  const s = m.session;
  const { rows } = await sql.query<{ revision: string; manifest_key: string; deleted_at: unknown; content_sha256: string | null }>(
    `SELECT revision::text, manifest_key, deleted_at, encode(content_sha256, 'hex') AS content_sha256
       FROM sessions.session_index
      WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4 AND machine_id=$5${lock ? " FOR UPDATE" : ""}`,
    [message.ctx.orgId, s.workspace_id, s.harness, s.session_id, s.machine_id],
  );
  const row = rows[0];
  return !!row && row.deleted_at === null && row.manifest_key === manifestKey &&
    Number(row.revision) === m.revision && row.content_sha256 === m.raw.content_sha256;
}
