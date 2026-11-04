/** Managed publications never mutate canonical manifests or legacy shared keys.
 * Source/model/crypto/S3 work happens before the short final SQL transaction.
 */
import { randomUUID } from "node:crypto";
import { publicationDerivedKey } from "../../packages/object-store/src/index.ts";
import { PipelineError, type ManifestV1, type StageHandler, type StageMessage } from "../shared/vendor.ts";
import { ctxOf } from "../shared/enrich.ts";
import { log } from "../shared/log.ts";
import { probePublication, registerPublicationObject } from "../shared/publication-authority.ts";
import { withDerivedCurrent, type DerivedPublication } from "../shared/derived-publication.ts";
import { prepareSummaryObject, putPreparedSummaryObject, readSummaryObject, type SummaryKind, type SummaryRecord, type SummaryStatus } from "./summary-objects.ts";
import type { BobCostasDeps } from "./handler.ts";

export interface ManagedSummaryOutcome {
  status: SummaryStatus; summary?: string; model?: string;
  tokensIn: number; tokensOut: number; records: number;
}
/** Reconstruct with the shared builder; no alternate production layout. */
export function isPublicationSummaryKey(key: string, manifest: string, kind: SummaryKind): boolean {
  const filename = key.slice(key.lastIndexOf("/") + 1);
  if (!filename.endsWith(".bin")) return false;
  try { return String(publicationDerivedKey(manifest, filename.slice(0, -4), `summary-${kind}`)) === key; }
  catch { return false; }
}

export async function managedSummary(args: {
  kind: SummaryKind; deps: BobCostasDeps; message: StageMessage; manifest: ManifestV1;
  manifestKey: string; publication: DerivedPublication;
  recorder: Parameters<StageHandler>[1]; build: () => Promise<ManagedSummaryOutcome>;
}): ReturnType<StageHandler> {
  const { kind, deps, message, manifest: m, manifestKey, publication } = args;
  if (!publication.authority.current) return { status: "skipped", reason: "publication_not_current" };
  const s = m.session, ids = { orgId: message.ctx.orgId, workspaceId: s.workspace_id };
  const pk = [ids.orgId, s.workspace_id, s.harness, s.session_id, s.machine_id, kind];
  const fence = { db: deps.db, message, manifest: m, key: manifestKey, publication };
  const currentKeyId = (await deps.keyring.currentKey(ids)).keyId;
  const { rows } = await deps.db.query<{
    revision: string; content_sha256: string; status: string; object_key: string | null;
    object_sha256: string | null; key_id: string | null;
  }>(`SELECT revision,content_sha256,status,object_key,object_sha256,key_id FROM sessions.session_summaries
      WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4 AND machine_id=$5 AND kind=$6`, pk);
  const row = rows[0];
  if (row?.status === "ready" && Number(row.revision) === m.revision && row.content_sha256 === m.raw.content_sha256 &&
      row.key_id === currentKeyId && row.object_key && row.object_sha256 && isPublicationSummaryKey(row.object_key, manifestKey, kind)) {
    // Authenticate outside SQL. A failure performs no metadata/object clear, so
    // a delayed old read-error cannot erase a newer summary's pointer or bytes.
    const existing = await readSummaryObject({ store: deps.store, keyring: deps.keyring, ids,
      key: row.object_key, sha256: row.object_sha256, kind, revision: m.revision });
    if (existing?.status !== "ready" || !existing.summary || !existing.model || existing.source_content_sha256 !== m.raw.content_sha256)
      throw new PipelineError("summary_object_integrity", "the current encrypted summary cannot be authenticated", false);
    const reuse = await withDerivedCurrent(fence, async tx => {
      const current = await tx.query(`SELECT 1 FROM sessions.session_summaries
        WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4 AND machine_id=$5 AND kind=$6
          AND revision=$7 AND content_sha256=$8 AND status='ready' AND object_key=$9 AND object_sha256=$10 AND key_id=$11`,
        [...pk, m.revision, m.raw.content_sha256, row.object_key, row.object_sha256, currentKeyId]);
      return current.rows.length > 0;
    });
    return { status: "skipped", reason: reuse.current && reuse.value ? "summary_current" : "publication_or_summary_changed" };
  }

  // Nonready rows have no qualified object key: never reuse them across images
  // merely because revision/content happen to match after an upload reset.
  const outcome = await args.build();
  const fresh = await probePublication(deps.db, publication.evidence);
  if (!fresh.managed || !fresh.current || fresh.generation !== publication.authority.generation || fresh.revision !== publication.evidence.revision)
    return { status: "skipped", reason: "publication_not_current" };
  let prepared: Awaited<ReturnType<typeof prepareSummaryObject>> | undefined;
  if (outcome.status === "ready") {
    const key = String(publicationDerivedKey(manifestKey, randomUUID(), `summary-${kind}`));
    const record: SummaryRecord = { v: 1, kind, revision: m.revision, status: "ready",
      summary: outcome.summary!, model: outcome.model!, source_content_sha256: m.raw.content_sha256,
      built_at: (deps.now?.() ?? new Date()).toISOString(), trace_id: m.trace_id };
    prepared = await prepareSummaryObject({ keyring: deps.keyring, ids, key, record });
    // Registration is exact, generation-gated and BEFORE immutable PUT. Failed
    // job retries allocate a fresh UUID; same-key retries keep prepared bytes.
    await registerPublicationObject(deps.db, publication.evidence,
      { key: prepared.key, sha256: prepared.sha256, bytes: prepared.bytes });
    await putPreparedSummaryObject({ store: deps.store, prepared });
  }
  const ctx = ctxOf(message);
  const result = await withDerivedCurrent(fence, async tx => {
    await tx.query(`INSERT INTO sessions.session_summaries
      (org_id,workspace_id,harness,session_id,machine_id,kind,session_uuid,trace_id,revision,
       content_sha256,status,object_key,object_sha256,key_id,tokens_in,tokens_out,generated_at)
      VALUES($1,$2,$3,$4,$5,$6,$7::uuid,$8::uuid,$9,$10,$11,$12,$13,$14,$15,$16,now())
      ON CONFLICT(org_id,workspace_id,harness,session_id,machine_id,kind) DO UPDATE SET
       session_uuid=coalesce(excluded.session_uuid,sessions.session_summaries.session_uuid),
       trace_id=excluded.trace_id,revision=excluded.revision,content_sha256=excluded.content_sha256,
       status=excluded.status,object_key=excluded.object_key,object_sha256=excluded.object_sha256,key_id=excluded.key_id,
       tokens_in=excluded.tokens_in,tokens_out=excluded.tokens_out,generated_at=now()`,
      [...pk, ctx.session_uuid ?? null, m.trace_id, m.revision, m.raw.content_sha256, outcome.status,
        prepared?.key ?? null, prepared?.sha256 ?? null, prepared?.keyId ?? null, outcome.tokensIn, outcome.tokensOut]);
  });
  if (!result.current) return { status: "skipped", reason: "publication_not_current" };
  // SQL holds pointers only; immutable canonical images and previous encrypted
  // objects are left to Native's registered-object GC/reset protocol.
  args.recorder.log.info("summary written", { kind, status: outcome.status, tokens_in: outcome.tokensIn, tokens_out: outcome.tokensOut });
  log.info("bob costas managed summary", { stage: `summary-${kind}`, kind, status: outcome.status,
    model_id: outcome.model, tokens_in: outcome.tokensIn, tokens_out: outcome.tokensOut,
    trace_id: m.trace_id, revision: m.revision, count: outcome.records });
}
