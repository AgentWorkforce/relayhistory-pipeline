/**
 * Bob Costas: summaries. Consumes `summary-short` and `summary-long`. Reads
 * the scrubbed chunks from the Locker (never the unscrubbed source), builds a
 * digest, summarises, and upserts `sessions.session_summaries`. Idempotent by
 * the manifest's content hash.
 */
import { zstdDecompressSync } from "node:zlib";
import { PipelineError, rawChunkKey, readAll, type ManifestReader, type ObjectStore, type RawPointer, type StageHandler } from "../shared/vendor.ts";
import type { HoopDb } from "../shared/db.ts";
import { ctxOf } from "../shared/enrich.ts";
import { log } from "../shared/log.ts";
import { digestRecords, type Summarizer } from "./summarizer.ts";

export function bobCostasHandler(kind: "short" | "long", deps: { db: HoopDb; reader: ManifestReader; store: ObjectStore; summarizer: Summarizer }): StageHandler {
  return async (message, recorder) => {
    const pointer = message.payload as RawPointer;
    if (!pointer || typeof pointer.manifestKey !== "string")
      throw new PipelineError("invalid_pointer", `summary-${kind} payload is not a raw pointer`, false);
    const read = await deps.reader.read(pointer.manifestKey);
    if (!read) return { status: "skipped", reason: "manifest_missing" };
    const m = read.manifest;
    const s = m.session;
    const pk = [message.ctx.orgId, s.workspace_id, s.harness, s.session_id, s.machine_id];
    const existing = await deps.db.query<{ content_sha256: string }>(
      `SELECT content_sha256 FROM sessions.session_summaries
        WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4 AND machine_id=$5 AND kind=$6`,
      [...pk, kind],
    );
    if (existing.rows[0]?.content_sha256 === m.raw.content_sha256) return { status: "skipped", reason: "summary_current" };
    if (!m.raw.scrub) throw new PipelineError("unscrubbed_session", "refusing to summarise an unscrubbed session", false);
    const loc = { workspaceId: s.workspace_id, machineId: s.machine_id, harness: s.harness, sessionId: s.session_id, startDate: s.started_at.slice(0, 10) };
    const lines: string[] = [];
    for (const chunk of m.raw.chunks) {
      const key = rawChunkKey(loc, chunk.seq, chunk.start_offset, chunk.sha256_8, m.raw.format);
      let stored: Uint8Array;
      try {
        stored = await readAll(await deps.store.get(key));
      } catch (err) {
        const code = (err as { code?: string }).code;
        if (code === "not_found") throw new PipelineError("chunk_missing", "a listed chunk is not in object storage", true);
        throw err;
      }
      const bytes = zstdDecompressSync(Buffer.from(stored));
      lines.push(...bytes.toString("utf8").split("\n"));
    }
    const digest = digestRecords(s.harness, lines);
    const summary = await deps.summarizer.summarize(kind, digest);
    const ctx = ctxOf(message);
    await deps.db.query(
      `INSERT INTO sessions.session_summaries
         (org_id, workspace_id, harness, session_id, machine_id, kind, session_uuid, trace_id, revision,
          content_sha256, summary, model_id, tokens_in, tokens_out, generated_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7::uuid,$8::uuid,$9,$10,$11,$12,$13,$14, now())
       ON CONFLICT (org_id, workspace_id, harness, session_id, machine_id, kind) DO UPDATE SET
         session_uuid = coalesce(excluded.session_uuid, sessions.session_summaries.session_uuid),
         trace_id = excluded.trace_id, revision = excluded.revision, content_sha256 = excluded.content_sha256,
         summary = excluded.summary, model_id = excluded.model_id, tokens_in = excluded.tokens_in,
         tokens_out = excluded.tokens_out, generated_at = now()`,
      [...pk, kind, ctx.session_uuid ?? null, m.trace_id, m.revision, m.raw.content_sha256, summary.text,
       summary.modelId, summary.tokensIn, summary.tokensOut],
    );
    recorder.log.info("summary written", { kind, model_id: summary.modelId, tokens_in: summary.tokensIn, tokens_out: summary.tokensOut });
    log.info("bob costas summary", {
      stage: `summary-${kind}`, kind, model_id: summary.modelId, tokens_in: summary.tokensIn, tokens_out: summary.tokensOut,
      trace_id: m.trace_id, revision: m.revision, session_uuid: ctx.session_uuid, count: digest.records,
    });
  };
}
