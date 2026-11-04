/**
 * Referee: the session-state classifier. Consumes `status-classify`. Rules only
 * by default (no model call): closed raw stream = done; no activity for
 * REFEREE_ABANDON_AFTER_MINUTES = abandoned; otherwise active. Writes the
 * verdict through the legacy state RPC or Native's qualified publication RPC,
 * with the verdict and directory projection in the same fenced transaction.
 */
import {
  PipelineError,
  type ManifestReader,
  type RawPointer,
  type StageHandler,
} from "../shared/vendor.ts";
import type { HoopDb } from "../shared/db.ts";
import { ctxOf } from "../shared/enrich.ts";
import { integer } from "../shared/env.ts";
import { log } from "../shared/log.ts";
import { projectSessionDirectory } from "../hoopdb/session-directory-projector.ts";
import { assertDerivedScope } from "../shared/derived-session.ts";
import { derivedPublication, withDerivedCurrent } from "../shared/derived-publication.ts";
import { setPublicationState } from "../shared/publication-authority.ts";

export type Verdict = {
  state: "active" | "done" | "abandoned" | "unknown";
  rule: string;
};

export function classify(input: {
  closed: boolean;
  lastActivity: Date;
  endedAt?: Date;
  now: Date;
  abandonAfterMinutes: number;
}): Verdict {
  if (input.closed || input.endedAt)
    return {
      state: "done",
      rule: input.closed ? "raw_closed" : "ended_at_present",
    };
  const idleMinutes =
    (input.now.getTime() - input.lastActivity.getTime()) / 60_000;
  if (idleMinutes > input.abandonAfterMinutes)
    return { state: "abandoned", rule: "idle_beyond_threshold" };
  return { state: "active", rule: "recent_activity" };
}

export function refereeHandler(deps: {
  db: HoopDb;
  reader: ManifestReader;
  now?: () => Date;
  schemaGeneration?: string;
}): StageHandler {
  const now = deps.now ?? (() => new Date());
  const abandonAfterMinutes = integer("REFEREE_ABANDON_AFTER_MINUTES", 24 * 60);
  return async (message, recorder) => {
    const pointer = message.payload as RawPointer;
    if (!pointer || typeof pointer.manifestKey !== "string")
      throw new PipelineError(
        "invalid_pointer",
        "status-classify payload is not a raw pointer",
        false,
      );
    const read = await deps.reader.read(pointer.manifestKey);
    if (!read) return { status: "skipped", reason: "manifest_missing" };
    const m = read.manifest;
    assertDerivedScope(message, m, pointer.manifestKey, deps.schemaGeneration !== undefined);
    const publication = await derivedPublication(deps.db, message, m, pointer.manifestKey, deps.schemaGeneration);
    if (publication?.authority.managed && !publication.authority.current)
      return { status: "skipped", reason: "publication_not_current" };
    const s = m.session;
    const lastActivity = new Date(s.ended_at ?? m.raw.updated_at);
    const verdict = classify({
      closed: m.raw.state === "closed",
      lastActivity,
      ...(s.ended_at ? { endedAt: new Date(s.ended_at) } : {}),
      now: now(),
      abandonAfterMinutes,
    });
    const classifiedAt = now();
    const ctx = ctxOf(message);
    let applied = false;
    const result = await withDerivedCurrent({ db: deps.db, message, manifest: m,
      key: pointer.manifestKey, publication }, async (client, authority) => {
      const managed = authority?.managed === true;
      if (managed) applied = await setPublicationState(client, publication!.evidence, verdict.state, classifiedAt);
      else {
        const { rows } = await client.query<{ set_session_state: boolean }>(
          "SELECT sessions.set_session_state($1,$2,$3,$4,$5,$6,$7::timestamptz)",
          [
            message.ctx.orgId,
            s.workspace_id,
            s.harness,
            s.session_id,
            s.machine_id,
            verdict.state,
            classifiedAt.toISOString(),
          ],
        );
        applied = rows[0]?.set_session_state ?? false;
      }
      if (managed && !applied) return;
      // set_session_state updates only the pointer; re-project the logical session so
      // the directory's state (and its p_state filter) follows the classification.
      if (applied)
        await projectSessionDirectory(client as never, {
          orgId: message.ctx.orgId,
          workspaceId: s.workspace_id,
          harness: s.harness,
          sessionId: s.session_id,
        });
      await client.query(
        `INSERT INTO sessions.session_verdicts
           (org_id, workspace_id, harness, session_id, machine_id, session_uuid, trace_id, revision, state, rule, model_id, classified_at)
         VALUES ($1,$2,$3,$4,$5,$6::uuid,$7::uuid,$8,$9,$10,'rules/v1',$11::timestamptz)
         ON CONFLICT (org_id, workspace_id, harness, session_id, machine_id, trace_id, revision) ${managed
          ? "DO UPDATE SET state=excluded.state,rule=excluded.rule,model_id=excluded.model_id,classified_at=excluded.classified_at WHERE sessions.session_verdicts.classified_at<=excluded.classified_at"
          : "DO NOTHING"}`,
        [
          message.ctx.orgId,
          s.workspace_id,
          s.harness,
          s.session_id,
          s.machine_id,
          ctx.session_uuid ?? null,
          m.trace_id,
          m.revision,
          verdict.state,
          verdict.rule,
          classifiedAt.toISOString(),
        ],
      );
    });
    if (!result.current) return { status: "skipped", reason: publication ? "publication_or_index_not_current" : "index_not_current" };
    recorder.log.info("verdict", { state: verdict.state, rule: verdict.rule });
    log.info("referee verdict", {
      stage: "status-classify",
      state: verdict.state,
      rule: verdict.rule,
      model_id: "rules/v1",
      trace_id: m.trace_id,
      revision: m.revision,
      session_uuid: ctx.session_uuid,
      ok: applied,
    });
  };
}
