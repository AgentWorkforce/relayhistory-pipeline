/** Exact-session replay from the same verified canonical source as timing and
 * turns. Search projection completeness covers only its retention window; it
 * must never substitute for reading these older objects. No new SQL or grant. */
import { CanonicalReadError, type CanonicalIdentity } from "./canonical.ts";
import type { CanonicalRows } from "./session-timing-source.ts";
import { projectedToNeonRow } from "./canonical-projection.ts";
import { toIndexRow } from "../session-pipeline/vendor/search-index/rows.ts";
import { eventRowFromIndex } from "./owner-buffered.ts";
import { keyOf, readBufferedEventPage, type EventPage, type Liveness } from "./page.ts";
import { decodeBackendCursor, decodeCursor, encodeCursor } from "./cursor.ts";
import type { EventQuery } from "./queries.ts";

export const CANONICAL_CURSOR_PREFIX = "b96c1:";
export interface CanonicalEventsDeps {
  source(identity: CanonicalIdentity, signal: AbortSignal): Promise<CanonicalRows & { generation: string; exists: boolean }>;
  deadlineMs?: number;
}
export async function canonicalEventPage(deps: CanonicalEventsDeps, liveness: Liveness,
  input: EventQuery & { sessionId: string; source: string }, rawCursor?: string): Promise<EventPage> {
  let generation: string | undefined;
  if (rawCursor) {
    if (!rawCursor.startsWith(CANONICAL_CURSOR_PREFIX) || rawCursor.length > 8192) throw new CanonicalReadError("cursor_scope", 400);
    let parsed: unknown;
    try { parsed = JSON.parse(Buffer.from(rawCursor.slice(CANONICAL_CURSOR_PREFIX.length), "base64url").toString("utf8")); }
    catch { throw new CanonicalReadError("cursor_scope", 400); }
    if (!Array.isArray(parsed) || parsed.length !== 2 || typeof parsed[0] !== "string" || !/^[a-f0-9]{64}$/.test(parsed[0]) || typeof parsed[1] !== "string") throw new CanonicalReadError("cursor_scope", 400);
    generation = parsed[0]; input = { ...input, cursor: decodeCursor(parsed[1]) };
    if (!input.cursor) throw new CanonicalReadError("cursor_scope", 400);
  }
  if (input.cursor && (input.cursor.sessionId !== input.sessionId || input.cursor.source !== input.source))
    throw new CanonicalReadError("cursor_scope", 400);
  const identity = { orgId: input.orgId, workspaceId: input.workspaceId, sessionId: input.sessionId, source: input.source };
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const snapshot = await Promise.race([
      deps.source(identity, controller.signal),
      new Promise<never>((_, reject) => { timer = setTimeout(() => { controller.abort(); reject(new CanonicalReadError("deadline")); }, deps.deadlineMs ?? 3000); }),
    ]);
    if (!snapshot.exists || !/^[a-f0-9]{64}$/.test(snapshot.generation) || generation !== undefined && generation !== snapshot.generation)
      throw new CanonicalReadError("source_generation_changed");
    const rows = snapshot.rows.filter(r => r.table === "convergence_events").map(({ row }) => {
      if (row.session_id !== identity.sessionId || row.source !== identity.source ||
          (row.org_id !== undefined && row.org_id !== identity.orgId) ||
          (row.workspace_id !== undefined && row.workspace_id !== identity.workspaceId))
        throw new CanonicalReadError("tenant_mismatch");
      const converted = eventRowFromIndex(toIndexRow(projectedToNeonRow(row,
        { ...identity, harness: identity.source }, ""), 0));
      if (!converted.event_id || !converted.machine_id || !Number.isSafeInteger(converted.ts_ms))
        throw new CanonicalReadError("event_invalid");
      return converted;
    });
    // Recheck erasure after object IO. A tombstone always wins over content.
    const erased = await liveness.erased(identity.orgId, identity.workspaceId, [{ source: identity.source, sessionId: identity.sessionId }]);
    if (erased.has(keyOf(identity))) return { events: [], nextCursor: null, erasedDropped: rows.length };
    const page = readBufferedEventPage(rows, input);
    // Reuse canonical position ordering/budget, but never mark this object
    // read as an owner-buffered source or accept a buffered continuation.
    if (page.nextCursor) page.nextCursor = CANONICAL_CURSOR_PREFIX + Buffer.from(JSON.stringify([snapshot.generation, encodeCursor(decodeBackendCursor(page.nextCursor)!.position)])).toString("base64url");
    return page;
  } finally { if (timer !== undefined) clearTimeout(timer); controller.abort(); }
}
