/** Offline, opt-in action scope. Consumes only manifest-pinned public sidecar bytes, never reverse maps. */
import { PROVENANCE_BOUNDS, PROVENANCE_FORMAT, type ActionRecord } from "../import/provenance.ts";
import type { Snapshot, SnapshotSession } from "../replay/snapshot.ts";
import { canonicalJson, isoTime, isRecord, onlyKeys, sha256Hex } from "../replay/util.ts";

export const ACTION_FILE = "action-provenance.json";
export type ActionKind = ActionRecord["kind"];
export const actionKey = (s: Pick<SnapshotSession, "harness" | "sessionId">, kind: ActionKind, f: { eventId: string; ts: string }) =>
  JSON.stringify([s.harness, s.sessionId, kind, f.eventId, f.ts]);
const fields = ["harness", "sessionId", "eventId", "kind", "factAt", "sourceAt", "location", "effectiveLocation", "startingLocation", "target", "source", "reason", "repository", "repositoryTarget", "repositoryBasis", "repositoryClaims", "availableAt", "repositoryHint"];
const definition = { format: PROVENANCE_FORMAT, rules: "static-action-scope/1", bounds: PROVENANCE_BOUNDS, identity: "repository-origin-claim/1", status: "capture_only", legacySessionScopeUnchanged: true,
  consumedByReplay: false, trust: "operator-and-trace-claims-not-independent-verification" };
function fail(): never { throw new Error("action scope: invalid, unbound or inconsistent provenance; values withheld"); }
const hash = (v: unknown, prefix = "") => typeof v === "string" && new RegExp(`^${prefix}[a-f0-9]{${prefix ? 24 : 64}}$`, "u").test(v);
const stamp = (v: unknown) => { const t = isoTime(v, "time"); if (t !== v) fail(); return t; };
const id = (v: unknown, max = 512) => { if (typeof v !== "string" || !v || v.length > max || /[\u0000-\u001f\u007f]/u.test(v)) fail(); };

/** Exact per-fact join, including kind and timestamp: an event id alone is not an action identity. */
export function readActionScope(snapshot: Snapshot, now: Date): Map<string, ActionRecord> {
  try {
    const text = snapshot.extraFiles?.[ACTION_FILE];
    if (text === undefined || Buffer.byteLength(text) > PROVENANCE_BOUNDS.exportBytes || sha256Hex(text) !== snapshot.manifest.files[ACTION_FILE]) fail();
    const raw: unknown = JSON.parse(text);
    if (!isRecord(raw)) fail();
    onlyKeys(raw, [...Object.keys(definition), "sessionsSha256", "implementationSha256", "counts", "definitionSha256", "deleteBy", "repositoryMapFingerprint", "records"], "sidecar");
    for (const [k, v] of Object.entries(definition)) {
      if (k === "bounds") {
        if (!isRecord(raw.bounds)) fail();
        onlyKeys(raw.bounds, Object.keys(PROVENANCE_BOUNDS), "bounds");
        for (const [key, n] of Object.entries(PROVENANCE_BOUNDS)) if (raw.bounds[key] !== n) fail();
      } else if (raw[k] !== v) fail();
    }
    if (raw.definitionSha256 !== sha256Hex(JSON.stringify(definition)) || !hash(raw.implementationSha256)
      || !hash(raw.sessionsSha256) || raw.sessionsSha256 !== snapshot.manifest.files["sessions.jsonl"]
      || (raw.repositoryMapFingerprint !== null && !(typeof raw.repositoryMapFingerprint === "string" && /^m_[a-f0-9]{64}$/u.test(raw.repositoryMapFingerprint)))) fail();
    const deleteBy = stamp(raw.deleteBy);
    if (deleteBy <= now.toISOString() || deleteBy !== snapshot.manifest.deleteBy) fail();
    if (!Array.isArray(raw.records) || raw.records.length > PROVENANCE_BOUNDS.actions) fail();
    const facts = new Map<string, string>();
    for (const s of snapshot.sessions) for (const [kind, list] of [["write", s.writes], ["commit", s.commits], ["outcome", s.outcomes]] as const) for (const f of list) {
      const key = actionKey(s, kind, f), value = canonicalJson(f);
      if (facts.has(key) && facts.get(key) !== value) fail();
      facts.set(key, value);
    }
    const records = new Map<string, ActionRecord>();
    for (const r of raw.records) {
      if (!isRecord(r)) fail();
      onlyKeys(r, fields, "record");
      if (Object.keys(r).length !== fields.length) fail();
      id(r.harness, 64); id(r.sessionId); id(r.eventId);
      if (!["write", "commit", "outcome"].includes(r.kind as string)) fail();
      const factAt = stamp(r.factAt), sourceAt = stamp(r.sourceAt), availableAt = stamp(r.availableAt);
      if (availableAt < factAt || availableAt < sourceAt) fail();
      for (const k of ["location", "effectiveLocation", "startingLocation", "target", "repositoryTarget", "repositoryHint"]) if (r[k] !== null && !hash(r[k], "l_")) fail();
      if (r.repository !== null && !hash(r.repository, "r_")) fail();
      if (!["tool_workdir", "session_cwd", "record_cwd", "generic_event", "code_mode_projection", "unknown"].includes(r.source as string)
        || !["literal", "missing_directory", "unsupported_shell", "dynamic_workdir", "projection_scope_unavailable"].includes(r.reason as string)) fail();
      if (!Array.isArray(r.repositoryClaims) || r.repositoryClaims.length > 2) fail();
      const bases = new Set<string>(), repositories = new Set<string>();
      for (const c of r.repositoryClaims) {
        if (!isRecord(c)) fail();
        onlyKeys(c, ["repository", "basis", "availableAt"], "claim");
        if (!hash(c.repository, "r_") || !["operator_declared", "trace_declared"].includes(c.basis as string) || bases.has(c.basis as string) || stamp(c.availableAt) > availableAt) fail();
        bases.add(c.basis as string); repositories.add(c.repository as string);
      }
      if (r.repositoryBasis === "unattributed") { if (r.repository !== null || repositories.size) fail(); }
      else if (r.repositoryBasis === "conflicting_claims") { if (r.repository !== null || repositories.size !== 2) fail(); }
      else if (!["operator_declared", "trace_declared"].includes(r.repositoryBasis as string) || !bases.has(r.repositoryBasis as string) || repositories.size !== 1 || !repositories.has(r.repository as string)) fail();
      if (r.repository === null && r.repositoryTarget !== null) fail();
      // Projection facts cannot recover an individual nested call's working directory.
      if ((r.source === "code_mode_projection" || r.reason === "projection_scope_unavailable") && (r.repository !== null || r.repositoryTarget !== null)) fail();
      const record = r as unknown as ActionRecord;
      const key = actionKey(record, record.kind, { ts: factAt, eventId: record.eventId });
      if (!facts.has(key) || records.has(key)) fail();
      records.set(key, record);
    }
    const values = [...records.values()];
    const counts = { records: values.length, attributedRecords: values.filter((r) => r.repository !== null).length,
      operatorDeclared: values.filter((r) => r.repositoryBasis === "operator_declared").length, traceDeclared: values.filter((r) => r.repositoryBasis === "trace_declared").length,
      unattributed: values.filter((r) => r.repository === null).length, conflictingClaims: values.filter((r) => r.repositoryBasis === "conflicting_claims").length, availableAfterFact: values.filter((r) => r.availableAt > r.factAt).length };
    if (!isRecord(raw.counts)) fail();
    onlyKeys(raw.counts, Object.keys(counts), "counts");
    for (const [k, n] of Object.entries(counts)) if (raw.counts[k] !== n) fail();
    return records;
  } catch { return fail(); }
}

export interface ResolvedAction {
  repository: string | null; target: string | null; availableAt: string | null;
  status: "attributed" | "missing" | "unattributed" | "conflicting_claims" | "not_available";
}
/** Descriptive by default. A causal caller must pass now; equality is hidden, matching replay's ts < now. */
export function resolveAction(records: ReadonlyMap<string, ActionRecord>, s: Pick<SnapshotSession, "harness" | "sessionId">, kind: ActionKind, f: { eventId: string; ts: string }, now?: Date): ResolvedAction {
  const r = records.get(actionKey(s, kind, f));
  if (!r) return { repository: null, target: null, availableAt: null, status: "missing" };
  if (now && r.availableAt >= now.toISOString()) return { repository: null, target: null, availableAt: r.availableAt, status: "not_available" };
  return { repository: r.repository, target: r.repositoryTarget, availableAt: r.availableAt,
    status: r.repository ? "attributed" : r.repositoryBasis === "conflicting_claims" ? "conflicting_claims" : "unattributed" };
}
