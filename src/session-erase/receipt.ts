/** Content-free public receipt. Do not forward arbitrary DB JSON or provider errors to Control. */
export const ARTIFACT_KEYS = ["transcriptObjects", "stagedObjects", "bufferedBatches", "bufferedRows", "pointerRows", "ledgerRows",
  "directoryRows", "catalogRows", "largeReadJobs", "searchDocuments", "briefDocuments", "summaries", "briefs",
  "replayPublications", "analysisPublications", "historyDerivedObjects"] as const;
export type ArtifactKey = typeof ARTIFACT_KEYS[number];
export interface EraseKey { orgId: string; workspaceId: string; source: string; sessionId: string }
export interface ErasureReceipt extends EraseKey {
  requestId: string; status: "in_progress" | "completed" | "failed";
  requestedAt: string; completedAt: string | null;
  unreadable: boolean; unreadableAt: string | null;
  bytesErased: boolean; bytesErasedAt: string | null;
  uploadsFenced: boolean; legacyScope: "excluded";
  artifacts: Record<ArtifactKey, { count: number; state: "pending" | "done" | "not_applicable" }>;
  failure: { step: ArtifactKey | "fence"; code: string; retryable: boolean } | null;
}
export class SessionEraseServiceError extends Error {
  constructor(readonly code: "unavailable" | "too_many_owners" | "unsupported_storage" = "unavailable") { super(code); }
}
const object = (v: unknown): v is Record<string, any> => !!v && typeof v === "object" && !Array.isArray(v);
const stamp = (v: unknown): v is string => typeof v === "string" && v.length <= 64 && /^\d{4}-\d\d-\d\dT/.test(v) && Number.isFinite(Date.parse(v));
const nullableStamp = (v: unknown) => v === null || stamp(v);
const refuse = (): never => { throw new SessionEraseServiceError(); };

export function erasureReceipt(value: unknown, key: EraseKey): ErasureReceipt {
  if (!object(value) || Object.entries(key).some(([name, id]) => value[name] !== id)
    || typeof value.requestId !== "string" || !value.requestId || new TextEncoder().encode(value.requestId).length > 128
    || !["in_progress", "completed", "failed"].includes(value.status) || !stamp(value.requestedAt) || !nullableStamp(value.completedAt)
    || typeof value.unreadable !== "boolean" || !nullableStamp(value.unreadableAt) || value.unreadable !== (value.unreadableAt !== null)
    || typeof value.bytesErased !== "boolean" || !nullableStamp(value.bytesErasedAt) || value.bytesErased !== (value.bytesErasedAt !== null)
    || typeof value.uploadsFenced !== "boolean" || (value.unreadable && !value.uploadsFenced) || value.legacyScope !== "excluded"
    || !object(value.artifacts) || Object.keys(value.artifacts).length !== ARTIFACT_KEYS.length) return refuse();
  const artifacts = {} as ErasureReceipt["artifacts"];
  for (const name of ARTIFACT_KEYS) {
    const a = value.artifacts[name];
    if (!object(a) || Object.keys(a).length !== 2 || !Number.isSafeInteger(a.count) || a.count < 0
      || !["pending", "done", "not_applicable"].includes(a.state)) return refuse();
    artifacts[name] = { count: a.count, state: a.state };
  }
  let failure: ErasureReceipt["failure"] = null;
  if (value.failure !== null) {
    const f = value.failure;
    if (!object(f) || Object.keys(f).length !== 3 || !["fence", ...ARTIFACT_KEYS].includes(f.step)
      || typeof f.code !== "string" || !/^[A-Za-z][A-Za-z0-9_.-]{0,63}$/.test(f.code) || typeof f.retryable !== "boolean") return refuse();
    failure = { step: f.step, code: f.code, retryable: f.retryable };
  }
  if ((value.status === "failed") !== (failure !== null) || (value.status === "completed") !== (value.completedAt !== null)
    || (value.status === "completed" && (!value.unreadable || !value.bytesErased || Object.values(artifacts).some(a => a.state === "pending")))) return refuse();
  const receipt: ErasureReceipt = {
    ...key, requestId: value.requestId, status: value.status, requestedAt: value.requestedAt, completedAt: value.completedAt,
    unreadable: value.unreadable, unreadableAt: value.unreadableAt, bytesErased: value.bytesErased, bytesErasedAt: value.bytesErasedAt,
    uploadsFenced: value.uploadsFenced, legacyScope: "excluded", artifacts, failure,
  };
  if (new TextEncoder().encode(JSON.stringify(receipt)).byteLength > 16 * 1024) return refuse();
  return receipt;
}
