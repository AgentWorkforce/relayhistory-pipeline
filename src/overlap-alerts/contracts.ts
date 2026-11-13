import { createHash } from "node:crypto";

export const KINDS = ["duplicate_work", "conflicting_changes", "dependency", "reusable_finding", "shared_outcome", "ownership_gap"] as const;
export type AlertKind = (typeof KINDS)[number];
export interface Scope { orgId: string; workspaceId: string }
export interface Evidence {
  id: string;
  at: string;
  /** When the collector could first see this evidence; not the time a backfill ran. */
  availableAt: string | null;
  kind: "intent" | "finding" | "blocker" | "decision" | "tool" | "message";
  text: string;
  files: { path: string; operation: "read" | "write" | "unknown" }[];
  taskRefs: string[];
}
export interface Session {
  source: string;
  id: string;
  userId: string;
  title: string;
  /** Full repository identity, not a basename or working-directory path. */
  repository: string | null;
  visibility: "shared" | "private";
  relatedSessionKeys: string[];
  /** Unknown relationships cannot suppress an earlier replay. */
  relationshipsAvailableAt?: string | null;
  complete: boolean;
  evidence: Evidence[];
}
export interface Dataset {
  version: 1;
  scope: Scope;
  provenance: {
    kind: "synthetic" | "production";
    reader: string;
    capturedAt: string;
    expiresAt: string;
    userIds: string[];
    selection: string;
    complete: boolean;
  };
  sessions: Session[];
}
export interface Candidate {
  id: string;
  asOf?: string;
  left: Session;
  right: Session;
  signals: { sharedWrites: string[]; sharedFiles: string[]; sharedTasks: string[]; topicTerms: string[]; sameRepository: boolean };
  rank: number;
}
export interface Citation { sessionKey: string; evidenceId: string; quote: string }
export interface Verdict {
  decision: "suggest" | "suppress" | "uncertain";
  kind: AlertKind | null;
  confidence: "high" | "medium" | "low";
  reason: string;
  citations: Citation[];
}
export interface Judge {
  version: string;
  contextLimits?: { eventsPerSession: number; charsPerEvent: number };
  assess(candidate: Candidate): Promise<Verdict>;
}
export interface Alert {
  id: string;
  pairId: string;
  scope: Scope;
  sessionKeys: [string, string];
  userIds: [string, string];
  kind: AlertKind;
  confidence: "high" | "medium";
  reason: string;
  citations: Citation[];
  evidenceFingerprint: string;
  evaluatedAt: string;
  expiresAt: string;
  status: "candidate";
}
export interface Assessment { pairId: string; sessionKeys: [string, string]; verdict: Verdict }
export interface Collection {
  version: 1;
  scope: Scope;
  datasetFingerprint: string;
  detectorVersion: string;
  asOf: string;
  mode: "retrospective" | "replay";
  expiresAt: string;
  coverage: { sourceComplete: boolean; eligibleSessions: number; candidatePairs: number; assessedPairs: number; omittedPairs: number; unknownAvailability: number; failedPairs: number; limitedContextPairs: number };
  alerts: Alert[];
  assessments: Assessment[];
}
export const sessionKey = (s: Pick<Session, "source" | "id">) => JSON.stringify([s.source, s.id]);
export const hash = (v: unknown) => createHash("sha256").update(JSON.stringify(v)).digest("hex");
export const pairId = (scope: Scope, a: string, b: string) => hash([scope.orgId, scope.workspaceId, ...[a, b].sort()]);
export function timestamp(v: unknown): v is string {
  return typeof v === "string" && /^\d{4}-\d\d-\d\dT.*Z$/.test(v) && Number.isFinite(Date.parse(v));
}
export function record(v: unknown): v is Record<string, unknown> { return !!v && typeof v === "object" && !Array.isArray(v); }
const str = (v: unknown, max = 512): v is string => typeof v === "string" && v.length > 0 && v.length <= max && !/[\u0000-\u001f]/.test(v);
const strings = (v: unknown, max = 100): v is string[] => Array.isArray(v) && v.length <= max && v.every(x => str(x));
function onlyKeys(v: Record<string, unknown>, allowed: string[]) {
  if (Object.keys(v).some(k => !allowed.includes(k))) throw new Error("unexpected_dataset_field");
}
export function parseDataset(v: unknown): Dataset {
  if (!record(v) || v.version !== 1 || !record(v.scope) || !str(v.scope.orgId) || !str(v.scope.workspaceId) ||
      !record(v.provenance) || !["synthetic", "production"].includes(String(v.provenance.kind)) ||
      !str(v.provenance.reader) || !str(v.provenance.selection, 2000) || !timestamp(v.provenance.capturedAt) || !timestamp(v.provenance.expiresAt) ||
      Date.parse(v.provenance.expiresAt) <= Date.parse(v.provenance.capturedAt) ||
      !strings(v.provenance.userIds, 20) || !v.provenance.userIds.length || typeof v.provenance.complete !== "boolean" ||
      !Array.isArray(v.sessions) || v.sessions.length > 250) throw new Error("invalid_dataset");
  onlyKeys(v, ["version", "scope", "provenance", "sessions"]);
  onlyKeys(v.scope, ["orgId", "workspaceId"]);
  onlyKeys(v.provenance, ["kind", "reader", "capturedAt", "expiresAt", "userIds", "selection", "complete"]);
  if (v.provenance.kind === "production" && Date.parse(v.provenance.expiresAt) - Date.parse(v.provenance.capturedAt) > 7 * 86400000) throw new Error("production_retention_exceeded");
  const keys = new Set<string>();
  let chars = 0;
  for (const s of v.sessions) {
    if (!record(s) || !str(s.source) || !str(s.id) || !str(s.userId) || !v.provenance.userIds.includes(s.userId) || !str(s.title, 500) ||
        !(s.repository === null || str(s.repository, 1000)) || !["shared", "private"].includes(String(s.visibility)) ||
        !strings(s.relatedSessionKeys) || typeof s.complete !== "boolean" || !Array.isArray(s.evidence) || s.evidence.length > 200)
      throw new Error("invalid_session");
    onlyKeys(s, ["source", "id", "userId", "title", "repository", "visibility", "relatedSessionKeys", "relationshipsAvailableAt", "complete", "evidence"]);
    if (s.relationshipsAvailableAt !== undefined && s.relationshipsAvailableAt !== null && !timestamp(s.relationshipsAvailableAt)) throw new Error("invalid_relationship_time");
    const key = sessionKey(s as unknown as Session);
    if (keys.has(key)) throw new Error("duplicate_session");
    keys.add(key);
    const ids = new Set<string>();
    for (const e of s.evidence) {
      if (!record(e) || !str(e.id) || !timestamp(e.at) || !(e.availableAt === null || timestamp(e.availableAt)) ||
          (e.availableAt !== null && Date.parse(e.availableAt) < Date.parse(e.at)) ||
          !["intent", "finding", "blocker", "decision", "tool", "message"].includes(String(e.kind)) ||
          typeof e.text !== "string" || e.text.length > 8000 || !strings(e.taskRefs, 30) ||
          !Array.isArray(e.files) || e.files.length > 100 || e.files.some(f => !record(f) || !str(f.path, 1000) || !["read", "write", "unknown"].includes(String(f.operation))))
        throw new Error("invalid_evidence");
      onlyKeys(e, ["id", "at", "availableAt", "kind", "text", "files", "taskRefs"]);
      for (const f of e.files) onlyKeys(f, ["path", "operation"]);
      if (ids.has(e.id)) throw new Error("duplicate_evidence");
      ids.add(e.id); chars += e.text.length;
    }
  }
  if (chars > 8_000_000) throw new Error("dataset_too_large");
  return v as unknown as Dataset;
}
