/** Directory-document pool rows for one canonical image page + its replaced
 *  keyset — the third projection pool (per-session directory document). */
import { EventFeatureError } from "./event-publisher.ts";
import { featureVersion } from "./source.ts";

/** Minimal snapshot for the document pool — self-contained so the legacy
 *  FeatureKind ("event" | "brief") stays two-pool. */
interface DocumentSnapshot {
  reference: {
    kind: "document";
    key: {
      orgId: string; workspaceId: string; source: string; sessionId: string;
    };
    revision: string; sourceHash: string; deleted: boolean;
  };
  row: {
    title: string; summary: string;
    titleFolded: string; summaryFolded: string;
    projectId: string | null; lastActivityAt: string;
  } | null;
}
import type {
  CanonicalImageEvidence,
  CanonicalProduceResult,
} from "./canonical-producer.ts";

export interface CanonicalDocumentRow {
  orgId: string; workspaceId: string; source: string; sessionId: string;
  title: string; summary: string;
  titleFolded: string; summaryFolded: string;
  projectId: string | null; lastActivityAt: string;
}
/** Document tombstone: prior-session keyset retired by a replaced image. */
export interface CanonicalDocumentTombstone {
  orgId: string; workspaceId: string; source: string; sessionId: string;
}

const fail = (r: string): never => {
  throw new EventFeatureError(r);
};
const stamp = (v: unknown): v is string =>
  typeof v === "string" &&
  /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z$/.test(v) &&
  Number.isFinite(Date.parse(v));
const version = (rev: string): string => {
  const n = Number(rev);
  if (!Number.isInteger(n) || n < 0) fail("revision_invalid");
  return rev;
};

export function documentFeatureRow(
  s: DocumentSnapshot,
): Record<string, unknown> {
  const r = s.reference, k = r.key, p = s.row;
  if (
    r.kind !== "document" ||
    Object.keys(k).length !== 4 ||
    !["orgId", "source", "sessionId", "workspaceId"].every((f) => f in k) ||
    typeof k.orgId !== "string" || !k.orgId ||
    typeof k.sessionId !== "string" || !k.sessionId ||
    typeof k.source !== "string" || !k.source ||
    !(typeof k.workspaceId === "string" || k.workspaceId === null) ||
    typeof r.deleted !== "boolean" ||
    !/^[a-f0-9]{64}$/.test(r.sourceHash)
  )
    return fail("reference_invalid");
  const base = {
    org_id: k.orgId,
    workspace_id: k.workspaceId ?? "",
    source: k.source,
    session_id: k.sessionId,
    version: featureVersion(r.revision),
    source_revision: r.revision,
    source_hash: r.sourceHash,
    deleted: r.deleted ? 1 : 0,
  };
  if (r.deleted) {
    if (p !== null) return fail("tombstone_invalid");
    return {
      ...base, title: "", summary: "", title_folded: "", summary_folded: "",
      project_id: null, project_folded: null,
      last_activity_at: "1970-01-01 00:00:00.000000",
    };
  }
  if (!p || typeof p !== "object") return fail("row_invalid");
  const d = p as { title?: unknown; summary?: unknown;
    titleFolded?: unknown; summaryFolded?: unknown;
    projectId?: unknown; lastActivityAt?: unknown };
  if (typeof d.title !== "string" || typeof d.summary !== "string" ||
      typeof d.titleFolded !== "string" ||
      typeof d.summaryFolded !== "string" ||
      !(typeof d.projectId === "string" || d.projectId === null) ||
      !stamp(d.lastActivityAt))
    return fail("row_invalid");
  return {
    ...base, title: d.title, summary: d.summary,
    title_folded: d.titleFolded, summary_folded: d.summaryFolded,
    project_id: d.projectId,
    project_folded: d.projectId === null ? null : d.projectId.toLowerCase(),
    last_activity_at: d.lastActivityAt.replace("T", " ").replace("Z", ""),
  };
}

const documentSnapshot = (
  r: CanonicalDocumentRow, e: CanonicalImageEvidence,
): DocumentSnapshot => ({
  reference: {
    kind: "document",
    key: {
      orgId: r.orgId, workspaceId: r.workspaceId, source: r.source,
      sessionId: r.sessionId,
    },
    revision: e.revision, sourceHash: e.sourceHash, deleted: false,
  },
  row: {
    title: r.title, summary: r.summary,
    titleFolded: r.titleFolded, summaryFolded: r.summaryFolded,
    projectId: r.projectId, lastActivityAt: r.lastActivityAt,
  },
});
const documentTombstoneSnapshot = (
  t: CanonicalDocumentTombstone, e: CanonicalImageEvidence,
): DocumentSnapshot => ({
  reference: {
    kind: "document",
    key: {
      orgId: t.orgId, workspaceId: t.workspaceId, source: t.source,
      sessionId: t.sessionId,
    },
    revision: e.revision, sourceHash: e.sourceHash, deleted: true,
  },
  row: null,
});

/** Document-pool rows for one canonical image page + its replaced keyset. */
export function produceDocumentFeatures(
  rows: CanonicalDocumentRow[],
  tombstones: CanonicalDocumentTombstone[],
  e: CanonicalImageEvidence,
  retentionDays = 0,
): CanonicalProduceResult {
  try {
    return {
      kind: "rows",
      rows: [
        ...rows.map((r) => documentFeatureRow(documentSnapshot(r, e))),
        ...tombstones.map((t) =>
          documentFeatureRow(documentTombstoneSnapshot(t, e))),
      ].map((r) => ({ ...r, retention_days: retentionDays })),
    };
  } catch (err) {
    if (err instanceof EventFeatureError)
      return { kind: "refused", reason: err.reason };
    throw err;
  }
}
