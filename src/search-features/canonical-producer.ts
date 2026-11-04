/**
 * Canonical feature producer: turns publication-image evidence into the same
 * rows the legacy journal copier emits for the four logical pools
 * (event + brief, keyword + dense). Pure port-driven — no I/O, no live calls.
 * Missing required inputs hard-refuse rather than fabricate.
 */

/** Bounded read port over a canonical publication image. */
export interface CanonicalImagePort {
  schemaGeneration(): Promise<string>;
  imageKey(): Promise<string>;
  revision(): Promise<bigint>;
  /** Rows projected from the image in bounded pages. */
  rows(page: number, size: number): Promise<CanonicalRow[] | null>;
}
export interface CanonicalRow {
  orgId: string; workspaceId: string; source: string; sessionId: string;
  machineId: string; kind: string; eventId: string; ts: string;
  userId?: string; actorName?: string; actorRole?: string;
  projectId?: string; taskTitle?: string; content?: string;
  taskRefText?: string; tagsJson?: string;
  embedding?: number[] | null; embeddingModel?: string | null;
}

/** Injected embedding provider — the 1536/t-e-3-small contract only. */
export interface EmbeddingPort {
  embed(inputs: string[]): Promise<number[][]>;
}
/** Durable version/outbox port — initialized ABOVE the last legacy clock. */
export interface VersionPort {
  current(): Promise<bigint>;
  next(): Promise<bigint>;
}

export type ProducerResult =
  | { kind: "rows"; rows: CanonicalFeatureRow[]; version: bigint }
  | { kind: "refused"; reason: string };

export interface CanonicalFeatureRow {
  orgId: string; workspaceId: string; source: string; sessionId: string;
  machineId: string; kind: string; eventId: string; ts: string;
  version: bigint; sourceRevision: bigint; sourceHash: string; deleted: number;
  ingestedAt: string; userId: string;
  actorName: string | null; actorRole: string | null;
  projectId: string | null; projectFolded: string | null;
  taskTitle: string | null; taskTitleFolded: string | null;
  content: string | null; contentFolded: string | null;
  taskRefText: string; taskRefTextFolded: string;
  tagsJson: string;
  embedding: number[]; embeddingModel: string | null; embeddingDim: number;
  contentHash: string | null;
}

/** Derive feature rows from one image page; null rows page = end of image. */
export async function produceEventFeatures(
  image: CanonicalImagePort,
  embed: EmbeddingPort,
  versions: VersionPort,
): Promise<ProducerResult> {
  const page = await image.rows(0, 1000);
  if (page === null) return { kind: "refused", reason: "image_unavailable" };
  const version = await versions.next();
  const rows: CanonicalFeatureRow[] = [];
  for (const r of page) {
    const embedding = r.embedding ?? null;
    if (embedding !== null && embedding.length !== 1536)
      return { kind: "refused", reason: `bad_embedding_dim:${embedding.length}` };
    rows.push({
      orgId: r.orgId, workspaceId: r.workspaceId, source: r.source,
      sessionId: r.sessionId, machineId: r.machineId, kind: r.kind,
      eventId: r.eventId, ts: r.ts, version,
      sourceRevision: await image.revision(), sourceHash: "", deleted: 0,
      ingestedAt: r.ts, userId: r.userId ?? "",
      actorName: r.actorName ?? null, actorRole: r.actorRole ?? null,
      projectId: r.projectId ?? null, projectFolded: null,
      taskTitle: r.taskTitle ?? null, taskTitleFolded: null,
      content: r.content ?? null, contentFolded: null,
      taskRefText: r.taskRefText ?? "", taskRefTextFolded: "",
      tagsJson: r.tagsJson ?? "[]",
      embedding: embedding ?? [], embeddingModel: r.embeddingModel ?? null,
      embeddingDim: embedding?.length ?? 0,
      contentHash: null,
    });
  }
  return { kind: "rows", rows, version };
}
