/**
 * Cloud ids for the export, shared by the grant lifecycle and the upload
 * courier so the grant's plan, every create request and every manifest the
 * courier writes carry the same ids.
 *
 * Only canonical lowercase UUIDs are Cloud ids. A legacy (non-UUID) id stands
 * for a Cloud id only through an explicit, reviewed, collision-free
 * `IdMappingLedger` (empty by default). No heuristic mapping: `relay_`
 * stripping, case folding or name matching never apply. An id that does not
 * resolve is left as it is; the lifecycle reports it as an exception that
 * blocks its shard, so it is never granted.
 */
import type { ExportPlan, PlannedManifest, UploadInput } from "./relayhistory-export-upload.ts";

const CLOUD_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
export const isCloudId = (id: string) => CLOUD_UUID.test(id);

export type IdKind = "org" | "workspace" | "user";

export interface IdMappingLedger {
  version: 1;
  reviewed_by: string;
  reviewed_at: string;
  entries: { kind: IdKind; legacy_id: string; cloud_id: string }[];
}

export const EMPTY_ID_MAPPING: IdMappingLedger = { version: 1, reviewed_by: "", reviewed_at: "", entries: [] };

export class IdMappingError extends Error {
  readonly code = "id_mapping_invalid";
  constructor(message: string) {
    super(message);
    this.name = "IdMappingError";
  }
}

/** Refuses a mapping that is unreviewed, malformed or not collision-free. */
export function validateIdMapping(ledger: IdMappingLedger): IdMappingLedger {
  const bad = (why: string): never => {
    throw new IdMappingError(`id mapping ledger: ${why}`);
  };
  if (ledger.version !== 1) bad("unsupported version");
  if (!ledger.entries.length) return ledger;
  if (!ledger.reviewed_by.trim() || !Number.isFinite(Date.parse(ledger.reviewed_at))) bad("entries need reviewed_by and reviewed_at");
  for (const e of ledger.entries) if (!["org", "workspace", "user"].includes(e.kind)) bad("unknown kind");
  for (const kind of ["org", "workspace", "user"] as const) {
    const legacy = new Set<string>();
    const cloud = new Set<string>();
    for (const e of ledger.entries.filter((x) => x.kind === kind)) {
      if (!isCloudId(e.cloud_id)) bad(`${kind} ${e.legacy_id} maps to a non-UUID`);
      if (!e.legacy_id || isCloudId(e.legacy_id)) bad(`${kind} legacy id must be a non-UUID alias`);
      if (legacy.has(e.legacy_id)) bad(`${kind} legacy id ${e.legacy_id} is mapped twice`);
      if (cloud.has(e.cloud_id)) bad(`${kind} Cloud id is the target of two legacy ids`);
      legacy.add(e.legacy_id);
      cloud.add(e.cloud_id);
    }
  }
  return ledger;
}

export type IdResolution =
  | { ok: true; cloud_id: string; via: "uuid" | "mapping" }
  | { ok: false; reason: "default_workspace" | "alias_id" | "invalid_id" };

/** A legacy id to its Cloud id: an exact UUID, or an explicit mapping entry. */
export function resolveCloudId(kind: IdKind, id: string, mapping: IdMappingLedger): IdResolution {
  const mapped = mapping.entries.find((e) => e.kind === kind && e.legacy_id === id);
  if (mapped) return { ok: true, cloud_id: mapped.cloud_id, via: "mapping" };
  if (isCloudId(id)) return { ok: true, cloud_id: id, via: "uuid" };
  if (kind === "workspace" && id === "default") return { ok: false, reason: "default_workspace" };
  if (id.startsWith("relay_")) return { ok: false, reason: "alias_id" };
  return { ok: false, reason: "invalid_id" };
}

const resolver = (mapping: IdMappingLedger) => {
  const m = validateIdMapping(mapping);
  return (kind: IdKind, id: string) => {
    const r = resolveCloudId(kind, id, m);
    return r.ok ? r.cloud_id : id;
  };
};

/**
 * Upload inputs with every resolvable id rewritten to its Cloud id:
 * `org_id`, `workspace_id`, `source_user_id` and the key's `ws=` segment.
 */
export function canonicalizeUploads(uploads: readonly UploadInput[], mapping: IdMappingLedger = EMPTY_ID_MAPPING): UploadInput[] {
  const to = resolver(mapping);
  return uploads.map((u) => {
    const ws = to("workspace", u.workspace_id);
    const prefix = `v1/ws=${u.workspace_id}/`;
    return {
      ...u,
      org_id: to("org", u.org_id),
      workspace_id: ws,
      source_user_id: to("user", u.source_user_id),
      key: u.key.startsWith(prefix) ? `v1/ws=${ws}/${u.key.slice(prefix.length)}` : u.key,
    };
  });
}

/**
 * The courier's export plan with the same rewrite: each planned manifest's
 * org, workspace (and so its location and manifest key) and source user. The
 * courier runs on this, so its plan digest, the identities it expects and the
 * manifests it writes match the lifecycle's `canonicalizeUploads` output.
 */
export function canonicalizeExportPlan(plan: ExportPlan, mapping: IdMappingLedger = EMPTY_ID_MAPPING): ExportPlan {
  const to = resolver(mapping);
  return {
    ...plan,
    manifests: plan.manifests.map((m): PlannedManifest => {
      const location: PlannedManifest["location"] = { ...m.location, workspaceId: to("workspace", m.location.workspaceId) };
      return { ...m, location, orgId: to("org", m.orgId), sourceUserId: to("user", m.sourceUserId) };
    }),
  };
}
