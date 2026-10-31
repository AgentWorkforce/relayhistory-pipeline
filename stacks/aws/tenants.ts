/**
 * Tenant orgs the ingest accepts.
 *
 * One org (`tenantOrgId`, the existing shape): the static single-tenant mode,
 * keys at `v1/...`, unchanged.
 *
 * Several orgs (`tenantOrgIds`, e.g. the proof's isolation tests): the
 * pipeline's org-derived prefixes (E3, #69). Every object lands under
 * `<TENANT_KEY_BASE>/<org>/v1/...`, the org taken from the caller's RelayAuth
 * token and applied server-side, and the workers read the org back from the
 * key. AlleyOop refuses any token whose org is not listed (TENANT_ORG_IDS,
 * src/alleyoop/routes.ts), and the pod roles' S3 scopes follow the prefix.
 */
export const TENANT_KEY_BASE = "96bulls";
const ORG_RE = /^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$/;

export interface TenantInputs {
  tenantOrgId?: string;
  tenantOrgIds?: string[];
}

export function resolveTenantOrgs(i: TenantInputs): string[] {
  const list = i.tenantOrgIds ?? (i.tenantOrgId !== undefined ? [i.tenantOrgId] : []);
  if (!list.length) throw new Error("96bulls: eks.tenantOrgId or eks.tenantOrgIds is required. Nothing was created.");
  if (i.tenantOrgIds && i.tenantOrgId !== undefined && i.tenantOrgId !== i.tenantOrgIds[0])
    throw new Error("96bulls: eks.tenantOrgId must equal eks.tenantOrgIds[0] when both are set (or drop tenantOrgId). Nothing was created.");
  for (const o of list) if (!ORG_RE.test(o)) throw new Error(`96bulls: tenant org ${JSON.stringify(o)} is not a valid org id. Nothing was created.`);
  if (new Set(list).size !== list.length) throw new Error("96bulls: eks.tenantOrgIds has duplicates. Nothing was created.");
  return list;
}

/** Chart wiring for the resolved orgs: the umbrella's required org, extra env for both apps, and the S3 key base. */
export function tenantWiring(orgs: string[]) {
  if (orgs.length === 1) return { primaryOrg: orgs[0]!, extraEnv: [] as { name: string; value: string }[], keyBase: undefined as string | undefined };
  return {
    // The charts require one org id; in prefix mode the services ignore it (src/shared/env.ts).
    primaryOrg: orgs[0]!,
    extraEnv: [
      { name: "PIPELINE_ORG_FROM_PREFIX", value: "true" },
      { name: "PIPELINE_KEY_PREFIX", value: TENANT_KEY_BASE },
      { name: "TENANT_ORG_IDS", value: orgs.join(",") },
    ],
    keyBase: TENANT_KEY_BASE,
  };
}
