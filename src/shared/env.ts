/** Typed access to the service environment. Secrets are read, never logged. */
export function required(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`96bulls: missing required env ${name}`);
  return v;
}

export function optional(name: string, fallback: string): string {
  return process.env[name] || fallback;
}

export function flag(name: string, fallback = false): boolean {
  const v = process.env[name];
  if (v === undefined || v === "") return fallback;
  return v === "1" || v.toLowerCase() === "true";
}

export function integer(name: string, fallback: number): number {
  const v = Number(process.env[name]);
  return Number.isFinite(v) && v >= 0 ? v : fallback;
}

/** The Locker (S3) settings. Endpoint and keys are optional (AWS S3 with Pod Identity); see shared/locker.ts. */
export function lockerEnv() {
  return {
    endpoint: process.env.LOCKER_ENDPOINT || undefined,
    bucket: required("LOCKER_BUCKET"),
    region: optional("LOCKER_REGION", "us-east-1"),
    accessKeyId: process.env.LOCKER_ACCESS_KEY_ID || undefined,
    secretAccessKey: process.env.LOCKER_SECRET_ACCESS_KEY || undefined,
  };
}

export function tenantEnv() {
  return {
    orgId: required("TENANT_ORG_ID"),
    workspaceId: optional("WORKSPACE_ID", "default"),
  };
}

/**
 * The vendored relayhistory-cloud code reads its own env names (`PIPELINE_*`,
 * `RELAYAUTH_*`). Map ours onto them once so one `LOCKER_*` block configures
 * both our services and the vendored routes.
 */
export function vendorBindings(extra: Record<string, string | undefined> = {}): Record<string, string | undefined> {
  const locker = lockerEnv();
  const tenant = tenantEnv();
  return {
    ENVIRONMENT: optional("ENVIRONMENT", "local"),
    PIPELINE_RAW_PROVIDER: "s3",
    PIPELINE_RAW_S3_BUCKET: locker.bucket,
    PIPELINE_RAW_S3_ENDPOINT: locker.endpoint,
    PIPELINE_RAW_S3_REGION: locker.region,
    PIPELINE_RAW_S3_ACCESS_KEY_ID: locker.accessKeyId,
    PIPELINE_RAW_S3_SECRET_ACCESS_KEY: locker.secretAccessKey,
    PIPELINE_ORG_ID: tenant.orgId,
    PIPELINE_RECONCILE_ENABLED: optional("PIPELINE_RECONCILE_ENABLED", "true"),
    PIPELINE_RECONCILE_LOOKBACK_MINUTES: optional("PIPELINE_RECONCILE_LOOKBACK_MINUTES", "15"),
    ...(process.env.PIPELINE_KEY_PREFIX ? { PIPELINE_KEY_PREFIX: process.env.PIPELINE_KEY_PREFIX } : {}),
    ...extra,
  };
}
