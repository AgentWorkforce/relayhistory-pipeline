/**
 * Data retention and deletion safety for the GCP stage (the backup and restore drill
 * in docs/gcp-stage.md found both gaps):
 *
 *   Cloud SQL  deletionProtectionEnabled, so the instance (and with it its automated
 *              backups) cannot be deleted by mistake; retainBackupsOnDelete, so its
 *              backups survive even a deliberate delete.
 *   Locker     one lifecycle rule: delete a noncurrent object version 30 days after it
 *              became noncurrent. That bounds both storage and how long an erased or
 *              overwritten transcript lingers; the live version is never touched, and
 *              the bucket's 7-day soft delete stays as the backstop.
 *
 * Neither Alchemy resource exposes these settings, so an action reconciles them
 * through the REST APIs each deploy. Both are read first, written only when they
 * differ, and read back afterwards.
 */

export const LOCKER_NONCURRENT_RETENTION_DAYS = 30;

export interface LifecycleRule {
  action: { type: string; storageClass?: string };
  condition: Record<string, unknown>;
}

export const LOCKER_LIFECYCLE_RULES: readonly LifecycleRule[] = Object.freeze([
  { action: { type: "Delete" }, condition: { isLive: false, daysSinceNoncurrentTime: LOCKER_NONCURRENT_RETENTION_DAYS } },
]);

const BUCKET = /^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$/;
const PROJECT = /^[a-z][a-z0-9-]{4,28}[a-z0-9]$/;
const INSTANCE = /^[a-z][a-z0-9-]{0,96}[a-z0-9]$/;

const canonical = (rules: readonly LifecycleRule[] | undefined) =>
  JSON.stringify(
    (rules ?? []).map((r) => ({
      action: Object.fromEntries(Object.entries(r.action).sort()),
      condition: Object.fromEntries(Object.entries(r.condition).sort()),
    })),
  );

/** True when the bucket's lifecycle is exactly the stack's rule set (the stack owns it whole). */
export function lifecycleMatches(current: { rule?: LifecycleRule[] } | undefined): boolean {
  return canonical(current?.rule) === canonical(LOCKER_LIFECYCLE_RULES);
}

export interface RestAuth {
  bearer: string;
}
export type RestFetch = (url: string, init: RequestInit) => Promise<Response>;

const reason = async (r: Response) => (await r.text().catch(() => "")).replace(/\s+/g, " ").slice(0, 300);

export async function ensureLockerLifecycle(
  bucket: string,
  auth: RestAuth,
  transport: RestFetch = fetch,
): Promise<{ changed: boolean }> {
  if (!BUCKET.test(bucket)) throw new Error("96bulls: locker bucket name is malformed. Nothing was changed.");
  const url = `https://storage.googleapis.com/storage/v1/b/${encodeURIComponent(bucket)}`;
  const headers = { authorization: `Bearer ${auth.bearer}`, "content-type": "application/json" };
  const read = async () => {
    const r = await transport(`${url}?fields=lifecycle`, { method: "GET", headers });
    if (!r.ok) throw new Error(`96bulls: locker lifecycle read failed with HTTP ${r.status}: ${await reason(r)}`);
    return ((await r.json()) as { lifecycle?: { rule?: LifecycleRule[] } }).lifecycle;
  };
  if (lifecycleMatches(await read())) return { changed: false };
  const w = await transport(`${url}?fields=lifecycle`, {
    method: "PATCH",
    headers,
    body: JSON.stringify({ lifecycle: { rule: LOCKER_LIFECYCLE_RULES } }),
  });
  if (!w.ok) throw new Error(`96bulls: locker lifecycle write failed with HTTP ${w.status}: ${await reason(w)}`);
  if (!lifecycleMatches(await read())) throw new Error("96bulls: locker lifecycle did not read back as written.");
  return { changed: true };
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

type SqlSettings = { deletionProtectionEnabled?: boolean; retainBackupsOnDelete?: boolean };

/**
 * Deletion protection and retained backups on the Cloud SQL instance. A PATCH carries
 * only those two settings (no restart); the operation is awaited and the instance is
 * read back. Turning protection off is a deliberate teardown step outside this code.
 */
export async function ensureCloudSqlProtection(
  project: string,
  instance: string,
  auth: RestAuth,
  transport: RestFetch = fetch,
  pollMs = 3_000,
  maxPolls = 100,
): Promise<{ changed: boolean }> {
  if (!PROJECT.test(project) || !INSTANCE.test(instance))
    throw new Error("96bulls: Cloud SQL project or instance name is malformed. Nothing was changed.");
  const base = `https://sqladmin.googleapis.com/v1/projects/${project}`;
  const headers = { authorization: `Bearer ${auth.bearer}`, "content-type": "application/json" };
  const settings = async (): Promise<SqlSettings> => {
    const r = await transport(`${base}/instances/${instance}`, { method: "GET", headers });
    if (!r.ok) throw new Error(`96bulls: Cloud SQL instance read failed with HTTP ${r.status}: ${await reason(r)}`);
    return ((await r.json()) as { settings?: SqlSettings }).settings ?? {};
  };
  const protectedNow = (s: SqlSettings) => s.deletionProtectionEnabled === true && s.retainBackupsOnDelete === true;
  if (protectedNow(await settings())) return { changed: false };
  const p = await transport(`${base}/instances/${instance}`, {
    method: "PATCH",
    headers,
    body: JSON.stringify({ settings: { deletionProtectionEnabled: true, retainBackupsOnDelete: true } }),
  });
  if (!p.ok) throw new Error(`96bulls: Cloud SQL protection patch failed with HTTP ${p.status}: ${await reason(p)}`);
  const op = ((await p.json()) as { name?: string }).name;
  if (!op || !/^[A-Za-z0-9-]+$/.test(op)) throw new Error("96bulls: Cloud SQL patch returned no operation.");
  for (let i = 0; i < maxPolls; i++) {
    const r = await transport(`${base}/operations/${op}`, { method: "GET", headers });
    if (!r.ok) throw new Error(`96bulls: Cloud SQL operation read failed with HTTP ${r.status}.`);
    const body = (await r.json()) as { status?: string; error?: unknown };
    if (body.status === "DONE") {
      if (body.error) throw new Error("96bulls: Cloud SQL protection patch finished with an error.");
      if (!protectedNow(await settings())) throw new Error("96bulls: Cloud SQL protection did not read back as patched.");
      return { changed: true };
    }
    await sleep(pollMs);
  }
  throw new Error("96bulls: Cloud SQL protection patch did not finish in time.");
}
