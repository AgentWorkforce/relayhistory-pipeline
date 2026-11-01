/**
 * Per-component write scopes on the locker bucket.
 *
 * Every 96Bulls pod reads the whole v1 tree (the worker reads manifests and raw
 * chunks; AlleyOop HEADs chunks and reads manifests), so reads stay a plain
 * `roles/storage.objectViewer` edge. Writes are narrowed with IAM Conditions on
 * the object name, matching the AWS split in stacks/aws/hardening.ts:
 *
 *   AlleyOop  v1/…/manifest.json (live and parked) and v1/…/raw/*.zst chunks
 *   worker    v1/…/summary/{short,long}.json.enc
 *
 * The write role is `roles/storage.objectUser` because a manifest compare-and-swap
 * and parked-session cleanup overwrite or delete, and Cloud Storage requires
 * storage.objects.delete for both. `objects.list` is bucket-level, so a conditional
 * objectUser edge never widens listing; the unconditional viewer edge supplies it.
 *
 * Alchemy's IAM.Member manages unconditional edges only, so the conditional edges
 * are reconciled here against the bucket's v3 policy under its etag. Only bindings
 * whose condition title carries {@link MANAGED_TITLE_PREFIX} are touched.
 */

export const MANAGED_TITLE_PREFIX = "96bulls-locker-write-";

export interface IamCondition {
  title: string;
  expression: string;
  description?: string;
}

export interface ConditionalBinding {
  role: string;
  member: string;
  condition: IamCondition;
}

export interface BucketPolicyBinding {
  role?: string;
  members?: string[];
  condition?: { title?: string; expression?: string; description?: string };
}

export interface BucketPolicy {
  version?: number;
  etag?: string;
  bindings?: BucketPolicyBinding[];
  [key: string]: unknown;
}

const BUCKET = /^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$/;
const MEMBER = /^serviceAccount:[a-z][a-z0-9-]{4,28}[a-z0-9]@[a-z][a-z0-9-]{4,28}[a-z0-9]\.iam\.gserviceaccount\.com$/;

function objects(bucket: string): string {
  if (!BUCKET.test(bucket)) throw new Error("96bulls: locker bucket name is malformed. Nothing was changed.");
  return `projects/_/buckets/${bucket}/objects/v1/`;
}

/** CEL for AlleyOop's writes: manifests (live and parked) and raw chunks. */
export function alleyoopWriteCondition(bucket: string): IamCondition {
  return {
    title: `${MANAGED_TITLE_PREFIX}alleyoop`,
    description: "AlleyOop writes only v1 manifests and raw .zst chunks",
    expression:
      `resource.name.startsWith("${objects(bucket)}") && ` +
      `(resource.name.endsWith("/manifest.json") || resource.name.endsWith(".zst"))`,
  };
}

/** CEL for the worker's writes: the two sealed summary objects. */
export function workerWriteCondition(bucket: string): IamCondition {
  return {
    title: `${MANAGED_TITLE_PREFIX}worker`,
    description: "The worker writes only v1 sealed summary objects",
    expression:
      `resource.name.startsWith("${objects(bucket)}") && ` +
      `(resource.name.endsWith("/summary/short.json.enc") || resource.name.endsWith("/summary/long.json.enc"))`,
  };
}

const sameCondition = (
  a: BucketPolicyBinding["condition"],
  b: IamCondition,
): boolean => a?.title === b.title && a?.expression === b.expression;

/**
 * The policy with exactly the desired managed conditional bindings: each desired
 * member is present under a binding carrying exactly its condition, and every
 * other member of a managed-title binding (stale expression, removed identity) is
 * dropped. Unmanaged bindings are returned untouched. Version is forced to 3, which
 * Cloud Storage requires for any conditional binding.
 */
export function reconcileConditionalBindings(
  policy: BucketPolicy,
  desired: readonly ConditionalBinding[],
  /**
   * Members that must hold no role at all on the bucket, conditional or not:
   * the retired shared runtime identity. Stripped from every binding, so an
   * upgrade never depends on the removed IAM.Member edge having revoked itself.
   */
  forbidden: readonly string[] = [],
): { policy: BucketPolicy; changed: boolean } {
  for (const f of forbidden)
    if (!MEMBER.test(f) || desired.some((d) => d.member === f))
      throw new Error("96bulls: a forbidden bucket member is malformed or also desired. Nothing was changed.");
  for (const d of desired) {
    if (!d.condition.title.startsWith(MANAGED_TITLE_PREFIX))
      throw new Error("96bulls: conditional bucket binding is not in the managed namespace. Nothing was changed.");
    if (!MEMBER.test(d.member))
      throw new Error("96bulls: conditional bucket binding member is not a service account. Nothing was changed.");
    if (!/^roles\/storage\.[A-Za-z]+$/.test(d.role))
      throw new Error("96bulls: conditional bucket binding role is not a Cloud Storage role. Nothing was changed.");
  }
  const kept: BucketPolicyBinding[] = [];
  let changed = false;
  for (const original of policy.bindings ?? []) {
    let b = original;
    if ((b.members ?? []).some((m) => forbidden.includes(m))) {
      b = { ...b, members: (b.members ?? []).filter((m) => !forbidden.includes(m)) };
      changed = true;
      if (!b.members!.length) continue;
    }
    const title = b.condition?.title;
    if (!title?.startsWith(MANAGED_TITLE_PREFIX)) {
      kept.push(b);
      continue;
    }
    const members = (b.members ?? []).filter((m) =>
      desired.some((d) => d.role === b.role && d.member === m && sameCondition(b.condition, d.condition)),
    );
    if (members.length !== (b.members ?? []).length) changed = true;
    if (members.length) kept.push({ ...b, members });
  }
  for (const d of desired) {
    const existing = kept.find((b) => b.role === d.role && sameCondition(b.condition, d.condition));
    if (existing) {
      if (!existing.members?.includes(d.member)) {
        existing.members = [...(existing.members ?? []), d.member];
        changed = true;
      }
      continue;
    }
    kept.push({ role: d.role, members: [d.member], condition: { ...d.condition } });
    changed = true;
  }
  if ((policy.version ?? 1) !== 3) changed = true;
  return { policy: { ...policy, version: 3, bindings: kept }, changed };
}

export interface BucketIamAuth {
  bearer: string;
}

export type BucketIamFetch = (url: string, init: RequestInit) => Promise<Response>;

const STORAGE = "https://storage.googleapis.com/storage/v1/b";
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Read-modify-write the bucket's v3 IAM policy under its etag, retrying a
 * concurrent write (412). Returns whether a write was needed.
 */
export async function ensureConditionalBucketBindings(
  bucket: string,
  desired: readonly ConditionalBinding[],
  auth: BucketIamAuth,
  transport: BucketIamFetch = fetch,
  attempts = 8,
  forbidden: readonly string[] = [],
  backoffMs = 3_000,
): Promise<{ changed: boolean }> {
  objects(bucket);
  const url = `${STORAGE}/${encodeURIComponent(bucket)}/iam`;
  const headers = { authorization: `Bearer ${auth.bearer}`, "content-type": "application/json" };
  for (let attempt = 1; attempt <= attempts; attempt++) {
    const read = await transport(`${url}?optionsRequestedPolicyVersion=3`, { method: "GET", headers });
    if (!read.ok) throw new Error(`96bulls: locker IAM policy read failed with HTTP ${read.status}. Nothing was changed.`);
    const current = (await read.json()) as BucketPolicy;
    const { policy, changed } = reconcileConditionalBindings(current, desired, forbidden);
    if (!changed) return { changed: false };
    const write = await transport(url, { method: "PUT", headers, body: JSON.stringify(policy) });
    if (write.ok) return { changed: true };
    // 412/409: a concurrent policy write; re-read. 400: a service account created
    // seconds ago is not yet a valid member (Alchemy's IAM helper notes the same);
    // back off and retry. Anything else, or a 400 that persists, fails closed.
    const reason = (await write.text().catch(() => "")).replace(/\s+/g, " ").slice(0, 300);
    if (write.status === 400 && attempt < attempts) {
      await sleep(attempt * backoffMs);
      continue;
    }
    if (write.status !== 412 && write.status !== 409)
      throw new Error(`96bulls: locker IAM policy write failed with HTTP ${write.status}: ${reason}`);
  }
  throw new Error("96bulls: locker IAM policy kept changing concurrently; conditional bindings not converged.");
}
