/**
 * The operator tools' stores for a relocated S3 upload ledger (docs/ledger-relocation.md). Used by the fence backfill
 * (scripts/upload-fence-backfill.ts) and the buffered drain (scripts/buffered-drain.ts), the two ledger writers the
 * backlog-ops workflow runs.
 *
 * The tool reads the ledger bucket from the SAME variables as the ingest edge, validated by the edge's own parser
 * (selfhost-config.ts selfhostLedgerStore, with the tool's --bucket / --region standing in for LOCKER_BUCKET /
 * LOCKER_REGION):
 *
 *   INGEST_LEDGER_BUCKET    unset | "" (default: one store, the Locker bucket, as before) | the ledger's own
 *                           bucket, which must differ from --bucket
 *   INGEST_LEDGER_REGION    its signing region; default --region. Only with INGEST_LEDGER_BUCKET.
 *   INGEST_LEDGER_ENDPOINT  its endpoint (bare origin); default AWS S3. Only with INGEST_LEDGER_BUCKET.
 *
 * Any invalid combination refuses before any request: `ledger_bucket_invalid` (a closed code; the value is never
 * printed). With a ledger bucket the tool's store is `ledgerRoutedStore(locker, ledger)`, the one the edge uses: the
 * fences and everything else go to the Locker store (SSE-KMS on an execute, as before), the ledger's own keys
 * (`<root>/.admission-inbox/<org>/v1/ws=<ws>/m=<srv>/ledger/...` and the ledger-home marker) to the ledger store, which sends
 * NO encryption header (the bucket's default, SSE-S3, applies, as on the edge). Both stores use the same credentials.
 *
 * The home check (`assertToolLedgerHome`): with a ledger bucket, always, on BOTH sides: the Locker bucket's own marker
 * (the old location, where the relocation wrote "moved") must exist and name exactly that ledger bucket
 * (`ledger_home_mismatch`: a setting pointing at another environment's or a stale ledger refuses here, before any
 * write), and the ledger bucket's marker must name it (`ledger_home_missing`, `ledger_home_moved`), as the edge checks.
 * Without one, the existing single-bucket check (`assertLedgerHomeHere`: with INGEST_LEDGER_HOME_GUARD=on,
 * `ledger_home_moved` when the Locker bucket's marker names another bucket; off, no request).
 *
 * Not byte-identical to before even unset: with INGEST_LEDGER_HOME_GUARD=on the single-bucket check is also re-run
 * during the run (one marker GET before a ledger write at most once a minute).
 *
 * Long runs re-check (`homeRecheckingStore`): every ledger-key write after TOOL_HOME_RECHECK_MS (60 s, the edge's
 * cadence) since the last good check first runs the same check again; a refusal blocks every later ledger-key write
 * with that code, and the tools stop at their next boundary and exit with it (Locker writes such as the drain's lease
 * release still go through).
 */
import { createS3Store, type ObjectStore } from "../shared/vendor.ts";
import { assertLedgerHomeHere, assertLedgerHomeIn, assertLedgerHomeMovedTo, LedgerHomeRefused, ledgerKeyMatcher, ledgerRoutedStore } from "./ledger-store.ts";
import { selfhostLedgerStore, type SelfhostLedgerStoreSettings } from "./selfhost-config.ts";

export type ToolLedgerBucket = SelfhostLedgerStoreSettings;

/** INGEST_LEDGER_BUCKET / _REGION / _ENDPOINT for a tool on `locker` (its --bucket / --region), or null when unset. */
export function toolLedgerBucket(env: Record<string, string | undefined>, locker: { bucket: string; region: string }): ToolLedgerBucket | null {
  const pick: NodeJS.ProcessEnv = { LOCKER_BUCKET: locker.bucket, LOCKER_REGION: locker.region };
  for (const k of ["INGEST_LEDGER_BUCKET", "INGEST_LEDGER_REGION", "INGEST_LEDGER_ENDPOINT"] as const) if (env[k] !== undefined) pick[k] = env[k];
  try { return selfhostLedgerStore(pick, "s3") ?? null; } catch { throw new Error("ledger_bucket_invalid"); }
}

type S3Credentials = Parameters<typeof createS3Store>[0]["credentials"];
export interface ToolStores {
  /** What the tool reads and writes through: the routed store with a ledger bucket, else the Locker store. */
  store: ObjectStore;
  locker: ObjectStore;
  /** Present only with a ledger bucket. */
  ledger?: { store: ObjectStore; bucket: string };
}

/**
 * The Locker store (SSE-KMS with `kmsKeyArn`, an execute) and, with INGEST_LEDGER_BUCKET, the ledger store (no
 * encryption header) routed under it. Makes no request.
 */
export function toolStores(opts: {
  env: Record<string, string | undefined>;
  bucket: string;
  region: string;
  keyRoot: string;
  credentials: S3Credentials;
  kmsKeyArn?: string;
  create?: (config: Parameters<typeof createS3Store>[0]) => unknown;
  /** The caller's own toolLedgerBucket(env, ...) result, when it already parsed it (parsed once); else parsed here. */
  ledgerBucket?: ToolLedgerBucket | null;
}): ToolStores {
  const create = opts.create ?? createS3Store;
  const sep = opts.ledgerBucket !== undefined ? opts.ledgerBucket : toolLedgerBucket(opts.env, { bucket: opts.bucket, region: opts.region });
  const locker = create({
    bucket: opts.bucket, region: opts.region, credentials: opts.credentials,
    ...(opts.kmsKeyArn !== undefined ? { serverSideEncryption: "aws:kms" as const, sseKmsKeyId: opts.kmsKeyArn } : {}),
  }) as ObjectStore;
  if (!sep) return { store: locker, locker };
  // No serverSideEncryption: the ledger bucket's default encryption applies, exactly as on the edge.
  const ledger = create({ bucket: sep.bucket, region: sep.region, credentials: opts.credentials, ...(sep.endpoint ? { endpoint: sep.endpoint } : {}) }) as ObjectStore;
  return { store: ledgerRoutedStore(locker, ledger, opts.keyRoot), locker, ledger: { store: ledger, bucket: sep.bucket } };
}

/** The tool's fork guard (see the header). Throws LedgerHomeRefused with a closed code. */
export async function assertToolLedgerHome(stores: ToolStores, keyRoot: string, lockerBucket: string,
  env: Record<string, string | undefined> = process.env): Promise<void> {
  if (stores.ledger) {
    await assertLedgerHomeMovedTo(stores.locker, keyRoot, stores.ledger.bucket);
    return assertLedgerHomeIn(stores.store, keyRoot, stores.ledger.bucket);
  }
  return assertLedgerHomeHere(stores.locker, keyRoot, lockerBucket, env);
}

/** The edge re-reads the ledger-home marker about every minute; the tools do the same before a write. */
export const TOOL_HOME_RECHECK_MS = 60_000;

/** A store with the home check re-run during the run; `homeRefusal()` is the sticky refusal once there is one. */
export type HomeGuardedStore = ObjectStore & { homeRefusal(): LedgerHomeRefused | undefined };

/**
 * `store` with the home check re-run before a guarded write (put or delete) once `intervalMs` has passed since the
 * last good check (the caller ran the first one: the clock starts at creation). With `keyRoot`, only writes of ledger
 * keys (ledger-store.ts isLedgerKey: ledger/** and the marker) are guarded: the Locker bucket's own objects (the
 * fences, staged batches, drained markers, the layout, the drain's lease) pass straight through, so a run that is
 * refused can still release its lease. Without `keyRoot`, every write is guarded. A refusal is sticky: every later
 * guarded write throws it without another request (and `homeRefusal()` returns it, so a caller can stop at its next
 * boundary), except `ledger_home_unavailable` (transient), which the next guarded write retries. Reads pass through
 * unchecked. Concurrent writes share one re-check.
 */
export function homeRecheckingStore(store: ObjectStore, recheck: () => Promise<void>,
  opts: { intervalMs?: number; now?: () => number; keyRoot?: string } = {}): HomeGuardedStore {
  const interval = opts.intervalMs ?? TOOL_HOME_RECHECK_MS, now = opts.now ?? Date.now;
  const guarded = opts.keyRoot === undefined ? () => true : ledgerKeyMatcher(opts.keyRoot);
  let checkedAt = now();
  let refused: unknown;
  let inflight: Promise<void> | null = null;
  const gate = async (key: unknown) => {
    if (!guarded(String(key))) return;
    if (refused !== undefined) throw refused;
    if (now() - checkedAt < interval) return;
    inflight ??= recheck().then(
      () => { checkedAt = now(); },
      (error) => { if (!(error instanceof LedgerHomeRefused && error.code === "ledger_home_unavailable")) refused = error; throw error; },
    ).finally(() => { inflight = null; });
    await inflight;
  };
  return {
    backend: store.backend,
    head: (key) => store.head(key),
    get: (key, range) => store.get(key, range),
    getObject: (key, range) => store.getObject(key, range),
    list: (prefix, cursor) => store.list(prefix, cursor),
    presignGet: (key, ttl) => store.presignGet(key, ttl),
    put: async (key, body, putOpts) => { await gate(key); return store.put(key, body, putOpts); },
    delete: async (key) => { await gate(key); return store.delete(key); },
    homeRefusal: () => (refused instanceof LedgerHomeRefused ? refused : refused === undefined ? undefined : new LedgerHomeRefused("ledger_home_store_error")),
  };
}

/** The tool's store with its home check re-run during a long run (assertToolLedgerHome, homeRecheckingStore). */
export function homeGuardedToolStore(stores: ToolStores, keyRoot: string, lockerBucket: string,
  env: Record<string, string | undefined> = process.env, opts: { intervalMs?: number; now?: () => number } = {}): HomeGuardedStore {
  // Only the ledger's own keys are blocked by a refusal; the Locker's objects (the lease included) are not.
  return homeRecheckingStore(stores.store, () => assertToolLedgerHome(stores, keyRoot, lockerBucket, env), { ...opts, keyRoot });
}
