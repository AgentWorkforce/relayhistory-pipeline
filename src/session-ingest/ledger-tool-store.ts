/**
 * The operator tools' stores for a relocated S3 upload ledger (docs/ledger-relocation.md). Used by the fence backfill
 * (scripts/upload-fence-backfill.ts) and the buffered drain (scripts/buffered-drain.ts), the two ledger writers the
 * backlog-ops workflow runs.
 *
 * The tool reads the ledger bucket from the SAME variables as the ingest edge, validated by the edge's own parser
 * (selfhost-config.ts selfhostLedgerStore, with the tool's --bucket / --region standing in for LOCKER_BUCKET /
 * LOCKER_REGION):
 *
 *   INGEST_LEDGER_BUCKET    unset | "" (default: one store, the Locker bucket, exactly as before) | the ledger's own
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
 * The home check (`assertToolLedgerHome`): with a ledger bucket, always, in the ledger bucket, refusing unless its
 * marker names that bucket (`ledger_home_missing`, `ledger_home_moved`), as the edge does. Without one, the existing
 * single-bucket check (`assertLedgerHomeHere`: with INGEST_LEDGER_HOME_GUARD=on, `ledger_home_moved` when the Locker
 * bucket's marker names another bucket; off, no request).
 */
import { createS3Store, type ObjectStore } from "../shared/vendor.ts";
import { assertLedgerHomeHere, assertLedgerHomeIn, ledgerRoutedStore } from "./ledger-store.ts";
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
}): ToolStores {
  const create = opts.create ?? createS3Store;
  const sep = toolLedgerBucket(opts.env, { bucket: opts.bucket, region: opts.region });
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
  if (stores.ledger) return assertLedgerHomeIn(stores.store, keyRoot, stores.ledger.bucket);
  return assertLedgerHomeHere(stores.locker, keyRoot, lockerBucket, env);
}
