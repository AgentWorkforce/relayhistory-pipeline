/** Worker adapter for Native's frozen publication authority contract.
 * All model/decrypt/object work belongs outside withDerivedCurrent's callback.
 */
import { inTransaction, type HoopDb } from "./db.ts";
import { PipelineError, parseSessionKey, type ManifestV1, type SqlExecutor, type StageMessage } from "./vendor.ts";
import { parseUploadGenerationPrefix } from "../../packages/object-store/src/index.ts";
import { derivedCurrent, managedDerivedManifest } from "./derived-session.ts";
import {
  publicationEvidence, probePublication, withPublicationAuthority,
  type PublicationAuthority, type PublicationEvidence,
} from "./publication-authority.ts";

export interface DerivedPublication {
  evidence: PublicationEvidence;
  authority: PublicationAuthority;
}
function checkAuthority(key: string, evidence: PublicationEvidence, authority: PublicationAuthority) {
  const path = parseUploadGenerationPrefix(parseSessionKey(key)?.prefix);
  if ((managedDerivedManifest(key) && !authority.managed) ||
      (authority.managed && authority.current && (
        !managedDerivedManifest(key) || path?.generation !== authority.generation || evidence.revision !== authority.revision)))
    throw new PipelineError("derived_publication_invalid", "the current publication authority did not match the source", true);
}

/** When enabled, probe ALL jobs, including old legacy keys after a managed
 * takeover. The compiled generation is supplied by the container/Lambda build.
 * A missing backend or invalid gate must fail closed, never retry as legacy.
 */
export async function derivedPublication(
  db: SqlExecutor, message: StageMessage, m: ManifestV1, key: string, schemaGeneration?: string,
): Promise<DerivedPublication | undefined> {
  if (schemaGeneration === undefined) {
    if (managedDerivedManifest(key))
      throw new PipelineError("derived_managed_authority_unavailable", "managed derived publication requires the current-image authority contract", false);
    return undefined;
  }
  if (!/^[0-9a-f]{64}$/.test(schemaGeneration))
    throw new PipelineError("derived_generation_invalid", "compiled publication schema generation is unavailable", false);
  const evidence = publicationEvidence(message.ctx.orgId, key, m, schemaGeneration);
  const authority = await probePublication(db, evidence);
  checkAuthority(key, evidence, authority);
  return { evidence, authority };
}

/** For existing legacy transactions: native authority first, before their row
 * or summary locks. Managed jobs use the separate immutable-output path. */
export async function lockDerivedPublication(tx: SqlExecutor, publication: DerivedPublication | undefined, key: string): Promise<boolean> {
  if (!publication) return true;
  const checked = await withPublicationAuthority(fn => fn(tx), publication.evidence, async (_tx, authority) => {
    checkAuthority(key, publication.evidence, authority);
    return true;
  });
  return checked.current;
}

/** Native's generation/user/head lock runs BEFORE the legacy index row lock.
 * Both fences and the metadata writes use the same short SQL transaction.
 * Returning current=false never invokes work and never reconstructs a pointer.
 */
export async function withDerivedCurrent<T>(args: {
  db: HoopDb; message: StageMessage; manifest: ManifestV1; key: string;
  publication?: DerivedPublication;
}, work: (tx: SqlExecutor, authority?: PublicationAuthority) => Promise<T>): Promise<{ current: false } | { current: true; value: T }> {
  const transact = <R>(fn: (tx: SqlExecutor) => Promise<R>) => inTransaction(args.db, async tx => {
    await tx.query("SET LOCAL lock_timeout = '5s'");
    await tx.query("SET LOCAL statement_timeout = '30s'");
    return fn(tx);
  });
  const lockedWork = async (tx: SqlExecutor, authority?: PublicationAuthority) => {
    if (!(await derivedCurrent(tx, args.message, args.manifest, args.key, true))) return { current: false } as const;
    return { current: true, value: await work(tx, authority) } as const;
  };
  if (!args.publication) return transact(tx => lockedWork(tx));
  const result = await withPublicationAuthority(transact, args.publication.evidence, async (tx, authority) => {
    checkAuthority(args.key, args.publication!.evidence, authority);
    return lockedWork(tx, authority);
  });
  return result.current ? result.value : { current: false };
}
