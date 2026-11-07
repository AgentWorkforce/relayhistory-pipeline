/**
 * Test-only versioned object store (S3-bucket-versioning semantics, in memory): every put adds a version, a delete adds
 * a delete marker, `ifNoneMatch`/`ifMatch` are conditional on the current version, and `versions()` exposes every
 * stored version so a test can require that no version of erased content survives. Paired with a
 * PreservedVersionEraser that permanently removes all versions of a key except one.
 */
import { createHash } from "node:crypto";
import { ObjectStoreError } from "../../../packages/object-store/src/errors.ts";
import type { ObjectKey } from "../../../packages/object-store/src/key.ts";
import type { GetObject, ObjectMeta, ObjectStore, Page, PutAck, PutOpts } from "../../../packages/object-store/src/types.ts";
import type { PreservedVersionEraser } from "../../migration/neon-s3-erasure/eraser.ts";

interface V { id: string; body: Uint8Array | null; at: Date }

export class VersionedMemoryStore implements ObjectStore {
  readonly backend = "s3";
  #n = 0;
  readonly #keys = new Map<string, V[]>();
  #current(key: string): V | undefined {
    const v = this.#keys.get(key)?.at(-1);
    return v && v.body ? v : undefined;
  }
  #meta(key: string, v: V): ObjectMeta {
    return { key: key as ObjectKey, size: v.body!.byteLength, lastModified: v.at, version: { eTag: v.id, version: v.id }, metadata: {} };
  }
  async head(key: ObjectKey) {
    const v = this.#current(key);
    return v ? this.#meta(key, v) : undefined;
  }
  async get(key: ObjectKey) {
    return (await this.getObject(key)).body;
  }
  async getObject(key: ObjectKey): Promise<GetObject> {
    const v = this.#current(key);
    if (!v) throw new ObjectStoreError("not_found", "s3", `${key}: not found`);
    const body = v.body!;
    return { meta: this.#meta(key, v), body: new ReadableStream({ start(c) { c.enqueue(body); c.close(); } }) } as GetObject;
  }
  async list(prefix: ObjectKey): Promise<Page<ObjectMeta>> {
    const items = [...this.#keys.keys()].filter((k) => k.startsWith(prefix)).sort().flatMap((k) => {
      const v = this.#current(k);
      return v ? [this.#meta(k, v)] : [];
    });
    return { items };
  }
  async presignGet(): Promise<URL> {
    throw new ObjectStoreError("unsupported", "s3", "presign not supported in tests");
  }
  async put(key: ObjectKey, body: Uint8Array, opts: PutOpts = {}): Promise<PutAck> {
    const cur = this.#current(key);
    const sha = createHash("sha256").update(body).digest("hex");
    if (opts.ifNoneMatch && cur) {
      if (Buffer.compare(Buffer.from(cur.body!), Buffer.from(body)) === 0) return { key, version: { eTag: cur.id, version: cur.id }, contentSha256: sha, size: body.byteLength, created: false };
      throw new ObjectStoreError("already_exists", "s3", `${key}: exists`);
    }
    if (opts.ifMatch && (!cur || (opts.ifMatch.eTag ?? opts.ifMatch.version) !== cur.id)) throw new ObjectStoreError("precondition_failed", "s3", `${key}: version changed`);
    const v: V = { id: `v${++this.#n}`, body: new Uint8Array(body), at: new Date() };
    this.#keys.set(key, [...(this.#keys.get(key) ?? []), v]);
    return { key, version: { eTag: v.id, version: v.id }, contentSha256: sha, size: body.byteLength, created: true };
  }
  async delete(key: ObjectKey): Promise<void> {
    if (this.#keys.has(key)) this.#keys.get(key)!.push({ id: `v${++this.#n}`, body: null, at: new Date() });
  }
  /** Every stored version (delete markers have body null). */
  versions(prefix = ""): { key: string; id: string; body: Uint8Array | null }[] {
    return [...this.#keys.entries()].filter(([k]) => k.startsWith(prefix)).flatMap(([key, vs]) => vs.map((v) => ({ key, id: v.id, body: v.body })));
  }
  eraser(): PreservedVersionEraser {
    return {
      eraseObjectExcept: async (key, keep) => {
        const vs = this.#keys.get(key) ?? [];
        const kept = vs.filter((v) => v.id === keep);
        this.#keys.set(key, kept);
        return { deleted: vs.length - kept.length, kept: kept.length };
      },
    };
  }
}
