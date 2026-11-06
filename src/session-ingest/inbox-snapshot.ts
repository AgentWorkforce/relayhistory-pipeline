/** Immutable, paged S3 metadata snapshots. Never treats a missing/corrupt page as absence.
 * Generic storage component only; no production handler imports this module.
 * One public operation at a time. maxOperations/bytes count logical ObjectStore
 * work, not hidden HTTP calls or bytes inside the adapter. The caller must also
 * enforce HTTP subrequest/abort budgets in the transport. Deadline refusal may
 * leave a late immutable page PUT; it never returns/adopts a root for that work.
 * Values are bounded metadata (8KiB each), not upload bodies. Cache bytes are
 * serialized page weights, not an exact heap measurement.
 */
import { createHash } from "node:crypto";
import { objectKey, type ObjectStore } from "../shared/vendor.ts";
export type SnapshotValue =
  | null
  | boolean
  | number
  | string
  | SnapshotValue[]
  | { [key: string]: SnapshotValue };
type Leaf = { v: 1; kind: "leaf"; entries: [string, SnapshotValue][] };
type Branch = { v: 1; kind: "branch"; children: [string, string][] };
type Page = Leaf | Branch;
export interface SnapshotBudget {
  maxOperations: number;
  maxBytes: number;
  maxCacheBytes: number;
  deadline: number;
}
const encoder = new TextEncoder(),
  MAX_PAGE = 128 * 1024,
  LEAF = 64,
  DEPTH = 16;
const sha = (b: string | Uint8Array) =>
  createHash("sha256").update(b).digest("hex");
const hex = (s: unknown): s is string =>
  typeof s === "string" && /^[a-f0-9]{64}$/.test(s);
export class InboxSnapshotError extends Error {
  constructor(readonly code: string) {
    super("inbox_snapshot_" + code);
    this.name = "InboxSnapshotError";
  }
}
function fail(code: string): never {
  throw new InboxSnapshotError(code);
}
function validKey(key: unknown): key is string {
  return (
    typeof key === "string" &&
    key.length > 0 &&
    key.length <= 2048 &&
    encoder.encode(key).length <= 2048 &&
    !/[\u0000\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/.test(
      key,
    )
  );
}
const MAX_VALUE_BYTES = 8192,
  MAX_INPUT_BYTES = 512 * 1024,
  MAX_CHANGES = 2048;
/** Bounded JSON encoding. Never invokes value getters or user toJSON methods. */
function canonical(
  v: SnapshotValue,
  limit: number,
  check: () => void,
  maxNodes = 512,
  maxDepth = 24,
): string {
  const parts: string[] = [];
  let size = 0,
    nodes = 0;
  function append(s: string) {
    size += encoder.encode(s).length;
    if (size > limit) fail("value_bound");
    parts.push(s);
  }
  function string(v: string) {
    if (v.length > MAX_VALUE_BYTES) fail("value_bound");
    return JSON.stringify(v);
  }
  function walk(value: SnapshotValue, depth: number) {
    check();
    if (++nodes > maxNodes) fail("value_nodes");
    if (depth > maxDepth) fail("value_depth");
    if (value === null || typeof value === "boolean") {
      append(JSON.stringify(value));
      return;
    }
    if (typeof value === "string") {
      append(string(value));
      return;
    }
    if (typeof value === "number") {
      if (!Number.isFinite(value)) fail("value_number");
      append(JSON.stringify(value));
      return;
    }
    if (Array.isArray(value)) {
      if (value.length > 1024) fail("value_count");
      append("[");
      for (let i = 0; i < value.length; i++) {
        const d = Object.getOwnPropertyDescriptor(value, String(i));
        if (!d || !("value" in d)) fail("value_type");
        if (i) append(",");
        walk(d.value, depth + 1);
      }
      append("]");
      return;
    }
    if (
      !value ||
      typeof value !== "object" ||
      (Object.getPrototypeOf(value) !== Object.prototype &&
        Object.getPrototypeOf(value) !== null)
    )
      fail("value_type");
    const keys: string[] = [];
    for (const key in value) {
      check();
      if (Object.hasOwn(value, key)) {
        if (keys.length >= 128) fail("value_count");
        keys.push(key);
      }
    }
    append("{");
    let first = true;
    for (const key of keys.sort()) {
      const d = Object.getOwnPropertyDescriptor(value, key);
      if (!d || !("value" in d)) fail("value_type");
      if (!first) append(",");
      first = false;
      append(string(key));
      append(":");
      walk(d.value, depth + 1);
    }
    append("}");
  }
  walk(v, 0);
  return parts.join("");
}
export function createInboxSnapshot(
  store: ObjectStore,
  prefix: string,
  budget: SnapshotBudget,
) {
  if (
    !/^[A-Za-z0-9][A-Za-z0-9_/-]{0,199}$/.test(prefix) ||
    prefix.endsWith("/") ||
    prefix.split("/").some((p) => !p || p === ".." || p === ".")
  )
    fail("prefix");
  for (const n of [
    budget.maxOperations,
    budget.maxBytes,
    budget.maxCacheBytes,
    budget.deadline,
  ])
    if (!Number.isSafeInteger(n) || n < 0) fail("budget_config");
  budget = { ...budget };
  let operations = 0,
    bytes = 0,
    cacheBytes = 0,
    busy = false;
  const cache = new Map<string, { page: Page; bytes: number }>();
  function check(n = 0) {
    if (Date.now() > budget.deadline) fail("deadline");
    if (operations > budget.maxOperations || bytes + n > budget.maxBytes)
      fail("budget");
  }
  function operation() {
    check();
    if (operations >= budget.maxOperations) fail("budget");
    operations++;
  }
  async function until<T>(
    start: () => Promise<T>,
    late?: (value: T) => void,
  ): Promise<T> {
    check();
    let expired = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const work = start();
    void work.then(
      (value) => {
        if (expired) {
          try {
            late?.(value);
          } catch {
            /* Best-effort disposal only. */
          }
        }
      },
      () => {},
    );
    try {
      const result = await Promise.race([
        work,
        new Promise<never>((_, reject) => {
          timer = setTimeout(
            () => {
              expired = true;
              reject(new InboxSnapshotError("deadline"));
            },
            Math.min(2147483647, Math.max(0, budget.deadline - Date.now())),
          );
        }),
      ]);
      check();
      return result;
    } finally {
      if (timer !== undefined) clearTimeout(timer);
    }
  }
  function remember(hash: string, page: Page, size: number) {
    if (size > budget.maxCacheBytes) return;
    while (cacheBytes + size > budget.maxCacheBytes || cache.size >= 8) {
      const first = cache.keys().next().value;
      if (first === undefined) break;
      cacheBytes -= cache.get(first)!.bytes;
      cache.delete(first);
    }
    const old = cache.get(hash);
    if (old) {
      cacheBytes -= old.bytes;
      cache.delete(hash);
    }
    cache.set(hash, { page, bytes: size });
    cacheBytes += size;
  }
  const path = (hash: string) =>
    objectKey(`${prefix}/snapshots/sha256/${hash}.json`);
  function validate(v: unknown, depth: number, route: string): Page {
    if (!v || typeof v !== "object" || Array.isArray(v)) fail("page_shape");
    const p = v as Record<string, unknown>;
    if (p.v !== 1 || depth > DEPTH) fail("page_version");
    if (p.kind === "leaf") {
      if (
        Object.keys(p).sort().join(",") !== "entries,kind,v" ||
        !Array.isArray(p.entries) ||
        p.entries.length > LEAF
      )
        fail("leaf_shape");
      let previous: string | undefined;
      for (const e of p.entries) {
        if (
          !Array.isArray(e) ||
          e.length !== 2 ||
          !validKey(e[0]) ||
          (previous !== undefined && previous >= e[0]) ||
          !sha(e[0]).startsWith(route)
        )
          fail("leaf_identity");
        canonical(e[1] as SnapshotValue, MAX_VALUE_BYTES, check);
        previous = e[0];
      }
      return p as unknown as Leaf;
    }
    if (
      p.kind !== "branch" ||
      Object.keys(p).sort().join(",") !== "children,kind,v" ||
      !Array.isArray(p.children) ||
      p.children.length < 1 ||
      p.children.length > 256 ||
      depth >= DEPTH
    )
      fail("branch_shape");
    let previous = "";
    for (const e of p.children) {
      if (
        !Array.isArray(e) ||
        e.length !== 2 ||
        typeof e[0] !== "string" ||
        !/^[a-f0-9]{2}$/.test(e[0]) ||
        previous >= e[0] ||
        !hex(e[1])
      )
        fail("branch_identity");
      previous = e[0];
    }
    return p as unknown as Branch;
  }
  async function read(
    hash: string,
    depth: number,
    route: string,
  ): Promise<Page> {
    if (!hex(hash)) fail("pointer");
    check();
    const hit = cache.get(hash);
    if (hit) return validate(hit.page, depth, route);
    operation();
    const object = await until(
      () => store.getObject(path(hash)),
      (value) => {
        void value.body.cancel().catch(() => {});
      },
    );
    const reader = object.body.getReader();
    const chunks: Uint8Array[] = [];
    let n = 0;
    try {
      if (
        !Number.isSafeInteger(object.meta.size) ||
        object.meta.size < 0 ||
        object.meta.size > MAX_PAGE
      )
        fail("page_bound");
      check(object.meta.size);
      bytes += object.meta.size;
      for (;;) {
        const r = await until(() => reader.read());
        if (r.done) break;
        n += r.value.length;
        if (n > MAX_PAGE || n > object.meta.size) fail("page_bound");
        check();
        chunks.push(r.value);
      }
    } finally {
      void reader.cancel().catch(() => {});
      try {
        reader.releaseLock();
      } catch {
        /* A timed-out pending read is already abandoned. */
      }
    }
    if (n !== object.meta.size) fail("page_size");
    check();
    const body = new Uint8Array(n);
    let at = 0;
    for (const c of chunks) {
      body.set(c, at);
      at += c.length;
    }
    if (sha(body) !== hash) fail("page_digest");
    let v: unknown;
    try {
      v = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(body));
    } catch {
      fail("page_json");
    }
    const page = validate(v, depth, route);
    remember(hash, page, n);
    return page;
  }
  async function write(page: Page): Promise<string> {
    const body = encoder.encode(
      canonical(page as unknown as SnapshotValue, MAX_PAGE, check, 65536, 27),
    );
    if (body.length > MAX_PAGE) fail("page_bound");
    check(body.length);
    operation();
    bytes += body.length;
    check();
    const hash = sha(body);
    const ack = await until(() =>
      store.put(path(hash), body, {
        ifNoneMatch: true,
        contentSha256: hash,
        contentType: "application/json",
      }),
    );
    if (!ack.created) {
      const old = cache.get(hash);
      if (old) {
        cacheBytes -= old.bytes;
        cache.delete(hash);
      }
      await read(hash, 0, "");
    } else remember(hash, page, body.length);
    return hash;
  }
  async function lookup(
    root: string | null,
    key: string,
  ): Promise<SnapshotValue | undefined> {
    if (!validKey(key)) fail("key");
    let hash = root;
    const address = sha(key);
    let route = "",
      depth = 0;
    while (hash !== null) {
      const p = await read(hash, depth, route);
      if (p.kind === "leaf") {
        const value = p.entries.find((e) => e[0] === key)?.[1];
        return value === undefined ? undefined : structuredClone(value);
      }
      const part = address.slice(depth * 2, depth * 2 + 2);
      hash = p.children.find((e) => e[0] === part)?.[1] ?? null;
      route += part;
      depth++;
    }
    return undefined;
  }
  async function build(
    entries: [string, SnapshotValue][],
    depth: number,
    route: string,
  ): Promise<string | null> {
    if (!entries.length) return null;
    if (entries.length <= LEAF) {
      const p: Leaf = {
        v: 1,
        kind: "leaf",
        entries: entries.sort((a, b) =>
          a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0,
        ),
      };
      let size = 40;
      for (const [key, value] of p.entries) {
        check();
        size +=
          encoder.encode(JSON.stringify(key)).length +
          encoder.encode(canonical(value, MAX_VALUE_BYTES, check)).length +
          4;
      }
      if (size <= MAX_PAGE) return write(p);
    }
    if (depth >= DEPTH) fail("depth_exhausted");
    const groups = new Map<string, [string, SnapshotValue][]>();
    for (const e of entries) {
      const part = sha(e[0]).slice(depth * 2, depth * 2 + 2);
      const g = groups.get(part) ?? [];
      g.push(e);
      groups.set(part, g);
    }
    const children: [string, string][] = [];
    for (const [part, g] of [...groups].sort((a, b) =>
      a[0].localeCompare(b[0]),
    )) {
      const hash = await build(g, depth + 1, route + part);
      if (hash) children.push([part, hash]);
    }
    return write({ v: 1, kind: "branch", children });
  }
  async function change(
    root: string | null,
    updates: Map<string, SnapshotValue | undefined>,
    depth: number,
    route: string,
  ): Promise<string | null> {
    if (!updates.size) return root;
    const old =
      root === null
        ? { v: 1 as const, kind: "leaf" as const, entries: [] }
        : await read(root, depth, route);
    if (old.kind === "leaf") {
      const merged = new Map(old.entries);
      for (const [k, v] of updates) {
        if (v === undefined) merged.delete(k);
        else merged.set(k, v);
      }
      return build([...merged], depth, route);
    }
    const children = new Map(old.children),
      groups = new Map<string, Map<string, SnapshotValue | undefined>>();
    for (const [k, v] of updates) {
      const part = sha(k).slice(depth * 2, depth * 2 + 2);
      const g = groups.get(part) ?? new Map();
      g.set(k, v);
      groups.set(part, g);
    }
    for (const [part, g] of groups) {
      const next = await change(
        children.get(part) ?? null,
        g,
        depth + 1,
        route + part,
      );
      if (next === null) children.delete(part);
      else children.set(part, next);
    }
    if (!children.size) return null;
    return write({
      v: 1,
      kind: "branch",
      children: [...children].sort((a, b) => a[0].localeCompare(b[0])),
    });
  }
  async function protect<T>(work: () => Promise<T>): Promise<T> {
    if (busy) fail("concurrent_use");
    busy = true;
    try {
      check();
      const result = await work();
      check();
      return result;
    } catch (e) {
      if (e instanceof InboxSnapshotError) throw e;
      return fail("storage_unavailable");
    } finally {
      busy = false;
    }
  }
  return {
    lookup: (root: string | null, key: string) =>
      protect(() => lookup(root, key)),
    apply: (
      root: string | null,
      changes: [string, SnapshotValue | undefined][],
    ) =>
      protect(async () => {
        if (root !== null && !hex(root)) fail("pointer");
        if (!Array.isArray(changes) || changes.length > MAX_CHANGES)
          fail("input_count");
        if (
          changes.length &&
          (budget.maxOperations === 0 || budget.maxBytes === 0)
        )
          fail("budget");
        let inputBytes = 0;
        const m = new Map<string, SnapshotValue | undefined>();
        for (const [k, v] of changes) {
          check(1);
          if (!validKey(k)) fail("key");
          const encoded =
            v === undefined
              ? undefined
              : canonical(v, Math.min(MAX_VALUE_BYTES, budget.maxBytes), check);
          inputBytes +=
            encoder.encode(k).length +
            (encoded === undefined ? 0 : encoder.encode(encoded).length);
          if (inputBytes > Math.min(MAX_INPUT_BYTES, budget.maxBytes))
            fail("input_bound");
          m.set(
            k,
            encoded === undefined
              ? undefined
              : (JSON.parse(encoded) as SnapshotValue),
          );
        }
        return change(root, m, 0, "");
      }),
    stats: () => ({ operations, bytes, cacheBytes }),
  };
}
