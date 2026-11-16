/**
 * A separate object store for the S3 upload ledger (INGEST_LEDGER_BUCKET, docs/ledger-relocation.md): the ledger's
 * own objects can live in a bucket next to the process (same region) while everything else (the inbox, the privacy
 * journals, the fence-authority markers, the canonical layout) stays in the Locker bucket. Unset, nothing changes:
 * the one Locker store holds everything, as before.
 *
 * Which keys are ledger keys (ledger-s3.ts, ledger-log.ts, rearm-sweeper.ts): under a per-user admission directory
 * (async-admission.ts admissionUserDir, `<keyRoot>/.admission-inbox/<org>/v1/ws=<ws>/m=<srv>`)
 *
 *   <dir>/ledger/...           batch records, revision shards, ownership stubs, re-arm markers and their completion
 *                              records, and the log-commit streams
 *
 * and `<keyRoot>/.ledger/fence-authority/.ledger-home.json` (LedgerHomeDoc), the marker that names the bucket the
 * ledger lives in (under the fence-authority prefix, which every ledger reader may already GET; its dot name is never
 * an org's marker). Nothing else moves. In particular the fence (`<dir>/upload-fences.json`) STAYS in the Locker
 * bucket: the hosted Worker's S3 fence mirror (INGEST_S3_FENCE_MIRROR, ledger-s3.ts s3FenceMirror) writes it there, so
 * moving it would hide a deletion or reset from the ledger. Nor do `privacy-journal.json` (Control and the inbox
 * bridge), the inbox, the outcomes, the fence-authority markers or the canonical layout.
 *
 * `ledgerRoutedStore(main, ledger, keyRoot)` is ONE ObjectStore over both: every per-key call goes to the store that
 * owns the key, and a `list` whose prefix can hold keys of both kinds lists both and merges them in key order (each
 * side filtered to the keys it owns, so a stale copy of a ledger key left in the Locker bucket after a relocation is
 * never listed), with an opaque composite cursor. The caller sees the same keys, in the same order, as one bucket
 * holding exactly the live objects, so no ledger, consumer or sweeper code changes.
 *
 * The ledger-home marker is the fork guard (ledger-s3.ts `ledgerHome`): a ledger whose own store holds a marker naming
 * ANOTHER bucket refuses every claim (`ledger_home_moved`), and one configured with a separate ledger bucket refuses
 * without a marker naming it (`ledger_home_missing`). A relocation writes "moved" into the old location before the
 * new one, so a process left on the old configuration can never admit against the stale copy.
 */
import { isObjectStoreError, objectKey, readAll, type ObjectStore } from "../shared/vendor.ts";
import { INBOX_NAMESPACE } from "./async-admission.ts";

type Page = Awaited<ReturnType<ObjectStore["list"]>>;
type Item = Page["items"][number];

/** The marker naming the ledger's bucket (per deployment and key root), in the ledger's own store. */
export const ledgerHomeKey = (keyRoot: string) => `${keyRoot}/.ledger/fence-authority/.ledger-home.json`;
/** `home`: the bucket that holds this key root's ledger. */
export interface LedgerHomeDoc { v: 1; home: string; writtenAtMs: number }
export function validLedgerHome(v: unknown): v is LedgerHomeDoc {
  if (v === null || typeof v !== "object" || Array.isArray(v)) return false;
  const o = v as Record<string, unknown>;
  return Object.keys(o).length === 3 && o.v === 1 && typeof o.home === "string" && o.home.length > 0
    && typeof o.writtenAtMs === "number" && Number.isSafeInteger(o.writtenAtMs) && o.writtenAtMs > 0;
}

const esc = (s: string) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

/** The predicate for one key root: is `key` a ledger object (it lives in the ledger store)? */
export function ledgerKeyMatcher(keyRoot: string): (key: string) => boolean {
  const user = new RegExp(`^${esc(keyRoot)}/${esc(INBOX_NAMESPACE)}/[^/]+/v1/ws=[^/]+/m=[^/]+/ledger/.`);
  const home = ledgerHomeKey(keyRoot);
  return (key) => key === home || user.test(key);
}
export const isLedgerKey = (keyRoot: string, key: string) => ledgerKeyMatcher(keyRoot)(key);

/**
 * Where a listing of `prefix` (keys under `prefix/`) can find objects: only ledger keys ("ledger"), no ledger key
 * ("main"), or both ("both").
 */
export function ledgerListScope(keyRoot: string, prefix: string): "main" | "ledger" | "both" {
  const segs = prefix.split("/");
  // These hold the home marker next to main objects.
  if (prefix === keyRoot || prefix === `${keyRoot}/.ledger` || prefix === `${keyRoot}/.ledger/fence-authority`) return "both";
  const want: ((s: string) => boolean)[] = [
    (s) => s === keyRoot, (s) => s === INBOX_NAMESPACE, (s) => s.length > 0, (s) => s === "v1",
    (s) => s.startsWith("ws=") && s.length > 3, (s) => s.startsWith("m=") && s.length > 2, (s) => s === "ledger",
  ];
  for (let i = 0; i < Math.min(segs.length, want.length); i++) if (!want[i]!(segs[i]!)) return "main";
  return segs.length >= want.length ? "ledger" : "both";
}

const CURSOR = "lr1.";
/** Whether `cursor` came from a routed store's merged listing (a plain store's cursor never does). */
export const isRoutedCursor = (cursor: string) => cursor.startsWith(CURSOR);
interface MergeState { m?: string | null; l?: string | null; a?: string }
const encode = (s: MergeState) => CURSOR + Buffer.from(JSON.stringify(s)).toString("base64url");
function decode(cursor: string | undefined): MergeState {
  if (cursor === undefined) return {};
  if (!cursor.startsWith(CURSOR)) throw new Error("ledger_store_cursor_invalid");
  let v: unknown;
  try { v = JSON.parse(Buffer.from(cursor.slice(CURSOR.length), "base64url").toString("utf8")); } catch { throw new Error("ledger_store_cursor_invalid"); }
  const ok = (x: unknown) => x === undefined || x === null || typeof x === "string";
  if (v === null || typeof v !== "object" || Array.isArray(v)) throw new Error("ledger_store_cursor_invalid");
  const s = v as Record<string, unknown>;
  if (!ok(s.m) || !ok(s.l) || !(s.a === undefined || typeof s.a === "string")) throw new Error("ledger_store_cursor_invalid");
  return s as MergeState;
}

/**
 * One store over the Locker store (`main`) and the ledger store (`ledger`). `ledger === main` is refused: unset the
 * setting instead (the caller then uses `main` alone).
 */
export function ledgerRoutedStore(main: ObjectStore, ledger: ObjectStore, keyRoot: string): ObjectStore {
  if (ledger === main) throw new Error("ledger_store_same");
  const isLedger = ledgerKeyMatcher(keyRoot);
  const pick = (key: unknown) => (isLedger(String(key)) ? ledger : main);
  const k = (i: Item) => String(i.key);

  async function merged(prefix: Parameters<ObjectStore["list"]>[0], cursor: string | undefined): Promise<Page> {
    const s = decode(cursor);
    const fetch = async (store: ObjectStore, at: string | null | undefined, own: (key: string) => boolean) => {
      if (at === null) return null;
      const page = await store.list(prefix, at);
      return { page, items: page.items.filter((i) => own(k(i)) && (s.a === undefined || k(i) > s.a)) };
    };
    const [pm, pl] = await Promise.all([fetch(main, s.m, (x) => !isLedger(x)), fetch(ledger, s.l, isLedger)]);
    // Everything up to the smallest last key of a side that has more pages is final; the rest waits for that side.
    const lastOf = (p: Page) => (p.items.length ? k(p.items[p.items.length - 1]!) : undefined);
    let bound: string | undefined;
    for (const p of [pm, pl]) {
      if (!p || p.page.nextCursor === undefined) continue;
      const last = lastOf(p.page);
      if (last !== undefined && (bound === undefined || last < bound)) bound = last;
    }
    const items = [...(pm?.items ?? []), ...(pl?.items ?? [])].filter((i) => bound === undefined || k(i) <= bound).sort((x, y) => (k(x) < k(y) ? -1 : k(x) > k(y) ? 1 : 0));
    // A side whose whole page is now emitted advances; the other re-reads the same page next time (after `a`).
    const advance = (p: typeof pm, at: string | null | undefined): string | null | undefined => {
      if (!p) return null;
      const last = lastOf(p.page);
      const done = bound === undefined || last === undefined || last <= bound;
      if (!done) return at;
      return p.page.nextCursor === undefined ? null : p.page.nextCursor;
    };
    const next: MergeState = { m: advance(pm, s.m), l: advance(pl, s.l), ...(items.length ? { a: k(items[items.length - 1]!) } : s.a !== undefined ? { a: s.a } : {}) };
    return { items, ...(next.m === null && next.l === null ? {} : { nextCursor: encode(next) }) };
  }

  return {
    backend: main.backend,
    head: (key) => pick(key).head(key),
    get: (key, range) => pick(key).get(key, range),
    getObject: (key, range) => pick(key).getObject(key, range),
    presignGet: (key, ttl) => pick(key).presignGet(key, ttl),
    put: (key, body, opts) => pick(key).put(key, body, opts),
    delete: (key) => pick(key).delete(key),
    list: async (prefix, cursor) => {
      const scope = ledgerListScope(keyRoot, String(prefix));
      if (scope === "main") return main.list(prefix, cursor);
      if (scope === "ledger") {
        const page = await ledger.list(prefix, cursor);
        return { ...page, items: page.items.filter((i) => isLedger(k(i))) };
      }
      return merged(prefix, cursor);
    },
  } as ObjectStore;
}

/** The ledger-home marker in `store` (the ledger's own store, or a routed store), or null when absent. */
export async function readLedgerHome(store: ObjectStore, keyRoot: string): Promise<LedgerHomeDoc | null> {
  let raw: Uint8Array;
  try { raw = await readAll((await store.getObject(objectKey(ledgerHomeKey(keyRoot)))).body); } catch (error) {
    if (isObjectStoreError(error, "not_found")) return null;
    throw error;
  }
  let v: unknown;
  try { v = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(raw)); } catch { throw new Error("ledger_home_invalid"); }
  if (!validLedgerHome(v)) throw new Error("ledger_home_invalid");
  return v;
}

/**
 * The fork guard's decision for a process whose ledger lives in bucket `expect`. `required`: a separate ledger
 * bucket is configured, so a missing marker refuses too (the relocation writes it before the switch).
 */
export function ledgerHomeVerdict(doc: LedgerHomeDoc | null, expect: string, required: boolean): "ok" | "ledger_home_moved" | "ledger_home_missing" {
  if (doc === null) return required ? "ledger_home_missing" : "ok";
  return doc.home === expect ? "ok" : "ledger_home_moved";
}
