/**
 * READ-ONLY pre-erasure verification of a stale session prefix against its date-moved survivor, the receipt it
 * produces, and the pure checks the receipt-fenced execute (job.ts `staleprefixPass`) runs on it.
 *
 * For one old prefix `{keyRoot}/{org}/v1/ws=../m=../h=../d=<OLD>/s=../`:
 *  1. EVERY version of every object under it (noncurrent versions and delete markers included) is listed with the
 *     eraser's strict inventory (eraser.ts `strictInventory`): exactly the set the eraser would delete.
 *  2. Every non-delete-marker raw chunk version is fetched BY VERSION ID, zstd-decompressed (bounded), checked
 *     against the sha256 prefix its key names, and split into ndjson records. Each record contributes its stable
 *     event identity (kind, origin_id, record_id, revision, operation) and the SHA-256 of its exact line bytes to a
 *     MULTISET (multiplicity preserved: two versions carrying the same record count twice).
 *  3. Every old manifest version is parsed (`parseManifest` against its own key); each chunk it references must be
 *     a compared chunk whose decompressed sha256 equals the manifest's entry, else `manifest_ref_unverified`.
 *  4. The survivor's CURRENT manifest version and the CURRENT version of every chunk it references are pinned by
 *     version id, fetched by those ids, checked against the manifest's sha256/bytes, and turned into the same
 *     multiset. The pins are re-listed afterwards; a pin no longer current is `survivor_unstable`.
 *  5. PASS only if old ⊆ survivor per (identity, digest) with old count <= survivor count. A source row that is
 *     missing from the survivor is never assumed intentionally deleted: it is `missing_identity`, a FAIL.
 *
 * Fail closed throughout: an object under the old prefix that is neither the manifest nor a raw ndjson chunk is
 * `unverified_object`; anything unreadable, undecodable or over a bound is `unreadable_version`.
 *
 * Receipts and logs carry hashes and counts only: sha256(key) + versionId, never a key, an org, a session or content.
 */
import { createHash } from "node:crypto";
import { zstdDecompressSync } from "node:zlib";
import { chunkKeys, parseManifest } from "../../shared/vendor.ts";
import { coded, type ListedVersion, type LifecycleRead, type VersioningRead, type VersionReader } from "./eraser.ts";
import { assertInventoryXml } from "./eraser.ts";
import { xmlElements, xmlText } from "../../../packages/object-store/src/xml.ts";

export const RECEIPT_KIND = "stale-prefix-verification-receipt";
export const ATTESTATION_KIND = "stale-prefix-protection-attestation";
export const MAX_MANIFEST_BYTES = 16 * 1024 * 1024;
export const MAX_CHUNK_STORED_BYTES = 64 * 1024 * 1024;
export const MAX_CHUNK_RAW_BYTES = 256 * 1024 * 1024;
/** An attestation's verified_at may be at most this old at an item's first delete. */
export const ATTESTATION_MAX_AGE_MS = 60 * 60 * 1000;

export type VerifyReason =
  | "missing_identity" | "digest_mismatch" | "multiplicity_short" | "unreadable_version" | "manifest_ref_unverified"
  | "unverified_object" | "inventory_failed" | "survivor_missing" | "survivor_ambiguous" | "survivor_unstable"
  | "no_stale_prefix_entry" | "identity_unresolved" | "shape_invalid";

export const sha256Hex = (v: string | Uint8Array) => createHash("sha256").update(v).digest("hex");

/** Canonical JSON (object keys sorted, recursively): every digest here is over this form. */
export function canonical(v: unknown): string {
  if (Array.isArray(v)) return `[${v.map(canonical).join(",")}]`;
  if (v && typeof v === "object")
    return `{${Object.keys(v as object).sort().map((k) => `${JSON.stringify(k)}:${canonical((v as Record<string, unknown>)[k])}`).join(",")}}`;
  return JSON.stringify(v);
}
const digest = (v: unknown) => sha256Hex(canonical(v));

// ------------------------------------------------------------------ records

/** identity -> (line digest -> count). */
export type RecordMultiset = Map<string, Map<string, number>>;

/**
 * The records of one decompressed ndjson chunk: every line terminated by `\n`, non-empty, strict UTF-8 JSON with
 * string kind/origin_id/record_id and a string or number revision. `operation` is carried when present (the
 * exporter's stream lines hold only upserts and omit it; absent is recorded as null, the same on both sides).
 * Throws `record_undecodable` on any violation.
 */
export function chunkRecords(raw: Uint8Array): { identity: string; digest: string }[] {
  const out: { identity: string; digest: string }[] = [];
  const dec = new TextDecoder("utf-8", { fatal: true });
  let start = 0;
  for (let i = 0; i < raw.length; i++) {
    if (raw[i] !== 0x0a) continue;
    const line = raw.subarray(start, i);
    start = i + 1;
    if (!line.length) throw coded("record_undecodable", "empty line");
    let r: Record<string, unknown>;
    try {
      const v: unknown = JSON.parse(dec.decode(line));
      if (!v || typeof v !== "object" || Array.isArray(v)) throw new Error("not an object");
      r = v as Record<string, unknown>;
    } catch { throw coded("record_undecodable", "line is not a JSON object"); }
    const str = (k: string) => typeof r[k] === "string";
    if (!str("kind") || !str("origin_id") || !str("record_id") || !(typeof r.revision === "string" || typeof r.revision === "number")
        || !(r.operation === undefined || typeof r.operation === "string"))
      throw coded("record_undecodable", "record identity fields");
    out.push({ identity: JSON.stringify([r.kind, r.origin_id, r.record_id, r.revision, r.operation ?? null]), digest: sha256Hex(line) });
  }
  if (start !== raw.length) throw coded("record_undecodable", "chunk does not end at a line boundary");
  return out;
}

function addRecords(m: RecordMultiset, recs: { identity: string; digest: string }[]): void {
  for (const { identity, digest: d } of recs) {
    const byDigest = m.get(identity) ?? m.set(identity, new Map()).get(identity)!;
    byDigest.set(d, (byDigest.get(d) ?? 0) + 1);
  }
}
const recordCount = (m: RecordMultiset) => { let n = 0; for (const b of m.values()) for (const c of b.values()) n += c; return n; };

/** Old ⊆ survivor, per (identity, digest) with multiplicity. Counts are in records. */
export function compareMultisets(old: RecordMultiset, survivor: RecordMultiset): Partial<Record<VerifyReason, number>> {
  const reasons: Partial<Record<VerifyReason, number>> = {};
  const add = (r: VerifyReason, n: number) => { if (n > 0) reasons[r] = (reasons[r] ?? 0) + n; };
  for (const [identity, oldDigests] of old) {
    const surv = survivor.get(identity);
    for (const [d, n] of oldDigests) {
      if (!surv) add("missing_identity", n);
      else if (!surv.has(d)) add("digest_mismatch", n);
      else add("multiplicity_short", n - surv.get(d)!);
    }
  }
  return reasons;
}

// ------------------------------------------------------------------ receipt shape

export interface VersionEntry { key_sha256: string; version_id: string; delete_marker: boolean }
export interface PinEntry { key_sha256: string; version_id: string }
export interface ItemReceipt {
  identity_hash: string;
  old_prefix_sha256: string;
  enqueued_at: number;
  status: "PASS" | "FAIL";
  reasons: Partial<Record<VerifyReason, number>>;
  old_versions: VersionEntry[];
  old_versions_sha256: string;
  survivor: { prefix_sha256: string; manifest: PinEntry; chunks: PinEntry[]; pin_sha256: string } | null;
  counts: { versions: number; delete_markers: number; manifest_versions: number; chunk_versions: number; other_versions: number; records_old: number; records_survivor: number };
}
export interface IdentityReceipt { identity_hash: string; status: "PASS" | "FAIL"; reasons: Partial<Record<VerifyReason, number>>; items: ItemReceipt[] }
export interface VerificationReceipt {
  v: 1;
  kind: typeof RECEIPT_KIND;
  bucket_sha256: string;
  key_root_sha256: string;
  /** The ORIGINAL allowlist, in the order given: never shrunk or filtered. */
  allowlist: string[];
  identities: IdentityReceipt[];
  status: "PASS" | "FAIL";
  receipt_sha256: string;
}

const byKeyVersion = (a: { key_sha256: string; version_id: string }, b: { key_sha256: string; version_id: string }) =>
  a.key_sha256 < b.key_sha256 ? -1 : a.key_sha256 > b.key_sha256 ? 1 : a.version_id < b.version_id ? -1 : a.version_id > b.version_id ? 1 : 0;

/** The exact version set of a listing, as the receipt records it (sorted, hashes only). */
export function versionSet(list: readonly ListedVersion[]): VersionEntry[] {
  return list.map((v) => ({ key_sha256: sha256Hex(v.key), version_id: v.versionId, delete_marker: v.deleteMarker })).sort(byKeyVersion);
}
export const versionSetDigest = (set: readonly VersionEntry[]) => digest(set);
export const pinDigest = (manifest: PinEntry, chunks: readonly PinEntry[]) => digest({ manifest, chunks });
export const sameVersionSet = (a: readonly VersionEntry[], b: readonly VersionEntry[]) => canonical(a) === canonical(b);

/** Seals a receipt: identity status from its items, overall status, and the digest over the canonical body. */
export function sealReceipt(body: Omit<VerificationReceipt, "receipt_sha256" | "status">): VerificationReceipt {
  const status: "PASS" | "FAIL" = body.identities.length > 0 && body.identities.every((i) => i.status === "PASS") ? "PASS" : "FAIL";
  const unsealed = { ...body, status };
  return { ...unsealed, receipt_sha256: digest(unsealed) };
}

const HEX64 = /^[0-9a-f]{64}$/;
const invalid = (why: string) => coded("stale_prefix_receipt_invalid", `verified receipt: ${why}`);

/**
 * A receipt as read back for execute: strict shape, every digest recomputed (receipt, each version set and each
 * survivor pin), and every PASS consistent with its reasons. Throws `stale_prefix_receipt_invalid`.
 */
export function parseReceipt(text: string): VerificationReceipt {
  let r: VerificationReceipt;
  try { r = JSON.parse(text) as VerificationReceipt; } catch { throw invalid("not JSON"); }
  if (!r || r.v !== 1 || r.kind !== RECEIPT_KIND || !HEX64.test(r.bucket_sha256) || !HEX64.test(r.key_root_sha256) || !Array.isArray(r.allowlist)
      || !Array.isArray(r.identities) || (r.status !== "PASS" && r.status !== "FAIL") || typeof r.receipt_sha256 !== "string")
    throw invalid("shape");
  const { receipt_sha256, ...unsealed } = r;
  if (digest(unsealed) !== receipt_sha256) throw invalid("receipt digest");
  for (const id of r.identities) {
    if (!id || !HEX64.test(id.identity_hash) || !Array.isArray(id.items) || (id.status !== "PASS" && id.status !== "FAIL")) throw invalid("identity shape");
    if (id.status === "PASS" && (Object.keys(id.reasons ?? {}).length || !id.items.length || id.items.some((i) => i.status !== "PASS"))) throw invalid("identity status");
    for (const it of id.items) {
      if (!it || it.identity_hash !== id.identity_hash || !HEX64.test(it.old_prefix_sha256) || !Number.isSafeInteger(it.enqueued_at) || !Array.isArray(it.old_versions))
        throw invalid("item shape");
      for (const v of it.old_versions)
        if (!v || !HEX64.test(v.key_sha256) || typeof v.version_id !== "string" || !v.version_id || typeof v.delete_marker !== "boolean") throw invalid("version entry");
      if (versionSetDigest(it.old_versions) !== it.old_versions_sha256 || !sameVersionSet([...it.old_versions].sort(byKeyVersion), it.old_versions)) throw invalid("version set digest");
      if (it.status === "PASS") {
        if (Object.keys(it.reasons ?? {}).length || !it.survivor) throw invalid("item status");
        const s = it.survivor;
        if (!HEX64.test(s.prefix_sha256) || !s.manifest || !HEX64.test(s.manifest.key_sha256) || !s.manifest.version_id || !Array.isArray(s.chunks)
            || s.chunks.some((c) => !c || !HEX64.test(c.key_sha256) || !c.version_id) || pinDigest(s.manifest, s.chunks) !== s.pin_sha256)
          throw invalid("survivor pin");
      }
    }
  }
  return r;
}

// ------------------------------------------------------------------ verification of one item

const CHUNK_NAME = /^raw\/(\d{10})-(\d+)-([0-9a-f]{8})\.ndjson\.zst$/;
const MANIFEST = "manifest.json";

function decompress(stored: Uint8Array): Uint8Array {
  if (typeof zstdDecompressSync !== "function") throw coded("zstd_unavailable", "this runtime cannot zstd-decompress");
  try { return new Uint8Array(zstdDecompressSync(stored, { maxOutputLength: MAX_CHUNK_RAW_BYTES })); }
  catch { throw coded("chunk_undecodable", "zstd"); }
}

/** The org root (`{keyRoot}/{org}`) a v1 session prefix sits under, for `chunkKeys`'s prefix. */
const orgRootOf = (sessionPrefix: string) => sessionPrefix.slice(0, sessionPrefix.indexOf("/v1/"));

/**
 * Verifies one queued old prefix against its survivor candidates (manifest keys of the same identity at another
 * date, from the exporter's state). `log` receives counts and hashes only.
 */
export async function verifyStalePrefix(reader: VersionReader, input: { identityHash: string; oldPrefix: string; enqueuedAt: number; survivorManifestKeys: readonly string[] }): Promise<ItemReceipt> {
  const { oldPrefix } = input;
  const reasons: Partial<Record<VerifyReason, number>> = {};
  const fail = (r: VerifyReason, n = 1) => { reasons[r] = (reasons[r] ?? 0) + n; };
  const counts = { versions: 0, delete_markers: 0, manifest_versions: 0, chunk_versions: 0, other_versions: 0, records_old: 0, records_survivor: 0 };
  const finish = (oldList: readonly ListedVersion[], survivor: ItemReceipt["survivor"]): ItemReceipt => {
    const old_versions = versionSet(oldList);
    return {
      identity_hash: input.identityHash, old_prefix_sha256: sha256Hex(oldPrefix), enqueued_at: input.enqueuedAt,
      status: Object.keys(reasons).length ? "FAIL" : "PASS", reasons, old_versions, old_versions_sha256: versionSetDigest(old_versions), survivor, counts,
    };
  };
  let oldList: ListedVersion[];
  try { oldList = await reader.listVersions(oldPrefix); } catch { fail("inventory_failed"); return finish([], null); }
  counts.versions = oldList.length;
  const oldSet: RecordMultiset = new Map();
  // key -> full sha256 of every successfully decompressed version of it.
  const compared = new Map<string, Set<string>>();
  const manifests: ListedVersion[] = [];
  for (const v of oldList) {
    if (v.deleteMarker) { counts.delete_markers++; continue; }
    const rest = v.key.slice(oldPrefix.length);
    if (rest === MANIFEST) { counts.manifest_versions++; manifests.push(v); continue; }
    const m = CHUNK_NAME.exec(rest);
    if (!m) { counts.other_versions++; fail("unverified_object"); continue; }
    counts.chunk_versions++;
    try {
      const raw = decompress(await reader.getVersion(v.key, v.versionId, MAX_CHUNK_STORED_BYTES));
      const full = sha256Hex(raw);
      if (full.slice(0, 8) !== m[3]) throw coded("chunk_digest", "chunk content does not match its key");
      addRecords(oldSet, chunkRecords(raw));
      (compared.get(v.key) ?? compared.set(v.key, new Set()).get(v.key)!).add(full);
    } catch { fail("unreadable_version"); }
  }
  for (const v of manifests) {
    try {
      const text = new TextDecoder("utf-8", { fatal: true }).decode(await reader.getVersion(v.key, v.versionId, MAX_MANIFEST_BYTES));
      const manifest = await parseManifest(text, v.key);
      for (const { entry, key } of chunkKeys(manifest, { prefix: orgRootOf(oldPrefix) }))
        if (!String(key).startsWith(oldPrefix) || !compared.get(String(key))?.has(entry.sha256)) fail("manifest_ref_unverified");
    } catch { fail("unreadable_version"); }
  }
  counts.records_old = recordCount(oldSet);

  // Survivor: exactly one candidate whose manifest is CURRENT (latest, not a delete marker).
  const live: { key: string; prefix: string; listing: ListedVersion[]; manifest: ListedVersion }[] = [];
  for (const key of new Set(input.survivorManifestKeys)) {
    if (!key.endsWith(`/${MANIFEST}`)) continue;
    const prefix = key.slice(0, -MANIFEST.length);
    try {
      const listing = await reader.listVersions(prefix);
      const cur = listing.filter((x) => x.key === key && x.latest);
      if (cur.length === 1 && !cur[0]!.deleteMarker) live.push({ key, prefix, listing, manifest: cur[0]! });
    } catch { fail("inventory_failed"); }
  }
  if (live.length !== 1) { fail(live.length ? "survivor_ambiguous" : "survivor_missing"); return finish(oldList, null); }
  const s = live[0]!;
  const survSet: RecordMultiset = new Map();
  const chunkPins: PinEntry[] = [];
  const pinned: { key: string; versionId: string }[] = [{ key: s.key, versionId: s.manifest.versionId }];
  try {
    const text = new TextDecoder("utf-8", { fatal: true }).decode(await reader.getVersion(s.key, s.manifest.versionId, MAX_MANIFEST_BYTES));
    const manifest = await parseManifest(text, s.key);
    for (const { entry, key: k } of chunkKeys(manifest, { prefix: orgRootOf(s.prefix) })) {
      const key = String(k);
      const cur = s.listing.filter((x) => x.key === key && x.latest);
      if (!key.startsWith(s.prefix) || cur.length !== 1 || cur[0]!.deleteMarker) { fail("unreadable_version"); continue; }
      const versionId = cur[0]!.versionId;
      chunkPins.push({ key_sha256: sha256Hex(key), version_id: versionId });
      pinned.push({ key, versionId });
      try {
        const raw = decompress(await reader.getVersion(key, versionId, MAX_CHUNK_STORED_BYTES));
        if (raw.byteLength !== entry.bytes || sha256Hex(raw) !== entry.sha256) throw coded("chunk_digest", "survivor chunk does not match its manifest");
        addRecords(survSet, chunkRecords(raw));
      } catch { fail("unreadable_version"); }
    }
  } catch { fail("unreadable_version"); }
  // The pins must still be current after the reads.
  try {
    const after = await reader.listVersions(s.prefix);
    if (!pinsCurrent(after, pinned)) fail("survivor_unstable");
  } catch { fail("inventory_failed"); }
  counts.records_survivor = recordCount(survSet);
  for (const [r, n] of Object.entries(compareMultisets(oldSet, survSet))) fail(r as VerifyReason, n);
  chunkPins.sort(byKeyVersion);
  const manifestPin = { key_sha256: sha256Hex(s.key), version_id: s.manifest.versionId };
  return finish(oldList, { prefix_sha256: sha256Hex(s.prefix), manifest: manifestPin, chunks: chunkPins, pin_sha256: pinDigest(manifestPin, chunkPins) });
}

/** Every pinned key's CURRENT version is exactly the pinned version id and not a delete marker. */
function pinsCurrent(listing: readonly ListedVersion[], pins: readonly { key: string; versionId: string }[]): boolean {
  return pins.every((p) => {
    const cur = listing.filter((x) => x.key === p.key && x.latest);
    return cur.length === 1 && !cur[0]!.deleteMarker && cur[0]!.versionId === p.versionId;
  });
}

/**
 * The fence's survivor check over a fresh strict listing of the survivor prefix, by key hash (the receipt holds no
 * raw key). A pin that is absent, superseded (a newer version is current: it became NONCURRENT, which the bucket's
 * lifecycle expires after about a day), or covered by a delete marker is drift.
 */
export function survivorPinsHold(listing: readonly ListedVersion[], survivor: NonNullable<ItemReceipt["survivor"]>): boolean {
  const latest = new Map<string, ListedVersion[]>();
  for (const v of listing) if (v.latest) { const h = sha256Hex(v.key); (latest.get(h) ?? latest.set(h, []).get(h)!).push(v); }
  return [survivor.manifest, ...survivor.chunks].every((p) => {
    const cur = latest.get(p.key_sha256);
    return !!cur && cur.length === 1 && !cur[0]!.deleteMarker && cur[0]!.versionId === p.version_id;
  });
}

// ------------------------------------------------------------------ protection preflight

/**
 * Lifecycle facts for the key root, from GetBucketLifecycleConfiguration (fail closed): a rule that is Enabled and
 * whose filter could match any key under `{keyRoot}/` (a prefix overlapping it; a tag or size filter without a
 * prefix is treated as matching) must not expire or transition CURRENT versions. `Expiration` with Days or Date is a
 * violation; `Expiration` with only `ExpiredObjectDeleteMarker` is allowed but noted (it removes a delete marker that
 * is the only version left, never a current object version); any `Transition` is a violation (availability). The
 * noncurrent actions are allowed: every survivor pin must be CURRENT at every per-delete check, so a pin made
 * noncurrent stops the run before lifecycle could expire it. An unknown action or status is a violation.
 */
export function evaluateLifecycle(read: LifecycleRead, keyRoot: string): { proven: boolean; violations: string[]; notes: string[] } {
  if (read.status === "unreadable") return { proven: false, violations: [`lifecycle_unreadable:${read.code}`], notes: [] };
  if (read.status === "none") return { proven: true, violations: [], notes: ["no_lifecycle_configuration"] };
  const violations: string[] = [], notes: string[] = [];
  try { assertInventoryXml(read.xml, "LifecycleConfiguration"); } catch { return { proven: false, violations: ["lifecycle_malformed"], notes }; }
  const root = `${keyRoot}/`;
  const KNOWN = new Set(["ID", "Status", "Filter", "Prefix", "Expiration", "Transition", "NoncurrentVersionExpiration", "NoncurrentVersionTransition", "AbortIncompleteMultipartUpload"]);
  for (const rule of xmlElements(read.xml, "Rule")) {
    const children = topLevelTags(rule);
    const status = xmlText(rule, "Status");
    if (status === "Disabled") continue;
    if (status !== "Enabled") { violations.push("lifecycle_rule_status_unknown"); continue; }
    const filter = children.includes("Filter") ? xmlElements(rule, "Filter")[0] ?? "" : undefined;
    const prefix = filter !== undefined ? xmlText(filter, "Prefix") ?? "" : children.includes("Prefix") ? xmlText(rule, "Prefix") ?? "" : "";
    if (!(root.startsWith(prefix) || prefix.startsWith(root))) continue;
    for (const c of children) {
      if (!KNOWN.has(c)) violations.push("lifecycle_unknown_action");
      if (c === "Transition") violations.push("lifecycle_current_transition");
    }
    for (const exp of xmlElements(rule, "Expiration")) {
      if (/<Days>|<Date>/.test(exp)) violations.push("lifecycle_current_expiration");
      else if (xmlText(exp, "ExpiredObjectDeleteMarker") === "true") notes.push("expired_object_delete_marker");
      else violations.push("lifecycle_unknown_action");
    }
    for (const n of xmlElements(rule, "NoncurrentVersionExpiration")) notes.push(`noncurrent_version_expiration_days:${xmlText(n, "NoncurrentDays") ?? "?"}`);
  }
  return { proven: violations.length === 0, violations: [...new Set(violations)], notes: [...new Set(notes)] };
}

function topLevelTags(xml: string): string[] {
  const out: string[] = []; let depth = 0;
  for (const m of xml.matchAll(/<(\/?)([A-Za-z_][\w:.-]*)[^>]*?(\/?)>/g)) {
    if (m[1]) { depth--; continue; }
    if (depth === 0) out.push(m[2]!);
    if (!m[3]) depth++;
  }
  return out;
}

/**
 * The operator attestation (a file infra signs off; see the receipt fence in job.ts). It is REQUIRED for execute,
 * because some protections cannot be read by this code at all:
 *  - `survivor_writers`: the principals that can PutObject (overwrite a survivor key), DeleteObject without a
 *    versionId (a delete marker) and DeleteObjectVersion under the key root, attested by infra;
 *  - `lifecycle`: no current-version Expiration or Transition for the key root, and the NoncurrentVersionExpiration
 *    days (it must agree with the configuration whenever the code could read it);
 *  - `lifecycle_change`: the principals that can PutBucketLifecycleConfiguration / DeleteBucketLifecycle. The
 *    preflight passes ONLY if every such principal is inside the explicit change-controlled `break_glass` set AND
 *    the change is PREVENTED (`prevented: true` with a named `preventive_control`) for the erase interval. An alarm
 *    or change-control reference is detection, not protection: it is recorded as informational only and never
 *    satisfies the check;
 *  - `versioning`: "Enabled";
 *  - `verified_at` (at most ATTESTATION_MAX_AGE_MS old at each item's first delete) and `change_control_ref`.
 */
export interface ProtectionAttestation {
  v: 1;
  kind: typeof ATTESTATION_KIND;
  bucket_sha256: string;
  key_root: string;
  versioning: "Enabled";
  survivor_writers: { put_object: string[]; delete_marker: string[]; delete_version: string[]; attested_by: string };
  lifecycle: { current_version_expiration: false; current_version_transition: false; noncurrent_version_expiration_days: number };
  lifecycle_change: { principals: string[]; break_glass: string[]; prevented: true; preventive_control: string };
  verified_at: string;
  change_control_ref: string;
  alarm_ref?: string;
}

const strList = (v: unknown) => Array.isArray(v) && v.length > 0 && v.every((x) => typeof x === "string" && x.length > 0);
const nonEmpty = (v: unknown) => typeof v === "string" && v.trim().length > 0;

/** Strict shape and fact check; returns the refusal detail (never the file's content) or undefined when it holds. */
export function attestationProblem(a: unknown, expect: { bucketSha256: string; keyRoot: string }): string | undefined {
  const x = a as ProtectionAttestation;
  if (!x || typeof x !== "object" || x.v !== 1 || x.kind !== ATTESTATION_KIND) return "attestation_shape";
  if (x.bucket_sha256 !== expect.bucketSha256 || x.key_root !== expect.keyRoot) return "attestation_scope";
  if (x.versioning !== "Enabled") return "attestation_versioning";
  const w = x.survivor_writers;
  if (!w || !strList(w.put_object) || !strList(w.delete_marker) || !strList(w.delete_version) || !nonEmpty(w.attested_by)) return "attestation_survivor_writers";
  const l = x.lifecycle;
  if (!l || l.current_version_expiration !== false || l.current_version_transition !== false || !Number.isSafeInteger(l.noncurrent_version_expiration_days) || l.noncurrent_version_expiration_days < 1)
    return "attestation_lifecycle";
  const c = x.lifecycle_change;
  if (!c || !strList(c.principals) || !Array.isArray(c.break_glass) || !c.break_glass.every((p) => typeof p === "string" && p.length > 0)) return "attestation_lifecycle_change";
  // Prevented, not alarmed: a change-capable principal outside the break-glass set, or no preventive control, refuses.
  if (c.principals.some((p) => !c.break_glass.includes(p)) || c.prevented !== true || !nonEmpty(c.preventive_control)) return "attestation_lifecycle_change_not_prevented";
  if (typeof x.verified_at !== "string" || !Number.isFinite(Date.parse(x.verified_at))) return "attestation_verified_at";
  if (!nonEmpty(x.change_control_ref)) return "attestation_change_control_ref";
  return undefined;
}

/** Fresh enough at `now` (checked at each item's first delete). */
export function attestationFresh(a: ProtectionAttestation, now: number, maxAgeMs = ATTESTATION_MAX_AGE_MS): boolean {
  const at = Date.parse(a.verified_at);
  return Number.isFinite(at) && at <= now + 60_000 && now - at <= maxAgeMs;
}

/** The code-read facts must agree with the attestation where the code could read them. */
export function protectionFacts(lifecycle: LifecycleRead, versioning: VersioningRead, keyRoot: string, attestation: ProtectionAttestation | undefined):
  { proven: boolean; problems: string[]; notes: string[]; attestationUsed: boolean } {
  const problems: string[] = [], notes: string[] = [];
  if (!attestation) problems.push("attestation_missing");
  const lc = evaluateLifecycle(lifecycle, keyRoot);
  notes.push(...lc.notes);
  if (lifecycle.status === "unreadable") notes.push("lifecycle_from_attestation");
  else problems.push(...lc.violations);
  if (versioning.status === "unreadable") notes.push("versioning_from_attestation");
  else if (versioning.status !== "Enabled") problems.push("versioning_not_enabled");
  if (attestation?.alarm_ref) notes.push("alarm_ref_informational_only");
  return { proven: problems.length === 0, problems, notes, attestationUsed: !!attestation };
}
