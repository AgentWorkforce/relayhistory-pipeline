/** Small shared helpers for the replay harness (docs/session-intel-replay.md). No I/O beyond the atomic writer. */
import { createHash } from "node:crypto";
import { link, mkdir, rename, unlink, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";

export const sha256Hex = (data: string | Uint8Array) => createHash("sha256").update(data).digest("hex");

export const textCompare = (a: string, b: string) => a < b ? -1 : a > b ? 1 : 0;

/** JSON with object keys sorted at every level, so a hash does not depend on key order. */
export function canonicalJson(value: unknown): string {
  return JSON.stringify(value, (_k, v) => v && typeof v === "object" && !Array.isArray(v)
    ? Object.fromEntries(Object.keys(v as Record<string, unknown>).sort(textCompare).map((k) => [k, (v as Record<string, unknown>)[k]]))
    : v);
}

/** A session reference as the labels and alerts write it: `<harness>:<sessionId>` (a harness has no colon). */
export const refString = (r: { harness: string; sessionId: string }) => `${r.harness}:${r.sessionId}`;
export function parseRef(ref: string): { harness: string; sessionId: string } {
  const i = ref.indexOf(":");
  if (i <= 0 || i === ref.length - 1) throw new Error("a session ref is <harness>:<sessionId>");
  return { harness: ref.slice(0, i), sessionId: ref.slice(i + 1) };
}
/** Order-insensitive pair identity. */
export const pairId = (pair: readonly [string, string]) => [...pair].sort(textCompare).join("\u0001");
export const sortedPair = (a: string, b: string): [string, string] => textCompare(a, b) <= 0 ? [a, b] : [b, a];

const ISO = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d{1,9})?)?(?:Z|[+-]\d{2}:\d{2})$/;
/** A strict ISO-8601 timestamp with a zone, normalized to `toISOString()`; throws naming the field. */
export function isoTime(value: unknown, field: string): string {
  if (typeof value !== "string" || !ISO.test(value)) throw new Error(`${field} must be an ISO-8601 timestamp with a zone`);
  const ms = Date.parse(value);
  if (!Number.isFinite(ms)) throw new Error(`${field} must be a valid timestamp`);
  return new Date(ms).toISOString();
}

export const median = (values: number[]): number | null => {
  if (!values.length) return null;
  const s = [...values].sort((a, b) => a - b);
  const m = s.length >> 1;
  return s.length % 2 ? s[m]! : (s[m - 1]! + s[m]!) / 2;
};

/** Writes a file atomically (temp file in the same directory, then rename). */
export async function writeAtomic(path: string, data: string): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  const tmp = join(dirname(path), `.${Date.now()}-${process.pid}-${Math.random().toString(16).slice(2)}.tmp`);
  await writeFile(tmp, data, { mode: 0o600 });
  await rename(tmp, path);
}

/** Writes a new file and never replaces one: link(2) of a temp file fails with EEXIST when `path` exists. */
export async function writeNew(path: string, data: string): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  const tmp = join(dirname(path), `.${Date.now()}-${process.pid}-${Math.random().toString(16).slice(2)}.tmp`);
  await writeFile(tmp, data, { mode: 0o600 });
  try {
    await link(tmp, path);
  } catch (err) {
    if ((err as { code?: string }).code === "EEXIST") throw new Error(`${path} already exists; it is never overwritten`);
    throw err;
  } finally {
    await unlink(tmp).catch(() => {});
  }
}

/** A deterministic 32-bit PRNG (mulberry32) seeded from a string through sha256. */
export function seededRandom(seed: string): () => number {
  let a = Number.parseInt(sha256Hex(seed).slice(0, 8), 16) >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4_294_967_296;
  };
}

export function seededShuffle<T>(items: readonly T[], seed: string): T[] {
  const out = [...items];
  const rand = seededRandom(seed);
  for (let i = out.length - 1; i > 0; i--) {
    const j = Math.floor(rand() * (i + 1));
    [out[i], out[j]] = [out[j]!, out[i]!];
  }
  return out;
}

export const isRecord = (v: unknown): v is Record<string, unknown> => !!v && typeof v === "object" && !Array.isArray(v);

/** Refuses keys outside `allowed`, so a typo or an unexpected (possibly content-bearing) field never loads silently. */
export function onlyKeys(v: Record<string, unknown>, allowed: readonly string[], where: string): void {
  for (const k of Object.keys(v)) if (!allowed.includes(k)) throw new Error(`${where}: unknown field ${JSON.stringify(k.slice(0, 64))}`);
}

export function boundedString(v: unknown, field: string, max: number, opts: { nullable?: boolean; min?: number } = {}): string | null {
  if (v === null || v === undefined) {
    if (opts.nullable) return null;
    throw new Error(`${field} is required`);
  }
  if (typeof v !== "string") throw new Error(`${field} must be a string`);
  if (v.length < (opts.min ?? 1) || v.length > max) throw new Error(`${field} must be ${opts.min ?? 1}..${max} characters`);
  return v;
}
