/**
 * GET /v1/sessions/:id/summary Bob Costas fallback (summary-fallback.ts): selection,
 * tenant / erased / refusal paths, decryption with a test keyring over real summary
 * objects (summary-objects.ts writer), and flag off = byte-identical responses.
 */
import { afterAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { manifestKey } from "../../../packages/object-store/src/index.ts";
import { masterKeyring, parseMasterKeys } from "../../shared/keyring.ts";
import { summaryObjectKey, writeSummaryObject, type SummaryKind, type SummaryRecord, type WorkspaceKeyring } from "../../bob-costas/summary-objects.ts";
import { AuthError, createReadApi, type VerifiedIdentity } from "../app.ts";
import { BriefSummaryError, type BriefSummary, type BriefSummaryRow } from "../brief-summary.ts";
import {
  SUMMARY_FALLBACK_MAX_CHARS,
  SUMMARY_POINTERS_SQL,
  SummaryFallbackError,
  pointerSummaryFallback,
  truncateOnWord,
  type SessionSummaryReady,
  type SummaryFallback,
  type SummaryPointerRow,
} from "../summary-fallback.ts";
import type { TenantRunner } from "../liveness.ts";
import { memoryLiveness } from "./support.ts";

const HEX_A = "a".repeat(64);
const HEX_B = "b".repeat(64);
const scope = { orgId: "org_1", workspaceId: "ws_1", source: "codex", sessionId: "sess_1" };
const loc = { prefix: "96bulls/org_1", workspaceId: "ws_1", machineId: "m1", harness: "codex", sessionId: "sess_1", startDate: "2026-11-01" };
const prefixEnv = { PIPELINE_KEY_PREFIX: "96bulls", PIPELINE_ORG_FROM_PREFIX: "true" };
const SECRET = "PLAINTEXT-SUMMARY-SENTINEL";
const record = (over: Partial<SummaryRecord> = {}): SummaryRecord => ({
  v: 1, kind: "long", revision: 4, status: "ready", summary: `# Session\n${SECRET} long text`, model: "gpt-5.6-luna",
  built_at: "2026-11-02T03:04:05.000Z", source_content_sha256: "c".repeat(64), trace_id: "01a188e1-ef86-7b16-938a-e91b6b78643b", ...over,
});
const pointer = (over: Partial<SummaryPointerRow> = {}): SummaryPointerRow => ({
  org_id: "org_1", workspace_id: "ws_1", harness: "codex", session_id: "sess_1", logical_session_id: "sess_1", machine_id: "m1",
  storage_provider: "s3", bucket: "bucket", manifest_key: String(manifestKey(loc)), revision: "4", content_sha256: "c".repeat(64), ...over,
});

const dirs: string[] = [];
afterAll(() => { for (const d of dirs) rmSync(d, { recursive: true, force: true }); });

async function fixture(opts: { rows?: SummaryPointerRow[]; keyring?: () => Promise<WorkspaceKeyring>; raw?: boolean } = {}) {
  const root = mkdtempSync(join(tmpdir(), "summary-fallback-"));
  dirs.push(root);
  const store = createFsStore({ root });
  const keyring = await masterKeyring(parseMasterKeys(`k1:${HEX_A}`));
  const calls: unknown[][] = [];
  const run: TenantRunner = async (org, ws, work) => work({
    async query<R>(text: string, params?: unknown[]) {
      calls.push([org, ws, text, params]);
      return { rows: (opts.rows ?? [pointer()]) as R[] };
    },
  });
  const fallback = pointerSummaryFallback({
    run,
    raw: () => (opts.raw === false ? undefined : { store, provider: "s3", bucket: "bucket" }),
    keyring: opts.keyring ?? (async () => keyring),
    prefix: prefixEnv,
  });
  const write = async (rec: SummaryRecord, at: { kind?: SummaryKind; ids?: { orgId: string; workspaceId: string }; ring?: WorkspaceKeyring; location?: typeof loc } = {}) => {
    const key = String(summaryObjectKey(at.location ?? loc, at.kind ?? rec.kind));
    await writeSummaryObject({ store, keyring: at.ring ?? keyring, ids: at.ids ?? { orgId: "org_1", workspaceId: "ws_1" }, key, record: rec, current: { revision: rec.revision, manifestSha256: "f".repeat(64) } });
    return key;
  };
  return { fallback, write, calls, store, keyring };
}

const reason = async (p: Promise<unknown>) => {
  try { await p; } catch (e) { expect(e).toBeInstanceOf(SummaryFallbackError); expect(String((e as Error).message)).not.toContain(SECRET); return (e as SummaryFallbackError).reason; }
  throw new Error("expected a refusal");
};

describe("pointerSummaryFallback: decryption with a test keyring", () => {
  test("serves the long and short summary at the live pointer's revision, recorded model and built_at", async () => {
    const f = await fixture();
    await f.write(record());
    await f.write(record({ kind: "short", summary: "Short recorded summary." }));
    expect(await f.fallback.read(scope, "long")).toEqual({ status: "ready", kind: "long", summary: record().summary!, model: "gpt-5.6-luna", generatedAt: "2026-11-02T03:04:05.000Z", revision: 4, truncated: false });
    expect(await f.fallback.read(scope, "short")).toMatchObject({ kind: "short", summary: "Short recorded summary.", revision: 4 });
    // Tenant-bound, bound parameters only, live rows only.
    expect(f.calls[0]!.slice(0, 2)).toEqual(["org_1", "ws_1"]);
    expect(f.calls[0]![3]).toEqual(["org_1", "ws_1", "codex", "sess_1"]);
    expect(SUMMARY_POINTERS_SQL).toContain("p.deleted_at IS NULL");
  });

  test("missing object, older revision, text-free record or no recorded model: nothing served", async () => {
    const f = await fixture();
    expect(await f.fallback.read(scope, "long")).toBeNull();
    await f.write(record({ revision: 3 }));
    expect(await f.fallback.read(scope, "long")).toBeNull(); // pointer is at 4
    const g = await fixture();
    await g.write(record({ status: "disabled", summary: undefined, model: undefined }));
    expect(await g.fallback.read(scope, "long")).toBeNull();
    const h = await fixture();
    await h.write(record({ model: undefined }));
    expect(await h.fallback.read(scope, "long")).toBeNull();
    const i = await fixture();
    await i.write(record({ model: "x".repeat(129) }));
    expect(await i.fallback.read(scope, "long")).toBeNull();
    // Same key, kind and revision, other raw content (a re-upload restarts revisions): not served.
    const j = await fixture();
    await j.write(record({ source_content_sha256: "d".repeat(64) }));
    expect(await j.fallback.read(scope, "long")).toBeNull();
    const k = await fixture();
    await k.write(record({ source_content_sha256: undefined }));
    expect(await k.fallback.read(scope, "long")).toBeNull();
  });

  test("an object under another workspace's key, or another kind's object at this key, never opens", async () => {
    const f = await fixture();
    await f.write(record(), { ids: { orgId: "org_1", workspaceId: "ws_other" } });
    expect(await f.fallback.read(scope, "long")).toBeNull();
    const g = await fixture();
    // A short record written at the long key: the AAD binds the kind.
    await g.write(record({ kind: "short" }), { kind: "long" });
    expect(await g.fallback.read(scope, "long")).toBeNull();
  });

  test("a key id this deployment does not hold is unavailable (503), not empty", async () => {
    const f = await fixture();
    await f.write(record(), { ring: await masterKeyring(parseMasterKeys(`k9:${HEX_B}`)) });
    expect(await reason(f.fallback.read(scope, "long"))).toBe("unavailable");
  });

  test("no live pointer (erased/tombstoned/unknown) or several physical rows: nothing served, no object read", async () => {
    for (const rows of [[], [pointer(), pointer({ machine_id: "m2", manifest_key: String(manifestKey({ ...loc, machineId: "m2" })) })]]) {
      const f = await fixture({ rows });
      await f.write(record());
      expect(await f.fallback.read(scope, "long")).toBeNull();
    }
  });

  test("tenant and pointer checks refuse before any object read", async () => {
    for (const row of [pointer({ org_id: "org_2" }), pointer({ workspace_id: "ws_2" })]) {
      const f = await fixture({ rows: [row] });
      expect(await reason(f.fallback.read(scope, "long"))).toBe("tenant_mismatch");
    }
    // A foreign row next to ours is a tenant failure too, not "several rows".
    expect(await reason((await fixture({ rows: [pointer(), pointer({ org_id: "org_2" })] })).fallback.read(scope, "long"))).toBe("tenant_mismatch");
    for (const row of [pointer({ harness: "claude" }), pointer({ logical_session_id: "sess_2" })])
      expect(await reason((await fixture({ rows: [row] })).fallback.read(scope, "long"))).toBe("tenant_mismatch");
    for (const row of [
      pointer({ bucket: "other" }),
      pointer({ storage_provider: "gcs" }),
      pointer({ revision: "0" }),
      pointer({ content_sha256: null }),
      pointer({ content_sha256: "C".repeat(64) }),
      pointer({ manifest_key: String(manifestKey({ ...loc, workspaceId: "ws_2" })) }),
      pointer({ manifest_key: String(manifestKey({ ...loc, machineId: "m2" })) }),
      pointer({ manifest_key: String(manifestKey({ ...loc, sessionId: "sess_2" })) }),
      pointer({ manifest_key: String(manifestKey({ ...loc, prefix: "96bulls/org_2" })) }),
      pointer({ manifest_key: `${String(summaryObjectKey(loc, "long"))}` }),
      pointer({ manifest_key: "not/a/session/key" }),
    ])
      expect(await reason((await fixture({ rows: [row] })).fallback.read(scope, "long"))).toBe("pointer_invalid");
  });

  test("not configured: no raw store, no or malformed key material", async () => {
    expect(await reason((await fixture({ raw: false })).fallback.read(scope, "long"))).toBe("not_configured");
    expect(await reason((await fixture({ keyring: () => Promise.reject(new Error("x")) })).fallback.read(scope, "long"))).toBe("not_configured");
  });

  test("short text over 320 code points is cut on a word boundary, flagged, never over the bound", async () => {
    const f = await fixture();
    const long = `${"word ".repeat(100)}end`;
    await f.write(record({ kind: "short", summary: long }));
    const got = (await f.fallback.read(scope, "short"))!;
    expect([...got.summary].length).toBeLessThanOrEqual(SUMMARY_FALLBACK_MAX_CHARS.short);
    expect(got.summary.endsWith("word…")).toBe(true);
    expect(got.truncated).toBe(true);
    expect(truncateOnWord("short", 320)).toEqual({ text: "short", truncated: false });
    const noSpace = truncateOnWord("😀".repeat(400), 320);
    expect([...noSpace.text].length).toBe(320);
  });
});

// Route level: the brief SQL first, then the fallback; flag off = byte-identical.
const id: VerifiedIdentity = { userId: "u", orgId: "o", workspaceId: "w", scopes: ["rth:read"] };
const brief: BriefSummaryRow = { org_id: "o", workspace_id: "w", harness: "codex", session_id: "s", description: "Actual model output.", model: "gpt-5.6-luna", generated_at: new Date("2026-11-09T00:00:00Z") };
const ready: SessionSummaryReady = { status: "ready", kind: "short", summary: "Bob summary.", model: "extractive/v1", generatedAt: "2026-11-10T00:00:00.000Z", revision: 7, truncated: false };
function api(opts: { rows?: BriefSummaryRow[] | null; briefError?: unknown; fallback?: false | SessionSummaryReady | null | Error } = {}) {
  const order: string[] = [];
  const briefSummary: BriefSummary = { async read() { order.push("brief"); if (opts.briefError) throw opts.briefError; return opts.rows === undefined ? [brief] : opts.rows; } };
  const summaryFallback: SummaryFallback = {
    async read(key, kind) {
      order.push(`fallback:${kind}:${key.orgId}/${key.workspaceId}/${key.source}/${key.sessionId}`);
      const v = opts.fallback;
      if (v instanceof Error) throw v;
      return v ? { ...v, kind } : null;
    },
  };
  const app = createReadApi({
    verify: async (t) => { if (t !== "good") throw new AuthError(401, "invalid_token", "invalid"); return id; },
    ch: { database: "x" } as never, liveness: memoryLiveness().liveness, completeSinceMs: 0, directoryWorkspaces: new Set(["w"]), briefSummary,
    ...(opts.fallback === false ? {} : { summaryFallback }),
  });
  return { order, get: (q = "kind=short", token = "good") => app.request(`/v1/sessions/s/summary?workspace=w&source=codex&${q}`, { headers: token ? { authorization: `Bearer ${token}` } : {} }) };
}

describe("GET /v1/sessions/:id/summary with the fallback", () => {
  test("flag off: brief, empty and kind=long are exactly as before (bytes)", async () => {
    expect(await (await api({ fallback: false }).get()).text()).toBe(JSON.stringify({ summary: { status: "ready", kind: "short", summary: brief.description, model: brief.model, builtAt: "2026-11-09T00:00:00.000Z", state: null } }));
    expect(await (await api({ fallback: false, rows: [] }).get()).text()).toBe(JSON.stringify({ summary: { status: "empty", kind: "short", state: null } }));
    const long = api({ fallback: false });
    const r = await long.get("kind=long");
    expect(r.status).toBe(501);
    expect(JSON.stringify(await r.json())).toContain("summary_kind");
    expect(long.order).toEqual([]);
  });

  test("flag on, brief exists: the brief, inner keys unchanged, provenance outside; no fallback read", async () => {
    const a = api({ fallback: ready });
    const body = await (await a.get()).json();
    expect(body).toEqual({
      summary: { status: "ready", kind: "short", summary: brief.description, model: brief.model, builtAt: "2026-11-09T00:00:00.000Z", state: null },
      provenance: { source: "session_brief", kind: "short", generatedAt: "2026-11-09T00:00:00.000Z", revision: null, truncated: false },
    });
    expect(a.order).toEqual(["brief"]);
  });

  test("flag on, no brief: the Bob Costas short summary in today's ready shape plus provenance", async () => {
    const a = api({ rows: [], fallback: ready });
    const body = await (await a.get()).json();
    expect(Object.keys(body.summary).sort()).toEqual(["builtAt", "kind", "model", "state", "status", "summary"]);
    expect(body).toEqual({
      summary: { status: "ready", kind: "short", summary: "Bob summary.", model: "extractive/v1", builtAt: "2026-11-10T00:00:00.000Z", state: null },
      provenance: { source: "session_summary", kind: "short", generatedAt: "2026-11-10T00:00:00.000Z", revision: 7, truncated: false },
    });
    expect(a.order).toEqual(["brief", "fallback:short:o/w/codex/s"]);
  });

  test("flag on, kind=long: the brief SQL runs first (its refusals hold), then the long summary", async () => {
    const a = api({ fallback: ready });
    const body = await (await a.get("kind=long")).json();
    expect(body.summary).toEqual({ status: "ready", kind: "long", summary: "Bob summary.", model: "extractive/v1", builtAt: "2026-11-10T00:00:00.000Z", state: null });
    expect(body.provenance).toMatchObject({ source: "session_summary", kind: "long", revision: 7 });
    expect(a.order).toEqual(["brief", "fallback:long:o/w/codex/s"]);
    for (const [o, status] of [[{ briefError: new BriefSummaryError("unqualified") }, 501], [{ briefError: new BriefSummaryError("refused") }, 503], [{ briefError: new BriefSummaryError("invalid_argument") }, 400], [{ rows: null }, 501]] as const) {
      const b = api({ ...o, fallback: ready });
      expect((await b.get("kind=long")).status).toBe(status);
      expect(b.order).toEqual(["brief"]);
    }
  });

  test("flag on, nothing servable: empty, unchanged inner shape, provenance null", async () => {
    expect(await (await api({ rows: [], fallback: null }).get()).text()).toBe(JSON.stringify({ summary: { status: "empty", kind: "short", state: null }, provenance: null }));
    expect(await (await api({ fallback: null }).get("kind=long")).json()).toEqual({ summary: { status: "empty", kind: "long", state: null }, provenance: null });
  });

  test("flag on: fallback refusals map to 500 / 501 / 503, never an empty 200", async () => {
    expect((await api({ rows: [], fallback: new SummaryFallbackError("tenant_mismatch") }).get()).status).toBe(500);
    const nc = await api({ rows: [], fallback: new SummaryFallbackError("not_configured") }).get();
    expect(nc.status).toBe(501);
    expect(JSON.stringify(await nc.json())).toContain("summary_fallback_not_configured");
    for (const r of ["pointer_invalid", "parse", "unavailable"] as const) expect((await api({ rows: [], fallback: new SummaryFallbackError(r) }).get()).status).toBe(503);
  });

  test("flag on: auth, unknown kinds and extra params refuse before any read", async () => {
    for (const [q, token, status] of [["kind=short", "bad", 401], ["kind=medium", "good", 501], ["kind=long&q=x", "good", 501], ["kind=long&kind=short", "good", 400]] as const) {
      const a = api({ fallback: ready });
      expect((await a.get(q, token)).status).toBe(status);
      expect(a.order).toEqual([]);
    }
  });
});
