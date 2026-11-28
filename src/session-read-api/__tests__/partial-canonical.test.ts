/**
 * The hosted Lambda's partial-session canonical fallback (partial-canonical.ts): startup parsing,
 * the Lambda source over the REAL canonicalSnapshot (synthetic objects in an fs store standing in
 * for S3, pointer rows from a fake tenant-bound runner), and the refusal classifier. The route is
 * covered by partial-canonical-events.test.ts.
 */
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdCompressSync } from "node:zlib";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { sha256Hex } from "../../../packages/object-store/src/crypto.ts";
import { ObjectStoreError } from "../../../packages/object-store/src/errors.ts";
import { computeContentSha256, manifestKey, objectKey, rawChunkKey, type ChunkEntry, type ManifestV1, type ObjectStore } from "../../../packages/object-store/src/index.ts";
import { PipelineError } from "../../../packages/pipeline/src/lib/pipeline/errors.ts";
import { CanonicalReadError } from "../canonical.ts";
import {
  PARTIAL_CANONICAL_DEADLINE_MS_DEFAULT, READ_API_PARTIAL_CANONICAL_EVENTS, classifyPartialCanonical,
  partialCanonicalConfig, partialCanonicalDeadlineMs, partialCanonicalDeps, partialCanonicalFlag,
} from "../partial-canonical.ts";
import type { TenantRunner } from "../liveness.ts";
import { SESSION_POINTERS_SQL } from "../session-timing-source.ts";

const ON = {
  READ_API_PARTIAL_CANONICAL_EVENTS: "true",
  READ_API_PARTIAL_CANONICAL_BUCKET: "synthetic-canonical",
  READ_API_PARTIAL_CANONICAL_WORKSPACES: "ws_1",
  AWS_REGION: "us-east-1",
  PIPELINE_ORG_FROM_PREFIX: "true",
  PIPELINE_KEY_PREFIX: "96bulls",
};
const message = (f: () => unknown) => { try { f(); } catch (e) { return (e as Error).message; } return ""; };

describe("partialCanonicalFlag / partialCanonicalConfig", () => {
  test("unset, empty or false is off and reads nothing else (garbage elsewhere is ignored)", () => {
    for (const raw of [undefined, "", "false"]) {
      expect(partialCanonicalFlag(raw)).toBe(false);
      expect(partialCanonicalConfig({ READ_API_PARTIAL_CANONICAL_EVENTS: raw, READ_API_PARTIAL_CANONICAL_BUCKET: "../NOT A BUCKET", READ_API_PARTIAL_CANONICAL_DEADLINE_MS: "junk" }, "service")).toBeUndefined();
    }
    expect(partialCanonicalDeps({}, "lambda", async () => { throw Error("no SQL when off"); })).toBeUndefined();
  });

  test.each(["TRUE", "True", "1", "yes", " true", "true ", "on"])("flag %j fails startup naming the variable, never the value", (raw) => {
    expect(message(() => partialCanonicalFlag(raw))).toBe(`session read api: ${READ_API_PARTIAL_CANONICAL_EVENTS} must be unset, "", "true" or "false"`);
  });

  test("on: the validated config, with the hosted defaults", () => {
    expect(partialCanonicalConfig(ON, "lambda")).toEqual({
      bucket: "synthetic-canonical", region: "us-east-1", keyPrefix: "96bulls",
      workspaces: new Set(["ws_1"]), deadlineMs: PARTIAL_CANONICAL_DEADLINE_MS_DEFAULT, originLabels: false, readerMaxRuleset: undefined,
    });
    expect(partialCanonicalConfig({ ...ON, READ_API_PARTIAL_CANONICAL_WORKSPACES: "*", MESSAGE_ORIGIN_LABELS_ENABLED: "true", LOCKER_PROVIDER: "s3" }, "lambda"))
      .toMatchObject({ workspaces: "*", originLabels: true });
    // On with no workspace: serves nothing (the canary step before naming one).
    expect(partialCanonicalConfig({ ...ON, READ_API_PARTIAL_CANONICAL_WORKSPACES: "" }, "lambda")!.workspaces).toEqual(new Set());
  });

  test.each([
    ["the self-host service entry", {}, "service", "is served only by the hosted Lambda"],
    ["no bucket", { READ_API_PARTIAL_CANONICAL_BUCKET: undefined }, "lambda", "needs READ_API_PARTIAL_CANONICAL_BUCKET"],
    ["a blank bucket", { READ_API_PARTIAL_CANONICAL_BUCKET: "  " }, "lambda", "needs READ_API_PARTIAL_CANONICAL_BUCKET"],
    ["a malformed bucket", { READ_API_PARTIAL_CANONICAL_BUCKET: "Bad_Bucket/../x" }, "lambda", "needs READ_API_PARTIAL_CANONICAL_BUCKET"],
    ["no region", { AWS_REGION: undefined }, "lambda", "needs AWS_REGION"],
    ["the static key layout", { PIPELINE_ORG_FROM_PREFIX: undefined }, "lambda", "needs PIPELINE_ORG_FROM_PREFIX=true"],
    ["org prefix not exactly true", { PIPELINE_ORG_FROM_PREFIX: "TRUE" }, "lambda", "needs PIPELINE_ORG_FROM_PREFIX=true"],
    ["no key prefix", { PIPELINE_KEY_PREFIX: undefined }, "lambda", "needs PIPELINE_KEY_PREFIX"],
    ["a traversing key prefix", { PIPELINE_KEY_PREFIX: "96bulls/../other" }, "lambda", "needs PIPELINE_KEY_PREFIX"],
    ["a slash-wrapped key prefix", { PIPELINE_KEY_PREFIX: "/96bulls/" }, "lambda", "needs PIPELINE_KEY_PREFIX"],
    ["another provider", { LOCKER_PROVIDER: "gcs" }, "lambda", "reads AWS S3 only"],
  ] as const)("refuses %s at startup", (_name, over, entry, want) => {
    const got = message(() => partialCanonicalConfig({ ...ON, ...over }, entry as "lambda" | "service"));
    expect(got).toContain(`${READ_API_PARTIAL_CANONICAL_EVENTS}=true ${want}`);
    for (const v of Object.values(over)) if (v && v.trim()) expect(got.includes(v) && !want.includes(v)).toBe(false);
  });

  test("deadline: 1000..3500 whole ms, unset/empty the default, never clamped", () => {
    expect(partialCanonicalDeadlineMs(undefined)).toBe(3000);
    expect(partialCanonicalDeadlineMs("")).toBe(3000);
    expect(partialCanonicalDeadlineMs("1000")).toBe(1000);
    expect(partialCanonicalDeadlineMs("3500")).toBe(3500);
    for (const raw of ["999", "3501", "4000", "0", "-1", "3000.0", " 3000", "3e3", "abc", "30000"])
      expect(message(() => partialCanonicalDeadlineMs(raw))).toBe("session read api: READ_API_PARTIAL_CANONICAL_DEADLINE_MS must be a whole number of milliseconds in 1000..3500");
  });
});

// The Lambda source over the real canonicalSnapshot.

const AT = "2026-11-01T10:00:00.000Z";
const identity = { orgId: "org_1", workspaceId: "ws_1", source: "claude", sessionId: "sess:1" };
const location = { prefix: "96bulls/org_1", workspaceId: "ws_1", machineId: "srv_bb", harness: "claude", sessionId: "sess_1", startDate: "2026-11-01" };
let root: string, store: ObjectStore, manifest: ManifestV1, chunkKey: string;
let pointerRow: Record<string, unknown>;
const sqlCalls: Array<{ org: string; ws: string; text: string; params: unknown[] }> = [];

beforeEach(async () => {
  sqlCalls.length = 0;
  root = await mkdtemp(join(tmpdir(), "partial-canonical-"));
  store = createFsStore({ root });
  const lines = ["first synthetic line", "second synthetic line"].map((text, i) => ({
    kind: "session_event", origin_id: "origin", record_id: `r${i + 1}`, revision: "1", operation: "upsert", received_at: AT,
    payload: { ts_ms: Date.parse(AT) + i, text, role: "assistant", message_id: `msg-${i + 1}` },
  }));
  const raw = new TextEncoder().encode(lines.map((r) => JSON.stringify(r)).join("\n") + "\n");
  const stored = new Uint8Array(zstdCompressSync(raw));
  const sha = await sha256Hex(raw);
  const chunk: ChunkEntry = {
    seq: 0, epoch: 0, start_offset: 0, bytes: raw.byteLength, stored_bytes: stored.byteLength, sha256: sha, sha256_8: sha.slice(0, 8),
    stored_sha256: await sha256Hex(stored), stored_raw_bytes: raw.byteLength, raw_sha256: sha, compression: "zstd", records: 2,
  };
  manifest = {
    schema_v: 1, layout: "v1", trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18", revision: 1, created_at: AT, updated_at: AT,
    session: { workspace_id: "ws_1", machine_id: "srv_bb", harness: "claude", session_id: "sess_1", started_at: AT },
    writer: { delivery_instance_id: "srv_bb", plugin_version: "test" },
    raw: {
      owner: "srv_bb", state: "open", updated_at: AT, format: "ndjson", compression: "zstd", chunk_count: 1,
      raw_bytes: raw.byteLength, stored_bytes: stored.byteLength, content_sha256: await computeContentSha256([chunk]), epoch: 0,
      epochs: [{ epoch: 0, opened_at: AT, reason: "initial", first_seq: 0, opened_by: "srv_bb" }],
      next_offset: raw.byteLength, chunks: [chunk], scrub: { ruleset: "relayhistory-scrub", version: 1 },
    },
    extensions: { "96bulls": { user_id: "user_1", canonical_session_id: "sess:1" } },
  } as ManifestV1;
  chunkKey = rawChunkKey(location, 0, 0, sha, "ndjson");
  await store.put(chunkKey, stored, {});
  await store.put(objectKey(manifestKey(location)), new TextEncoder().encode(JSON.stringify(manifest)), {});
  pointerRow = {
    org_id: "org_1", workspace_id: "ws_1", harness: "claude", session_id: "sess:1", machine_id: "srv_bb", user_id: "user_1",
    storage_provider: "s3", bucket: "synthetic-canonical", manifest_key: manifestKey(location),
    // bytea through pg arrives as a Buffer (session-timing-source.ts hexDigest).
    content_sha256: Buffer.from(manifest.raw.content_sha256, "hex"), raw_format: "ndjson", revision: 1, deleted_at: null,
  };
});
afterEach(async () => { await rm(root, { recursive: true, force: true }); });

/** A fake tenant-bound runner: records the tenant it was bound to and answers the pointer SQL only. */
function runner(rows: () => Array<Record<string, unknown>>): TenantRunner {
  return (async (org: string, ws: string, work: (q: { query: (text: string, params?: unknown[]) => Promise<{ rows: unknown[] }> }) => Promise<unknown>) =>
    work({
      async query(text: string, params: unknown[] = []) {
        sqlCalls.push({ org, ws, text, params });
        if (text !== SESSION_POINTERS_SQL) throw Error("unexpected SQL");
        // The read role's RLS: only the bound tenant's rows.
        return { rows: rows().filter((r) => r.org_id === org && r.workspace_id === ws) };
      },
    })) as unknown as TenantRunner;
}

function lambdaSource(over: { rows?: () => Array<Record<string, unknown>>; store?: ObjectStore; env?: Record<string, string> } = {}) {
  const made: Array<Record<string, unknown>> = [];
  const deps = partialCanonicalDeps({ ...ON, ...over.env }, "lambda", runner(over.rows ?? (() => [pointerRow])), {
    credentials: async () => ({ accessKeyId: "AKIASYNTHETIC", secretAccessKey: "synthetic" }),
    makeStore: ((config: Record<string, unknown>) => { made.push(config); return over.store ?? store; }) as never,
  })!;
  return { deps, made, read: (id = identity) => deps.events.source(id, new AbortController().signal) };
}
const refusal = async (p: Promise<unknown>) => { try { await p; } catch (e) { return classifyPartialCanonical(e); } throw Error("expected a refusal"); };

describe("partialCanonicalDeps: the Lambda source over canonicalSnapshot", () => {
  test("serves the verified rows through the tenant-bound pointer query, with the hosted store settings", async () => {
    const { deps, made, read } = lambdaSource();
    const got = await read();
    expect(got.exists).toBe(true);
    expect(got.generation).toMatch(/^[a-f0-9]{64}$/);
    expect(got.rows.filter((r) => r.table === "convergence_events").map((r) => r.row.content)).toEqual(["first synthetic line", "second synthetic line"]);
    // Pointers read twice (before and after the object IO), each bound to the caller's tenant.
    expect(sqlCalls.map((c) => [c.org, c.ws, c.params])).toEqual([
      ["org_1", "ws_1", ["org_1", "ws_1", "claude", "sess:1"]],
      ["org_1", "ws_1", ["org_1", "ws_1", "claude", "sess:1"]],
    ]);
    expect(made[0]).toMatchObject({ bucket: "synthetic-canonical", region: "us-east-1" });
    expect(typeof made[0]!.credentials).toBe("function");
    expect(deps.workspaces).toEqual(new Set(["ws_1"]));
    expect(deps.deadlineMs).toBe(3000);
    expect("deadlineMs" in deps.events).toBe(false);
  });

  test("another tenant's identity sees no pointer and reads nothing", async () => {
    let touched = false;
    const spy = new Proxy(store, { get(t, k) { if (k === "getObject") touched = true; return Reflect.get(t, k); } });
    const got = await lambdaSource({ store: spy }).read({ ...identity, orgId: "org_2" });
    expect([got.exists, got.rows.length, touched]).toEqual([false, 0, false]);
  });

  test.each([
    ["another bucket", { bucket: "other-bucket" }, "pointer_invalid"],
    ["another provider", { storage_provider: "gcs" }, "pointer_invalid"],
    ["another raw format", { raw_format: "jsonl" }, "pointer_invalid"],
    ["a malformed hash", { content_sha256: "\\xabc" }, "pointer_invalid"],
    ["a different hash", { content_sha256: "\\x" + "0".repeat(64) }, "manifest_mismatch"],
    ["another revision", { revision: 2 }, "manifest_mismatch"],
    ["revision zero", { revision: 0 }, "pointer_invalid"],
    ["another user", { user_id: "user_2" }, "manifest_mismatch"],
    ["another machine", { machine_id: "srv_cc" }, "manifest_mismatch"],
    ["a foreign harness row", { harness: "codex" }, "tenant_mismatch"],
  ])("%s is refused as forbidden", async (_name, over, reason) => {
    const got = await refusal(lambdaSource({ rows: () => [{ ...pointerRow, ...over }] }).read());
    expect(got).toEqual({ class: "forbidden", codes: [reason] });
  });

  test("an object under another org's key prefix is refused as forbidden, never served", async () => {
    // org_2's pointer row naming org_1's manifest (the static-layout hazard the org prefix closes).
    const got = await refusal(lambdaSource({ rows: () => [{ ...pointerRow, org_id: "org_2" }] }).read({ ...identity, orgId: "org_2" }));
    expect(got.class).toBe("forbidden");
    expect(got.codes).toEqual(["prefix_mismatch"]);
  });

  test("a missing manifest is missing; a missing chunk is missing", async () => {
    await rm(join(root, objectKey(manifestKey(location))), { force: true });
    expect(await refusal(lambdaSource().read())).toEqual({ class: "missing", codes: ["object_missing"] });
  });

  test("a missing chunk is missing", async () => {
    await rm(join(root, chunkKey), { force: true });
    expect((await refusal(lambdaSource().read())).class).toBe("missing");
  });

  test("a store 403 is forbidden and a store 503 transient", async () => {
    const failing = (error: ObjectStoreError) => new Proxy(store, { get(t, k) { return k === "getObject" ? async () => { throw error; } : Reflect.get(t, k); } });
    expect(await refusal(lambdaSource({ store: failing(new ObjectStoreError("unauthorized", "s3", "synthetic", { status: 403 })) }).read()))
      .toEqual({ class: "forbidden", codes: ["store_unauthorized"] });
    expect(await refusal(lambdaSource({ store: failing(new ObjectStoreError("transient", "s3", "synthetic", { status: 503 })) }).read()))
      .toEqual({ class: "transient", codes: ["store_transient"] });
  });

  test("the hosted pointer bound (64) is oversize, before any object read", async () => {
    let touched = false;
    const spy = new Proxy(store, { get(t, k) { if (k === "getObject") touched = true; return Reflect.get(t, k); } });
    const rows = () => Array.from({ length: 64 }, (_, i) => ({ ...pointerRow, manifest_key: String(pointerRow.manifest_key).replace("/m=srv_bb/", `/m=srv_${i}/`) }));
    expect(await refusal(lambdaSource({ rows, store: spy }).read())).toEqual({ class: "oversize", codes: ["pointer_bound"] });
    expect(touched).toBe(false);
  });

  test("a pointer generation that moves during the read is refused (transient)", async () => {
    let call = 0;
    // A second live pointer appears between the pre-read and the post-read pointer query.
    const rows = () => ++call === 1 ? [pointerRow] : [pointerRow, { ...pointerRow, manifest_key: String(pointerRow.manifest_key).replace("/m=srv_bb/", "/m=srv_new/") }];
    expect(await refusal(lambdaSource({ rows }).read())).toEqual({ class: "transient", codes: ["source_generation_changed"] });
  });

  test("IAM dependency: a served read calls only getObject on the store (no list, head or presign)", async () => {
    const calls: string[] = [];
    const audited = new Proxy(store, { get(t, k) { const v = Reflect.get(t, k); return typeof v === "function" ? (...a: unknown[]) => { calls.push(String(k)); return (v as (...x: unknown[]) => unknown).apply(t, a); } : v; } });
    const got = await lambdaSource({ store: audited }).read();
    expect(got.exists).toBe(true);
    expect([...new Set(calls)]).toEqual(["getObject"]);
    // Manifest then chunk: exactly one GET each.
    expect(calls).toEqual(["getObject", "getObject"]);
  });

  test("IAM dependency: every object key read matches the class (a) pattern 96bulls/*/v1/*", async () => {
    const keys: string[] = [];
    const audited = new Proxy(store, { get(t, k) { const v = Reflect.get(t, k); return k === "getObject" ? (key: unknown, ...a: unknown[]) => { keys.push(String(key)); return (v as (...x: unknown[]) => unknown).call(t, key, ...a); } : v; } });
    await lambdaSource({ store: audited }).read();
    expect(keys).toHaveLength(2);
    for (const key of keys) expect(key).toMatch(/^96bulls\/[^/]+\/v1\//);
    // A pointer row naming a key outside the caller's scope is refused before any GET.
    const outside: string[] = [];
    const spy = new Proxy(store, { get(t, k) { const v = Reflect.get(t, k); return k === "getObject" ? (key: unknown, ...a: unknown[]) => { outside.push(String(key)); return (v as (...x: unknown[]) => unknown).call(t, key, ...a); } : v; } });
    const key = String(pointerRow.manifest_key);
    for (const bad of [
      key.replace("/v1/", "/v2/"),                       // another layout
      key.replace("96bulls/org_1/", "96bulls/org_2/"),   // another org
      key.replace("96bulls/", "other/"),                 // another base
      key.replace("/ws=ws_1/", "/ws=ws_2/"),             // another workspace
      key.replace("/h=claude/", "/h=codex/"),            // another harness
      key.replace("/manifest.json", "/raw/0000000000-0-aaaaaaaa.ndjson.zst"), // not a manifest
      key.replace("96bulls/org_1/", "96bulls/org_1/../org_2/"), // traversal
    ]) {
      expect([bad, await refusal(lambdaSource({ store: spy, rows: () => [{ ...pointerRow, manifest_key: bad }] }).read())]).toEqual([bad, { class: "forbidden", codes: ["prefix_mismatch"] }]);
    }
    expect(outside).toEqual([]);
  });

  test("session and machine: a row for another session is refused before any GET; a row naming another session's or machine's manifest is refused after one manifest GET, with no chunk read", async () => {
    // Another real session in the same org, workspace and harness (its own manifest object).
    const other = { ...location, sessionId: "sess_2" };
    const otherManifest = { ...manifest, session: { ...manifest.session, session_id: "sess_2" }, extensions: { "96bulls": { user_id: "user_1", canonical_session_id: "sess:2" } } };
    await store.put(objectKey(manifestKey(other)), new TextEncoder().encode(JSON.stringify(otherManifest)), {});
    const keys: string[] = [];
    const spy = new Proxy(store, { get(t, k) { const v = Reflect.get(t, k); return k === "getObject" ? (key: unknown, ...a: unknown[]) => { keys.push(String(key)); return (v as (...x: unknown[]) => unknown).call(t, key, ...a); } : v; } });
    const run = async (row: Record<string, unknown>) => { keys.length = 0; const got = await refusal(lambdaSource({ store: spy, rows: () => [row] }).read()); return { got, keys: [...keys] }; };

    // (1) The row itself is for another session (session_id is the canonical id the read role returns): refused pre-GET.
    expect(await run({ ...pointerRow, session_id: "sess:2", manifest_key: manifestKey(other) })).toEqual({ got: { class: "forbidden", codes: ["tenant_mismatch"] }, keys: [] });
    // (2) The row claims this session but names the other session's manifest: one manifest GET, then refused.
    expect(await run({ ...pointerRow, manifest_key: manifestKey(other) })).toEqual({ got: { class: "forbidden", codes: ["manifest_mismatch"] }, keys: [manifestKey(other)] });
    // (3) The row's machine differs from the manifest's: one manifest GET, then refused.
    expect(await run({ ...pointerRow, machine_id: "srv_cc" })).toEqual({ got: { class: "forbidden", codes: ["manifest_mismatch"] }, keys: [manifestKey(location)] });
    // (4) The row names this session's manifest under another machine's key (m=), absent here: one GET, nothing served.
    const otherMachineKey = String(pointerRow.manifest_key).replace("/m=srv_bb/", "/m=srv_cc/");
    expect(await run({ ...pointerRow, manifest_key: otherMachineKey })).toEqual({ got: { class: "missing", codes: ["object_missing"] }, keys: [otherMachineKey] });
    // No chunk key was ever requested in any case.
    expect(keys.some((k) => k.includes("/raw/"))).toBe(false);
  });

  test("key scope admits the caller's upload-generation publication manifests", async () => {
    const { parseSessionKey } = await import("../../../packages/object-store/src/index.ts");
    const at = parseSessionKey(String(pointerRow.manifest_key))!;
    const pub = `96bulls/org_1/upload-generations/g=0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b/v1/ws=${at.workspaceId}/m=${at.machineId}/h=${at.harness}/d=2026/11/01/s=${at.sessionId}/publications/0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5c/manifest.json`;
    // Admitted by the scope check: the read proceeds to the GET (absent here, so object_missing).
    const keys: string[] = [];
    const spy = new Proxy(store, { get(t, k) { const v = Reflect.get(t, k); return k === "getObject" ? (key: unknown, ...a: unknown[]) => { keys.push(String(key)); return (v as (...x: unknown[]) => unknown).call(t, key, ...a); } : v; } });
    const got = await refusal(lambdaSource({ store: spy, rows: () => [{ ...pointerRow, manifest_key: pub }] }).read());
    expect(got.codes).not.toContain("prefix_mismatch");
    expect(keys).toEqual([pub]);
  });

  test("IAM dependency: without ListBucket S3 answers 403 for a missing key; that is a refusal, never a read of anything else", async () => {
    const calls: string[] = [];
    const noList = new Proxy(store, { get(t, k) { if (typeof Reflect.get(t, k) !== "function") return Reflect.get(t, k); return async (...a: unknown[]) => { calls.push(String(k)); if (k === "getObject") throw new ObjectStoreError("unauthorized", "s3", "synthetic", { status: 403 }); return (Reflect.get(t, k) as (...x: unknown[]) => unknown).apply(t, a); }; } });
    expect(await refusal(lambdaSource({ store: noList }).read())).toEqual({ class: "forbidden", codes: ["store_unauthorized"] });
    expect(calls).toEqual(["getObject"]);
  });

  test("every object request carries the route's abort signal", async () => {
    const seen: Array<AbortSignal | undefined> = [];
    const deps = partialCanonicalDeps(ON, "lambda", runner(() => [pointerRow]), {
      fetch: (async (_input: unknown, init?: RequestInit) => { seen.push(init?.signal ?? undefined); return new Response("", { status: 503 }); }) as typeof fetch,
      credentials: async () => ({ accessKeyId: "AKIASYNTHETIC", secretAccessKey: "synthetic" }),
      makeStore: ((config: { fetch: typeof fetch }) => new Proxy(store, { get(t, k) { return k === "getObject" ? async () => { await config.fetch("https://synthetic.invalid/x"); throw new ObjectStoreError("transient", "s3", "synthetic", { status: 503 }); } : Reflect.get(t, k); } })) as never,
    })!;
    const route = new AbortController();
    const pending = deps.events.source(identity, route.signal).catch(() => {});
    await pending;
    expect(seen).toHaveLength(1);
    expect(seen[0]!.aborted).toBe(false);
    route.abort();
    expect(seen[0]!.aborted).toBe(true);
  });
});

describe("classifyPartialCanonical", () => {
  test.each([
    [new CanonicalReadError("deadline"), "transient"],
    [new CanonicalReadError("source_generation_changed"), "transient"],
    [new CanonicalReadError("object_missing"), "missing"],
    [new CanonicalReadError("pointer_bound"), "oversize"],
    [new CanonicalReadError("event_invalid"), "invalid"],
    [new CanonicalReadError("canonical_projection_incomplete", 503, ["scope_too_large"]), "oversize"],
    [new CanonicalReadError("canonical_projection_incomplete", 503, ["manifest_missing"]), "missing"],
    [new CanonicalReadError("canonical_projection_incomplete", 503, ["manifest_unscrubbed"]), "forbidden"],
    // A forbidden code wins over any other in the same refusal.
    [new CanonicalReadError("canonical_projection_incomplete", 503, ["scope_too_large", "manifest_key_mismatch", "manifest_missing"]), "forbidden"],
    [new CanonicalReadError("canonical_projection_incomplete", 503, ["fold_failed"]), "invalid"],
    [new ObjectStoreError("not_found", "s3", "x", { status: 404 }), "missing"],
    [new ObjectStoreError("unauthorized", "s3", "x", { status: 403 }), "forbidden"],
    [new ObjectStoreError("transient", "s3", "x", { status: 500 }), "transient"],
    [new ObjectStoreError("invalid_argument", "manifest", "manifest is larger than the reader accepts"), "oversize"],
    [new ObjectStoreError("backend", "s3", "x", { status: 400 }), "invalid"],
    [new PipelineError("chunk_unavailable", "x", true), "transient"],
    [new PipelineError("chunk_missing", "x", true), "missing"],
    [new PipelineError("scope_too_large", "x", false), "oversize"],
    [new PipelineError("something_retryable", "x", true), "transient"],
    [new PipelineError("something_fatal", "x", false), "invalid"],
    [new TypeError("fetch failed"), "transient"],
    [Object.assign(new Error("aborted"), { name: "AbortError" }), "transient"],
  ] as const)("%p is %s", (error, want) => {
    expect(classifyPartialCanonical(error).class).toBe(want);
  });

  test("codes are snake_case tokens only: no foreign text passes", () => {
    expect(classifyPartialCanonical(new PipelineError("Bad Code: /secret/key", "x", false)).codes).toEqual([]);
    expect(classifyPartialCanonical(new TypeError("fetch https://bucket/secret failed")).codes).toEqual([]);
    expect(classifyPartialCanonical(new ObjectStoreError("unauthorized", "s3", "96bulls/org_1/key: HTTP 403")).codes).toEqual(["store_unauthorized"]);
  });
});
