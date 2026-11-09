/**
 * Self-host async admission without a database: the SELFHOST_INGEST_ADMISSION settings, the Locker
 * conditional-write probe (a backend that ignores If-None-Match / If-Match refuses startup), the
 * dead-letter key shape, and the hosted import boundary (worker.ts never reaches a self-host module).
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { ConditionalWriteUnsupported, assertConditionalWrites } from "../../shared/locker.ts";
import { createGcsStore, createS3Store, type ObjectStore } from "../../shared/vendor.ts";
import { selfhostAdmissionSettings, selfhostIngestConfig } from "../selfhost-config.ts";
import { admissionStartupGate, conditionalWriteProbeKey } from "../selfhost-server.ts";
import { DEAD_LETTER_RETENTION_MS, deadLetterKey, selfhostAdmissionConsumer } from "../selfhost-admission.ts";
import { enqueueAdmission, inboxKey } from "../async-admission.ts";
import { LedgerUnavailableError } from "../ledger.ts";
import { prepareDelivery } from "../vendor/delivery/prepare.ts";
import { MemoryLedger, TENANT, batch, record, scratchStore } from "./support.ts";
import { VersionedMemoryStore } from "./versioned-store.ts";

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));

describe("SELFHOST_INGEST_ADMISSION", () => {
  const enabled = {
    RELAYHISTORY_SELFHOST_INGEST: "true",
    RELAYHISTORY_INGEST_ORIGIN: "https://history.selfhost.test",
    RELAYCONTROL_INTROSPECT_URL: "https://control.selfhost.test",
    RELAYCONTROL_INTROSPECT_SECRET: "s",
    RELAYHISTORY_INGEST_DATABASE_URL: "postgresql://bulls96_session_ingest:pw@db.internal:5432/postgres",
    SESSION_POSTGRES_TLS_PROFILE: "supabase",
    LOCKER_BUCKET: "transcripts",
  };

  test("unset, empty and sync leave the settings exactly as before (no admission field at all)", () => {
    const base = selfhostIngestConfig(enabled);
    for (const v of [undefined, "", "sync"]) {
      const c = selfhostIngestConfig({ ...enabled, SELFHOST_INGEST_ADMISSION: v, SELFHOST_ADMISSION_CONCURRENCY: "999" });
      expect(c).toEqual(base);
      expect(Object.hasOwn(c, "admission")).toBe(false);
    }
  });

  test("async and drain parse with bounded knobs; anything else refuses, naming the variable only", () => {
    expect(selfhostAdmissionSettings({ SELFHOST_INGEST_ADMISSION: "async" })).toEqual({ mode: "async", concurrency: 4, scanIntervalMs: 15_000, maxAttempts: 20 });
    expect(selfhostAdmissionSettings({ SELFHOST_INGEST_ADMISSION: "drain", SELFHOST_ADMISSION_CONCURRENCY: "8", SELFHOST_ADMISSION_SCAN_MS: "2000", SELFHOST_ADMISSION_MAX_ATTEMPTS: "5" }))
      .toEqual({ mode: "drain", concurrency: 8, scanIntervalMs: 2000, maxAttempts: 5 });
    for (const bad of ["on", "ASYNC", "true", " async"]) expect(() => selfhostAdmissionSettings({ SELFHOST_INGEST_ADMISSION: bad })).toThrow(/^96bulls: SELFHOST_INGEST_ADMISSION must be sync, async or drain$/);
    expect(() => selfhostAdmissionSettings({ SELFHOST_INGEST_ADMISSION: "async", SELFHOST_ADMISSION_CONCURRENCY: "0" })).toThrow(/SELFHOST_ADMISSION_CONCURRENCY/);
    expect(() => selfhostAdmissionSettings({ SELFHOST_INGEST_ADMISSION: "async", SELFHOST_ADMISSION_SCAN_MS: "10" })).toThrow(/SELFHOST_ADMISSION_SCAN_MS/);
    expect(selfhostIngestConfig({ ...enabled, SELFHOST_INGEST_ADMISSION: "async" })).toMatchObject({ enabled: true, admission: { mode: "async" } });
    // Dormant: nothing but the port is parsed, as before.
    expect(selfhostIngestConfig({ SELFHOST_INGEST_ADMISSION: "nonsense" })).toEqual({ enabled: false, port: 8081 });
  });
});

/** A fake S3 endpoint over the real S3 adapter (SigV4, headers, status mapping). `honour` false ignores the conditions. */
function fakeS3(honour: { ifNoneMatch: boolean; ifMatch: boolean }) {
  const objects = new Map<string, { body: Uint8Array; etag: string; at: Date }>();
  const seen: { method: string; ifNoneMatch: string | null; ifMatch: string | null }[] = [];
  let n = 0;
  const fetcher = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const req = new Request(input, init);
    const key = new URL(req.url).pathname;
    const found = objects.get(key);
    seen.push({ method: req.method, ifNoneMatch: req.headers.get("if-none-match"), ifMatch: req.headers.get("if-match") });
    const meta = (o: { body: Uint8Array; etag: string; at: Date }) => ({ etag: o.etag, "last-modified": o.at.toUTCString(), "content-length": String(o.body.byteLength) });
    if (req.method === "PUT") {
      if (honour.ifNoneMatch && req.headers.get("if-none-match") === "*" && found) return new Response("<Error><Code>PreconditionFailed</Code></Error>", { status: 412 });
      const im = req.headers.get("if-match");
      if (honour.ifMatch && im && (!found || found.etag !== im)) return new Response("<Error><Code>PreconditionFailed</Code></Error>", { status: 412 });
      const o = { body: new Uint8Array(await req.arrayBuffer()), etag: `"e${++n}"`, at: new Date() };
      objects.set(key, o);
      return new Response(null, { status: 200, headers: { etag: o.etag } });
    }
    if (req.method === "GET") return found ? new Response(found.body, { status: 200, headers: meta(found) }) : new Response("<Error><Code>NoSuchKey</Code></Error>", { status: 404 });
    if (req.method === "HEAD") return found ? new Response(null, { status: 200, headers: meta(found) }) : new Response(null, { status: 404 });
    if (req.method === "DELETE") { objects.delete(key); return new Response(null, { status: 204 }); }
    return new Response(null, { status: 405 });
  }) as typeof fetch;
  const store = createS3Store({ bucket: "transcripts", region: "us-east-1", endpoint: "http://minio.test:9000", credentials: { accessKeyId: "AKIDEXAMPLE", secretAccessKey: "x" }, fetch: fetcher }) as unknown as ObjectStore;
  return { store, objects, seen };
}

/** Wraps a store so it silently drops one kind of condition (an S3 look-alike or a stripping proxy). */
function ignoring(inner: ObjectStore, drop: "ifNoneMatch" | "ifMatch"): ObjectStore {
  return new Proxy(inner, {
    get(target, prop, receiver) {
      if (prop !== "put") return Reflect.get(target, prop, receiver);
      return (k: Parameters<ObjectStore["put"]>[0], b: Uint8Array, o: Parameters<ObjectStore["put"]>[2] = {}) => {
        const { [drop]: _dropped, ...rest } = o;
        return target.put(k, b, rest);
      };
    },
  });
}

describe("Locker conditional writes (assertConditionalWrites)", () => {
  const key = conditionalWriteProbeKey("96bulls", "0123456789abcdef0123456789abcdef");

  test("the probe key is disjoint from tenants and inbox scans, inside the srv-writer key shape", () => {
    expect(key).toBe("96bulls/.admission-probe/v1/ws=probe/m=srv_0123456789abcdef0123456789abcdef/probe.json");
    expect(/^96bulls\/[^/]+.*\/v1\/ws=[^/]+\/m=srv_/.test(key)).toBe(true);
    expect(key.startsWith("96bulls/.admission-inbox")).toBe(false);
  });

  test("honoured by the fs store, a versioned S3-like store and the S3 adapter (If-None-Match: *, If-Match: <etag>); the probe cleans up", async () => {
    const fs = scratchStore();
    cleanups.push(fs.done);
    await assertConditionalWrites(fs.store, key);
    expect(await fs.store.head(key as never)).toBeUndefined();
    await assertConditionalWrites(new VersionedMemoryStore(), key);
    const s3 = fakeS3({ ifNoneMatch: true, ifMatch: true });
    await assertConditionalWrites(s3.store, key);
    expect(s3.objects.size).toBe(0);
    const puts = s3.seen.filter((r) => r.method === "PUT");
    expect(puts.map((p) => p.ifNoneMatch ?? (p.ifMatch ? "if-match" : "none"))).toEqual(["*", "*", "if-match", "if-match"]);
  });

  test("a backend that ignores If-None-Match (overwrites on create) refuses startup", async () => {
    const s3 = fakeS3({ ifNoneMatch: false, ifMatch: true });
    const err = await assertConditionalWrites(s3.store, key).catch((e) => e);
    expect(err).toBeInstanceOf(ConditionalWriteUnsupported);
    expect(err.property).toBe("create_only");
    expect(String(err.message)).not.toContain(key);
    const fs = scratchStore();
    cleanups.push(fs.done);
    await expect(assertConditionalWrites(ignoring(fs.store, "ifNoneMatch"), key)).rejects.toMatchObject({ property: "create_only" });
  });

  test("a backend that ignores If-Match (no compare-and-swap) refuses startup", async () => {
    const s3 = fakeS3({ ifNoneMatch: true, ifMatch: false });
    await expect(assertConditionalWrites(s3.store, key)).rejects.toMatchObject({ property: "compare_and_swap" });
    const fs = scratchStore();
    cleanups.push(fs.done);
    await expect(assertConditionalWrites(ignoring(fs.store, "ifMatch"), key)).rejects.toMatchObject({ property: "compare_and_swap" });
  });

  test("the startup gate: sync never touches the store; async and drain refuse a backend that ignores the conditions", async () => {
    let touched = 0;
    const untouchable = new Proxy({} as ObjectStore, { get: () => () => { touched++; throw new Error("touched"); } });
    await admissionStartupGate({ keyRoot: "96bulls" }, untouchable);
    expect(touched).toBe(0);
    const admission = { concurrency: 4, scanIntervalMs: 15_000, maxAttempts: 20 };
    for (const mode of ["async", "drain"] as const) {
      await expect(admissionStartupGate({ keyRoot: "96bulls", admission: { mode, ...admission } }, fakeS3({ ifNoneMatch: false, ifMatch: true }).store)).rejects.toBeInstanceOf(ConditionalWriteUnsupported);
      await admissionStartupGate({ keyRoot: "96bulls", admission: { mode, ...admission } }, fakeS3({ ifNoneMatch: true, ifMatch: true }).store);
    }
  });

  test("GCS: the create is x-goog-if-generation-match: 0 (and a refusing GCS refuses startup)", async () => {
    const seen: (string | null)[] = [];
    const gcs = createGcsStore({ bucket: "transcripts", baseUrl: "http://gcs.test:4443", token: async () => ({ accessToken: "t" }),
      fetch: (async (input: RequestInfo | URL, init?: RequestInit) => {
        const req = new Request(input, init);
        if (req.method === "PUT" || req.method === "POST") seen.push(req.headers.get("x-goog-if-generation-match"));
        return new Response("{}", { status: 503 });
      }) as unknown as typeof fetch }) as unknown as ObjectStore;
    await expect(assertConditionalWrites(gcs, key)).rejects.toMatchObject({ property: "probe_failed" });
    expect(seen[0]).toBe("0");
  });

  test("an unreachable or refusing backend refuses startup with a code, never a key or message", async () => {
    const down = createS3Store({ bucket: "transcripts", region: "us-east-1", endpoint: "http://minio.test:9000", credentials: { accessKeyId: "AKIDEXAMPLE", secretAccessKey: "x" },
      fetch: (async () => new Response("<Error><Code>AccessDenied</Code><Message>secret detail</Message></Error>", { status: 403 })) as unknown as typeof fetch }) as unknown as ObjectStore;
    const err = await assertConditionalWrites(down, key).catch((e) => e);
    expect(err).toMatchObject({ property: "probe_failed" });
    expect(String(err.message)).not.toContain("secret detail");
    expect(String(err.message)).not.toContain(key);
  });
});

describe("dead letters", () => {
  test("the marker mirrors the inbox path under its own dot-namespace, and keeps the hosted 14-day retention", () => {
    const inbox = inboxKey("96bulls", TENANT, "origin-1", "rhr_" + "a".repeat(64), 1_800_000_000_000, "0123456789abcdef");
    const dead = deadLetterKey("96bulls", inbox);
    expect(dead.startsWith("96bulls/.admission-dlq/org-a/v1/ws=ws-a/m=srv_")).toBe(true);
    expect(dead.endsWith(`/r=${"a".repeat(64)}/a=001800000000000-0123456789abcdef.json`)).toBe(true);
    expect(() => deadLetterKey("96bulls", "96bulls/org-a/v1/x.ndjson.zst")).toThrow();
    expect(DEAD_LETTER_RETENTION_MS).toBe(1_209_600_000);
  });
});

describe("hosted boundary", () => {
  test("worker.ts never reaches a self-host module (the full static import graph under src/)", () => {
    const srcRoot = resolve(import.meta.dir, "../..");
    const seen = new Set<string>();
    const walk = (file: string) => {
      if (seen.has(file)) return;
      seen.add(file);
      const text = readFileSync(file, "utf8");
      for (const m of text.matchAll(/(?:^|\n)\s*(?:import|export)\s[^;]*?from\s+"(\.{1,2}\/[^"]+\.ts)"/g)) {
        const next = resolve(dirname(file), m[1]!);
        if (next.startsWith(srcRoot)) walk(next);
      }
    };
    walk(join(srcRoot, "session-ingest/worker.ts"));
    // (session-read-api/selfhost.ts is reached already on main through app.ts; this change adds no edge.)
    const selfhost = [...seen].filter((f) => /session-ingest\/selfhost-|shared\/locker\.ts$/.test(f));
    expect(selfhost).toEqual([]);
    expect(seen.size).toBeGreaterThan(10);
  });
});

describe("#405 INGEST_CLAIM_LOCK_BUSY through the self-host consumer", () => {
  test("a claim lock wait (ClaimLockBusy) backs off like IngestUnavailable and never dead-letters early", async () => {
    const fs = scratchStore();
    cleanups.push(fs.done);
    const ledger = new MemoryLedger();
    let claims = 0;
    ledger.claim = async () => { claims++; throw new LedgerUnavailableError("ledger_lock_timeout", { db_error_class: "lock", db_error_code: "55P03", db_error_basis: "sqlstate" } as never); };
    const prepared = await prepareDelivery(TENANT, await batch([record({ record_id: "lock-1" })]));
    let clock = Date.now();
    const key = await enqueueAdmission({ store: fs.store, keyRoot: "96bulls", enqueue: async () => {} }, TENANT, prepared, clock, 0);
    const lines: Record<string, unknown>[] = [];
    const sink = (_m: string, f: Record<string, unknown> = {}) => void lines.push(f);
    const consumer = selfhostAdmissionConsumer({
      mode: "on", concurrency: 1, scanIntervalMs: 15_000, maxAttempts: 3, nowMs: () => clock, log: { info: sink, warn: sink, error: sink },
      deliver: { ledger, acceptanceMode: "canonical", target: { store: fs.store, keyRoot: "96bulls" }, claimLockBusy: true, random: () => 0 },
    });
    await consumer.scan();
    await consumer.settle();
    // The HTTP 429 never reaches the queue path: the IngestUnavailable cause, stage and backoff.
    expect(lines.find((l) => l.route === "ingest.admission")).toMatchObject({ outcome: "retry", error_code: "ledger_lock_timeout", stage: "claim", attempt: 1 });
    // (The lock-wait alert line covers buffered claims only, as on hosted; canonical claims are not an alert stage.)
    // Backed off 15 s (attempt 1): nothing re-runs before it, then it does; no dead-letter marker.
    clock += 14_000;
    await consumer.scan();
    await consumer.settle();
    expect(claims).toBe(1);
    clock += 2_000;
    await consumer.scan();
    await consumer.settle();
    expect(claims).toBe(2);
    expect(await fs.store.head(deadLetterKey("96bulls", key) as never)).toBeUndefined();
    expect(consumer.status()).toMatchObject({ backlog: 1, deadLetters: 0 });
    await consumer.stop();
  });
});
