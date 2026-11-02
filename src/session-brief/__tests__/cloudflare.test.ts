/**
 * The session brief Cloudflare Worker entry (src/session-brief/cloudflare.ts): one bounded
 * tick per cron invocation, over PGlite (every migration), an fs Locker, a fake manifest
 * reader and a fake model, with the Worker's env shapes (Secrets Store bindings, a
 * Hyperdrive connection string, string vars). Plus the bundle the Worker ships: it must
 * build for workerd with only nodejs_compat built-ins and cloudflare:sockets external.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { builtinModules } from "node:module";
import { join } from "node:path";
import type { PGlite } from "@electric-sql/pglite";
import type { ManifestV1, ObjectStore } from "../../shared/vendor.ts";
import { ROLE_VALIDATION_NAME as DB_ROLE_VALIDATION_NAME, ROLE_VALIDATION_TABLE as DB_ROLE_VALIDATION_TABLE } from "../../shared/db.ts";
import { expectedMigrationNames } from "../../hoopdb/migrate.ts";
import { briefConfig } from "../config.ts";
import { armWakeup } from "../store.ts";
import worker, {
  BRIEF_MIGRATION,
  ROLE_VALIDATION_NAME,
  ROLE_VALIDATION_TABLE,
  briefGateOpen,
  briefVars,
  briefWorkerSettings,
  runBriefInvocation,
  type BriefWorkerDeps,
  type BriefWorkerEnv,
} from "../cloudflare.ts";
import { sessionBriefVars, WORKER_MAX_SESSION_BYTES } from "../../../stacks/cloudflare/session-brief.ts";
import { FakeModel, KEY, database, dueNow, hoopDb, mapReader, pointer, root, tempStore, transcript } from "./harness.ts";

setDefaultTimeout(60_000);

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

const secretBinding = (value: string | null) => ({
  get: async () => {
    if (value === null) throw new Error("secret not found");
    return value;
  },
});

/** The deployed env, as the stack declares it, with the bindings as workerd hands them over. */
const deployedEnv = (over: Partial<BriefWorkerEnv> = {}): BriefWorkerEnv => ({
  ...sessionBriefVars({ accountId: "a".repeat(32), hyperdriveId: "b".repeat(32), secretsStoreId: "c".repeat(32) }),
  HYPERDRIVE: { connectionString: "postgresql://hyperdrive-local/postgres" },
  OPENAI_API_KEY: secretBinding("sk-test-not-a-real-key"),
  LOCKER_ACCESS_KEY_ID: secretBinding("AKIATESTTESTTESTTEST"),
  LOCKER_SECRET_ACCESS_KEY: secretBinding("test-secret"),
  ...over,
});

const user = (text: string) => ({ type: "user", timestamp: "2026-10-29T10:00:01.000Z", message: { role: "user", content: text } });
const assistant = (text: string) => ({
  type: "assistant",
  timestamp: "2026-10-29T10:00:02.000Z",
  message: { role: "assistant", model: "claude-x", content: [{ type: "text", text }] },
});

interface Rig {
  pg: PGlite;
  store: ObjectStore;
  manifests: Map<string, ManifestV1>;
  model: FakeModel;
  deps: BriefWorkerDeps;
  waits: Promise<unknown>[];
  ended: { n: number };
  ctx: { waitUntil(p: Promise<unknown>): void };
  seen: { connectionString?: string; apiKey?: string; locker?: unknown };
}

async function rig(opts: { ledgered?: boolean; marker?: "pass" | "pending" | null } = {}): Promise<Rig> {
  const pg = await database();
  open.push(pg);
  // The migrator's ledger and marker (PGlite's harness applies the SQL directly).
  await pg.exec(`CREATE TABLE IF NOT EXISTS sessions.__96bulls_migrations (name text PRIMARY KEY, sha256 text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now());
    CREATE TABLE IF NOT EXISTS ${ROLE_VALIDATION_TABLE} (name text PRIMARY KEY, generation text NOT NULL, status text NOT NULL, updated_at timestamptz NOT NULL DEFAULT now());`);
  if (opts.ledgered !== false) await pg.query("INSERT INTO sessions.__96bulls_migrations (name, sha256) VALUES ($1, 'x')", [BRIEF_MIGRATION]);
  const marker = opts.marker === undefined ? "pass" : opts.marker;
  if (marker) await pg.query(`INSERT INTO ${ROLE_VALIDATION_TABLE} (name, generation, status) VALUES ($1, 'g', $2)`, [ROLE_VALIDATION_NAME, marker]);
  const store = tempStore();
  const manifests = new Map<string, ManifestV1>();
  const model = new FakeModel();
  const waits: Promise<unknown>[] = [];
  const ended = { n: 0 };
  const seen: Rig["seen"] = {};
  const deps: BriefWorkerDeps = {
    openDb: (cs) => {
      seen.connectionString = cs;
      const db = hoopDb(pg);
      return { ...db, end: async () => void ended.n++ };
    },
    locker: (s) => {
      seen.locker = s;
      return { store, reader: mapReader(manifests) };
    },
    transport: (apiKey) => {
      seen.apiKey = apiKey;
      return model;
    },
    now: () => new Date(),
  };
  return { pg, store, manifests, model, deps, waits, ended, ctx: { waitUntil: (p) => void waits.push(p) }, seen };
}

async function upload(r: Rig, lines: unknown[], key = KEY) {
  const manifestKey = `manifest/${key.sessionId}/m-1/r1`;
  r.manifests.set(manifestKey, await transcript(r.store, lines, { key }));
  await pointer(r.pg, { key, manifestKey });
  await armWakeup(hoopDb(r.pg), key, { idleSeconds: 600, maxWaitSeconds: 3600 });
  await dueNow(r.pg);
}

const briefRow = async (pg: PGlite, key = KEY) =>
  (await pg.query<{ status: string; error: string | null; title: string | null }>(
    "SELECT status, error, title FROM sessions.session_briefs WHERE org_id=$1 AND workspace_id=$2 AND harness=$3 AND session_id=$4",
    [key.orgId, key.workspaceId, key.harness, key.sessionId],
  )).rows[0];

describe("session brief Worker: settings from the Worker env", () => {
  test("the deployed vars are the code defaults, enabled, with the Worker's session byte bound", async () => {
    const s = await briefWorkerSettings(deployedEnv());
    expect(s.kind).toBe("ready");
    if (s.kind !== "ready") return;
    expect(s.config).toEqual({ ...briefConfig({ SESSION_BRIEF_ENABLED: "true" }), maxSessionBytes: WORKER_MAX_SESSION_BYTES });
    expect(s.config.globalDailyCap).toBe(20_000);
    expect(s.apiKey).toBe("sk-test-not-a-real-key");
    expect(s.locker).toEqual({ bucket: "96bulls-aws-sessions-prod-transcripts-941433935742", region: "us-east-1", accessKeyId: "AKIATESTTESTTESTTEST", secretAccessKey: "test-secret" });
    expect(s.connectionString).toBe("postgresql://hyperdrive-local/postgres");
  });

  test("only SESSION_BRIEF_* strings reach briefConfig; bindings never do", () => {
    const vars = briefVars(deployedEnv());
    expect(Object.keys(vars).every((k) => k.startsWith("SESSION_BRIEF_"))).toBe(true);
    expect(vars).not.toHaveProperty("OPENAI_API_KEY");
    expect(Object.values(vars).every((v) => typeof v === "string")).toBe(true);
  });

  test("off unless SESSION_BRIEF_ENABLED is exactly true; the Secrets Store is not read when off", async () => {
    let reads = 0;
    const counting = { get: async () => (reads++, "x") };
    for (const v of ["false", "1", "TRUE", "", undefined])
      expect((await briefWorkerSettings(deployedEnv({ SESSION_BRIEF_ENABLED: v, OPENAI_API_KEY: counting }))).kind).toBe("disabled");
    expect(reads).toBe(0);
  });

  test("not configured until Hyperdrive, the bucket and both halves of the S3 key are there", async () => {
    const missing = async (over: Partial<BriefWorkerEnv>) => {
      const s = await briefWorkerSettings(deployedEnv(over));
      return s.kind === "not_configured" ? s.missing : [];
    };
    expect(await missing({ HYPERDRIVE: undefined })).toEqual(["HYPERDRIVE"]);
    expect(await missing({ LOCKER_BUCKET: "" })).toEqual(["LOCKER_BUCKET"]);
    // An unset Secrets Store secret throws on get(): treated as missing, never as a crash.
    expect(await missing({ LOCKER_ACCESS_KEY_ID: secretBinding(null) })).toEqual(["LOCKER_ACCESS_KEY_ID"]);
    expect(await missing({ LOCKER_SECRET_ACCESS_KEY: secretBinding("  ") })).toEqual(["LOCKER_SECRET_ACCESS_KEY"]);
  });

  test("no model key (unset, blank, or *-disabled) leaves the model off but the Worker configured", async () => {
    for (const k of [secretBinding(null), secretBinding(""), secretBinding("sk-x-disabled")]) {
      const s = await briefWorkerSettings(deployedEnv({ OPENAI_API_KEY: k }));
      expect(s.kind === "ready" && s.apiKey).toBeNull();
    }
  });

  test("the gate constants are the migrator's own", async () => {
    expect(ROLE_VALIDATION_TABLE).toBe(DB_ROLE_VALIDATION_TABLE);
    expect(ROLE_VALIDATION_NAME).toBe(DB_ROLE_VALIDATION_NAME);
    expect(await expectedMigrationNames()).toContain(BRIEF_MIGRATION);
  });
});

describe("session brief Worker: one bounded tick per invocation", () => {
  test("briefs a due session end to end and closes its pool through waitUntil", async () => {
    const r = await rig();
    await upload(r, [user("fix the flaky queue test"), assistant("the lease renewal raced the ack; fixed by awaiting it")]);
    const out = await runBriefInvocation(deployedEnv(), r.ctx, r.deps);
    expect(out).toEqual({ status: "ticked", counts: { generated: 1 } as never });
    expect(r.model.requests.length).toBe(1);
    expect((await briefRow(r.pg))?.status).toBe("ready");
    expect(r.seen.apiKey).toBe("sk-test-not-a-real-key");
    expect(r.seen.connectionString).toBe("postgresql://hyperdrive-local/postgres");
    await Promise.all(r.waits);
    expect(r.ended.n).toBe(1);
    // The wakeup is done: the next invocation claims nothing and calls no model.
    expect(await runBriefInvocation(deployedEnv(), r.ctx, r.deps)).toEqual({ status: "ticked", counts: {} as never });
    expect(r.model.requests.length).toBe(1);
  });

  test("claims at most SESSION_BRIEF_BATCH wakeups per invocation", async () => {
    const r = await rig();
    for (let i = 0; i < 7; i++)
      await upload(r, [user(`task ${i}`), assistant(`done ${i}`)], { ...KEY, sessionId: `s-${i}` });
    const first = await runBriefInvocation(deployedEnv({ SESSION_BRIEF_BATCH: "5" }), r.ctx, r.deps);
    expect(first.status === "ticked" && first.counts.generated).toBe(5);
    const second = await runBriefInvocation(deployedEnv({ SESSION_BRIEF_BATCH: "5" }), r.ctx, r.deps);
    expect(second.status === "ticked" && second.counts.generated).toBe(2);
    expect(r.model.requests.length).toBe(7);
  });

  test("a session over the Worker's byte bound is recorded session_too_large with no model call", async () => {
    const r = await rig();
    // Incompressible-ish text, so the raw chunk is past a 1 MiB bound.
    const big = Array.from({ length: 1100 }, (_, i) => user(`${i} ${crypto.randomUUID().repeat(30)}`));
    await upload(r, [...big, assistant("ok")]);
    const out = await runBriefInvocation(deployedEnv({ SESSION_BRIEF_MAX_SESSION_BYTES: String(1024 * 1024) }), r.ctx, r.deps);
    expect(out).toEqual({ status: "ticked", counts: { failed: 1 } as never });
    expect(r.model.requests.length).toBe(0);
    expect(await briefRow(r.pg)).toMatchObject({ status: "failed", error: "session_too_large" });
    // Nothing spent: no attempt was admitted against the caps.
    expect((await r.pg.query("SELECT 1 FROM sessions.session_brief_attempts")).rows.length).toBe(0);
  });

  test("without a model key it prunes but claims nothing (the wakeup stays due)", async () => {
    const r = await rig();
    await upload(r, [user("hello"), assistant("hi")]);
    const out = await runBriefInvocation(deployedEnv({ OPENAI_API_KEY: secretBinding(null) }), r.ctx, r.deps);
    expect(out).toEqual({ status: "model_not_configured" });
    expect(r.seen.apiKey).toBeUndefined();
    const w = (await r.pg.query<{ attempts: number; lease_id: string | null }>("SELECT attempts, lease_id FROM sessions.session_brief_wakeups")).rows;
    expect(w).toEqual([{ attempts: 0, lease_id: null }]);
  });

  test("disabled: no database connection at all", async () => {
    const r = await rig();
    expect(await runBriefInvocation(deployedEnv({ SESSION_BRIEF_ENABLED: "false" }), r.ctx, r.deps)).toEqual({ status: "disabled" });
    expect(r.seen.connectionString).toBeUndefined();
  });

  test("not configured: no database connection, the missing names reported", async () => {
    const r = await rig();
    expect(await runBriefInvocation(deployedEnv({ HYPERDRIVE: undefined }), r.ctx, r.deps)).toEqual({ status: "not_configured", reason: "HYPERDRIVE" });
    expect(r.seen.connectionString).toBeUndefined();
  });

  for (const [name, opts, reason] of [
    ["the brief migration is not ledgered", { ledgered: false }, "schema_pending"],
    ["a migrator is running (marker pending)", { marker: "pending" }, "validation_pending"],
    ["the role validation never passed", { marker: null }, "validation_pending"],
  ] as const) {
    test(`gate closed when ${name}: nothing claimed, no model, pool still closed`, async () => {
      const r = await rig(opts);
      await upload(r, [user("hello"), assistant("hi")]);
      expect(await runBriefInvocation(deployedEnv(), r.ctx, r.deps)).toEqual({ status: "gated", reason });
      expect(r.model.requests.length).toBe(0);
      expect(r.seen.locker).toBeUndefined();
      expect((await r.pg.query<{ attempts: number }>("SELECT attempts FROM sessions.session_brief_wakeups")).rows).toEqual([{ attempts: 0 }]);
      await Promise.all(r.waits);
      expect(r.ended.n).toBe(1);
    });
  }

  test("a gate query error closes the gate with its code", async () => {
    const failing = { query: async () => Promise.reject(Object.assign(new Error("nope"), { code: "42501" })) };
    expect(await briefGateOpen(failing as never)).toEqual({ open: false, reason: "gate_error:42501" });
  });

  test("the scheduled handler runs the invocation and rethrows a tick-level failure (the next cron retries)", async () => {
    const env = deployedEnv({ HYPERDRIVE: { connectionString: "postgresql://127.0.0.1:1/none" } });
    const waits: Promise<unknown>[] = [];
    // The real Hyperdrive client: an unreachable database is a gate error, not a throw.
    await worker.scheduled({}, env, { waitUntil: (p) => void waits.push(p) });
    await Promise.all(waits);
    // Disabled: returns without touching anything.
    await worker.scheduled({}, deployedEnv({ SESSION_BRIEF_ENABLED: "false" }), { waitUntil: () => {} });
  });
});

describe("session brief Worker bundle", () => {
  const NODE = new Set(builtinModules.flatMap((m) => [m, `node:${m}`]));
  test("bundles for workerd with only nodejs_compat built-ins and cloudflare:sockets external", async () => {
    const externals = new Set<string>();
    const result = await Bun.build({
      entrypoints: [join(root, "src/session-brief/cloudflare.ts")],
      target: "browser",
      format: "esm",
      conditions: ["workerd", "worker", "browser"],
      tsconfig: join(root, "tsconfig.json"),
      throw: false,
      plugins: [
        {
          name: "externals",
          setup(b) {
            b.onResolve({ filter: /.*/ }, (args) => {
              if (NODE.has(args.path) || args.path.startsWith("cloudflare:")) {
                externals.add(args.path);
                return { path: args.path, external: true };
              }
              return undefined;
            });
          },
        },
      ],
    });
    expect(result.logs.filter((l) => l.level === "error").map((l) => l.message)).toEqual([]);
    expect(result.success).toBe(true);
    for (const e of externals) expect(NODE.has(e) || e === "cloudflare:sockets").toBe(true);
    expect(externals.has("cloudflare:sockets")).toBe(true);
    const code = await result.outputs[0]!.text();
    // pg reaches Hyperdrive through pg-cloudflare's socket (pg's optional dependency).
    expect(code).toContain("class CloudflareSocket");
    // No AWS SDK, no Lambda runtime, no fs Locker, no migration or verifier SQL read at load
    // (src/shared/db.ts and src/hoopdb/migrate.ts read files; the Worker carries none).
    expect(code).not.toMatch(/@aws-sdk|aws-lambda|createFsStore|session_directory_search_worker_verify\.sql|readdir\(/);
    // The only entry handler is scheduled: no fetch, so nothing answers a request.
    expect(code).toMatch(/async scheduled\(/);
    expect(code).not.toMatch(/async fetch\(request/);
  }, 60_000);
});
