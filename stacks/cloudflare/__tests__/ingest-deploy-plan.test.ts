/**
 * 96bulls-session-ingest CI deploy (stacks/cloudflare/ingest-deploy/plan.ts, cli.ts): strict
 * profile parsing, precedence (explicit > committed profile > live), the binding guard, the
 * sanitized diff gate and the no-op render. Fixture ids are synthetic and built at runtime, so
 * no file carries anything that looks like a real Cloudflare id.
 */
import { describe, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PROFILE_PATH, main as cliMain, plan as cliPlan } from "../ingest-deploy/cli.ts";
import {
  DEFAULT_INPUTS,
  INPUT_ENV,
  IngestDeployError,
  activeVersionId,
  assertDiffAllowed,
  guardRender,
  parseCommittedProfile,
  parseDispatchInputs,
  parseLiveSettings,
  parseLiveVersion,
  planDeploy,
  sanitizedDiff,
  valuesToMask,
  type DispatchInputs,
  type LiveBinding,
  type LiveVersion,
  type Plan,
} from "../ingest-deploy/plan.ts";

const HYPERDRIVE = "1".repeat(32);
const STORE = "2".repeat(32);
const VERSION = `${"3".repeat(8)}-${"3".repeat(4)}-${"3".repeat(4)}-${"3".repeat(4)}-${"3".repeat(12)}`;
const GENERATION = "4".repeat(64);
const SECRET_VALUE = "MUST_NOT_BE_COPIED";
const PATHS = { main: "../src/session-ingest/worker.ts", tsconfig: "../tsconfig.json" };
const PROFILE = parseCommittedProfile(readFileSync(PROFILE_PATH, "utf8"));

/** Shaped like the live buffered+drain+claim-lock Worker (runbooks/sessions-prod-session-ingest.md). */
function liveBindings(over: Record<string, string | undefined> = {}): LiveBinding[] {
  const vars: Record<string, string | undefined> = {
    INGEST_S3_BUCKET: "synthetic-transcripts-bucket",
    INGEST_S3_REGION: "us-east-1",
    INGEST_KEY_ROOT: "96bulls",
    INGEST_KMS_KEY_ARN: "synthetic-kms-key-reference",
    INGEST_MAX_PER_USER: "4",
    INGEST_MAX_PER_ORG: "32",
    INGEST_INLINE_MAX_SESSIONS: "0",
    INGEST_REPLICA_MODE: "off",
    INGEST_ACCEPTANCE_MODE: "buffered",
    INGEST_EXPECT_GENERATION: GENERATION,
    INGEST_CUSTODY_SOURCE: "frozen-supabase",
    INGEST_ASYNC_ADMISSION: "drain",
    INGEST_CLAIM_LOCK_BUSY: "on",
    ...over,
  };
  return [
    ...Object.entries(vars).filter((e): e is [string, string] => e[1] !== undefined).map(([name, text]) => ({ type: "plain_text" as const, name, text })),
    { type: "hyperdrive", name: "HYPERDRIVE", id: HYPERDRIVE },
    { type: "queue", name: "INGEST_FANOUT", queue_name: "96bulls-session-ingest-fanout" },
    { type: "queue", name: "INGEST_ADMISSION", queue_name: "96bulls-session-ingest-admission" },
    { type: "secrets_store_secret", name: "S3_ACCESS_KEY_ID", store_id: STORE, secret_name: "synthetic-s3-access" },
    { type: "secrets_store_secret", name: "S3_SECRET_ACCESS_KEY", store_id: STORE, secret_name: "synthetic-s3-secret" },
    { type: "secrets_store_secret", name: "INGEST_SIGNING_KEY", store_id: STORE, secret_name: "synthetic-signing" },
    { type: "secret_text", name: "LEGACY_WORKER_SECRET" },
  ];
}

const live = (over?: Record<string, string | undefined>): LiveVersion => ({
  id: VERSION,
  bindings: liveBindings(over),
  compatibilityDate: "2026-10-01",
  compatibilityFlags: ["nodejs_compat"],
});
const settings = { placementMode: "smart" as const, observability: { enabled: true, head_sampling_rate: 1 }, logpush: false };
const inputs = (over: Partial<DispatchInputs> = {}): DispatchInputs => ({ ...DEFAULT_INPUTS, ...over });
const refuses = (fn: () => unknown, why: RegExp) => {
  expect(fn).toThrow(IngestDeployError);
  expect(fn).toThrow(why);
};
const decision = (p: Plan, knob: string) => p.decisions.find((d) => d.knob === knob)!;

describe("committed production profile", () => {
  test("the committed file parses and owns exactly the four knobs", () => {
    expect(PROFILE).toEqual({ INGEST_ACCEPTANCE_MODE: "buffered", INGEST_ASYNC_ADMISSION: "drain", INGEST_CLAIM_LOCK_BUSY: "on", INGEST_CLAIM_TIMEOUT_BUSY: "off" });
  });
  const base = { INGEST_ACCEPTANCE_MODE: "buffered", INGEST_ASYNC_ADMISSION: "drain", INGEST_CLAIM_LOCK_BUSY: "on", INGEST_CLAIM_TIMEOUT_BUSY: "off" };
  test("refuses unknown keys, including preserved settings and bindings", () => {
    for (const extra of ["INGEST_EXPECT_GENERATION", "INGEST_KMS_KEY_ARN", "HYPERDRIVE", "INGEST_SYNC_ADMISSION_FROZEN_RECEIPT", "comment"])
      refuses(() => parseCommittedProfile(JSON.stringify({ ...base, [extra]: "x" })), /unknown key/);
  });
  test("refuses a missing knob", () => {
    const { INGEST_CLAIM_TIMEOUT_BUSY: _, ...rest } = base;
    refuses(() => parseCommittedProfile(JSON.stringify(rest)), /must set INGEST_CLAIM_TIMEOUT_BUSY/);
  });
  test("refuses non-canonical values and non-strings", () => {
    for (const bad of [{ INGEST_CLAIM_LOCK_BUSY: "ON" }, { INGEST_CLAIM_LOCK_BUSY: " on" }, { INGEST_CLAIM_LOCK_BUSY: true }, { INGEST_ASYNC_ADMISSION: "yes" }, { INGEST_ACCEPTANCE_MODE: "" }, { INGEST_CLAIM_TIMEOUT_BUSY: null }])
      refuses(() => parseCommittedProfile(JSON.stringify({ ...base, ...bad })), /must be one of/);
  });
  test("refuses duplicate keys, arrays, non-JSON, a BOM and oversized input", () => {
    refuses(() => parseCommittedProfile(JSON.stringify(base).replace("}", ',"INGEST_CLAIM_LOCK_BUSY":"off"}')), /more than once/);
    refuses(() => parseCommittedProfile("[]"), /JSON object/);
    refuses(() => parseCommittedProfile("{"), /not valid JSON/);
    refuses(() => parseCommittedProfile("﻿" + JSON.stringify(base)), /byte-order mark/);
    refuses(() => parseCommittedProfile(" ".repeat(5000)), /small JSON/);
  });
});

describe("dispatch inputs", () => {
  const env = (over: Record<string, string> = {}) => ({
    [INPUT_ENV.INGEST_ASYNC_ADMISSION]: "profile",
    [INPUT_ENV.INGEST_CLAIM_LOCK_BUSY]: "profile",
    [INPUT_ENV.INGEST_CLAIM_TIMEOUT_BUSY]: "profile",
    [INPUT_ENV.INGEST_SYNC_ADMISSION_FROZEN_RECEIPT]: "",
    [INPUT_ENV.dryRun]: "true",
    ...over,
  });
  test("defaults parse to the default inputs (dry run on)", () => {
    expect(parseDispatchInputs(env())).toEqual(DEFAULT_INPUTS);
  });
  test("the frozen-receipt string must be exactly empty, off or on", () => {
    expect(parseDispatchInputs(env({ [INPUT_ENV.INGEST_SYNC_ADMISSION_FROZEN_RECEIPT]: "on" })).INGEST_SYNC_ADMISSION_FROZEN_RECEIPT).toBe("on");
    for (const bad of ["ON", " on", "on\n", "true", "1", "profile", "off,on"])
      refuses(() => parseDispatchInputs(env({ [INPUT_ENV.INGEST_SYNC_ADMISSION_FROZEN_RECEIPT]: bad })), /INGEST_SYNC_ADMISSION_FROZEN_RECEIPT/);
  });
  test("refuses unknown choices and a missing dry_run", () => {
    refuses(() => parseDispatchInputs(env({ [INPUT_ENV.INGEST_ASYNC_ADMISSION]: "enabled" })), /ASYNC_ADMISSION/);
    refuses(() => parseDispatchInputs(env({ [INPUT_ENV.INGEST_CLAIM_LOCK_BUSY]: "drain" })), /CLAIM_LOCK_BUSY/);
    refuses(() => parseDispatchInputs(env({ [INPUT_ENV.dryRun]: "" })), /DRY_RUN/);
  });
});

describe("no-op render", () => {
  test("default inputs against the live shape render exactly live: empty diff, every binding preserved", () => {
    const p = planDeploy(live(), settings, PROFILE, inputs(), PATHS);
    expect(sanitizedDiff(p.decisions)).toEqual([]);
    expect(() => assertDiffAllowed(p.decisions)).not.toThrow();
    const liveVars = Object.fromEntries(liveBindings().flatMap((b) => (b.type === "plain_text" ? [[b.name, b.text]] : [])));
    expect(p.config.vars).toEqual(liveVars);
    expect(Object.keys(p.config.vars)).toEqual(Object.keys(liveVars));
    expect(p.config.hyperdrive).toEqual([{ binding: "HYPERDRIVE", id: HYPERDRIVE }]);
    expect(p.config.queues).toEqual({ producers: [{ binding: "INGEST_FANOUT", queue: "96bulls-session-ingest-fanout" }, { binding: "INGEST_ADMISSION", queue: "96bulls-session-ingest-admission" }] });
    expect(p.config.secrets_store_secrets).toHaveLength(3);
    expect(p.config.observability).toEqual(settings.observability);
    expect(p.config.logpush).toBe(false);
    expect(p.config).toMatchObject({ name: "96bulls-session-ingest", workers_dev: false, preview_urls: false, placement: { mode: "smart" }, keep_vars: false, compatibility_date: "2026-10-01", compatibility_flags: ["nodejs_compat"] });
    expect(JSON.stringify(p.config)).not.toContain("LEGACY_WORKER_SECRET");
  });
  test("preserves a non-canonical live literal that means the profile value", () => {
    const p = planDeploy(live({ INGEST_CLAIM_LOCK_BUSY: "ON" }), settings, PROFILE, inputs(), PATHS);
    expect(p.config.vars.INGEST_CLAIM_LOCK_BUSY).toBe("ON");
    expect(sanitizedDiff(p.decisions)).toEqual([]);
  });
  test("a profile off with the variable unset stays unset (absent means off)", () => {
    const p = planDeploy(live(), settings, PROFILE, inputs(), PATHS);
    expect("INGEST_CLAIM_TIMEOUT_BUSY" in p.config.vars).toBe(false);
    expect(decision(p, "INGEST_CLAIM_TIMEOUT_BUSY")).toMatchObject({ source: "profile", liveValue: "off", renderedValue: "off" });
  });
  test("an explicit off on a live literal off keeps the literal", () => {
    const p = planDeploy(live({ INGEST_CLAIM_TIMEOUT_BUSY: "off" }), settings, PROFILE, inputs({ INGEST_CLAIM_TIMEOUT_BUSY: "off" }), PATHS);
    expect(p.config.vars.INGEST_CLAIM_TIMEOUT_BUSY).toBe("off");
    expect(sanitizedDiff(p.decisions)).toEqual([]);
  });
});

describe("precedence: explicit > committed profile > live", () => {
  test("profile mode fills an unset live value (the diff gate then needs the input explicitly)", () => {
    const p = planDeploy(live({ INGEST_CLAIM_LOCK_BUSY: undefined }), settings, PROFILE, inputs(), PATHS);
    expect(decision(p, "INGEST_CLAIM_LOCK_BUSY")).toMatchObject({ source: "profile", liveRaw: undefined, renderedRaw: "on" });
    expect(p.config.vars.INGEST_CLAIM_LOCK_BUSY).toBe("on");
    expect(sanitizedDiff(p.decisions)).toEqual(["INGEST_CLAIM_LOCK_BUSY: off -> on (profile)"]);
    refuses(() => assertDiffAllowed(p.decisions), /dispatch that input explicitly/);
  });
  test("the same fill is deployable when the input is explicit", () => {
    const p = planDeploy(live({ INGEST_CLAIM_LOCK_BUSY: undefined }), settings, PROFILE, inputs({ INGEST_CLAIM_LOCK_BUSY: "on" }), PATHS);
    expect(sanitizedDiff(p.decisions)).toEqual(["INGEST_CLAIM_LOCK_BUSY: off -> on (explicit)"]);
    expect(() => assertDiffAllowed(p.decisions)).not.toThrow();
  });
  test("profile mode refuses a set-and-differing live value", () => {
    refuses(() => planDeploy(live({ INGEST_CLAIM_LOCK_BUSY: "off" }), settings, PROFILE, inputs(), PATHS), /live INGEST_CLAIM_LOCK_BUSY is off but the production profile says on/);
    refuses(() => planDeploy(live({ INGEST_ASYNC_ADMISSION: "on" }), settings, PROFILE, inputs(), PATHS), /live INGEST_ASYNC_ADMISSION is on but the production profile says drain/);
    refuses(() => planDeploy(live({ INGEST_CLAIM_TIMEOUT_BUSY: "on" }), settings, PROFILE, inputs(), PATHS), /INGEST_CLAIM_TIMEOUT_BUSY is on but the production profile says off/);
    refuses(() => planDeploy(live({ INGEST_ACCEPTANCE_MODE: "canonical", INGEST_ASYNC_ADMISSION: undefined }), settings, PROFILE, inputs(), PATHS), /INGEST_ACCEPTANCE_MODE is canonical/);
  });
  test("profile mode refuses off -> on from a live literal off", () => {
    const profileOn = { ...PROFILE, INGEST_CLAIM_TIMEOUT_BUSY: "on" };
    refuses(() => planDeploy(live({ INGEST_CLAIM_TIMEOUT_BUSY: "off" }), settings, profileOn, inputs(), PATHS), /is off but the production profile says on; refusing in profile mode/);
  });
  test("an explicit input beats the profile and live, both ways", () => {
    const off = planDeploy(live(), settings, PROFILE, inputs({ INGEST_CLAIM_LOCK_BUSY: "off" }), PATHS);
    expect("INGEST_CLAIM_LOCK_BUSY" in off.config.vars).toBe(false);
    expect(sanitizedDiff(off.decisions)).toEqual(["INGEST_CLAIM_LOCK_BUSY: on -> off (explicit)"]);
    const on = planDeploy(live(), settings, PROFILE, inputs({ INGEST_CLAIM_TIMEOUT_BUSY: "on" }), PATHS);
    expect(on.config.vars.INGEST_CLAIM_TIMEOUT_BUSY).toBe("on");
    expect(sanitizedDiff(on.decisions)).toEqual(["INGEST_CLAIM_TIMEOUT_BUSY: off -> on (explicit)"]);
    // An explicit input also overrides a live value the profile would refuse.
    const fromOn = planDeploy(live({ INGEST_ASYNC_ADMISSION: "on" }), settings, PROFILE, inputs({ INGEST_ASYNC_ADMISSION: "drain" }), PATHS);
    expect(fromOn.config.vars.INGEST_ASYNC_ADMISSION).toBe("drain");
  });
  test("an explicit input equal to live changes nothing", () => {
    const p = planDeploy(live(), settings, PROFILE, inputs({ INGEST_ASYNC_ADMISSION: "drain", INGEST_CLAIM_LOCK_BUSY: "on", INGEST_CLAIM_TIMEOUT_BUSY: "off", INGEST_SYNC_ADMISSION_FROZEN_RECEIPT: "off" }), PATHS);
    expect(sanitizedDiff(p.decisions)).toEqual([]);
    expect(p.config.vars).toEqual(planDeploy(live(), settings, PROFILE, inputs(), PATHS).config.vars);
  });
  test("the frozen receipt is preserved from live unless given", () => {
    const kept = planDeploy(live({ INGEST_SYNC_ADMISSION_FROZEN_RECEIPT: "on" }), settings, PROFILE, inputs(), PATHS);
    expect(decision(kept, "INGEST_SYNC_ADMISSION_FROZEN_RECEIPT")).toMatchObject({ source: "live", renderedRaw: "on" });
    const on = planDeploy(live(), settings, PROFILE, inputs({ INGEST_SYNC_ADMISSION_FROZEN_RECEIPT: "on" }), PATHS);
    expect(on.config.vars.INGEST_SYNC_ADMISSION_FROZEN_RECEIPT).toBe("on");
  });
  test("cross-knob rules of the stack hold", () => {
    refuses(() => planDeploy(live({ INGEST_ASYNC_ADMISSION: undefined }), settings, PROFILE, inputs({ INGEST_ASYNC_ADMISSION: "on" }), PATHS), /drain before on/);
    refuses(() => planDeploy(live({ INGEST_CUSTODY_SOURCE: "relayhistory" }), settings, PROFILE, inputs({ INGEST_SYNC_ADMISSION_FROZEN_RECEIPT: "on" }), PATHS), /frozen-supabase/);
    const noQueue = live();
    noQueue.bindings = noQueue.bindings.filter((b) => b.name !== "INGEST_ADMISSION");
    refuses(() => planDeploy(noQueue, settings, PROFILE, inputs(), PATHS), /INGEST_ADMISSION queue binding/);
    refuses(() => planDeploy(live({ INGEST_EXPECT_GENERATION: undefined }), settings, PROFILE, inputs(), PATHS), /INGEST_EXPECT_GENERATION/);
  });
  test("a live knob value the Worker would not accept is refused", () => {
    refuses(() => planDeploy(live({ INGEST_ASYNC_ADMISSION: "maybe" }), settings, PROFILE, inputs(), PATHS), /does not understand/);
  });
});

describe("guard", () => {
  const good = () => planDeploy(live(), settings, PROFILE, inputs(), PATHS);
  test("refuses a render that drops a binding", () => {
    const p = good();
    p.config.queues!.producers = p.config.queues!.producers.filter((q) => q.binding !== "INGEST_ADMISSION");
    refuses(() => guardRender(live(), settings, p), /drops live binding INGEST_ADMISSION/);
    const q = good();
    delete q.config.vars.INGEST_EXPECT_GENERATION;
    refuses(() => guardRender(live(), settings, q), /drops live binding INGEST_EXPECT_GENERATION/);
  });
  test("refuses a changed Hyperdrive id, queue, Secrets Store reference or preserved variable", () => {
    const h = good();
    h.config.hyperdrive![0]!.id = "9".repeat(32);
    refuses(() => guardRender(live(), settings, h), /changes live binding HYPERDRIVE/);
    const s = good();
    s.config.secrets_store_secrets![0]!.secret_name = "other";
    refuses(() => guardRender(live(), settings, s), /changes live binding S3_ACCESS_KEY_ID/);
    const v = good();
    v.config.vars.INGEST_MAX_PER_ORG = "64";
    refuses(() => guardRender(live(), settings, v), /changes live binding INGEST_MAX_PER_ORG/);
  });
  test("refuses a variable shadowing a Worker secret or a Secrets Store binding", () => {
    const p = good();
    p.config.vars.LEGACY_WORKER_SECRET = SECRET_VALUE;
    refuses(() => guardRender(live(), settings, p), /LEGACY_WORKER_SECRET is declared twice or shadows a live secret/);
    const q = good();
    q.config.vars.INGEST_SIGNING_KEY = SECRET_VALUE;
    refuses(() => guardRender(live(), settings, q), /INGEST_SIGNING_KEY is declared twice/);
  });
  test("refuses added bindings, queue consumers, workers.dev and setting drift", () => {
    const add = good();
    add.config.vars.NEW_THING = "x";
    refuses(() => guardRender(live(), settings, add), /adds binding NEW_THING/);
    const consumer = good();
    (consumer.config.queues as Record<string, unknown>).consumers = [{ queue: "q" }];
    refuses(() => guardRender(live(), settings, consumer), /queue consumers/);
    const dev = good();
    (dev.config as unknown as Record<string, unknown>).workers_dev = true;
    refuses(() => guardRender(live(), settings, dev), /workers.dev/);
    const obs = good();
    obs.config.observability = { enabled: false };
    refuses(() => guardRender(live(), settings, obs), /observability/);
    const routes = good();
    (routes.config as unknown as Record<string, unknown>).routes = [];
    refuses(() => guardRender(live(), settings, routes), /unexpected key routes/);
  });
  test("refuses a knob change that came from live without an input or the profile", () => {
    const p = good();
    const d = p.decisions.find((x) => x.knob === "INGEST_CLAIM_LOCK_BUSY")!;
    Object.assign(d, { source: "live", renderedRaw: undefined, renderedValue: "off" });
    delete p.config.vars.INGEST_CLAIM_LOCK_BUSY;
    refuses(() => guardRender(live(), settings, p), /changed without an input or the profile/);
  });
  test("refuses compatibility drift from the stack and non-smart placement", () => {
    refuses(() => planDeploy({ ...live(), compatibilityDate: "2026-01-01" }, settings, PROFILE, inputs(), PATHS), /compatibility date/);
    refuses(() => parseLiveSettings({ placement: { mode: "off" } }), /smart placement/);
  });
});

describe("live reads", () => {
  test("only a single version at 100% is deployable over", () => {
    expect(activeVersionId({ deployments: [{ versions: [{ version_id: VERSION, percentage: 100 }] }] })).toBe(VERSION);
    refuses(() => activeVersionId({ deployments: [{ versions: [{ version_id: VERSION, percentage: 90 }, { version_id: VERSION, percentage: 10 }] }] }), /split deployment/);
    refuses(() => activeVersionId({ deployments: [] }), /no deployment/);
    refuses(() => activeVersionId({ deployments: [{ versions: [{ version_id: "latest", percentage: 100 }] }] }), /not a version id/);
  });
  const raw = (bindings: unknown[], id = VERSION) => ({ id, resources: { bindings, script_runtime: { compatibility_date: "2026-10-01", compatibility_flags: ["nodejs_compat"] } } });
  test("parses the supported binding types and refuses anything else", () => {
    expect(parseLiveVersion(raw(liveBindings()), VERSION).bindings).toEqual(liveBindings());
    refuses(() => parseLiveVersion(raw([{ type: "r2_bucket", name: "B", bucket_name: "x" }]), VERSION), /cannot reproduce live binding B \(type r2_bucket\)/);
    refuses(() => parseLiveVersion(raw([{ type: "queue", name: "Q", queue_name: "x", extra: 1 }]), VERSION), /unexpected field extra/);
    refuses(() => parseLiveVersion(raw([{ type: "plain_text", name: "A", text: "1" }, { type: "plain_text", name: "A", text: "2" }]), VERSION), /appears twice/);
    refuses(() => parseLiveVersion(raw([{ type: "plain_text", name: "A", text: "x\ny" }]), VERSION), /control characters/);
    refuses(() => parseLiveVersion(raw([], "other"), VERSION), /does not match the active version/);
  });
  test("settings keep observability and logpush, refusing shapes it does not understand", () => {
    expect(parseLiveSettings({ placement: { mode: "smart" }, observability: { enabled: true }, logpush: false })).toEqual({ placementMode: "smart", observability: { enabled: true }, logpush: false });
    refuses(() => parseLiveSettings({ placement: { mode: "smart" }, observability: "on" }), /observability/);
  });
  test("masks preserved values for the job log but never knob tokens", () => {
    const masks = valuesToMask(live());
    expect(masks).toContain(HYPERDRIVE);
    expect(masks).toContain(STORE);
    expect(masks).toContain(GENERATION);
    expect(masks).not.toContain("buffered");
    expect(masks).not.toContain(VERSION);
  });
});

describe("cli plan with a fake read-only Cloudflare", () => {
  const env = { [INPUT_ENV.INGEST_ASYNC_ADMISSION]: "profile", [INPUT_ENV.INGEST_CLAIM_LOCK_BUSY]: "profile", [INPUT_ENV.INGEST_CLAIM_TIMEOUT_BUSY]: "profile", [INPUT_ENV.INGEST_SYNC_ADMISSION_FROZEN_RECEIPT]: "", [INPUT_ENV.dryRun]: "true" };
  const get = async (path: string) => {
    if (path === "/deployments") return { deployments: [{ versions: [{ version_id: VERSION, percentage: 100 }] }] };
    if (path === `/versions/${VERSION}`) return { id: VERSION, resources: { bindings: liveBindings(), script_runtime: { compatibility_date: "2026-10-01", compatibility_flags: ["nodejs_compat"] } } };
    if (path === "/settings") return { placement: { mode: "smart" }, observability: { enabled: true }, logpush: false };
    throw new Error(`unexpected read ${path}`);
  };
  test("renders the no-op with paths relative to the output directory and no secret value", async () => {
    const out = join(import.meta.dir, "..", "..", "..", ".tmp-never-written");
    const p = await cliPlan(env, get, out);
    expect(sanitizedDiff(p.decisions)).toEqual([]);
    expect(p.config.main).toBe("../src/session-ingest/worker.ts");
    expect(p.config.tsconfig).toBe("../tsconfig.json");
    expect(p.json).not.toContain(SECRET_VALUE);
    expect(p.sha256).toMatch(/^[0-9a-f]{64}$/);
    expect(p.liveVersionId).toBe(VERSION);
  });
});

describe("cli plan, recheck and verify end to end with a fake read-only Cloudflare", () => {
  const NEXT = `${"4".repeat(8)}-${"4".repeat(4)}-${"4".repeat(4)}-${"4".repeat(4)}-${"4".repeat(12)}`;
  const version = (id: string, bindings = liveBindings()) => ({ id, resources: { bindings, script_runtime: { compatibility_date: "2026-10-01", compatibility_flags: ["nodejs_compat"] } } });
  const fake = (active: string, versions: Record<string, unknown>, settings: unknown = { placement: { mode: "smart" }, observability: { enabled: true }, logpush: false }) => async (path: string) => {
    if (path === "/deployments") return { deployments: [{ versions: [{ version_id: active, percentage: 100 }] }] };
    if (path === "/settings") return settings;
    const v = versions[path.replace("/versions/", "")];
    if (v) return v;
    throw new Error(`unexpected read ${path}`);
  };
  const run = async (f: (dir: string, env: NodeJS.ProcessEnv) => Promise<void>) => {
    const dir = mkdtempSync(join(tmpdir(), "ingest-deploy-test-"));
    try {
      const output = join(dir, "github-output");
      writeFileSync(output, "");
      const env = { [INPUT_ENV.INGEST_ASYNC_ADMISSION]: "profile", [INPUT_ENV.INGEST_CLAIM_LOCK_BUSY]: "profile", [INPUT_ENV.INGEST_CLAIM_TIMEOUT_BUSY]: "profile", [INPUT_ENV.INGEST_SYNC_ADMISSION_FROZEN_RECEIPT]: "", [INPUT_ENV.dryRun]: "false", GITHUB_OUTPUT: output };
      await f(join(dir, "out"), env);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  };
  const outputs = (env: NodeJS.ProcessEnv) => Object.fromEntries(readFileSync(env.GITHUB_OUTPUT!, "utf8").trim().split("\n").map((l) => l.split("=") as [string, string]));

  test("plan writes the outputs and a config without secret values; recheck and verify accept an unchanged deploy", async () => {
    await run(async (out, env) => {
      const before = fake(VERSION, { [VERSION]: version(VERSION) });
      await cliMain(["plan", "--out-dir", out], env, before);
      const o = outputs(env);
      expect(o).toMatchObject({ live_version: VERSION, diff_empty: "true", dry_run: "false" });
      expect(readFileSync(join(out, "wrangler.json"), "utf8")).not.toContain(SECRET_VALUE);
      await cliMain(["recheck", "--out-dir", out, "--expect-version", VERSION, "--expect-sha", o.render_sha256!], env, before);
      const after = fake(NEXT, { [VERSION]: version(VERSION), [NEXT]: version(NEXT) });
      await cliMain(["verify", "--out-dir", out, "--previous", VERSION], env, after);
    });
  });
  test("recheck refuses a live change or another render; verify refuses no new version, a lost binding and changed settings", async () => {
    await run(async (out, env) => {
      const before = fake(VERSION, { [VERSION]: version(VERSION) });
      await cliMain(["plan", "--out-dir", out], env, before);
      const sha = outputs(env).render_sha256!;
      const moved = fake(NEXT, { [NEXT]: version(NEXT) });
      await expect(cliMain(["recheck", "--out-dir", out, "--expect-version", VERSION, "--expect-sha", sha], env, moved)).rejects.toThrow(/live Worker changed/);
      await expect(cliMain(["recheck", "--out-dir", out, "--expect-version", VERSION, "--expect-sha", "0".repeat(64)], env, before)).rejects.toThrow(/render changed/);
      await expect(cliMain(["verify", "--out-dir", out, "--previous", VERSION], env, before)).rejects.toThrow(/still the previous one/);
      const lost = fake(NEXT, { [VERSION]: version(VERSION), [NEXT]: version(NEXT, liveBindings().filter((b) => b.type !== "hyperdrive")) });
      await expect(cliMain(["verify", "--out-dir", out, "--previous", VERSION], env, lost)).rejects.toThrow(/bindings differ/);
      const unobserved = fake(NEXT, { [VERSION]: version(VERSION), [NEXT]: version(NEXT) }, { placement: { mode: "smart" }, observability: { enabled: false }, logpush: false });
      await expect(cliMain(["verify", "--out-dir", out, "--previous", VERSION], env, unobserved)).rejects.toThrow(/settings after the deploy/);
    });
  });
});
