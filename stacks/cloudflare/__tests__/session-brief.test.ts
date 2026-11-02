import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import * as Cloudflare from "@alchemy.run/cloudflare";
import { briefConfig } from "../../../src/session-brief/config.ts";
import { assertApprovalBinding } from "../../aws/deploy-guard.ts";
import {
  COMPATIBILITY_DATE,
  OPENAI_KEY_SECRET,
  S3_ACCESS_KEY_SECRET,
  S3_SECRET_KEY_SECRET,
  SESSION_BRIEF_CPU_MS,
  SESSION_BRIEF_CRONS,
  SESSION_BRIEF_RESOURCES,
  SESSION_BRIEF_WORKER,
  WORKER_MAX_SESSION_BYTES,
  assertNoSecretLiterals,
  assertPrivateBriefWorker,
  assertSessionBriefInputs,
  assertSessionBriefPlanContract,
  secretsStoreSecret,
  sessionBriefVars,
  sessionBriefWorkerProps,
  type SessionBriefInputs,
} from "../session-brief.ts";
import { assertSessionBriefStage, cloudflareMode, sessionBriefCaptureFiles, sessionBriefInputsPath } from "../alchemy.session-brief.run.ts";
import { sessionBriefCapture } from "../../../scripts/cf-session-brief-capture.ts";

const refuse = (why: string): never => {
  throw new Error(why);
};
const hex = (c: string) => c.repeat(32);
const inputs: SessionBriefInputs = { accountId: hex("a"), hyperdriveId: hex("b"), secretsStoreId: hex("c") };
const bindings = { HYPERDRIVE: Cloudflare.Hyperdrive.lookup("HYPERDRIVE", { hyperdriveId: hex("b") }) };
const props = (p: SessionBriefInputs = inputs) => sessionBriefWorkerProps(p, bindings) as unknown as Record<string, unknown> & { env: Record<string, unknown> };
const src = (rel: string) => readFileSync(fileURLToPath(new URL(rel, import.meta.url)), "utf8");

describe("session brief worker: a cron and no way in", () => {
  it("the declared Worker passes the private-worker invariant", () => {
    expect(() => assertPrivateBriefWorker(props(), refuse)).not.toThrow();
    const p = props();
    expect(p.name).toBe(SESSION_BRIEF_WORKER);
    expect(p.main).toBe("src/session-brief/cloudflare.ts");
    expect(p.workersDev).toBe(false);
    expect(p.routes).toEqual([]);
    expect(p.domain).toBeNull();
    expect(p.crons).toEqual(["* * * * *"]);
  });

  it("refuses any public surface, another entry, or another schedule", () => {
    const bad: Record<string, unknown>[] = [
      { workersDev: true },
      { workersDev: undefined },
      { workersDev: { enabled: false, previewsEnabled: true } },
      { routes: [{ pattern: "brief.agentrelay.com/*" }] },
      { routes: undefined },
      { domain: "brief.agentrelay.com" },
      { domain: undefined },
      { url: true },
      { preview: { of: "x" } },
      { access: { policies: [] } },
      { assets: "./public" },
      { name: "other" },
      { main: "src/session-ingest/worker.ts" },
      { crons: [] },
      { crons: undefined },
      { crons: ["*/1 * * * *", "0 * * * *"] },
      { crons: ["* * * * * *"] },
    ];
    for (const b of bad) expect(() => assertPrivateBriefWorker({ ...props(), ...b }, refuse)).toThrow();
  });

  it("needs nodejs_compat at the pinned date and the reviewed CPU limit", () => {
    const p = props();
    expect(p.compatibility).toEqual({ date: COMPATIBILITY_DATE, flags: ["nodejs_compat"] });
    expect(p.limits).toEqual({ cpuMs: SESSION_BRIEF_CPU_MS });
    expect(p.observability).toEqual({ enabled: true });
    expect(() => assertPrivateBriefWorker({ ...p, compatibility: { date: COMPATIBILITY_DATE, flags: [] } }, refuse)).toThrow(/nodejs_compat/);
    expect(() => assertPrivateBriefWorker({ ...p, compatibility: { date: COMPATIBILITY_DATE, flags: ["nodejs_compat", "no_nodejs_compat"] } }, refuse)).toThrow(/nodejs_compat/);
    expect(() => assertPrivateBriefWorker({ ...p, compatibility: { date: "2027-01-01", flags: ["nodejs_compat"] } }, refuse)).toThrow(/pinned/);
    expect(() => assertPrivateBriefWorker({ ...p, limits: undefined }, refuse)).toThrow(/CPU limit/);
  });

  it("the stage applies the invariants to exactly the props it deploys, and declares nothing else", () => {
    const stage = src("../session-brief.ts");
    expect(stage).toContain("assertPrivateBriefWorker(props, refuse);");
    expect(stage).toContain("assertNoSecretLiterals(props.env, refuse);");
    expect(stage).toContain('Cloudflare.Worker("session-brief", props as never)');
    expect(stage).not.toMatch(/Cloudflare\.Workers?\.(Route|Domain|Subdomain|CustomDomain)\(/);
    expect([...stage.matchAll(/yield\* Cloudflare\.[A-Za-z.]+\(/g)].map((m) => m[0])).toEqual(["yield* Cloudflare.Worker("]);
    // The entry exports a scheduled handler and no fetch handler.
    const entry = src("../../../src/session-brief/cloudflare.ts");
    expect(entry).toContain("async scheduled(");
    expect(entry).not.toMatch(/async fetch\(/);
  });
});

describe("session brief worker: vars, bindings and no credential in state", () => {
  it("every brief limit is the code default (global cap 20000/day) except the Worker's byte bound", () => {
    const vars = sessionBriefVars(inputs);
    expect(vars.SESSION_BRIEF_ENABLED).toBe("true");
    expect(vars.SESSION_BRIEF_GLOBAL_DAILY_CAP).toBe("20000");
    expect(briefConfig(vars)).toEqual({ ...briefConfig({ SESSION_BRIEF_ENABLED: "true" }), maxSessionBytes: WORKER_MAX_SESSION_BYTES });
    // Each var is one briefConfig reads (a typo would silently fall back to a default).
    const read = src("../../../src/session-brief/config.ts");
    for (const k of Object.keys(vars).filter((k) => k.startsWith("SESSION_BRIEF_"))) expect([k, read.includes(`"${k}"`) || read.includes(`env.${k} `)]).toEqual([k, true]);
    expect(vars.LOCKER_BUCKET).toBe("96bulls-aws-sessions-prod-transcripts-941433935742");
    expect(sessionBriefVars({ ...inputs, enabled: false }).SESSION_BRIEF_ENABLED).toBe("false");
    expect(briefConfig(sessionBriefVars({ ...inputs, enabled: false })).enabled).toBe(false);
  });

  it("the byte bound fits the isolate and is accepted by briefConfig", () => {
    expect(WORKER_MAX_SESSION_BYTES).toBeLessThanOrEqual(32 * 1024 * 1024);
    expect(briefConfig({ SESSION_BRIEF_MAX_SESSION_BYTES: String(WORKER_MAX_SESSION_BYTES) }).maxSessionBytes).toBe(WORKER_MAX_SESSION_BYTES);
  });

  it("the OpenAI key and the S3 key are Secrets Store bindings, never values; Hyperdrive is the only database path", () => {
    const env = props().env;
    expect(env.OPENAI_API_KEY).toEqual(secretsStoreSecret(hex("c"), OPENAI_KEY_SECRET));
    expect(env.LOCKER_ACCESS_KEY_ID).toEqual(secretsStoreSecret(hex("c"), S3_ACCESS_KEY_SECRET));
    expect(env.LOCKER_SECRET_ACCESS_KEY).toEqual(secretsStoreSecret(hex("c"), S3_SECRET_KEY_SECRET));
    expect(Cloudflare.Hyperdrive.isHyperdriveConnection(env.HYPERDRIVE)).toBe(true);
    expect(() => assertNoSecretLiterals(env, refuse)).not.toThrow();
    for (const bad of [
      { OPENAI_API_KEY: "sk-proj-abc" },
      { LOCKER_SECRET_ACCESS_KEY: "wJalrXUtnFEMI/K7MDENG" },
      { HOOPDB_URL: "postgresql://bulls96_runtime:pw@pooler/postgres" },
      { DATABASE_URL: "postgresql://u:p@host/db" },
      { SESSION_BRIEF_NOTE: "postgresql://u:p@host/db" },
      { SESSION_BRIEF_MODEL: "sk-proj-0123456789abcdef" },
      { LOCKER_ACCESS_KEY_ID: { Type: "Cloudflare.SecretsStore.Secret", value: "AKIA" } },
      { HYPERDRIVE: { connectionString: "postgresql://u:p@h/db" } },
    ])
      expect(() => assertNoSecretLiterals({ ...env, ...bad }, refuse)).toThrow();
    const { OPENAI_API_KEY: _, ...missing } = env;
    expect(() => assertNoSecretLiterals(missing, refuse)).toThrow(/missing/);
    // Every plain var is a string: no object Alchemy would bind as json.
    for (const [k, v] of Object.entries(sessionBriefVars(inputs))) expect([k, typeof v]).toEqual([k, "string"]);
  });

  it("the Secrets Store descriptors are classified as Alchemy's Worker classifies the real resource", () => {
    expect(Cloudflare.SecretsStore.isSecret(secretsStoreSecret(hex("c"), "s"))).toBe(true);
    const classifier = src("../../../node_modules/@alchemy.run/cloudflare/src/Workers/WorkerAsyncBindings.ts");
    expect(classifier).toMatch(/isSecret\(binding\)\) \{\s*return \{\s*type: "secrets_store_secret",\s*name: bindingName,\s*secretName: binding\.secretName,\s*storeId: binding\.storeId,/);
  });
});

describe("session brief worker: inputs and stage guard", () => {
  it("accepts the documented shapes and refuses everything else", () => {
    assertSessionBriefInputs(inputs, refuse);
    assertSessionBriefInputs({ ...inputs, enabled: false }, refuse);
    for (const bad of [
      null,
      [],
      { ...inputs, extra: 1 },
      { ...inputs, accountId: "ABC" },
      { ...inputs, hyperdriveId: undefined },
      { ...inputs, secretsStoreId: hex("g") },
      { ...inputs, enabled: "true" },
      { ...inputs, openaiApiKey: "sk-x" },
    ])
      expect(() => assertSessionBriefInputs(bad, refuse)).toThrow();
  });

  it("the stage refuses another stage, destroy, another account and a deploy without approval", () => {
    const env = { CLOUDFLARE_ACCOUNT_ID: hex("a") };
    expect(() => assertSessionBriefStage("cf-sessions-prod", "plan", { sessionBrief: inputs }, env)).not.toThrow();
    expect(() => assertSessionBriefStage("cf-other", "plan", { sessionBrief: inputs }, env)).toThrow(/only/);
    expect(() => assertSessionBriefStage("cf-sessions-prod", "destroy", { sessionBrief: inputs }, env)).toThrow(/never destroyed/);
    expect(() => assertSessionBriefStage("cf-sessions-prod", "plan", { sessionBrief: inputs }, { CLOUDFLARE_ACCOUNT_ID: hex("d") })).toThrow(/CLOUDFLARE_ACCOUNT_ID/);
    expect(() => assertSessionBriefStage("cf-sessions-prod", "plan", { sessionBrief: inputs, sessionIngest: {} }, env)).toThrow(/not a known setting/);
    expect(() => assertSessionBriefStage("cf-sessions-prod", "plan", { sessionBrief: { ...inputs, hyperdriveId: "<hyperdrive id>" } }, env)).toThrow();
    expect(() => assertSessionBriefStage("cf-sessions-prod", "deploy", { sessionBrief: inputs }, env)).toThrow(/approval/);
    expect(() =>
      assertSessionBriefStage("cf-sessions-prod", "deploy", { sessionBrief: inputs, approval: { decision: "approved", by: "Someone Else", date: "2026-11-02", quote: "ok" } }, env),
    ).toThrow(/approval/);
    expect(cloudflareMode(["alchemy", "destroy"])).toBe("destroy");
    expect(cloudflareMode(["alchemy", "plan"])).toBe("plan");
    expect(cloudflareMode(["alchemy"])).toBe("deploy");
  });

  it("inputs and captures have their own paths, apart from #154's ingest stage", () => {
    expect(sessionBriefInputsPath("cf-sessions-prod")).toBe(".96bulls/cf-sessions-prod/session-brief-inputs.json");
    expect(sessionBriefCaptureFiles("cf-sessions-prod")).toEqual({
      plan: "docs/plans/cf-sessions-prod.session-brief.plan.txt",
      detailed: "docs/plans/cf-sessions-prod.session-brief.plan.detailed.txt",
      provenance: "docs/plans/cf-sessions-prod.session-brief.provenance.json",
    });
    expect(src("../alchemy.session-brief.run.ts")).toContain('export const SESSION_BRIEF_STACK = "96bulls-session-brief";');
  });
});

describe("session brief worker: deploy plan contract", () => {
  const plan = (action: string, resources: readonly string[] = SESSION_BRIEF_RESOURCES, extra = "") => {
    const summary = action === "noop" ? "Plan: 0 changes" : `Plan: 1 to ${action}`;
    return `${summary}\n${resources.map((r) => `[${r}] ${r === "session-brief" ? action : "create"}`).join("\n")}\n${extra}`;
  };
  const check = (compact: string, detailed = compact.split("\n").map((l) => (l.startsWith("[") ? `  ${l}` : l)).join("\n")) =>
    assertSessionBriefPlanContract(compact, detailed);

  it("rollout creates, update changes in place, post-apply changes nothing", () => {
    expect(check(plan("create"))).toBe("session brief rollout");
    expect(check(plan("update"))).toBe("session brief update");
    expect(check(plan("noop"))).toBe("post-apply");
  });

  it("refuses replacement, deletion, a missing or foreign resource, and disagreeing plans", () => {
    for (const a of ["replace", "delete"]) expect(() => check(`Plan: 1 to ${a}\n[session-brief] ${a}\n`)).toThrow(/forbidden action/);
    expect(() => check("Plan: 0 changes\n")).toThrow(/missing resource session-brief/);
    expect(() => check(plan("create", [...SESSION_BRIEF_RESOURCES, "session-brief-route"]).replace("Plan: 1 to create", "Plan: 2 to create"))).toThrow(/unexpected resource session-brief-route/);
    expect(() => check(plan("create"), plan("update"))).toThrow(/summaries differ|disagree/);
    expect(() => check(plan("update").replace("Plan: 1 to update", "Plan: 2 to update"))).toThrow(/summary/);
    expect(() => check(`${plan("update")}[session-brief] update\n`)).toThrow(/more than once/);
  });

  it("refuses a detailed plan that shows a credential (a secret value in state)", () => {
    const ok = plan("update");
    for (const leak of [
      '  "type": "secret_text"',
      "  AKIAABCDEFGHIJKLMNOP",
      "  OPENAI_API_KEY: sk-proj-abcdefghijklmnopqrstuvwxyz",
      '  "x": "postgresql://bulls96_runtime:pw@aws-0-us-east-1.pooler.supabase.com:5432/postgres"',
      "  -----BEGIN RSA PRIVATE KEY-----",
    ])
      expect(() => check(ok, `${ok}${leak}\n`)).toThrow(/credential/);
    // Binding descriptors (store id and secret name) are not credentials.
    expect(check(ok, `${ok}    storeId: ${hex("c")}\n    secretName: ${OPENAI_KEY_SECRET}\n`)).toBe("session brief update");
  });

  it("the deploy entry checks the approval binding and the contract before declaring anything", () => {
    const run = src("../alchemy.session-brief.run.ts");
    const deployBlock = run.slice(run.indexOf('if (mode === "deploy") {\n      const files'), run.indexOf("const brief = yield* sessionBriefStage"));
    expect(deployBlock).toContain("assertApprovalBinding(inputs.approval?.capture, { provenanceText, planText, inputsText });");
    expect(deployBlock).toContain("assertSessionBriefPlanContract(planText, detailedPlanText);");
  });
});

describe("session brief worker: capture provenance binds the approval", () => {
  const inputsText = JSON.stringify({ sessionBrief: inputs });
  const planText = "Plan: 1 to create\n[session-brief] create\n";
  const detailedText = "Plan: 1 to create\n[session-brief] create\n  properties:\n    name: 96bulls-session-brief\n";
  const commit = "0123456789abcdef0123456789abcdef01234567";
  const out = sessionBriefCapture({ stage: "cf-sessions-prod", accountId: hex("a"), commit, inputsText, planText, detailedText, now: new Date("2026-11-02T00:00:00Z") });

  it("records the phase and an approval.capture the deploy guard accepts", () => {
    expect(out.phase).toBe("session brief rollout");
    expect(() => assertApprovalBinding(out.capture, { provenanceText: out.provenanceText, planText, inputsText })).not.toThrow();
    // The approval record itself is not part of the inputs digest.
    const withApproval = JSON.stringify({ sessionBrief: inputs, approval: { decision: "approved", capture: out.capture } });
    expect(() => assertApprovalBinding(out.capture, { provenanceText: out.provenanceText, planText, inputsText: withApproval })).not.toThrow();
  });

  it("another plan, other inputs or a recapture refuse", () => {
    expect(() => assertApprovalBinding(out.capture, { provenanceText: out.provenanceText, planText: plan2(), inputsText })).toThrow(/planSha256/);
    expect(() => assertApprovalBinding(out.capture, { provenanceText: out.provenanceText, planText, inputsText: JSON.stringify({ sessionBrief: { ...inputs, enabled: false } }) })).toThrow(/inputsSha256/);
    expect(() => assertApprovalBinding(out.capture, { provenanceText: out.provenanceText.replace("2026-11-02", "2026-11-03"), planText, inputsText })).toThrow(/provenanceSha256/);
  });

  it("refuses to capture a plan the contract refuses", () => {
    expect(() => sessionBriefCapture({ stage: "cf-sessions-prod", accountId: hex("a"), commit, inputsText, planText: "Plan: 1 to delete\n[session-brief] delete\n", detailedText: "Plan: 1 to delete\n[session-brief] delete\n", now: new Date() })).toThrow(/forbidden/);
  });
});

function plan2() {
  return "Plan: 1 to update\n[session-brief] update\n";
}
