import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import * as Cloudflare from "@alchemy.run/cloudflare";
import {
  ADMISSION_DLQ,
  ADMISSION_QUEUE,
  ADMISSION_RESOURCES,
  COMPATIBILITY_DATE,
  FANOUT_DLQ,
  INBOX_AUTHORITY_MODE,
  INBOX_CANDIDATE_KEY_ROOT,
  SESSION_INGEST_RESOURCES,
  SESSION_INGEST_WORKER,
  admissionConsumerProps,
  assertAdmissionConsumer,
  assertFanoutConsumer,
  assertNoSecretLiterals,
  assertPrivateWorker,
  assertSessionIngestInputs,
  assertSessionIngestPlanContract,
  externalQueue,
  fanoutConsumerProps,
  secretsStoreSecret,
  resolvedAsyncAdmission,
  sessionIngestResources,
  sessionIngestVars,
  sessionIngestWorkerProps,
  type SessionIngestInputs,
} from "../session-ingest.ts";
import { assertCloudflareStage, cloudflareMode } from "../alchemy.cloudflare.run.ts";
import { ingestSettings, type IngestWorkerEnv } from "../../../src/session-ingest/worker.ts";
import { NeonExitRefusal } from "../../../src/shared/neon-exit.ts";

const refuse = (why: string): never => {
  throw new Error(why);
};
const hex = (c: string) => c.repeat(32);
const KEY_ARN = "arn:aws:kms:us-east-1:941433935742:key/12345678-90ab-cdef-1234-567890abcdef";
const inputs: SessionIngestInputs = { accountId: hex("a"), hyperdriveId: hex("b"), secretsStoreId: hex("c"), kmsKeyArn: KEY_ARN };
const authority = {
  mode: INBOX_AUTHORITY_MODE,
  keyRoot: "96bulls/inbox-authority/v1",
  expectedGeneration: "1".repeat(64),
  expectedControlEpoch: "2".repeat(64),
  expectedAuthEpoch: "3".repeat(64),
  expectedSourceIdentitySha256: "4".repeat(64),
} as const;
const bindings = { HYPERDRIVE: Cloudflare.Hyperdrive.lookup("HYPERDRIVE", { hyperdriveId: hex("b") }), INGEST_FANOUT: { Type: "Cloudflare.Queues.Queue", queueName: "96bulls-session-ingest-fanout" } };
const props = (p: SessionIngestInputs = inputs) => sessionIngestWorkerProps(p, bindings) as unknown as Record<string, unknown> & { env: Record<string, unknown> };
const src = (rel: string) => readFileSync(fileURLToPath(new URL(rel, import.meta.url)), "utf8");

describe("session ingest worker: reachable only through relayhistory's service binding", () => {
  it("the declared Worker passes the private-worker invariant", () => {
    expect(() => assertPrivateWorker(props(), refuse)).not.toThrow();
    const p = props();
    expect(p.name).toBe(SESSION_INGEST_WORKER);
    expect(p.workersDev).toBe(false);
    expect(p.routes).toEqual([]);
    expect(p.domain).toBeNull();
    expect(p.main).toBe("src/session-ingest/worker.ts");
  });

  it("refuses any public surface", () => {
    const bad: Record<string, unknown>[] = [
      { workersDev: true },
      { workersDev: undefined },
      { workersDev: { enabled: false, previewsEnabled: true } },
      { routes: [{ pattern: "ingest.agentrelay.com/*" }] },
      { routes: undefined },
      { domain: "ingest.agentrelay.com" },
      { domain: undefined },
      { url: true },
      { preview: { of: "x" } },
      { access: { policies: [] } },
      { crons: ["* * * * *"] },
      { name: "other" },
    ];
    for (const b of bad) expect(() => assertPrivateWorker({ ...props(), ...b }, refuse)).toThrow();
  });

  it("needs nodejs_compat, the pinned date and smart placement", () => {
    const p = props();
    expect(p.compatibility).toEqual({ date: COMPATIBILITY_DATE, flags: ["nodejs_compat"] });
    expect(p.placement).toEqual({ mode: "smart" });
    expect(() => assertPrivateWorker({ ...p, compatibility: { date: COMPATIBILITY_DATE, flags: [] } }, refuse)).toThrow(/nodejs_compat/);
    expect(() => assertPrivateWorker({ ...p, compatibility: { date: COMPATIBILITY_DATE, flags: ["nodejs_compat", "no_nodejs_compat"] } }, refuse)).toThrow(/nodejs_compat/);
    expect(() => assertPrivateWorker({ ...p, compatibility: { date: "2027-01-01", flags: ["nodejs_compat"] } }, refuse)).toThrow(/pinned/);
    expect(() => assertPrivateWorker({ ...p, placement: undefined }, refuse)).toThrow(/smart/);
  });

  it("the stage applies the invariants to exactly the props it deploys", () => {
    const stage = src("../session-ingest.ts");
    expect(stage).toContain("assertPrivateWorker(props, refuse);");
    expect(stage).toContain("assertNoSecretLiterals(props.env, refuse);");
    expect(stage).toContain('Cloudflare.Worker("session-ingest", props as never)');
    expect(stage).not.toMatch(/Cloudflare\.Workers?\.(Route|Domain|Subdomain|CustomDomain)\(/);
  });
});

describe("session ingest worker: bindings and no credential in state", () => {
  it("vars carry ids and switches only", () => {
    expect(sessionIngestVars(inputs)).toEqual({
      INGEST_S3_BUCKET: "96bulls-aws-sessions-prod-transcripts-941433935742",
      INGEST_S3_REGION: "us-east-1",
      INGEST_KEY_ROOT: "96bulls",
      INGEST_KMS_KEY_ARN: KEY_ARN,
      INGEST_MAX_PER_USER: "4",
      INGEST_MAX_PER_ORG: "32",
      INGEST_INLINE_MAX_SESSIONS: "40",
      INGEST_REPLICA_MODE: "off",
    });
  });

  it("selects the inbox authority only with the exact all-or-nothing managed vars", () => {
    expect(sessionIngestVars({ ...inputs, authority })).toEqual({
      ...sessionIngestVars(inputs),
      INGEST_AUTHORITY_MODE: "authority-v1",
      INGEST_AUTHORITY_KEY_ROOT: "96bulls/inbox-authority/v1",
      INGEST_AUTHORITY_EXPECT_GENERATION: "1".repeat(64),
      INGEST_AUTHORITY_EXPECT_CONTROL_EPOCH: "2".repeat(64),
      INGEST_AUTHORITY_EXPECT_AUTH_EPOCH: "3".repeat(64),
      INGEST_AUTHORITY_EXPECT_SOURCE_IDENTITY: "4".repeat(64),
    });
    expect(Object.keys(sessionIngestVars(inputs)).filter((key) => key.startsWith("INGEST_AUTHORITY_"))).toEqual([]);
    expect(() => assertNoSecretLiterals(props({ ...inputs, authority }).env, refuse)).not.toThrow();
  });

  it("S3 credentials are Secrets Store bindings, never values", () => {
    const env = props().env;
    expect(env.S3_ACCESS_KEY_ID).toEqual(secretsStoreSecret(hex("c"), "96bulls-session-ingest-s3-access-key-id"));
    expect(env.S3_SECRET_ACCESS_KEY).toEqual(secretsStoreSecret(hex("c"), "96bulls-session-ingest-s3-secret-access-key"));
    // The identity signing key shared with relayhistory (review Medium: unsigned identity).
    expect(env.INGEST_SIGNING_KEY).toEqual(secretsStoreSecret(hex("c"), "96bulls-session-ingest-signing-key"));
    expect(() => assertNoSecretLiterals(env, refuse)).not.toThrow();
    for (const bad of [
      { S3_SECRET_ACCESS_KEY: "wJalrXUtnFEMI/K7MDENG" },
      { SUPABASE_PASSWORD: "x" },
      { DATABASE_URL: "postgresql://u:p@host/db" },
      { INGEST_NOTE: "postgresql://u:p@host/db" },
      { S3_ACCESS_KEY_ID: { Type: "Cloudflare.SecretsStore.Secret", value: "AKIA" } },
    ])
      expect(() => assertNoSecretLiterals({ ...env, ...bad }, refuse)).toThrow();
    const { S3_ACCESS_KEY_ID: _, ...missing } = env;
    expect(() => assertNoSecretLiterals(missing, refuse)).toThrow(/missing/);
    const selected = props({ ...inputs, authority }).env;
    for (const key of [
      "INGEST_AUTHORITY_MODE",
      "INGEST_AUTHORITY_KEY_ROOT",
      "INGEST_AUTHORITY_EXPECT_GENERATION",
      "INGEST_AUTHORITY_EXPECT_CONTROL_EPOCH",
      "INGEST_AUTHORITY_EXPECT_AUTH_EPOCH",
      "INGEST_AUTHORITY_EXPECT_SOURCE_IDENTITY",
    ]) {
      const partial = { ...selected };
      delete partial[key];
      expect(() => assertNoSecretLiterals(partial, refuse)).toThrow(/missing/);
    }
    expect(() => assertNoSecretLiterals({ ...env, INGEST_AUTHORITY_STALE_BINDING: "x" }, refuse)).toThrow(/not a known/);
    expect(() => assertNoSecretLiterals({ ...env, INGEST_AUTHORITY_MODE: "off" }, refuse)).toThrow(/missing/);
  });

  it("Hyperdrive is a lookup by id; the replica queue is bound only in queue mode, producer only", () => {
    expect(Cloudflare.Hyperdrive.isHyperdriveConnection(props().env.HYPERDRIVE)).toBe(true);
    expect(props().env.NEON_REPLICA).toBeUndefined();
    const q = props({ ...inputs, replicaMode: "queue", neonReplicaQueue: "relayhistory-neon-replica" }).env.NEON_REPLICA;
    expect(q).toEqual(externalQueue("relayhistory-neon-replica"));
  });

  it("the external descriptors are classified as Alchemy's Worker classifies the real resources", () => {
    expect(Cloudflare.Queues.isQueue(externalQueue("relayhistory-neon-replica"))).toBe(true);
    expect(Cloudflare.SecretsStore.isSecret(secretsStoreSecret(hex("c"), "s"))).toBe(true);
    // And the installed classifier emits the wire binding from exactly the fields they carry.
    const classifier = src("../../../node_modules/@alchemy.run/cloudflare/src/Workers/WorkerAsyncBindings.ts");
    expect(classifier).toMatch(/isSecret\(binding\)\) \{\s*return \{\s*type: "secrets_store_secret",\s*name: bindingName,\s*secretName: binding\.secretName,\s*storeId: binding\.storeId,/);
    expect(classifier).toMatch(/type: "queue",\s*name: bindingName,\s*queueName: binding\.queueName,/);
  });
});

describe("session ingest worker: fan-out queue consumer", () => {
  it("this Worker consumes, one message per batch, 10 retries, then the DLQ", () => {
    const c = fanoutConsumerProps("qid");
    expect(c).toEqual({ queueId: "qid", scriptName: SESSION_INGEST_WORKER, deadLetterQueue: FANOUT_DLQ, settings: { batchSize: 1, maxRetries: 10 } });
    expect(() => assertFanoutConsumer(c, refuse)).not.toThrow();
    for (const bad of [{ scriptName: "other" }, { deadLetterQueue: undefined }, { settings: { batchSize: 10, maxRetries: 10 } }, { settings: { batchSize: 1, maxRetries: 3 } }])
      expect(() => assertFanoutConsumer({ ...c, ...bad }, refuse)).toThrow();
  });

  it("the stage declares the consumer, ordered after the Worker and the DLQ", () => {
    const stage = src("../session-ingest.ts");
    expect(stage).toContain('Cloudflare.Queues.Consumer("session-ingest-fanout-consumer", {');
    expect(stage).toContain("assertFanoutConsumer(consumer, refuse);");
    expect(stage).toMatch(/scriptName: \(worker as unknown as \{ workerName: string \}\)\.workerName,\s*deadLetterQueue: dlq\.queueName,/);
  });
});

describe("session ingest worker: inputs", () => {
  it("accepts the documented shapes and refuses everything else", () => {
    assertSessionIngestInputs(inputs, refuse);
    assertSessionIngestInputs({ ...inputs, maxPerUser: 2, maxPerOrg: 8, inlineMaxSessions: 48, replicaMode: "queue", neonReplicaQueue: "relayhistory-neon-replica" }, refuse);
    assertSessionIngestInputs({ ...inputs, authority }, refuse);
    for (const bad of [
      null,
      [],
      { ...inputs, extra: 1 },
      { ...inputs, accountId: "ABC" },
      { ...inputs, hyperdriveId: undefined },
      { ...inputs, secretsStoreId: hex("g") },
      { ...inputs, kmsKeyArn: "alias/x" },
      { ...inputs, inlineMaxSessions: 49 },
      { ...inputs, maxPerUser: 0 },
      { ...inputs, maxPerUser: 10, maxPerOrg: 5 },
      { ...inputs, replicaMode: "dual" },
      { ...inputs, replicaMode: "queue" },
      { ...inputs, neonReplicaQueue: "relayhistory-neon-replica" },
      { ...inputs, replicaMode: "queue", neonReplicaQueue: "some-other-queue" },
      { ...inputs, authority: null },
      { ...inputs, authority: { ...authority, extra: true } },
      { ...inputs, authority: { ...authority, mode: "off" } },
      { ...inputs, authority: { ...authority, expectedAuthEpoch: undefined } },
      { ...inputs, authority: { ...authority, expectedGeneration: "A".repeat(64) } },
      { ...inputs, authority: { ...authority, keyRoot: "other/inbox-authority/v1" } },
      { ...inputs, authority: { ...authority, keyRoot: `${INBOX_CANDIDATE_KEY_ROOT}` } },
      { ...inputs, authority: { ...authority, keyRoot: "96bulls/upload-inbox" } },
      { ...inputs, authority: { ...authority, keyRoot: "96bulls//inbox-authority" } },
      { ...inputs, authority: { ...authority, keyRoot: "96bulls/inbox-authority/../escape" } },
    ])
      expect(() => assertSessionIngestInputs(bad, refuse)).toThrow();
    expect(() => sessionIngestVars({ ...inputs, authority: { ...authority, expectedControlEpoch: "bad" } as never })).toThrow(/expectedControlEpoch/);
  });

  it("C3 privacyAuthority: supabase only with frozen-supabase custody in buffered mode, rendered as INGEST_PRIVACY_AUTHORITY", () => {
    const frozen = { ...inputs, acceptanceMode: "buffered" as const, expectedGeneration: "f".repeat(64), inlineMaxSessions: 0, custodySource: "frozen-supabase" as const };
    assertSessionIngestInputs({ ...frozen, privacyAuthority: "supabase" }, refuse);
    expect(sessionIngestVars({ ...frozen, privacyAuthority: "supabase" })).toMatchObject({ INGEST_CUSTODY_SOURCE: "frozen-supabase", INGEST_PRIVACY_AUTHORITY: "supabase" });
    expect(sessionIngestVars(frozen)).not.toHaveProperty("INGEST_PRIVACY_AUTHORITY");
    for (const bad of [{ ...frozen, privacyAuthority: "neon" }, { ...frozen, custodySource: "shadow", privacyAuthority: "supabase" }, { ...inputs, privacyAuthority: "supabase" }])
      expect(() => assertSessionIngestInputs(bad, refuse)).toThrow();
  });

  it("the stage refuses another stage, destroy, another account and a deploy without approval", () => {
    const env = { CLOUDFLARE_ACCOUNT_ID: hex("a") };
    expect(() => assertCloudflareStage("cf-sessions-prod", "plan", { sessionIngest: inputs }, env)).not.toThrow();
    expect(() => assertCloudflareStage("cf-other", "plan", { sessionIngest: inputs }, env)).toThrow(/only/);
    expect(() => assertCloudflareStage("cf-sessions-prod", "destroy", { sessionIngest: inputs }, env)).toThrow(/never destroyed/);
    expect(() => assertCloudflareStage("cf-sessions-prod", "plan", { sessionIngest: inputs }, { CLOUDFLARE_ACCOUNT_ID: hex("d") })).toThrow(/cannot cross accounts/);
    expect(() => assertCloudflareStage("cf-sessions-prod", "plan", { sessionIngest: inputs, eks: {} }, env)).toThrow(/not a known setting/);
    expect(() => assertCloudflareStage("cf-sessions-prod", "deploy", { sessionIngest: inputs }, env)).toThrow(/approval/);
    expect(cloudflareMode(["alchemy", "destroy"])).toBe("destroy");
    expect(cloudflareMode(["alchemy", "plan"])).toBe("plan");
  });
});

describe("session ingest worker: deploy plan contract", () => {
  const plan = (actions: Record<string, string>, resources: readonly string[] = SESSION_INGEST_RESOURCES, extra = "") => {
    const counts = new Map<string, number>();
    for (const [r, a] of Object.entries(actions)) if (resources.includes(r) && a !== "noop") counts.set(a, (counts.get(a) ?? 0) + 1);
    const summary = counts.size ? `Plan: ${[...counts].map(([a, c]) => `${c} to ${a}`).join(", ")}` : "Plan: 0 changes";
    return `${summary}\n${resources.map((r) => `[${r}] ${actions[r] ?? "noop"}`).join("\n")}\n${extra}`;
  };
  const check = (compact: string, detailed = compact.split("\n").map((l) => (l.startsWith("[") ? `  ${l}` : l)).join("\n")) =>
    assertSessionIngestPlanContract(compact, detailed);
  const creates = Object.fromEntries(SESSION_INGEST_RESOURCES.map((r) => [r, "create"]));

  it("rollout creates, update changes in place, post-apply changes nothing", () => {
    expect(check(plan(creates))).toBe("session ingest rollout");
    expect(check(plan({ "session-ingest-fanout-consumer": "create" }))).toBe("session ingest rollout");
    expect(check(plan({ "session-ingest": "update" }))).toBe("session ingest update");
    expect(check(plan({}))).toBe("post-apply");
  });

  it("refuses replacement, deletion, mixed creates and updates, and a partial or foreign resource list", () => {
    for (const a of ["replace", "delete"]) expect(() => check(plan({ "session-ingest": a }))).toThrow(/forbidden action/);
    expect(() => check(plan({ "session-ingest": "update", "session-ingest-fanout-consumer": "create" }))).toThrow(/neither a rollout/);
    expect(() => check(plan(creates, SESSION_INGEST_RESOURCES.filter((r) => r !== "session-ingest-fanout-consumer")))).toThrow(/missing resource session-ingest-fanout-consumer/);
    expect(() => check(plan(creates, [...SESSION_INGEST_RESOURCES, "session-ingest-route"]))).toThrow(/unexpected resource session-ingest-route/);
    expect(() => check(plan(creates), plan({ "session-ingest": "update" }))).toThrow(/summaries differ|disagree/);
    expect(() => check(plan({ "session-ingest": "update" }).replace("Plan: 1 to update", "Plan: 2 to update"))).toThrow(/summary/);
  });

  it("async admission: the three admission resources all-or-none, only when declared", () => {
    const all = sessionIngestResources(true);
    expect(all).toEqual([...SESSION_INGEST_RESOURCES, ...ADMISSION_RESOURCES]);
    const allCreates = Object.fromEntries(all.map((r) => [r, "create"]));
    const checkWith = (compact: string, mode: "on" | "drain" = "drain") => assertSessionIngestPlanContract(compact, compact.split("\n").map((l) => (l.startsWith("[") ? `  ${l}` : l)).join("\n"), mode);
    expect(checkWith(plan(allCreates, all))).toBe("session ingest rollout");
    // An existing stage adding admission: three creates; the Worker binding update is its own phase.
    expect(checkWith(plan(Object.fromEntries(ADMISSION_RESOURCES.map((r) => [r, "create"])), all))).toBe("session ingest rollout");
    expect(checkWith(plan({ "session-ingest": "update" }, all))).toBe("session ingest update");
    expect(() => check(plan(allCreates, all))).toThrow(/unexpected resource session-ingest-admission/);
    expect(() => checkWith(plan(allCreates, all.filter((r) => r !== "session-ingest-admission-consumer")))).toThrow(/missing resource session-ingest-admission-consumer/);
    expect(() => checkWith(plan(creates))).toThrow(/missing resource session-ingest-admission/);
    expect(() => checkWith(plan({ "session-ingest-admission": "delete" }, all))).toThrow(/forbidden action/);
    // Adding admission to a deployed stage: the three creates + the Worker's in-place binding update.
    const addition = { ...Object.fromEntries(ADMISSION_RESOURCES.map((r) => [r, "create"])), "session-ingest": "update" };
    expect(checkWith(plan(addition, all))).toBe("async admission addition");
    // Retention convergence on existing admission queues is an in-place update.
    expect(checkWith(plan({ "session-ingest-admission": "update", "session-ingest-admission-dlq": "update" }, all))).toBe("session ingest update");
    expect(() => checkWith(plan({ "session-ingest-admission": "update", "session-ingest-admission-consumer": "create" }, all))).toThrow(/neither a rollout/);
    expect(() => check(plan({ "session-ingest": "update", "session-ingest-fanout-consumer": "create" }))).toThrow(/neither a rollout/);
    for (const extra of [{ "session-ingest-fanout": "update" }, { "session-ingest-fanout-consumer": "update" }, { "session-ingest-admission-consumer": "update" }])
      expect(() => checkWith(plan({ ...addition, ...extra }, all))).toThrow(/neither a rollout/);
    expect(() => checkWith(plan({ "session-ingest-admission": "create", "session-ingest": "update" }, all))).toThrow(/neither a rollout/);
  });

  it("the admission resources follow the RESOLVED mode: on/drain need all three, off needs none", () => {
    const all = sessionIngestResources(true);
    const detailed = (compact: string) => compact.split("\n").map((l) => (l.startsWith("[") ? `  ${l}` : l)).join("\n");
    const as = (compact: string, mode: "on" | "drain" | "off") => assertSessionIngestPlanContract(compact, detailed(compact), mode);
    const allCreates = Object.fromEntries(all.map((r) => [r, "create"]));
    for (const mode of ["on", "drain"] as const) {
      expect(as(plan(allCreates, all), mode)).toBe("session ingest rollout");
      expect(() => as(plan(creates), mode)).toThrow(/missing resource session-ingest-admission/);
    }
    // "off" (and the default) is exactly the four-resource contract.
    expect(as(plan(creates), "off")).toBe("session ingest rollout");
    expect(assertSessionIngestPlanContract(plan(creates), detailed(plan(creates)))).toBe("session ingest rollout");
    expect(() => as(plan(allCreates, all), "off")).toThrow(/unexpected resource session-ingest-admission/);
    expect(() => assertSessionIngestPlanContract(plan(creates), detailed(plan(creates)), "yes" as never)).toThrow(/unknown async admission mode/);
    expect(() => assertSessionIngestPlanContract(plan(creates), detailed(plan(creates)), true as never)).toThrow(/unknown async admission mode/);
  });

  it("an already-deployed Worker gains admission with drain only; on is refused there, allowed on a fresh rollout", () => {
    const all = sessionIngestResources(true);
    const detailed = (compact: string) => compact.split("\n").map((l) => (l.startsWith("[") ? `  ${l}` : l)).join("\n");
    const as = (compact: string, mode: "on" | "drain") => assertSessionIngestPlanContract(compact, detailed(compact), mode);
    const addition = { ...Object.fromEntries(ADMISSION_RESOURCES.map((r) => [r, "create"])), "session-ingest": "update" };
    // Addition phase: drain ok, on refused with the reason and the next step.
    expect(as(plan(addition, all), "drain")).toBe("async admission addition");
    expect(() => as(plan(addition, all), "on")).toThrow(/plan contract refused: async admission addition to a deployed Worker must be captured with asyncAdmission "drain" first; let drain reach 100% of Worker versions.*Nothing was changed/);
    // Fresh rollout (nothing deployed): on directly.
    expect(as(plan(Object.fromEntries(all.map((r) => [r, "create"])), all), "on")).toBe("session ingest rollout");
    // After drain reached 100%: the var-only flip to on is an in-place update.
    expect(as(plan({ "session-ingest": "update" }, all), "on")).toBe("session ingest update");
    expect(as(plan({}, all), "on")).toBe("post-apply");
  });

  it("refuses a detailed plan that shows a credential (a secret value in state)", () => {
    const ok = plan({ "session-ingest": "update" });
    for (const leak of ['  "type": "secret_text"', "  AKIAABCDEFGHIJKLMNOP", '  "x": "postgresql://bulls96_session_ingest:pw@pooler/postgres"', "  -----BEGIN RSA PRIVATE KEY-----"])
      expect(() => check(ok, `${ok}${leak}\n`)).toThrow(/credential/);
  });
});

describe("session ingest: S3-first async admission (stack default on for buffered, off otherwise)", () => {
  const buffered: SessionIngestInputs = { ...inputs, acceptanceMode: "buffered", expectedGeneration: "a".repeat(64), inlineMaxSessions: 0 };
  const off: SessionIngestInputs = { ...buffered, asyncAdmission: "off" };
  const admissionQueue = { Type: "Cloudflare.Queues.Queue", queueName: ADMISSION_QUEUE };
  const withQueue = { ...bindings, INGEST_ADMISSION: admissionQueue };
  /** The props as rendered before async admission existed: the same render minus its two admission keys. */
  const withoutAdmissionKeys = (p: { env: Record<string, unknown> }) => {
    const { INGEST_ADMISSION: _q, INGEST_ASYNC_ADMISSION: _v, ...env } = p.env;
    return { ...p, env };
  };

  it("resolves: explicit wins; omitted is on for buffered and off for canonical", () => {
    expect(resolvedAsyncAdmission(buffered)).toBe("on");
    for (const mode of ["on", "drain", "off"] as const) expect(resolvedAsyncAdmission({ ...buffered, asyncAdmission: mode })).toBe(mode);
    expect(resolvedAsyncAdmission(inputs)).toBe("off");
    expect(resolvedAsyncAdmission({ ...inputs, acceptanceMode: "canonical" })).toBe("off");
    expect(resolvedAsyncAdmission({ ...inputs, asyncAdmission: "off" })).toBe("off");
    expect(() => resolvedAsyncAdmission({ ...buffered, asyncAdmission: "yes" })).toThrow(/asyncAdmission/);
  });

  it("omitted on a buffered stage = on: the var, the INGEST_ADMISSION binding and the three resources", () => {
    expect(() => assertSessionIngestInputs(buffered, refuse)).not.toThrow();
    expect(sessionIngestVars(buffered).INGEST_ASYNC_ADMISSION).toBe("on");
    const p = sessionIngestWorkerProps(buffered, withQueue) as unknown as { env: Record<string, unknown> };
    expect(p.env.INGEST_ASYNC_ADMISSION).toBe("on");
    expect(p.env.INGEST_ADMISSION).toBe(admissionQueue);
    expect(JSON.stringify(p)).toBe(JSON.stringify(sessionIngestWorkerProps({ ...buffered, asyncAdmission: "on" }, withQueue)));
    // The binding is required: an omitted-on buffered stage without the admission queue refuses.
    expect(() => sessionIngestWorkerProps(buffered, bindings)).toThrow(/INGEST_ADMISSION/);
    expect(sessionIngestResources(resolvedAsyncAdmission(buffered) !== "off")).toEqual([...SESSION_INGEST_RESOURCES, ...ADMISSION_RESOURCES]);
  });

  it("explicit off on a buffered stage: vars, props and the resource list are byte-identical to before", () => {
    expect(() => assertSessionIngestInputs(off, refuse)).not.toThrow();
    expect(Object.keys(sessionIngestVars(off))).not.toContain("INGEST_ASYNC_ADMISSION");
    expect(Object.keys(props(off).env)).not.toContain("INGEST_ADMISSION");
    // Byte-identical: the "off" render equals the "on" render with exactly its two admission keys removed
    // (the pre-default omitted render), key order included.
    const on = sessionIngestWorkerProps({ ...buffered, asyncAdmission: "on" }, withQueue) as unknown as { env: Record<string, unknown> };
    expect(JSON.stringify(props(off))).toBe(JSON.stringify(withoutAdmissionKeys(on)));
    const { INGEST_ASYNC_ADMISSION: _v, ...onVars } = sessionIngestVars({ ...buffered, asyncAdmission: "on" });
    expect(JSON.stringify(sessionIngestVars(off))).toBe(JSON.stringify(onVars));
    expect(() => sessionIngestWorkerProps(off, withQueue)).toThrow(/INGEST_ADMISSION/);
    expect(sessionIngestResources(resolvedAsyncAdmission(off) !== "off")).toBe(SESSION_INGEST_RESOURCES);
    expect(sessionIngestResources()).toBe(SESSION_INGEST_RESOURCES);
    expect(sessionIngestResources(false)).toBe(SESSION_INGEST_RESOURCES);
  });

  it("canonical stage, omitted or off: nothing (no var, no binding, four resources), byte-identical", () => {
    for (const p of [inputs, { ...inputs, acceptanceMode: "canonical" as const }, { ...inputs, asyncAdmission: "off" as const }]) {
      expect(() => assertSessionIngestInputs(p, refuse)).not.toThrow();
      expect(Object.keys(sessionIngestVars(p))).not.toContain("INGEST_ASYNC_ADMISSION");
      expect(Object.keys(props(p).env)).not.toContain("INGEST_ADMISSION");
      expect(sessionIngestResources(resolvedAsyncAdmission(p) !== "off")).toBe(SESSION_INGEST_RESOURCES);
      expect(() => sessionIngestWorkerProps(p, withQueue)).toThrow(/INGEST_ADMISSION/);
    }
    expect(JSON.stringify(props({ ...inputs, asyncAdmission: "off" }))).toBe(JSON.stringify(props(inputs)));
  });

  it("is refused outside buffered acceptance (anything but off) and for any value but on/drain/off", () => {
    for (const bad of [
      { ...inputs, asyncAdmission: "on" },
      { ...inputs, asyncAdmission: "drain" },
      { ...inputs, acceptanceMode: "canonical", asyncAdmission: "on" },
      { ...buffered, asyncAdmission: "yes" },
      { ...buffered, asyncAdmission: "OFF" },
      { ...buffered, asyncAdmission: true },
      { ...buffered, asyncAdmission: null },
    ])
      expect(() => assertSessionIngestInputs(bad, refuse)).toThrow(
        'sessionIngest.asyncAdmission must be "on", "drain" or "off" in buffered mode (omitted = "on"), and omitted or "off" outside buffered mode',
      );
    for (const mode of ["on", "drain", "off"] as const) expect(() => assertSessionIngestInputs({ ...buffered, asyncAdmission: mode }, refuse)).not.toThrow();
  });

  it("on/drain: the var and the INGEST_ADMISSION binding, and the binding only together with the input", () => {
    for (const mode of ["on", "drain"] as const) {
      const p = sessionIngestWorkerProps({ ...buffered, asyncAdmission: mode }, { ...bindings, INGEST_ADMISSION: admissionQueue }) as unknown as { env: Record<string, unknown> };
      expect(p.env.INGEST_ASYNC_ADMISSION).toBe(mode);
      expect(p.env.INGEST_ADMISSION).toBe(admissionQueue);
      expect(p.env.INGEST_FANOUT).toBe(bindings.INGEST_FANOUT);
      expect(() => assertNoSecretLiterals(p.env, refuse)).not.toThrow();
    }
    expect(() => sessionIngestWorkerProps({ ...buffered, asyncAdmission: "on" }, bindings)).toThrow(/INGEST_ADMISSION/);
    expect(() => sessionIngestWorkerProps(off, { ...bindings, INGEST_ADMISSION: admissionQueue })).toThrow(/INGEST_ADMISSION/);
  });

  it("the consumer is this Worker, batch 1, 20 retries, concurrency 4, its own DLQ", () => {
    const c = admissionConsumerProps("qid");
    expect(c).toEqual({ queueId: "qid", scriptName: SESSION_INGEST_WORKER, deadLetterQueue: ADMISSION_DLQ, settings: { batchSize: 1, maxRetries: 20, maxConcurrency: 4 } });
    expect(() => assertAdmissionConsumer(c, refuse)).not.toThrow();
    expect(() => assertAdmissionConsumer({ ...c, deadLetterQueue: FANOUT_DLQ }, refuse)).toThrow(/dead-letter/);
    expect(() => assertAdmissionConsumer({ ...c, scriptName: "other" }, refuse)).toThrow(/ingest Worker/);
    expect(() => assertAdmissionConsumer({ ...c, settings: { ...c.settings, maxConcurrency: 64 } }, refuse)).toThrow(/settings/);
    expect(() => assertAdmissionConsumer({ ...c, settings: { ...c.settings, maxWaitTimeMs: 1 } as never }, refuse)).toThrow(/settings/);
  });

  it("the stage declares the admission queues with no pause prop, the fanout queues unchanged", () => {
    const s = src("../session-ingest.ts");
    expect(s).toContain('Cloudflare.Queues.Queue("session-ingest-admission-dlq", { name: ADMISSION_DLQ } as never)');
    expect(s).toContain('Cloudflare.Queues.Queue("session-ingest-admission", { name: ADMISSION_QUEUE } as never)');
    expect(s).toContain('Cloudflare.Queues.Queue("session-ingest-fanout", { name: FANOUT_QUEUE, ...(p.acceptanceMode === "buffered" ? { deliveryPaused: true } : {}) } as never)');
    expect(s).toContain("assertAdmissionConsumer(ac, refuse)");
    expect([ADMISSION_QUEUE, ADMISSION_DLQ]).toEqual(["96bulls-session-ingest-admission", "96bulls-session-ingest-admission-dlq"]);
  });
});

describe("session ingest: INGEST_SYNC_ADMISSION_FROZEN_RECEIPT (default off)", () => {
  // Explicit asyncAdmission "off" (a buffered stage now defaults to "on"): this block isolates the frozen-receipt var.
  const frozen: SessionIngestInputs = { ...inputs, acceptanceMode: "buffered", expectedGeneration: "a".repeat(64), inlineMaxSessions: 0, custodySource: "frozen-supabase", asyncAdmission: "off" };
  const admissionQueue = { Type: "Cloudflare.Queues.Queue", queueName: ADMISSION_QUEUE };

  it("absent: no var, and vars and props are byte-identical to a stack without the input", () => {
    for (const p of [inputs, frozen, { ...frozen, asyncAdmission: "on" as const }]) {
      const b = resolvedAsyncAdmission(p) !== "off" ? { ...bindings, INGEST_ADMISSION: admissionQueue } : bindings;
      expect(Object.keys(sessionIngestVars(p))).not.toContain("INGEST_SYNC_ADMISSION_FROZEN_RECEIPT");
      expect(JSON.stringify(sessionIngestWorkerProps(p, b))).toBe(JSON.stringify(sessionIngestWorkerProps({ ...p, syncAdmissionFrozenReceipt: undefined }, b)));
    }
    expect(JSON.stringify(sessionIngestVars(frozen))).toBe(JSON.stringify(sessionIngestVars({ ...frozen })));
  });

  it("on: exactly one more var, no new binding or resource", () => {
    const on = { ...frozen, syncAdmissionFrozenReceipt: "on" as const };
    expect(() => assertSessionIngestInputs(on, refuse)).not.toThrow();
    expect(() => assertSessionIngestInputs({ ...on, asyncAdmission: "drain" }, refuse)).not.toThrow();
    expect(sessionIngestVars(on)).toEqual({ ...sessionIngestVars(frozen), INGEST_SYNC_ADMISSION_FROZEN_RECEIPT: "on" });
    const p = props(on);
    expect(p.env.INGEST_SYNC_ADMISSION_FROZEN_RECEIPT).toBe("on");
    expect(Object.keys(p.env).sort()).toEqual([...Object.keys(props(frozen).env), "INGEST_SYNC_ADMISSION_FROZEN_RECEIPT"].sort());
    expect(p.env.INGEST_ADMISSION).toBeUndefined();
    expect(() => assertNoSecretLiterals(p.env, refuse)).not.toThrow();
    expect(() => assertPrivateWorker(p, refuse)).not.toThrow();
    // Together with async admission: both vars, and only async admission's binding.
    const both = sessionIngestWorkerProps({ ...on, asyncAdmission: "on" }, { ...bindings, INGEST_ADMISSION: admissionQueue }) as unknown as { env: Record<string, unknown> };
    expect(both.env.INGEST_ASYNC_ADMISSION).toBe("on");
    expect(both.env.INGEST_SYNC_ADMISSION_FROZEN_RECEIPT).toBe("on");
    expect(sessionIngestResources()).toBe(SESSION_INGEST_RESOURCES);
    const env = { CLOUDFLARE_ACCOUNT_ID: hex("a") };
    expect(() => assertCloudflareStage("cf-sessions-prod", "plan", { sessionIngest: on }, env)).not.toThrow();
  });

  it("is refused for any value but on, and outside buffered acceptance with frozen-supabase custody", () => {
    const { custodySource: _c, ...bufferedNoCustody } = frozen;
    for (const bad of [
      { ...frozen, syncAdmissionFrozenReceipt: "yes" },
      { ...frozen, syncAdmissionFrozenReceipt: "off" },
      { ...frozen, syncAdmissionFrozenReceipt: true },
      { ...frozen, syncAdmissionFrozenReceipt: "ON" },
      { ...frozen, syncAdmissionFrozenReceipt: null },
      { ...inputs, syncAdmissionFrozenReceipt: "on" },
      { ...inputs, acceptanceMode: "canonical", syncAdmissionFrozenReceipt: "on" },
      { ...bufferedNoCustody, syncAdmissionFrozenReceipt: "on" },
      { ...frozen, custodySource: "shadow", syncAdmissionFrozenReceipt: "on" },
    ])
      expect(() => assertSessionIngestInputs(bad, refuse)).toThrow(
        'sessionIngest.syncAdmissionFrozenReceipt must be "on", in buffered mode with custodySource frozen-supabase only (omit for off)',
      );
    const env = { CLOUDFLARE_ACCOUNT_ID: hex("a") };
    expect(() => assertCloudflareStage("cf-sessions-prod", "plan", { sessionIngest: { ...inputs, syncAdmissionFrozenReceipt: "on" } }, env)).toThrow(/syncAdmissionFrozenReceipt.*Nothing was changed/);
  });

  it("a var-only change plans as an in-place update of the Worker", () => {
    const lines = SESSION_INGEST_RESOURCES.map((r) => `  [${r}] ${r === "session-ingest" ? "update" : "noop"}`).join("\n");
    const text = `Plan: 1 to update\n${lines}\n`;
    expect(assertSessionIngestPlanContract(text, text)).toBe("session ingest update");
  });
});

describe("session ingest: INGEST_CLAIM_LOCK_BUSY (default off)", () => {
  // Explicit asyncAdmission "off" on the buffered shape: this block isolates the claim lock-busy var.
  const buffered: SessionIngestInputs = { ...inputs, acceptanceMode: "buffered", expectedGeneration: "a".repeat(64), inlineMaxSessions: 0, asyncAdmission: "off" };
  const canonical: SessionIngestInputs = { ...inputs, acceptanceMode: "canonical" };
  const admissionQueue = { Type: "Cloudflare.Queues.Queue", queueName: ADMISSION_QUEUE };

  it("omitted: no INGEST_CLAIM_LOCK_BUSY key at all, vars and props byte-identical to a stack without the input", () => {
    for (const p of [inputs, canonical, buffered, { ...buffered, asyncAdmission: "on" as const }]) {
      const b = resolvedAsyncAdmission(p) !== "off" ? { ...bindings, INGEST_ADMISSION: admissionQueue } : bindings;
      expect(Object.hasOwn(sessionIngestVars(p), "INGEST_CLAIM_LOCK_BUSY")).toBe(false);
      const env = (sessionIngestWorkerProps(p, b) as unknown as { env: Record<string, unknown> }).env;
      expect(Object.hasOwn(env, "INGEST_CLAIM_LOCK_BUSY")).toBe(false);
      expect(JSON.stringify(sessionIngestWorkerProps(p, b))).toBe(JSON.stringify(sessionIngestWorkerProps({ ...p, claimLockBusy: undefined }, b)));
    }
  });

  it('"on": exactly one more var in canonical and buffered acceptance, no new binding or resource', () => {
    const env = { CLOUDFLARE_ACCOUNT_ID: hex("a") };
    for (const base of [inputs, canonical, buffered]) {
      const on = { ...base, claimLockBusy: "on" as const };
      expect(() => assertSessionIngestInputs(on, refuse)).not.toThrow();
      expect(sessionIngestVars(on)).toEqual({ ...sessionIngestVars(base), INGEST_CLAIM_LOCK_BUSY: "on" });
      const p = props(on);
      expect(p.env.INGEST_CLAIM_LOCK_BUSY).toBe("on");
      expect(Object.keys(p.env).sort()).toEqual([...Object.keys(props(base).env), "INGEST_CLAIM_LOCK_BUSY"].sort());
      expect(p.env.INGEST_ADMISSION).toBeUndefined();
      expect(() => assertNoSecretLiterals(p.env, refuse)).not.toThrow();
      expect(() => assertPrivateWorker(p, refuse)).not.toThrow();
      expect(() => assertCloudflareStage("cf-sessions-prod", "plan", { sessionIngest: on }, env)).not.toThrow();
    }
    // Together with async admission on and the frozen receipt: every var, only async admission's binding.
    const all = { ...buffered, custodySource: "frozen-supabase" as const, syncAdmissionFrozenReceipt: "on" as const, asyncAdmission: "on" as const, claimLockBusy: "on" as const };
    expect(() => assertSessionIngestInputs(all, refuse)).not.toThrow();
    const both = sessionIngestWorkerProps(all, { ...bindings, INGEST_ADMISSION: admissionQueue }) as unknown as { env: Record<string, unknown> };
    expect(both.env.INGEST_ASYNC_ADMISSION).toBe("on");
    expect(both.env.INGEST_SYNC_ADMISSION_FROZEN_RECEIPT).toBe("on");
    expect(both.env.INGEST_CLAIM_LOCK_BUSY).toBe("on");
    expect(sessionIngestResources()).toBe(SESSION_INGEST_RESOURCES);
  });

  it('is refused for any value but "on"', () => {
    for (const base of [inputs, canonical, buffered])
      for (const value of ["off", "ON", "On", true, 1, "", null, "yes"])
        expect(() => assertSessionIngestInputs({ ...base, claimLockBusy: value }, refuse)).toThrow('sessionIngest.claimLockBusy must be "on" (omit for off)');
    const env = { CLOUDFLARE_ACCOUNT_ID: hex("a") };
    expect(() => assertCloudflareStage("cf-sessions-prod", "plan", { sessionIngest: { ...inputs, claimLockBusy: "off" } }, env)).toThrow(/claimLockBusy.*Nothing was changed/);
  });

  it("an unknown key next to it is still refused", () => {
    expect(() => assertSessionIngestInputs({ ...inputs, claimLockBusy: "on", claimLockBusyRetryAfter: 5 }, refuse)).toThrow("sessionIngest.claimLockBusyRetryAfter is not a known setting");
    expect(() => assertSessionIngestInputs({ ...inputs, INGEST_CLAIM_LOCK_BUSY: "on" }, refuse)).toThrow("sessionIngest.INGEST_CLAIM_LOCK_BUSY is not a known setting");
  });

  it("the var-only change (Worker updated in place, nothing else) plans as a session ingest update", () => {
    // Same shape as the frozen-receipt var-only test above, for the canonical and the admission-on resource lists.
    for (const withAdmission of [false, true]) {
      const resources = sessionIngestResources(withAdmission);
      const lines = resources.map((r) => `  [${r}] ${r === "session-ingest" ? "update" : "noop"}`).join("\n");
      const text = `Plan: 1 to update\n${lines}\n`;
      expect(assertSessionIngestPlanContract(text, text, withAdmission ? "on" : "off")).toBe("session ingest update");
    }
  });
});

describe("session ingest: INGEST_CLAIM_TIMEOUT_BUSY (default off)", () => {
  // Same shape as the INGEST_CLAIM_LOCK_BUSY block above; the two inputs are independent.
  const buffered: SessionIngestInputs = { ...inputs, acceptanceMode: "buffered", expectedGeneration: "a".repeat(64), inlineMaxSessions: 0, asyncAdmission: "off" };
  const canonical: SessionIngestInputs = { ...inputs, acceptanceMode: "canonical" };
  const admissionQueue = { Type: "Cloudflare.Queues.Queue", queueName: ADMISSION_QUEUE };

  it("omitted: no INGEST_CLAIM_TIMEOUT_BUSY key at all, vars and props byte-identical to a stack without the input", () => {
    for (const p of [inputs, canonical, buffered, { ...buffered, asyncAdmission: "on" as const }, { ...canonical, claimLockBusy: "on" as const }]) {
      const b = resolvedAsyncAdmission(p) !== "off" ? { ...bindings, INGEST_ADMISSION: admissionQueue } : bindings;
      expect(Object.hasOwn(sessionIngestVars(p), "INGEST_CLAIM_TIMEOUT_BUSY")).toBe(false);
      const env = (sessionIngestWorkerProps(p, b) as unknown as { env: Record<string, unknown> }).env;
      expect(Object.hasOwn(env, "INGEST_CLAIM_TIMEOUT_BUSY")).toBe(false);
      expect(JSON.stringify(sessionIngestWorkerProps(p, b))).toBe(JSON.stringify(sessionIngestWorkerProps({ ...p, claimTimeoutBusy: undefined }, b)));
    }
  });

  it('"on": exactly one more var in canonical and buffered acceptance, no new binding or resource, independent of claimLockBusy', () => {
    const env = { CLOUDFLARE_ACCOUNT_ID: hex("a") };
    for (const base of [inputs, canonical, buffered]) {
      const on = { ...base, claimTimeoutBusy: "on" as const };
      expect(() => assertSessionIngestInputs(on, refuse)).not.toThrow();
      expect(sessionIngestVars(on)).toEqual({ ...sessionIngestVars(base), INGEST_CLAIM_TIMEOUT_BUSY: "on" });
      expect(Object.hasOwn(sessionIngestVars(on), "INGEST_CLAIM_LOCK_BUSY")).toBe(false);
      const p = props(on);
      expect(p.env.INGEST_CLAIM_TIMEOUT_BUSY).toBe("on");
      expect(Object.keys(p.env).sort()).toEqual([...Object.keys(props(base).env), "INGEST_CLAIM_TIMEOUT_BUSY"].sort());
      expect(p.env.INGEST_ADMISSION).toBeUndefined();
      expect(() => assertNoSecretLiterals(p.env, refuse)).not.toThrow();
      expect(() => assertPrivateWorker(p, refuse)).not.toThrow();
      expect(() => assertCloudflareStage("cf-sessions-prod", "plan", { sessionIngest: on }, env)).not.toThrow();
    }
    // Together with every other var-only flag: each var once, only async admission's binding.
    const all = { ...buffered, custodySource: "frozen-supabase" as const, syncAdmissionFrozenReceipt: "on" as const, asyncAdmission: "on" as const, claimLockBusy: "on" as const, claimTimeoutBusy: "on" as const };
    expect(() => assertSessionIngestInputs(all, refuse)).not.toThrow();
    const both = sessionIngestWorkerProps(all, { ...bindings, INGEST_ADMISSION: admissionQueue }) as unknown as { env: Record<string, unknown> };
    expect(both.env.INGEST_ASYNC_ADMISSION).toBe("on");
    expect(both.env.INGEST_SYNC_ADMISSION_FROZEN_RECEIPT).toBe("on");
    expect(both.env.INGEST_CLAIM_LOCK_BUSY).toBe("on");
    expect(both.env.INGEST_CLAIM_TIMEOUT_BUSY).toBe("on");
    expect(sessionIngestResources()).toBe(SESSION_INGEST_RESOURCES);
  });

  it('is refused for any value but "on"', () => {
    for (const base of [inputs, canonical, buffered])
      for (const value of ["off", "ON", "On", true, 1, "", null, "yes"])
        expect(() => assertSessionIngestInputs({ ...base, claimTimeoutBusy: value }, refuse)).toThrow('sessionIngest.claimTimeoutBusy must be "on" (omit for off)');
    const env = { CLOUDFLARE_ACCOUNT_ID: hex("a") };
    expect(() => assertCloudflareStage("cf-sessions-prod", "plan", { sessionIngest: { ...inputs, claimTimeoutBusy: "off" } }, env)).toThrow(/claimTimeoutBusy.*Nothing was changed/);
  });

  it("an unknown key next to it is still refused", () => {
    expect(() => assertSessionIngestInputs({ ...inputs, claimTimeoutBusy: "on", claimTimeoutBusyRetryAfter: 5 }, refuse)).toThrow("sessionIngest.claimTimeoutBusyRetryAfter is not a known setting");
    expect(() => assertSessionIngestInputs({ ...inputs, INGEST_CLAIM_TIMEOUT_BUSY: "on" }, refuse)).toThrow("sessionIngest.INGEST_CLAIM_TIMEOUT_BUSY is not a known setting");
  });
});

describe("session ingest: SESSIONS_NEON_EXIT (neonExit, default off)", () => {
  // The only shape the Neon-exit Worker serves (worker.ts ingestSettings): buffered, frozen-supabase, replica off.
  const exit: SessionIngestInputs = {
    ...inputs, acceptanceMode: "buffered", expectedGeneration: "a".repeat(64), inlineMaxSessions: 0, replicaMode: "off",
    custodySource: "frozen-supabase", asyncAdmission: "off", neonExit: "on",
  };
  const { neonExit: _off, ...withoutExit } = exit;
  const admissionQueue = { Type: "Cloudflare.Queues.Queue", queueName: ADMISSION_QUEUE };
  const vars = () => sessionIngestVars(exit) as Record<string, string>;

  it("omitted: no SESSIONS_NEON_EXIT key at all; the default vars are unchanged", () => {
    expect(sessionIngestVars(inputs)).toEqual({
      INGEST_S3_BUCKET: "96bulls-aws-sessions-prod-transcripts-941433935742",
      INGEST_S3_REGION: "us-east-1",
      INGEST_KEY_ROOT: "96bulls",
      INGEST_KMS_KEY_ARN: KEY_ARN,
      INGEST_MAX_PER_USER: "4",
      INGEST_MAX_PER_ORG: "32",
      INGEST_INLINE_MAX_SESSIONS: "40",
      INGEST_REPLICA_MODE: "off",
    });
    const queue: SessionIngestInputs = { ...inputs, replicaMode: "queue", neonReplicaQueue: "relayhistory-neon-replica" };
    for (const p of [inputs, queue, withoutExit, { ...withoutExit, asyncAdmission: "on" as const }]) {
      const b = resolvedAsyncAdmission(p) !== "off" ? { ...bindings, INGEST_ADMISSION: admissionQueue } : bindings;
      expect(Object.hasOwn(sessionIngestVars(p), "SESSIONS_NEON_EXIT")).toBe(false);
      expect(Object.hasOwn((sessionIngestWorkerProps(p, b) as unknown as { env: Record<string, unknown> }).env, "SESSIONS_NEON_EXIT")).toBe(false);
      expect(JSON.stringify(sessionIngestWorkerProps(p, b))).toBe(JSON.stringify(sessionIngestWorkerProps({ ...p, neonExit: undefined }, b)));
    }
  });

  it('"on" with the accepted shape: exactly SESSIONS_NEON_EXIT "true" more, and no NEON_REPLICA binding', () => {
    expect(() => assertSessionIngestInputs(exit, refuse)).not.toThrow();
    expect(sessionIngestVars(exit)).toEqual({ ...sessionIngestVars(withoutExit), SESSIONS_NEON_EXIT: "true" });
    const p = props(exit);
    expect(p.env.SESSIONS_NEON_EXIT).toBe("true");
    expect(Object.hasOwn(p.env, "NEON_REPLICA")).toBe(false);
    expect(p.env.INGEST_REPLICA_MODE).toBe("off");
    expect(Object.keys(p.env).sort()).toEqual([...Object.keys(props(withoutExit).env), "SESSIONS_NEON_EXIT"].sort());
    expect(() => assertNoSecretLiterals(p.env, refuse)).not.toThrow();
    expect(() => assertPrivateWorker(p, refuse)).not.toThrow();
    expect(() => assertCloudflareStage("cf-sessions-prod", "plan", { sessionIngest: exit }, { CLOUDFLARE_ACCOUNT_ID: hex("a") })).not.toThrow();
    // replicaMode omitted is off too; async admission and the frozen receipt stay independent.
    const { replicaMode: _r, ...omittedReplica } = exit;
    expect(() => assertSessionIngestInputs(omittedReplica, refuse)).not.toThrow();
    const all = { ...exit, asyncAdmission: "on" as const, syncAdmissionFrozenReceipt: "on" as const, claimLockBusy: "on" as const };
    expect(() => assertSessionIngestInputs(all, refuse)).not.toThrow();
    const allEnv = (sessionIngestWorkerProps(all, { ...bindings, INGEST_ADMISSION: admissionQueue }) as unknown as { env: Record<string, unknown> }).env;
    expect(allEnv.SESSIONS_NEON_EXIT).toBe("true");
    expect(Object.hasOwn(allEnv, "NEON_REPLICA")).toBe(false);
  });

  it("refuses every configuration that still binds a Neon edge, naming the setting", () => {
    const cases: Array<[Record<string, unknown>, RegExp]> = [
      [{ ...exit, replicaMode: "queue", neonReplicaQueue: "relayhistory-neon-replica" }, /neonExit "on" refuses replicaMode/],
      [{ ...exit, replicaMode: "queue" }, /neonExit "on" refuses replicaMode/],
      [{ ...exit, neonReplicaQueue: "relayhistory-neon-replica" }, /neonExit "on" refuses neonReplicaQueue/],
      [{ ...exit, replicaMode: undefined, neonReplicaQueue: "relayhistory-neon-replica" }, /neonExit "on" refuses neonReplicaQueue/],
      [{ ...inputs, custodySource: "frozen-supabase", neonExit: "on" }, /neonExit "on" requires acceptanceMode "buffered"/],
      [{ ...inputs, acceptanceMode: "canonical", custodySource: "frozen-supabase", neonExit: "on" }, /neonExit "on" requires acceptanceMode "buffered"/],
      [{ ...inputs, neonExit: "on" }, /neonExit "on" requires acceptanceMode "buffered"/],
      [{ ...exit, custodySource: undefined }, /neonExit "on" requires custodySource "frozen-supabase"/],
      [{ ...exit, custodySource: "shadow" }, /neonExit "on" requires custodySource "frozen-supabase"/],
      [{ ...exit, custodySource: "relayhistory" }, /neonExit "on" requires custodySource "frozen-supabase"/],
    ];
    for (const [bad, why] of cases) {
      const shaped = Object.fromEntries(Object.entries(bad).filter(([, v]) => v !== undefined));
      expect(() => assertSessionIngestInputs(shaped, refuse)).toThrow(why);
      expect(() => assertCloudflareStage("cf-sessions-prod", "plan", { sessionIngest: shaped }, { CLOUDFLARE_ACCOUNT_ID: hex("a") })).toThrow(/neonExit.*Nothing was changed/);
    }
  });

  it('is refused for any value but "on"', () => {
    for (const value of ["true", true, "off", "ON", "On", 1, "", null, false])
      expect(() => assertSessionIngestInputs({ ...exit, neonExit: value }, refuse)).toThrow('sessionIngest.neonExit must be "on" (omit for off)');
    expect(() => assertSessionIngestInputs({ ...exit, SESSIONS_NEON_EXIT: "true" }, refuse)).toThrow("sessionIngest.SESSIONS_NEON_EXIT is not a known setting");
  });

  it("the Worker props never render NEON_REPLICA with neonExit set, even past the validator", () => {
    const unchecked = { ...exit, replicaMode: "queue", neonReplicaQueue: "relayhistory-neon-replica" } as SessionIngestInputs;
    expect(() => sessionIngestWorkerProps(unchecked, bindings)).toThrow('sessionIngest.neonExit "on" never renders the NEON_REPLICA binding');
  });

  it("stack and runtime agree: the rendered vars pass the Worker's own Neon-exit gate, and a Neon edge on them is refused", async () => {
    const runtime = (extra: Partial<IngestWorkerEnv> = {}): IngestWorkerEnv => ({
      ...vars(),
      HYPERDRIVE: { connectionString: "postgres://never:never@hyperdrive.invalid:5432/never" },
      INGEST_FANOUT: { send: async () => {}, sendBatch: async () => {} } as never,
      S3_ACCESS_KEY_ID: "k",
      S3_SECRET_ACCESS_KEY: "s",
      INGEST_SIGNING_KEY: "synthetic-signing-key",
      ...extra,
    });
    const settings = await ingestSettings(runtime());
    expect(settings).not.toBeNull();
    expect(settings?.replicaMode).toBe("off");
    expect(settings?.acceptanceMode).toBe("buffered");
    // Without the stack's own refusals, these would ship a Worker that refuses every request.
    for (const extra of [
      { NEON_REPLICA: { send: async () => {}, sendBatch: async () => {} } as never },
      { INGEST_REPLICA_MODE: "queue" },
      { INGEST_CUSTODY_SOURCE: "shadow" },
      { INGEST_ACCEPTANCE_MODE: "canonical" },
    ])
      await expect(ingestSettings(runtime(extra))).rejects.toBeInstanceOf(NeonExitRefusal);
  });
});

describe("session ingest: INGEST_UPLOAD_CONSENT (default off)", () => {
  // Same shape as the INGEST_CLAIM_TIMEOUT_BUSY block: a var-only flag, no binding or resource.
  const buffered: SessionIngestInputs = { ...inputs, acceptanceMode: "buffered", expectedGeneration: "a".repeat(64), inlineMaxSessions: 0, asyncAdmission: "off" };
  const canonical: SessionIngestInputs = { ...inputs, acceptanceMode: "canonical" };

  it("omitted: the var set is exactly main's (pinned), with no INGEST_UPLOAD_CONSENT", () => {
    // Pinned from origin/main before this input existed, so the comparison is not the PR against itself.
    const base = ["INGEST_INLINE_MAX_SESSIONS", "INGEST_KEY_ROOT", "INGEST_KMS_KEY_ARN", "INGEST_MAX_PER_ORG", "INGEST_MAX_PER_USER", "INGEST_REPLICA_MODE", "INGEST_S3_BUCKET", "INGEST_S3_REGION"];
    expect(Object.keys(sessionIngestVars(inputs)).sort()).toEqual(base);
    expect(Object.keys(sessionIngestVars(canonical)).sort()).toEqual(base);
    expect(Object.keys(sessionIngestVars(buffered)).sort()).toEqual(["INGEST_ACCEPTANCE_MODE", "INGEST_EXPECT_GENERATION", ...base].sort());
  });

  it("omitted: no INGEST_UPLOAD_CONSENT key at all, vars and props byte-identical to a stack without the input", () => {
    for (const p of [inputs, canonical, buffered]) {
      expect(Object.hasOwn(sessionIngestVars(p), "INGEST_UPLOAD_CONSENT")).toBe(false);
      expect(JSON.stringify(sessionIngestWorkerProps(p, bindings))).toBe(JSON.stringify(sessionIngestWorkerProps({ ...p, uploadConsent: undefined }, bindings)));
    }
  });

  it("report / hold-only / enforce: exactly one more var, no new binding or resource", () => {
    const env = { CLOUDFLARE_ACCOUNT_ID: hex("a") };
    for (const mode of ["report", "hold-only", "enforce"] as const)
      for (const base of [inputs, canonical, buffered]) {
        const on = { ...base, uploadConsent: mode };
        expect(() => assertSessionIngestInputs(on, refuse)).not.toThrow();
        expect(sessionIngestVars(on)).toEqual({ ...sessionIngestVars(base), INGEST_UPLOAD_CONSENT: mode });
        const p = props(on);
        expect(p.env.INGEST_UPLOAD_CONSENT).toBe(mode);
        expect(Object.keys(p.env).sort()).toEqual([...Object.keys(props(base).env), "INGEST_UPLOAD_CONSENT"].sort());
        expect(() => assertNoSecretLiterals(p.env, refuse)).not.toThrow();
        expect(() => assertPrivateWorker(p, refuse)).not.toThrow();
        expect(() => assertCloudflareStage("cf-sessions-prod", "plan", { sessionIngest: on }, env)).not.toThrow();
      }
    expect(sessionIngestResources()).toBe(SESSION_INGEST_RESOURCES);
  });

  it("composes with neonExit: both vars render, each once", () => {
    const exit: SessionIngestInputs = { ...buffered, custodySource: "frozen-supabase", neonExit: "on", uploadConsent: "report" };
    expect(() => assertSessionIngestInputs(exit, refuse)).not.toThrow();
    const vars = sessionIngestVars(exit);
    expect([vars.INGEST_UPLOAD_CONSENT, vars.SESSIONS_NEON_EXIT]).toEqual(["report", "true"]);
  });

  it("is refused for any other value (off is spelled by omitting it)", () => {
    for (const value of ["off", "on", "Report", "true", true, 1, "", null])
      expect(() => assertSessionIngestInputs({ ...inputs, uploadConsent: value }, refuse)).toThrow('sessionIngest.uploadConsent must be "report", "hold-only" or "enforce" (omit for off)');
    const env = { CLOUDFLARE_ACCOUNT_ID: hex("a") };
    expect(() => assertCloudflareStage("cf-sessions-prod", "plan", { sessionIngest: { ...inputs, uploadConsent: "on" } }, env)).toThrow(/uploadConsent.*Nothing was changed/);
  });
});
