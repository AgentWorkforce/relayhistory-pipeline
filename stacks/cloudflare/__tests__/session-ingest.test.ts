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
  sessionIngestResources,
  sessionIngestVars,
  sessionIngestWorkerProps,
  type SessionIngestInputs,
} from "../session-ingest.ts";
import { assertCloudflareStage, cloudflareMode } from "../alchemy.cloudflare.run.ts";

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
    const checkWith = (compact: string) => assertSessionIngestPlanContract(compact, compact.split("\n").map((l) => (l.startsWith("[") ? `  ${l}` : l)).join("\n"), true);
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

  it("refuses a detailed plan that shows a credential (a secret value in state)", () => {
    const ok = plan({ "session-ingest": "update" });
    for (const leak of ['  "type": "secret_text"', "  AKIAABCDEFGHIJKLMNOP", '  "x": "postgresql://bulls96_session_ingest:pw@pooler/postgres"', "  -----BEGIN RSA PRIVATE KEY-----"])
      expect(() => check(ok, `${ok}${leak}\n`)).toThrow(/credential/);
  });
});

describe("session ingest: S3-first async admission (default off)", () => {
  const buffered: SessionIngestInputs = { ...inputs, acceptanceMode: "buffered", expectedGeneration: "a".repeat(64), inlineMaxSessions: 0 };
  const admissionQueue = { Type: "Cloudflare.Queues.Queue", queueName: ADMISSION_QUEUE };

  it("absent asyncAdmission: vars, props and the resource list are exactly today's", () => {
    expect(Object.keys(sessionIngestVars(buffered))).not.toContain("INGEST_ASYNC_ADMISSION");
    expect(Object.keys(props(buffered).env)).not.toContain("INGEST_ADMISSION");
    expect(Object.keys(props(inputs).env)).not.toContain("INGEST_ASYNC_ADMISSION");
    expect(sessionIngestResources()).toBe(SESSION_INGEST_RESOURCES);
    expect(sessionIngestResources(false)).toBe(SESSION_INGEST_RESOURCES);
  });

  it("is refused outside buffered acceptance and for any value but on/drain", () => {
    for (const bad of [{ ...inputs, asyncAdmission: "on" }, { ...buffered, asyncAdmission: "yes" }, { ...buffered, asyncAdmission: true }])
      expect(() => assertSessionIngestInputs(bad, refuse)).toThrow(/asyncAdmission/);
    for (const mode of ["on", "drain"] as const) expect(() => assertSessionIngestInputs({ ...buffered, asyncAdmission: mode }, refuse)).not.toThrow();
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
    expect(() => sessionIngestWorkerProps(buffered, { ...bindings, INGEST_ADMISSION: admissionQueue })).toThrow(/INGEST_ADMISSION/);
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
