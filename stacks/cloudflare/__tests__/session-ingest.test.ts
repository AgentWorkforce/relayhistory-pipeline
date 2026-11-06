import { describe, expect, it } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import * as Cloudflare from "@alchemy.run/cloudflare";
import {
  COMPATIBILITY_DATE,
  FANOUT_DLQ,
  INBOX_AUTHORITY_MODE,
  INBOX_CANDIDATE_KEY_ROOT,
  SESSION_INGEST_RESOURCES,
  SESSION_INGEST_WORKER,
  assertFanoutConsumer,
  assertNoSecretLiterals,
  assertPrivateWorker,
  assertSessionIngestInputs,
  assertSessionIngestPlanContract,
  externalQueue,
  fanoutConsumerProps,
  secretsStoreSecret,
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

  it("refuses a detailed plan that shows a credential (a secret value in state)", () => {
    const ok = plan({ "session-ingest": "update" });
    for (const leak of ['  "type": "secret_text"', "  AKIAABCDEFGHIJKLMNOP", '  "x": "postgresql://bulls96_session_ingest:pw@pooler/postgres"', "  -----BEGIN RSA PRIVATE KEY-----"])
      expect(() => check(ok, `${ok}${leak}\n`)).toThrow(/credential/);
  });
});
