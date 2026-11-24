/** Strictly synthetic transports; unknown requests throw rather than use real fetch. */
import { describe, test, expect, spyOn } from "bun:test";
import * as Stacks from "alchemy/Alchemist/routes/stack";
import * as Effect from "effect/Effect";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  assertOperatorSnapshot,
  advertisedMaxRecordsDeletionAllowed,
  inflightLeaseDeletionAllowed,
  bufferCommitBudgetDeletionAllowed,
  bufferCommitLockWaitDeletionAllowed,
  bufferTimeoutBusyDeletionAllowed,
  bufferLockBusyDeletionAllowed,
  retainedOperatorPlan,
  requireStateAuthority,
  operatorStateUrl,
  assertOperatorSource,
  guardOperatorFetch,
  OPERATOR_METADATA_MAX_BYTES,
  operatorCapture,
  operatorSafePlans,
  verifyOperatorCapture,
  EXECUTION_EXCEPTION,
  OperatorRefusal,
  operatorExecutionHost,
  type OperatorSecrets,
} from "../../../scripts/private-ingest-operator.ts";
import { inputsDigest } from "../../aws/deploy-guard.ts";
import { createHash } from "node:crypto";
const secrets: OperatorSecrets = {
  accountId: "a".repeat(32),
  apiToken: "synthetic-api-token-not-authorized",
  stateUrl: "https://alchemy-state-store-96bulls.fixture.workers.dev",
  stateToken: "synthetic-state-token-not-authorized",
};

test("separate GitHub-hosted plan and deploy runners share one capture host identity", () => {
  expect(operatorExecutionHost({ GITHUB_ACTIONS: "true" }, "runner-a")).toBe("github-actions");
  expect(operatorExecutionHost({ GITHUB_ACTIONS: "true" }, "runner-b")).toBe("github-actions");
  expect(operatorExecutionHost({}, "operator-host")).toBe("operator-host");
});
const baseInput = {
  sessionIngest: {
    accountId: secrets.accountId,
    hyperdriveId: "b".repeat(32),
    secretsStoreId: "c".repeat(32),
    kmsKeyArn:
      "arn:aws:kms:us-east-1:941433935742:key/12345678-1234-1234-1234-123456789abc",
  },
};
const input = baseInput;
const control = {
  status: "created",
  resourceType: "Cloudflare.Worker",
  logicalId: "Api",
  fqn: "Api",
  instanceId: "fixture",
  providerVersion: 1,
  downstream: [],
  bindings: [],
  attr: {
    workerName: "alchemy-state-store-96bulls",
    accountId: secrets.accountId,
  },
};
const tools = {
    bun: "1.4.2",
    node: "v24.21.0",
    alchemy: "2.0.0-beta.84",
    cloudflare: "2.0.0-beta.84",
  },
  source = { commit: "d".repeat(40), changedFiles: [], host: "synthetic" },
  authority = {
    stateVersion: 7,
    controlStateSha256: "e".repeat(64),
    nativeBindingsSha256: "f".repeat(64),
  };
const digest = (v: string) => createHash("sha256").update(v).digest("hex");
async function actualPlanProof(declared: "canonical" | "buffered" | "admission" | "admission-default" | "admission-drain" | "custody" | "custody-shadow" | "custody-shadow-drain" | "frozen-receipt" | "neon-exit" | "advertised" | "inflight" | "buffer-busy" | "buffer-commit-wait" | "buffer-commit-budget" | "buffer-timeout-busy") {
  // "admission": buffered + asyncAdmission "on" (the three admission resources and two more bindings).
  // "admission-default": buffered with asyncAdmission OMITTED, which resolves to "on" (same as "admission").
  // "admission-drain": buffered + asyncAdmission "drain" (same resources, the var says drain).
  // "buffered": buffered + explicit asyncAdmission "off": the four resources, byte-identical to before.
  // "custody" / "frozen-receipt": buffered + frozen custody, optionally the receipt; asyncAdmission "off".
  // "custody-shadow" / "custody-shadow-drain": shadow custody, no receipt bridge; the latter retains admission resources.
  // "neon-exit": frozen custody plus SESSIONS_NEON_EXIT, with no NEON_REPLICA binding.
  // "advertised": buffered + explicit asyncAdmission "off" + advertisedMaxRecords 120 (one more plain-text binding).
  // "inflight": buffered + explicit asyncAdmission "off" + inflightLease "on" (one more plain-text binding).
  // "buffer-busy": buffered + explicit asyncAdmission "off" + bufferLockBusy "on" (one more plain-text binding).
  // "buffer-commit-wait": buffered + explicit asyncAdmission "off" + bufferCommitLockWaitMs 3000 (one more plain-text binding).
  const mode = declared === "canonical" ? "canonical" : "buffered";
  const neonExit = declared === "neon-exit";
  const shadowDrain = declared === "custody-shadow-drain", shadow = declared === "custody-shadow" || shadowDrain;
  const admission = declared === "admission" || declared === "admission-default" || declared === "admission-drain" || shadowDrain;
  const admissionVar = declared === "admission-drain" || shadowDrain ? "drain" : "on";
  const advertised = declared === "advertised";
  const inflight = declared === "inflight";
  const bufferBusy = declared === "buffer-busy";
  const commitWait = declared === "buffer-commit-wait";
  const commitBudget = declared === "buffer-commit-budget", timeoutBusy = declared === "buffer-timeout-busy";
  const frozenReceipt = declared === "frozen-receipt", custody = declared === "custody" || shadow || frozenReceipt || neonExit;
  const bindingCount = (admission ? 17 : mode === "buffered" ? 15 : 13) + (custody ? 1 : 0) + (frozenReceipt ? 1 : 0) + (neonExit ? 1 : 0) + (advertised ? 1 : 0) + (inflight ? 1 : 0) + (bufferBusy ? 1 : 0) + (commitWait ? 1 : 0) + (commitBudget ? 1 : 0) + (timeoutBusy ? 1 : 0);
  const input = {
    ...baseInput,
    sessionIngest: {
      ...baseInput.sessionIngest,
      ...(declared === "admission" ? { asyncAdmission: "on" as const } : {}),
      ...(declared === "admission-drain" ? { asyncAdmission: "drain" as const } : {}),
      ...(declared === "buffered" || advertised || inflight || bufferBusy || commitWait || commitBudget || timeoutBusy || custody ? { asyncAdmission: shadowDrain ? "drain" as const : "off" as const } : {}),
      ...(custody ? { custodySource: shadow ? "shadow" as const : "frozen-supabase" as const } : {}),
      ...(frozenReceipt ? { syncAdmissionFrozenReceipt: "on" as const } : {}),
      ...(neonExit ? { neonExit: "on" as const } : {}),
      ...(advertised ? { advertisedMaxRecords: 120 } : {}),
      ...(inflight ? { inflightLease: "on" as const } : {}),
      ...(bufferBusy ? { bufferLockBusy: "on" as const } : {}),
      ...(commitWait ? { bufferCommitLockWaitMs: 3000 } : {}),
      ...(commitBudget ? { bufferCommitBudgetMs: 6000 } : {}),
      ...(timeoutBusy ? { bufferTimeoutBusy: "on" as const } : {}),
      ...(mode === "buffered"
        ? {
            acceptanceMode: "buffered" as const,
            expectedGeneration: "e".repeat(64),
            inlineMaxSessions: 0,
            replicaMode: "off" as const,
          }
        : {}),
    },
  };
  let native: any;
  const originalPlan = Stacks.plan;
  const watched = spyOn(Stacks, "plan").mockImplementation((args: any) =>
    originalPlan(args).pipe(
      Effect.tap((snapshot) =>
        Effect.sync(() => {
          native = snapshot;
        }),
      ),
    ),
  );
  const calls: Array<{ path: string; method: string }> = [],
    dir = await mkdtemp(join(tmpdir(), "private-operator-proof-")),
    empty = join(dir, "empty");
  await writeFile(empty, "");
  const old = {
    CI: process.env.CI,
    CLOUDFLARE_ACCOUNT_ID: process.env.CLOUDFLARE_ACCOUNT_ID,
    CLOUDFLARE_API_TOKEN: process.env.CLOUDFLARE_API_TOKEN,
    GITHUB_ACTIONS: process.env.GITHUB_ACTIONS,
    PRIVATE_INGEST_GITHUB_GATE: process.env.PRIVATE_INGEST_GITHUB_GATE,
  };
  process.env.CI = "true";
  process.env.CLOUDFLARE_ACCOUNT_ID = secrets.accountId;
  process.env.CLOUDFLARE_API_TOKEN = secrets.apiToken;
  process.env.GITHUB_ACTIONS = "true";
  process.env.PRIVATE_INGEST_GITHUB_GATE = "approved";
  const fetcher: typeof fetch = async (input, init) => {
    const u = new URL(String(input)),
      method = init?.method ?? "GET";
    calls.push({ path: u.pathname, method });
    if (method !== "GET")
      throw Error("no mutation allowed in this synthetic plan");
    if (u.origin === secrets.stateUrl) {
      if (u.pathname === "/version") return Response.json({ version: 7 });
      if (u.pathname.endsWith("/resources/Api")) return Response.json(control);
      if (u.pathname.endsWith("/output")) return new Response(null);
      if (
        u.pathname.endsWith("/resources") ||
        u.pathname.endsWith("/replaced-resources") ||
        u.pathname.endsWith("/stages") ||
        u.pathname === "/state/stacks"
      )
        return Response.json([]);
      if (u.pathname.includes("/resources/")) return new Response(null);
    }
    if (
      u.origin === "https://api.cloudflare.com" &&
      u.pathname.endsWith("/queues")
    )
      return Response.json({
        success: true,
        result: [],
        result_info: { page: 1, per_page: 100, count: 0, total_count: 0 },
      });
    if (
      u.origin === "https://api.cloudflare.com" &&
      u.pathname.endsWith("/hyperdrive/configs/" + "b".repeat(32))
    )
      return Response.json({
        success: true,
        result: {
          id: "b".repeat(32),
          name: "synthetic-existing-hyperdrive",
          origin: {
            host: "synthetic.invalid",
            port: 5432,
            database: "postgres",
            scheme: "postgres",
            user: "synthetic",
          },
          caching: { disabled: true },
          created_on: "2026-01-01T00:00:00Z",
          modified_on: "2026-01-01T00:00:00Z",
        },
      });
    throw Error("unexpected synthetic request " + u.pathname);
  };
  try {
    let reviewed = 0;
    const result = await retainedOperatorPlan(
      secrets,
      "plan",
      input,
      (plans, attestation) => {
        reviewed++;
        expect(plans.planText).toContain(admission ? "7 to create" : "4 to create");
        expect(attestation.stateVersion).toBe(7);
      },
      { directory: dir, emptyEnvPath: empty, fetcher },
    );
    expect(reviewed).toBe(1);
    expect(result.plans.planText).toContain("[session-ingest] create");
    // Explicit "off" (and canonical): no admission resource, binding or var anywhere in the plan.
    if (!admission) {
      expect(result.plans.planText).not.toContain("admission-");
      expect(result.plans.planText).not.toContain("INGEST_ADMISSION");
      expect(result.plans.planText).not.toContain("INGEST_ASYNC_ADMISSION");
      expect(result.plans.detailedText).not.toContain("96bulls-session-ingest-admission");
    }
    expect(result.plans.detailedText).toContain("workersDev: false");
    expect(result.plans.detailedText).not.toContain(secrets.stateToken);
    expect(calls.every((c) => c.method === "GET")).toBe(true);
    expect(calls.some((c) => /preview|secrets_store/u.test(c.path))).toBe(
      false,
    );
    const text = JSON.stringify(input),
      capture = operatorCapture(
        source,
        text,
        secrets,
        result.plans,
        tools,
        result.authority,
      ),
      provenanceText = JSON.stringify(capture);
    const approved = {
      ...input,
      approval: {
        decision: "approved",
        by: "Khaliq Gant",
        date: new Date().toISOString().slice(0, 10),
        quote: "Explicitly approve retained-plan operator exception only",
        execution: EXECUTION_EXCEPTION,
        capture: {
          provenanceSha256: digest(provenanceText),
          planSha256: digest(result.plans.planText),
          inputsSha256: inputsDigest(text),
          sourceCommit: source.commit,
        },
      },
    };
    const options = {
      mode: "deploy" as const,
      sourceSha: source.commit,
      inputsSha256: inputsDigest(text),
      provenanceSha256: digest(provenanceText),
    };
    let reviewedSnapshot: any;
    let applied = 0,
      rechecked = 0;
    const applySpy = spyOn(Stacks, "apply").mockImplementation(
      (snapshot: any) =>
        Effect.sync(() => {
          expect(snapshot === reviewedSnapshot).toBe(true);
          applied++;
          return undefined;
        }),
    );
    const runtime = {
      directory: dir,
      emptyEnvPath: empty,
      fetcher,
      reviewed: {
        options,
        source,
        inputText: JSON.stringify(approved),
        provenanceText,
        tools,
        recheckSource: () => {
          rechecked++;
        },
      },
    };
    try {
      await retainedOperatorPlan(
        secrets,
        "deploy",
        approved,
        () => {
          reviewedSnapshot = native;
        },
        runtime,
      );
      expect(applied).toBe(1);
      expect(rechecked).toBe(1);
      await expect(
        retainedOperatorPlan(
          secrets,
          "deploy",
          approved,
          () => {
            throw Error("review refuses fresh plan");
          },
          runtime,
        ),
      ).rejects.toThrow("review refuses");
      expect(applied).toBe(1);
    } finally {
      applySpy.mockRestore();
    }
    expect(assertOperatorSnapshot(native, input)).toHaveLength(
      bindingCount,
    );
    // Installed Alchemy's NoopUpdate omits row.props. Check the exact
    // no-op node shape against existing resolved state and keep all guards.
    const noopRows = Object.fromEntries(Object.entries(native.native.resources)
      .map(([id, node]: [string, any]) => {
        const { props, ...rest } = node;
        return [id, { ...rest, action: "noop",
          state: { ...node.state, status: "updated", props } }];
      }));
    const noopSnapshot = { ...native, native: { ...native.native, resources: noopRows } };
    expect(assertOperatorSnapshot(noopSnapshot, input)).toHaveLength(
      bindingCount);
    const fanout = noopRows["session-ingest-fanout"];
    const queueId = "9".repeat(32), queueName = "96bulls-session-ingest-fanout";
    fanout.state.attr = { queueId, queueName, accountId: secrets.accountId };
    const noopWorker = noopRows["session-ingest"];
    const queueData = { type: "queue", name: "INGEST_FANOUT", queueId, queueName };
    noopWorker.bindings = noopWorker.bindings.map((b: any) => b.sid !== "INGEST_FANOUT" ? b :
      { ...b, action: "noop", data: { bindings: [queueData] } });
    expect(assertOperatorSnapshot(noopSnapshot, input)).toHaveLength(
      bindingCount);
    for (const delta of [{ queueName: "foreign" }, { queueId: "8".repeat(32) },
      { queueId: undefined }, { queueName: undefined }, { shim: "foreign" }]) {
      const rows = { ...noopRows, "session-ingest": { ...noopWorker,
        bindings: noopWorker.bindings.map((b: any) => b.sid !== "INGEST_FANOUT" ? b :
          { ...b, data: { bindings: [{ ...queueData, ...delta }] } }) } };
      expect(() => assertOperatorSnapshot(
        { ...native, native: { ...native.native, resources: rows } }, input))
        .toThrow("native_binding_target");
    }
    for (const attr of [undefined, { ...fanout.state.attr, accountId: "8".repeat(32) }]) {
      const rows = { ...noopRows, "session-ingest-fanout": {
        ...fanout, state: { ...fanout.state, attr } } };
      expect(() => assertOperatorSnapshot(
        { ...native, native: { ...native.native, resources: rows } }, input))
        .toThrow("native_binding_target");
    }
    for (const status of ["creating", "updating", "deleted", undefined]) {
      const rows = { ...noopRows, "session-ingest-fanout": {
        ...fanout, state: { ...fanout.state, status } } };
      expect(() => assertOperatorSnapshot(
        { ...native, native: { ...native.native, resources: rows } }, input))
        .toThrow("native_noop_state");
    }
    for (const props of [undefined, null, [], { ...fanout.state.props, name: "foreign" },
      { ...fanout.state.props, deliveryPaused: mode === "buffered" ? false : true }]) {
      const rows = { ...noopRows, "session-ingest-fanout": {
        ...fanout, state: { ...fanout.state, props } } };
      expect(() => assertOperatorSnapshot(
        { ...native, native: { ...native.native, resources: rows } }, input)).toThrow();
    }
    expect(() => assertOperatorSnapshot({ ...native, native: { ...native.native,
      resources: { ...noopRows, "session-ingest-fanout": { ...fanout, state: undefined } } } },
      input)).toThrow("native_noop_state");
    // A stale valid state must never hide missing or unsafe proposed props.
    for (const props of [undefined, { ...fanout.state.props, name: "foreign" },
      { ...fanout.state.props, deliveryPaused: mode === "buffered" ? false : true }]) {
      const rows = { ...noopRows, "session-ingest-fanout": {
        ...fanout, action: "update", props } };
      expect(() => assertOperatorSnapshot(
        { ...native, native: { ...native.native, resources: rows } }, input)).toThrow();
    }
    const queue = native.native.resources["session-ingest-fanout"];
    if (admission) {
      const rowsOf = native.native.resources;
      const worker = rowsOf["session-ingest"];
      expect(worker.bindings.find((b: any) => b.sid === "INGEST_ASYNC_ADMISSION").data.bindings[0])
        .toEqual({ type: "plain_text", name: "INGEST_ASYNC_ADMISSION", text: admissionVar });
      expect(worker.bindings.some((b: any) => b.sid === "INGEST_ADMISSION")).toBe(true);
      for (const id of ["session-ingest-admission", "session-ingest-admission-dlq"]) {
        expect(rowsOf[id].props.deliveryPaused).toBeUndefined();
        // Paused admission queue, or a foreign name: refused.
        for (const props of [{ ...rowsOf[id].props, deliveryPaused: true }, { ...rowsOf[id].props, name: "foreign" }])
          expect(() => assertOperatorSnapshot({ ...native, native: { ...native.native, resources: { ...rowsOf, [id]: { ...rowsOf[id], props } } } }, input)).toThrow();
      }
      // The fanout queues stay paused.
      expect(queue.props.deliveryPaused).toBe(true);
      const ac = rowsOf["session-ingest-admission-consumer"];
      expect(ac.props.settings).toEqual({ batchSize: 1, maxRetries: 20, maxConcurrency: 4 });
      expect(() => assertOperatorSnapshot({ ...native, native: { ...native.native, resources: { ...rowsOf,
        "session-ingest-admission-consumer": { ...ac, props: { ...ac.props, settings: { ...ac.props.settings, maxConcurrency: 64 } } } } } }, input))
        .toThrow("native_admission_consumer");
      // All or none: dropping one admission resource, or the binding, refuses.
      const { ["session-ingest-admission-consumer"]: _gone, ...partial } = rowsOf;
      expect(() => assertOperatorSnapshot({ ...native, native: { ...native.native, resources: partial } }, input)).toThrow("native_resource_inventory");
      expect(() => assertOperatorSnapshot({ ...native, native: { ...native.native, resources: { ...rowsOf, "session-ingest": { ...worker,
        bindings: worker.bindings.filter((b: any) => b.sid !== "INGEST_ADMISSION") } } } }, input)).toThrow("native_binding_inventory");
      // The same snapshot against an explicit "off" input: refused (seven resources are not four).
      expect(() => assertOperatorSnapshot(native, { ...input, sessionIngest: { ...input.sessionIngest, asyncAdmission: "off" } })).toThrow("native_resource_inventory");
      // Omitted and explicit "on" resolve alike on a buffered capture; "drain" vs "on" differs only in the var.
      const { asyncAdmission: _a, ...omitted } = input.sessionIngest as any;
      if (admissionVar === "on") {
        expect(assertOperatorSnapshot(native, { ...input, sessionIngest: omitted })).toHaveLength(bindingCount);
        expect(assertOperatorSnapshot(native, { ...input, sessionIngest: { ...omitted, asyncAdmission: "on" } })).toHaveLength(bindingCount);
      }
      expect(() => assertOperatorSnapshot(native, { ...input, sessionIngest: { ...omitted, asyncAdmission: admissionVar === "on" ? "drain" : "on" } }))
        .toThrow("native_binding_target");
    }
    if (frozenReceipt) {
      const rowsOf = native.native.resources;
      const worker = rowsOf["session-ingest"];
      expect(worker.bindings.find((b: any) => b.sid === "INGEST_SYNC_ADMISSION_FROZEN_RECEIPT").data.bindings[0])
        .toEqual({ type: "plain_text", name: "INGEST_SYNC_ADMISSION_FROZEN_RECEIPT", text: "on" });
      // No admission queue or binding: the var alone.
      expect(Object.keys(rowsOf).some((id) => id.includes("admission"))).toBe(false);
      expect(worker.bindings.some((b: any) => b.sid === "INGEST_ADMISSION")).toBe(false);
      // The read-back refuses the var dropped, changed, or present for an input without the flag.
      expect(() => assertOperatorSnapshot({ ...native, native: { ...native.native, resources: { ...rowsOf, "session-ingest": { ...worker,
        bindings: worker.bindings.filter((b: any) => b.sid !== "INGEST_SYNC_ADMISSION_FROZEN_RECEIPT") } } } }, input)).toThrow("native_binding_inventory");
      expect(() => assertOperatorSnapshot({ ...native, native: { ...native.native, resources: { ...rowsOf, "session-ingest": { ...worker,
        bindings: worker.bindings.map((b: any) => b.sid !== "INGEST_SYNC_ADMISSION_FROZEN_RECEIPT" ? b :
          { ...b, data: { bindings: [{ ...b.data.bindings[0], text: "off" }] } }) } } } }, input)).toThrow("native_binding_target");
      const { syncAdmissionFrozenReceipt: _s, ...withoutFlag } = input.sessionIngest as any;
      expect(() => assertOperatorSnapshot(native, { ...input, sessionIngest: withoutFlag })).toThrow("native_binding_inventory");
      const receiptBinding = worker.bindings.find((b: any) =>
        b.sid === "INGEST_SYNC_ADMISSION_FROZEN_RECEIPT");
      const { INGEST_SYNC_ADMISSION_FROZEN_RECEIPT: _receipt, ...receiptRollbackEnv } = worker.props.env;
      const receiptRollbackWorker = {
        ...worker,
        action: "update",
        props: { ...worker.props, env: receiptRollbackEnv },
        state: { status: "updated", props: worker.props },
        bindings: worker.bindings.map((b: any) => b === receiptBinding
          ? { ...b, action: "delete" }
          : b),
      };
      expect(assertOperatorSnapshot({ ...native, native: { ...native.native, resources: {
        ...rowsOf, "session-ingest": receiptRollbackWorker,
      } } }, { ...input, sessionIngest: withoutFlag })).toHaveLength(bindingCount);
      expect(() => assertOperatorSnapshot({ ...native, native: { ...native.native, resources: {
        ...rowsOf,
        "session-ingest": {
          ...receiptRollbackWorker,
          state: { ...receiptRollbackWorker.state, props: {
            ...receiptRollbackWorker.state.props,
            env: { ...receiptRollbackWorker.state.props.env, INGEST_CUSTODY_SOURCE: "shadow" },
          } },
        },
      } } }, { ...input, sessionIngest: withoutFlag })).toThrow("native_binding_target");
    }
    if (neonExit) {
      const rowsOf = native.native.resources, worker = rowsOf["session-ingest"];
      const at = (bindings: any[]) => ({ ...native, native: { ...native.native, resources: { ...rowsOf, "session-ingest": { ...worker, bindings } } } });
      // The expected var flows from sessionIngestVars: rendered, read back, and required.
      expect(worker.bindings.find((b: any) => b.sid === "SESSIONS_NEON_EXIT").data.bindings[0])
        .toEqual({ type: "plain_text", name: "SESSIONS_NEON_EXIT", text: "true" });
      expect(worker.bindings.some((b: any) => b.sid === "NEON_REPLICA")).toBe(false);
      expect(() => assertOperatorSnapshot(at(worker.bindings.filter((b: any) => b.sid !== "SESSIONS_NEON_EXIT")), input))
        .toThrow("native_binding_inventory");
      expect(() => assertOperatorSnapshot(at(worker.bindings.map((b: any) => b.sid !== "SESSIONS_NEON_EXIT" ? b :
        { ...b, data: { bindings: [{ ...b.data.bindings[0], text: "false" }] } })), input)).toThrow("native_binding_target");
      const { neonExit: _exit, ...withoutExit } = input.sessionIngest as any;
      expect(() => assertOperatorSnapshot(native, { ...input, sessionIngest: withoutExit })).toThrow("native_binding_inventory");
      // A NEON_REPLICA binding that would remain on the Worker: refused by name, for every remaining action,
      // by sid or by the wire binding's name.
      const replica = { type: "queue", name: "NEON_REPLICA", queue_name: "relayhistory-neon-replica" };
      for (const action of ["create", "update", "noop"])
        expect(() => assertOperatorSnapshot(at([...worker.bindings, { sid: "NEON_REPLICA", action, data: { bindings: [replica] } }]), input))
          .toThrow("native_neon_exit_replica");
      expect(() => assertOperatorSnapshot(at([...worker.bindings, { sid: "FOREIGN", action: "create", data: { bindings: [replica] } }]), input))
        .toThrow("native_neon_exit_replica");
      // Without the profile the same row is only an unknown binding (generic inventory refusal).
      expect(() => assertOperatorSnapshot(at([...worker.bindings, { sid: "NEON_REPLICA", action: "create", data: { bindings: [replica] } }]),
        { ...input, sessionIngest: withoutExit })).toThrow("native_binding_inventory");
      // A delete of a stale NEON_REPLICA is the desired transition, so it is NOT the Neon-exit refusal. The
      // reviewed delete allowlist admits no NEON_REPLICA unbind today, so it still fails closed.
      const unbind = { ...worker, action: "update", state: { status: "updated", props: { ...worker.props,
        env: { ...worker.props.env, NEON_REPLICA: { Type: "Cloudflare.Queues.Queue", queueName: "relayhistory-neon-replica" } } } },
        bindings: [...worker.bindings, { sid: "NEON_REPLICA", action: "delete", data: { bindings: [replica] } }] };
      let refusal: unknown;
      try {
        assertOperatorSnapshot({ ...native, native: { ...native.native, resources: { ...rowsOf, "session-ingest": unbind } } }, input);
      } catch (error) {
        refusal = error;
      }
      expect(refusal).toBeInstanceOf(OperatorRefusal);
      expect(String((refusal as Error).message)).toContain("native_binding_target");
      expect(String((refusal as Error).message)).not.toContain("native_neon_exit_replica");
    }
    if (advertised) {
      const rowsOf = native.native.resources, worker = rowsOf["session-ingest"];
      const advertisedBinding = worker.bindings.find((b: any) => b.sid === "INGEST_ADVERTISED_MAX_RECORDS");
      expect(advertisedBinding.data.bindings[0])
        .toEqual({ type: "plain_text", name: "INGEST_ADVERTISED_MAX_RECORDS", text: "120" });
      expect(worker.props.env.INGEST_ADVERTISED_MAX_RECORDS).toBe("120");
      const { INGEST_ADVERTISED_MAX_RECORDS: _adv, ...unbindEnv } = worker.props.env;
      const { advertisedMaxRecords: _max, ...withoutAdvertised } = input.sessionIngest as any;
      const omitted = { ...input, sessionIngest: withoutAdvertised };
      // Live (state) has "120"; the candidate omits the value, so the plan deletes the binding.
      const unbindWorker = {
        ...worker,
        action: "update",
        props: { ...worker.props, env: unbindEnv },
        state: { status: "updated", props: worker.props },
        bindings: worker.bindings.map((b: any) => b === advertisedBinding ? { ...b, action: "delete" } : b),
      };
      const at = (w: any) => ({ ...native, native: { ...native.native, resources: { ...rowsOf, "session-ingest": w } } });
      const withLive = (live: unknown) => ({ ...unbindWorker, state: { ...unbindWorker.state, props: {
        ...unbindWorker.state.props, env: { ...unbindWorker.state.props.env, INGEST_ADVERTISED_MAX_RECORDS: live } } } });
      const withDeleted = (value: unknown) => ({ ...unbindWorker, bindings: unbindWorker.bindings.map((b: any) =>
        b.sid !== "INGEST_ADVERTISED_MAX_RECORDS" ? b : { ...b, data: { bindings: [value] } }) });
      // Admitted: one delete beyond the candidate's expected inventory, pinned to the live text.
      const admitted = assertOperatorSnapshot(at(unbindWorker), omitted);
      expect(admitted).toHaveLength(bindingCount);
      expect(admitted.find((e) => e.sid === "INGEST_ADVERTISED_MAX_RECORDS")).toEqual({
        sid: "INGEST_ADVERTISED_MAX_RECORDS", action: "delete",
        target: { type: "plain_text", name: "INGEST_ADVERTISED_MAX_RECORDS", text: "120" },
      });
      // Admitted for other valid live values too; the delete entry carries each live value.
      for (const live of ["1", "500"]) {
        const target = { type: "plain_text", name: "INGEST_ADVERTISED_MAX_RECORDS", text: live };
        const each = assertOperatorSnapshot(at({ ...withLive(live), bindings: withDeleted(target).bindings }), omitted);
        expect(each).toHaveLength(bindingCount);
        expect(each.find((e) => e.sid === "INGEST_ADVERTISED_MAX_RECORDS")).toEqual({
          sid: "INGEST_ADVERTISED_MAX_RECORDS", action: "delete", target });
      }
      // The candidate profile still sets the value while the plan row deletes it: the binding count no longer
      // matches the candidate inventory; with a retained row for the value as well, the delete names an
      // expected binding (both refused before the allowlist; the helper's own profile check is unit-tested).
      for (const max of [120, 500]) {
        const keeps = { ...input, sessionIngest: { ...withoutAdvertised, advertisedMaxRecords: max } };
        expect(() => assertOperatorSnapshot(at(unbindWorker), keeps)).toThrow("native_binding_inventory");
        expect(() => assertOperatorSnapshot(at({ ...unbindWorker, bindings: [...unbindWorker.bindings, {
          sid: "INGEST_ADVERTISED_MAX_RECORDS", action: "update",
          data: { bindings: [{ type: "plain_text", name: "INGEST_ADVERTISED_MAX_RECORDS", text: String(max) }] },
        }] }), keeps)).toThrow("native_binding_target");
      }
      // The proposed env still carries the variable.
      expect(() => assertOperatorSnapshot(at({ ...unbindWorker, props: worker.props }), omitted)).toThrow("native_binding_target");
      // Live text not in the strict 1..500 form; the deleted binding is forged to match so only the live-value
      // check can refuse.
      for (const live of ["0", "501", "0120", " 120", "120 ", "2.5", "+120", "", 120, undefined]) {
        const forged = withLive(live);
        expect(() => assertOperatorSnapshot(at({ ...forged, bindings: withDeleted({
          type: "plain_text", name: "INGEST_ADVERTISED_MAX_RECORDS", text: live }).bindings }), omitted))
          .toThrow("native_binding_target");
      }
      // The native binding text differs from the live text (digest mismatch), or the binding is not plain text.
      for (const value of [
        { type: "plain_text", name: "INGEST_ADVERTISED_MAX_RECORDS", text: "500" },
        { type: "plain_text", name: "INGEST_ADVERTISED_MAX_RECORDS", text: 120 },
        { type: "plain_text", name: "FOREIGN", text: "120" },
        { type: "plain_text", name: "INGEST_ADVERTISED_MAX_RECORDS", text: "120", extra: "x" },
        { type: "secret_text", name: "INGEST_ADVERTISED_MAX_RECORDS", text: "120" },
        { type: "queue", name: "INGEST_ADVERTISED_MAX_RECORDS", queue_name: "foreign" },
      ])
        expect(() => assertOperatorSnapshot(at(withDeleted(value)), omitted)).toThrow("native_binding_target");
      // Two deleted bindings, or an extra data key on the delete row.
      expect(() => assertOperatorSnapshot(at({ ...unbindWorker, bindings: unbindWorker.bindings.map((b: any) =>
        b.sid !== "INGEST_ADVERTISED_MAX_RECORDS" ? b : { ...b, data: { ...b.data, shim: "x" } }) }), omitted))
        .toThrow("native_binding_target");
      const generation = "e".repeat(64), accepted = `${"f".repeat(64)},${generation}`;
      const twoDeletes = {
        ...unbindWorker,
        state: { ...unbindWorker.state, props: { ...unbindWorker.state.props, env: {
          ...unbindWorker.state.props.env, INGEST_EXPECT_GENERATION: "f".repeat(64), INGEST_ACCEPT_GENERATIONS: accepted } } },
        bindings: [...unbindWorker.bindings, { sid: "INGEST_ACCEPT_GENERATIONS", action: "delete",
          data: { bindings: [{ type: "plain_text", name: "INGEST_ACCEPT_GENERATIONS", text: accepted }] } }],
      };
      expect(() => assertOperatorSnapshot(at(twoDeletes), omitted)).toThrow("native_binding_inventory");
      // The Worker resource row must be an update.
      for (const action of ["create", "noop"])
        expect(() => assertOperatorSnapshot(at({ ...unbindWorker, action }), omitted)).toThrow("native_binding_target");
      // Any other plain-text unbind stays refused, even with a matching live value.
      const foreign = {
        ...unbindWorker,
        state: { ...unbindWorker.state, props: { ...unbindWorker.state.props, env: {
          ...unbindWorker.state.props.env, INGEST_CLAIM_LOCK_BUSY: "on" } } },
        bindings: [...worker.bindings, { sid: "INGEST_CLAIM_LOCK_BUSY", action: "delete",
          data: { bindings: [{ type: "plain_text", name: "INGEST_CLAIM_LOCK_BUSY", text: "on" }] } }],
      };
      expect(() => assertOperatorSnapshot(at({ ...foreign, props: worker.props }), input)).toThrow("native_binding_target");
      // The same with every advertised condition satisfied: the candidate omits the value, the advertised binding
      // is absent from the plan, the live value is valid, and a foreign INGEST_CLAIM_LOCK_BUSY delete carries text
      // equal to it. Inventory and digest would both pass, so only the arm's name match refuses.
      const foreignWithLiveAdvertised = {
        ...unbindWorker,
        state: { ...unbindWorker.state, props: { ...unbindWorker.state.props, env: {
          ...unbindWorker.state.props.env, INGEST_CLAIM_LOCK_BUSY: "120" } } },
        bindings: [...worker.bindings.filter((b: any) => b !== advertisedBinding), { sid: "INGEST_CLAIM_LOCK_BUSY", action: "delete",
          data: { bindings: [{ type: "plain_text", name: "INGEST_CLAIM_LOCK_BUSY", text: "120" }] } }],
      };
      expect(foreignWithLiveAdvertised.state.props.env.INGEST_ADVERTISED_MAX_RECORDS).toBe("120");
      expect(() => assertOperatorSnapshot(at(foreignWithLiveAdvertised), omitted)).toThrow("native_binding_target");
      // Changing the value is an update row checked against the candidate profile alone: live "120" -> candidate
      // 500 with text "500" is admitted with no delete; text "500" while the candidate says 120 is refused.
      const update500 = {
        ...worker,
        action: "update",
        props: { ...worker.props, env: { ...worker.props.env, INGEST_ADVERTISED_MAX_RECORDS: "500" } },
        state: { status: "updated", props: worker.props },
        bindings: worker.bindings.map((b: any) => b !== advertisedBinding ? b : { ...b, action: "update",
          data: { bindings: [{ type: "plain_text", name: "INGEST_ADVERTISED_MAX_RECORDS", text: "500" }] } }),
      };
      const to500 = assertOperatorSnapshot(at(update500), { ...input, sessionIngest: { ...withoutAdvertised, advertisedMaxRecords: 500 } });
      expect(to500).toHaveLength(bindingCount);
      expect(to500.find((e) => e.sid === "INGEST_ADVERTISED_MAX_RECORDS")).toEqual({
        sid: "INGEST_ADVERTISED_MAX_RECORDS", action: "update",
        target: { type: "plain_text", name: "INGEST_ADVERTISED_MAX_RECORDS", text: "500" },
      });
      expect(to500.some((e) => e.action === "delete")).toBe(false);
      expect(() => assertOperatorSnapshot(at(update500), input)).toThrow("native_binding_target");
    }
    if (inflight) {
      const rowsOf = native.native.resources, worker = rowsOf["session-ingest"];
      const leaseBinding = worker.bindings.find((b: any) => b.sid === "INGEST_INFLIGHT_LEASE");
      expect(leaseBinding.data.bindings[0]).toEqual({ type: "plain_text", name: "INGEST_INFLIGHT_LEASE", text: "on" });
      expect(worker.props.env.INGEST_INFLIGHT_LEASE).toBe("on");
      const { INGEST_INFLIGHT_LEASE: _lease, ...unbindEnv } = worker.props.env;
      const { inflightLease: _mode, ...withoutLease } = input.sessionIngest as any;
      const omitted = { ...input, sessionIngest: withoutLease };
      // Rollback to off: live (state) has the value, the candidate omits it, so the plan deletes the binding.
      const unbindWorker = {
        ...worker,
        action: "update",
        props: { ...worker.props, env: unbindEnv },
        state: { status: "updated", props: worker.props },
        bindings: worker.bindings.map((b: any) => b === leaseBinding ? { ...b, action: "delete" } : b),
      };
      const at = (w: any) => ({ ...native, native: { ...native.native, resources: { ...rowsOf, "session-ingest": w } } });
      const withLive = (live: unknown) => ({ ...unbindWorker, state: { ...unbindWorker.state, props: {
        ...unbindWorker.state.props, env: { ...unbindWorker.state.props.env, INGEST_INFLIGHT_LEASE: live } } } });
      const withDeleted = (w: any, value: unknown) => ({ ...w, bindings: w.bindings.map((b: any) =>
        b.sid !== "INGEST_INFLIGHT_LEASE" ? b : { ...b, data: { bindings: [value] } }) });
      // Admitted for live "on" and live "observe"; the delete entry is pinned to the live text.
      for (const live of ["on", "observe"]) {
        const target = { type: "plain_text", name: "INGEST_INFLIGHT_LEASE", text: live };
        const each = assertOperatorSnapshot(at(withDeleted(withLive(live), target)), omitted);
        expect(each).toHaveLength(bindingCount);
        expect(each.find((e) => e.sid === "INGEST_INFLIGHT_LEASE")).toEqual({ sid: "INGEST_INFLIGHT_LEASE", action: "delete", target });
      }
      // The candidate profile still sets the value: the inventory no longer matches, or (with a retained row for
      // it as well) the delete names an expected binding.
      for (const mode of ["observe", "on"] as const) {
        const keeps = { ...input, sessionIngest: { ...withoutLease, inflightLease: mode } };
        expect(() => assertOperatorSnapshot(at(unbindWorker), keeps)).toThrow("native_binding_inventory");
        expect(() => assertOperatorSnapshot(at({ ...unbindWorker, bindings: [...unbindWorker.bindings, {
          sid: "INGEST_INFLIGHT_LEASE", action: "update",
          data: { bindings: [{ type: "plain_text", name: "INGEST_INFLIGHT_LEASE", text: mode }] },
        }] }), keeps)).toThrow("native_binding_target");
      }
      // The proposed env still carries the variable.
      expect(() => assertOperatorSnapshot(at({ ...unbindWorker, props: worker.props }), omitted)).toThrow("native_binding_target");
      // Live text other than exactly "observe" / "on"; the deleted binding is forged to match so only the
      // live-value check can refuse.
      for (const live of ["On", "ON", " on", "on ", "Observe", "off", "1", "", 1, undefined]) {
        expect(() => assertOperatorSnapshot(at(withDeleted(withLive(live), {
          type: "plain_text", name: "INGEST_INFLIGHT_LEASE", text: live })), omitted)).toThrow("native_binding_target");
      }
      // The native binding text differs from the live text, or the binding is not plain text.
      for (const value of [
        { type: "plain_text", name: "INGEST_INFLIGHT_LEASE", text: "observe" },
        { type: "plain_text", name: "FOREIGN", text: "on" },
        { type: "plain_text", name: "INGEST_INFLIGHT_LEASE", text: "on", extra: "x" },
        { type: "secret_text", name: "INGEST_INFLIGHT_LEASE", text: "on" },
      ])
        expect(() => assertOperatorSnapshot(at(withDeleted(unbindWorker, value)), omitted)).toThrow("native_binding_target");
      // The Worker resource row must be an update.
      for (const action of ["create", "noop"])
        expect(() => assertOperatorSnapshot(at({ ...unbindWorker, action }), omitted)).toThrow("native_binding_target");
      // The same delete row under another sid stays refused, with every lease condition satisfied: the candidate
      // omits the value, the live lease value is valid, and the foreign var's live text equals the deleted text.
      const foreign = {
        ...unbindWorker,
        state: { ...unbindWorker.state, props: { ...unbindWorker.state.props, env: {
          ...unbindWorker.state.props.env, INGEST_BUFFER_LOCK_RETRY: "on" } } },
        bindings: [...worker.bindings.filter((b: any) => b !== leaseBinding), { sid: "INGEST_BUFFER_LOCK_RETRY", action: "delete",
          data: { bindings: [{ type: "plain_text", name: "INGEST_BUFFER_LOCK_RETRY", text: "on" }] } }],
      };
      expect(foreign.state.props.env.INGEST_INFLIGHT_LEASE).toBe("on");
      expect(() => assertOperatorSnapshot(at(foreign), omitted)).toThrow("native_binding_target");
      // on -> observe stays an update row checked against the candidate profile alone, with no delete.
      const toObserve = {
        ...worker,
        action: "update",
        props: { ...worker.props, env: { ...worker.props.env, INGEST_INFLIGHT_LEASE: "observe" } },
        state: { status: "updated", props: worker.props },
        bindings: worker.bindings.map((b: any) => b !== leaseBinding ? b : { ...b, action: "update",
          data: { bindings: [{ type: "plain_text", name: "INGEST_INFLIGHT_LEASE", text: "observe" }] } }),
      };
      const observed = assertOperatorSnapshot(at(toObserve), { ...input, sessionIngest: { ...withoutLease, inflightLease: "observe" } });
      expect(observed.some((e) => e.action === "delete")).toBe(false);
      expect(() => assertOperatorSnapshot(at(toObserve), input)).toThrow("native_binding_target");
    }
    if (bufferBusy) {
      const rowsOf = native.native.resources, worker = rowsOf["session-ingest"];
      const busyBinding = worker.bindings.find((b: any) => b.sid === "INGEST_BUFFER_LOCK_BUSY");
      expect(busyBinding.data.bindings[0]).toEqual({ type: "plain_text", name: "INGEST_BUFFER_LOCK_BUSY", text: "on" });
      expect(worker.props.env.INGEST_BUFFER_LOCK_BUSY).toBe("on");
      const { INGEST_BUFFER_LOCK_BUSY: _busy, ...unbindEnv } = worker.props.env;
      const { bufferLockBusy: _flag, ...withoutBusy } = input.sessionIngest as any;
      const omitted = { ...input, sessionIngest: withoutBusy };
      // Rollback to off: live (state) has "on", the candidate omits it, so the plan deletes the binding.
      const unbindWorker = {
        ...worker,
        action: "update",
        props: { ...worker.props, env: unbindEnv },
        state: { status: "updated", props: worker.props },
        bindings: worker.bindings.map((b: any) => b === busyBinding ? { ...b, action: "delete" } : b),
      };
      const at = (w: any) => ({ ...native, native: { ...native.native, resources: { ...rowsOf, "session-ingest": w } } });
      const withLive = (w: any, live: unknown) => ({ ...w, state: { ...w.state, props: {
        ...w.state.props, env: { ...w.state.props.env, INGEST_BUFFER_LOCK_BUSY: live } } } });
      const withDeleted = (w: any, value: unknown) => ({ ...w, bindings: w.bindings.map((b: any) =>
        b.sid !== "INGEST_BUFFER_LOCK_BUSY" ? b : { ...b, data: { bindings: [value] } }) });
      // Admitted: one delete beyond the candidate's expected inventory, pinned to the live text "on".
      const admitted = assertOperatorSnapshot(at(unbindWorker), omitted);
      expect(admitted).toHaveLength(bindingCount);
      expect(admitted.find((e) => e.sid === "INGEST_BUFFER_LOCK_BUSY")).toEqual({
        sid: "INGEST_BUFFER_LOCK_BUSY", action: "delete",
        target: { type: "plain_text", name: "INGEST_BUFFER_LOCK_BUSY", text: "on" },
      });
      expect(admitted.filter((e) => e.action === "delete")).toHaveLength(1);
      // The candidate profile still sets bufferLockBusy: the inventory no longer matches, or (with a retained
      // row for it as well) the delete names an expected binding.
      const keeps = { ...input, sessionIngest: { ...withoutBusy, bufferLockBusy: "on" } };
      expect(() => assertOperatorSnapshot(at(unbindWorker), keeps)).toThrow("native_binding_inventory");
      expect(() => assertOperatorSnapshot(at({ ...unbindWorker, bindings: [...unbindWorker.bindings, {
        sid: "INGEST_BUFFER_LOCK_BUSY", action: "update",
        data: { bindings: [{ type: "plain_text", name: "INGEST_BUFFER_LOCK_BUSY", text: "on" }] },
      }] }), keeps)).toThrow("native_binding_target");
      // A candidate value that renders no binding ("off") keeps the inventory and wanted checks satisfied, so
      // only the predicate's profile-omits clause can refuse.
      expect(() => assertOperatorSnapshot(at(unbindWorker), {
        ...input, sessionIngest: { ...withoutBusy, bufferLockBusy: "off" } } as any)).toThrow("native_binding_target");
      // The proposed env still carries the variable (any value).
      expect(() => assertOperatorSnapshot(at({ ...unbindWorker, props: worker.props }), omitted)).toThrow("native_binding_target");
      for (const proposed of ["off", ""])
        expect(() => assertOperatorSnapshot(at({ ...unbindWorker, props: { ...worker.props,
          env: { ...unbindEnv, INGEST_BUFFER_LOCK_BUSY: proposed } } }), omitted)).toThrow("native_binding_target");
      // Live text other than exactly "on"; the deleted binding is forged to match so only the live-value
      // check can refuse.
      for (const live of ["ON", "On", " on", "on ", "off", "observe", "true", "1", "", 1, true, null, undefined]) {
        expect(() => assertOperatorSnapshot(at(withDeleted(withLive(unbindWorker, live), {
          type: "plain_text", name: "INGEST_BUFFER_LOCK_BUSY", text: live })), omitted)).toThrow("native_binding_target");
      }
      // The native binding differs from the live "on": type mismatch, name mismatch, digest mismatch (other
      // text, a non-string text, an extra key), or not a plain-text binding at all.
      for (const value of [
        { type: "secret_text", name: "INGEST_BUFFER_LOCK_BUSY", text: "on" },
        { type: "plain_text", name: "FOREIGN", text: "on" },
        { type: "plain_text", name: "INGEST_BUFFER_LOCK_RETRY", text: "on" },
        { type: "plain_text", name: "INGEST_BUFFER_LOCK_BUSY", text: "off" },
        { type: "plain_text", name: "INGEST_BUFFER_LOCK_BUSY", text: "ON" },
        { type: "plain_text", name: "INGEST_BUFFER_LOCK_BUSY", text: true },
        { type: "plain_text", name: "INGEST_BUFFER_LOCK_BUSY", text: "on", extra: "x" },
        { type: "queue", name: "INGEST_BUFFER_LOCK_BUSY", queue_name: "foreign" },
      ])
        expect(() => assertOperatorSnapshot(at(withDeleted(unbindWorker, value)), omitted)).toThrow("native_binding_target");
      // An extra data key on the delete row.
      expect(() => assertOperatorSnapshot(at({ ...unbindWorker, bindings: unbindWorker.bindings.map((b: any) =>
        b.sid !== "INGEST_BUFFER_LOCK_BUSY" ? b : { ...b, data: { ...b.data, shim: "x" } }) }), omitted))
        .toThrow("native_binding_target");
      // The Worker resource row must be an update.
      for (const action of ["create", "noop"])
        expect(() => assertOperatorSnapshot(at({ ...unbindWorker, action }), omitted)).toThrow("native_binding_target");
      // A second deletion in the same plan, each one admissible on its own, refuses on the single-delete limit.
      for (const [sid, text] of [["INGEST_INFLIGHT_LEASE", "on"], ["INGEST_ADVERTISED_MAX_RECORDS", "120"]] as const) {
        const second = {
          ...unbindWorker,
          state: { ...unbindWorker.state, props: { ...unbindWorker.state.props, env: {
            ...unbindWorker.state.props.env, [sid]: text } } },
          bindings: [...unbindWorker.bindings, { sid, action: "delete",
            data: { bindings: [{ type: "plain_text", name: sid, text }] } }],
        };
        expect(() => assertOperatorSnapshot(at(second), omitted)).toThrow("native_binding_inventory");
        // Alone, that other deletion is admitted (so only the limit refuses above).
        const alone = { ...second, props: worker.props,
          bindings: [...worker.bindings, { sid, action: "delete", data: { bindings: [{ type: "plain_text", name: sid, text }] } }] };
        expect(assertOperatorSnapshot(at(alone), input).filter((e) => e.action === "delete")).toEqual([
          { sid, action: "delete", target: { type: "plain_text", name: sid, text } }]);
      }
      // No arm for INGEST_CLAIM_LOCK_BUSY (pinned) or INGEST_BUFFER_LOCK_RETRY: a delete of either, with live
      // "on", nothing proposed, the candidate omitting it, and matching native text, still refuses by name.
      for (const sid of ["INGEST_CLAIM_LOCK_BUSY", "INGEST_BUFFER_LOCK_RETRY"]) {
        const foreign = {
          ...worker,
          action: "update",
          state: { status: "updated", props: { ...worker.props, env: { ...worker.props.env, [sid]: "on" } } },
          bindings: [...worker.bindings, { sid, action: "delete",
            data: { bindings: [{ type: "plain_text", name: sid, text: "on" }] } }],
        };
        expect(() => assertOperatorSnapshot(at(foreign), input)).toThrow("native_binding_target");
        // The same delete row in place of an admissible buffer BUSY unbind (live BUSY "on" too).
        const swapped = withLive({ ...unbindWorker,
          state: { ...unbindWorker.state, props: { ...unbindWorker.state.props, env: {
            ...unbindWorker.state.props.env, [sid]: "on" } } },
          bindings: [...worker.bindings.filter((b: any) => b !== busyBinding), { sid, action: "delete",
            data: { bindings: [{ type: "plain_text", name: sid, text: "on" }] } }] }, "on");
        expect(() => assertOperatorSnapshot(at(swapped), omitted)).toThrow("native_binding_target");
      }
    }
    if (timeoutBusy) {
      const rowsOf = native.native.resources, worker = rowsOf["session-ingest"];
      const tbBinding = worker.bindings.find((b: any) => b.sid === "INGEST_BUFFER_TIMEOUT_BUSY");
      expect(tbBinding.data.bindings[0]).toEqual({ type: "plain_text", name: "INGEST_BUFFER_TIMEOUT_BUSY", text: "on" });
      expect(worker.props.env.INGEST_BUFFER_TIMEOUT_BUSY).toBe("on");
      const { INGEST_BUFFER_TIMEOUT_BUSY: _tb, ...unbindEnv } = worker.props.env;
      const { bufferTimeoutBusy: _flag, ...withoutTb } = input.sessionIngest as any;
      const omitted = { ...input, sessionIngest: withoutTb };
      // Rollback to off: live (state) has "on", the candidate omits it, so the plan deletes the binding.
      const unbindWorker = {
        ...worker,
        action: "update",
        props: { ...worker.props, env: unbindEnv },
        state: { status: "updated", props: worker.props },
        bindings: worker.bindings.map((b: any) => b === tbBinding ? { ...b, action: "delete" } : b),
      };
      const at = (w: any) => ({ ...native, native: { ...native.native, resources: { ...rowsOf, "session-ingest": w } } });
      const withLive = (w: any, live: unknown) => ({ ...w, state: { ...w.state, props: {
        ...w.state.props, env: { ...w.state.props.env, INGEST_BUFFER_TIMEOUT_BUSY: live } } } });
      const withDeleted = (w: any, value: unknown) => ({ ...w, bindings: w.bindings.map((b: any) =>
        b.sid !== "INGEST_BUFFER_TIMEOUT_BUSY" ? b : { ...b, data: { bindings: [value] } }) });
      // Admitted: one delete beyond the candidate's expected inventory, pinned to the live text "on".
      const admitted = assertOperatorSnapshot(at(unbindWorker), omitted);
      expect(admitted).toHaveLength(bindingCount);
      expect(admitted.find((e) => e.sid === "INGEST_BUFFER_TIMEOUT_BUSY")).toEqual({
        sid: "INGEST_BUFFER_TIMEOUT_BUSY", action: "delete",
        target: { type: "plain_text", name: "INGEST_BUFFER_TIMEOUT_BUSY", text: "on" },
      });
      expect(admitted.filter((e) => e.action === "delete")).toHaveLength(1);
      // The candidate profile still sets bufferTimeoutBusy: the inventory no longer matches, or (with a retained
      // row for it as well) the delete names an expected binding.
      const keeps = { ...input, sessionIngest: { ...withoutTb, bufferTimeoutBusy: "on" } };
      expect(() => assertOperatorSnapshot(at(unbindWorker), keeps)).toThrow("native_binding_inventory");
      expect(() => assertOperatorSnapshot(at({ ...unbindWorker, bindings: [...unbindWorker.bindings, {
        sid: "INGEST_BUFFER_TIMEOUT_BUSY", action: "update",
        data: { bindings: [{ type: "plain_text", name: "INGEST_BUFFER_TIMEOUT_BUSY", text: "on" }] },
      }] }), keeps)).toThrow("native_binding_target");
      // A candidate value that renders no binding ("off") keeps the inventory and wanted checks satisfied, so
      // only the predicate's profile-omits clause can refuse.
      expect(() => assertOperatorSnapshot(at(unbindWorker), {
        ...input, sessionIngest: { ...withoutTb, bufferTimeoutBusy: "off" } } as any)).toThrow("native_binding_target");
      // The proposed env still carries the variable (any value).
      expect(() => assertOperatorSnapshot(at({ ...unbindWorker, props: worker.props }), omitted)).toThrow("native_binding_target");
      for (const proposed of ["off", ""])
        expect(() => assertOperatorSnapshot(at({ ...unbindWorker, props: { ...worker.props,
          env: { ...unbindEnv, INGEST_BUFFER_TIMEOUT_BUSY: proposed } } }), omitted)).toThrow("native_binding_target");
      // Live text other than exactly "on"; the deleted binding is forged to match so only the live-value
      // check can refuse.
      for (const live of ["ON", "On", " on", "on ", "off", "observe", "true", "1", "", 1, true, null, undefined]) {
        expect(() => assertOperatorSnapshot(at(withDeleted(withLive(unbindWorker, live), {
          type: "plain_text", name: "INGEST_BUFFER_TIMEOUT_BUSY", text: live })), omitted)).toThrow("native_binding_target");
      }
      // The native binding differs from the live "on": type mismatch, name mismatch, digest mismatch (other
      // text, a non-string text, an extra key), or not a plain-text binding at all.
      for (const value of [
        { type: "secret_text", name: "INGEST_BUFFER_TIMEOUT_BUSY", text: "on" },
        { type: "plain_text", name: "FOREIGN", text: "on" },
        { type: "plain_text", name: "INGEST_BUFFER_LOCK_RETRY", text: "on" },
        { type: "plain_text", name: "INGEST_BUFFER_TIMEOUT_BUSY", text: "off" },
        { type: "plain_text", name: "INGEST_BUFFER_TIMEOUT_BUSY", text: "ON" },
        { type: "plain_text", name: "INGEST_BUFFER_TIMEOUT_BUSY", text: true },
        { type: "plain_text", name: "INGEST_BUFFER_TIMEOUT_BUSY", text: "on", extra: "x" },
        { type: "queue", name: "INGEST_BUFFER_TIMEOUT_BUSY", queue_name: "foreign" },
      ])
        expect(() => assertOperatorSnapshot(at(withDeleted(unbindWorker, value)), omitted)).toThrow("native_binding_target");
      // An extra data key on the delete row.
      expect(() => assertOperatorSnapshot(at({ ...unbindWorker, bindings: unbindWorker.bindings.map((b: any) =>
        b.sid !== "INGEST_BUFFER_TIMEOUT_BUSY" ? b : { ...b, data: { ...b.data, shim: "x" } }) }), omitted))
        .toThrow("native_binding_target");
      // The Worker resource row must be an update.
      for (const action of ["create", "noop"])
        expect(() => assertOperatorSnapshot(at({ ...unbindWorker, action }), omitted)).toThrow("native_binding_target");
      // A second deletion in the same plan, each one admissible on its own, refuses on the single-delete limit.
      for (const [sid, text] of [["INGEST_INFLIGHT_LEASE", "on"], ["INGEST_ADVERTISED_MAX_RECORDS", "120"]] as const) {
        const second = {
          ...unbindWorker,
          state: { ...unbindWorker.state, props: { ...unbindWorker.state.props, env: {
            ...unbindWorker.state.props.env, [sid]: text } } },
          bindings: [...unbindWorker.bindings, { sid, action: "delete",
            data: { bindings: [{ type: "plain_text", name: sid, text }] } }],
        };
        expect(() => assertOperatorSnapshot(at(second), omitted)).toThrow("native_binding_inventory");
        // Alone, that other deletion is admitted (so only the limit refuses above).
        const alone = { ...second, props: worker.props,
          bindings: [...worker.bindings, { sid, action: "delete", data: { bindings: [{ type: "plain_text", name: sid, text }] } }] };
        expect(assertOperatorSnapshot(at(alone), input).filter((e) => e.action === "delete")).toEqual([
          { sid, action: "delete", target: { type: "plain_text", name: sid, text } }]);
      }
      // No arm for INGEST_CLAIM_LOCK_BUSY (pinned) or INGEST_BUFFER_LOCK_RETRY: a delete of either, with live
      // "on", nothing proposed, the candidate omitting it, and matching native text, still refuses by name.
      for (const sid of ["INGEST_CLAIM_LOCK_BUSY", "INGEST_BUFFER_LOCK_RETRY"]) {
        const foreign = {
          ...worker,
          action: "update",
          state: { status: "updated", props: { ...worker.props, env: { ...worker.props.env, [sid]: "on" } } },
          bindings: [...worker.bindings, { sid, action: "delete",
            data: { bindings: [{ type: "plain_text", name: sid, text: "on" }] } }],
        };
        expect(() => assertOperatorSnapshot(at(foreign), input)).toThrow("native_binding_target");
        // The same delete row in place of an admissible buffer BUSY unbind (live BUSY "on" too).
        const swapped = withLive({ ...unbindWorker,
          state: { ...unbindWorker.state, props: { ...unbindWorker.state.props, env: {
            ...unbindWorker.state.props.env, [sid]: "on" } } },
          bindings: [...worker.bindings.filter((b: any) => b !== tbBinding), { sid, action: "delete",
            data: { bindings: [{ type: "plain_text", name: sid, text: "on" }] } }] }, "on");
        expect(() => assertOperatorSnapshot(at(swapped), omitted)).toThrow("native_binding_target");
      }
    }
    if (commitWait) {
      const NAME = "INGEST_BUFFER_COMMIT_LOCK_WAIT_MS";
      const rowsOf = native.native.resources, worker = rowsOf["session-ingest"];
      const waitBinding = worker.bindings.find((b: any) => b.sid === NAME);
      // Create: the plan binds exactly the plain-text value.
      expect(waitBinding.data.bindings[0]).toEqual({ type: "plain_text", name: NAME, text: "3000" });
      expect(worker.props.env[NAME]).toBe("3000");
      const { [NAME]: _wait, ...unbindEnv } = worker.props.env;
      const { bufferCommitLockWaitMs: _ms, ...withoutWait } = input.sessionIngest as any;
      const omitted = { ...input, sessionIngest: withoutWait };
      const unbindWorker = {
        ...worker,
        action: "update",
        props: { ...worker.props, env: unbindEnv },
        state: { status: "updated", props: worker.props },
        bindings: worker.bindings.map((b: any) => b === waitBinding ? { ...b, action: "delete" } : b),
      };
      const at = (w: any) => ({ ...native, native: { ...native.native, resources: { ...rowsOf, "session-ingest": w } } });
      const withLive = (w: any, live: unknown) => ({ ...w, state: { ...w.state, props: {
        ...w.state.props, env: { ...w.state.props.env, [NAME]: live } } } });
      const withDeleted = (w: any, value: unknown) => ({ ...w, bindings: w.bindings.map((b: any) =>
        b.sid !== NAME ? b : { ...b, data: { bindings: [value] } }) });
      // Delete (rollback to the role default): admitted, pinned to the live text, the only delete.
      const admitted = assertOperatorSnapshot(at(unbindWorker), omitted);
      expect(admitted).toHaveLength(bindingCount);
      expect(admitted.filter((e) => e.action === "delete")).toEqual([
        { sid: NAME, action: "delete", target: { type: "plain_text", name: NAME, text: "3000" } }]);
      // Every in-range live value is admitted when the deleted binding carries that exact text.
      for (const live of ["2001", "3500", "4000"]) {
        const each = assertOperatorSnapshot(at(withDeleted(withLive(unbindWorker, live), { type: "plain_text", name: NAME, text: live })), omitted);
        expect(each.find((e) => e.sid === NAME)).toEqual({ sid: NAME, action: "delete", target: { type: "plain_text", name: NAME, text: live } });
      }
      // The candidate still sets a value: the inventory no longer matches, or the delete names an expected binding.
      const keeps = { ...input, sessionIngest: { ...withoutWait, bufferCommitLockWaitMs: 3000 } };
      expect(() => assertOperatorSnapshot(at(unbindWorker), keeps)).toThrow("native_binding_inventory");
      expect(() => assertOperatorSnapshot(at({ ...unbindWorker, bindings: [...unbindWorker.bindings, {
        sid: NAME, action: "update", data: { bindings: [{ type: "plain_text", name: NAME, text: "3000" }] },
      }] }), keeps)).toThrow("native_binding_target");
      // The proposed env still carries the variable (any value).
      expect(() => assertOperatorSnapshot(at({ ...unbindWorker, props: worker.props }), omitted)).toThrow("native_binding_target");
      for (const proposed of ["3000", "", "6000"])
        expect(() => assertOperatorSnapshot(at({ ...unbindWorker, props: { ...worker.props,
          env: { ...unbindEnv, [NAME]: proposed } } }), omitted)).toThrow("native_binding_target");
      // Malformed or unknown live values; the deleted binding is forged to match, so only the live-value check refuses.
      for (const live of ["2000", "4001", "6000", "0", "02500", " 3000", "3000 ", "+3000", "3000ms", "3e3", "on", "", 3000, true, null, undefined])
        expect(() => assertOperatorSnapshot(at(withDeleted(withLive(unbindWorker, live), {
          type: "plain_text", name: NAME, text: live })), omitted)).toThrow("native_binding_target");
      // The native binding differs from the live "3000": type, name, text, a non-string text, an extra key, not plain text.
      for (const value of [
        { type: "secret_text", name: NAME, text: "3000" },
        { type: "plain_text", name: "FOREIGN", text: "3000" },
        { type: "plain_text", name: "INGEST_CLAIM_LOCK_WAIT_MS", text: "3000" },
        { type: "plain_text", name: NAME, text: "3500" },
        { type: "plain_text", name: NAME, text: 3000 },
        { type: "plain_text", name: NAME, text: "3000", extra: "x" },
        { type: "queue", name: NAME, queue_name: "foreign" },
      ])
        expect(() => assertOperatorSnapshot(at(withDeleted(unbindWorker, value)), omitted)).toThrow("native_binding_target");
      expect(() => assertOperatorSnapshot(at({ ...unbindWorker, bindings: unbindWorker.bindings.map((b: any) =>
        b.sid !== NAME ? b : { ...b, data: { ...b.data, shim: "x" } }) }), omitted)).toThrow("native_binding_target");
      for (const action of ["create", "noop"])
        expect(() => assertOperatorSnapshot(at({ ...unbindWorker, action }), omitted)).toThrow("native_binding_target");
      // A second deletion in the same plan, each admissible alone, refuses on the single-delete limit.
      const second = {
        ...unbindWorker,
        state: { ...unbindWorker.state, props: { ...unbindWorker.state.props, env: { ...unbindWorker.state.props.env, INGEST_INFLIGHT_LEASE: "on" } } },
        bindings: [...unbindWorker.bindings, { sid: "INGEST_INFLIGHT_LEASE", action: "delete",
          data: { bindings: [{ type: "plain_text", name: "INGEST_INFLIGHT_LEASE", text: "on" }] } }],
      };
      expect(() => assertOperatorSnapshot(at(second), omitted)).toThrow("native_binding_inventory");
      // No arm for INGEST_CLAIM_LOCK_WAIT_MS: the same delete row under that sid still refuses by name.
      const claimWait = {
        ...worker, action: "update",
        state: { status: "updated", props: { ...worker.props, env: { ...worker.props.env, INGEST_CLAIM_LOCK_WAIT_MS: "250" } } },
        bindings: [...worker.bindings, { sid: "INGEST_CLAIM_LOCK_WAIT_MS", action: "delete",
          data: { bindings: [{ type: "plain_text", name: "INGEST_CLAIM_LOCK_WAIT_MS", text: "250" }] } }],
      };
      expect(() => assertOperatorSnapshot(at(claimWait), input)).toThrow("native_binding_target");
      // Update (the live replacement path, 3000 -> 3500): an update row checked against the candidate alone, no delete.
      const longer = {
        ...worker, action: "update",
        props: { ...worker.props, env: { ...worker.props.env, [NAME]: "3500" } },
        state: { status: "updated", props: worker.props },
        bindings: worker.bindings.map((b: any) => b !== waitBinding ? b : { ...b, action: "update",
          data: { bindings: [{ type: "plain_text", name: NAME, text: "3500" }] } }),
      };
      const updated = assertOperatorSnapshot(at(longer), { ...input, sessionIngest: { ...withoutWait, bufferCommitLockWaitMs: 3500 } });
      expect(updated.some((e) => e.action === "delete")).toBe(false);
      expect(updated.find((e) => e.sid === NAME)).toMatchObject({ action: "update" });
      expect(() => assertOperatorSnapshot(at(longer), input)).toThrow("native_binding_target");
    }
    if (commitBudget) {
      const NAME = "INGEST_BUFFER_COMMIT_STATEMENT_BUDGET_MS";
      const rowsOf = native.native.resources, worker = rowsOf["session-ingest"];
      const budgetBinding = worker.bindings.find((b: any) => b.sid === NAME);
      // Create: the plan binds exactly the plain-text value.
      expect(budgetBinding.data.bindings[0]).toEqual({ type: "plain_text", name: NAME, text: "6000" });
      expect(worker.props.env[NAME]).toBe("6000");
      const { [NAME]: _budget, ...unbindEnv } = worker.props.env;
      const { bufferCommitBudgetMs: _ms, ...withoutBudget } = input.sessionIngest as any;
      const omitted = { ...input, sessionIngest: withoutBudget };
      const unbindWorker = {
        ...worker,
        action: "update",
        props: { ...worker.props, env: unbindEnv },
        state: { status: "updated", props: worker.props },
        bindings: worker.bindings.map((b: any) => b === budgetBinding ? { ...b, action: "delete" } : b),
      };
      const at = (w: any) => ({ ...native, native: { ...native.native, resources: { ...rowsOf, "session-ingest": w } } });
      const withLive = (w: any, live: unknown) => ({ ...w, state: { ...w.state, props: {
        ...w.state.props, env: { ...w.state.props.env, [NAME]: live } } } });
      const withDeleted = (w: any, value: unknown) => ({ ...w, bindings: w.bindings.map((b: any) =>
        b.sid !== NAME ? b : { ...b, data: { bindings: [value] } }) });
      // Delete (rollback to the role default): admitted, pinned to the live text, the only delete.
      const admitted = assertOperatorSnapshot(at(unbindWorker), omitted);
      expect(admitted).toHaveLength(bindingCount);
      expect(admitted.filter((e) => e.action === "delete")).toEqual([
        { sid: NAME, action: "delete", target: { type: "plain_text", name: NAME, text: "6000" } }]);
      // Every in-range live value is admitted when the deleted binding carries that exact text.
      for (const live of ["5001", "6500", "7000"]) {
        const each = assertOperatorSnapshot(at(withDeleted(withLive(unbindWorker, live), { type: "plain_text", name: NAME, text: live })), omitted);
        expect(each.find((e) => e.sid === NAME)).toEqual({ sid: NAME, action: "delete", target: { type: "plain_text", name: NAME, text: live } });
      }
      // The candidate still sets a value: the inventory no longer matches, or the delete names an expected binding.
      const keeps = { ...input, sessionIngest: { ...withoutBudget, bufferCommitBudgetMs: 6000 } };
      expect(() => assertOperatorSnapshot(at(unbindWorker), keeps)).toThrow("native_binding_inventory");
      expect(() => assertOperatorSnapshot(at({ ...unbindWorker, bindings: [...unbindWorker.bindings, {
        sid: NAME, action: "update", data: { bindings: [{ type: "plain_text", name: NAME, text: "6000" }] },
      }] }), keeps)).toThrow("native_binding_target");
      // The proposed env still carries the variable (any value).
      expect(() => assertOperatorSnapshot(at({ ...unbindWorker, props: worker.props }), omitted)).toThrow("native_binding_target");
      for (const proposed of ["6000", "", "9000"])
        expect(() => assertOperatorSnapshot(at({ ...unbindWorker, props: { ...worker.props,
          env: { ...unbindEnv, [NAME]: proposed } } }), omitted)).toThrow("native_binding_target");
      // Malformed or unknown live values; the deleted binding is forged to match, so only the live-value check refuses.
      for (const live of ["5000", "7001", "4000", "0", "06000", " 6000", "6000 ", "+6000", "6000ms", "6e3", "on", "", 6000, true, null, undefined])
        expect(() => assertOperatorSnapshot(at(withDeleted(withLive(unbindWorker, live), {
          type: "plain_text", name: NAME, text: live })), omitted)).toThrow("native_binding_target");
      // The native binding differs from the live "6000": type, name, text, a non-string text, an extra key, not plain text.
      for (const value of [
        { type: "secret_text", name: NAME, text: "6000" },
        { type: "plain_text", name: "FOREIGN", text: "6000" },
        { type: "plain_text", name: "INGEST_CLAIM_LOCK_WAIT_MS", text: "6000" },
        { type: "plain_text", name: NAME, text: "6500" },
        { type: "plain_text", name: NAME, text: 6000 },
        { type: "plain_text", name: NAME, text: "6000", extra: "x" },
        { type: "queue", name: NAME, queue_name: "foreign" },
      ])
        expect(() => assertOperatorSnapshot(at(withDeleted(unbindWorker, value)), omitted)).toThrow("native_binding_target");
      expect(() => assertOperatorSnapshot(at({ ...unbindWorker, bindings: unbindWorker.bindings.map((b: any) =>
        b.sid !== NAME ? b : { ...b, data: { ...b.data, shim: "x" } }) }), omitted)).toThrow("native_binding_target");
      for (const action of ["create", "noop"])
        expect(() => assertOperatorSnapshot(at({ ...unbindWorker, action }), omitted)).toThrow("native_binding_target");
      // A second deletion in the same plan, each admissible alone, refuses on the single-delete limit.
      const second = {
        ...unbindWorker,
        state: { ...unbindWorker.state, props: { ...unbindWorker.state.props, env: { ...unbindWorker.state.props.env, INGEST_INFLIGHT_LEASE: "on" } } },
        bindings: [...unbindWorker.bindings, { sid: "INGEST_INFLIGHT_LEASE", action: "delete",
          data: { bindings: [{ type: "plain_text", name: "INGEST_INFLIGHT_LEASE", text: "on" }] } }],
      };
      expect(() => assertOperatorSnapshot(at(second), omitted)).toThrow("native_binding_inventory");
      // No arm for INGEST_CLAIM_LOCK_WAIT_MS: the same delete row under that sid still refuses by name.
      const claimWait = {
        ...worker, action: "update",
        state: { status: "updated", props: { ...worker.props, env: { ...worker.props.env, INGEST_CLAIM_LOCK_WAIT_MS: "250" } } },
        bindings: [...worker.bindings, { sid: "INGEST_CLAIM_LOCK_WAIT_MS", action: "delete",
          data: { bindings: [{ type: "plain_text", name: "INGEST_CLAIM_LOCK_WAIT_MS", text: "250" }] } }],
      };
      expect(() => assertOperatorSnapshot(at(claimWait), input)).toThrow("native_binding_target");
      // Update (the live replacement path, 3000 -> 3500): an update row checked against the candidate alone, no delete.
      const longer = {
        ...worker, action: "update",
        props: { ...worker.props, env: { ...worker.props.env, [NAME]: "6500" } },
        state: { status: "updated", props: worker.props },
        bindings: worker.bindings.map((b: any) => b !== budgetBinding ? b : { ...b, action: "update",
          data: { bindings: [{ type: "plain_text", name: NAME, text: "6500" }] } }),
      };
      const updated = assertOperatorSnapshot(at(longer), { ...input, sessionIngest: { ...withoutBudget, bufferCommitBudgetMs: 6500 } });
      expect(updated.some((e) => e.action === "delete")).toBe(false);
      expect(updated.find((e) => e.sid === NAME)).toMatchObject({ action: "update" });
      expect(() => assertOperatorSnapshot(at(longer), input)).toThrow("native_binding_target");
    }
    if (custody && !frozenReceipt && !neonExit) {
      const rowsOf = native.native.resources, worker = rowsOf["session-ingest"];
      const custodyBinding = worker.bindings.find((b: any) => b.sid === "INGEST_CUSTODY_SOURCE");
      const { INGEST_CUSTODY_SOURCE: _custody, ...custodyRollbackEnv } = worker.props.env;
      const { custodySource: _source, ...withoutCustody } = input.sessionIngest as any;
      const custodyRollbackWorker = {
        ...worker,
        action: "update",
        props: { ...worker.props, env: custodyRollbackEnv },
        state: { status: "updated", props: worker.props },
        bindings: worker.bindings.map((b: any) => b === custodyBinding
          ? { ...b, action: "delete" }
          : b),
      };
      expect(assertOperatorSnapshot({ ...native, native: { ...native.native, resources: {
        ...rowsOf, "session-ingest": custodyRollbackWorker,
      } } }, { ...input, sessionIngest: withoutCustody })).toHaveLength(bindingCount);
      expect(() => assertOperatorSnapshot({ ...native, native: { ...native.native, resources: {
        ...rowsOf,
        "session-ingest": {
          ...custodyRollbackWorker,
          state: { ...custodyRollbackWorker.state, props: {
            ...custodyRollbackWorker.state.props,
            env: {
              ...custodyRollbackWorker.state.props.env,
              INGEST_SYNC_ADMISSION_FROZEN_RECEIPT: "on",
            },
          } },
        },
      } } }, { ...input, sessionIngest: withoutCustody })).toThrow("native_binding_target");
      // Forge the old env AND deleted binding consistently: only the known custody
      // mode guard, not an incidental env/render disagreement, must refuse these.
      for (const oldMode of ["foreign", "SHADOW", "shadow "]) {
        const forged = {
          ...custodyRollbackWorker,
          state: { ...custodyRollbackWorker.state, props: {
            ...custodyRollbackWorker.state.props,
            env: { ...custodyRollbackWorker.state.props.env, INGEST_CUSTODY_SOURCE: oldMode },
          } },
          bindings: custodyRollbackWorker.bindings.map((b: any) => b.sid === "INGEST_CUSTODY_SOURCE"
            ? { ...b, data: { ...b.data, bindings: b.data.bindings.map((v: any) => ({ ...v, text: oldMode })) } }
            : b),
        };
        expect(() => assertOperatorSnapshot({ ...native, native: { ...native.native, resources: {
          ...rowsOf, "session-ingest": forged,
        } } }, { ...input, sessionIngest: withoutCustody })).toThrow("native_binding_target");
      }
    }
    if (mode === "buffered") {
      const worker = native.native.resources["session-ingest"];
      expect(worker.props.env.INGEST_EXPECT_GENERATION).toBe("e".repeat(64));
      const generationBinding = worker.bindings.find((b: any) => b.sid === "INGEST_EXPECT_GENERATION");
      expect(generationBinding.data.bindings[0]).toEqual({
        type: "plain_text", name: "INGEST_EXPECT_GENERATION", text: "e".repeat(64),
      });
      const changedInput = { ...input, sessionIngest: { ...input.sessionIngest, expectedGeneration: "f".repeat(64) } };
      expect(operatorCapture(source, JSON.stringify(changedInput), secrets, result.plans, tools, result.authority).generation)
        .not.toBe(capture.generation);
      for (const change of ["missing", "wrong"]) {
        const rows = { ...native.native.resources };
        rows["session-ingest"] = { ...worker, bindings: worker.bindings.map((b: any) =>
          b.sid !== "INGEST_EXPECT_GENERATION" ? b : {
            ...b, data: { bindings: [{ ...b.data.bindings[0], text: "f".repeat(64) }] },
          }).filter((b: any) => change !== "missing" || b.sid !== "INGEST_EXPECT_GENERATION") };
        expect(() => assertOperatorSnapshot({ ...native, native: { ...native.native, resources: rows } }, input))
          .toThrow(change === "missing" ? "native_binding_inventory" : "native_binding_target");
      }
      expect(queue.props.deliveryPaused).toBe(true);
      expect(queue.provider.version).toBe(1);
      expect(queue.downstream).toContain("session-ingest-fanout-consumer");

      // The one permitted binding deletion is the exact G -> H overlap
      // closure: old expected G + accepted G,H becomes expected H alone.
      const oldExpected = "f".repeat(64),
        newExpected = "e".repeat(64),
        oldAccepted = `${oldExpected},${newExpected}`,
        deletedOverlap = {
          sid: "INGEST_ACCEPT_GENERATIONS",
          action: "delete",
          data: {
            bindings: [{
              type: "plain_text",
              name: "INGEST_ACCEPT_GENERATIONS",
              text: oldAccepted,
            }],
          },
        },
        cleanupWorker = {
          ...worker,
          action: "update",
          state: {
            status: "updated",
            props: {
              ...worker.props,
              env: {
                ...worker.props.env,
                INGEST_EXPECT_GENERATION: oldExpected,
                INGEST_ACCEPT_GENERATIONS: oldAccepted,
              },
            },
          },
          bindings: [...worker.bindings, deletedOverlap],
        },
        cleanupRows = {
          ...native.native.resources,
          "session-ingest": cleanupWorker,
        },
        cleanupSnapshot = {
          ...native,
          native: { ...native.native, resources: cleanupRows },
        };
      expect(assertOperatorSnapshot(cleanupSnapshot, input)).toHaveLength(
        bindingCount + 1,
      );
      for (const invalid of [
        { ...deletedOverlap, sid: "INGEST_CLAIM_LOCK_BUSY" },
        { ...deletedOverlap, action: "update" },
        { ...deletedOverlap, data: { bindings: [{
          ...deletedOverlap.data.bindings[0], text: `${newExpected},${oldExpected}`,
        }] } },
        { ...deletedOverlap, data: { bindings: [{
          ...deletedOverlap.data.bindings[0], name: "FOREIGN",
        }] } },
      ]) {
        const rows = {
          ...cleanupRows,
          "session-ingest": {
            ...cleanupWorker,
            bindings: [...worker.bindings, invalid],
          },
        };
        expect(() => assertOperatorSnapshot(
          { ...native, native: { ...native.native, resources: rows } }, input,
        )).toThrow();
      }
      const reversedState = {
        ...cleanupWorker,
        state: {
          ...cleanupWorker.state,
          props: {
            ...cleanupWorker.state.props,
            env: {
              ...cleanupWorker.state.props.env,
              INGEST_ACCEPT_GENERATIONS: `${newExpected},${oldExpected}`,
            },
          },
        },
      };
      expect(() => assertOperatorSnapshot({
        ...native,
        native: {
          ...native.native,
          resources: { ...cleanupRows, "session-ingest": reversedState },
        },
      }, input)).toThrow("native_binding_target");
    }
    for (const change of ["binding", "target", "type", "action", "resource"]) {
      const rows = { ...native.native.resources },
        original = rows["session-ingest"];
      const row = {
        ...original,
        resource: { ...original.resource },
        bindings: original.bindings.map((b: any) => ({
          ...b,
          data: {
            ...b.data,
            bindings: b.data.bindings.map((x: any) => ({ ...x })),
          },
        })),
      };
      rows["session-ingest"] = row;
      if (change === "binding")
        row.bindings.push({
          sid: "FOREIGN",
          action: "create",
          data: {
            bindings: [{ type: "plain_text", name: "FOREIGN", text: "x" }],
          },
        });
      if (change === "target")
        row.bindings.find(
          (b: any) => b.sid === "HYPERDRIVE",
        ).data.bindings[0].id = "f".repeat(32);
      if (change === "type")
        row.bindings.find(
          (b: any) => b.sid === "HYPERDRIVE",
        ).data.bindings[0].type = "service";
      if (change === "action") row.action = "replace";
      if (change === "resource") row.resource.Type = "Cloudflare.KVNamespace";
      expect(() =>
        assertOperatorSnapshot(
          { ...native, native: { ...native.native, resources: rows } },
          input,
        ),
      ).toThrow();
    }
  } finally {
    watched.mockRestore();
    for (const [k, v] of Object.entries(old)) {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    }
    await rm(dir, { recursive: true, force: true });
  }
}
test.each(["canonical", "buffered", "admission", "admission-default", "admission-drain", "custody", "custody-shadow", "custody-shadow-drain", "frozen-receipt", "neon-exit", "advertised", "inflight", "buffer-busy", "buffer-commit-wait", "buffer-commit-budget", "buffer-timeout-busy"] as const)(
  "actual original four-resource declaration %s uses authenticated HTTP state and retained plan",
  actualPlanProof,
);
test.each(["version", "missing", "foreign"] as const)(
  "state %s prerequisite refuses without a refresh helper",
  async (mode) => {
    const remote: any = {
      getVersion: () => Effect.succeed(mode === "version" ? 6 : 7),
      get: () =>
        Effect.succeed(
          mode === "missing"
            ? undefined
            : {
                ...control,
                attr: {
                  ...control.attr,
                  accountId:
                    mode === "foreign" ? "f".repeat(32) : secrets.accountId,
                },
              },
        ),
    };
    await expect(
      Effect.runPromise(requireStateAuthority(remote, secrets)),
    ).rejects.toThrow();
  },
);
test("state URL/source scope refuse alternate account surface or dirty source", () => {
  for (const url of [
    "http://alchemy-state-store-96bulls.fixture.workers.dev",
    "https://foreign.fixture.workers.dev",
    "https://alchemy-state-store-96bulls.fixture.workers.dev/path",
  ])
    expect(() => operatorStateUrl(url)).toThrow();
  expect(() =>
    assertOperatorSource(
      { mode: "plan", sourceSha: source.commit, inputsSha256: "a".repeat(64) },
      { ...source, changedFiles: ["app.ts"] },
    ),
  ).toThrow();
});
test("plan transport cannot mutate CF, preview, secrets or another account", async () => {
  let count = 0;
  const fetcher: typeof fetch = async () => {
    count++;
    return Response.json({ success: true });
  };
  const guarded = guardOperatorFetch(secrets, "plan", fetcher),
    root =
      "https://api.cloudflare.com/client/v4/accounts/" +
      secrets.accountId +
      "/";
  for (const path of [
    "workers/scripts/96bulls-session-ingest",
    "queues",
    "workers/subdomain/edge-preview",
    "secrets_store/stores",
  ])
    await expect(guarded(root + path, { method: "POST" })).rejects.toThrow();
  expect(count).toBe(0);
});
test("capture/approval binds both plans, native state authority and explicit operator exception", () => {
  const plans = { planText: "safe compact", detailedText: "safe detailed" },
    text = JSON.stringify(input),
    capture = operatorCapture(source, text, secrets, plans, tools, authority),
    provenance = JSON.stringify(capture),
    approved = {
      ...input,
      approval: {
        decision: "approved",
        by: "Khaliq Gant",
        date: new Date().toISOString().slice(0, 10),
        quote: "Explicitly approve retained-plan operator exception only",
        execution: EXECUTION_EXCEPTION,
        capture: {
          provenanceSha256: digest(provenance),
          planSha256: digest(plans.planText),
          inputsSha256: inputsDigest(text),
          sourceCommit: source.commit,
        },
      },
    };
  const options = {
    mode: "deploy" as const,
    sourceSha: source.commit,
    inputsSha256: inputsDigest(text),
    provenanceSha256: digest(provenance),
  };
  expect(
    verifyOperatorCapture(
      options,
      source,
      JSON.stringify(approved),
      secrets,
      plans,
      provenance,
      tools,
      authority,
    ).generation,
  ).toBe(capture.generation);
  expect(() =>
    verifyOperatorCapture(
      options,
      source,
      JSON.stringify(approved),
      secrets,
      { ...plans, detailedText: "changed" },
      provenance,
      tools,
      authority,
    ),
  ).toThrow();
  expect(() =>
    verifyOperatorCapture(
      options,
      source,
      JSON.stringify(approved),
      secrets,
      plans,
      provenance,
      tools,
      { ...authority, controlStateSha256: "a".repeat(64) },
    ),
  ).toThrow();
  expect(() =>
    verifyOperatorCapture(
      options,
      source,
      JSON.stringify({
        ...approved,
        approval: { ...approved.approval, execution: "future-ci" },
      }),
      secrets,
      plans,
      provenance,
      tools,
      authority,
    ),
  ).toThrow();
});

test("retained deploy rejects a missing actual capture before any transport", async () => {
  let called = 0;
  const fetcher: typeof fetch = async () => {
    called++;
    throw Error("network must not run");
  };
  await expect(
    retainedOperatorPlan(secrets, "deploy", input, () => {}, {
      directory: "unused",
      emptyEnvPath: "unused",
      fetcher,
    }),
  ).rejects.toMatchObject({ code: "retained_deploy_approval_required" });
  expect(called).toBe(0);
});

test("even an imported retained deploy refuses outside the guarded GitHub Actions lane", async () => {
  let called = 0;
  const before = {
    CI: process.env.CI,
    CLOUDFLARE_ACCOUNT_ID: process.env.CLOUDFLARE_ACCOUNT_ID,
    CLOUDFLARE_API_TOKEN: process.env.CLOUDFLARE_API_TOKEN,
    GITHUB_ACTIONS: process.env.GITHUB_ACTIONS,
    PRIVATE_INGEST_GITHUB_GATE: process.env.PRIVATE_INGEST_GITHUB_GATE,
  };
  process.env.CI = "true";
  process.env.CLOUDFLARE_ACCOUNT_ID = secrets.accountId;
  process.env.CLOUDFLARE_API_TOKEN = secrets.apiToken;
  delete process.env.GITHUB_ACTIONS;
  delete process.env.PRIVATE_INGEST_GITHUB_GATE;
  try {
    await expect(retainedOperatorPlan(secrets, "deploy", input, () => {}, {
      directory: "unused",
      emptyEnvPath: "unused",
      fetcher: async () => { called++; throw Error("network must not run"); },
      reviewed: {
        options: { mode: "deploy", sourceSha: source.commit, inputsSha256: "a".repeat(64) },
        source,
        inputText: JSON.stringify(input),
        provenanceText: "{}",
        tools,
        recheckSource: () => {},
      },
    })).rejects.toMatchObject({ code: "github_actions_deploy_required" });
    expect(called).toBe(0);
  } finally {
    for (const [key, value] of Object.entries(before)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
});

test("deployment transport refuses public URLs, foreign queue/state writes and control-store upgrades before network", async () => {
  let count = 0;
  const base: typeof fetch = async () => {
    count++;
    return Response.json({ success: true });
  };
  const guarded = guardOperatorFetch(secrets, "deploy", base),
    root =
      "https://api.cloudflare.com/client/v4/accounts/" +
      secrets.accountId +
      "/";
  for (const [path, method, body] of [
    ["workers/scripts/alchemy-state-store-96bulls", "PUT", "bundle"],
    [
      "workers/scripts/96bulls-session-ingest/subdomain",
      "POST",
      JSON.stringify({ enabled: true, previews_enabled: true }),
    ],
    ["queues", "POST", JSON.stringify({ queue_name: "foreign" })],
    [
      "queues/" + "f".repeat(32) + "/consumers",
      "POST",
      JSON.stringify({ script_name: "96bulls-session-ingest" }),
    ],
    ["workers/domains/foreign", "DELETE", undefined],
  ])
    await expect(guarded(root + path, { method, body })).rejects.toThrow();
  await expect(
    guarded(
      secrets.stateUrl +
        "/state/stacks/CloudflareStateStore/stages/alchemy-state-store-96bulls/resources/Api",
      { method: "PUT", body: "{}" },
    ),
  ).rejects.toThrow();
  expect(count).toBe(0);
});

test("queue metadata over 64 KiB is observed; over the 2 MiB bound it refuses", async () => {
  const fanout = { queue_name: "96bulls-session-ingest-fanout", queue_id: "d".repeat(32) };
  const filler = (n: number) => Array.from({ length: n }, (_, i) => ({ queue_name: "other-queue-" + i, queue_id: "e".repeat(32), pad: "x".repeat(200) }));
  const listing = (rows: unknown[]) => async () => new Response(JSON.stringify({ success: true, result: rows }), { status: 200 });
  const url = "https://api.cloudflare.com/client/v4/accounts/" + secrets.accountId + "/queues";
  const big = [...filler(600), fanout];
  expect(JSON.stringify(big).length).toBeGreaterThan(65536);
  const ok = await guardOperatorFetch(secrets, "plan", listing(big) as never)(url);
  expect(ok.status).toBe(200);
  const huge = filler(Math.ceil(OPERATOR_METADATA_MAX_BYTES / 200) + 100);
  await expect(guardOperatorFetch(secrets, "plan", listing(huge) as never)(url)).rejects.toThrow("metadata_bound");
});

describe("operatorSafePlans: async admission follows the resolved mode; a deployed Worker gains it with drain only", () => {
  const FOUR = ["session-ingest-fanout-dlq", "session-ingest-fanout", "session-ingest", "session-ingest-fanout-consumer"];
  const SEVEN = [...FOUR, "session-ingest-admission-dlq", "session-ingest-admission", "session-ingest-admission-consumer"];
  /** A synthetic retained plan: resource lines, the Worker's binding lines and a matching summary. */
  const plans = (resources: string[], actions: Record<string, string>, bindings: Array<{ sid: string; action: string; target: Record<string, unknown> }>) => {
    const counts = new Map<string, number>();
    for (const r of resources) {
      const a = actions[r] ?? "noop";
      if (a !== "noop") counts.set(a, (counts.get(a) ?? 0) + 1);
    }
    const changed = bindings.filter((b) => b.action !== "noop").length;
    const clauses = [...counts].map(([a, n]) => `${n} to ${a}`);
    const summary = `Plan: ${clauses.length ? clauses.join(", ") : "0 changes"}${changed ? `, ${changed} binding changes` : ""}`;
    const lines = [summary, ...resources.map((r) => `[${r}] ${actions[r] ?? "noop"}`), ...bindings.map((b) => `[session-ingest/${b.sid}] ${b.action === "delete" ? "unbind" : b.action}`)];
    const text = lines.join("\n") + "\n";
    return { planText: text, detailedText: lines.map((l) => (l.startsWith("[") ? `  ${l}` : l)).join("\n") + "\n" };
  };
  const queue = (action: string) => ({ sid: "INGEST_ADMISSION", action, target: { type: "queue", name: "INGEST_ADMISSION", queue_name: "96bulls-session-ingest-admission", logicalId: "session-ingest-admission" } });
  const flag = (action: string, text: string) => ({ sid: "INGEST_ASYNC_ADMISSION", action, target: { type: "plain_text", name: "INGEST_ASYNC_ADMISSION", text } });
  const run = (resources: string[], actions: Record<string, string>, bindings: Array<{ sid: string; action: string; target: Record<string, unknown> }>) =>
    operatorSafePlans(plans(resources, actions, bindings), secrets, bindings);
  const addition = { "session-ingest-admission-dlq": "create", "session-ingest-admission": "create", "session-ingest-admission-consumer": "create", "session-ingest": "update" };

  test("addition phase (deployed Worker gains the three resources): drain ok, on refused", () => {
    expect(() => run(SEVEN, addition, [queue("create"), flag("create", "drain")])).not.toThrow();
    expect(() => run(SEVEN, addition, [queue("create"), flag("create", "on")])).toThrow("original_plan_contract");
  });

  test("re-capture after a partial addition (queues exist, Worker not updated) plans as an update: on refused, drain ok", () => {
    const update = { "session-ingest": "update" };
    expect(() => run(SEVEN, update, [queue("create"), flag("create", "on")])).toThrow("admission_drain_first");
    expect(() => run(SEVEN, update, [queue("create"), flag("create", "drain")])).not.toThrow();
  });

  test("drain at 100%, then the var-only flip to on (or omitted) is an update with the queue binding unchanged", () => {
    expect(() => run(SEVEN, { "session-ingest": "update" }, [queue("noop"), flag("update", "on")])).not.toThrow();
    expect(() => run(SEVEN, {}, [queue("noop"), flag("noop", "on")])).not.toThrow();
  });

  test("generation overlap cleanup renders one reviewed binding deletion", () => {
    const generation = "e".repeat(64), overlap = "f".repeat(64);
    // Alchemy emits this env-only Worker change as resource noop plus binding
    // update/delete rows. The verified adapter presents it to the original
    // stage guard as the in-place Worker update it represents.
    const cleanupBindings = [
      queue("noop"),
      flag("noop", "drain"),
      {
        sid: "INGEST_ACCEPT_GENERATIONS",
        action: "delete",
        target: {
          type: "plain_text",
          name: "INGEST_ACCEPT_GENERATIONS",
          text: `${overlap},${generation}`,
        },
      },
    ];
    expect(() => run(SEVEN, {}, cleanupBindings)).not.toThrow();
    const rendered = plans(SEVEN, {}, cleanupBindings);
    expect(rendered.planText).toContain(
      "[session-ingest/INGEST_ACCEPT_GENERATIONS] unbind",
    );
    const wrongVerb = {
      planText: rendered.planText.replace(
        "[session-ingest/INGEST_ACCEPT_GENERATIONS] unbind",
        "[session-ingest/INGEST_ACCEPT_GENERATIONS] delete",
      ),
      detailedText: rendered.detailedText.replace(
        "[session-ingest/INGEST_ACCEPT_GENERATIONS] unbind",
        "[session-ingest/INGEST_ACCEPT_GENERATIONS] delete",
      ),
    };
    expect(() => operatorSafePlans(
      wrongVerb,
      secrets,
      cleanupBindings,
    )).toThrow("binding_render_inventory");
  });

  test("advertised limit removal renders one reviewed binding deletion", () => {
    const unbind = {
      sid: "INGEST_ADVERTISED_MAX_RECORDS",
      action: "delete",
      target: { type: "plain_text", name: "INGEST_ADVERTISED_MAX_RECORDS", text: "120" },
    };
    const removal = [queue("noop"), flag("noop", "drain"), unbind];
    expect(() => run(SEVEN, {}, removal)).not.toThrow();
    const rendered = plans(SEVEN, {}, removal);
    expect(rendered.planText).toContain("[session-ingest/INGEST_ADVERTISED_MAX_RECORDS] unbind");
    expect(rendered.planText).toContain(", 1 binding changes");
    const altered = (from: string, to: string) => ({
      planText: rendered.planText.replace(from, to),
      detailedText: rendered.detailedText.replace(from, to),
    });
    expect(() => operatorSafePlans(altered(
      "[session-ingest/INGEST_ADVERTISED_MAX_RECORDS] unbind",
      "[session-ingest/INGEST_ADVERTISED_MAX_RECORDS] delete",
    ), secrets, removal)).toThrow("binding_render_inventory");
    expect(() => operatorSafePlans(altered(", 1 binding changes", ", 2 binding changes"), secrets, removal))
      .toThrow("binding_render_summary");
  });

  test("in-flight lease removal renders one reviewed binding deletion", () => {
    const unbind = {
      sid: "INGEST_INFLIGHT_LEASE",
      action: "delete",
      target: { type: "plain_text", name: "INGEST_INFLIGHT_LEASE", text: "on" },
    };
    const removal = [queue("noop"), flag("noop", "drain"), unbind];
    expect(() => run(SEVEN, {}, removal)).not.toThrow();
    const rendered = plans(SEVEN, {}, removal);
    expect(rendered.planText).toContain("[session-ingest/INGEST_INFLIGHT_LEASE] unbind");
    expect(rendered.planText).toContain(", 1 binding changes");
  });

  test("buffer lock-wait busy removal renders one reviewed binding deletion", () => {
    const unbind = {
      sid: "INGEST_BUFFER_LOCK_BUSY",
      action: "delete",
      target: { type: "plain_text", name: "INGEST_BUFFER_LOCK_BUSY", text: "on" },
    };
    const removal = [queue("noop"), flag("noop", "drain"), unbind];
    expect(() => run(SEVEN, {}, removal)).not.toThrow();
    const rendered = plans(SEVEN, {}, removal);
    expect(rendered.planText).toContain("[session-ingest/INGEST_BUFFER_LOCK_BUSY] unbind");
    expect(rendered.planText).toContain(", 1 binding changes");
    const altered = (from: string, to: string) => ({
      planText: rendered.planText.replace(from, to),
      detailedText: rendered.detailedText.replace(from, to),
    });
    expect(() => operatorSafePlans(altered(
      "[session-ingest/INGEST_BUFFER_LOCK_BUSY] unbind",
      "[session-ingest/INGEST_BUFFER_LOCK_BUSY] delete",
    ), secrets, removal)).toThrow("binding_render_inventory");
    expect(() => operatorSafePlans(altered(", 1 binding changes", ", 2 binding changes"), secrets, removal))
      .toThrow("binding_render_summary");
  });

  test("buffer commit lock wait removal renders one reviewed binding deletion", () => {
    const unbind = {
      sid: "INGEST_BUFFER_COMMIT_LOCK_WAIT_MS",
      action: "delete",
      target: { type: "plain_text", name: "INGEST_BUFFER_COMMIT_LOCK_WAIT_MS", text: "3000" },
    };
    const removal = [queue("noop"), flag("noop", "drain"), unbind];
    expect(() => run(SEVEN, {}, removal)).not.toThrow();
    const rendered = plans(SEVEN, {}, removal);
    expect(rendered.planText).toContain("[session-ingest/INGEST_BUFFER_COMMIT_LOCK_WAIT_MS] unbind");
    expect(rendered.planText).toContain(", 1 binding changes");
    const altered = (from: string, to: string) => ({
      planText: rendered.planText.replace(from, to),
      detailedText: rendered.detailedText.replace(from, to),
    });
    expect(() => operatorSafePlans(altered(
      "[session-ingest/INGEST_BUFFER_COMMIT_LOCK_WAIT_MS] unbind",
      "[session-ingest/INGEST_BUFFER_COMMIT_LOCK_WAIT_MS] delete",
    ), secrets, removal)).toThrow("binding_render_inventory");
    expect(() => operatorSafePlans(altered(", 1 binding changes", ", 2 binding changes"), secrets, removal))
      .toThrow("binding_render_summary");
  });

  test("buffer commit statement budget removal renders one reviewed binding deletion", () => {
    const unbind = {
      sid: "INGEST_BUFFER_COMMIT_STATEMENT_BUDGET_MS",
      action: "delete",
      target: { type: "plain_text", name: "INGEST_BUFFER_COMMIT_STATEMENT_BUDGET_MS", text: "6000" },
    };
    const removal = [queue("noop"), flag("noop", "drain"), unbind];
    expect(() => run(SEVEN, {}, removal)).not.toThrow();
    const rendered = plans(SEVEN, {}, removal);
    expect(rendered.planText).toContain("[session-ingest/INGEST_BUFFER_COMMIT_STATEMENT_BUDGET_MS] unbind");
    expect(rendered.planText).toContain(", 1 binding changes");
    const altered = (from: string, to: string) => ({
      planText: rendered.planText.replace(from, to),
      detailedText: rendered.detailedText.replace(from, to),
    });
    expect(() => operatorSafePlans(altered(
      "[session-ingest/INGEST_BUFFER_COMMIT_STATEMENT_BUDGET_MS] unbind",
      "[session-ingest/INGEST_BUFFER_COMMIT_STATEMENT_BUDGET_MS] delete",
    ), secrets, removal)).toThrow("binding_render_inventory");
    expect(() => operatorSafePlans(altered(", 1 binding changes", ", 2 binding changes"), secrets, removal))
      .toThrow("binding_render_summary");
  });

  test("buffer timeout busy removal renders one reviewed binding deletion", () => {
    const unbind = {
      sid: "INGEST_BUFFER_TIMEOUT_BUSY",
      action: "delete",
      target: { type: "plain_text", name: "INGEST_BUFFER_TIMEOUT_BUSY", text: "on" },
    };
    const removal = [queue("noop"), flag("noop", "drain"), unbind];
    expect(() => run(SEVEN, {}, removal)).not.toThrow();
    const rendered = plans(SEVEN, {}, removal);
    expect(rendered.planText).toContain("[session-ingest/INGEST_BUFFER_TIMEOUT_BUSY] unbind");
    expect(rendered.planText).toContain(", 1 binding changes");
    const altered = (from: string, to: string) => ({
      planText: rendered.planText.replace(from, to),
      detailedText: rendered.detailedText.replace(from, to),
    });
    expect(() => operatorSafePlans(altered(
      "[session-ingest/INGEST_BUFFER_TIMEOUT_BUSY] unbind",
      "[session-ingest/INGEST_BUFFER_TIMEOUT_BUSY] delete",
    ), secrets, removal)).toThrow("binding_render_inventory");
    expect(() => operatorSafePlans(altered(", 1 binding changes", ", 2 binding changes"), secrets, removal))
      .toThrow("binding_render_summary");
  });

  test("a refused contract exposes actions only, never binding values", () => {
    let refusal: unknown;
    try {
      run(SEVEN, {
        "session-ingest-fanout": "create",
        "session-ingest": "update",
      }, [queue("noop"), flag("noop", "drain")]);
    } catch (error) {
      refusal = error;
    }
    expect(refusal).toBeInstanceOf(OperatorRefusal);
    const diagnostic = (refusal as OperatorRefusal).diagnostic!;
    expect(diagnostic.kind).toBe("plan_contract");
    if (diagnostic.kind !== "plan_contract") throw new Error("wrong diagnostic kind");
    expect(diagnostic.reason).toBe("action_set");
    expect(diagnostic.compact.resources).toContainEqual({
      id: "session-ingest",
      action: "update",
    });
    expect(diagnostic.bindings).toEqual(expect.arrayContaining([
      { sid: "INGEST_ADMISSION", action: "noop" },
      { sid: "INGEST_ASYNC_ADMISSION", action: "noop" },
    ]));
    expect(JSON.stringify(diagnostic)).not.toContain("drain");
  });

  test("a binding-render refusal exposes identifiers/actions only", () => {
    const inventory = [queue("noop"), flag("noop", "drain")],
      rendered = plans(SEVEN, {}, inventory),
      altered = {
        planText: rendered.planText.replace(
          "[session-ingest/INGEST_ADMISSION] noop",
          "[session-ingest/FOREIGN] noop",
        ),
        detailedText: rendered.detailedText.replace(
          "[session-ingest/INGEST_ADMISSION] noop",
          "[session-ingest/FOREIGN] noop",
        ),
      };
    let refusal: unknown;
    try {
      operatorSafePlans(altered, secrets, inventory);
    } catch (error) {
      refusal = error;
    }
    expect(refusal).toBeInstanceOf(OperatorRefusal);
    const diagnostic = (refusal as OperatorRefusal).diagnostic!;
    expect(diagnostic.kind).toBe("binding_render");
    if (diagnostic.kind !== "binding_render") throw new Error("wrong diagnostic kind");
    expect(diagnostic.reason).toBe("binding_render_inventory");
    expect(diagnostic.compact.renderedBindings).toContainEqual({
      sid: "FOREIGN",
      action: "noop",
    });
    expect(diagnostic.expectedBindings).toContainEqual({
      sid: "INGEST_ADMISSION",
      action: "noop",
    });
    expect(JSON.stringify(diagnostic)).not.toContain("drain");
  });

  test("fresh rollout (nothing deployed) may be on directly", () => {
    const creates = Object.fromEntries(SEVEN.map((r) => [r, "create"]));
    expect(() => run(SEVEN, creates, [queue("create"), flag("create", "on")])).not.toThrow();
  });

  test("off: four resources and neither binding; the binding and the var come together or not at all", () => {
    expect(() => run(FOUR, { "session-ingest": "update" }, [])).not.toThrow();
    expect(() => run(SEVEN, addition, [])).toThrow("original_plan_contract");
    expect(() => run(SEVEN, addition, [queue("create")])).toThrow("original_plan_contract");
    expect(() => run(FOUR, { "session-ingest": "update" }, [flag("create", "on")])).toThrow("original_plan_contract");
    expect(() => run(SEVEN, addition, [queue("create"), flag("create", "yes")])).toThrow("original_plan_contract");
  });
});
test("the retained plan unbinds INGEST_ADVERTISED_MAX_RECORDS only for an omitted candidate value and a strict live value", () => {
  const profile = (extra: Record<string, unknown> = {}) =>
    ({ ...baseInput.sessionIngest, ...extra }) as any;
  const live = (value: unknown) => ({ INGEST_ADVERTISED_MAX_RECORDS: value });
  for (const value of ["1", "9", "10", "120", "499", "500"])
    expect(advertisedMaxRecordsDeletionAllowed(profile(), live(value), {})).toBe(true);
  for (const value of ["0", "501", "999", "1000", "0120", "00", "01", "050", "099", " 120", "120 ", "2.5", "+120", "-1", "1e2", "0x10", "", 120, null, undefined])
    expect(advertisedMaxRecordsDeletionAllowed(profile(), live(value), {})).toBe(false);
  expect(advertisedMaxRecordsDeletionAllowed(profile(), undefined, {})).toBe(false);
  expect(advertisedMaxRecordsDeletionAllowed(profile(), live("120"), undefined)).toBe(true);
  expect(advertisedMaxRecordsDeletionAllowed(profile({ advertisedMaxRecords: 120 }), live("120"), {})).toBe(false);
  expect(advertisedMaxRecordsDeletionAllowed(profile({ advertisedMaxRecords: 500 }), live("120"), {})).toBe(false);
  expect(advertisedMaxRecordsDeletionAllowed(profile(), live("120"), live("120"))).toBe(false);
  expect(advertisedMaxRecordsDeletionAllowed(profile(), live("120"), live(""))).toBe(false);
});
test("the retained plan unbinds INGEST_INFLIGHT_LEASE only for an omitted candidate value and a strict live observe or on", () => {
  const profile = (extra: Record<string, unknown> = {}) =>
    ({ ...baseInput.sessionIngest, ...extra }) as any;
  const live = (value: unknown) => ({ INGEST_INFLIGHT_LEASE: value });
  for (const value of ["observe", "on"]) {
    expect(inflightLeaseDeletionAllowed(profile(), live(value), {})).toBe(true);
    expect(inflightLeaseDeletionAllowed(profile(), live(value), undefined)).toBe(true);
    // The candidate profile still sets it, or a value is proposed.
    for (const mode of ["observe", "on"])
      expect(inflightLeaseDeletionAllowed(profile({ inflightLease: mode }), live(value), {})).toBe(false);
    for (const proposed of ["observe", "on", ""])
      expect(inflightLeaseDeletionAllowed(profile(), live(value), live(proposed))).toBe(false);
  }
  for (const value of ["On", "ON", "Observe", "OBSERVE", " on", "on ", "observe\t", "off", "true", "1", "", 0, 1, null, undefined])
    expect(inflightLeaseDeletionAllowed(profile(), live(value), {})).toBe(false);
  expect(inflightLeaseDeletionAllowed(profile(), undefined, {})).toBe(false);
});
test("the retained plan unbinds INGEST_BUFFER_COMMIT_STATEMENT_BUDGET_MS only for an omitted candidate value, nothing proposed and a strict in-range live value", () => {
  const profile = (extra: Record<string, unknown> = {}) => ({ ...baseInput.sessionIngest, ...extra }) as any;
  const live = (value: unknown) => ({ INGEST_BUFFER_COMMIT_STATEMENT_BUDGET_MS: value });
  for (const v of ["5001", "6000", "7000"]) {
    expect(bufferCommitBudgetDeletionAllowed(profile(), live(v), {})).toBe(true);
    expect(bufferCommitBudgetDeletionAllowed(profile(), live(v), undefined)).toBe(true);
  }
  for (const ms of [6000, 5001, 7000, 9000, "6000"])
    expect(bufferCommitBudgetDeletionAllowed(profile({ bufferCommitBudgetMs: ms }), live("6000"), {})).toBe(false);
  for (const proposed of ["6000", "", "9000"])
    expect(bufferCommitBudgetDeletionAllowed(profile(), live("6000"), live(proposed))).toBe(false);
  for (const value of ["5000", "7001", "4000", "0", "06000", " 6000", "6000 ", "6000\n", "+6000", "-6000", "6000ms", "6s", "6e3", "6000.0", "on", "", 6000, true, null, undefined])
    expect(bufferCommitBudgetDeletionAllowed(profile(), live(value), {})).toBe(false);
  expect(bufferCommitBudgetDeletionAllowed(profile(), undefined, {})).toBe(false);
  expect(bufferCommitBudgetDeletionAllowed(profile(), { INGEST_BUFFER_COMMIT_LOCK_WAIT_MS: "6000" }, {})).toBe(false);
});
test("the retained plan unbinds INGEST_BUFFER_TIMEOUT_BUSY only for an omitted candidate value, nothing proposed and a live exactly on", () => {
  const profile = (extra: Record<string, unknown> = {}) =>
    ({ ...baseInput.sessionIngest, ...extra }) as any;
  const live = (value: unknown) => ({ INGEST_BUFFER_TIMEOUT_BUSY: value });
  expect(bufferTimeoutBusyDeletionAllowed(profile(), live("on"), {})).toBe(true);
  expect(bufferTimeoutBusyDeletionAllowed(profile(), live("on"), undefined)).toBe(true);
  // The candidate profile still carries bufferTimeoutBusy (any value).
  for (const mode of ["on", "off", ""])
    expect(bufferTimeoutBusyDeletionAllowed(profile({ bufferTimeoutBusy: mode }), live("on"), {})).toBe(false);
  // The binding is present in the proposed environment (any value).
  for (const proposed of ["on", "off", ""])
    expect(bufferTimeoutBusyDeletionAllowed(profile(), live("on"), live(proposed))).toBe(false);
  // Live text other than exactly "on": no trim, no case folding, no other value; absent or non-string.
  for (const value of ["ON", "On", "oN", " on", "on ", "on\n", "off", "observe", "true", "1", "", 1, true, null, undefined])
    expect(bufferTimeoutBusyDeletionAllowed(profile(), live(value), {})).toBe(false);
  expect(bufferTimeoutBusyDeletionAllowed(profile(), undefined, {})).toBe(false);
  expect(bufferTimeoutBusyDeletionAllowed(profile(), {}, {})).toBe(false);
  // Only its own variable counts: a live INGEST_BUFFER_LOCK_RETRY or INGEST_CLAIM_LOCK_BUSY "on" is not it.
  expect(bufferTimeoutBusyDeletionAllowed(profile(), { INGEST_BUFFER_LOCK_RETRY: "on" }, {})).toBe(false);
  expect(bufferTimeoutBusyDeletionAllowed(profile(), { INGEST_CLAIM_LOCK_BUSY: "on" }, {})).toBe(false);
});
test("the retained plan unbinds INGEST_BUFFER_COMMIT_LOCK_WAIT_MS only for an omitted candidate value, nothing proposed and a strict in-range live value", () => {
  const profile = (extra: Record<string, unknown> = {}) => ({ ...baseInput.sessionIngest, ...extra }) as any;
  const live = (value: unknown) => ({ INGEST_BUFFER_COMMIT_LOCK_WAIT_MS: value });
  for (const v of ["2001", "3000", "4000"]) {
    expect(bufferCommitLockWaitDeletionAllowed(profile(), live(v), {})).toBe(true);
    expect(bufferCommitLockWaitDeletionAllowed(profile(), live(v), undefined)).toBe(true);
  }
  for (const ms of [3000, 2001, 4000, 6000, "3000"])
    expect(bufferCommitLockWaitDeletionAllowed(profile({ bufferCommitLockWaitMs: ms }), live("3000"), {})).toBe(false);
  for (const proposed of ["3000", "", "6000"])
    expect(bufferCommitLockWaitDeletionAllowed(profile(), live("3000"), live(proposed))).toBe(false);
  for (const value of ["2000", "4001", "6000", "0", "02500", " 3000", "3000 ", "3000\n", "+3000", "-3000", "3000ms", "3s", "3e3", "3000.0", "on", "", 3000, true, null, undefined])
    expect(bufferCommitLockWaitDeletionAllowed(profile(), live(value), {})).toBe(false);
  expect(bufferCommitLockWaitDeletionAllowed(profile(), undefined, {})).toBe(false);
  expect(bufferCommitLockWaitDeletionAllowed(profile(), { INGEST_CLAIM_LOCK_WAIT_MS: "3000" }, {})).toBe(false);
});
test("the retained plan unbinds INGEST_BUFFER_LOCK_BUSY only for an omitted candidate value, nothing proposed and a live exactly on", () => {
  const profile = (extra: Record<string, unknown> = {}) =>
    ({ ...baseInput.sessionIngest, ...extra }) as any;
  const live = (value: unknown) => ({ INGEST_BUFFER_LOCK_BUSY: value });
  expect(bufferLockBusyDeletionAllowed(profile(), live("on"), {})).toBe(true);
  expect(bufferLockBusyDeletionAllowed(profile(), live("on"), undefined)).toBe(true);
  // The candidate profile still carries bufferLockBusy (any value).
  for (const mode of ["on", "off", ""])
    expect(bufferLockBusyDeletionAllowed(profile({ bufferLockBusy: mode }), live("on"), {})).toBe(false);
  // The binding is present in the proposed environment (any value).
  for (const proposed of ["on", "off", ""])
    expect(bufferLockBusyDeletionAllowed(profile(), live("on"), live(proposed))).toBe(false);
  // Live text other than exactly "on": no trim, no case folding, no other value; absent or non-string.
  for (const value of ["ON", "On", "oN", " on", "on ", "on\n", "off", "observe", "true", "1", "", 1, true, null, undefined])
    expect(bufferLockBusyDeletionAllowed(profile(), live(value), {})).toBe(false);
  expect(bufferLockBusyDeletionAllowed(profile(), undefined, {})).toBe(false);
  expect(bufferLockBusyDeletionAllowed(profile(), {}, {})).toBe(false);
  // Only its own variable counts: a live INGEST_BUFFER_LOCK_RETRY or INGEST_CLAIM_LOCK_BUSY "on" is not it.
  expect(bufferLockBusyDeletionAllowed(profile(), { INGEST_BUFFER_LOCK_RETRY: "on" }, {})).toBe(false);
  expect(bufferLockBusyDeletionAllowed(profile(), { INGEST_CLAIM_LOCK_BUSY: "on" }, {})).toBe(false);
});
