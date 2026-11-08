/** Strictly synthetic transports; unknown requests throw rather than use real fetch. */
import { test, expect, spyOn } from "bun:test";
import * as Stacks from "alchemy/Alchemist/routes/stack";
import * as Effect from "effect/Effect";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  assertOperatorSnapshot,
  retainedOperatorPlan,
  requireStateAuthority,
  operatorStateUrl,
  assertOperatorSource,
  guardOperatorFetch,
  OPERATOR_METADATA_MAX_BYTES,
  operatorCapture,
  verifyOperatorCapture,
  EXECUTION_EXCEPTION,
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
async function actualPlanProof(declared: "canonical" | "buffered" | "admission" | "frozen-receipt") {
  // "admission": buffered + asyncAdmission (the three admission resources and two more bindings).
  // "frozen-receipt": buffered + frozen-supabase custody + syncAdmissionFrozenReceipt (two more vars, no resource).
  const mode = declared === "canonical" ? "canonical" : "buffered";
  const admission = declared === "admission";
  const frozenReceipt = declared === "frozen-receipt";
  const bindingCount = admission || frozenReceipt ? 17 : mode === "buffered" ? 15 : 13;
  const input = {
    ...baseInput,
    sessionIngest: {
      ...baseInput.sessionIngest,
      ...(admission ? { asyncAdmission: "on" as const } : {}),
      ...(frozenReceipt ? { custodySource: "frozen-supabase" as const, syncAdmissionFrozenReceipt: "on" as const } : {}),
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
  };
  process.env.CI = "true";
  process.env.CLOUDFLARE_ACCOUNT_ID = secrets.accountId;
  process.env.CLOUDFLARE_API_TOKEN = secrets.apiToken;
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
        .toEqual({ type: "plain_text", name: "INGEST_ASYNC_ADMISSION", text: "on" });
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
      // The same snapshot against an input without asyncAdmission: refused (seven resources are not four).
      const { asyncAdmission: _a, ...withoutAdmission } = input.sessionIngest as any;
      expect(() => assertOperatorSnapshot(native, { ...input, sessionIngest: withoutAdmission })).toThrow();
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
test.each(["canonical", "buffered", "admission", "frozen-receipt"] as const)(
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
