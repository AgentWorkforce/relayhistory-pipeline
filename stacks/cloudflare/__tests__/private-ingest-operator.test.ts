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
async function actualPlanProof(mode: "canonical" | "buffered") {
  const input = {
    ...baseInput,
    sessionIngest: {
      ...baseInput.sessionIngest,
      ...(mode === "buffered"
        ? {
            acceptanceMode: "buffered" as const,
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
        expect(plans.planText).toContain("4 to create");
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
      mode === "buffered" ? 14 : 13,
    );
    const queue = native.native.resources["session-ingest-fanout"];
    if (mode === "buffered") {
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
test.each(["canonical", "buffered"] as const)(
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
