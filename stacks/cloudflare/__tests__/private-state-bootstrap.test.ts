/** Synthetic metadata/Alchemy only; no live credentials or network. */
import * as Effect from "effect/Effect";
import {
  checkpointThenHoist,
  prepareOnePasswordSink,
} from "../../../scripts/private-state-bootstrap.ts";
import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import {
  runStateBootstrap,
  bootstrapMetadata,
  bootstrapFetch,
  bootstrapSpec,
  captureBootstrap,
  verifyBootstrap,
  assertBootstrapSnapshot,
  STATE_SCRIPT,
  SECRET_NAMES,
  EXECUTION,
  type BootstrapSpec,
  type BootstrapSource,
  type Plans,
} from "../../../scripts/private-state-bootstrap.ts";
import { inputsDigest } from "../../aws/deploy-guard.ts";
const spec: BootstrapSpec = {
  accountId: "a".repeat(32),
  secretsStoreId: "b".repeat(32),
  onePasswordVaultId: "v".repeat(26),
};
const token = "synthetic-api-token-not-authorized";
const source: BootstrapSource = {
  commit: "c".repeat(40),
  host: "synthetic",
  clean: true,
  tools: {
    bun: "1.4.2",
    node: "v24.21.0",
    alchemy: "2.0.0-beta.84",
    cloudflare: "2.0.0-beta.84",
  },
};
const digest = (text: string) =>
  createHash("sha256").update(text).digest("hex");
function fixture() {
  const calls: Array<{ method: string; path: string }> = [];
  let worker = false,
    badCode = false,
    secret = false,
    namespace = false,
    otherStore = false,
    unknownPage = false;
  const pages = (rows: any[], page = 1, total = rows.length, pages = 1) => ({
    success: true,
    result: rows,
    result_info: {
      page,
      total_pages: pages,
      per_page: 100,
      count: rows.length,
      total_count: total,
    },
  });
  const fetcher: typeof fetch = async (input, init) => {
    const u = new URL(String(input)),
      method = init?.method ?? "GET";
    calls.push({ method, path: u.pathname });
    const prefix = "/client/v4/accounts/" + spec.accountId + "/";
    if (!u.pathname.startsWith(prefix)) throw Error("unexpected origin");
    const path = u.pathname.slice(prefix.length);
    if (path === "workers/scripts/" + STATE_SCRIPT + "/settings")
      return Response.json(
        worker
          ? { success: true, result: {} }
          : { success: false, errors: [{ code: badCode ? 1000 : 10007 }] },
        { status: worker ? 200 : 404 },
      );
    if (path === "workers/subdomain")
      return Response.json({ success: true, result: { subdomain: "fixture" } });
    if (path === "secrets_store/stores")
      return Response.json(
        pages([
          {
            id: otherStore ? "e".repeat(32) : spec.secretsStoreId,
            name: "default_secrets_store",
          },
        ]),
      );
    if (path === "secrets_store/stores/" + spec.secretsStoreId + "/secrets")
      return Response.json(
        pages(
          secret
            ? [{ name: SECRET_NAMES[0] }]
            : [{ name: "existing-ingest-secret" }],
        ),
      );
    if (path === "workers/durable_objects/namespaces") {
      const page = Number(u.searchParams.get("page") ?? 1);
      if (unknownPage) return Response.json({ success: true, result: [] });
      const rows = Array.from({ length: page === 1 ? 100 : 1 }, (_, i) => ({
        id: "ns" + page + "-" + i,
        name: "other-" + i,
        script: namespace ? STATE_SCRIPT : "other-worker",
      }));
      return Response.json(pages(rows, page, 101, 2));
    }
    throw Error("unexpected fixture request " + path);
  };
  return {
    fetcher,
    calls,
    worker: () => (worker = true),
    badCode: () => (badCode = true),
    secret: () => (secret = true),
    namespace: () => (namespace = true),
    otherStore: () => (otherStore = true),
    unknownPage: () => (unknownPage = true),
  };
}
test("official state-store resource plan is exact create-only control footprint, with one pinned existing store adoption", async () => {
  const x = fixture(),
    dir = await mkdtemp(join(tmpdir(), "state-bootstrap-proof-"));
  const old = {
    CI: process.env.CI,
    CLOUDFLARE_ACCOUNT_ID: process.env.CLOUDFLARE_ACCOUNT_ID,
    CLOUDFLARE_API_TOKEN: process.env.CLOUDFLARE_API_TOKEN,
  };
  process.env.CI = "true";
  process.env.CLOUDFLARE_ACCOUNT_ID = spec.accountId;
  process.env.CLOUDFLARE_API_TOKEN = token;
  try {
    const result = await runStateBootstrap({
      mode: "plan",
      spec,
      token,
      source,
      directory: dir,
      fetcher: x.fetcher,
    });
    expect(result.operation).toBe("captured-only");
    expect(result.plans!.planText).toContain(
      "5 to create, 1 to adopted, 3 binding changes",
    );
    expect(result.plans!.detailedText).toContain("name: " + STATE_SCRIPT);
    expect(result.plans!.detailedText).toContain("workersDev: true");
    expect(result.plans!.detailedText).not.toContain(token);
    expect(result.plans!.detailedText).not.toContain("96bulls-session-ingest");
    expect(x.calls.every((c) => c.method === "GET")).toBe(true);
    expect(x.calls.filter((c) => c.path.includes("namespaces")).length).toBe(2);
    expect(x.calls.some((c) => /preview|\/secrets\//u.test(c.path))).toBe(
      false,
    );
  } finally {
    for (const [key, value] of Object.entries(old)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    await rm(dir, { recursive: true, force: true });
  }
});
test.each([
  "worker",
  "badCode",
  "secret",
  "namespace",
  "otherStore",
  "unknownPage",
] as const)("current %s refuses bootstrap admission", async (mode) => {
  const x = fixture();
  x[mode]();
  await expect(
    bootstrapMetadata(spec, token, x.fetcher).admission(),
  ).rejects.toThrow();
  expect(x.calls.every((c) => c.method === "GET")).toBe(true);
});
test("no create can start without reviewed capture and mandatory memory-only credential sink", async () => {
  const x = fixture();
  await expect(
    runStateBootstrap({
      mode: "create",
      spec,
      token,
      source,
      directory: "unused",
      fetcher: x.fetcher,
    }),
  ).rejects.toMatchObject({ code: "review_and_memory_sink_required" });
  expect(x.calls).toHaveLength(0);
});
test("plan capability blocks writes, edge previews, foreign accounts and store creation before network", async () => {
  let calls = 0;
  const base: typeof fetch = async () => {
    calls++;
    return Response.json({ success: true });
  };
  const guarded = bootstrapFetch(spec, token, "plan", "fixture", base),
    account =
      "https://api.cloudflare.com/client/v4/accounts/" + spec.accountId + "/";
  for (const [path, method] of [
    ["workers/scripts/" + STATE_SCRIPT, "PUT"],
    ["workers/subdomain/edge-preview", "POST"],
    ["secrets_store/stores", "POST"],
  ]) {
    await expect(guarded(account + path, { method })).rejects.toThrow();
  }
  await expect(
    guarded(
      "https://api.cloudflare.com/client/v4/accounts/" +
        "e".repeat(32) +
        "/workers/scripts/x/settings",
    ),
  ).rejects.toThrow();
  expect(calls).toBe(0);
});
test("create capability refuses a now-existing worker and never uploads over it", async () => {
  const x = fixture();
  x.worker();
  const guarded = bootstrapFetch(spec, token, "create", "fixture", x.fetcher);
  await expect(
    guarded(
      "https://api.cloudflare.com/client/v4/accounts/" +
        spec.accountId +
        "/workers/scripts/" +
        STATE_SCRIPT,
      { method: "PUT", body: "bundle" },
    ),
  ).rejects.toMatchObject({ code: "state_worker_exists_or_unproven" });
  expect(x.calls.every((c) => c.method === "GET")).toBe(true);
});
test("input shape cannot smuggle an app flag, token, another store or plaintext value", () => {
  for (const extra of [
    { token: "secret" },
    { ingestApply: true },
    { force: true },
  ])
    expect(() => bootstrapSpec({ ...spec, ...extra })).toThrow();
});
test("operator approval binds exact source/host/store/vault/plan/generation and explicit bootstrap-only scope", async () => {
  const admission = await bootstrapMetadata(
      spec,
      token,
      fixture().fetcher,
    ).admission(),
    plans: Plans = {
      planText: "safe compact\n",
      detailedText: "safe detail\n",
    },
    capture = captureBootstrap(source, spec, admission, plans),
    text = JSON.stringify(capture),
    approval = {
      decision: "approved",
      by: "Khaliq Gant",
      date: new Date().toISOString().slice(0, 10),
      quote: "Approved prerequisite state-store bootstrap only.",
      execution: EXECUTION,
      capture: {
        provenanceSha256: digest(text),
        planSha256: digest(plans.planText),
        inputsSha256: inputsDigest(JSON.stringify(spec)),
        sourceCommit: source.commit,
      },
    };
  const approved = { ...spec, approval };
  expect(
    verifyBootstrap(source, approved, admission, plans, text, digest(text))
      .generation,
  ).toBe(capture.generation);
  expect(() =>
    verifyBootstrap(
      source,
      {
        ...approved,
        approval: { ...approval, execution: "ingest-app-deploy" },
      },
      admission,
      plans,
      text,
      digest(text),
    ),
  ).toThrow();
  expect(() =>
    verifyBootstrap(
      { ...source, host: "foreign" },
      approved,
      admission,
      plans,
      text,
      digest(text),
    ),
  ).toThrow();
  expect(() =>
    verifyBootstrap(
      source,
      { ...approved, onePasswordVaultId: "w".repeat(26) },
      admission,
      plans,
      text,
      digest(text),
    ),
  ).toThrow();
  expect(() =>
    verifyBootstrap(
      source,
      approved,
      admission,
      { ...plans, detailedText: "changed" },
      text,
      digest(text),
    ),
  ).toThrow();
});

const keys = [
  "StateStoreAuthTokenValue",
  "Api",
  "StateStoreSecrets",
  "AlchemyStateStoreToken",
  "StateStoreEncryptionKeyValue",
  "StateStoreEncryptionKey",
];
function hoistFixture() {
  const order: string[] = [],
    data = new Map<string, any>(),
    records = Object.fromEntries(
      keys.map((id) => [
        id,
        { status: "created", logicalId: id, attr: { id } },
      ]),
    );
  const remote: any = {
    getVersion: () =>
      Effect.sync(() => {
        order.push("version");
        return 7;
      }),
    listStacks: () => Effect.succeed([]),
    set: ({ fqn, value }: any) =>
      Effect.sync(() => {
        order.push("set:" + fqn);
        data.set(fqn, value);
        return value;
      }),
    get: ({ fqn }: any) => Effect.sync(() => data.get(fqn)),
  };
  const envelope: any = {
    protocol: "private-state-bootstrap-v1",
    status: "created",
    generation: "f".repeat(64),
    accountId: spec.accountId,
    url: "https://" + STATE_SCRIPT + ".fixture.workers.dev",
    authToken: "d".repeat(64),
    encodedBootstrapState: records,
  };
  return { order, data, records, remote, envelope };
}
test("confirmed RAM credential/recovery receipt precedes every control metadata hoist/readback", async () => {
  const x = hoistFixture();
  const receipt = await Effect.runPromise(
    checkpointThenHoist(
      spec,
      x.envelope,
      x.records,
      async (value) => {
        x.order.push("sink");
        expect(value.authToken).toBe("d".repeat(64));
        return {
          stored: true,
          vaultId: spec.onePasswordVaultId,
          itemId: "item",
        };
      },
      () => Effect.succeed(x.remote),
    ),
  );
  expect(x.order[0]).toBe("sink");
  expect(receipt.itemId).toBe("item");
  expect(x.data.size).toBe(6);
  expect([...x.data.keys()].sort()).toEqual([...keys].sort());
});
test.each(["failed", "wrongVault"] as const)(
  "a %s credential receipt leaves remote state untouched",
  async (mode) => {
    const x = hoistFixture();
    let opened = false;
    await expect(
      Effect.runPromise(
        checkpointThenHoist(
          spec,
          x.envelope,
          x.records,
          async () =>
            ({
              stored: mode === "failed" ? false : true,
              vaultId:
                mode === "wrongVault" ? "foreign" : spec.onePasswordVaultId,
              itemId: "item",
            }) as any,
          () => {
            opened = true;
            return Effect.succeed(x.remote);
          },
        ),
      ),
    ).rejects.toThrow();
    expect(opened).toBe(false);
    expect(x.data.size).toBe(0);
  },
);
test("hoist refuses existing remote data and a corrupt exact-key readback", async () => {
  for (const mode of ["existing", "corrupt"]) {
    const x = hoistFixture();
    if (mode === "existing")
      x.remote.listStacks = () => Effect.succeed(["some-stack"]);
    else x.remote.get = () => Effect.succeed({ changed: true });
    await expect(
      Effect.runPromise(
        checkpointThenHoist(
          spec,
          x.envelope,
          x.records,
          async () => ({
            stored: true,
            vaultId: spec.onePasswordVaultId,
            itemId: "item",
          }),
          () => Effect.succeed(x.remote),
        ),
      ),
    ).rejects.toThrow();
    expect(x.data.size).toBe(mode === "existing" ? 0 : 1);
  }
});

function onePasswordFixture() {
  const calls: Array<{ args: string[]; input?: string }> = [];
  let item: any;
  const port = async (args: string[], input?: string) => {
    calls.push({ args, input });
    if (args[1] === "create")
      item = { ...JSON.parse(input!), id: "i".repeat(26) };
    if (args[1] === "edit") item = JSON.parse(input!);
    return structuredClone(item);
  };
  return { calls, port };
}
test("dedicated 1Password recovery slot is verified before create; secrets travel only through captured JSON stdin", async () => {
  const x = onePasswordFixture(),
    envelope = hoistFixture().envelope;
  const prepared = await prepareOnePasswordSink(
    spec,
    envelope.generation,
    x.port,
  );
  expect(x.calls.map((c) => c.args[1])).toEqual(["create", "get", "edit"]);
  const receipt = await prepared.sink(envelope);
  expect(receipt).toEqual({
    stored: true,
    vaultId: spec.onePasswordVaultId,
    itemId: "i".repeat(26),
  });
  expect(x.calls.map((c) => c.args[1])).toEqual([
    "create",
    "get",
    "edit",
    "get",
    "edit",
    "get",
  ]);
  expect(JSON.stringify(x.calls.map((c) => c.args))).not.toContain(
    envelope.authToken,
  );
  expect(
    x.calls.filter((c) => c.input?.includes(envelope.authToken)),
  ).toHaveLength(1);
});
test("wrong-vault or corrupt 1Password readback cannot acknowledge recovery", async () => {
  const x = onePasswordFixture(),
    envelope = hoistFixture().envelope;
  const wrong = async (args: string[], input?: string) => ({
    ...(await x.port(args, input)),
    vault: { id: "w".repeat(26) },
  });
  await expect(
    prepareOnePasswordSink(spec, envelope.generation, wrong),
  ).rejects.toMatchObject({ code: "one_password_identity" });
  const y = onePasswordFixture();
  let gets = 0;
  const corrupt = async (args: string[], input?: string) => {
    const v = await y.port(args, input);
    if (args[1] === "get" && ++gets === 3) v.fields = [];
    return v;
  };
  const prepared = await prepareOnePasswordSink(
    spec,
    envelope.generation,
    corrupt,
  );
  await expect(prepared.sink(envelope)).rejects.toMatchObject({
    code: "one_password_readback",
  });
});
test("credential sink rejects a different generation before accessing 1Password", async () => {
  const x = onePasswordFixture(),
    envelope = hoistFixture().envelope,
    prepared = await prepareOnePasswordSink(spec, envelope.generation, x.port);
  const count = x.calls.length;
  await expect(
    prepared.sink({ ...envelope, generation: "0".repeat(64) }),
  ).rejects.toMatchObject({ code: "sink_envelope_identity" });
  expect(x.calls).toHaveLength(count);
});

test("created-here control state permits only six resource keys and refuses app state or a second upload", async () => {
  const calls: string[] = [];
  const base: typeof fetch = async (input, init) => {
    const u = new URL(String(input));
    calls.push((init?.method ?? "GET") + " " + u.pathname);
    if (u.pathname.endsWith("/settings"))
      return Response.json(
        { success: false, errors: [{ code: 10007 }] },
        { status: 404 },
      );
    return Response.json({ success: true });
  };
  const guarded = bootstrapFetch(spec, token, "create", "fixture", base),
    cf =
      "https://api.cloudflare.com/client/v4/accounts/" +
      spec.accountId +
      "/workers/scripts/" +
      STATE_SCRIPT,
    state = "https://" + STATE_SCRIPT + ".fixture.workers.dev";
  await expect(guarded(state + "/version")).rejects.toMatchObject({
    code: "state_scope",
  });
  await guarded(cf, { method: "PUT", body: "synthetic-bundle" });
  await guarded(state + "/version");
  await guarded(
    state +
      "/state/stacks/CloudflareStateStore/stages/" +
      STATE_SCRIPT +
      "/resources/Api",
    { method: "PUT", body: "{}" },
  );
  const count = calls.length;
  await expect(
    guarded(cf, { method: "PUT", body: "again" }),
  ).rejects.toMatchObject({ code: "worker_second_upload" });
  await expect(
    guarded(state + "/state/stacks/96bulls/stages/prod/resources/Ingest", {
      method: "PUT",
      body: "{}",
    }),
  ).rejects.toMatchObject({ code: "state_scope" });
  await expect(
    guarded(state + "/state/stacks", { method: "DELETE" }),
  ).rejects.toMatchObject({ code: "state_scope" });
  expect(calls).toHaveLength(count);
});
