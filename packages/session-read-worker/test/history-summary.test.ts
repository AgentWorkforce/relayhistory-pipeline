import { expect, it } from "vitest";
import fixture from "./fixtures/history-summary-v2/fixture.json";
import shortCipher from "./fixtures/history-summary-v2/short.cipher?raw";
import longCipher from "./fixtures/history-summary-v2/long.cipher?raw";
import { objectKey } from "../../object-store/src/index.ts";
import { masterKeyring, parseMasterKeys } from "../../../src/shared/keyring.ts";
import type { HistorySummaryReadDeps } from "../../../src/shared/history-summary-reader.ts";
import { canonicalHistorySummaryHandle } from "../src/history-summary.ts";
import { handle, type Env } from "../src/index.ts";
const env = {
  CANONICAL_READ_ENABLED: "true",
  CANONICAL_HISTORY_READ_ENABLED: "true",
  CANONICAL_READ_MODE: "pages",
};
const identity = fixture.claims.short.identity;
const token =
  "header." +
  btoa(
    JSON.stringify({
      role: "session_directory_search_worker",
      app_metadata: {
        org_id: identity.orgId,
        workspace_ids: [identity.workspaceId],
      },
    }),
  ) +
  ".signature";
function request(kind = "short", signal?: AbortSignal) {
  return new Request(
    "https://read.test/v1/canonical/summaries/" +
      encodeURIComponent(identity.sessionId) +
      "?" +
      new URLSearchParams({
        workspace: identity.workspaceId,
        source: identity.source,
        kind,
      }),
    { headers: { authorization: "Bearer " + token }, signal },
  );
}
const forbidden = () => {
  throw Error("Neon/CH/unconfigured network forbidden");
};
async function setup(kind: "short" | "long" = "short") {
  let claim: any = structuredClone(fixture.claims[kind]),
    claims = 0,
    reads = 0,
    keys = 0;
  const ring = await masterKeyring(
    parseMasterKeys("reader-fixture:" + "d".repeat(64)),
  );
  const deps: HistorySummaryReadDeps = {
    storage: { provider: "s3", bucket: "fixture-bucket", region: "us-east-1" },
    claim: async () => {
      claims++;
      return claim;
    },
    read: async (key) => {
      reads++;
      const found = Object.values(fixture.objects).find(
        (x) => x.meta.key === key,
      );
      if (!found) throw Error("unregistered fixture key");
      const bytes = new TextEncoder().encode(
        found.file === "short.cipher" ? shortCipher : longCipher,
      );
      expect(bytes.length).toBe(found.bytes);
      return {
        meta: {
          ...found.meta,
          key: objectKey(key),
          lastModified: new Date(found.meta.lastModified),
        },
        body: new ReadableStream({
          start(c) {
            c.enqueue(bytes);
            c.close();
          },
        }),
      };
    },
    keyring: {
      keyById: async (ids, id, signal) => {
        keys++;
        signal.throwIfAborted();
        expect(ids).toEqual({
          orgId: identity.orgId,
          workspaceId: identity.workspaceId,
        });
        expect(id).toBe("hkdf1:reader-fixture");
        return ring.keyById(ids, id);
      },
    },
  };
  return {
    deps,
    set: (v: unknown) => {
      claim = v;
    },
    counts: () => ({ claims, reads, keys }),
  };
}
it.each(["short", "long"] as const)(
  "HTTP workerd decrypts frozen09cc %s producer bytes and keeps qualified Referee state",
  async (kind) => {
    const f = await setup(kind);
    const response = await canonicalHistorySummaryHandle(
      request(kind),
      env,
      forbidden,
      undefined,
      f.deps,
    );
    expect(response.status).toBe(200);
    const body: any = await response.json();
    expect(body).toMatchObject({
      version: 1,
      identity,
      kind,
      status: "ready",
      source: fixture.claims[kind].source,
      state: { state: "active" },
    });
    expect(body.source.epoch).toBe("9007199254740993");
    expect(body.summary).toContain(
      kind === "short" ? "FIXTURE SHORT" : "FIXTURE LONG",
    );
    if (kind === "long") expect(body.summary).toContain("\ud800");
    expect(body.state.state).not.toBe("done");
    expect(response.headers.get("X-Relayhistory-Workspace-Id")).toBe(
      identity.workspaceId,
    );
    expect(f.counts()).toEqual({ claims: 2, reads: 1, keys: 1 });
  },
);
it("real worker dispatch uses the exact same bearer/RPC arguments before and after configured S3 IO", async () => {
  const f = await setup("long");
  let claims = 0,
    objects = 0;
  const config: Env = {
    ...env,
    CANONICAL_SUPABASE_URL: "https://metadata.fixture.invalid",
    CANONICAL_SUPABASE_API_KEY: "synthetic-anon",
    PIPELINE_RAW_S3_BUCKET: "fixture-bucket",
    PIPELINE_RAW_S3_REGION: "us-east-1",
    PIPELINE_RAW_S3_ENDPOINT: "https://s3.fixture.invalid",
    PIPELINE_RAW_S3_ACCESS_KEY_ID: "synthetic",
    PIPELINE_RAW_S3_SECRET_ACCESS_KEY: "synthetic",
  } as Env;
  const fetcher: typeof fetch = async (input, init) => {
    const url = new URL(String(input));
    if (url.hostname === "metadata.fixture.invalid") {
      claims++;
      expect(url.pathname).toBe(
        "/rest/v1/rpc/session_directory_history_summary",
      );
      expect(new Headers(init?.headers).get("authorization")).toBe(
        "Bearer " + token,
      );
      expect(JSON.parse(String(init?.body))).toEqual({
        p_workspace_id: identity.workspaceId,
        p_harness: identity.source,
        p_session_id: identity.sessionId,
        p_kind: "long",
      });
      return Response.json(fixture.claims.long);
    }
    expect(url.hostname).toBe("s3.fixture.invalid");
    objects++;
    const key = decodeURIComponent(url.pathname).slice(
      "/fixture-bucket/".length,
    );
    expect(key).toBe(fixture.claims.long.pointer.objectKey);
    const meta = Object.values(fixture.objects).find(
      (x) => x.meta.key === key,
    )!.meta;
    return new Response(longCipher, {
      headers: {
        "content-length": String(meta.size),
        etag: meta.version.eTag,
        "last-modified": new Date(meta.lastModified).toUTCString(),
        "x-amz-meta-summary-key-id": meta.metadata["summary-key-id"],
        "x-amz-meta-content-sha256": meta.metadata["content-sha256"],
      },
    });
  };
  const response = await handle(
    request("long"),
    config,
    { waitUntil: forbidden } as never,
    {
      fetcher,
      summaryKeyring: f.deps.keyring,
      connect: async () => forbidden(),
    },
  );
  expect(response.status).toBe(200);
  expect(((await response.json()) as any).summary).toContain("FIXTURE LONG");
  expect({ claims, objects }).toEqual({ claims: 2, objects: 1 });
});
it("default/off mode and invalid scope do no metadata, object, key or SQL work", async () => {
  for (const [req, config, status] of [
    [request(), {}, 503],
    [request("invalid"), env, 400],
    [new Request(request().url), env, 401],
    [
      new Request(request().url + "&objectKey=untrusted", {
        headers: { authorization: "Bearer " + token },
      }),
      env,
      400,
    ],
  ] as const) {
    const f = await setup();
    expect(
      (
        await canonicalHistorySummaryHandle(
          req,
          config,
          forbidden,
          undefined,
          f.deps,
        )
      ).status,
    ).toBe(status);
    expect(f.counts()).toEqual({ claims: 0, reads: 0, keys: 0 });
  }
  const response = await canonicalHistorySummaryHandle(
    request(),
    env,
    forbidden,
  );
  expect(response.status).toBe(503);
});
it.each([
  "not_ready",
  "wrong_scope",
  "oversized_object",
  "missing_key",
] as const)("refuses %s before any ciphertext/key access", async (failure) => {
  const f = await setup(),
    claim: any = structuredClone(fixture.claims.short);
  if (failure === "not_ready") {
    claim.ready = false;
    claim.authority = claim.storage = claim.pointer = claim.state = null;
  }
  if (failure === "wrong_scope") claim.identity.orgId = "foreign";
  if (failure === "oversized_object")
    claim.pointer.objectBytes = 2 * 1024 * 1024;
  if (failure === "missing_key") f.deps.keyring = undefined;
  f.set(claim);
  expect(
    (
      await canonicalHistorySummaryHandle(
        request(),
        env,
        forbidden,
        undefined,
        f.deps,
      )
    ).status,
  ).toBe(503);
  expect(f.counts()).toEqual({ claims: 1, reads: 0, keys: 0 });
});
it("refuses a changed final logical source after real decrypt and does not expose plaintext", async () => {
  const f = await setup();
  let calls = 0;
  f.deps.claim = async () => {
    calls++;
    const c = structuredClone(fixture.claims.short);
    if (calls === 2) c.source.fingerprint = "f".repeat(64);
    return c;
  };
  const response = await canonicalHistorySummaryHandle(
    request(),
    env,
    forbidden,
    undefined,
    f.deps,
  );
  expect(response.status).toBe(503);
  expect(await response.text()).not.toContain("FIXTURE SHORT");
  expect(calls).toBe(2);
});
it("explicit current empty is rechecked and never borrows old prose", async () => {
  const f = await setup();
  const c: any = structuredClone(fixture.claims.short);
  c.pointer = {
    ...c.pointer,
    status: "empty",
    objectKey: null,
    objectSha256: null,
    objectBytes: null,
    keyId: null,
    model: null,
  };
  f.set(c);
  f.deps.keyring = undefined;
  const response = await canonicalHistorySummaryHandle(
    request(),
    env,
    forbidden,
    undefined,
    f.deps,
  );
  expect(response.status).toBe(200);
  expect(await response.json()).toMatchObject({
    status: "empty",
    summary: null,
    model: null,
    builtAt: null,
    state: { state: "active" },
  });
  expect(f.counts()).toEqual({ claims: 2, reads: 0, keys: 0 });
});
it("caller abort stops a pending key port without waiting for that port to settle", async () => {
  const f = await setup(),
    abort = new AbortController();
  let started!: () => void, seen: AbortSignal | undefined;
  const pending = new Promise<void>((resolve) => {
    started = resolve;
  });
  f.deps.keyring = {
    keyById: async (_ids, _id, signal) => {
      seen = signal;
      started();
      return new Promise<CryptoKey>(() => {});
    },
  };
  const read = canonicalHistorySummaryHandle(
    request("short", abort.signal),
    env,
    forbidden,
    undefined,
    f.deps,
  );
  await pending;
  abort.abort();
  const response = await Promise.race([
    read,
    new Promise<null>((resolve) => setTimeout(() => resolve(null), 500)),
  ]);
  if (response === null) await read;
  expect(response).toBeInstanceOf(Response);
  expect(response?.status).toBe(503);
  expect(seen?.aborted).toBe(true);
  expect(f.counts().claims).toBe(1);
});
