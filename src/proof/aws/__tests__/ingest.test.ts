import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Hono } from "hono";
import { createFsStore } from "../../../../packages/object-store/src/fs.ts";
import { isUuidV7 } from "../../../shared/ids.ts";
import {
  objectKey,
  objectStoreManifestReader,
  readAll,
  sealRawChunk,
  type AppEnv,
  type StageMessage,
} from "../../../shared/vendor.ts";
import { zstdDecompressSync } from "node:zlib";
import { createCourierManifestRoutes, liveWriteMachineGate } from "../../../alleyoop/manifests.ts";
import { allCanaries, FAKE_AWS_KEY_ID, plantCorpus } from "../corpus.ts";
import {
  IngestError,
  locationOf,
  mintTenantToken,
  newUploadState,
  plantNextRevision,
} from "../ingest.ts";

const RELAYAUTH = "http://127.0.0.1:18787";

/** RelayAuth #113's relevant contract, with the claim carried only by the token. */
function fakeRelayAuth(claimless = false) {
  const minted: Array<Record<string, unknown>> = [];
  const fetch = async (url: string, init?: RequestInit) => {
    if (url !== `${RELAYAUTH}/v1/tokens/relayhistory-assertion`) return new Response(null, { status: 404 });
    const body = JSON.parse(String(init?.body)) as Record<string, unknown>;
    minted.push(body);
    if (!claimless && (typeof body.machineId !== "string" || !body.machineId)) return Response.json({ code: "machineId_required" }, { status: 400 });
    const claims = { orgId: body.orgId, workspaceId: body.workspaceId, ...(claimless ? {} : { machineId: body.machineId }) };
    return Response.json({ accessToken: `fake.${Buffer.from(JSON.stringify(claims)).toString("base64url")}` }, { status: 201 });
  };
  return { fetch, minted };
}

const mint = (issuer: ReturnType<typeof fakeRelayAuth>, orgId: string, machineId: string) =>
  mintTenantToken({ relayauthUrl: RELAYAUTH, bootstrapKey: "synthetic-proof-key", orgId, workspaceId: "default", machineId, sponsorId: "proof-operator", historyOrigin: "https://history.example", fetch: issuer.fetch });

const tenants = [
  { orgId: "org_proof_a", workspaceId: "default" },
  { orgId: "org_proof_b", workspaceId: "default" },
];

describe("planted corpus", () => {
  const c = plantCorpus({
    runId: "aws-proof-corpus-0001",
    sessions: 100,
    tenants,
  });
  test("UUIDv7 ids, across both tenants, unique canaries", () => {
    expect(c).toHaveLength(100);
    for (const p of c)
      expect([p.sessionId, p.sessionUuid, p.traceId].every(isUuidV7)).toBe(
        true,
      );
    expect(new Set(c.map((p) => p.sessionId)).size).toBe(100);
    expect(c.filter((p) => p.tenant.orgId === "org_proof_a")).toHaveLength(50);
    expect(new Set(c.map((p) => p.summaryTerm)).size).toBe(100);
    expect(new Set(c.map((p) => p.transcriptCanary)).size).toBe(100);
    // No canary is a substring of another session's canary (s1 vs s10).
    const all = allCanaries(c);
    for (const a of all)
      expect(all.filter((b) => b !== a && b.includes(a))).toEqual([]);
  });
  test("transcript-only canary never appears in the first user turn", () => {
    for (const p of c)
      expect(p.revisions[0]![0]).not.toContain(p.transcriptCanary);
  });
  test("refuses a single tenant or a shared org", () => {
    expect(() =>
      plantCorpus({
        runId: "aws-proof-x-0001",
        sessions: 4,
        tenants: [tenants[0]!],
      }),
    ).toThrow();
    expect(() =>
      plantCorpus({
        runId: "aws-proof-x-0001",
        sessions: 4,
        tenants: [tenants[0]!, { orgId: "org_proof_a", workspaceId: "other" }],
      }),
    ).toThrow();
  });
});

/**
 * The planter's wire contract, against the real AlleyOop manifest route and the
 * real server-side chunk sealer: revision 1 is create-only, revision 2 carries
 * the previous object version, and a token for another org is refused.
 */
describe("planting through the real manifest route", () => {
  let root: string;
  beforeEach(async () => {
    root = await mkdtemp(join(tmpdir(), "aws-proof-ingest-"));
  });
  afterEach(async () => {
    await rm(root, { recursive: true, force: true });
  });

  function server(orgId: string, keyPrefix?: string) {
    const store = createFsStore({ root });
    const raw = { store, provider: "fs" as const, bucket: "raw" };
    const sent: StageMessage[] = [];
    const app = new Hono<AppEnv>();
    app.use("*", async (c, next) => {
      c.set("correlationId", "test");
      const encoded = /^Bearer fake\.(.+)$/.exec(c.req.header("authorization") ?? "")?.[1];
      if (!encoded) return c.json({ error: { code: "unauthorized" } }, 401);
      const claims = JSON.parse(Buffer.from(encoded, "base64url").toString("utf8")) as { orgId?: string; machineId?: string };
      c.set("auth", {
        userId: "u",
        orgId: claims.orgId ?? orgId,
        workspaceId: "default",
        tokenSubject: "agent_relayhistory_assertion",
        scopes: ["rth:sync"],
        claims: { meta: claims.machineId ? { machineId: claims.machineId } : {} },
        historyOrigin: "https://history.example",
      });
      await next();
    });
    app.use("/v1/*", liveWriteMachineGate);
    // The chunk hop: the same server-side sealer the ingest uses (Technical Foul scrubs here).
    app.post("/v1/pipeline/chunks", async (c) => {
      const q = c.req.query();
      const sealed = await sealRawChunk(store, {
        location: {
          ...(keyPrefix ? { prefix: `${keyPrefix}/${orgId}` } : {}),
          workspaceId: "default",
          machineId: q.machine!,
          harness: q.harness!,
          sessionId: q.session!,
          startDate: q.date!,
        },
        format: "jsonl",
        seq: Number(q.seq),
        epoch: Number(q.epoch),
        startOffset: Number(q.offset),
        source: new Uint8Array(await c.req.arrayBuffer()),
        traceId: c.req.header("x-relay-trace-id"),
      });
      return c.json(sealed);
    });
    app.route(
      "/v1",
      createCourierManifestRoutes({
        manifests: objectStoreManifestReader(store, {
          provider: "fs",
          bucket: "raw",
        }),
        raw,
        send: async (_e, _s, m) => void sent.push(m),
      }),
    );
    const env = {
      ...(keyPrefix
        ? { PIPELINE_KEY_PREFIX: keyPrefix, PIPELINE_ORG_FROM_PREFIX: "true" }
        : { PIPELINE_ORG_ID: "org_proof_a" }),
      RELAYHISTORY_ORIGIN: "https://history.example",
      PIPELINE_TELEMETRY_ID_KEY: "test-telemetry-key-at-least-32-bytes-long",
    };
    const fetch = async (input: string, init?: RequestInit) =>
      app.request(input.replace("http://ingest", ""), init, env);
    return { fetch, sent, store };
  }

  test("revision 1 then revision 2 land (202) and enqueue object-landed twice", async () => {
    const [p] = plantCorpus({
      runId: "aws-proof-ingest-0001",
      sessions: 2,
      tenants,
    });
    const s = server("org_proof_a");
    const token = await mint(fakeRelayAuth(), "org_proof_a", p!.machineId);
    const st = newUploadState();
    const r1 = await plantNextRevision(
      { ingestUrl: "http://ingest", fetch: s.fetch },
      p!,
      token,
      st,
    );
    const r2 = await plantNextRevision(
      { ingestUrl: "http://ingest", fetch: s.fetch },
      p!,
      token,
      st,
    );
    expect([r1.revision, r2.revision]).toEqual([1, 2]);
    expect(r1.manifestKey).toContain(`s=${p!.sessionId}/manifest.json`);
    expect(st.chunks).toHaveLength(2);
    expect(s.sent).toHaveLength(2);
    // Technical Foul ran server-side: the planted transcript text is stored, the fake key is not.
    const listed = (
      await s.store.list(
        objectKey(`v1/ws=default/m=${locationOf(p!).machineId}`),
      )
    ).items.filter((o) => o.key.includes("/raw/"));
    expect(listed).toHaveLength(2);
    let text = "";
    for (const o of listed)
      text += zstdDecompressSync(
        Buffer.from(await readAll(await s.store.get(o.key))),
      ).toString("utf8");
    expect(text).toContain(p!.transcriptCanary);
    expect(text).not.toContain(FAKE_AWS_KEY_ID);
    await expect(
      plantNextRevision(
        { ingestUrl: "http://ingest", fetch: s.fetch },
        p!,
        token,
        st,
      ),
    ).rejects.toThrow(/only 2 revisions/);
  });

  test("a token for another org is refused by the manifest route (owner check)", async () => {
    const [p] = plantCorpus({
      runId: "aws-proof-ingest-0002",
      sessions: 2,
      tenants,
    });
    const s = server("org_proof_b");
    const err = await plantNextRevision(
      { ingestUrl: "http://ingest", fetch: s.fetch },
      p!,
      await mint(fakeRelayAuth(), "org_proof_b", p!.machineId),
      newUploadState(),
    ).catch((e) => e);
    expect(err).toBeInstanceOf(IngestError);
    expect((err as IngestError).status).toBe(403);
  });

  // Not run against the real manifest route: on this branch's main, chunksAreServerSealed
  // (src/alleyoop/manifests.ts) rebuilds chunk keys without the key prefix, so with
  // PIPELINE_ORG_FROM_PREFIX a prefixed manifest fails with 409 chunks_unverified.
  // Reported on the PR; the harness side is checked here.
  test("proof-stage manifests are keyed 96bulls/<org>/v1/... and validate", async () => {
    const tenants2 = [
      { orgId: "org_aws_proof_1", workspaceId: "default" },
      { orgId: "org_aws_proof_2", workspaceId: "default" },
    ];
    const [p] = plantCorpus({
      runId: "aws-proof-ingest-0003",
      sessions: 2,
      tenants: tenants2,
      keyPrefixBase: "96bulls",
    });
    const store = createFsStore({ root });
    const bodies: unknown[] = [];
    const fetch = async (url: string, init?: RequestInit) => {
      if (url.includes("/v1/pipeline/chunks")) {
        const q = new URL(url).searchParams;
        const sealed = await sealRawChunk(store, {
          location: {
            prefix: "96bulls/org_aws_proof_1",
            workspaceId: "default",
            machineId: q.get("machine")!,
            harness: q.get("harness")!,
            sessionId: q.get("session")!,
            startDate: q.get("date")!,
          },
          format: "jsonl",
          seq: 0,
          epoch: 0,
          startOffset: 0,
          source: new Uint8Array(init!.body as Uint8Array),
        });
        return new Response(JSON.stringify(sealed));
      }
      bodies.push(JSON.parse(String(init!.body)));
      return new Response(JSON.stringify({ version: { eTag: "e1" } }), {
        status: 202,
      });
    };
    const r1 = await plantNextRevision(
      { ingestUrl: "http://ingest", fetch },
      p!,
      "tok",
      newUploadState(),
    );
    expect(r1.manifestKey).toMatch(
      /^96bulls\/org_aws_proof_1\/v1\/ws=default\//,
    );
    expect((bodies[0] as { key: string }).key).toBe(r1.manifestKey);
  });

  test("machine-bound proof tokens: exact match lands; claimless and mismatch write nothing", async () => {
    const [p] = plantCorpus({ runId: "aws-proof-machine-0001", sessions: 2, tenants });
    const matching = server("org_proof_a");
    const issuer = fakeRelayAuth();
    const token = await mint(issuer, "org_proof_a", p!.machineId);
    expect(issuer.minted[0]).toMatchObject({ orgId: "org_proof_a", machineId: p!.machineId });
    await expect(mint(fakeRelayAuth(), "org_proof_a", "relay_reserved")).rejects.toMatchObject({ code: "machine_id_invalid" });
    expect((await plantNextRevision({ ingestUrl: "http://ingest", fetch: matching.fetch }, p!, token, newUploadState())).revision).toBe(1);
    expect(matching.sent).toHaveLength(1);

    for (const denied of [
      await mint(fakeRelayAuth(true), "org_proof_a", p!.machineId),
      await mint(fakeRelayAuth(), "org_proof_a", "proof-other-machine"),
    ]) {
      const isolated = server("org_proof_a");
      const prefix = objectKey(`v1/ws=default/m=${p!.machineId}`);
      const before = (await isolated.store.list(prefix)).items.length;
      const error = await plantNextRevision({ ingestUrl: "http://ingest", fetch: isolated.fetch }, p!, denied, newUploadState()).catch((caught) => caught);
      expect(error).toBeInstanceOf(IngestError);
      expect([(error as IngestError).status, (error as IngestError).code]).toEqual([403, "machine_claim_refused"]);
      expect(isolated.sent).toHaveLength(0);
      expect((await isolated.store.list(prefix)).items).toHaveLength(before);
    }
  });
});
