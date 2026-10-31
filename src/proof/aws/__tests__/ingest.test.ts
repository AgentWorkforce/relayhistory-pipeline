import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Hono } from "hono";
import { createFsStore } from "../../../../packages/object-store/src/fs.ts";
import { isUuidV7 } from "../../../shared/ids.ts";
import { objectKey, objectStoreManifestReader, readAll, sealRawChunk, type AppEnv, type StageMessage } from "../../../shared/vendor.ts";
import { zstdDecompressSync } from "node:zlib";
import { createCourierManifestRoutes } from "../../../alleyoop/manifests.ts";
import { allCanaries, FAKE_AWS_KEY_ID, plantCorpus } from "../corpus.ts";
import { IngestError, locationOf, newUploadState, plantNextRevision } from "../ingest.ts";

const tenants = [
  { orgId: "org_proof_a", workspaceId: "default" },
  { orgId: "org_proof_b", workspaceId: "default" },
];

describe("planted corpus", () => {
  const c = plantCorpus({ runId: "aws-proof-corpus-0001", sessions: 100, tenants });
  test("UUIDv7 ids, across both tenants, unique canaries", () => {
    expect(c).toHaveLength(100);
    for (const p of c) expect([p.sessionId, p.sessionUuid, p.traceId].every(isUuidV7)).toBe(true);
    expect(new Set(c.map((p) => p.sessionId)).size).toBe(100);
    expect(c.filter((p) => p.tenant.orgId === "org_proof_a")).toHaveLength(50);
    expect(new Set(c.map((p) => p.summaryTerm)).size).toBe(100);
    expect(new Set(c.map((p) => p.transcriptCanary)).size).toBe(100);
    // No canary is a substring of another session's canary (s1 vs s10).
    const all = allCanaries(c);
    for (const a of all) expect(all.filter((b) => b !== a && b.includes(a))).toEqual([]);
  });
  test("transcript-only canary never appears in the first user turn", () => {
    for (const p of c) expect(p.revisions[0]![0]).not.toContain(p.transcriptCanary);
  });
  test("refuses a single tenant or a shared org", () => {
    expect(() => plantCorpus({ runId: "aws-proof-x-0001", sessions: 4, tenants: [tenants[0]!] })).toThrow();
    expect(() => plantCorpus({ runId: "aws-proof-x-0001", sessions: 4, tenants: [tenants[0]!, { orgId: "org_proof_a", workspaceId: "other" }] })).toThrow();
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

  function server(orgId: string) {
    const store = createFsStore({ root });
    const raw = { store, provider: "fs" as const, bucket: "raw" };
    const sent: StageMessage[] = [];
    const app = new Hono<AppEnv>();
    app.use("*", async (c, next) => {
      c.set("correlationId", "test");
      c.set("auth", { userId: "u", orgId, workspaceId: "default", tokenSubject: "agent_relayhistory_assertion", scopes: ["rth:sync"], claims: {}, historyOrigin: "https://history.example" });
      await next();
    });
    // The chunk hop: the same server-side sealer the ingest uses (Technical Foul scrubs here).
    app.post("/v1/pipeline/chunks", async (c) => {
      const q = c.req.query();
      const sealed = await sealRawChunk(store, {
        location: { workspaceId: "default", machineId: q.machine!, harness: q.harness!, sessionId: q.session!, startDate: q.date! },
        format: "jsonl", seq: Number(q.seq), epoch: Number(q.epoch), startOffset: Number(q.offset),
        source: new Uint8Array(await c.req.arrayBuffer()), traceId: c.req.header("x-relay-trace-id"),
      });
      return c.json(sealed);
    });
    app.route("/v1", createCourierManifestRoutes({ manifests: objectStoreManifestReader(store, { provider: "fs", bucket: "raw" }), raw, send: async (_e, _s, m) => void sent.push(m) }));
    const env = { PIPELINE_ORG_ID: "org_proof_a", RELAYHISTORY_ORIGIN: "https://history.example", PIPELINE_TELEMETRY_ID_KEY: "test-telemetry-key-at-least-32-bytes-long" };
    const fetch = async (input: string, init?: RequestInit) => app.request(input.replace("http://ingest", ""), init, env);
    return { fetch, sent, store };
  }

  test("revision 1 then revision 2 land (202) and enqueue object-landed twice", async () => {
    const [p] = plantCorpus({ runId: "aws-proof-ingest-0001", sessions: 2, tenants });
    const s = server("org_proof_a");
    const st = newUploadState();
    const r1 = await plantNextRevision({ ingestUrl: "http://ingest", fetch: s.fetch }, p!, "tok", st);
    const r2 = await plantNextRevision({ ingestUrl: "http://ingest", fetch: s.fetch }, p!, "tok", st);
    expect([r1.revision, r2.revision]).toEqual([1, 2]);
    expect(r1.manifestKey).toContain(`s=${p!.sessionId}/manifest.json`);
    expect(st.chunks).toHaveLength(2);
    expect(s.sent).toHaveLength(2);
    // Technical Foul ran server-side: the planted transcript text is stored, the fake key is not.
    const listed = (await s.store.list(objectKey(`v1/ws=default/m=${locationOf(p!).machineId}`))).items.filter((o) => o.key.includes("/raw/"));
    expect(listed).toHaveLength(2);
    let text = "";
    for (const o of listed) text += zstdDecompressSync(Buffer.from(await readAll(await s.store.get(o.key)))).toString("utf8");
    expect(text).toContain(p!.transcriptCanary);
    expect(text).not.toContain(FAKE_AWS_KEY_ID);
    await expect(plantNextRevision({ ingestUrl: "http://ingest", fetch: s.fetch }, p!, "tok", st)).rejects.toThrow(/only 2 revisions/);
  });

  test("a token for another org is refused by the manifest route (owner check)", async () => {
    const [p] = plantCorpus({ runId: "aws-proof-ingest-0002", sessions: 2, tenants });
    const s = server("org_proof_b");
    const err = await plantNextRevision({ ingestUrl: "http://ingest", fetch: s.fetch }, p!, "tok", newUploadState()).catch((e) => e);
    expect(err).toBeInstanceOf(IngestError);
    expect((err as IngestError).status).toBe(403);
  });
});
