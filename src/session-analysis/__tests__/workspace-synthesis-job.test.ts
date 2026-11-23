/** The offline workspace-synthesis job and its artifact store (docs/session-intel-workspace-synthesis.md, "Storage"). */
import { describe, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import {
  artifactJson, latestWorkspaceSynthesis, memoryArtifactStore, namedSessions, objectStoreArtifacts, runWorkspaceSynthesisJob,
  SYNTHESIS_ARTIFACT_FORMAT, SynthesisArtifactError, workspaceSynthesisKey, workspaceSynthesisPrefix, type SynthesisArtifact,
} from "../workspace-synthesis.ts";
import { synthesisJson } from "../../session-intel/synthesis/synthesize.ts";

const FIXTURE = new URL("../../session-intel/__tests__/fixtures/synthesis/", import.meta.url);
const cohort = () => JSON.parse(readFileSync(new URL("cohort.json", FIXTURE), "utf8"));
const NOW = "2026-11-08T00:00:00.000Z";
const WINDOW = { since: "2026-11-01T00:00:00.000Z", until: "2026-11-08T00:00:00.000Z" };
const LOOKUP = { root: "", orgId: "fx-org", workspace: "fx-ws-synthesis", window: WINDOW };
const job = (store = memoryArtifactStore(), now = NOW, c: unknown = cohort()) => runWorkspaceSynthesisJob({ store, root: "", orgId: "fx-org", cohort: c, options: { now } });

describe("key layout", () => {
  test("one prefix per workspace and window, keys sorted by generation", () => {
    const prefix = workspaceSynthesisPrefix("", "fx-ws-synthesis", WINDOW);
    expect(prefix).toBe(`derived/workspace-synthesis-v1/ws=fx-ws-synthesis/w=${Date.parse(WINDOW.since)}-${Date.parse(WINDOW.until)}/`);
    expect(workspaceSynthesisPrefix("tenant-root/", "fx-ws-synthesis", WINDOW).startsWith("tenant-root/derived/")).toBe(true);
    const a = workspaceSynthesisKey(prefix, "2026-11-08T00:00:00.000Z", "a".repeat(64));
    const b = workspaceSynthesisKey(prefix, "2026-11-09T00:00:00.000Z", "0".repeat(64));
    expect(a < b).toBe(true);
    for (const bad of [() => workspaceSynthesisPrefix("../", "fx-ws", WINDOW), () => workspaceSynthesisPrefix("", "fx/ws", WINDOW), () => workspaceSynthesisPrefix("", "fx-ws", { since: WINDOW.until, until: WINDOW.since }), () => workspaceSynthesisKey(prefix, NOW, "xyz")])
      expect(bad).toThrow(SynthesisArtifactError);
  });
});

describe("the job", () => {
  test("writes one artifact with the synthesis and every considered session; a rerun is idempotent", async () => {
    const store = memoryArtifactStore();
    const first = await job(store);
    expect(first.outcome).toBe("created");
    expect(store.objects.size).toBe(1);
    expect(first.key.startsWith(workspaceSynthesisPrefix("", "fx-ws-synthesis", WINDOW))).toBe(true);
    const artifact = JSON.parse(new TextDecoder().decode(store.objects.get(first.key)!)) as SynthesisArtifact;
    expect(artifact.format).toBe(SYNTHESIS_ARTIFACT_FORMAT);
    expect(artifact.orgId).toBe("fx-org");
    expect(artifact.sessions.length).toBe(artifact.synthesis.inputs.considered);
    // The synthesis inside is the golden's.
    expect(synthesisJson(artifact.synthesis)).toBe(readFileSync(new URL("expected-synthesis.json", FIXTURE), "utf8"));
    const listed = new Set(artifact.sessions.map((r) => `${r.source}:${r.sessionId}`));
    for (const r of namedSessions(artifact.synthesis)) expect(listed.has(`${r.source}:${r.sessionId}`)).toBe(true);
    // Skipped sessions are counted, so they are listed too (an erased one must invalidate the counts).
    expect(listed.has("fx-claude:fx-skip-not-analyzed")).toBe(true);
    const again = await job(store);
    expect([again.outcome, again.key, store.objects.size]).toEqual(["exists", first.key, 1]);
  });

  test("the newest complete artifact is authoritative; an altered or mismatched one is passed over", async () => {
    const store = memoryArtifactStore();
    const older = await job(store, "2026-11-08T00:00:00.000Z");
    const newer = await job(store, "2026-11-08T06:00:00.000Z");
    expect((await latestWorkspaceSynthesis(store, LOOKUP))!.generatedAt).toBe("2026-11-08T06:00:00.000Z");
    // Altered bytes (the digest in the key no longer matches): the older one is served.
    const bytes = store.objects.get(newer.key)!;
    store.objects.set(newer.key, new Uint8Array([...bytes.slice(0, -2), 0x20, 0x0a]));
    expect((await latestWorkspaceSynthesis(store, LOOKUP))!.generatedAt).toBe("2026-11-08T00:00:00.000Z");
    // Another window or workspace finds nothing.
    expect(await latestWorkspaceSynthesis(store, { ...LOOKUP, window: { since: WINDOW.since, until: "2026-11-07T00:00:00.000Z" } })).toBeNull();
    expect(await latestWorkspaceSynthesis(store, { ...LOOKUP, workspace: "fx-ws-other" })).toBeNull();
    expect(older.outcome).toBe("created");
  });

  test("an artifact of another org under the prefix is refused loudly; an incomplete one is never served", async () => {
    const store = memoryArtifactStore();
    await job(store);
    await expect(latestWorkspaceSynthesis(store, { ...LOOKUP, orgId: "fx-org-other" })).rejects.toThrow("synthesis_cross_tenant");
    // An artifact whose session list misses a drilldown session is not complete.
    const fresh = memoryArtifactStore();
    const { key } = await job(fresh);
    const a = JSON.parse(new TextDecoder().decode(fresh.objects.get(key)!)) as SynthesisArtifact;
    const named = namedSessions(a.synthesis)[0]!;
    const broken: SynthesisArtifact = { ...a, generatedAt: "2026-11-08T07:00:00.000Z", synthesis: { ...a.synthesis, generatedAt: "2026-11-08T07:00:00.000Z" }, sessions: a.sessions.filter((r) => !(r.source === named.source && r.sessionId === named.sessionId)) };
    const bytes = new TextEncoder().encode(artifactJson(broken));
    const digest = new Bun.CryptoHasher("sha256").update(bytes).digest("hex");
    await fresh.putNew(workspaceSynthesisKey(workspaceSynthesisPrefix("", a.workspace, a.window), broken.generatedAt, digest), bytes, digest);
    expect((await latestWorkspaceSynthesis(fresh, LOOKUP))!.generatedAt).toBe(NOW);
  });

  test("works over a real ObjectStore (the filesystem store): create-only, read back, listed", async () => {
    const root = mkdtempSync(join(tmpdir(), "si-synthesis-store-"));
    try {
      const store = objectStoreArtifacts(createFsStore({ root }));
      const first = await runWorkspaceSynthesisJob({ store, root: "", orgId: "fx-org", cohort: cohort(), options: { now: NOW } });
      expect(first.outcome).toBe("created");
      expect((await runWorkspaceSynthesisJob({ store, root: "", orgId: "fx-org", cohort: cohort(), options: { now: NOW } })).outcome).toBe("exists");
      expect((await latestWorkspaceSynthesis(store, LOOKUP))!.synthesis.inputs.analyzed).toBe(40);
      expect(await store.get(`${first.key}.missing`, 10)).toBeUndefined();
      await expect(store.get(first.key, 10)).rejects.toThrow("synthesis_artifact_bound");
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("an invalid org id or cohort is refused before anything is written", async () => {
    const store = memoryArtifactStore();
    await expect(runWorkspaceSynthesisJob({ store, root: "", orgId: "fx org", cohort: cohort(), options: { now: NOW } })).rejects.toThrow("synthesis_org_invalid");
    const c = cohort();
    c.sessions[0].workspace = "fx-ws-other";
    await expect(job(store, NOW, c)).rejects.toThrow(/cross_workspace/u);
    expect(store.objects.size).toBe(0);
  });
});
