/** The offline workspace-synthesis job and its artifact store (docs/session-intel-workspace-synthesis.md, "Storage"). */
import { describe, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import {
  artifactJson, latestWorkspaceSynthesis, SYNTHESIS_ARTIFACT_MAX_BYTES, memoryArtifactStore, namedSessions, objectStoreArtifacts, runWorkspaceSynthesisJob,
  SYNTHESIS_ARTIFACT_FORMAT, SynthesisArtifactError, workspaceSynthesisDirectory, workspaceSynthesisKey, workspaceSynthesisPrefix, type SynthesisArtifact,
} from "../workspace-synthesis.ts";
import { SynthesisLayoutError } from "../workspace-synthesis-layout.ts";
import { indexPointer } from "../../session-pipeline/pointer.ts";
import { synthesisJson, synthesisProvenance, synthesize } from "../../session-intel/synthesis/synthesize.ts";
import { storeEraser, type EraseCheckpoint } from "../../migration/neon-s3-erasure/eraser.ts";
import { userErasePrefixes } from "../../session-erase/user-erase.ts";
import { ANALYSIS_RESULT_OBJECT_GLOB } from "../../../stacks/aws/session-pipeline.ts";
import { TENANT_KEY_BASE } from "../../../stacks/aws/tenants.ts";
import { withChild } from "./workspace-synthesis-fixtures.ts";

const FIXTURE = new URL("../../session-intel/__tests__/fixtures/synthesis/", import.meta.url);
const cohort = () => JSON.parse(readFileSync(new URL("cohort.json", FIXTURE), "utf8"));
const NOW = "2026-11-08T00:00:00.000Z";
const WINDOW = { since: "2026-11-01T00:00:00.000Z", until: "2026-11-08T00:00:00.000Z" };
const LOOKUP = { keyRoot: "fxroot", orgId: "fx-org", workspace: "fx-ws-synthesis", window: WINDOW };
const job = (store = memoryArtifactStore(), now = NOW, c: unknown = cohort()) => runWorkspaceSynthesisJob({ store, keyRoot: "fxroot", orgId: "fx-org", cohort: c, options: { now } });

describe("key layout", () => {
  test("one directory per workspace inside the analysis-result family, one prefix per window, keys sorted by generation", () => {
    const prefix = workspaceSynthesisPrefix("fxroot", "fx-org", "fx-ws-synthesis", WINDOW);
    expect(prefix).toBe(`fxroot/fx-org/v1/ws=fx-ws-synthesis/normalized/session-analysis-v2/workspace-synthesis-v1/w=${Date.parse(WINDOW.since)}-${Date.parse(WINDOW.until)}/`);
    expect(prefix.startsWith(workspaceSynthesisDirectory("fxroot", "fx-org", "fx-ws-synthesis"))).toBe(true);
    // A workspace id with a character the layout maps stays one path segment (layoutComponent).
    expect(workspaceSynthesisDirectory("fxroot", "fx-org", "fx:ws")).toBe("fxroot/fx-org/v1/ws=fx_ws/normalized/session-analysis-v2/workspace-synthesis-v1/");
    const a = workspaceSynthesisKey(prefix, "2026-11-08T00:00:00.000Z", "a".repeat(64));
    const b = workspaceSynthesisKey(prefix, "2026-11-09T00:00:00.000Z", "0".repeat(64));
    expect(a < b).toBe(true);
    for (const bad of [() => workspaceSynthesisPrefix("../", "fx-org", "fx-ws", WINDOW), () => workspaceSynthesisPrefix("fxroot", "fx/org", "fx-ws", WINDOW), () => workspaceSynthesisPrefix("fxroot", "fx-org", "fx/ws", WINDOW), () => workspaceSynthesisPrefix("fxroot", "fx-org", "fx-ws", { since: WINDOW.until, until: WINDOW.since })])
      expect(bad).toThrow(SynthesisLayoutError);
    expect(() => workspaceSynthesisKey(prefix, NOW, "xyz")).toThrow(SynthesisArtifactError);
  });

  test("the pipeline's pointer stage ignores an artifact key, and no manifest notification can match it", async () => {
    const key = workspaceSynthesisKey(workspaceSynthesisPrefix("fxroot", "fx-org", "fx-ws-synthesis", WINDOW), NOW, "a".repeat(64));
    expect(key.endsWith("/manifest.json")).toBe(false);
    const outcome = await indexPointer({ bucket: "fx-bucket", key, size: 1, etag: "e", eventTime: NOW } as never, {} as never);
    expect(outcome).toEqual({ status: "ignored", reason: "not_a_session_key" });
  });
});

describe("the job", () => {
  test("writes one artifact with the synthesis and every considered session; a rerun is idempotent", async () => {
    const store = memoryArtifactStore();
    const first = await job(store);
    expect(first.outcome).toBe("created");
    expect(store.objects.size).toBe(1);
    expect(first.key.startsWith(workspaceSynthesisPrefix("fxroot", "fx-org", "fx-ws-synthesis", WINDOW))).toBe(true);
    const artifact = JSON.parse(new TextDecoder().decode(store.objects.get(first.key)!)) as SynthesisArtifact;
    expect(artifact.format).toBe(SYNTHESIS_ARTIFACT_FORMAT);
    expect(artifact.orgId).toBe("fx-org");
    expect(artifact.considered.length).toBe(artifact.synthesis.inputs.considered);
    expect(Object.keys(artifact).sort()).toEqual(["considered", "format", "generatedAt", "orgId", "related", "synthesis", "window", "workspace"]);
    // The fixture's evidence all belongs to the considered sessions themselves: nothing related.
    expect(artifact.related).toEqual([]);
    // The synthesis inside is the golden's.
    expect(synthesisJson(artifact.synthesis)).toBe(readFileSync(new URL("expected-synthesis.json", FIXTURE), "utf8"));
    const listed = new Set(artifact.considered.map((r) => `${r.source}:${r.sessionId}`));
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
    const { key: own } = await job(store);
    // Another org's lookup reads its own prefix: nothing there.
    expect(await latestWorkspaceSynthesis(store, { ...LOOKUP, orgId: "fx-org-other" })).toBeNull();
    // This org's bytes copied under the other org's prefix are refused loudly.
    const otherPrefix = workspaceSynthesisPrefix("fxroot", "fx-org-other", "fx-ws-synthesis", WINDOW);
    store.objects.set(`${otherPrefix}${own.slice(own.lastIndexOf("/") + 1)}`, store.objects.get(own)!);
    await expect(latestWorkspaceSynthesis(store, { ...LOOKUP, orgId: "fx-org-other" })).rejects.toThrow("synthesis_cross_tenant");
    // An artifact whose session list misses a drilldown session is not complete.
    const fresh = memoryArtifactStore();
    const { key } = await job(fresh);
    const a = JSON.parse(new TextDecoder().decode(fresh.objects.get(key)!)) as SynthesisArtifact;
    const named = namedSessions(a.synthesis)[0]!;
    const broken: SynthesisArtifact = { ...a, generatedAt: "2026-11-08T07:00:00.000Z", synthesis: { ...a.synthesis, generatedAt: "2026-11-08T07:00:00.000Z" }, considered: a.considered.filter((r) => !(r.source === named.source && r.sessionId === named.sessionId)) };
    const bytes = new TextEncoder().encode(artifactJson(broken));
    const digest = new Bun.CryptoHasher("sha256").update(bytes).digest("hex");
    await fresh.putNew(workspaceSynthesisKey(workspaceSynthesisPrefix("fxroot", "fx-org", a.workspace, a.window), broken.generatedAt, digest), bytes, digest);
    expect((await latestWorkspaceSynthesis(fresh, LOOKUP))!.generatedAt).toBe(NOW);
  });

  test("works over a real ObjectStore (the filesystem store): create-only, read back, listed", async () => {
    const root = mkdtempSync(join(tmpdir(), "si-synthesis-store-"));
    try {
      const store = objectStoreArtifacts(createFsStore({ root }));
      const first = await runWorkspaceSynthesisJob({ store, keyRoot: "fxroot", orgId: "fx-org", cohort: cohort(), options: { now: NOW } });
      expect(first.outcome).toBe("created");
      expect((await runWorkspaceSynthesisJob({ store, keyRoot: "fxroot", orgId: "fx-org", cohort: cohort(), options: { now: NOW } })).outcome).toBe("exists");
      expect((await latestWorkspaceSynthesis(store, LOOKUP))!.synthesis.inputs.analyzed).toBe(40);
      expect(await store.get(`${first.key}.missing`, 10)).toBeUndefined();
      await expect(store.get(first.key, 10)).rejects.toThrow("synthesis_artifact_bound");
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("an invalid org id or cohort is refused before anything is written", async () => {
    const store = memoryArtifactStore();
    await expect(runWorkspaceSynthesisJob({ store, keyRoot: "fxroot", orgId: "fx org", cohort: cohort(), options: { now: NOW } })).rejects.toThrow("synthesis_org_invalid");
    const c = cohort();
    c.sessions[0].workspace = "fx-ws-other";
    await expect(job(store, NOW, c)).rejects.toThrow(/cross_workspace/u);
    expect(store.objects.size).toBe(0);
  });
});

describe("erasure", () => {
  test("the per-user erase lane's prefixes cover the workspace's synthesis directory, and erasing them REMOVES the artifacts", async () => {
    const root = mkdtempSync(join(tmpdir(), "si-synthesis-erase-"));
    try {
      const fs = createFsStore({ root });
      const store = objectStoreArtifacts(fs);
      const first = await runWorkspaceSynthesisJob({ store, keyRoot: "fxroot", orgId: "fx-org", cohort: cohort(), options: { now: NOW } });
      const second = await runWorkspaceSynthesisJob({ store, keyRoot: "fxroot", orgId: "fx-org", cohort: cohort(), options: { now: "2026-11-08T06:00:00.000Z" } });
      // Another workspace's artifact under the same org must survive.
      const other = cohort();
      other.workspace = "fx-ws-other";
      for (const e of other.sessions) e.workspace = "fx-ws-other";
      const kept = await runWorkspaceSynthesisJob({ store, keyRoot: "fxroot", orgId: "fx-org", cohort: other, options: { now: NOW } });

      const prefixes = userErasePrefixes("fxroot", { requestId: "fx-req", orgId: "fx-org", workspaceId: "fx-ws-synthesis", userId: "fx-user" }, ["fx-origin"]);
      expect(prefixes).toContain(workspaceSynthesisDirectory("fxroot", "fx-org", "fx-ws-synthesis"));
      const eraser = storeEraser(fs);
      for (const prefix of prefixes) {
        let checkpoint: EraseCheckpoint | null = null;
        for (;;) {
          const r = await eraser.eraseStep(prefix, { scope: "fx-erase", checkpoint, authorize: async () => {} });
          if (r.status === "traversed") break;
          checkpoint = r.checkpoint;
        }
      }
      expect(await store.get(first.key, SYNTHESIS_ARTIFACT_MAX_BYTES)).toBeUndefined();
      expect(await store.get(second.key, SYNTHESIS_ARTIFACT_MAX_BYTES)).toBeUndefined();
      expect(await store.list(workspaceSynthesisDirectory("fxroot", "fx-org", "fx-ws-synthesis"), 100)).toEqual([]);
      expect(await latestWorkspaceSynthesis(store, LOOKUP)).toBeNull();
      expect(await store.get(kept.key, SYNTHESIS_ARTIFACT_MAX_BYTES)).toBeDefined();
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("an org-wide user erase (empty workspace) names no synthesis directory", () => {
    const prefixes = userErasePrefixes("fxroot", { requestId: "fx-req", orgId: "fx-org", workspaceId: "", userId: "fx-user" }, []);
    expect(prefixes.some((p) => p.includes("workspace-synthesis-v1"))).toBe(false);
  });

  test("the artifact key is inside the bucket's analysis-result object glob (no policy change)", () => {
    const key = workspaceSynthesisKey(workspaceSynthesisPrefix(TENANT_KEY_BASE, "fx-org", "fx-ws-synthesis", WINDOW), NOW, "a".repeat(64));
    // S3 policy `*` matches any run of characters, `/` included.
    const glob = new RegExp(`^${ANALYSIS_RESULT_OBJECT_GLOB.split("*").map((p) => p.replace(/[.?+^$()[\]{}|\\]/gu, "\\$&")).join(".*")}$`, "u");
    expect(glob.test(key)).toBe(true);
  });

  test("provenance records every other session the evidence belongs to, children included", () => {
    const c = withChild(cohort());
    const s = synthesize(c, { now: NOW });
    expect(s.inputs.analyzed).toBe(40);
    const p = synthesisProvenance(c, { now: NOW });
    expect(p.related).toEqual([{ source: "fx-claude", sessionId: "fx-s01-child" }, { source: "fx-claude", sessionId: "fx-s01-spawned" }]);
  });
});
