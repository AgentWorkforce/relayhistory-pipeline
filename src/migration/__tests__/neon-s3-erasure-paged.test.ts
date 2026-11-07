/**
 * The E3 erasure job on the shared strict, paged, resumable eraser (eraser.ts eraseStep/verifyErased, owned by the
 * Babysitter lane): machines over 10k versions resume across runs from a durable checkpoint instead of failing closed;
 * crash/restart and a lost DELETE acknowledgement resume; a re-enable stops before any further delete; a checkpoint
 * bound to another scope is refused; successor versions are kept; nothing is marked done without a fresh strict zero
 * inventory. Offline provider: fake-s3-versions.ts.
 */
import { describe, expect, setDefaultTimeout, test } from "bun:test";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { FakeS3Versions } from "./fake-s3-versions.ts";
import { DEFAULT_ERASURE_CONFIG, ErasureJob, eraseScope } from "../neon-s3-erasure/job.ts";
import { liveStagedPrefix, liveTargets, livePointerPrefix } from "../neon-s3-erasure/live.ts";
import type { ErasureAuthority } from "../neon-s3-erasure/authority.ts";
import type { LiveFencedGroup } from "../neon-s3-sync/neon-reader.ts";
import { SyncState } from "../neon-s3-sync/state.ts";

/**
 * TEST-ONLY: exporter-path completion is opted into explicitly through the job's test seam
 * (ErasureConfig.obligationsSettled; production never sets it). The state file itself can never attest.
 */
const SETTLED_CFG = { ...DEFAULT_ERASURE_CONFIG, obligationsSettled: () => true };
const attestedState = (path: string) => new SyncState(path);

setDefaultTimeout(120_000);
const G: LiveFencedGroup = { org_id: "org-1", workspace_id: "ws/1", user_id: "user-1", origin_id: "origin-1", source: "claude", session_id: "sess:1" };
const T = liveTargets("96bulls", G);
const dir = (d: string) => `${T.base}d=${d}/${T.sessionSegment}/`;
const OLD = Date.parse("2026-11-01T00:00:00Z");
/** The job's clock: well after the fixture versions (beyond LIVE_SKEW_MS). */
const NOW = Date.parse("2026-11-07T00:00:00Z");

function authority(over: Partial<ErasureAuthority> = {}): ErasureAuthority & { fenced: boolean } {
  const a = {
    fenced: true,
    kind: "supabase" as const,
    ready: async () => {},
    lockWaiters: async () => ({ total: 0, nonAdvisory: 0 }),
    fencedGroups: async () => [],
    fencedLivePage: async (after: LiveFencedGroup | null) => (after ? [] : [G]),
    stillFenced: async () => a.fenced,
    ...over,
  };
  return a;
}
let lastPath = "";
async function state() {
  lastPath = join(await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "e3-paged-")), "s.sqlite");
  return attestedState(lastPath);
}
function fill(s: FakeS3Versions, n: number, at = OLD) {
  for (let i = 0; i < n; i++) s.put(`${dir("2026/10/30")}raw/b=${String(i).padStart(6, "0")}.ndjson.zst`, at);
  s.put(`${dir("2026/10/30")}manifest.json`, at);
  s.put(T.pointerKey, at);
}
const job = (s: FakeS3Versions, st: SyncState, a: ErasureAuthority, now = () => NOW) =>
  new ErasureJob(a, st, s.eraser(), () => {}, SETTLED_CFG, now, async () => {});

describe("E3 job on the shared paged eraser", () => {
  test("a machine over 10k versions resumes across sweeps (and a fresh process) instead of failing closed; pointer last", async () => {
    const s = new FakeS3Versions();
    fill(s, 12_500);
    const st = await state();
    const path = lastPath;
    const a = authority();
    await job(s, st, a).sweep();
    // Run 1: the per-run 10k ceiling, a durable checkpoint, the session and its pointer NOT reported done.
    expect(s.count(dir("2026/10/30"))).toBe(12_501 - 10_000);
    expect(s.count(T.pointerKey)).toBe(1);
    expect(st.eraseCheckpoint(eraseScope(["live-session", "supabase", G.org_id, G.workspace_id, G.user_id, G.origin_id, G.source, G.session_id]))).not.toBeNull();
    // Crash: a brand-new process on the same durable state resumes from the checkpoint.
    st.close();
    const st2 = attestedState(path);
    await job(s, st2, a).sweep();
    expect(s.count(dir("2026/10/30"))).toBe(0);
    expect(s.count(T.pointerKey)).toBe(0);
    expect(st2.eraseCheckpoint(eraseScope(["live-session", "supabase", G.org_id, G.workspace_id, G.user_id, G.origin_id, G.source, G.session_id]))).toBeNull();
    st2.close();
  });

  test("a lost DELETE acknowledgement is unresolved, not done; the next sweep resumes and finishes", async () => {
    const s = new FakeS3Versions();
    fill(s, 30);
    let lost = false;
    s.onDelete = () => (lost ? undefined : ((lost = true), "lose"));
    const st = await state();
    const j = job(s, st, authority());
    await j.sweep();
    expect(j.counters.liveFenceFailed).toBeGreaterThan(0);
    expect(s.count(T.pointerKey)).toBe(1); // never erased the pointer without a proven-empty directory
    await job(s, st, authority()).sweep();
    expect(s.count(dir("2026/10/30"))).toBe(0);
    expect(s.count(T.pointerKey)).toBe(0);
    st.close();
  });

  test("a re-enable during the traversal stops before any further delete (authority re-checked per page)", async () => {
    const s = new FakeS3Versions();
    s.pageSize = 100;
    fill(s, 1_000);
    const st = await state();
    const a = authority();
    let pages = 0;
    a.stillFenced = async () => (++pages <= 3 ? true : false);
    await job(s, st, a).sweep();
    const left = s.count(dir("2026/10/30"));
    expect(left).toBeGreaterThan(0);
    expect(left).toBeLessThan(1_001);
    expect(s.count(T.pointerKey)).toBe(1);
    st.close();
  });

  test("successor versions written after the re-read are kept; the session is still proven erased", async () => {
    const s = new FakeS3Versions();
    fill(s, 20);
    const fresh = `${dir("2026/11/06")}raw/b=new.ndjson.zst`;
    s.put(fresh, NOW); // newer than the re-check time minus skew
    const st = await state();
    await job(s, st, authority()).sweep();
    expect(s.count(dir("2026/10/30"))).toBe(0);
    expect(s.count(fresh)).toBe(1);
    st.close();
  });

  test("a checkpoint bound to another scope is refused and deletes nothing", async () => {
    const s = new FakeS3Versions();
    fill(s, 12_500);
    const st = await state();
    // Produce a real checkpoint under the "neon" authority's scope, then plant it under the supabase scope.
    await job(s, st, authority({ kind: "neon" as never })).sweep();
    const neonScope = eraseScope(["live-session", "neon", G.org_id, G.workspace_id, G.user_id, G.origin_id, G.source, G.session_id]);
    const supaScope = eraseScope(["live-session", "supabase", G.org_id, G.workspace_id, G.user_id, G.origin_id, G.source, G.session_id]);
    const cp = st.eraseCheckpoint(neonScope);
    expect(cp).not.toBeNull();
    st.setEraseCheckpoint(supaScope, cp);
    const before = s.count(dir("2026/10/30"));
    const j = job(s, st, authority());
    await j.sweep();
    expect(j.counters.liveFenceFailed).toBeGreaterThan(0);
    expect(s.count(dir("2026/10/30"))).toBe(before);
    st.close();
  });

  test("a queued session prefix over 10k versions stays queued across drains and is done only after a strict zero inventory", async () => {
    const s = new FakeS3Versions();
    const prefix = "96bulls/org-1/v1/ws=ws_1/m=delivery_x/h=claude/d=2026/10/30/s=sess_1/";
    for (let i = 0; i < 12_000; i++) s.put(`${prefix}raw/b=${String(i).padStart(6, "0")}.ndjson.zst`, OLD);
    const st = await state();
    st.enqueueErasure([{ key: prefix, kind: "prefix", org_id: "org-1", reason: "fenced" }], NOW - 1000);
    const j = job(s, st, authority({ fencedLivePage: async () => [] }));
    await j.drain();
    expect(s.count(prefix)).toBe(2_000);
    expect(st.nextErasures(NOW + 1, 10).map((e) => e.key)).toEqual([prefix]);
    await j.drain();
    expect(s.count(prefix)).toBe(0);
    expect(st.nextErasures(NOW + 1, 10)).toEqual([]);
    expect(j.counters.prefixesErased).toBe(1);
    st.close();
  });
});

describe("the live pointer and staged objects go through the same reserve gate", () => {
  const STAGED = `${liveStagedPrefix("96bulls", G)}d=2026/10/30/b=batch-1.staged.ndjson.zst`;
  test("a claimed (owned) pointer / staged version refuses its reservation and is not deleted; unclaimed ones are erased", async () => {
    for (const claimed of [true, false]) {
      const s = new FakeS3Versions();
      fill(s, 3);
      s.put(STAGED, OLD);
      const st = await state();
      if (claimed) {
        st.acquireLease("96bulls/", "exporter:claim");
        st.publishWrites([{ key: T.pointerKey, versionId: "*" }, { key: STAGED, versionId: "*" }], { holder: "exporter:claim", gens: { "96bulls/": st.leaseGen("96bulls/")! } });
      }
      await job(s, st, authority()).sweep();
      expect(s.count(dir("2026/10/30"))).toBe(0);
      expect([claimed, s.count(T.pointerKey), s.count(STAGED)]).toEqual([claimed, claimed ? 1 : 0, claimed ? 1 : 0]);
      expect(s.deletes.some((d) => d.startsWith(T.pointerKey) || d.startsWith(STAGED))).toBe(!claimed);
      st.close();
    }
  });
});

describe("absence proof includes delete markers", () => {
  test("a kept (newer) DELETE MARKER reserved by an earlier subset is NOT retired as erased by the session erase", async () => {
    const s = new FakeS3Versions();
    fill(s, 3);
    const K = `${dir("2026/10/30")}raw/b=000001.ndjson.zst`;
    // A delete marker newer than the job's re-read (a successor the time-subset keeps), still listed.
    s.keys.set(K, [{ id: "m-kept", at: NOW, marker: true }, ...(s.keys.get(K) ?? [])]);
    const st = await state();
    expect(st.reserveErase(T.base, st.obligationRev(T.base), "earlier-subset", [{ key: K, versionId: "m-kept" }]).ok).toBe(true);
    await job(s, st, authority()).sweep();
    expect(s.keys.get(K)?.map((v) => v.id)).toEqual(["m-kept"]); // the old object versions are gone, the marker kept
    expect(st.reservationsAt(K)).toEqual(["m-kept"]); // still reserved ...
    expect(st.erasedAt(K)).not.toContain("m-kept"); // ... and never in the erased history (the erased object version is)
    expect(st.erasedAt(K).length).toBe(1);
    st.close();
  });
});

// Keep the pointer-prefix helper referenced (the live layout the tests address).
void livePointerPrefix;
