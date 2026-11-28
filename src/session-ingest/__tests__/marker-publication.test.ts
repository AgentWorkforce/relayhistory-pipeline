/**
 * marker-publication.ts (marker-dry / marker-execute under the 062320 hold) with an in-memory hold model
 * (marker-support.ts FakeHolds: the state machine and the org key's SHARED / EXCLUSIVE semantics) and scratch S3
 * stores: readiness, the approved wave, stale / absent / unsealed holds, a concurrent release (waits for the
 * publication; one committed before it refuses with nothing written), omitted attestations, a stale sweeper, partial
 * failure, retry (adopt, never overwrite), foreign markers, duplicate and oversized input, and redaction. Then the
 * backlog-ops wiring: dispatch, bindings, the gate's order, the credential step's bound receipts, and the workflow.
 */
import { afterEach, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { evidenceHash } from "../authority-evidence.ts";
import { orgFenceAuthorityDoc } from "../backlog-authority.ts";
import {
  BACKLOG_OPS_MARKER_EXECUTE_ENABLED, BACKLOG_OPS_RECEIPT_CONTRACT, BACKLOG_OPS_WORKFLOW, backlogOpsBindings, backlogOpsConfirmation, backlogOpsDispatch,
  backlogOpsHomeId, backlogOpsReceipt, backlogOpsRevisionsSince, backlogOpsScope, backlogOpsTarget, backlogOpsToolArgs, backlogOpsCloudflareToken, type BacklogOpsMode,
} from "../backlog-ops.ts";
import { epochHash } from "../erasure-window.ts";
import { orgFenceAuthorityKey, validOrgFenceAuthority } from "../ledger-s3.ts";
import {
  MARKER_ATTESTATION_FIELDS, MARKER_CLOCK_SKEW_MS, MARKER_IDENTITY_META, checkMarkerDryReceipt, markerAdoptable, markerIdentity, markerBoundFrom, markerOk, markerReceiptSummary, markerRun,
  parseLiveHold, type MarkerRunDeps, type MarkerSummary,
} from "../marker-publication.ts";
import { waveId } from "../org-wave.ts";
import { objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import { checkBoundReceipts } from "../../../scripts/backlog-ops.ts";
import { backlogOpsGithubGate } from "../../../scripts/backlog-ops-github.ts";
import { markerStdin, parseMarkerArgs, sessionPool } from "../../../scripts/backlog-marker.ts";
import { SCHEMA_RELEASE as P } from "../../hoopdb/schema-release-contract.ts";
import { PRIVATE_INGEST_RELEASE } from "../private-ingest-release.ts";
import { scratchStore } from "./support.ts";
import { CATCHUP_EVIDENCE, FakeHolds, NOW, ORGS, ROOT, RUNS, SHA, baseline, bound, evidenceReport, putJson, scopeIdOf, seedSweeper, spec } from "./marker-support.ts";

const cleanups: (() => void)[] = [];
afterEach(() => { for (const c of cleanups.splice(0)) c(); });
const fresh = () => { const s = scratchStore(); cleanups.push(s.done); return s.store; };
const acode = async (fn: () => Promise<unknown>) => { try { await fn(); return null; } catch (e) { return (e as { code?: string }).code ?? String(e); } };
const code = (fn: () => unknown) => { try { fn(); return null; } catch (e) { return (e as { code?: string }).code ?? String(e); } };
const SCOPE = scopeIdOf(ORGS);
const by = (s: MarkerSummary) => Object.fromEntries(s.per_org.map((p) => [p.org_hash, p]));
const H = (o: string) => evidenceHash("org", o);
const text = async (store: ObjectStore, key: string) => new TextDecoder().decode(await readAll((await store.getObject(objectKey(key))).body));
const marker = async (store: ObjectStore, org: string) => { try { return JSON.parse(await text(store, orgFenceAuthorityKey(ROOT, org))); } catch { return null; } };

async function world(o: { orgs?: readonly string[]; store?: ObjectStore; holds?: FakeHolds; seal?: readonly string[]; clock?: () => number } = {}) {
  const orgs = o.orgs ?? ORGS, store = o.store ?? fresh(), holds = o.holds ?? new FakeHolds();
  await seedSweeper(store, orgs);
  for (const org of o.seal ?? orgs) holds.ready(org);
  const deps = (mode: "dry_run" | "execute", over: Partial<MarkerRunDeps> = {}): MarkerRunDeps => ({
    mode, orgs, sha: SHA, scopeId: scopeIdOf(orgs), runs: RUNS, sources: {} as never, baseline: baseline(orgs), bound: bound({}, orgs),
    store, keyRoot: ROOT, bucket: "transcripts", ledgerBucket: null, withSession: (fn) => fn(holds.session()), now: o.clock ?? (() => NOW),
    evidence: (async () => evidenceReport(orgs)) as never, concurrency: 4, ...over });
  const dry = async (over: Partial<MarkerRunDeps> = {}) => markerRun(deps("dry_run", over));
  const wave = (s: MarkerSummary) => ({ id: s.wave_id!, orgs: s.per_org.filter((p) => p.verdict === "ready").map((p) => ({ orgHash: p.org_hash, epochHash: p.epoch_hash! })) });
  const execute = async (w: { id: string; orgs: { orgHash: string; epochHash: string }[] }, over: Partial<MarkerRunDeps> = {}) => markerRun(deps("execute", { wave: w, ...over }));
  return { store, holds, deps, dry, execute, wave, orgs };
}

// ---- the run --------------------------------------------------------------------------------------------------------

test("dry then execute: every org ready, the wave pinned to its epochs, every marker published under its hold", async () => {
  const w = await world();
  const d = await w.dry();
  expect(d.status).toBe("marker_wave_ready");
  expect(d.counts).toEqual({ ready: 3, refused: 0, published: 0, adopted: 0, incomplete: 0, not_in_wave: 0 });
  expect(d.per_org.every((p) => p.hold === "sealed" && p.marker === "absent" && !p.attempted && p.sweeper === "ok")).toBe(true);
  expect(d.per_org.every((p) => MARKER_ATTESTATION_FIELDS.every((k) => p.attest[k]))).toBe(true);
  expect(d.wave_id).toBe(waveId({ sha: SHA, scope: SCOPE, runs: RUNS, orgs: ORGS.map((o) => ({ orgHash: H(o), epochHash: epochHash(w.holds.live(o)!.epoch) })) }));
  expect(markerOk(d)).toBe(true);
  // The dry wrote nothing and attempted nothing.
  for (const o of ORGS) { expect(await marker(w.store, o)).toBeNull(); expect(w.holds.live(o)!.state).toBe("sealed"); }
  const e = await w.execute(w.wave(d));
  expect(e.status).toBe("marker_wave_published");
  expect(e.counts.published).toBe(3);
  expect(markerOk(e)).toBe(true);
  for (const o of ORGS) {
    const m = await marker(w.store, o);
    expect(validOrgFenceAuthority(m)).toBe(true);
    expect(m).toMatchObject({ orgId: o, revisionLayout: "4096", workerHttpAdmission: "off", rearmSweeper: "live", controlMirror: "on" });
    const h = w.holds.live(o)!;
    expect(h.state).toBe("marker_attempted");
    expect(h.events.find((x) => x.kind === "marker_attempted")!.evidence).toBe(CATCHUP_EVIDENCE);
  }
  // Order per org: the attempt committed, THEN the key was taken and the hold re-read, the PUT, the re-read, the unlock.
  for (const o of ORGS) {
    const l = w.holds.log.filter((x) => x.endsWith(o));
    expect(l.indexOf(`attempt ${o}`)).toBeLessThan(l.indexOf(`lock ${o}`));
    expect(l.slice(l.indexOf(`lock ${o}`))).toEqual([`lock ${o}`, `status ${o}`, `status ${o}`, `unlock ${o}`]);
  }
  // Both receipts conform exactly and name no org or epoch.
  expect(markerReceiptSummary(d, { mode: "dry_run", orgs: ORGS, waveId: null }).ok).toBe(true);
  expect(markerReceiptSummary(e, { mode: "execute", orgs: ORGS, waveId: d.wave_id }).ok).toBe(true);
  const all = JSON.stringify([d, e]);
  for (const o of ORGS) { expect(all).not.toContain(o); expect(all).not.toContain(w.holds.live(o)!.epoch); }
});

test("holds: absent, still held, released, or a new epoch since the dry each refuse that org alone; others publish", async () => {
  const [A, B, C] = ORGS as [string, string, string];
  const w = await world({ seal: [A, B, C] });
  const d = await w.dry();
  // After the dry: A's hold released and re-installed (new epoch, sealed); B released (no live hold); C fine.
  w.holds.all.find((h) => h.org === A)!.state = "released";
  w.holds.ready(A);
  w.holds.all.find((h) => h.org === B)!.state = "released";
  const e = await w.execute(w.wave(d));
  const p = by(e);
  expect(p[H(A)]).toMatchObject({ verdict: "refused", refused: "hold_epoch_changed", attempted: false });
  expect(p[H(B)]).toMatchObject({ verdict: "refused", refused: "hold_released", hold: "released", attempted: false });
  expect(p[H(C)]).toMatchObject({ verdict: "published", refused: null });
  expect(e.status).toBe("marker_wave_incomplete");
  expect(await marker(w.store, A)).toBeNull();
  expect(await marker(w.store, B)).toBeNull();
  // A dry over a held-but-unsealed org and an org with no hold at all.
  const w2 = await world({ seal: [C] });
  w2.holds.install(A);
  const d2 = await w2.dry();
  expect(by(d2)[H(A)]).toMatchObject({ verdict: "refused", refused: "hold_not_sealed", hold: "held" });
  expect(by(d2)[H(B)]).toMatchObject({ verdict: "refused", refused: "hold_absent", hold: "absent", epoch_hash: null });
  expect(d2.status).toBe("marker_wave_partial");
  // The wave covers the ready org only.
  expect(d2.wave_org_count).toBe(1);
  const e2 = await w2.execute(w2.wave(d2));
  expect(by(e2)[H(A)]).toMatchObject({ verdict: "not_in_wave", refused: "not_in_wave", hold: "not_read" });
  expect(by(e2)[H(C)]!.verdict).toBe("published");
  expect(e2.status).toBe("marker_wave_published");
  expect(await marker(w2.store, A)).toBeNull();
});

test("the approved wave: its id pins the runs and epochs; a wave outside the scope or with another id refuses before any read", async () => {
  const w = await world();
  const d = await w.dry();
  const wave = w.wave(d);
  expect(await acode(() => w.execute({ ...wave, id: "h_" + "0".repeat(32) }))).toBe("marker_wave_mismatch");
  expect(await acode(() => w.execute(wave, { runs: { ...RUNS, reshard: "699" } }))).toBe("marker_wave_mismatch");
  expect(await acode(() => w.execute({ id: wave.id, orgs: [{ orgHash: H("dddddddd-0000-4000-8000-000000000000"), epochHash: wave.orgs[0]!.epochHash }] }))).toBe("marker_wave_invalid");
  expect(await acode(() => w.execute({ id: wave.id, orgs: [] }))).toBe("marker_wave_invalid");
  expect(await acode(() => w.execute({ id: wave.id, orgs: [wave.orgs[0]!, wave.orgs[0]!] }))).toBe("marker_wave_invalid");
  expect(await acode(() => markerRun(w.deps("dry_run", { wave })))).toBe("marker_wave_invalid");
  expect(await acode(() => markerRun(w.deps("execute")))).toBe("marker_wave_invalid");
  // An approved epoch hash that is not the live one (the dry saw another epoch): that org refuses.
  const forged = { ...wave, orgs: wave.orgs.map((x, i) => i === 0 ? { ...x, epochHash: "h_" + "9".repeat(32) } : x) };
  const e = await w.execute({ ...forged, id: waveId({ sha: SHA, scope: SCOPE, runs: RUNS, orgs: forged.orgs }) });
  expect(e.per_org.find((p) => p.org_hash === forged.orgs[0]!.orgHash)).toMatchObject({ refused: "hold_epoch_changed", attempted: false });
  expect(e.counts.published).toBe(2);
  // No marker for nothing in the store and nothing in the log for any org outside the scope.
  expect(w.holds.log.some((l) => l.includes("dddddddd"))).toBe(false);
});

test("a concurrent release waits for the guarded publication; one committed before the lock refuses with nothing written", async () => {
  const [A, B] = ORGS as [string, string];
  const store = fresh();
  // A's PUT pauses until the test lets it go; meanwhile a release (EXCLUSIVE) is requested and must wait.
  let letGo!: () => void, putStarted!: () => void;
  const started = new Promise<void>((r) => { putStarted = r; }), gate = new Promise<void>((r) => { letGo = r; });
  const paused = new Proxy(store, { get: (t, p) => p === "put" ? async (key: { toString(): string }, ...rest: unknown[]) => {
    if (String(key).includes(`fence-authority/${A}`)) { putStarted(); await gate; }
    return (t as any).put(key, ...rest);
  } : (t as any)[p]?.bind?.(t) ?? (t as any)[p] }) as ObjectStore;
  const w = await world({ store: paused, orgs: [A] });
  const d = await w.dry();
  const run = w.execute(w.wave(d));
  await started;
  let released = false;
  const rel = w.holds.release(A).then(() => { released = true; });
  await new Promise((r) => setTimeout(r, 20));
  expect(released).toBe(false); // blocked on the SHARED key the publication holds
  letGo();
  const e = await run;
  await rel;
  expect(e.per_org[0]).toMatchObject({ verdict: "published", marker: "written" });
  const l = w.holds.log;
  expect(l.indexOf(`unlock ${A}`)).toBeLessThan(l.indexOf(`release ${A}`));

  // B: the release commits right after the attempt, before the guard: refused, attempted, nothing written.
  const w2 = await world({ orgs: [B] });
  const d2 = await w2.dry();
  w2.holds.afterAttempt = async (org) => { w2.holds.afterAttempt = null; await w2.holds.release(org); };
  const e2 = await w2.execute(w2.wave(d2));
  expect(e2.per_org[0]).toMatchObject({ verdict: "incomplete", refused: "hold_released_before_put", attempted: true, hold: "released" });
  expect(await marker(w2.store, B)).toBeNull();
  expect(e2.status).toBe("marker_wave_incomplete");
  expect(markerOk(e2)).toBe(false);
});

test("attestations: a section, a reshard verdict or the evidence missing refuses exactly the orgs it covers", async () => {
  const [A, B, C] = ORGS as [string, string, string];
  const w = await world();
  const s1 = await w.dry({ bound: bound({ sections: { fences: true, ownership: true, revisions: false } }) });
  expect(s1.status).toBe("marker_wave_refused");
  expect(s1.wave_id).toBeNull();
  expect(s1.per_org.every((p) => p.refused === "attestation_missing" && p.attest.revision_backfill === false)).toBe(true);
  expect(markerOk(s1)).toBe(false);
  const s2 = await w.dry({ bound: bound({ reshardVerified: [H(A), H(C)] }) });
  expect(by(s2)[H(B)]).toMatchObject({ refused: "attestation_missing" });
  expect(by(s2)[H(B)]!.attest.revision_layout).toBe(false);
  expect(s2.counts.ready).toBe(2);
  const s3 = await w.dry({ evidence: (async () => evidenceReport(ORGS, NOW - 1000, (o) => o === C ? { ok: false, refusals: ["mirror_off"], mirror: { ok: false, code: "mirror_off" } } : {})) as never });
  expect(by(s3)[H(C)]).toMatchObject({ refused: "evidence_not_corroborated" });
  expect(by(s3)[H(C)]!.attest.control_mirror).toBe(false);
  const s4 = await w.dry({ evidence: (async () => evidenceReport([A, B])) as never });
  expect(by(s4)[H(C)]).toMatchObject({ refused: "evidence_org_missing" });
  // A bound report gone stale before the point of no return: no attempt, nothing written.
  const d = await w.dry();
  const e = await w.execute(w.wave(d), { evidence: (async () => evidenceReport(ORGS, NOW - 6 * 60_000)) as never });
  expect(e.per_org.every((p) => p.refused === "marker_handoff_lapsed" && !p.attempted)).toBe(true);
  for (const o of ORGS) expect(w.holds.live(o)!.state).toBe("sealed");
});

test("the sweeper: a stale or missing pass refuses its org only; a stale spec refuses the wave", async () => {
  const [A, B] = ORGS as [string, string];
  const sweepWorld = async (passAt: Partial<Record<string, number>>, s = spec(ORGS)) => {
    const store = fresh(), holds = new FakeHolds();
    await seedSweeper(store, ORGS, passAt, s);
    for (const o of ORGS) holds.ready(o);
    return markerRun({ mode: "dry_run", orgs: ORGS, sha: SHA, scopeId: SCOPE, runs: RUNS, sources: {} as never, baseline: baseline(), bound: bound(),
      store, keyRoot: ROOT, bucket: "transcripts", ledgerBucket: null, withSession: (fn) => fn(holds.session()), now: () => NOW,
      evidence: (async () => evidenceReport()) as never });
  };
  const stale = await sweepWorld({ [A]: NOW - 7 * 3_600_000 });
  expect(by(stale)[H(A)]).toMatchObject({ sweeper: "sweeper_stale", refused: "sweeper_stale" });
  expect(by(stale)[H(A)]!.attest.rearm_sweeper).toBe(false);
  expect(stale.counts.ready).toBe(2);
  const missing = await sweepWorld({ [B]: 0 });
  expect(by(missing)[H(B)]).toMatchObject({ sweeper: "sweeper_missing", refused: "sweeper_missing" });
  expect(missing.counts.ready).toBe(2);
  const oldSpec = await sweepWorld({}, spec(ORGS, { observedAtMs: NOW - 2 * 3_600_000 }));
  expect(oldSpec.per_org.every((p) => p.sweeper === "sweeper_spec_stale" && p.refused === "sweeper_spec_stale")).toBe(true);
  expect(oldSpec.wave_id).toBeNull();
  const noSpec = await sweepWorld({}, null as never);
  expect(noSpec.per_org.every((p) => p.refused === "sweeper_spec_missing")).toBe(true);
});

test("partial failure then retry: a failed PUT leaves its org held (marker_attempted); the rerun publishes it and adopts the rest", async () => {
  const [A, B] = ORGS as [string, string];
  const store = fresh();
  let failA = true;
  const flaky = new Proxy(store, { get: (t, p) => p === "put" ? async (key: { toString(): string }, ...rest: unknown[]) => {
    if (failA && String(key).includes(`fence-authority`) && String(key).includes(A)) throw new Error(`s3 exploded for ${A}`);
    return (t as any).put(key, ...rest);
  } : (t as any)[p]?.bind?.(t) ?? (t as any)[p] }) as ObjectStore;
  const w = await world({ store: flaky });
  const d = await w.dry();
  const e1 = await w.execute(w.wave(d));
  expect(by(e1)[H(A)]).toMatchObject({ verdict: "incomplete", refused: "marker_write_failed", attempted: true });
  expect(by(e1)[H(B)]!.verdict).toBe("published");
  expect(e1.status).toBe("marker_wave_incomplete");
  expect(markerOk(e1)).toBe(false);
  expect(w.holds.live(A)!.state).toBe("marker_attempted"); // fail safe: still held
  expect(JSON.stringify(e1)).not.toContain(A);
  const firstB = await marker(w.store, B);
  failA = false;
  const e2 = await w.execute(w.wave(d), { now: () => NOW + 60_000, evidence: (async () => evidenceReport(ORGS, NOW + 59_000)) as never });
  expect(by(e2)[H(A)]!.verdict).toBe("published");
  expect(by(e2)[H(B)]).toMatchObject({ verdict: "adopted", marker: "adopted" });
  expect(e2.status).toBe("marker_wave_published");
  expect(await marker(w.store, B)).toEqual(firstB); // adopted, never rewritten
  // Each epoch recorded exactly one attempt event (idempotent at the same epoch).
  for (const o of ORGS) expect(w.holds.live(o)!.events.filter((x) => x.kind === "marker_attempted")).toHaveLength(1);
});

test("foreign markers: one before any attempt, one from an earlier epoch, or the wrong org's are never adopted or overwritten", async () => {
  const [A, B, C] = ORGS as [string, string, string];
  const w = await world();
  await putJson(w.store, orgFenceAuthorityKey(ROOT, A), orgFenceAuthorityDoc(A, NOW - 10_000));
  const d = await w.dry();
  expect(by(d)[H(A)]).toMatchObject({ refused: "marker_present", marker: "present" });
  // The execute (its wave from a dry before the foreign marker appeared) refuses A before its attempt.
  const wave = { id: "", orgs: ORGS.map((o) => ({ orgHash: H(o), epochHash: epochHash(w.holds.live(o)!.epoch) })) };
  wave.id = waveId({ sha: SHA, scope: SCOPE, runs: RUNS, orgs: wave.orgs });
  const e = await w.execute(wave);
  expect(by(e)[H(A)]).toMatchObject({ refused: "marker_conflict", attempted: false });
  expect(w.holds.live(A)!.state).toBe("sealed");
  expect((await marker(w.store, A)).writtenAtMs).toBe(NOW - 10_000);
  // markerAdoptable's rules (the identity is over org, epoch and the DURABLE attempt's evidence).
  const live = { epoch: w.holds.live(B)!.epoch, state: "marker_attempted" as const, attemptedMs: NOW - 1000 };
  const ctx = { org: B, hold: live, approvedEpoch: live.epoch, otherAttempts: 0, durableEvidence: CATCHUP_EVIDENCE, boundEvidence: CATCHUP_EVIDENCE, nowMs: NOW };
  const own = { doc: orgFenceAuthorityDoc(B, NOW), identity: markerIdentity(B, live.epoch, CATCHUP_EVIDENCE) };
  expect(markerAdoptable(own, ctx)).toBe(true);
  expect(markerAdoptable({ ...own, identity: null }, ctx)).toBe(false); // written outside the protocol
  expect(markerAdoptable({ ...own, identity: markerIdentity(B, live.epoch, "e".repeat(64)) }, ctx)).toBe(false); // another attempt's evidence
  expect(markerAdoptable(own, { ...ctx, durableEvidence: "e".repeat(64) })).toBe(false); // the durable attempt is another publisher's
  expect(markerAdoptable(own, { ...ctx, durableEvidence: null })).toBe(false);
  expect(markerAdoptable(own, { ...ctx, otherAttempts: 1 })).toBe(false); // another epoch attempted a marker
  expect(markerAdoptable({ ...own, doc: orgFenceAuthorityDoc(B, NOW - 1000 - MARKER_CLOCK_SKEW_MS - 1) }, ctx)).toBe(false); // older than the attempt
  expect(markerAdoptable({ ...own, doc: orgFenceAuthorityDoc(B, NOW + MARKER_CLOCK_SKEW_MS + 1) }, ctx)).toBe(false); // from the future
  expect(markerAdoptable({ ...own, doc: orgFenceAuthorityDoc(C, NOW) }, ctx)).toBe(false); // another org's
  expect(markerAdoptable({ ...own, doc: { ...own.doc, revisionLayout: "64" } }, ctx)).toBe(false);
  expect(markerAdoptable(own, { ...ctx, hold: { ...live, state: "sealed", attemptedMs: null } })).toBe(false);
  expect(markerAdoptable(own, { ...ctx, approvedEpoch: "00000000-0000-4000-8000-000000000000" })).toBe(false);
});

test("a PUT that times out is unknown (held, retried later); a guard lost after the PUT is proven by a fresh hold read", async () => {
  const [A, B] = ORGS as [string, string];
  const store = fresh();
  const hang = new Proxy(store, { get: (t, p) => p === "put" ? async (key: { toString(): string }, ...rest: unknown[]) => {
    if (String(key).includes(`fence-authority/${A}`)) return new Promise(() => {});
    return (t as any).put(key, ...rest);
  } : (t as any)[p]?.bind?.(t) ?? (t as any)[p] }) as ObjectStore;
  const w = await world({ store: hang, orgs: [A, B] });
  const d = await w.dry();
  w.holds.loseCommit.add(B);
  const e = await w.execute(w.wave(d), { s3TimeoutMs: 50 });
  expect(by(e)[H(A)]).toMatchObject({ verdict: "incomplete", refused: "marker_put_unknown", marker: "unknown", attempted: true });
  expect(by(e)[H(B)]).toMatchObject({ verdict: "published", marker: "written" });
  // B again, but the hold was released by the time of the fresh read: lapsed, needs the hold owner.
  const w2 = await world({ orgs: [B] });
  const d2 = await w2.dry();
  w2.holds.loseCommit.add(B);
  const realSession = w2.holds.session.bind(w2.holds);
  let n = 0;
  w2.holds.session = () => {
    const s = realSession();
    return { ...s, read: async (t, p) => { if (++n >= 2 && t.includes("hold_status")) { const h = w2.holds.live(B); if (h) h.state = "released"; } return s.read(t, p); } };
  };
  const e2 = await w2.execute(w2.wave(d2));
  expect(e2.per_org[0]).toMatchObject({ verdict: "incomplete", refused: "marker_hold_lapsed", marker: "written" });
});

test("input: duplicate, oversized, malformed orgs and a bad bound refuse before any read", async () => {
  const w = await world();
  const many = Array.from({ length: 65 }, (_, i) => `${(i + 1).toString(16).padStart(8, "0")}-0000-4000-8000-000000000000`);
  expect(await acode(() => w.dry({ orgs: [ORGS[0]!, ORGS[0]!] }))).toBe("wave_org_duplicate");
  expect(await acode(() => w.dry({ orgs: many }))).toBe("wave_orgs_oversized");
  expect(await acode(() => w.dry({ orgs: [] }))).toBe("wave_orgs_empty");
  expect(await acode(() => w.dry({ orgs: ["Org-A"] }))).toBe("wave_org_invalid");
  expect(await acode(() => w.dry({ bound: { ...bound(), catchupEvidence: "x" } }))).toBe("marker_bound_invalid");
  expect(await acode(() => w.dry({ bound: { ...bound(), extra: 1 } as never }))).toBe("marker_bound_invalid");
  expect(w.holds.log).toEqual([]);
  // 64 orgs, bounded: at most `concurrency` sessions at once.
  const big = many.slice(0, 64).sort();
  const holds = new FakeHolds();
  let open = 0, peak = 0;
  const store = fresh();
  await seedSweeper(store, big);
  for (const o of big) holds.ready(o);
  const s = await markerRun({ mode: "dry_run", orgs: big, sha: SHA, scopeId: scopeIdOf(big), runs: RUNS, sources: {} as never, baseline: baseline(big), bound: bound({}, big),
    store, keyRoot: ROOT, bucket: "transcripts", ledgerBucket: null, now: () => NOW, evidence: (async () => evidenceReport(big)) as never, concurrency: 6,
    withSession: async (fn) => { open++; peak = Math.max(peak, open); try { await new Promise((r) => setTimeout(r, 1)); return await fn(holds.session()); } finally { open--; } } });
  expect(s.counts.ready).toBe(64);
  expect(peak).toBeLessThanOrEqual(6);
  expect(markerReceiptSummary(s, { mode: "dry_run", orgs: big, waveId: null }).ok).toBe(true);
});

test("parseLiveHold: the hold only (a null erasure watermark is fine); contradictory timestamps refuse", () => {
  const h = { epoch: "0f0e0d0c-0b0a-4908-8706-050403020100", state: "sealed", installed_at: "2026-11-27T10:00:00Z", sealed_at: "2026-11-27T11:00:00Z", marker_attempted_at: null };
  expect(parseLiveHold([{ status: { hold: h, erasures: null } }]).hold!.state).toBe("sealed");
  expect(parseLiveHold([{ status: JSON.stringify({ hold: null, erasures: 0 }) }])).toEqual({ hold: null, released: false });
  expect(parseLiveHold([{ status: { hold: { ...h, state: "released" } } }])).toEqual({ hold: null, released: true });
  for (const bad of [{ ...h, sealed_at: null }, { ...h, state: "held" }, { ...h, state: "marker_attempted" }, { ...h, epoch: "x" }, { ...h, state: "open" }, { ...h, installed_at: "nope" }])
    expect(code(() => parseLiveHold([{ status: { hold: bad } }]))).toBe("hold_state_invalid");
  expect(code(() => parseLiveHold([]))).toBe("hold_state_invalid");
  expect(code(() => parseLiveHold([{ status: {} }]))).toBe("hold_state_invalid");
});

test("the receipt schema: exact, self-consistent for the scope and the wave; a refusal keeps only a closed code", async () => {
  const w = await world();
  const d = await w.dry();
  const ok = (s: unknown, mode: "dry_run" | "execute" = "dry_run", wave: string | null = null, orgs: readonly string[] = ORGS) => markerReceiptSummary(s, { mode, orgs, waveId: wave }).ok;
  expect(ok(d)).toBe(true);
  expect(ok(d, "dry_run", null, ORGS.slice(0, 2))).toBe(false); // another scope
  expect(ok({ ...d, per_org: [...d.per_org, d.per_org[0]] })).toBe(false); // a duplicate org
  expect(ok({ ...d, org_count: 2 })).toBe(false);
  expect(ok({ ...d, counts: { ...d.counts, ready: 2 } })).toBe(false);
  expect(ok({ ...d, extra: 1 })).toBe(false);
  expect(ok({ ...d, per_org: d.per_org.map((p, i) => i === 0 ? { ...p, org_hash: ORGS[0] } : p) })).toBe(false); // a raw org id
  expect(ok({ ...d, per_org: d.per_org.map((p, i) => i === 0 ? { ...p, refused: "made_up" } : p) })).toBe(false);
  expect(ok(d, "execute", d.wave_id)).toBe(false); // a dry is not an execute
  const e = await w.execute(w.wave(d));
  expect(ok(e, "execute", d.wave_id)).toBe(true);
  expect(ok(e, "execute", "h_" + "1".repeat(32))).toBe(false);
  expect(markerReceiptSummary({ refused: "marker_execute_held" }, { mode: "execute", orgs: ORGS, waveId: null }).summary).toEqual({ refused: "marker_execute_held" });
  expect(markerReceiptSummary({ refused: `pg error for ${ORGS[0]}` }, { mode: "execute", orgs: ORGS, waveId: null }).summary).toEqual({ refused: "internal_error" });
});

test("the tool: execute is held in code; usage, stdin and errors print fixed codes and never an org", () => {
  expect(BACKLOG_OPS_MARKER_EXECUTE_ENABLED).toBe(false);
  const root = fileURLToPath(new URL("../../../", import.meta.url));
  const args = ["--bucket", "transcripts", "--region", "us-east-1", "--sha", SHA, "--authority-run", "1", "--backfill-run", "2", "--full-run", "3", "--reshard-run", "4",
    ...ORGS.flatMap((o) => ["--org", o])];
  const run = (argv: string[], input = "") => spawnSync(process.execPath, [join(root, "scripts/backlog-marker.ts"), ...argv], { input, encoding: "utf8", env: { PATH: process.env.PATH ?? "" } });
  const held = run(["execute", ...args, "--kms-key-arn", "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-4000-8000-000000000000"]);
  expect(held.stdout.trim()).toBe(JSON.stringify({ refused: "marker_execute_held" }));
  expect(held.status).toBe(1);
  const dup = run(["dry", ...args, "--org", ORGS[0]!]);
  expect(dup.stdout.trim()).toBe(JSON.stringify({ refused: "wave_org_duplicate" }));
  const bad = run(["dry", ...args], JSON.stringify({ dbUrl: `postgresql://u:p@h/${ORGS[0]}`, cloudflareToken: "short" }));
  expect(bad.stdout.trim()).toBe(JSON.stringify({ refused: "stdin_invalid" }));
  for (const r of [held, dup, bad]) for (const o of ORGS) { expect(r.stdout).not.toContain(o); expect(r.stderr).not.toContain(o); }
  expect(code(() => parseMarkerArgs(["dry", ...args.slice(0, 6)]))).toBe("usage");
  expect(code(() => parseMarkerArgs(["dry", ...args, "--full-run", "1"]))).toBe("usage");
  expect(parseMarkerArgs(["dry", ...args]).concurrency).toBe(3);
  const stdin = { dbUrl: "postgresql://x", cloudflareToken: "c".repeat(40), baseline: {}, ledgerBucket: null, bound: bound() };
  expect(markerStdin(JSON.stringify(stdin), "dry").bound).toEqual(bound());
  expect(code(() => markerStdin(JSON.stringify(stdin), "execute"))).toBe("stdin_invalid");
  expect(code(() => markerStdin(JSON.stringify({ ...stdin, wave: { id: 1, orgs: [] } }), "execute"))).toBe("marker_wave_invalid");
  expect(code(() => markerStdin(JSON.stringify({ ...stdin, bound: { ...bound(), reshardVerified: [ORGS[0]] } }), "dry"))).toBe("marker_bound_invalid");
});

test("the session pool: at most `size` sessions, each used by one org at a time, all closed", async () => {
  let opened = 0, closed = 0, busy = 0, peak = 0;
  const pool = sessionPool(async () => { opened++; return { session: {} as never, close: async () => { closed++; } }; }, 3);
  await Promise.all(Array.from({ length: 10 }, () => pool.withSession(async () => { busy++; peak = Math.max(peak, busy); await new Promise((r) => setTimeout(r, 2)); busy--; })));
  await pool.close();
  expect([opened, closed, peak]).toEqual([3, 3, 3]);
});

// ---- backlog-ops wiring ---------------------------------------------------------------------------------------------

const target = {
  BACKLOG_OPS_BUCKET: "96bulls-aws-sessions-prod-transcripts-000000000000", BACKLOG_OPS_REGION: "us-east-1",
  BACKLOG_OPS_KMS_KEY_ARN: "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-4000-8000-000000000000",
  BACKLOG_OPS_DRY_ROLE_ARN: "arn:aws:iam::000000000000:role/dry", BACKLOG_OPS_EXECUTE_ROLE_ARN: "arn:aws:iam::000000000000:role/exec",
};
const SCOPE_ENV = { BACKLOG_OPS_ORGS: ORGS.join(",") };
const DRY = "605", WAVE = "h_" + "7".repeat(32);
const dispatchEnv = (mode: BacklogOpsMode, over: Record<string, string> = {}) => {
  const base: Record<string, string> = { BACKLOG_OPS_MODE: mode, BACKLOG_OPS_SHA: SHA, BACKLOG_OPS_AUTHORITY_RUN: RUNS.authority, BACKLOG_OPS_BACKFILL_RUN: RUNS.backfill,
    BACKLOG_OPS_FULL_BACKFILL_RUN: RUNS.full, BACKLOG_OPS_RESHARD_RUN: RUNS.reshard };
  if (mode === "marker-execute") {
    base.BACKLOG_OPS_DRY_RUN = DRY; base.BACKLOG_OPS_WAVE_ID = WAVE;
    base.BACKLOG_OPS_CONFIRMATION = `MARKER-EXECUTE ${SHA} DRY ${DRY} BACKFILL ${RUNS.backfill} FULL ${RUNS.full} RESHARD ${RUNS.reshard} AUTHORITY ${RUNS.authority} WAVE ${WAVE}`;
  }
  return { ...base, ...over };
};
/** A SYNTHETIC reviewed pair, tests only: the dispatch needs active lists for these modes (the shipped lists are empty). */
const TEST_LISTS = { marker: ["h_" + "1".repeat(32), "h_" + "2".repeat(32)], hold: ["h_" + "1".repeat(32), "h_" + "2".repeat(32)] };
const on = (env: Record<string, string>) => backlogOpsDispatch(env, true, false, false, true, false, TEST_LISTS);

test("dispatch: both marker modes bind AUTHORITY, BACKFILL, FULL and RESHARD; the execute is held, binds its DRY and names the WAVE", () => {
  expect(code(() => backlogOpsDispatch(dispatchEnv("marker-execute")))).toBe("backlog_ops_marker_held");
  expect(code(() => backlogOpsDispatch(dispatchEnv("marker-execute"), true, true, true, false))).toBe("backlog_ops_marker_held");
  const d = on(dispatchEnv("marker-execute"));
  expect(d.waveId).toBe(WAVE);
  expect(backlogOpsConfirmation(d)).toBe(dispatchEnv("marker-execute").BACKLOG_OPS_CONFIRMATION);
  expect(on(dispatchEnv("marker-dry")).dryRun).toBeNull();
  for (const k of ["BACKLOG_OPS_AUTHORITY_RUN", "BACKLOG_OPS_BACKFILL_RUN", "BACKLOG_OPS_FULL_BACKFILL_RUN", "BACKLOG_OPS_RESHARD_RUN"])
    for (const mode of ["marker-dry", "marker-execute"] as const) expect(code(() => on(dispatchEnv(mode, { [k]: "" })))).not.toBeNull();
  // WAVE: marker-execute only, h_ + 32 hex; and it must be in the confirmation.
  expect(code(() => on(dispatchEnv("marker-execute", { BACKLOG_OPS_WAVE_ID: "" })))).toBe("backlog_ops_wave_id");
  expect(code(() => on(dispatchEnv("marker-execute", { BACKLOG_OPS_WAVE_ID: ORGS[0]! })))).toBe("backlog_ops_wave_id");
  expect(code(() => on(dispatchEnv("marker-dry", { BACKLOG_OPS_WAVE_ID: WAVE })))).toBe("backlog_ops_wave_id");
  expect(code(() => on({ BACKLOG_OPS_MODE: "backfill-dry", BACKLOG_OPS_SHA: SHA, BACKLOG_OPS_WAVE_ID: WAVE }))).toBe("backlog_ops_wave_id");
  const env = dispatchEnv("marker-execute");
  expect(code(() => on({ ...env, BACKLOG_OPS_CONFIRMATION: env.BACKLOG_OPS_CONFIRMATION!.replace(` WAVE ${WAVE}`, "") }))).toBe("backlog_ops_confirmation");
  expect(code(() => on({ ...env, BACKLOG_OPS_WAVE_ID: "h_" + "8".repeat(32) }))).toBe("backlog_ops_confirmation");
  expect(code(() => on(dispatchEnv("marker-dry", { BACKLOG_OPS_CONFIRMATION: "x" })))).toBe("backlog_ops_confirmation");
  // Existing confirmations are unchanged (no WAVE token anywhere else).
  expect(backlogOpsConfirmation({ mode: "authority-execute", sha: SHA, dryRun: "1", backfillRun: "2", exportRun: null, fullBackfillRun: "3", reshardRun: "4" }))
    .toBe(`AUTHORITY-EXECUTE ${SHA} DRY 1 BACKFILL 2 FULL 3 RESHARD 4`);
  expect(backlogOpsBindings(d).map((b) => [b.kind, b.mode, b.run])).toEqual([["dry", "marker-dry", DRY], ["authority", "authority-dry", RUNS.authority],
    ["backfill", "backfill-catchup-execute", RUNS.backfill], ["full", "backfill-execute", RUNS.full], ["reshard", "reshard-dry", RUNS.reshard]]);
  expect(backlogOpsBindings(on(dispatchEnv("marker-dry"))).map((b) => b.kind)).toEqual(["authority", "backfill", "full", "reshard"]);
  // argv: fixed, scoped only; t is wanted (the catch-up's record is checked against it).
  const t = backlogOpsTarget(target), scope = backlogOpsScope(SCOPE_ENV);
  expect(backlogOpsToolArgs(d, t, scope)).toEqual({ script: "scripts/backlog-marker.ts", needsDb: true, args: ["execute", "--bucket", t.bucket, "--region", t.region,
    "--kms-key-arn", t.kmsKeyArn, "--sha", SHA, "--authority-run", RUNS.authority, "--backfill-run", RUNS.backfill, "--full-run", RUNS.full, "--reshard-run", RUNS.reshard,
    ...ORGS.flatMap((o) => ["--org", o])] });
  expect(code(() => backlogOpsToolArgs(d, t, backlogOpsScope({})))).toBe("backlog_ops_scope_required");
  expect(backlogOpsRevisionsSince({ BACKLOG_OPS_REVISIONS_SINCE_MS: "1700000000000" }, d)).toBe(1_700_000_000_000);
  expect(backlogOpsCloudflareToken({ BACKLOG_OPS_CLOUDFLARE_TOKEN: "c".repeat(40) }, d)).toBe("c".repeat(40));
  // The receipt records every run the wave covers, and the home.
  const home = backlogOpsHomeId({ bucket: t.bucket, region: t.region }, null);
  const r = backlogOpsReceipt(d, "9", 0, "{}", scope.id, "none", "none", home);
  expect(r).toMatchObject({ authority_run: RUNS.authority, after_run: RUNS.backfill, full_run: RUNS.full, reshard_run: RUNS.reshard, home });
});

const hoursAgo = (h: number) => new Date(Date.now() - h * 3600_000).toISOString();
function gateFixture(mode: BacklogOpsMode) {
  const actor = { id: P.dispatcher.id, login: P.dispatcher.login, type: "User" };
  const run = (id: string, m: BacklogOpsMode, h: number) => ({ id: Number(id), head_sha: SHA, head_branch: "main", event: "workflow_dispatch", path: BACKLOG_OPS_WORKFLOW,
    run_attempt: 1, status: "completed", conclusion: "success", actor: { ...actor }, triggering_actor: { ...actor }, display_title: `backlog ops ${m} ${SHA}`, created_at: hoursAgo(h) });
  return {
    main: SHA,
    run: { id: 100, head_sha: SHA, head_branch: "main", event: "workflow_dispatch", path: BACKLOG_OPS_WORKFLOW, run_attempt: 1, actor, triggering_actor: { ...actor },
      display_title: `backlog ops ${mode} ${SHA}` },
    environment: { id: 7, name: mode.endsWith("-execute") ? "session-backlog-execute" : "session-schema-production", protection_rules: [{ type: "branch_policy" }],
      deployment_branch_policy: { protected_branches: false, custom_branch_policies: true } },
    bound: { [DRY]: run(DRY, "marker-dry", 0.5), [RUNS.backfill]: run(RUNS.backfill, "backfill-catchup-execute", 2), [RUNS.full]: run(RUNS.full, "backfill-execute", 10),
      [RUNS.reshard]: run(RUNS.reshard, "reshard-dry", 1), [RUNS.authority]: run(RUNS.authority, "authority-dry", 5) } as Record<string, any>,
  };
}
type GateFx = ReturnType<typeof gateFixture>;
const gateGet = (f: GateFx) => async (p: string): Promise<any> => {
  if (p.endsWith("/git/ref/heads/main")) return { object: { sha: f.main } };
  if (p.endsWith("/actions/runs/100")) return f.run;
  if (p.endsWith("/actions/runs/100/approvals")) return [];
  for (const id of Object.keys(f.bound)) {
    if (p.endsWith(`/actions/runs/${id}`)) return f.bound[id];
    if (p.endsWith(`/actions/runs/${id}/artifacts?per_page=100`)) return { total_count: 1, artifacts: [{ name: "backlog-ops-receipt", expired: false }] };
  }
  if (p.endsWith("/environments/" + f.environment.name)) return f.environment;
  if (p.includes("deployment-branch-policies")) return { total_count: 1, branch_policies: [{ name: "main", type: "branch" }] };
  if (p.includes("/actions/workflows/")) return { workflow_runs: [{ id: 10, head_sha: SHA, head_branch: "main", event: "push", status: "completed", conclusion: "success", run_attempt: 1 }] };
  if (p.includes("/runs/10/")) return { total_count: 5, jobs: ["checks", "session-read-worker / worker", "real PostgreSQL 15", "real PostgreSQL 16", "real PostgreSQL 17"]
    .map((name) => ({ name, head_sha: SHA, status: "completed", conclusion: "success" })) };
  throw new Error("unexpected " + p);
};
const gateContext = { GITHUB_REPOSITORY: P.repository, GITHUB_REPOSITORY_ID: PRIVATE_INGEST_RELEASE.repositoryId, GITHUB_EVENT_NAME: "workflow_dispatch",
  GITHUB_REF: "refs/heads/main", GITHUB_SHA: SHA, GITHUB_RUN_ATTEMPT: "1", GITHUB_RUN_ID: "100" };
const gate = (mode: BacklogOpsMode, mutate: (f: GateFx) => void = () => {}, markerEnabled = true) => {
  const f = gateFixture(mode);
  mutate(f);
  return acode(() => backlogOpsGithubGate({ ...gateContext, ...dispatchEnv(mode) }, gateGet(f), false, { enabled: true, ...(markerEnabled ? { enabledOrgHashes: TEST_LISTS } : {}) }));
};

test("gate: the exact main head, then the marker order (authority and FULL before the catch-up, reshard after it, the dry last), the catch-up fresh", async () => {
  for (const mode of ["marker-dry", "marker-execute"] as const) {
    expect(await gate(mode)).toBeNull();
    expect(await gate(mode, (f) => { f.main = "2".repeat(40); })).toBe("backlog_ops_gate_main_moved");
    expect(await gate(mode, (f) => { f.bound[RUNS.reshard].created_at = hoursAgo(3); })).toBe("backlog_ops_gate_reshard_order");
    expect(await gate(mode, (f) => { f.bound[RUNS.full].created_at = hoursAgo(1.5); })).toBe("backlog_ops_gate_full_order");
    expect(await gate(mode, (f) => { f.bound[RUNS.authority].created_at = hoursAgo(1.5); })).toBe("backlog_ops_gate_order");
    expect(await gate(mode, (f) => { f.bound[RUNS.backfill].created_at = hoursAgo(3.5); f.bound[RUNS.full].created_at = hoursAgo(10); })).toBe("backlog_ops_gate_backfill_run");
    expect(await gate(mode, (f) => { f.bound[RUNS.backfill].display_title = `backlog ops backfill-execute ${SHA}`; })).toBe("backlog_ops_gate_backfill_run");
    expect(await gate(mode, (f) => { f.bound[RUNS.reshard].head_sha = "3".repeat(40); })).toBe("backlog_ops_gate_reshard_run");
  }
  expect(await gate("marker-execute", (f) => { f.bound[DRY].created_at = hoursAgo(1.5); })).toBe("backlog_ops_gate_order");
  expect(await gate("marker-execute", (f) => { f.bound[DRY].display_title = `backlog ops authority-dry ${SHA}`; })).toBe("backlog_ops_gate_dry_run");
  // Held: refused before any GitHub read.
  expect(await gate("marker-execute", () => {}, false)).toBe("backlog_ops_marker_held");
  expect(await acode(() => backlogOpsGithubGate({ ...gateContext, ...dispatchEnv("marker-execute") }, (async () => { throw new Error("no read"); }) as never, false, { enabled: true })))
    .toBe("backlog_ops_marker_held");
});

test("credential step: marker-dry and marker-execute check every bound receipt, the catch-up's exact bytes and the approved wave", () => {
  const tmp = mkdtempSync(join(tmpdir(), "marker-bound-"));
  cleanups.push(() => rmSync(tmp, { recursive: true, force: true }));
  const t = backlogOpsTarget(target);
  const home = backlogOpsHomeId({ bucket: t.bucket, region: t.region }, null);
  const SINCE = 1_700_000_000_000;
  const rcpt = (mode: BacklogOpsMode, run: string, summary: unknown, extra: Record<string, unknown> = {}) =>
    JSON.stringify({ contract: BACKLOG_OPS_RECEIPT_CONTRACT, mode, sha: SHA, run, scope: backlogOpsScope(SCOPE_ENV).id, ledger: "none", home, exitCode: 0, summary, ...extra });
  const sec = { fences: { failed: 0 }, ownership: { failed: 0 }, revisions: { failed: 0 } };
  const evidence = (() => { const r = evidenceReport(ORGS); return { ...r, status: "authority_evidence_unbound", orgs: r.orgs.map((o) => ({ ...o, ok: false, refusals: ["sql_unbound"] })) }; })();
  const { toReceiptShape } = require("../backlog-authority.ts");
  const files: Record<string, string> = {
    authority: rcpt("authority-dry", RUNS.authority, { mode: "dry_run", evidence: toReceiptShape(evidence) }),
    full: rcpt("backfill-execute", RUNS.full, { mode: "execute", ...sec, sections: { fences: "complete", ownership: "complete", revisions: "complete" } }),
    backfill: rcpt("backfill-catchup-execute", RUNS.backfill, { mode: "execute", ...sec, sections: { fences: "complete", ownership: "complete", revisions: "complete" },
      revisions_since_ms: SINCE, authority_baseline: toReceiptShape(baseline()) }, { authority_run: RUNS.authority, full_run: RUNS.full }),
    reshard: rcpt("reshard-dry", RUNS.reshard, { mode: "dry_run", tool: "reshard_verify", org_count: 3, verify_passed: false, per_org: ORGS.map((o, i) => ({ org_hash: H(o),
      verify_passed: i !== 1, listed: 1, legacy_shards: 0, entries: 1, verified: 1, missing: i === 1 ? 1 : 0, invalid: 0, failed: 0, merged: 0, would_merge: 0, shards: 0, refused: null })) },
      { after_run: RUNS.backfill }),
  };
  const write = (f: Record<string, string>) => { for (const [k, v] of Object.entries(f)) { mkdirSync(join(tmp, "backlog-ops-bound", k), { recursive: true }); writeFileSync(join(tmp, "backlog-ops-bound", k, "backlog-ops-receipt.json"), v); } };
  write(files);
  const env = { ...target, ...SCOPE_ENV, RUNNER_TEMP: tmp, BACKLOG_OPS_REVISIONS_SINCE_MS: String(SINCE) };
  const dd = on(dispatchEnv("marker-dry"));
  const b = checkBoundReceipts(env, dd, undefined, SINCE);
  expect(b.marker!.sections).toEqual({ fences: true, ownership: true, revisions: true });
  expect(b.marker!.reshardVerified.sort()).toEqual([H(ORGS[0]!), H(ORGS[2]!)].sort()); // the failed verify drops only its org
  expect(b.marker!.catchupEvidence).toBe(require("node:crypto").createHash("sha256").update(files.backfill!).digest("hex"));
  expect(b.baseline).toBeTruthy();
  expect(b.evidence).toBeUndefined();
  // A catch-up that recorded another t, another authority run, or a reshard bound to another catch-up: refused.
  write({ backfill: files.backfill!.replace(String(SINCE), String(SINCE + 1)) });
  expect(code(() => checkBoundReceipts(env, dd, undefined, SINCE))).toBe("backlog_ops_bound_backfill_since");
  write({ backfill: files.backfill!.replace(`"authority_run":"${RUNS.authority}"`, `"authority_run":"777"`) });
  expect(code(() => checkBoundReceipts(env, dd, undefined, SINCE))).toBe("backlog_ops_bound_baseline_run");
  write({ backfill: files.backfill!, reshard: files.reshard!.replace(`"after_run":"${RUNS.backfill}"`, `"after_run":"778"`) });
  expect(code(() => checkBoundReceipts(env, dd, undefined, SINCE))).toBe("backlog_ops_reshard_not_after");
  write({ reshard: files.reshard! });
  // Another scope: every binding is exact.
  expect(code(() => checkBoundReceipts({ ...env, BACKLOG_OPS_ORGS: ORGS.slice(0, 2).join(",") }, dd, undefined, SINCE))).toMatch(/^backlog_ops_bound_[a-z]+_scope$/u);
  // marker-execute: the marker-dry's wave must be the dispatch's WAVE and its runs the execute's.
  const dryWave = waveId({ sha: SHA, scope: backlogOpsScope(SCOPE_ENV).id, runs: RUNS, orgs: [{ orgHash: H(ORGS[0]!), epochHash: "h_" + "3".repeat(32) }] });
  const drySummary = { mode: "dry_run", tool: "marker_publication", status: "marker_wave_partial", wave_id: dryWave, org_count: 3, wave_org_count: 1,
    counts: { ready: 1, refused: 2, published: 0, adopted: 0, incomplete: 0, not_in_wave: 0 }, evidence: toReceiptShape(evidenceReport()),
    per_org: ORGS.map((o, i) => ({ org_hash: H(o), epoch_hash: i === 0 ? "h_" + "3".repeat(32) : null, hold: i === 0 ? "sealed" : "absent", sweeper: "ok",
      attest: Object.fromEntries(MARKER_ATTESTATION_FIELDS.map((k) => [k, true])), marker: i === 0 ? "absent" : "not_read", attempted: false,
      verdict: i === 0 ? "ready" : "refused", refused: i === 0 ? null : "hold_absent" })) };
  const dryRaw = rcpt("marker-dry", DRY, drySummary, { authority_run: RUNS.authority, after_run: RUNS.backfill, full_run: RUNS.full, reshard_run: RUNS.reshard });
  write({ dry: dryRaw });
  const de = on(dispatchEnv("marker-execute", { BACKLOG_OPS_WAVE_ID: dryWave,
    BACKLOG_OPS_CONFIRMATION: `MARKER-EXECUTE ${SHA} DRY ${DRY} BACKFILL ${RUNS.backfill} FULL ${RUNS.full} RESHARD ${RUNS.reshard} AUTHORITY ${RUNS.authority} WAVE ${dryWave}` }));
  const be = checkBoundReceipts(env, de, undefined, SINCE);
  expect(be.wave).toEqual({ id: dryWave, orgs: [{ orgHash: H(ORGS[0]!), epochHash: "h_" + "3".repeat(32) }] });
  const other = "h_" + "5".repeat(32);
  const de2 = on(dispatchEnv("marker-execute", { BACKLOG_OPS_WAVE_ID: other,
    BACKLOG_OPS_CONFIRMATION: `MARKER-EXECUTE ${SHA} DRY ${DRY} BACKFILL ${RUNS.backfill} FULL ${RUNS.full} RESHARD ${RUNS.reshard} AUTHORITY ${RUNS.authority} WAVE ${other}` }));
  expect(code(() => checkBoundReceipts(env, de2, undefined, SINCE))).toBe("backlog_ops_bound_marker_wave");
  write({ dry: dryRaw.replace(`"reshard_run":"${RUNS.reshard}"`, `"reshard_run":"780"`) });
  expect(code(() => checkBoundReceipts(env, de, undefined, SINCE))).toBe("backlog_ops_bound_marker_runs");
  expect(code(() => checkMarkerDryReceipt(JSON.stringify({ summary: { ...drySummary, counts: { ...drySummary.counts, ready: 3 } } }), ORGS, RUNS, dryWave))).toBe("backlog_ops_bound_marker_dry");
  // markerBoundFrom: an incomplete FULL section is carried as false (the tool then refuses attestation_missing).
  const mb = markerBoundFrom(files.full!.replace(`"revisions":"complete"`, `"revisions":"incomplete"`), files.backfill!, files.reshard!, ORGS, RUNS.backfill, CATCHUP_EVIDENCE);
  expect(mb.sections.revisions).toBe(false);
});

test("workflow: the marker inputs, downloads and secrets; no expression in a run line; no org literal; wave_id is gated", () => {
  const yml = readFileSync(fileURLToPath(new URL("../../../" + BACKLOG_OPS_WORKFLOW, import.meta.url)), "utf8");
  expect(yml).toContain("marker-dry, marker-execute, hold-dry, hold-install-execute, hold-seal-execute, hold-rollback-execute]");
  expect(yml).toContain("  BACKLOG_OPS_WAVE_ID: ${{ inputs.wave_id }}");
  expect(yml).toContain("if: inputs.mode == 'authority-execute' || startsWith(inputs.mode, 'marker-')");
  expect(yml).toContain("if: (startsWith(inputs.mode, 'backfill-catchup-') || startsWith(inputs.mode, 'marker-')) && inputs.authority_run != ''");
  expect(yml).toContain("BACKLOG_OPS_CLOUDFLARE_TOKEN: ${{ (startsWith(inputs.mode, 'authority-') || startsWith(inputs.mode, 'marker-')) && secrets.BACKLOG_OPS_CLOUDFLARE_TOKEN || '' }}");
  // Actions expansion: no `${{ }}` inside any run script (a value expanded into a script is echoed into the log and
  // is an injection), and the org scope only ever comes from the secret, in a step env.
  const runLines = yml.split("\n").filter((l) => /^\s+run: /u.test(l) || /^\s{10,}bun /u.test(l));
  expect(runLines.length).toBeGreaterThan(5);
  for (const l of runLines) expect(l).not.toContain("${{");
  const blockRuns = [...yml.matchAll(/run: \|\n((?:\s{10}.*\n)+)/gu)].map((m) => m[1]!);
  for (const b of blockRuns) expect(b).not.toContain("${{");
  expect(yml).not.toMatch(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/u);
  expect(yml.match(/BACKLOG_OPS_ORGS: \$\{\{ (.*) \}\}/u)![1]).toBe("secrets.BACKLOG_OPS_ORGS");
  expect(yml).not.toMatch(/inputs\.(?!per_org\b)[a-z_]*org/u); // per_org is a flag, never an org
  // Every input with its dispatch check: wave_id is refused on every mode but marker-execute.
  for (const mode of ["backfill-dry", "drain-execute", "authority-execute", "marker-dry"] as BacklogOpsMode[])
    expect(code(() => backlogOpsDispatch({ BACKLOG_OPS_MODE: mode, BACKLOG_OPS_SHA: SHA, BACKLOG_OPS_WAVE_ID: WAVE }, true, true, true, true))).not.toBeNull();
});

test("the credential step masks every scope org before any tool runs; the marker tool's receipt line names none", () => {
  const src = readFileSync(fileURLToPath(new URL("../../../scripts/backlog-ops.ts", import.meta.url)), "utf8");
  expect(src).toContain("for (const org of scope.orgs ?? []) console.log(`::add-mask::${org}`);");
  // The marker tool writes only its one JSON line to stdout and nothing to stderr.
  const tool = readFileSync(fileURLToPath(new URL("../../../scripts/backlog-marker.ts", import.meta.url)), "utf8");
  expect(tool).not.toMatch(/console\.(log|error|warn)|process\.stderr/u);
});

test("child spawn: a marker mode's stdin carries the baseline, the proven bound and (execute) the wave; mismatched bindings refuse", () => {
  const { backlogOpsChildSpawn } = require("../../../scripts/backlog-ops.ts");
  const aws = { accessKeyId: "ASIA" + "A".repeat(16), secretAccessKey: "s", sessionToken: "t" };
  const env = { ...target, ...SCOPE_ENV, BACKLOG_OPS_CLOUDFLARE_TOKEN: "c".repeat(40), BACKLOG_OPS_REVISIONS_SINCE_MS: "1700000000000" };
  const wave = { id: WAVE, orgs: [{ orgHash: H(ORGS[0]!), epochHash: "h_" + "3".repeat(32) }] };
  const dry = backlogOpsChildSpawn(env, on(dispatchEnv("marker-dry")), aws, "postgresql://x", null, { baseline: baseline(), marker: bound() });
  expect(JSON.parse(dry.input)).toEqual({ dbUrl: "postgresql://x", cloudflareToken: "c".repeat(40), baseline: JSON.parse(JSON.stringify(baseline())), ledgerBucket: null, bound: bound() });
  expect(dry.args.slice(1, 2)).toEqual(["dry"]);
  const ex = backlogOpsChildSpawn(env, on(dispatchEnv("marker-execute")), aws, "postgresql://x", null, { baseline: baseline(), marker: bound(), wave });
  expect(JSON.parse(ex.input).wave).toEqual(wave);
  for (const o of ORGS) { expect(ex.input).not.toContain(o); expect(Object.values(ex.env).join(" ")).not.toContain(o); }
  expect(code(() => backlogOpsChildSpawn(env, on(dispatchEnv("marker-dry")), aws, "postgresql://x", null, { baseline: baseline() }))).toBe("backlog_ops_authority_binding");
  expect(code(() => backlogOpsChildSpawn(env, on(dispatchEnv("marker-dry")), aws, "postgresql://x", null, { baseline: baseline(), marker: bound(), wave }))).toBe("backlog_ops_authority_binding");
  expect(code(() => backlogOpsChildSpawn(env, on(dispatchEnv("marker-execute")), aws, "postgresql://x", null, { baseline: baseline(), marker: bound() }))).toBe("backlog_ops_authority_binding");
});

test("reshard-verify for a wave: the same per-org verdicts and shape as reshardVerifyOrgs, bounded, independent; the CLI takes orgs on stdin only", async () => {
  const { reshardVerifyWave } = await import("../reshard-wave.ts");
  const { reshardVerifyOrgs } = await import("../backlog-authority.ts");
  const { authorityReceiptSummary } = await import("../backlog-authority-schema.ts");
  const { createHash } = await import("node:crypto");
  const store = fresh();
  const [A, B] = ORGS as [string, string];
  const dir = `${ROOT}/.admission-inbox/${B}/v1/ws=w/m=srv_${"0".repeat(32)}`;
  const originHash = createHash("sha256").update("origin-1").digest("hex").slice(0, 32);
  await putJson(store, `${dir}/ledger/revisions/o=${originHash}/r=0a.json`, { v: 1, originId: "origin-1", shard: "0a", records: { "rec-1": [3, "rv", null, null, 0, 0] } });
  const wave = await reshardVerifyWave(store, ROOT, ORGS, 2);
  expect(wave).toEqual(await reshardVerifyOrgs(store, ROOT, ORGS));
  expect(by(wave as never)[H(A)]!.verify_passed).toBe(true);
  expect(by(wave as never)[H(B)]!.verify_passed).toBe(false);
  expect(authorityReceiptSummary("reshard", wave).ok).toBe(true);
  expect(await acode(() => reshardVerifyWave(store, ROOT, [A, A]))).toBe("wave_org_duplicate");
  const cli = await import("../../../scripts/backlog-reshard-verify.ts");
  expect(cli.parseReshardVerifyStdin(JSON.stringify({ orgs: [B, A] }))).toEqual([A, B].sort());
  for (const [raw, c] of [[JSON.stringify({ orgs: [A, A] }), "wave_org_duplicate"], [JSON.stringify({ orgs: [] }), "wave_orgs_empty"], ["{", "stdin_invalid"],
    [JSON.stringify({ orgs: [A], extra: 1 }), "stdin_invalid"], [`${JSON.stringify({ orgs: [A] })}\n${JSON.stringify({ orgs: [B] })}`, "stdin_invalid"]] as const)
    expect(code(() => cli.parseReshardVerifyStdin(raw))).toBe(c);
  expect(code(() => cli.parseReshardVerifyArgs(["--bucket", "transcripts", "--region", "us-east-1", "--org", A]))).toBe("usage"); // never argv
  const r = await cli.main(["--bucket", "transcripts", "--region", "us-east-1"], {}, () => JSON.stringify({ orgs: [A] }));
  expect(r).toEqual({ summary: { refused: "aws_credentials_required" }, ok: false });
  const r2 = await cli.main(["--bucket", "transcripts", "--region", "us-east-1"], {}, () => JSON.stringify({ orgs: [A, A] }));
  expect(JSON.stringify(r2)).not.toContain(A);
});

test("the credential step end to end (fake OIDC/STS and tool child): the marker-dry receipt is exact, logged and persisted without an org", async () => {
  const { backlogOpsRun } = await import("../../../scripts/backlog-ops.ts");
  const { toReceiptShape } = await import("../backlog-authority.ts");
  const { createHash } = await import("node:crypto");
  const temp = mkdtempSync(join(tmpdir(), "marker-step-"));
  cleanups.push(() => rmSync(temp, { recursive: true, force: true }));
  const t = backlogOpsTarget(target), home = backlogOpsHomeId({ bucket: t.bucket, region: t.region }, null), SINCE = 1_700_000_000_000;
  const scopeId = backlogOpsScope(SCOPE_ENV).id;
  const rcpt = (mode: BacklogOpsMode, run: string, summary: unknown, extra: Record<string, unknown> = {}) =>
    JSON.stringify({ contract: BACKLOG_OPS_RECEIPT_CONTRACT, mode, sha: SHA, run, scope: scopeId, ledger: "none", home, exitCode: 0, summary, ...extra });
  const sec = { fences: { failed: 0 }, ownership: { failed: 0 }, revisions: { failed: 0 }, sections: { fences: "complete", ownership: "complete", revisions: "complete" } };
  const unbound = (() => { const r = evidenceReport(ORGS); return { ...r, status: "authority_evidence_unbound", orgs: r.orgs.map((o) => ({ ...o, ok: false, refusals: ["sql_unbound"] })) }; })();
  const files: Record<string, string> = {
    authority: rcpt("authority-dry", RUNS.authority, { mode: "dry_run", evidence: toReceiptShape(unbound) }),
    full: rcpt("backfill-execute", RUNS.full, { mode: "execute", ...sec }),
    backfill: rcpt("backfill-catchup-execute", RUNS.backfill, { mode: "execute", ...sec, revisions_since_ms: SINCE, authority_baseline: toReceiptShape(baseline()) },
      { authority_run: RUNS.authority, full_run: RUNS.full }),
    reshard: rcpt("reshard-dry", RUNS.reshard, { mode: "dry_run", tool: "reshard_verify", org_count: 3, verify_passed: true, per_org: ORGS.map((o) => ({ org_hash: H(o),
      verify_passed: true, listed: 1, legacy_shards: 0, entries: 1, verified: 1, missing: 0, invalid: 0, failed: 0, merged: 0, would_merge: 0, shards: 0, refused: null })) },
      { after_run: RUNS.backfill }),
  };
  for (const [k, v] of Object.entries(files)) { mkdirSync(join(temp, "backlog-ops-bound", k), { recursive: true }); writeFileSync(join(temp, "backlog-ops-bound", k, "backlog-ops-receipt.json"), v); }
  const w = await world();
  const summary = await w.dry();
  const xml = `<AssumeRoleWithWebIdentityResponse><Credentials><AccessKeyId>ASIAAAAAAAAAAAAAAAAA</AccessKeyId><SecretAccessKey>s</SecretAccessKey>
    <SessionToken>t</SessionToken><Expiration>${new Date(Date.now() + 6 * 3600_000).toISOString()}</Expiration></Credentials></AssumeRoleWithWebIdentityResponse>`;
  const fetcher = (async (url: string) => url.startsWith("https://token.example/") ? new Response(JSON.stringify({ value: "a.b.c" })) : new Response(xml)) as unknown as typeof fetch;
  const ownerUrl = `postgresql://${P.owner}.${P.project}:p%40ss@${P.host}:${P.port}/${P.database}?sslmode=verify-full`;
  const step = async (stdout: string, out: string, enabledOrgHashes?: { marker: string[]; hold: string[] }) => {
    const lines: string[] = [];
    const log = console.log;
    console.log = (...a: unknown[]) => { lines.push(a.join(" ")); };
    let child: { input: string | null; args: string[] } | null = null;
    let exit: number;
    try {
      exit = await backlogOpsRun({ ...target, ...SCOPE_ENV, ...dispatchEnv("marker-dry"), GITHUB_SHA: SHA, GITHUB_RUN_ID: "900", RUNNER_TEMP: join(temp, out),
        BACKLOG_OPS_REVISIONS_SINCE_MS: String(SINCE), BACKLOG_OPS_CLOUDFLARE_TOKEN: "c".repeat(40), SUPABASE_MIGRATION_OWNER_URL: ownerUrl,
        ACTIONS_ID_TOKEN_REQUEST_URL: "https://token.example/x", ACTIONS_ID_TOKEN_REQUEST_TOKEN: "bearer" },
        { fetcher, runChild: async (c) => { child = c as never; return { code: 0, stdout }; }, ...(enabledOrgHashes ? { enabledOrgHashes } : {}) });
    } finally { console.log = log; }
    return { exit, lines, child: child!, receipt: JSON.parse(lines.find((l) => l.startsWith("backlog_ops_receipt "))!.slice("backlog_ops_receipt ".length)) };
  };
  // Every bound receipt lives under RUNNER_TEMP: point each run's temp at a copy.
  const { cpSync } = await import("node:fs");
  for (const out of ["a", "b", "c"]) cpSync(join(temp, "backlog-ops-bound"), join(temp, out, "backlog-ops-bound"), { recursive: true });
  const ok = await step(JSON.stringify(summary), "a");
  // The wave-1 enable record on marker-dry: the shipped lists are empty, so `disabled`.
  expect(ok.receipt.enable_scope).toBe("disabled");
  expect(ok.exit).toBe(0);
  expect(ok.receipt.summary).toEqual(JSON.parse(JSON.stringify(summary)));
  expect(ok.receipt).toMatchObject({ mode: "marker-dry", scope: scopeId, home, authority_run: RUNS.authority, after_run: RUNS.backfill, full_run: RUNS.full, reshard_run: RUNS.reshard });
  const input = JSON.parse(ok.child.input!);
  expect({ ...input.bound, reshardVerified: [...input.bound.reshardVerified].sort() }).toEqual({ sections: { fences: true, ownership: true, revisions: true },
    reshardVerified: ORGS.map(H).sort(), catchupEvidence: createHash("sha256").update(files.backfill!).digest("hex") });
  // Each scope org is masked before the tool runs; no other line names one.
  for (const o of ORGS) {
    expect(ok.lines).toContain(`::add-mask::${o}`);
    expect(ok.lines.filter((l) => !l.startsWith("::add-mask::")).join("\n")).not.toContain(o);
  }
  // With a SYNTHETIC populated pair (tests only), the 3-org scope is not exactly the pair: `mismatch`; nothing names an org.
  const pairOfTwo = [H(ORGS[0]!), H(ORGS[1]!)];
  const mm = await step(JSON.stringify(summary), "c", { marker: pairOfTwo, hold: pairOfTwo });
  expect(mm.receipt.enable_scope).toBe("mismatch");
  expect(JSON.stringify(mm.receipt)).not.toContain(ORGS[0]!);
  // A tool line smuggling an org id (in a code-shaped position) is refused and never logged or persisted.
  const leak = JSON.parse(JSON.stringify(summary));
  leak.per_org[0].org_hash = ORGS[0];
  const bad = await step(JSON.stringify(leak), "b");
  expect(bad.exit).toBe(1);
  // countsOnly refuses the non-code string first; the exact schema would refuse it too.
  expect(["backlog_ops_receipt_string", "backlog_ops_receipt_schema"]).toContain(bad.receipt.summary.refused);
  expect(bad.lines.filter((l) => !l.startsWith("::add-mask::")).join("\n")).not.toContain(ORGS[0]);
  expect(readFileSync(join(temp, "b", "backlog-ops-out", "backlog-ops-receipt.json"), "utf8")).not.toContain(ORGS[0]);
});

test("publication identity: a same-org, same-window marker from a different attempt is never adopted, on the first GET or the If-None-Match path; an own retry is", async () => {
  const [A, B, C] = ORGS as [string, string, string];
  const store = fresh();
  // A's first PUT fails, so A is left marker_attempted with this run's evidence and no marker.
  let failA = true, raceC: (() => Promise<void>) | null = null;
  const s3 = new Proxy(store, { get: (t, p) => p === "put" ? async (key: { toString(): string }, ...rest: unknown[]) => {
    if (failA && String(key).includes(`fence-authority/${A}`)) throw new Error("s3 down");
    if (raceC && String(key).includes(`fence-authority/${C}`)) { const r = raceC; raceC = null; await r(); }
    return (t as any).put(key, ...rest);
  } : (t as any)[p]?.bind?.(t) ?? (t as any)[p] }) as ObjectStore;
  const w = await world({ store: s3 });
  const d = await w.dry();
  const e1 = await w.execute(w.wave(d));
  expect(by(e1)[H(A)]).toMatchObject({ verdict: "incomplete", refused: "marker_write_failed", attempted: true });
  // Published markers carry the identity over (org, epoch, durable evidence).
  const meta = async (o: string) => (await store.getObject(objectKey(orgFenceAuthorityKey(ROOT, o)))).meta.metadata[MARKER_IDENTITY_META];
  expect(await meta(B)).toBe(markerIdentity(B, w.holds.live(B)!.epoch, CATCHUP_EVIDENCE));
  // Another publisher now writes A's marker in the window (same org, same time), valid doc, a different attempt's identity.
  const epochA = w.holds.live(A)!.epoch;
  await store.put(objectKey(orgFenceAuthorityKey(ROOT, A)), new TextEncoder().encode(JSON.stringify(orgFenceAuthorityDoc(A, NOW))),
    { contentType: "application/json", metadata: { [MARKER_IDENTITY_META]: markerIdentity(A, epochA, "f".repeat(64)) } });
  failA = false;
  const e2 = await w.execute(w.wave(d));
  expect(by(e2)[H(A)]).toMatchObject({ verdict: "refused", refused: "marker_conflict", attempted: false });
  expect(await meta(A)).toBe(markerIdentity(A, epochA, "f".repeat(64))); // never overwritten
  expect(by(e2)[H(B)]).toMatchObject({ verdict: "adopted", marker: "adopted" }); // own retry
  // The If-None-Match path: C in a fresh store; a foreign object lands between our GET and our PUT.
  const store3 = fresh();
  const s33 = new Proxy(store3, { get: (t, p) => p === "put" ? async (key: { toString(): string }, ...rest: unknown[]) => {
    if (raceC && String(key).includes(`fence-authority/${C}`)) { const r = raceC; raceC = null; await r(); }
    return (t as any).put(key, ...rest);
  } : (t as any)[p]?.bind?.(t) ?? (t as any)[p] }) as ObjectStore;
  const w3 = await world({ store: s33, orgs: [C] });
  const d3 = await w3.dry();
  raceC = async () => {
    await store3.put(objectKey(orgFenceAuthorityKey(ROOT, C)), new TextEncoder().encode(JSON.stringify(orgFenceAuthorityDoc(C, NOW))), { contentType: "application/json" });
  };
  const e3 = await w3.execute(w3.wave(d3));
  expect(e3.per_org[0]).toMatchObject({ verdict: "incomplete", refused: "marker_conflict", marker: "conflict", attempted: true });
  expect((await store3.getObject(objectKey(orgFenceAuthorityKey(ROOT, C)))).meta.metadata[MARKER_IDENTITY_META]).toBeUndefined();
});

test("publication identity: an epoch whose durable attempt recorded another publisher's evidence refuses before any PUT", async () => {
  const [A] = ORGS as [string];
  const w = await world({ orgs: [A] });
  const d = await w.dry();
  // Another publisher attempted first at the same epoch with different bound evidence.
  const h = w.holds.live(A)!;
  h.state = "marker_attempted"; h.attemptedMs = NOW - 500; h.events.push({ kind: "marker_attempted", evidence: "e".repeat(64) });
  const e = await w.execute(w.wave(d));
  expect(e.per_org[0]).toMatchObject({ refused: "hold_attempt_evidence_mismatch", attempted: true, marker: "absent" });
  expect(await marker(w.store, A)).toBeNull();
});

test("per-org binding refusals (a wave FULL or catch-up): those orgs are refused before any read; the evidence runs over the rest", async () => {
  const [A, B, C] = ORGS as [string, string, string];
  const w = await world();
  let evidenceOrgs: readonly string[] = [];
  const s = await w.dry({ bound: bound({ refusedOrgs: [H(B)] }), evidence: (async (orgs: readonly string[]) => { evidenceOrgs = orgs; return evidenceReport(orgs); }) as never });
  expect(by(s)[H(B)]).toMatchObject({ verdict: "refused", refused: "binding_refused", hold: "not_read", attempted: false });
  expect([by(s)[H(A)]!.verdict, by(s)[H(C)]!.verdict]).toEqual(["ready", "ready"]);
  expect([...evidenceOrgs].sort()).toEqual([A, C].sort());
  expect(w.holds.log.some((l) => l.endsWith(B))).toBe(false);
  expect(markerReceiptSummary(s, { mode: "dry_run", orgs: ORGS, waveId: null }).ok).toBe(true);
  expect(await acode(() => w.dry({ bound: bound({ refusedOrgs: ORGS.map(H) }) }))).toBe("marker_bound_invalid");
  expect(await acode(() => w.dry({ bound: bound({ refusedOrgs: [H("dddddddd-0000-4000-8000-000000000000")] }) }))).toBe("marker_bound_invalid");
});
