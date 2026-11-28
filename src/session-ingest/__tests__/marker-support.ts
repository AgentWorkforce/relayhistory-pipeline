/**
 * Fixtures for the marker publication tests (marker-publication.test.ts, marker-publication.pglite.test.ts): a
 * corroborated evidence report and its baseline, the sweeper's serving spec and fresh pass receipts, and FakeHolds, an
 * in-memory model of slot 062320's hold (its state machine, events, and the org key: SHARED for the guarded
 * publication, EXCLUSIVE for a release, which waits for every SHARED holder).
 */
import { evidenceHash, type AuthorityBaseline, type AuthorityEvidenceReport } from "../authority-evidence.ts";
import { rearmServingSpecKey, type RearmServingSpec } from "../backlog-authority.ts";
import { HOLD_FUNCTIONS } from "../hold-bridge.ts";
import type { MarkerBound, MarkerSqlSession } from "../marker-publication.ts";
import { rearmBucketHash, writeRearmPassReceipt, type RearmPassReceipt } from "../rearm-sweeper-receipt.ts";
import { rearmScopeHash, rearmScopes } from "../rearm-sweeper.ts";
import { objectKey, type ObjectStore } from "../../shared/vendor.ts";
import { createHash } from "node:crypto";

export const ROOT = "96bulls";
export const SHA = "1".repeat(40);
export const NOW = Date.parse("2026-11-27T12:00:00Z");
export const ORGS = ["11111111-2222-4333-8444-555555555555", "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee", "cccccccc-dddd-4eee-8fff-000000000000"].sort();
export const RUNS = { authority: "601", backfill: "602", full: "603", reshard: "604" } as const;
export const DIGEST = `sha256:${"b".repeat(64)}`;
export const CATCHUP_EVIDENCE = "c".repeat(64);
export const scopeIdOf = (orgs: readonly string[]) => "orgs_" + createHash("sha256").update([...orgs].sort().join(",")).digest("hex").slice(0, 32);

export function evidenceReport(orgs: readonly string[] = ORGS, finishedAtMs = NOW - 1000, over: (o: string) => Record<string, unknown> = () => ({})): AuthorityEvidenceReport {
  return {
    status: "authority_evidence_corroborated", orgCount: orgs.length, observationWindowMs: 120_000, startedAtMs: finishedAtMs - 200_000, finishedAtMs,
    ingestDeploymentHash: evidenceHash("deployment", "d"), relayhistoryDeploymentHash: evidenceHash("deployment", "r"),
    orgs: orgs.map((o) => ({
      orgHash: evidenceHash("org", o), ok: true, refusals: [],
      stop: { ok: true, code: null, servingVersions: 1, deploymentsWalked: 3, stopDeployments: 2, effectiveStopMs: NOW - 7_200_000, stopDeploymentHash: evidenceHash("deployment", "s") },
      mirror: { ok: true, code: null },
      queueDrained: { ok: true, code: null, backlogReads: [0, 0], dlqBacklogReads: [3, 3], dlqConsumers: 0, observedWindowMs: 120_000 },
      sqlQuiet: { ok: true, code: null, reads: 2,
        counts: { batchesClaimedSince: 0, batchesCommittedSince: 0, batchesPending: 0, revisionsUpdatedSince: 0, revisionsCount: 120, ownershipCount: 40 },
        fingerprint: { revisionsCount: 120, revisionsDigest: "h" + "ab".repeat(16), ownershipCount: 40, ownershipDigest: "h" + "cd".repeat(16) },
        revisionsStable: true, ownershipStable: true, bound: true },
      turnsNeon: { ok: true, code: null, servingVersions: 1, path: "neon" },
      ...over(o),
    })),
  } as AuthorityEvidenceReport;
}
export function baseline(orgs: readonly string[] = ORGS): AuthorityBaseline {
  return { kind: "authority_baseline", v: 1, status: "authority_baseline_ready", orgCount: orgs.length, orgs: orgs.map((o) => ({
    orgHash: evidenceHash("org", o), ok: true, refusals: [], stopDeploymentHash: evidenceHash("deployment", "s"),
    effectiveStopMs: NOW - 7_200_000, readAtMs: NOW - 3_600_000, snapshot: { xmin: 1000, xmax: 1005, xip: [1001] },
    fingerprint: { revisionsCount: 120, revisionsDigest: "h" + "ab".repeat(16), ownershipCount: 40, ownershipDigest: "h" + "cd".repeat(16) },
    stats: { batchesDeleted: 7, statsResetMs: 0, startedMs: NOW - 86_400_000 } })) } as AuthorityBaseline;
}
export const bound = (over: Partial<MarkerBound> = {}, orgs: readonly string[] = ORGS): MarkerBound => ({
  sections: { fences: true, ownership: true, revisions: true }, reshardVerified: orgs.map((o) => evidenceHash("org", o)), catchupEvidence: CATCHUP_EVIDENCE, ...over });

export const spec = (orgs: readonly string[] = ORGS, over: Partial<RearmServingSpec> = {}): RearmServingSpec => ({ v: 1, kind: "rearm-sweeper-serving-spec",
  observedAtMs: NOW - 60_000, namespace: "ingest", cronJobName: "rearm-sweeper", cronJobUid: "0f0e0d0c-0b0a-4908-8706-050403020100", suspended: false,
  orgs: [...orgs], bucket: "transcripts", ledgerBucket: null, imageDigests: [DIGEST], ...over });
export function passReceipt(org: string, finishedAtMs: number, orgs: readonly string[] = ORGS): RearmPassReceipt {
  const scopes = rearmScopes(ROOT, [...orgs]);
  return { v: 1, kind: "rearm-sweeper-pass", org, enabled: true, dryRun: false, complete: true, fromStart: true, failures: 0, startedAtMs: finishedAtMs - 1000, finishedAtMs,
    scopeHash: rearmScopeHash(scopes), scopes: scopes.length, keyRoot: ROOT, bucketHash: rearmBucketHash("transcripts"), revision: { imageDigest: DIGEST },
    workload: { namespace: "ingest", cronJobName: "rearm-sweeper", jobName: "rearm-sweeper-29000000", jobUid: "0f0e0d0c-0b0a-4908-8706-050403020100" },
    counts: { listed: 0, erased: 0, superseded: 0, alreadyDone: 0, pending: 0, resetRunning: 0, reenabled: 0, fenceAbsent: 0, fenceUnreadable: 0, corrupt: 0, ownerMissing: 0,
      machineMismatch: 0, machineUnbound: 0, layoutCollision: 0, layoutScanTooLarge: 0, machineDerived: 0, emptyUnbound: 0, deferred: 0, gone: 0, failed: 0, wouldErase: 0,
      wouldSupersede: 0, versions: 0 } } as RearmPassReceipt;
}
export const putJson = (store: ObjectStore, key: string, v: unknown) =>
  store.put(objectKey(key), new TextEncoder().encode(typeof v === "string" ? v : JSON.stringify(v)), { contentType: "application/json" });
/** The serving spec and a fresh pass for every org (`passAt` overrides one org's pass time; 0 omits it). */
export async function seedSweeper(store: ObjectStore, orgs: readonly string[] = ORGS, passAt: Partial<Record<string, number>> = {}, s: RearmServingSpec | null = spec(orgs)) {
  if (s) await putJson(store, rearmServingSpecKey(ROOT), s);
  for (const org of orgs) {
    const at = passAt[org] ?? NOW - 3_600_000;
    if (at > 0) await writeRearmPassReceipt(store, passReceipt(org, at, orgs));
  }
}

// ---- the 062320 hold, in memory ----------------------------------------------------------------------------------

type State = "held" | "sealed" | "marker_attempted" | "released";
interface FakeHold { epoch: string; org: string; state: State; installedMs: number; sealedMs: number | null; attemptedMs: number | null; events: { kind: string; evidence: string | null }[] }
const sqlError = (code: string) => Object.assign(new Error("session ingest: org admission hold"), { code });
let seq = 0;
const uuid = () => { seq++; const h = createHash("sha256").update(`epoch-${seq}`).digest("hex"); return `${h.slice(0, 8)}-${h.slice(8, 12)}-4${h.slice(13, 16)}-8${h.slice(17, 20)}-${h.slice(20, 32)}`; };

export class FakeHolds {
  readonly all: FakeHold[] = [];
  private shared = new Map<string, number>();
  private drained = new Map<string, (() => void)[]>();
  /** Every statement the sessions ran, by kind, in order (for ordering assertions). */
  readonly log: string[] = [];
  constructor(private clock: () => number = () => NOW) {}
  live(org: string) { return this.all.find((h) => h.org === org && h.state !== "released") ?? null; }
  latest(org: string) { return this.live(org) ?? [...this.all].reverse().find((h) => h.org === org) ?? null; }
  install(org: string) {
    if (this.live(org)) throw sqlError("55000");
    const h: FakeHold = { epoch: uuid(), org, state: "held", installedMs: this.clock() - 7_000_000, sealedMs: null, attemptedMs: null, events: [{ kind: "installed", evidence: null }] };
    this.all.push(h);
    return h.epoch;
  }
  seal(org: string, epoch = this.live(org)!.epoch) {
    const h = this.live(org);
    if (!h || h.epoch !== epoch) throw sqlError("55000");
    if (h.state === "sealed") return;
    if (h.state !== "held") throw sqlError("55000");
    h.state = "sealed"; h.sealedMs = this.clock() - 600_000; h.events.push({ kind: "sealed", evidence: "b".repeat(64) });
  }
  /** installed and sealed: the state a marker needs. */
  ready(org: string) { const e = this.install(org); this.seal(org, e); return e; }
  /** EXCLUSIVE on the org key: waits for every SHARED holder (the guarded publication) to finish. */
  async release(org: string, reason: "bridge_complete" | "verified_rollback" = "bridge_complete") {
    while ((this.shared.get(org) ?? 0) > 0) await new Promise<void>((r) => { const l = this.drained.get(org) ?? []; l.push(r); this.drained.set(org, l); });
    const h = this.live(org);
    if (!h) throw sqlError("55000");
    const attempted = h.events.some((e) => e.kind === "marker_attempted");
    if (reason === "bridge_complete" ? h.state !== "marker_attempted" : attempted) throw sqlError("55000");
    h.state = "released"; h.events.push({ kind: "released", evidence: "d".repeat(64) });
    this.log.push(`release ${org}`);
  }
  private status(org: string) {
    const h = this.latest(org);
    const iso = (ms: number | null) => (ms === null ? null : new Date(ms).toISOString());
    return [{ status: { hold: h === null ? null : { epoch: h.epoch, held_org: h.org, state: h.state, installed_at: iso(h.installedMs), sealed_at: iso(h.sealedMs),
      marker_attempted_at: iso(h.attemptedMs), released_at: null, release_reason: null }, erasures: null } }];
  }
  private run(text: string, params: readonly unknown[]): Record<string, unknown>[] {
    const org = params[0] as string;
    if (text.includes(HOLD_FUNCTIONS.status)) { this.log.push(`status ${org}`); return this.status(org); }
    if (text.includes(HOLD_FUNCTIONS.markerAttempt)) {
      this.log.push(`attempt ${org}`);
      const h = this.live(org);
      if (!h || h.epoch !== params[1]) throw sqlError("55000");
      if (h.state === "marker_attempted") return [{}];
      if (h.state !== "sealed") throw sqlError("55000");
      h.state = "marker_attempted"; h.attemptedMs = this.clock(); h.events.push({ kind: "marker_attempted", evidence: params[2] as string });
      return [{}];
    }
    if (text.includes("attempt_evidence")) {
      const h = this.all.find((x) => x.epoch === params[0]);
      const ev = h?.events.find((e) => e.kind === "marker_attempted");
      return ev ? [{ attempt_evidence: ev.evidence }] : [];
    }
    if (text.includes("ingest_org_admission_hold_events")) {
      return [{ n: this.all.filter((h) => h.org === org && h.epoch !== params[1] && h.events.some((e) => e.kind === "marker_attempted")).length }];
    }
    throw new Error("unexpected statement");
  }
  /** Hooks: called after the marker attempt commits / inside the guard before its first read. */
  afterAttempt: ((org: string) => Promise<void>) | null = null;
  /** A guard that fails at commit (the transaction was lost), after its body ran. */
  loseCommit = new Set<string>();
  session(): MarkerSqlSession {
    return {
      read: async (text, params) => this.run(text, params),
      write: async (text, params) => {
        const rows = this.run(text, params);
        if (this.afterAttempt) await this.afterAttempt(params[0] as string);
        return rows;
      },
      guarded: async (org, body) => {
        this.shared.set(org, (this.shared.get(org) ?? 0) + 1);
        this.log.push(`lock ${org}`);
        try {
          const out = await body(async (text, params) => this.run(text, params));
          if (this.loseCommit.has(org)) { this.loseCommit.delete(org); throw Object.assign(new Error("connection lost"), { code: undefined }); }
          return out;
        } finally {
          this.shared.set(org, (this.shared.get(org) ?? 1) - 1);
          this.log.push(`unlock ${org}`);
          if ((this.shared.get(org) ?? 0) === 0) for (const r of this.drained.get(org)?.splice(0) ?? []) r();
        }
      },
    };
  }
}
