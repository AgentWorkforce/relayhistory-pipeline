// Pure tests of the progress module with a simulated DB. Fake values only.
import { test, expect } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { FrozenCopyRefusal, FrozenCopyUnknownCommit, type Queryable } from "../frozen-source-copy-seal.ts";
import {
  COPY_OWNERSHIP_LOCK, PROGRESS_COLUMNS, PROGRESS_TABLE, PROGRESS_TABLE_DDL, abortController, bindingSha, classifyJobOutcome, commitPage, copyDataset,
  jobReceipt, operatorSha256, progressTracker, reconcileInDoubt, requireProgressTable, requireResumeBinding, serialQueue, takeCopyOwnership, type CopyBinding,
} from "../frozen-source-copy-progress.ts";
import { testEnvelope } from "./fixtures/frozen-copy-envelope.ts";

const B: CopyBinding = { generation: "a".repeat(64), sourceIdentitySha256: "c".repeat(64), controlEpoch: "3".repeat(64), privacyBaselineSha256: "b".repeat(64), operatorSha256: "0".repeat(64) };
const answer = (rows: unknown[]): Queryable => ({ query: async () => ({ rows }) as never });
const code = async (p: Promise<unknown> | (() => unknown)) => {
  try {
    await (typeof p === "function" ? p() : p);
    return "no-refusal";
  } catch (e) {
    return (e as { code?: string }).code ?? String(e);
  }
};
const shape = (over: Record<string, unknown> = {}) => ({ rls: true, owned: true, foreign_grants: 0, cols: [...PROGRESS_COLUMNS].reverse(), ...over });

test("requireProgressTable: absent -> missing; not owned / RLS off / any grant / column drift -> unsafe", async () => {
  expect(await code(requireProgressTable(answer([])))).toBe("frozen_copy_progress_table_missing");
  expect(await code(requireProgressTable(answer([shape()])))).toBe("no-refusal");
  for (const bad of [{ rls: false }, { owned: false }, { foreign_grants: 1 }, { cols: [...PROGRESS_COLUMNS, "note"] }, { cols: PROGRESS_COLUMNS.slice(1) }, { cols: null }])
    expect(await code(requireProgressTable(answer([shape(bad)])))).toBe("frozen_copy_progress_table_unsafe");
});

test("ownership: a dedicated try-lock key (not the authority keys 96/97); not granted -> frozen_copy_operator_busy", async () => {
  expect(COPY_OWNERSHIP_LOCK).toEqual([1919249529, 98]);
  const seen: unknown[] = [];
  const c = (ok: unknown): Queryable => ({ query: async (sql: string, p?: unknown[]) => (seen.push([sql, p]), { rows: [{ ok }] }) as never });
  expect(await code(takeCopyOwnership(c(true)))).toBe("no-refusal");
  expect(seen[0]).toEqual(["SELECT pg_try_advisory_lock($1,$2) AS ok", [1919249529, 98]]);
  for (const ok of [false, null, "t"]) expect(await code(takeCopyOwnership(c(ok)))).toBe("frozen_copy_operator_busy");
});

test("bindingSha and operatorSha256 change with every bound input", () => {
  const base = bindingSha(B);
  for (const k of Object.keys(B) as Array<keyof CopyBinding>) expect(bindingSha({ ...B, [k]: "f".repeat(64) })).not.toBe(base);
  const files = [{ name: "a.ts", bytes: "x" }, { name: "b.ts", bytes: "y" }];
  const op = operatorSha256(files);
  expect(op).toMatch(/^[0-9a-f]{64}$/);
  expect(operatorSha256([{ name: "a.ts", bytes: "x" }, { name: "b.ts", bytes: "y " }])).not.toBe(op);
  expect(operatorSha256([{ name: "a.ts", bytes: "xb.ts" }, { name: "", bytes: "y" }])).not.toBe(op); // length-framed
  expect(operatorSha256([...files].reverse())).not.toBe(op);
  expect(operatorSha256([{ name: "a.ts", bytes: new TextEncoder().encode("x") }, { name: "b.ts", bytes: "y" }])).toBe(op);
});

test("requireResumeBinding: no rows -> 0; exact binding -> count; operator-only drift vs any other drift", async () => {
  const row = (over: Record<string, unknown> = {}) => ({ binding_sha256: bindingSha(B), generation: B.generation, source_identity_sha256: B.sourceIdentitySha256, control_epoch: B.controlEpoch, privacy_baseline_sha256: B.privacyBaselineSha256, operator_sha256: B.operatorSha256, n: 3, ...over });
  expect(await requireResumeBinding(answer([]), B)).toBe(0);
  expect(await requireResumeBinding(answer([row()]), B)).toBe(3);
  expect(await code(requireResumeBinding(answer([row({ operator_sha256: "1".repeat(64) })]), B))).toBe("frozen_copy_resume_operator_mismatch");
  for (const over of [{ generation: "e".repeat(64) }, { control_epoch: "4".repeat(64) }, { privacy_baseline_sha256: "4".repeat(64) }, { source_identity_sha256: "4".repeat(64) }, { binding_sha256: "9".repeat(64) }])
    expect(await code(requireResumeBinding(answer([row(), row(over)]), B))).toBe("frozen_copy_resume_binding_mismatch");
});

/** A simulated target that records statements and lets a test fail one of them. */
function sim(fail: (sql: string) => unknown = () => undefined) {
  const log: string[] = [];
  const q: Queryable = {
    async query(sql: string) {
      log.push(sql.startsWith("INSERT INTO " + PROGRESS_TABLE) ? "CHECKPOINT" : sql.startsWith("SELECT cursor") ? "LOAD" : sql);
      const e = fail(sql);
      if (e) throw e;
      if (sql.startsWith("INSERT INTO " + PROGRESS_TABLE)) return { rows: [{ seq: "1", at: "t" }] } as never;
      return { rows: [] } as never;
    },
  };
  return { q, log };
}
const page = { binding: B, org: "org-x", dataset: "records" as const, from: null, cursor: ["w", "o", "r"], rows: 1, last: false };

test("commitPage: BEGIN, page, checkpoint, COMMIT in one transaction; every failure mode is classified", async () => {
  const ok = sim();
  expect(await commitPage(ok.q, page, () => ok.q.query("PAGE"))).toMatchObject({ pageSeq: 1, rows: 1, status: "in_progress" });
  expect(ok.log).toEqual(["BEGIN", "PAGE", "CHECKPOINT", "COMMIT"]);
  // No server answer before COMMIT: rolled back, page_not_committed.
  const lost = sim((s) => (s === "PAGE" ? new Error("socket hang up") : undefined));
  expect(await code(commitPage(lost.q, page, () => lost.q.query("PAGE")))).toBe("frozen_copy_page_not_committed");
  expect(lost.log).toEqual(["BEGIN", "PAGE", "ROLLBACK"]);
  // Server refusal before COMMIT: rolled back, SQLSTATE kept.
  const srv = sim((s) => (s.startsWith("INSERT INTO " + PROGRESS_TABLE) ? Object.assign(new Error("x"), { code: "23514" }) : undefined));
  expect(await code(commitPage(srv.q, page, () => srv.q.query("PAGE")))).toBe("23514");
  expect(srv.log.at(-1)).toBe("ROLLBACK");
  // COMMIT answered with a SQLSTATE: not committed, rolled back, rethrown.
  const cs = sim((s) => (s === "COMMIT" ? Object.assign(new Error("x"), { code: "40001" }) : undefined));
  expect(await code(commitPage(cs.q, page, () => cs.q.query("PAGE")))).toBe("40001");
  expect(cs.log.slice(-2)).toEqual(["COMMIT", "ROLLBACK"]);
  // COMMIT without an answer: in doubt, no ROLLBACK and no retry.
  const doubt = sim((s) => (s === "COMMIT" ? new Error("Connection terminated") : undefined));
  const e = await commitPage(doubt.q, page, () => doubt.q.query("PAGE"), () => 42).catch((x) => x);
  expect(e).toBeInstanceOf(FrozenCopyUnknownCommit);
  expect(e.context).toEqual({ phase: "copy", dataset: "records", checkpointSeq: 1, org: "org-x", detectedAt: 42 });
  expect(doubt.log).toEqual(["BEGIN", "PAGE", "CHECKPOINT", "COMMIT"]);
  // An empty final page still writes its checkpoint, without calling insert.
  const empty = sim();
  let called = false;
  await commitPage(empty.q, { ...page, rows: 0, last: true }, async () => (called = true));
  expect(called).toBe(false);
  expect(empty.log).toEqual(["BEGIN", "CHECKPOINT", "COMMIT"]);
  // Writing past a complete checkpoint is a conflict before any statement.
  const done = sim();
  expect(await code(commitPage(done.q, { ...page, from: { cursor: [], pageSeq: 2, rows: 1, status: "complete", updatedAt: null } }, async () => {}))).toBe("frozen_copy_progress_conflict");
  expect(done.log).toEqual([]);
});

test("copyDataset: abort before the next page leaves no transaction open and never advances the cursor", async () => {
  const s = sim();
  const abort = abortController(() => 5);
  const reads: string[][] = [];
  const p = progressTracker(1);
  const r = await code(copyDataset({
    target: s.q, binding: B, org: "org-x", dataset: "records", pageSize: 1,
    readPage: async (cursor) => (reads.push(cursor), abort.request("SIGINT"), [{ k: "1" }]),
    keyOf: () => ["w", "o", "1"], insert: async () => {}, check: abort.check, serialize: serialQueue(), progress: p,
  }));
  expect(r).toBe("frozen_copy_aborted");
  expect(reads).toEqual([["", "", ""]]);
  expect(s.log).toEqual(["LOAD"]); // aborted after the read: no BEGIN
  expect(p.state).toMatchObject({ pages: 0, inFlight: 0 });
});

test("abortController records the first reason only and refuses only once observed", () => {
  const a = abortController(() => 9);
  expect(() => a.check()).not.toThrow();
  a.request("SIGTERM");
  a.request("SIGINT");
  expect(a.state).toEqual({ requested: true, observed: false, reason: "SIGTERM", requestedAt: 9 });
  expect(() => a.check()).toThrow(FrozenCopyRefusal);
  expect(a.state.observed).toBe(true);
});

test("serialQueue runs one at a time in order, and a failure does not block the next", async () => {
  const q = serialQueue();
  const order: string[] = [];
  let running = 0;
  const job = (n: string, ms: number, fail = false) => q(async () => {
    expect(++running).toBe(1);
    await new Promise((r) => setTimeout(r, ms));
    order.push(n);
    running--;
    if (fail) throw new Error(n);
    return n;
  });
  const res = await Promise.allSettled([job("a", 20), job("b", 1, true), job("c", 1)]);
  expect(order).toEqual(["a", "b", "c"]);
  expect(res.map((r) => r.status)).toEqual(["fulfilled", "rejected", "fulfilled"]);
});

test("reconcileInDoubt: copy and seal readbacks; privacy insert and a failed readback stay PENDING", async () => {
  const copy = { phase: "copy" as const, dataset: "records" as const, checkpointSeq: 4, org: "org-x", detectedAt: 1 };
  expect(await reconcileInDoubt(answer([{ seq: "4" }]), copy)).toBe("COMMITTED");
  expect(await reconcileInDoubt(answer([{ seq: "3" }]), copy)).toBe("NOT_COMMITTED");
  expect(await reconcileInDoubt(answer([]), copy)).toBe("NOT_COMMITTED");
  expect(await reconcileInDoubt(answer([{ seq: "9" }]), { ...copy, checkpointSeq: null })).toBe("PENDING");
  const seal = { phase: "seal" as const, dataset: null, checkpointSeq: null, manifestSha256: "m".repeat(64), detectedAt: 1 };
  expect(await reconcileInDoubt(answer([{ manifest_sha256: "m".repeat(64) }]), seal)).toBe("COMMITTED");
  expect(await reconcileInDoubt(answer([]), seal)).toBe("NOT_COMMITTED");
  expect(await reconcileInDoubt(answer([{ manifest_sha256: "n".repeat(64) }]), seal)).toBe("REFUSED");
  expect(await reconcileInDoubt({ query: async () => { throw new Error("down"); } }, seal)).toBe("PENDING");
});

test("classifyJobOutcome: aborted, refused, unknown-commit, SQLSTATE and unclassified errors", () => {
  expect(classifyJobOutcome(undefined)).toEqual({ outcome: "pass", code: null });
  expect(classifyJobOutcome(new FrozenCopyRefusal("frozen_copy_aborted"))).toEqual({ outcome: "aborted", code: "frozen_copy_aborted" });
  expect(classifyJobOutcome(new FrozenCopyRefusal("frozen_copy_budget_exhausted"))).toEqual({ outcome: "refused", code: "frozen_copy_budget_exhausted" });
  expect(classifyJobOutcome(new FrozenCopyUnknownCommit({ phase: "copy", dataset: "records", checkpointSeq: 1, detectedAt: 1 })).outcome).toBe("unknown-commit");
  expect(classifyJobOutcome(Object.assign(new Error("x"), { code: "42501" }))).toEqual({ outcome: "refused", code: "frozen_copy_sql_42501" });
  expect(classifyJobOutcome(new Error("password=hunter2"))).toEqual({ outcome: "refused", code: "frozen_copy_job_failed" });
});

test("jobReceipt v2: statuses, budget/abort/unknown-commit blocks, and no org id, raw key or error text", () => {
  const p = progressTracker(2);
  p.bind(B);
  p.page("records", { cursor: ["ws-secret", "origin-secret", "rec-secret"], pageSeq: 1, rows: 3, status: "in_progress", updatedAt: "2026-01-01 00:00:00+00" }, 3);
  const env = testEnvelope({ progress: p.state });
  const copied = jobReceipt("copy", undefined, env);
  expect(copied).toMatchObject({ receipt: "frozen-source-copy-job-v2", status: "COPIED", outcome: "pass", code: null });
  expect(copied.progress).toMatchObject({ schemaVersion: 1, stateTable: PROGRESS_TABLE, bindingSha: bindingSha(B), pages: 1, rows: 3, totalOrgs: 2 });
  expect(copied.progress.cursorSha256).toMatch(/^[0-9a-f]{64}$/);
  expect(copied.unknownCommit).toMatchObject({ detected: false, reconciliation: "NOT_REQUIRED", noBlindRetry: true });
  expect(Object.keys(copied.budget).sort()).toEqual(["concurrency", "deadline", "elapsedWallMs", "exceeded", "lockTimeoutMs", "maxPageMs", "maxWallMs", "pageSizes", "statementTimeoutMs"]);
  expect(Object.keys(copied.abort).sort()).toEqual(["boundedReason", "completedAt", "inFlightResolved", "observed", "requested", "requestedAt", "sourceClosed", "targetClosed", "writesStopped"]);

  const aborted = jobReceipt("copy", new FrozenCopyRefusal("frozen_copy_aborted", { org: "org-secret" }), testEnvelope({ progress: { ...p.state, inFlight: 0 }, abort: { requested: true, observed: true, reason: "SIGTERM", requestedAt: 0, completedAt: 1, sourceClosed: true, targetClosed: true, writesStopped: true } }));
  expect(aborted).toMatchObject({ status: "ABORTED", outcome: "aborted", code: "frozen_copy_aborted" });
  expect(aborted.abort).toMatchObject({ requested: true, observed: true, boundedReason: "SIGTERM", inFlightResolved: true });
  expect(aborted.next).toContain("resumes");

  const budget = jobReceipt("copy", new FrozenCopyRefusal("frozen_copy_budget_exhausted"), env);
  expect(budget).toMatchObject({ status: "REFUSED" });
  expect(budget.budget.exceeded).toBe(true);

  const doubt = new FrozenCopyUnknownCommit({ phase: "copy", dataset: "receipts", checkpointSeq: 7, org: "org-secret", detectedAt: 0 });
  const unknown = jobReceipt("copy", doubt, testEnvelope({ progress: p.state, reconciliation: { result: "PENDING", readbackAt: null } }));
  expect(unknown.status).toBe("UNKNOWN_COMMIT");
  expect(unknown.unknownCommit).toMatchObject({ detected: true, phase: "copy", dataset: "receipts", checkpointSeq: 7, reconciliation: "PENDING", resolvedAt: null, noBlindRetry: true });
  const resolved = jobReceipt("copy", doubt, testEnvelope({ reconciliation: { result: "COMMITTED", readbackAt: 5 } }));
  expect(resolved.unknownCommit.resolvedAt).toBe(new Date(5).toISOString());

  // A seal "pass" without a committed manifest is never SEALED.
  expect(jobReceipt("seal", undefined, env, null)).toMatchObject({ status: "REFUSED", outcome: "refused", code: "frozen_copy_seal_missing" });
  const sealed = jobReceipt("seal", undefined, env, { manifestSha256: "m".repeat(64), inserted: true, orgs: 2, records: "3", deletions: "0", resets: "1", controlHeads: "0" });
  expect(sealed.status).toBe("SEALED");

  for (const r of [copied, aborted, budget, unknown, resolved, sealed]) {
    const text = JSON.stringify(r);
    for (const secret of ["org-secret", "ws-secret", "origin-secret", "rec-secret", "hunter2", "postgres"]) expect(text).not.toContain(secret);
  }
});

test("the custody doc carries the proposed DDL verbatim and the required statements", () => {
  const doc = readFileSync(join(import.meta.dir, "../../../docs/p1-frozen-source-custody.md"), "utf8");
  expect(doc).toContain("```sql\n" + PROGRESS_TABLE_DDL + "\n```");
  for (const s of ["No production run of this operator is authorized", "Rollback = no seal row", "not recomputed", "frozen_copy_mirror_advanced", "frozen_copy_progress_table_missing", "PR #558", "does not select or depend on it", "op read"])
    expect(doc).toContain(s);
});
