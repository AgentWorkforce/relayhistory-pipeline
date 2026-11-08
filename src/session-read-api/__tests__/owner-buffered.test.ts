/**
 * Owner-only buffered read (Option C, PR B): the control-head gate (contract r2-control-head/v1,
 * v4 consumer rules), admission visibility (cases from the untrusted fixtures v3, read as JSON
 * and transcribed here as synthetic values), the gated session read (two observations, recheck,
 * erasure race, 404 retry, tenant checks, revision/tombstone folding), the SQL reader shapes,
 * the export surface (no ungated path), and the read-time scrub wrapper.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import * as ownerBuffered from "../owner-buffered.ts";
import {
  BUFFERED_SESSIONS_SQL,
  BUFFERED_STAGES_SQL,
  BUFFERED_USER_SQL,
  BufferedFenced,
  CONTROL_HEAD_CONTRACT,
  CONTROL_HEAD_PATH,
  admissionVisible,
  ownerBufferedEnabled,
  parseControlHeadResponse,
  readOwnerBufferedList,
  readOwnerBufferedTurns,
  scrubBufferedText,
  serviceBindingControlHead,
  sqlOwnerBufferedReads,
  type AuthorityHead,
} from "../owner-buffered.ts";
import { TenantMismatchError } from "../page.ts";
import { objectKey } from "../../shared/vendor.ts";
import { DIGEST, E1, E2, SCOPE, SESSION, authority, bufferedRow, deps, fencedGate, headBody, putStage, scratch, scriptedGate, scriptedReads, turn } from "./owner-buffered-support.ts";

const cleanup: Array<() => void> = [];
afterEach(() => {
  for (const done of cleanup.splice(0)) done();
});
function store() {
  const s = scratch();
  cleanup.push(s.done);
  return s.store;
}
const reason = async (p: Promise<unknown>) => {
  try {
    await p;
  } catch (error) {
    return error instanceof BufferedFenced ? error.reason : `other:${(error as Error).name}`;
  }
  return "resolved";
};

describe("flag", () => {
  test("only '1' or 'true' enables", () => {
    for (const v of ["1", "true", " true "]) expect(ownerBufferedEnabled(v)).toBe(true);
    for (const v of [undefined, "", "0", "false", "TRUE", "yes", "on"]) expect(ownerBufferedEnabled(v)).toBe(false);
  });
});

describe("control-head gate (HISTORY_CONTROL_HEAD binding)", () => {
  const binding = (respond: (url: string, init?: RequestInit) => Promise<Response> | Response) => {
    const calls: Array<{ url: string; init?: RequestInit }> = [];
    return { calls, fetch: async (url: string, init?: RequestInit) => (calls.push({ url, init }), respond(url, init)) };
  };
  const ok = (b: unknown) => new Response(JSON.stringify(b), { status: 200, headers: { "content-type": "application/json" } });

  test("POSTs only the verified tenant to the pinned path and returns the head; never caches", async () => {
    const b = binding(() => ok(headBody()));
    const gate = serviceBindingControlHead(b);
    expect(await gate.observe(SCOPE)).toEqual(authority("7"));
    await gate.observe(SCOPE);
    expect(b.calls).toHaveLength(2);
    expect(new URL(b.calls[0]!.url).pathname).toBe(CONTROL_HEAD_PATH);
    expect(b.calls[0]!.init?.method).toBe("POST");
    expect(JSON.parse(String(b.calls[0]!.init?.body))).toEqual({ contract: CONTROL_HEAD_CONTRACT, org: "org-a", workspace: "ws-a", user: "u-1" });
    expect(Object.keys((b.calls[0]!.init?.headers ?? {}) as object).map((k) => k.toLowerCase()).sort()).toEqual(["accept", "content-type"]);
  });

  test("head null with an active cutover is a valid observation", async () => {
    expect(await serviceBindingControlHead(binding(() => ok(headBody({}, null)))).observe(SCOPE)).toEqual(authority(null));
  });

  // fixtures v3: authority-gate-* (every one: finalBufferedContentEmitted false).
  test("authority-gate-unbound-service: no binding is fenced", async () => {
    expect(await reason(serviceBindingControlHead(undefined).observe(SCOPE))).toBe("gate_unbound");
    expect(await reason(serviceBindingControlHead({} as never).observe(SCOPE))).toBe("gate_unbound");
  });
  test("authority-gate-non2xx: any status but 200 is fenced (including 2xx other than 200)", async () => {
    for (const status of [204, 201, 404, 500, 503]) {
      const r = status === 204 ? new Response(null, { status }) : new Response(JSON.stringify(headBody()), { status });
      expect(await reason(serviceBindingControlHead(binding(() => r)).observe(SCOPE))).toBe("gate_status");
    }
  });
  test("authority-gate-timeout: a binding past the deadline is fenced and aborted", async () => {
    let aborted = false;
    const b = binding((_u, init) => new Promise<Response>((_, reject) => init?.signal?.addEventListener("abort", () => ((aborted = true), reject(new Error("aborted"))))));
    expect(await reason(serviceBindingControlHead(b, { deadlineMs: 20 }).observe(SCOPE))).toBe("gate_timeout");
    expect(aborted).toBe(true);
  });
  test("a binding that throws is fenced", async () => {
    expect(await reason(serviceBindingControlHead(binding(() => Promise.reject(new Error("boom")))).observe(SCOPE))).toBe("gate_error");
  });
  test("authority-gate-invalid-response: non-JSON, oversized, missing/extra keys, bad types", async () => {
    const cases: Array<[unknown, string]> = [
      ["not json", "gate_shape"],
      [JSON.stringify({ ...headBody(), pad: "x".repeat(20_000) }), "gate_shape"],
      [headBody({ extra: 1 }), "gate_shape"],
      [{ ...headBody(), head: undefined }, "gate_shape"],
      [[headBody()], "gate_shape"],
      [headBody({ contract: "r2-control-head/v2" }), "gate_contract"],
      [headBody({ cutover: { epoch: E1, mode: "ingest", status: "draining" } }), "gate_cutover"],
      [headBody({ cutover: { epoch: E1, mode: "legacy", status: "active" } }), "gate_cutover"],
      [headBody({ cutover: { epoch: "1".repeat(63), mode: "ingest", status: "active" } }), "gate_shape"],
      [headBody({ cutover: { epoch: E1.toUpperCase().replace(/1/gu, "A"), mode: "ingest", status: "active" } }), "gate_shape"],
      [headBody({}, { epoch: E1, version: 7, digest: DIGEST, kind: "selection", pending: false }), "gate_shape"],
      [headBody({}, { epoch: E1, version: "07", digest: DIGEST, kind: "selection", pending: false }), "gate_shape"],
      [headBody({}, { epoch: E1, version: "-1", digest: DIGEST, kind: "selection", pending: false }), "gate_shape"],
      [headBody({}, { epoch: E1, version: "9223372036854775808", digest: DIGEST, kind: "selection", pending: false }), "gate_shape"],
      [headBody({}, { epoch: E1, version: "7", digest: "x", kind: "selection", pending: false }), "gate_shape"],
      [headBody({}, { epoch: E1, version: "7", digest: DIGEST, kind: "delete", pending: false }), "gate_shape"],
      [headBody({}, { epoch: E1, version: "7", digest: DIGEST, kind: "selection", pending: "false" }), "gate_shape"],
      [headBody({}, { epoch: E1, version: "7", digest: DIGEST, kind: "selection", pending: false, extra: 1 }), "gate_shape"],
      [headBody({ observedAt: 5 }), "gate_shape"],
    ];
    for (const [b, want] of cases) {
      const raw = typeof b === "string" ? b : JSON.stringify(b);
      expect(`${raw.slice(0, 60)} -> ${await reason(serviceBindingControlHead(binding(() => new Response(raw, { status: 200 }))).observe(SCOPE))}`).toBe(`${raw.slice(0, 60)} -> ${want}`);
    }
  });
  test("authority-gate-wrong-user / wrong-workspace / wrong-origin (org): an echo of anyone else is fenced", async () => {
    for (const over of [{ user: "u-2" }, { workspace: "ws-b" }, { org: "org-b" }, { user: "U-1" }, { user: "u-1 " }])
      expect(await reason(serviceBindingControlHead(binding(() => ok(headBody(over)))).observe(SCOPE))).toBe("gate_echo");
  });
  test("authority-gate-wrong-epoch: head epoch other than the cutover epoch is fenced", () => {
    expect(() => parseControlHeadResponse(headBody({}, { epoch: E2, version: "7", digest: DIGEST, kind: "selection", pending: false }), SCOPE)).toThrow(BufferedFenced);
    try {
      parseControlHeadResponse(headBody({}, { epoch: E2, version: "7", digest: DIGEST, kind: "reset", pending: false }), SCOPE);
    } catch (e) {
      expect((e as BufferedFenced).reason).toBe("gate_epoch");
    }
  });
  test("direct-consumer-must-enforce-final-gate: a pending head is fenced", () => {
    expect(() => parseControlHeadResponse(headBody({}, { epoch: E1, version: "8", digest: DIGEST, kind: "selection", pending: true }), SCOPE)).toThrow("buffered read fenced");
  });
  test("head-none-without-proven-current-epoch: no active cutover epoch is fenced", () => {
    for (const cutover of [null, { epoch: null, mode: "ingest", status: "active" }])
      expect(() => parseControlHeadResponse(headBody({ cutover }, null), SCOPE)).toThrow(BufferedFenced);
  });
});

describe("admission visibility (fixtures v3, transcribed)", () => {
  // [fixture case id, admission epoch, admission version, final observation, emitted]
  const cases: Array<[string, string | null, string | null, AuthorityHead, boolean]> = [
    ["healthy-bounded-row-positive", E1, "7", authority("7"), true],
    ["disable-reenable-aba-old-admission-must-stay-hidden", E1, "5", authority("7"), false],
    ["same-version-different-control-epoch", E1, "7", authority("7", E2, E2), false],
    ["missing-immutable-admission-version", E1, null, authority("7"), false],
    ["missing-immutable-admission-epoch", null, "7", authority("7"), false],
    ["head-none-with-nonzero-admission", E1, "7", authority(null), false],
    ["head-none-zero-admission-proven-same-epoch", E1, "0", authority(null), true],
    ["head-none-zero-admission-other-epoch (synthetic)", E1, "0", authority(null, E2, E2), false],
    ["any-later-control-conservatively-hides-original-row", E1, "7", authority("8"), false],
    ["gc-replacement-cannot-lend-newer-admission (original)", E1, "7", authority("8"), false],
    ["decimal-versions-must-not-round-to-same-number", E1, "9007199254740992", authority("9007199254740993"), false],
    ["large-exact-decimal-version-positive", E1, "9007199254740993", authority("9007199254740993"), true],
    ["malformed admission version (synthetic)", E1, "7.0", authority("7"), false],
    ["malformed admission epoch (synthetic)", "1".repeat(63), "7", authority("7"), false],
  ];
  for (const [id, epoch, version, auth, emit] of cases)
    test(`${id}: ${emit ? "emitted" : "hidden"}`, () => {
      expect(admissionVisible(auth, { admission_epoch: epoch, admission_version: version })).toBe(emit);
    });
  test("the fixture's decimal pair really collides as Number", () => {
    expect(Number("9007199254740992")).toBe(Number("9007199254740993"));
  });
});

describe("readOwnerBufferedTurns (gated)", () => {
  test("healthy-bounded-row-positive: observe, stages, fetch, recheck, observe; scrubbed rows", async () => {
    const s = store();
    const st = await putStage(s, 1, [turn(0), turn(1)]);
    const { reads, calls } = scriptedReads({ stages: [[st.row]] });
    const gate = scriptedGate(authority("7"));
    const out = await readOwnerBufferedTurns(deps({ store: s, reads, gate }), SCOPE, "claude", SESSION);
    expect(out?.erased).toBe(false);
    expect(out && !out.erased && out.rows.map((r) => r.content)).toEqual(["buffered 0", "buffered 1"]);
    expect(gate.calls).toEqual([SCOPE, SCOPE]);
    expect(calls.map((c) => c.fn)).toEqual(["stages", "stages"]);
    expect(calls.every((c) => c.authority === calls[0]!.authority && JSON.stringify(c.authority) === JSON.stringify(authority("7")))).toBe(true);
  });

  test("first observation fenced: no RPC and no S3 read", async () => {
    const { reads, calls } = scriptedReads({ stages: [[]] });
    expect(await reason(readOwnerBufferedTurns(deps({ store: store(), reads, gate: fencedGate("gate_timeout") }), SCOPE, "claude", SESSION))).toBe("gate_timeout");
    expect(calls).toHaveLength(0);
  });

  test("direct-consumer-must-enforce-final-gate: the final observation failing after S3 is fenced, no content", async () => {
    const s = store();
    const st = await putStage(s, 1, [turn(0)]);
    const gate = scriptedGate(authority("7"), new BufferedFenced("gate_pending"));
    expect(await reason(readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[st.row]] }).reads, gate }), SCOPE, "claude", SESSION))).toBe("gate_pending");
  });

  test("neon-*-between-prefetch-witness-and-s3-finish: the head moving between observations is fenced", async () => {
    const s = store();
    const st = await putStage(s, 1, [turn(0)]);
    for (const moved of [authority("8"), authority(null), authority("7", E2, E2), { ...authority("7"), head: { ...authority("7").head!, digest: "4".repeat(64) } }, { ...authority("7"), head: { ...authority("7").head!, kind: "reset" as const } }]) {
      const gate = scriptedGate(authority("7"), moved);
      expect(await reason(readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[st.row]] }).reads, gate }), SCOPE, "claude", SESSION))).toBe("gate_moved");
    }
  });

  test("disable-reenable-aba-old-admission-must-stay-hidden: old admission under a later head is not served", async () => {
    const s = store();
    const st = await putStage(s, 1, [turn(0)], { admission: { epoch: E1, version: "5" } });
    const out = await readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[st.row]] }).reads, gate: scriptedGate(authority("7")) }), SCOPE, "claude", SESSION);
    expect(out).toBeNull();
  });

  test("NULL admission (missing-immutable-admission-*) hides the session", async () => {
    const s = store();
    for (const admission of [{ epoch: E1, version: null }, { epoch: null, version: "7" }]) {
      const st = await putStage(s, 1, [turn(0)], { admission });
      expect(await readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[st.row]] }).reads }), SCOPE, "claude", SESSION)).toBeNull();
    }
  });

  test("gc-replacement-cannot-lend-newer-admission: one stage at the head does not lend visibility to an older one", async () => {
    const s = store();
    const original = await putStage(s, 1, [turn(0)], { admission: { epoch: E1, version: "7" } });
    const replacement = await putStage(s, 2, [turn(1)], { admission: { epoch: E1, version: "8" } });
    const out = await readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[original.row, replacement.row]] }).reads, gate: scriptedGate(authority("8")) }), SCOPE, "claude", SESSION);
    expect(out).toBeNull();
  });

  test("bigint precision: 9007199254740992 under head 9007199254740993 hidden; exact equal served", async () => {
    const s = store();
    const near = await putStage(s, 1, [turn(0)], { admission: { epoch: E1, version: "9007199254740992" } });
    const head = authority("9007199254740993");
    expect(await readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[near.row]] }).reads, gate: scriptedGate(head) }), SCOPE, "claude", SESSION)).toBeNull();
    const exact = await putStage(s, 2, [turn(0)], { admission: { epoch: E1, version: "9007199254740993" } });
    const out = await readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[exact.row]] }).reads, gate: scriptedGate(head) }), SCOPE, "claude", SESSION);
    expect(out && !out.erased && out.rows).toHaveLength(1);
  });

  test("head none: admission 0 in the active epoch served; admission 7 hidden", async () => {
    const s = store();
    const zero = await putStage(s, 1, [turn(0)], { admission: { epoch: E1, version: "0" } });
    const seven = await putStage(s, 2, [turn(0)], { admission: { epoch: E1, version: "7" } });
    const gate = () => scriptedGate(authority(null));
    expect((await readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[zero.row]] }).reads, gate: gate() }), SCOPE, "claude", SESSION))?.erased).toBe(false);
    expect(await readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[seven.row]] }).reads, gate: gate() }), SCOPE, "claude", SESSION)).toBeNull();
  });

  test("stale-native-row-after-source-deletion-cleared: the post-fetch recheck drops the stage, no content", async () => {
    const s = store();
    const a = await putStage(s, 1, [turn(0, { content: "STALE-NATIVE-A" })]);
    const b = await putStage(s, 2, [turn(1, { content: "kept b" })]);
    // The recheck no longer lists `a` (native custody moved): only `b` is served.
    const out = await readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[a.row, b.row], [b.row]] }).reads }), SCOPE, "claude", SESSION);
    expect(JSON.stringify(out)).not.toContain("STALE-NATIVE-A");
    expect(out && !out.erased && out.rows.map((r) => r.content)).toEqual(["kept b"]);
    // A recheck row that changed (other kept ordinals, other admission) is not the fetched one.
    const changed = { ...a.row, admission_version: "8" };
    const out2 = await readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[a.row], [changed]] }).reads }), SCOPE, "claude", SESSION);
    expect(out2).toEqual({ erased: false, rows: [] });
  });

  test("erasure race: stages present before the fetch, none at the recheck -> erased (no rows)", async () => {
    const s = store();
    const a = await putStage(s, 1, [turn(0, { content: "ERASED-RACE" })]);
    const out = await readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[a.row], []] }).reads }), SCOPE, "claude", SESSION);
    expect(out).toEqual({ erased: true, rows: [] });
  });

  test("no kept stage: null (the route's existing answer)", async () => {
    expect(await readOwnerBufferedTurns(deps({ store: store() }), SCOPE, "claude", SESSION)).toBeNull();
  });

  test("S3 404: re-asks stages once; a stage gone from the ledger is omitted, still listed and missing twice is omitted", async () => {
    const s = store();
    const a = await putStage(s, 1, [turn(0, { content: "A" })]);
    const b = await putStage(s, 2, [turn(1, { content: "B" })]);
    await s.delete(objectKey(a.key));
    const { reads, calls } = scriptedReads({ stages: [[a.row, b.row], [b.row], [b.row]] });
    const out = await readOwnerBufferedTurns(deps({ store: s, reads }), SCOPE, "claude", SESSION);
    expect(out && !out.erased && out.rows.map((r) => r.content)).toEqual(["B"]);
    expect(calls.filter((c) => c.fn === "stages")).toHaveLength(3);
    const again = scriptedReads({ stages: [[a.row, b.row]] });
    const out2 = await readOwnerBufferedTurns(deps({ store: s, reads: again.reads }), SCOPE, "claude", SESSION);
    expect(out2 && !out2.erased && out2.rows.map((r) => r.content)).toEqual(["B"]);
  });

  test("a stage header of another tenant or user is a TenantMismatchError", async () => {
    const s = store();
    for (const header of [{ user_id: "u-2" }, { workspace_id: "ws-b" }, { org_id: "org-b" }]) {
      const st = await putStage(s, 1, [turn(0)], { header });
      expect(await reason(readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[st.row]] }).reads }), SCOPE, "claude", SESSION))).toBe("other:TenantMismatchError");
    }
  });

  test("a group of another session or a record of another origin is refused", async () => {
    const s = store();
    const other = await putStage(s, 1, [turn(0)], { header: { groups: [["claude", "sess-other"]] } });
    await expect(readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[other.row]] }).reads }), SCOPE, "claude", SESSION)).rejects.toThrow("buffered_group_mismatch");
    const origin = await putStage(s, 2, [turn(0)], { header: { origin_id: "other-origin" } });
    await expect(readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[origin.row]] }).reads }), SCOPE, "claude", SESSION)).rejects.toThrow("buffered_origin_mismatch");
  });

  test("a tampered object (sha mismatch) is refused, never served", async () => {
    const s = store();
    const st = await putStage(s, 1, [turn(0)]);
    await expect(readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[{ ...st.row, stage_sha256: "f".repeat(64) }]] }).reads }), SCOPE, "claude", SESSION)).rejects.toThrow();
  });

  test("normalize: stage 2 revises one stage-1 turn and tombstones another", async () => {
    const s = store();
    const one = await putStage(s, 1, [turn(0, { content: "v1 zero" }), turn(1, { content: "v1 one" }), turn(2, { content: "v1 two" })]);
    const two = await putStage(s, 2, [turn(0, { content: "v2 zero" }, 1_793_000_000_000_100), turn(1, {}, 1_793_000_000_000_101, "claude", "delete")]);
    const out = await readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[one.row, two.row]] }).reads }), SCOPE, "claude", SESSION);
    expect(out && !out.erased && out.rows.map((r) => r.content)).toEqual(["v2 zero", "v1 two"]);
    // Stage order does not matter: the higher revision wins either way.
    const rev = await readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[two.row, one.row]] }).reads }), SCOPE, "claude", SESSION);
    expect(rev).toEqual(out);
  });

  test("kept ordinals select lines: a dropped line is never read into the answer", async () => {
    const s = store();
    const st = await putStage(s, 1, [turn(0, { content: "DROPPED-LINE" }), turn(1, { content: "kept" })]);
    const row = { ...st.row, kept_ordinals: [st.row.group_ordinals[0]![1]!] };
    const out = await readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[row]] }).reads }), SCOPE, "claude", SESSION);
    expect(JSON.stringify(out)).not.toContain("DROPPED-LINE");
  });
});

describe("readOwnerBufferedList (gated)", () => {
  test("observe, list with the head, observe; per-row visibility from the final head", async () => {
    const { reads, calls } = scriptedReads({ list: [bufferedRow(), bufferedRow({ session_id: "old", admission_version: "5" }), bufferedRow({ session_id: "nul", admission_epoch: null })] });
    const gate = scriptedGate(authority("7"));
    const rows = await readOwnerBufferedList(deps({ store: store(), reads, gate }), SCOPE, { limit: 10 });
    expect(rows.map((r) => [r.session_id, r.visible])).toEqual([[SESSION, true], ["old", false], ["nul", false]]);
    expect(gate.calls).toHaveLength(2);
    expect(calls[0]).toMatchObject({ fn: "list", authority: authority("7"), args: { limit: 10 } });
  });
  test("fenced before or after the RPC, or moved: BufferedFenced", async () => {
    expect(await reason(readOwnerBufferedList(deps({ store: store(), gate: fencedGate() }), SCOPE, { limit: 1 }))).toBe("gate_status");
    expect(await reason(readOwnerBufferedList(deps({ store: store(), gate: scriptedGate(authority("7"), authority("8")) }), SCOPE, { limit: 1 }))).toBe("gate_moved");
  });
});

describe("sqlOwnerBufferedReads", () => {
  function runner(rows: Record<string, unknown>[] | Error) {
    const seen: Array<{ org: string; ws: string; sql: string; params?: unknown[] }> = [];
    const run = (async (org: string, ws: string, work: (q: { query: (sql: string, params?: unknown[]) => Promise<{ rows: unknown[] }> }) => Promise<unknown>) =>
      work({
        async query(sql, params) {
          seen.push({ org, ws, sql, params });
          if (sql === BUFFERED_SESSIONS_SQL || sql === BUFFERED_STAGES_SQL) {
            if (rows instanceof Error) throw rows;
            return { rows };
          }
          return { rows: [] };
        },
      })) as never;
    return { run, seen };
  }
  const sessionRaw = (over: Record<string, unknown> = {}) => ({
    source: "claude", session_id: SESSION, first_ts: new Date("2026-11-05T12:00:00Z"), last_ts: new Date("2026-11-05T12:05:00Z"), stage_count: "2", bytes: "300",
    user_id: "u-1", admission_epoch: E1, admission_version: "7", cursor_ts: "2026-11-05T12:05:00.000000Z", ...over,
  });

  test("list: timeout, user binding, then named-arg RPC with the observed head; limit clamped", async () => {
    const { run, seen } = runner([sessionRaw()]);
    const rows = await sqlOwnerBufferedReads(run).list(SCOPE, authority("9007199254740993"), { limit: 500, cursor: { ts: "2026-11-05T12:05:00.000000Z", sessionId: "s", source: "codex" } });
    expect(rows[0]).toMatchObject({ session_id: SESSION, stage_count: 2, bytes: 300, admission_version: "7" });
    expect(seen.map((s) => s.sql)).toEqual(["SET LOCAL statement_timeout = 2000", BUFFERED_USER_SQL, BUFFERED_SESSIONS_SQL]);
    expect(seen[1]!.params).toEqual(["u-1"]);
    expect(seen[2]).toMatchObject({ org: "org-a", ws: "ws-a" });
    expect(seen[2]!.params).toEqual(["ws-a", E1, E1, "9007199254740993", DIGEST, false, "2026-11-05T12:05:00.000000Z", "s", "codex", 100]);
    for (const p of ["p_workspace => $1", "p_cutover_epoch => $2", "p_head_epoch => $3", "p_head_version => $4::text", "p_head_digest => $5", "p_head_pending => $6::boolean", "p_limit => $10::integer"])
      expect(BUFFERED_SESSIONS_SQL).toContain(p);
  });
  test("head none passes NULL head params", async () => {
    const { run, seen } = runner([]);
    await sqlOwnerBufferedReads(run).stages(SCOPE, authority(null), "claude", SESSION);
    expect(seen[2]!.params).toEqual(["ws-a", E1, null, null, null, null, "claude", SESSION]);
    expect(BUFFERED_STAGES_SQL).toContain("p_source => $7, p_session_id => $8");
  });
  test("a row of another user is a TenantMismatchError (500), never a page", async () => {
    const { run } = runner([sessionRaw({ user_id: "u-2" })]);
    await expect(sqlOwnerBufferedReads(run).list(SCOPE, authority("7"), { limit: 5 })).rejects.toBeInstanceOf(TenantMismatchError);
  });
  test("malformed admission or cursor is refused; NULL admission stays null", async () => {
    for (const over of [{ admission_version: "7.5" }, { admission_epoch: "x" }, { cursor_ts: "2026-11-05T12:05:00Z" }])
      await expect(sqlOwnerBufferedReads(runner([sessionRaw(over)]).run).list(SCOPE, authority("7"), { limit: 5 })).rejects.toThrow("buffered_row_invalid");
    const rows = await sqlOwnerBufferedReads(runner([sessionRaw({ admission_version: null, admission_epoch: null })]).run).list(SCOPE, authority("7"), { limit: 5 });
    expect(rows[0]).toMatchObject({ admission_epoch: null, admission_version: null });
  });
  test("SQLSTATE 55006 from the RPC is BufferedFenced(rpc_fenced)", async () => {
    const err = Object.assign(new Error("buffered read fenced"), { code: "55006" });
    expect(await reason(sqlOwnerBufferedReads(runner(err).run).stages(SCOPE, authority("7"), "claude", SESSION))).toBe("rpc_fenced");
  });
});

describe("no ungated path", () => {
  test("the module exports only gated readers of buffered content", () => {
    const names = Object.keys(ownerBuffered);
    for (const hidden of ["readStagesUngated", "finalGate", "bufferedTurnRows", "redactQuotedTail", "scrubTurnRow", "scrubDeep", "sameAuthority", "specOf"])
      expect(names).not.toContain(hidden);
    expect(names.filter((n) => /^read/u.test(n)).sort()).toEqual(["readOwnerBufferedList", "readOwnerBufferedTurns"]);
  });
  test("app.ts reaches buffered content only through the gated readers", () => {
    const app = readFileSync(join(import.meta.dir, "..", "app.ts"), "utf8");
    expect(app).not.toMatch(/readBufferedStage|readStagesUngated|\.reads\.(list|stages)\(|sqlOwnerBufferedReads/u);
    expect(app).toContain("readOwnerBufferedTurns(");
    expect(app).toContain("readOwnerBufferedList(");
  });
});

describe("scrubBufferedText (v4 over-cap rule, wrapping scrub.ts)", () => {
  const SUFFIX = "zzTAILzz9q8w7e6r5t4y";
  test("a quoted secret value longer than 4096 units is redacted in full: no suffix byte", () => {
    const text = `config {"api_key": "${"A".repeat(5000)}${SUFFIX}"} done`;
    const out = scrubBufferedText(text);
    expect(out).toContain("[REDACTED]");
    expect(out).not.toContain(SUFFIX);
    expect(out).not.toContain("AAAA");
  });
  test("an unclosed quoted secret (v3 cut at whitespace) loses the whole remainder", () => {
    const out = scrubBufferedText(`"password": "${"b".repeat(40)} ${SUFFIX} more text`);
    expect(out).not.toContain(SUFFIX);
    expect(out.endsWith("[REDACTED]")).toBe(true);
  });
  test("escaped JSON inside a string is handled the same way", () => {
    const out = scrubBufferedText(`{\\"token\\": \\"${"c".repeat(4200)}${SUFFIX}\\"}`);
    expect(out).not.toContain(SUFFIX);
  });
  test("ordinary text and closed short secrets keep the rest of the text", () => {
    expect(scrubBufferedText("hello world")).toBe("hello world");
    const out = scrubBufferedText(`{"api_key": "short-value", "note": "kept tail"}`);
    expect(out).not.toContain("short-value");
    expect(out).toContain("kept tail");
  });
});
