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
import { createRequire } from "node:module";
import { zstdCompressSync } from "node:zlib";
import * as ownerBuffered from "../owner-buffered.ts";
import {
  BUFFERED_INDEX_NAME,
  BUFFERED_INDEX_REASONS,
  BUFFERED_INDEX_RETRY_MS,
  BUFFERED_INDEX_SQL,
  BUFFERED_SESSIONS_SQL,
  BUFFERED_STAGES_SQL,
  BUFFERED_USER_SQL,
  BUFFERED_SESSION_MAX_BYTES,
  BUFFERED_STAGE_MAX_BYTES,
  BufferedFenced,
  BufferedIndexVerifier,
  BufferedTooLarge,
  OwnerBufferedIndexVerifyFlagError,
  ownerBufferedIndexVerifyEnabled,
  sqlBufferedIndexProbe,
  CONTROL_HEAD_V5_KEYS,
  CONTROL_HEAD_WIRE,
  CONTROL_HEAD_DEADLINE_MS,
  CONTROL_HEAD_PATH,
  admissionVisible,
  assertZstdFramesWithin,
  boundedStreamDecompress,
  ownerBufferedEnabled,
  headParams,
  parseControlHeadResponse,
  readOwnerBufferedList,
  readOwnerBufferedTurns,
  scrubBufferedText,
  serviceBindingControlHead,
  sqlOwnerBufferedReads,
  type AuthorityHead,
  type BufferedStageRow,
} from "../owner-buffered.ts";
import { TenantMismatchError } from "../page.ts";
import { objectKey } from "../../shared/vendor.ts";
import { BASELINE, DIGEST, E1, E2, SCOPE, SESSION, authority, bufferedRow, deps, fencedGate, headBody, noHeadBody, indexChecked, putStage, scratch, scriptedGate, scriptedIndexProbe, scriptedReads, turn, unzip as unzipBounded } from "./owner-buffered-support.ts";

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

describe("control-head gate (HISTORY_CONTROL_HEAD binding, frozen wire #328 a06a58f9)", () => {
  const binding = (respond: (url: string, init?: RequestInit) => Promise<Response> | Response) => {
    const calls: Array<{ url: string; init?: RequestInit }> = [];
    return { calls, fetch: async (url: string, init?: RequestInit) => (calls.push({ url, init }), respond(url, init)) };
  };
  const ok = (b: unknown) => new Response(JSON.stringify(b), { status: 200, headers: { "content-type": "application/json", "cache-control": "private, no-store" } });
  const parseReason = (b: unknown) => {
    try {
      parseControlHeadResponse(b, SCOPE);
    } catch (e) {
      return e instanceof BufferedFenced ? e.reason : "other";
    }
    return "parsed";
  };

  test("GETs the pinned path, no query, identity only in the x-relayhistory headers, no body; 2 s; never caches", async () => {
    const b = binding(() => ok(headBody()));
    const gate = serviceBindingControlHead(b);
    expect(await gate.observe(SCOPE)).toEqual(authority("7"));
    await gate.observe(SCOPE);
    expect(b.calls).toHaveLength(2);
    expect(CONTROL_HEAD_PATH).toBe("/v1/session-read/control-head");
    expect(CONTROL_HEAD_DEADLINE_MS).toBe(2000);
    const url = new URL(b.calls[0]!.url);
    expect(url.pathname).toBe(CONTROL_HEAD_PATH);
    expect(url.search).toBe("");
    expect(b.calls[0]!.init?.method).toBe("GET");
    expect(b.calls[0]!.init?.body).toBeUndefined();
    expect(b.calls[0]!.init?.headers).toEqual({
      accept: "application/json", "cache-control": "no-store",
      "x-relayhistory-user-id": "u-1", "x-relayhistory-org-id": "org-a", "x-relayhistory-workspace-id": "ws-a",
    });
    await reason(gate.observe({ orgId: "org-b", workspaceId: "ws-b", userId: "u-9" }));
    expect(b.calls[2]!.init?.headers).toMatchObject({ "x-relayhistory-user-id": "u-9", "x-relayhistory-org-id": "org-b", "x-relayhistory-workspace-id": "ws-b" });
  });

  test("no head: version 0, null digest, null kind, pending false", async () => {
    expect(await serviceBindingControlHead(binding(() => ok(noHeadBody()))).observe(SCOPE)).toEqual(authority(null));
    expect(parseControlHeadResponse(headBody({ sourceControlKind: "reset" }), SCOPE).head?.kind).toBe("reset");
    // the spec's exact large-decimal head round-trips without Number
    expect(parseControlHeadResponse(headBody({ sourceControlVersion: "9007199254740993" }), SCOPE).head?.version).toBe("9007199254740993");
    expect(CONTROL_HEAD_WIRE.top).toHaveLength(12);
    expect(Object.keys(headBody()).sort()).toEqual([...CONTROL_HEAD_WIRE.top].sort());
  });

  // fixtures v3: authority-gate-* (every one: finalBufferedContentEmitted false).
  test("authority-gate-unbound-service: no binding is fenced", async () => {
    expect(await reason(serviceBindingControlHead(undefined).observe(SCOPE))).toBe("gate_unbound");
    expect(await reason(serviceBindingControlHead({} as never).observe(SCOPE))).toBe("gate_unbound");
  });
  test("identifiers: 1..512 UTF-8 bytes, equal to trim(), no control characters; otherwise fenced without a binding call", async () => {
    const b = binding(() => ok(headBody()));
    const gate = serviceBindingControlHead(b);
    for (const over of [{ userId: "" }, { orgId: "o".repeat(513) }, { workspaceId: "\u00e9".repeat(257) }, { userId: " u-1" }, { userId: "u-1\n" }, { orgId: "org\u0000a" }, { workspaceId: "ws\u007fa" }, { userId: "u\u001f1" }])
      expect(await reason(gate.observe({ ...SCOPE, ...over }))).toBe("gate_identity");
    expect(b.calls).toHaveLength(0);
    // exactly 512 bytes is accepted (and echoed)
    const id = "\u00e9".repeat(256);
    const ok512 = serviceBindingControlHead(binding(() => ok(headBody({ tenant: { orgId: SCOPE.orgId, workspaceId: SCOPE.workspaceId, userId: id } }))));
    expect((await ok512.observe({ ...SCOPE, userId: id })).head?.version).toBe("7");
  });
  test("authority-gate-non2xx: any status but 200 is fenced (including 2xx other than 200)", async () => {
    for (const status of [204, 201, 304, 400, 404, 405, 500, 503]) {
      const r = status === 204 || status === 304 ? new Response(null, { status }) : new Response(JSON.stringify(status < 300 ? headBody() : { error: "x" }), { status });
      expect(await reason(serviceBindingControlHead(binding(() => r)).observe(SCOPE))).toBe("gate_status");
    }
    // wire v5.1: 403 for a refused identity; 503 with the producer's nested error body (not parsed).
    const errs: Array<[number, unknown]> = [[403, { error: "invalid_owner_context" }], [404, { error: "read_route_not_allowed" }], [405, { error: "read_only" }], [503, { version: 1, error: { code: "control_head_unavailable" } }], [503, headBody()]];
    for (const [status, body] of errs)
      expect(await reason(serviceBindingControlHead(binding(() => new Response(JSON.stringify(body), { status, headers: { "cache-control": "private, no-store" } }))).observe(SCOPE))).toBe("gate_status");
  });
  test("a 200 without a no-store cache-control is fenced", async () => {
    for (const cc of [null, "", "max-age=600", "private", "no-cache", "no-storex", "public, max-age=0"]) {
      const headers: Record<string, string> = cc === null ? {} : { "cache-control": cc };
      expect(`${cc} -> ${await reason(serviceBindingControlHead(binding(() => new Response(JSON.stringify(headBody()), { status: 200, headers }))).observe(SCOPE))}`).toBe(`${cc} -> gate_cache`);
    }
    for (const cc of ["no-store", "private, no-store", "No-Store", "no-cache,no-store"])
      expect((await serviceBindingControlHead(binding(() => new Response(JSON.stringify(headBody()), { status: 200, headers: { "cache-control": cc } }))).observe(SCOPE)).head?.version).toBe("7");
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
    const without = (k: string) => {
      const b: Record<string, unknown> = headBody();
      delete b[k];
      return b;
    };
    const cases: Array<[unknown, string]> = [
      ["not json", "gate_shape"],
      [JSON.stringify({ ...headBody(), pad: "x".repeat(20_000) }), "gate_shape"],
      [[headBody()], "gate_shape"],
      ["null", "gate_shape"],
      [headBody({ extra: 1 }), "gate_shape"],
      // keys outside the wire table are extras
      ...["observedAt", "phase", "error", "headEpoch"].map((k): [unknown, string] => [headBody({ [k]: "x" }), "gate_shape"]),
      // constants of the frozen wire
      [headBody({ protocol: "relay-session-read-control-head-v2" }), "gate_shape"],
      [headBody({ protocol: "" }), "gate_shape"],
      [headBody({ protocol: null }), "gate_shape"],
      [headBody({ version: "1" }), "gate_shape"],
      [headBody({ version: 2 }), "gate_shape"],
      [headBody({ version: null }), "gate_shape"],
      [headBody({ issuer: "relayhistory-cloud " }), "gate_shape"],
      [headBody({ issuer: "other" }), "gate_shape"],
      // cutover {epoch == controlEpoch, status active, mode ingest}, exact keys
      [headBody({ cutover: { epoch: E1, status: "draining", mode: "ingest" } }), "gate_shape"],
      [headBody({ cutover: { epoch: E1, status: "active", mode: "read" } }), "gate_shape"],
      [headBody({ cutover: { epoch: "1".repeat(63), status: "active", mode: "ingest" } }), "gate_shape"],
      [headBody({ cutover: { epoch: null, status: "active", mode: "ingest" } }), "gate_shape"],
      [headBody({ cutover: { epoch: E1, status: "active" } }), "gate_shape"],
      [headBody({ cutover: { epoch: E1, status: "active", mode: "ingest", phase: "ingest" } }), "gate_shape"],
      [headBody({ cutover: null }), "gate_shape"],
      [headBody({ cutover: { epoch: E2, status: "active", mode: "ingest" } }), "gate_epoch"],
      [noHeadBody({ cutover: { epoch: E2, status: "active", mode: "ingest" } }), "gate_epoch"],
      // sourceControlEpoch: == controlEpoch with a head (foreign or null epoch never reads as no head), null without
      [headBody({ sourceControlEpoch: null }), "gate_epoch"],
      [headBody({ sourceControlEpoch: E2 }), "gate_epoch"],
      [headBody({ sourceControlEpoch: "1".repeat(63) }), "gate_shape"],
      [noHeadBody({ sourceControlEpoch: E1 }), "gate_shape"],
      [noHeadBody({ sourceControlEpoch: E2 }), "gate_shape"],
      [headBody({ controlEpoch: E2 }), "gate_epoch"],
      [headBody({ sourceControlVersion: "12345678901234567890" }), "gate_shape"],
      [headBody({ tenant: { orgId: "org-a", workspaceId: "ws-a", userId: "u-1", headEpoch: E1 } }), "gate_shape"],
      ...CONTROL_HEAD_V5_KEYS.map((k): [unknown, string] => [without(k), "gate_shape"]),
      [headBody({ controlEpoch: "1".repeat(63) }), "gate_shape"],
      [headBody({ controlEpoch: "A".repeat(64) }), "gate_shape"],
      [headBody({ controlEpoch: null }), "gate_shape"],
      [headBody({ privacyBaselineSha256: "4".repeat(63) }), "gate_shape"],
      [headBody({ privacyBaselineSha256: null }), "gate_shape"],
      [headBody({ sourceControlVersion: 7 }), "gate_shape"],
      [headBody({ sourceControlVersion: "07" }), "gate_shape"],
      [headBody({ sourceControlVersion: "-1" }), "gate_shape"],
      [headBody({ sourceControlVersion: "9223372036854775808" }), "gate_shape"],
      [headBody({ sourceControlVersion: null }), "gate_shape"],
      [headBody({ sourceControlDigest: "x" }), "gate_shape"],
      [headBody({ sourceControlDigest: null }), "gate_shape"],
      [headBody({ sourceControlKind: "other" }), "gate_shape"],
      [headBody({ sourceControlKind: "Selection" }), "gate_shape"],
      [headBody({ sourceControlKind: null }), "gate_shape"],
      [headBody({ pending: "false" }), "gate_shape"],
      [headBody({ pending: null }), "gate_shape"],
      [headBody({ tenant: { orgId: "org-a", workspaceId: "ws-a" } }), "gate_shape"],
      [headBody({ tenant: { orgId: "org-a", workspaceId: "ws-a", userId: "u-1", extra: 1 } }), "gate_shape"],
      [headBody({ tenant: { orgId: "org-a", workspaceId: "ws-a", userId: 1 } }), "gate_shape"],
      [headBody({ tenant: null }), "gate_shape"],
      [noHeadBody({ sourceControlDigest: DIGEST }), "gate_shape"],
      [noHeadBody({ sourceControlKind: "selection" }), "gate_shape"],
      [noHeadBody({ sourceControlKind: "other" }), "gate_shape"],
    ];
    for (const [b, want] of cases) {
      const raw = typeof b === "string" ? b : JSON.stringify(b);
      expect(`${raw.slice(-240)} -> ${await reason(serviceBindingControlHead(binding(() => new Response(raw, { status: 200, headers: { "cache-control": "private, no-store" } }))).observe(SCOPE))}`).toBe(`${raw.slice(-240)} -> ${want}`);
    }
  });
  test("authority-gate-wrong-user / wrong-workspace / wrong-origin (org): a tenant echo of anyone else is fenced", async () => {
    const t = { orgId: "org-a", workspaceId: "ws-a", userId: "u-1" };
    for (const over of [{ userId: "u-2" }, { workspaceId: "ws-b" }, { orgId: "org-b" }, { userId: "U-1" }, { userId: "u-1 " }])
      expect(await reason(serviceBindingControlHead(binding(() => ok(headBody({ tenant: { ...t, ...over } })))).observe(SCOPE))).toBe("gate_echo");
    expect(parseReason(noHeadBody({ tenant: { ...t, userId: "u-2" } }))).toBe("gate_echo");
  });
  test("authority-gate-wrong-epoch: the head epoch is controlEpoch (== sourceControlEpoch); an admission at another epoch is hidden", () => {
    const a = parseControlHeadResponse(headBody({ controlEpoch: E2, sourceControlEpoch: E2, cutover: { epoch: E2, status: "active", mode: "ingest" } }), SCOPE);
    expect(a).toMatchObject({ cutoverEpoch: E2, head: { epoch: E2, version: "7" } });
    expect(admissionVisible(a, { admission_epoch: E1, admission_version: "7" })).toBe(false);
    expect(admissionVisible(a, { admission_epoch: E2, admission_version: "7" })).toBe(true);
  });
  test("direct-consumer-must-enforce-final-gate: a pending head is fenced (with or without a head)", () => {
    expect(() => parseControlHeadResponse(headBody({ pending: true, sourceControlVersion: "8" }), SCOPE)).toThrow("buffered read fenced");
    expect(parseReason(headBody({ pending: true }))).toBe("gate_pending");
    expect(parseReason(noHeadBody({ pending: true }))).toBe("gate_pending");
  });
  test("head-none-without-proven-current-epoch: no hex64 controlEpoch is fenced", () => {
    for (const controlEpoch of [null, "", "0"]) expect(parseReason(noHeadBody({ controlEpoch, cutover: { epoch: controlEpoch, status: "active", mode: "ingest" } }))).toBe("gate_shape");
  });
  test("two-lookup consistency: baseline, controlEpoch or head drift between observations is gate_moved", async () => {
    const s = store();
    const st = await putStage(s, 1, [turn(0)]);
    const variants: Array<Record<string, unknown>> = [
      { privacyBaselineSha256: "5".repeat(64) },
      { controlEpoch: E2, sourceControlEpoch: E2, cutover: { epoch: E2, status: "active", mode: "ingest" } },
      { sourceControlVersion: "8" },
      { sourceControlDigest: "6".repeat(64) },
      { sourceControlKind: "reset" },
      { sourceControlEpoch: null, sourceControlVersion: "0", sourceControlDigest: null, sourceControlKind: null },
    ];
    for (const second of variants) {
      let n = 0;
      const gate = serviceBindingControlHead(binding(() => ok(n++ === 0 ? headBody() : headBody(second))));
      const got = await reason(readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[st.row]] }).reads, gate }), SCOPE, "claude", SESSION));
      expect(`${JSON.stringify(second)} -> ${got}`).toBe(`${JSON.stringify(second)} -> gate_moved`);
    }
    const steady = serviceBindingControlHead(binding(() => ok(headBody())));
    expect(await readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[st.row]] }).reads, gate: steady }), SCOPE, "claude", SESSION)).not.toBeNull();
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

  test("first observation fenced: no RPC and no S3 read; reported as unverified before any candidate (route keeps main's 501)", async () => {
    const { reads, calls } = scriptedReads({ stages: [[]] });
    expect(await readOwnerBufferedTurns(deps({ store: store(), reads, gate: fencedGate("gate_timeout") }), SCOPE, "claude", SESSION)).toEqual({ unverifiedBeforeCandidate: "gate_timeout" });
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

  test("stale-native-row-after-source-deletion-cleared: a stage dropped at the post-fetch recheck hides the whole session", async () => {
    const s = store();
    const a = await putStage(s, 1, [turn(0, { content: "STALE-NATIVE-A" })]);
    const b = await putStage(s, 2, [turn(1, { content: "kept b" })]);
    // The recheck no longer lists `a` (native custody moved): the session is hidden, `b` is not served alone.
    const out = await readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[a.row, b.row], [b.row]] }).reads }), SCOPE, "claude", SESSION);
    expect(JSON.stringify(out)).not.toContain("STALE-NATIVE-A");
    expect(out).toBeNull();
    // A recheck row that changed (other kept ordinals, other admission) is not the fetched one: hidden.
    const changed = { ...a.row, admission_version: "8" };
    const out2 = await readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[a.row], [changed]] }).reads }), SCOPE, "claude", SESSION);
    expect(out2).toBeNull();
  });

  test("partial sessions: any stage added, removed or changed between fetch and recheck hides the whole session", async () => {
    const s = store();
    const a = await putStage(s, 1, [turn(0, { content: "PART-A" })]);
    const b = await putStage(s, 2, [turn(1, { content: "PART-B" })]);
    const variants: BufferedStageRow[][] = [
      [a.row, b.row, { ...b.row, batch_id: "b-extra", stage_key: `${b.row.stage_key}.x` }],
      [a.row, { ...b.row, kept_ordinals: [] }],
      [a.row, { ...b.row, reset_epoch: "1" }],
      [a.row, { ...b.row, stage_sha256: "e".repeat(64) }],
      [a.row],
    ];
    for (const recheck of variants) {
      const out = await readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[a.row, b.row], recheck] }).reads }), SCOPE, "claude", SESSION);
      expect(out).toBeNull();
    }
    const same = await readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[a.row, b.row], [b.row, a.row]] }).reads }), SCOPE, "claude", SESSION);
    expect(same && !same.erased && same.rows.map((r) => r.content)).toEqual(["PART-A", "PART-B"]);
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
    // Still listed but missing twice: the fetched set ({b}) differs from the recheck ({a,b}), so the whole session is hidden.
    const again = scriptedReads({ stages: [[a.row, b.row]] });
    const out2 = await readOwnerBufferedTurns(deps({ store: s, reads: again.reads }), SCOPE, "claude", SESSION);
    expect(out2).toBeNull();
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

describe("byte bounds on detail reads (hidden, never partial)", () => {
  const MiB = 1024 * 1024;
  test("constants: 16 MiB per stage (the writer's cap), 32 MiB per session", () => {
    expect(BUFFERED_STAGE_MAX_BYTES).toBe(16 * MiB);
    expect(BUFFERED_SESSION_MAX_BYTES).toBe(32 * MiB);
  });

  test("a stage row claiming more than 16 MiB compressed is BufferedTooLarge before any GetObject or recheck", async () => {
    const s = store();
    const st = await putStage(s, 1, [turn(0, { content: "OVER-CAP" })]);
    const { reads, calls } = scriptedReads({ stages: [[{ ...st.row, stage_bytes: BUFFERED_STAGE_MAX_BYTES + 1 }]] });
    let decompressCalls = 0;
    const d = deps({ store: s, reads, decompress: (b, m) => (decompressCalls++, unzipBounded(b, m)) });
    await expect(readOwnerBufferedTurns(d, SCOPE, "claude", SESSION)).rejects.toBeInstanceOf(BufferedTooLarge);
    expect(decompressCalls).toBe(0);
    expect(calls.filter((c) => c.fn === "stages")).toHaveLength(1);
  });

  test("stages summing past the session bound (compressed) are BufferedTooLarge before any GetObject", async () => {
    const s = store();
    const a = await putStage(s, 1, [turn(0)]);
    const b = await putStage(s, 2, [turn(1)]);
    const half = BUFFERED_SESSION_MAX_BYTES / 2;
    const rows = [{ ...a.row, stage_bytes: half }, { ...b.row, stage_bytes: half + 1 }];
    let decompressCalls = 0;
    const d = deps({ store: s, reads: scriptedReads({ stages: [rows] }).reads, decompress: (x, m) => (decompressCalls++, unzipBounded(x, m)) });
    await expect(readOwnerBufferedTurns(d, SCOPE, "claude", SESSION)).rejects.toBeInstanceOf(BufferedTooLarge);
    expect(decompressCalls).toBe(0);
  });

  test("an over-cap stage (decompresses past 16 MiB) is BufferedTooLarge; the decompressor is given the 16 MiB bound", async () => {
    const s = store();
    const bomb = await putStage(s, 1, [turn(0, { content: "x".repeat(BUFFERED_STAGE_MAX_BYTES + 1) })]);
    expect(bomb.row.stage_bytes).toBeLessThan(MiB);
    const bounds: number[] = [];
    const d = deps({ store: s, reads: scriptedReads({ stages: [[bomb.row]] }).reads, decompress: (x, m) => (bounds.push(m), unzipBounded(x, m)) });
    await expect(readOwnerBufferedTurns(d, SCOPE, "claude", SESSION)).rejects.toBeInstanceOf(BufferedTooLarge);
    expect(bounds).toEqual([BUFFERED_STAGE_MAX_BYTES]);
  });

  test("the session bound shrinks the room for later stages: the third 12 MiB stage is BufferedTooLarge", async () => {
    const s = store();
    const big = (n: number) => putStage(s, n, [turn(0, { content: String(n).repeat(12 * MiB) })]);
    const rows = [(await big(1)).row, (await big(2)).row, (await big(3)).row];
    const bounds: number[] = [];
    const d = deps({ store: s, reads: scriptedReads({ stages: [rows] }).reads, decompress: (x, m) => (bounds.push(m), unzipBounded(x, m)) });
    await expect(readOwnerBufferedTurns(d, SCOPE, "claude", SESSION)).rejects.toBeInstanceOf(BufferedTooLarge);
    expect(bounds).toHaveLength(3);
    expect(bounds[0]).toBe(BUFFERED_STAGE_MAX_BYTES);
    expect(bounds[2]!).toBeLessThan(BUFFERED_STAGE_MAX_BYTES);
  });

  /** One zstd frame: single segment, 1-byte content size, one raw last block. */
  const rawFrame = (payload: number[]) => new Uint8Array([0x28, 0xb5, 0x2f, 0xfd, 0x20, payload.length, ((payload.length << 3) | 1) & 0xff, (payload.length << 3) >> 8, 0, ...payload]);
  test("boundedStreamDecompress: concatenates chunks within the bound; stops at the first chunk past it", () => {
    let pushedAfterStop = 0;
    class FakeStream {
      constructor(private readonly ondata: (chunk: Uint8Array, final?: boolean) => unknown) {}
      push(input: Uint8Array) {
        const payload = input.subarray(9);
        for (let i = 0; i < payload.byteLength; i++) {
          this.ondata(new Uint8Array([payload[i]!, payload[i]!]), i === payload.byteLength - 1);
          pushedAfterStop += 1;
        }
      }
    }
    const d = boundedStreamDecompress(FakeStream);
    expect([...d(rawFrame([1, 2, 3]), 6)]).toEqual([1, 1, 2, 2, 3, 3]);
    pushedAfterStop = 0;
    // header (content size 5) passes; the fake doubles each byte, so chunks 1 and 2 (4 bytes)
    // fit and chunk 3 (6 bytes) is past 5: the stream stops there.
    expect(() => d(rawFrame([1, 2, 3, 4, 5]), 5)).toThrow(BufferedTooLarge);
    expect(pushedAfterStop).toBe(2);
  });

  test("zstd frame headers over the bound are refused before the decoder sees them (N3)", () => {
    let constructed = 0;
    class Never {
      constructor() {
        constructed += 1;
      }
      push() {}
    }
    const d = boundedStreamDecompress(Never);
    const MiB = 1024 * 1024;
    // single segment, 8-byte content size of 1 GiB, then an empty raw last block.
    const bigFcs = new Uint8Array([0x28, 0xb5, 0x2f, 0xfd, 0xe0, 0, 0, 0, 0x40, 0, 0, 0, 0, 1, 0, 0]);
    // not single segment, no content size, window descriptor 2^(10+21) = 2 GiB.
    const bigWindow = new Uint8Array([0x28, 0xb5, 0x2f, 0xfd, 0x00, 21 << 3, 1, 0, 0]);
    // a small first frame followed by an oversized second one.
    const second = new Uint8Array([...rawFrame([1]), ...bigWindow]);
    for (const frame of [bigFcs, bigWindow, second]) expect(() => d(frame, 16 * MiB)).toThrow(BufferedTooLarge);
    for (const junk of [new Uint8Array([1, 2, 3]), new Uint8Array([0x28, 0xb5, 0x2f, 0xfd]), rawFrame([1, 2]).subarray(0, 10), new Uint8Array([0x28, 0xb5, 0x2f, 0xfd, 0x20, 1, 7, 0, 0, 9])])
      expect(() => d(junk, 16 * MiB)).toThrow("buffered_zstd_invalid");
    expect(constructed).toBe(0);
    // skippable frame then a real one passes the walk.
    const skippable = new Uint8Array([0x50, 0x2a, 0x4d, 0x18, 2, 0, 0, 0, 9, 9]);
    expect(() => assertZstdFramesWithin(new Uint8Array([...skippable, ...rawFrame([1, 2])]), 2)).not.toThrow();
    expect(() => assertZstdFramesWithin(rawFrame([1, 2, 3]), 2)).toThrow(BufferedTooLarge);
  });

  test("real fzstd over real zstd output (single and multi frame) passes the header walk", () => {
    // The Worker's own fzstd (packages/session-read-worker dependency), resolved from there.
    const FzstdDecompress = createRequire(join(import.meta.dir, "../../../packages/session-read-worker/package.json"))("fzstd").Decompress as ownerBuffered.StreamDecompressor;
    const a = new Uint8Array(zstdCompressSync(Buffer.from("x".repeat(200_000))));
    const b = new Uint8Array(zstdCompressSync(Buffer.from("hello")));
    const d = boundedStreamDecompress(FzstdDecompress);
    expect(d(a, 200_000).byteLength).toBe(200_000);
    expect(new TextDecoder().decode(d(new Uint8Array([...b, ...b]), 10))).toBe("hellohello");
    expect(() => d(a, 199_999)).toThrow(BufferedTooLarge);
  });
});

describe("parity addendum v1 (PR-B-scoped cases; PR B layer, stubbed reader; proof binding is PR A SQL)", () => {
  // Transcribed from fixtures-v3/parity-addendum-v1-exact.json (synthetic vectors). The proof row
  // binding itself is enforced by the PR A RPC (043700); here the reader result is stubbed and the
  // PR B checks are exercised: the RPC refusing (no rows) hides, and a foreign tenant/user in what
  // the stubbed reader returns fails closed (TenantMismatchError), never content.
  const foreign = { user: { user_id: "foreign-user" }, workspace: { workspace_id: "foreign-workspace" }, org: { org_id: "foreign-org" } } as const;

  test("exact-current-user-proof-positive (PR B layer, stubbed reader; proof binding is PR A SQL): served", async () => {
    const s = store();
    const st = await putStage(s, 1, [turn(0, { content: "PROOF-POSITIVE" })]);
    const out = await readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[st.row]] }).reads, gate: scriptedGate(authority("7")) }), SCOPE, "claude", SESSION);
    expect(out && !out.erased && out.rows.map((r) => r.content)).toEqual(["PROOF-POSITIVE"]);
  });

  for (const [id, kind] of [["proof-foreign-user", "user"], ["proof-foreign-workspace", "workspace"], ["proof-foreign-org", "org"]] as const) {
    test(`${id} (PR B layer, stubbed reader; proof binding is PR A SQL): not emitted`, async () => {
      const s = store();
      // The RPC refuses the foreign proof: no stage rows -> hidden (the route's existing 501).
      expect(await readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[]] }).reads }), SCOPE, "claude", SESSION)).toBeNull();
      // A stubbed reader that wrongly returned a stage of the foreign tenant/user: fail closed, no content.
      const st = await putStage(s, 1, [turn(0, { content: `FOREIGN-${kind}` })], { header: foreign[kind] });
      await expect(readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[st.row]] }).reads }), SCOPE, "claude", SESSION)).rejects.toBeInstanceOf(TenantMismatchError);
    });
  }
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
  test("presence runs between the RPC and the final observation, only for admitted ids; a move during it is fenced", async () => {
    const order: string[] = [];
    const { reads } = scriptedReads({ list: [bufferedRow(), bufferedRow({ session_id: "old", admission_version: "5" }), bufferedRow({ session_id: "live" })] });
    const answers = [authority("7"), authority("7")];
    const gate = { async observe() { order.push("observe"); return answers.shift()!; } };
    const presence = async (ids: string[]) => {
      order.push(`presence:${ids.join(",")}`);
      return new Set(["live"]);
    };
    const rows = await readOwnerBufferedList(deps({ store: store(), reads, gate }), SCOPE, { limit: 10 }, presence);
    expect(order).toEqual(["observe", `presence:${SESSION},live`, "observe"]);
    expect(rows.map((r) => [r.session_id, r.visible, r.live])).toEqual([[SESSION, true, false], ["old", false, false], ["live", true, true]]);
    const moved = scriptedGate(authority("7"), authority("8"));
    expect(await reason(readOwnerBufferedList(deps({ store: store(), reads: scriptedReads({ list: [bufferedRow()] }).reads, gate: moved }), SCOPE, { limit: 1 }, presence))).toBe("gate_moved");
    expect(moved.calls).toHaveLength(2);
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
    expect(seen[2]!.params).toEqual(["ws-a", E1, E1, "9007199254740993", DIGEST, "selection", false, BASELINE, "2026-11-05T12:05:00.000000Z", "s", "codex", 100]);
    for (const p of ["p_workspace => $1", "p_cutover_epoch => $2", "p_head_epoch => $3", "p_head_version => $4::text", "p_head_digest => $5", "p_head_kind => $6", "p_head_pending => $7::boolean", "p_privacy_baseline_sha256 => $8", "p_cursor_ts => $9::timestamptz", "p_limit => $12::integer"])
      expect(BUFFERED_SESSIONS_SQL).toContain(p);
  });
  test("both readers name the same authority args, baseline and kind included", () => {
    for (const p of ["p_cutover_epoch => $2", "p_head_epoch => $3", "p_head_version => $4::text", "p_head_digest => $5", "p_head_kind => $6", "p_head_pending => $7::boolean", "p_privacy_baseline_sha256 => $8"])
      expect(BUFFERED_STAGES_SQL).toContain(p);
    expect(BUFFERED_STAGES_SQL).toContain("p_source => $9, p_session_id => $10)");
    expect(BUFFERED_SESSIONS_SQL.match(/=> \$/g)!.length).toBe(12);
    expect(BUFFERED_STAGES_SQL.match(/=> \$/g)!.length).toBe(10);
  });
  test("head none: NULL epoch/version/digest/kind, pending FALSE (never NULL, never '0'), baseline passed", async () => {
    const { run, seen } = runner([]);
    const a = parseControlHeadResponse(noHeadBody(), SCOPE);
    expect(headParams(a)).toEqual({ cutoverEpoch: E1, headEpoch: null, headVersion: null, headDigest: null, headKind: null, headPending: false, privacyBaselineSha256: BASELINE });
    await sqlOwnerBufferedReads(run).stages(SCOPE, a, "claude", SESSION);
    expect(seen[2]!.params).toEqual(["ws-a", E1, null, null, null, null, false, BASELINE, "claude", SESSION]);
    await sqlOwnerBufferedReads(run).list(SCOPE, a, { limit: 5 });
    expect(seen[5]!.params).toEqual(["ws-a", E1, null, null, null, null, false, BASELINE, null, null, null, 5]);
  });
  test("head present: epoch = controlEpoch, exact version string, digest, kind and baseline from the wire", async () => {
    for (const kind of ["selection", "reset"] as const) {
      const baseline = "a".repeat(64);
      const a = parseControlHeadResponse(headBody({ sourceControlKind: kind, sourceControlVersion: "9223372036854775807", privacyBaselineSha256: baseline }), SCOPE);
      expect(headParams(a)).toEqual({ cutoverEpoch: E1, headEpoch: E1, headVersion: "9223372036854775807", headDigest: DIGEST, headKind: kind, headPending: false, privacyBaselineSha256: baseline });
      const { run, seen } = runner([]);
      await sqlOwnerBufferedReads(run).stages(SCOPE, a, "claude", SESSION);
      expect(seen[2]!.params).toEqual(["ws-a", E1, E1, "9223372036854775807", DIGEST, kind, false, baseline, "claude", SESSION]);
    }
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
  test("57014 (statement_timeout) and any other RPC error are fenced on both readers, never a generic error", async () => {
    const cases: Array<[Error, string]> = [
      [Object.assign(new Error("canceling statement due to statement timeout"), { code: "57014" }), "rpc_timeout"],
      [Object.assign(new Error("function sessions.buffered_sessions_for does not exist"), { code: "42883" }), "rpc_error"],
      [Object.assign(new Error("permission denied"), { code: "42501" }), "rpc_error"],
      [new Error("connection terminated unexpectedly"), "rpc_error"],
    ];
    for (const [err, want] of cases) {
      const reads = sqlOwnerBufferedReads(runner(err).run);
      expect(await reason(reads.stages(SCOPE, authority("7"), "claude", SESSION))).toBe(want);
      expect(await reason(reads.list(SCOPE, authority("7"), { limit: 5 }))).toBe(want);
    }
    const failingRun = (async () => {
      throw new Error("pool exhausted");
    }) as never;
    expect(await reason(sqlOwnerBufferedReads(failingRun).list(SCOPE, authority("7"), { limit: 5 }))).toBe("rpc_error");
  });
});

describe("out-of-band index verify (READ_API_OWNER_BUFFERED_INDEX_VERIFY)", () => {
  test("flag: unset or empty is off, exactly \"on\" is on, anything else throws a named error naming no value", () => {
    expect(ownerBufferedIndexVerifyEnabled(undefined)).toBe(false);
    expect(ownerBufferedIndexVerifyEnabled("")).toBe(false);
    expect(ownerBufferedIndexVerifyEnabled("on")).toBe(true);
    for (const v of ["ON", " on", "on ", "1", "true", "off", "yes", "0"]) {
      let err: unknown;
      try {
        ownerBufferedIndexVerifyEnabled(v);
      } catch (e) {
        err = e;
      }
      expect(err, v).toBeInstanceOf(OwnerBufferedIndexVerifyFlagError);
      expect((err as Error).name).toBe("OwnerBufferedIndexVerifyFlagError");
      expect((err as Error).message).toBe('READ_API_OWNER_BUFFERED_INDEX_VERIFY must be unset or "on"');
    }
  });

  test("reason names are fixed", () => {
    expect(BUFFERED_INDEX_REASONS).toEqual({ missing: "index_missing", invalid: "index_invalid", unverified: "index_unverified" });
    expect(BUFFERED_INDEX_RETRY_MS).toBe(60_000);
  });

  test("the probe: catalog SQL only, constants, no parameter; validity needs indisvalid (and ready, live)", () => {
    expect(BUFFERED_INDEX_NAME).toBe("ingest_buffer_outbox_user_buffered_idx");
    for (const part of ["pg_catalog.pg_index", "pg_catalog.pg_class", "pg_catalog.pg_namespace", "i.indisvalid", "n.nspname = 'sessions'", "t.relname = 'ingest_buffer_outbox'", "c.relname = 'ingest_buffer_outbox_user_buffered_idx'"])
      expect(BUFFERED_INDEX_SQL).toContain(part);
    expect(BUFFERED_INDEX_SQL).not.toMatch(/\$\d/u);
    expect(BUFFERED_INDEX_SQL).toMatch(/^SELECT /u);
  });

  function probeRunner(answer: Array<{ valid: unknown }> | Error) {
    const seen: Array<{ org: string; ws: string; sql: string; params?: unknown[] }> = [];
    const run = (async (org: string, ws: string, work: (q: { query: (sql: string, params?: unknown[]) => Promise<{ rows: unknown[] }> }) => Promise<unknown>) =>
      work({
        async query(sql, params) {
          seen.push({ org, ws, sql, params });
          if (sql === BUFFERED_INDEX_SQL) {
            if (answer instanceof Error) throw answer;
            return { rows: answer };
          }
          return { rows: [] };
        },
      })) as never;
    return { run, seen };
  }

  test("sqlBufferedIndexProbe: the tenant runner, the buffered statement timeout, then the catalog query; maps rows to valid/invalid/missing", async () => {
    const ok = probeRunner([{ valid: true }]);
    expect(await sqlBufferedIndexProbe(ok.run)(SCOPE)).toBe("valid");
    expect(ok.seen.map((s) => s.sql)).toEqual(["SET LOCAL statement_timeout = 2000", BUFFERED_INDEX_SQL]);
    expect(ok.seen[1]).toMatchObject({ org: "org-a", ws: "ws-a", params: undefined });
    expect(await sqlBufferedIndexProbe(probeRunner([]).run)(SCOPE)).toBe("missing");
    expect(await sqlBufferedIndexProbe(probeRunner([{ valid: false }]).run)(SCOPE)).toBe("invalid");
    expect(await sqlBufferedIndexProbe(probeRunner([{ valid: null }]).run)(SCOPE)).toBe("invalid");
    const custom = probeRunner([{ valid: true }]);
    await sqlBufferedIndexProbe(custom.run, { statementTimeoutMs: 750 })(SCOPE);
    expect(custom.seen[0]!.sql).toBe("SET LOCAL statement_timeout = 750");
    const boom = new Error("canceling statement due to statement timeout");
    expect(await sqlBufferedIndexProbe(probeRunner(boom).run)(SCOPE).then(() => "resolved", (e) => e)).toBe(boom);
  });

  test("valid: served, and remembered for the isolate's lifetime (no second query, even past the TTL)", async () => {
    let t = 0;
    const v = new BufferedIndexVerifier({ now: () => t });
    const probe = scriptedIndexProbe("valid", "missing");
    expect(await reason(v.check(probe).ensure(SCOPE))).toBe("resolved");
    expect(probe.calls).toEqual([SCOPE]);
    t = 10 * BUFFERED_INDEX_RETRY_MS;
    expect(await reason(v.check(probe).ensure(SCOPE))).toBe("resolved");
    expect(await reason(v.check(scriptedIndexProbe("missing")).ensure(SCOPE))).toBe("resolved");
    expect(probe.calls).toHaveLength(1);
  });

  test("missing, invalid and a query error fence under their own names; each is remembered for the TTL, then re-checked", async () => {
    for (const [answer, name] of [["missing", "index_missing"], ["invalid", "index_invalid"], [new Error("connection terminated"), "index_unverified"]] as const) {
      let t = 1_000;
      const v = new BufferedIndexVerifier({ now: () => t });
      const probe = scriptedIndexProbe(answer, "valid");
      expect(await reason(v.check(probe).ensure(SCOPE)), name).toBe(name);
      expect(probe.calls).toHaveLength(1);
      t += BUFFERED_INDEX_RETRY_MS - 1;
      expect(await reason(v.check(probe).ensure(SCOPE)), name).toBe(name);
      expect(probe.calls).toHaveLength(1); // cached failure: no query
      t += 1;
      expect(await reason(v.check(probe).ensure(SCOPE)), name).toBe("resolved");
      expect(probe.calls).toHaveLength(2); // TTL elapsed: re-checked, now valid
    }
  });

  test("concurrent requests share one in-flight probe", async () => {
    let release!: (s: "valid" | "missing") => void;
    let calls = 0;
    const probe = () => {
      calls += 1;
      return new Promise<"valid" | "missing">((resolve) => (release = resolve));
    };
    const v = new BufferedIndexVerifier();
    const waiting = [1, 2, 3, 4].map(() => reason(v.check(probe).ensure(SCOPE)));
    await Promise.resolve();
    expect(calls).toBe(1);
    release("missing");
    expect(await Promise.all(waiting)).toEqual(["index_missing", "index_missing", "index_missing", "index_missing"]);
    expect(calls).toBe(1);
  });

  test("a probe that does not answer in time: index_unverified for the waiter, probe forgotten, re-checked after the TTL; a late valid answer still counts", async () => {
    let t = 0;
    let release!: (s: "valid") => void;
    let calls = 0;
    const hang = () => {
      calls += 1;
      return new Promise<"valid">((resolve) => (release = resolve));
    };
    const v = new BufferedIndexVerifier({ now: () => t, waitMs: 5 });
    expect(await reason(v.check(hang).ensure(SCOPE))).toBe("index_unverified");
    expect(await reason(v.check(hang).ensure(SCOPE))).toBe("index_unverified");
    expect(calls).toBe(1); // within the TTL: fenced without a query
    t = BUFFERED_INDEX_RETRY_MS;
    const second = scriptedIndexProbe("missing");
    expect(await reason(v.check(second).ensure(SCOPE))).toBe("index_missing"); // forgotten: a new probe ran
    expect(second.calls).toHaveLength(1);
    release("valid"); // the first, abandoned probe finally answers
    await new Promise((r) => setTimeout(r, 0));
    expect(await reason(v.check(scriptedIndexProbe("missing")).ensure(SCOPE))).toBe("resolved");
  });

  test("list: not proven is BufferedFenced before the gate, the RPC or presence; proven runs the read unchanged", async () => {
    for (const [answer, name] of [["missing", "index_missing"], ["invalid", "index_invalid"], [new Error("x"), "index_unverified"]] as const) {
      const { reads, calls } = scriptedReads({ list: [bufferedRow()] });
      const gate = scriptedGate(authority("7"));
      let presence = 0;
      const { indexCheck, probe } = indexChecked(answer);
      expect(await reason(readOwnerBufferedList(deps({ store: store(), reads, gate, indexCheck }), SCOPE, { limit: 10 }, async () => (presence++, new Set())))).toBe(name);
      expect(probe.calls).toEqual([SCOPE]);
      expect([gate.calls.length, calls.length, presence]).toEqual([0, 0, 0]);
    }
    const { reads } = scriptedReads({ list: [bufferedRow()] });
    const gate = scriptedGate(authority("7"));
    const { indexCheck, probe } = indexChecked("valid");
    const rows = await readOwnerBufferedList(deps({ store: store(), reads, gate, indexCheck }), SCOPE, { limit: 10 });
    expect(rows.map((r) => [r.session_id, r.visible])).toEqual([[SESSION, true]]);
    expect(probe.calls).toHaveLength(1);
    expect(gate.calls).toHaveLength(2);
  });

  test("turns: not proven is unverified before any candidate (no gate, no RPC, no row data); proven reads as without the check", async () => {
    for (const [answer, name] of [["missing", "index_missing"], ["invalid", "index_invalid"], [new Error("x"), "index_unverified"]] as const) {
      const { reads, calls } = scriptedReads({ stages: [[]] });
      const gate = scriptedGate(authority("7"));
      const { indexCheck } = indexChecked(answer);
      expect(await readOwnerBufferedTurns(deps({ store: store(), reads, gate, indexCheck }), SCOPE, "claude", SESSION)).toEqual({ unverifiedBeforeCandidate: name });
      expect([gate.calls.length, calls.length]).toEqual([0, 0]);
    }
    const s = store();
    const st = await putStage(s, 1, [turn(0)]);
    const gate = scriptedGate(authority("7"));
    const { indexCheck, probe } = indexChecked("valid");
    const out = await readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[st.row]] }).reads, gate, indexCheck }), SCOPE, "claude", SESSION);
    const without = await readOwnerBufferedTurns(deps({ store: s, reads: scriptedReads({ stages: [[st.row]] }).reads, gate: scriptedGate(authority("7")) }), SCOPE, "claude", SESSION);
    expect(out).not.toBeNull();
    expect(out).toEqual(without);
    expect(probe.calls).toHaveLength(1);
  });

  test("no indexCheck in the deps (flag off): no probe at all, reads exactly as before", async () => {
    const probe = scriptedIndexProbe("missing");
    const { reads } = scriptedReads({ list: [bufferedRow()] });
    const d = deps({ store: store(), reads });
    expect("indexCheck" in d).toBe(false);
    expect((await readOwnerBufferedList(d, SCOPE, { limit: 10 })).map((r) => r.session_id)).toEqual([SESSION]);
    expect(probe.calls).toHaveLength(0);
  });
});

describe("no ungated path", () => {
  test("the module exports only gated readers of buffered content", () => {
    const names = Object.keys(ownerBuffered);
    for (const hidden of ["readStagesUngated", "finalGate", "bufferedTurnRows", "redactQuotedTail", "scrubTurnRow", "scrubDeep", "sameAuthority", "specOf", "readGatedSession", "bufferedEventRows", "scrubEventRow"])
      expect(names).not.toContain(hidden);
    expect(names.filter((n) => /^read/u.test(n)).sort()).toEqual(["readOwnerBufferedEvents", "readOwnerBufferedList", "readOwnerBufferedTurns"]);
  });
  test("app.ts reaches buffered content only through the gated readers", () => {
    const app = readFileSync(join(import.meta.dir, "..", "app.ts"), "utf8");
    expect(app).not.toMatch(/readBufferedStage|readStagesUngated|\.reads\.(list|stages)\(|sqlOwnerBufferedReads/u);
    expect(app).toContain("readOwnerBufferedTurns(");
    expect(app).toContain("readOwnerBufferedList(");
    expect(app).toContain("readOwnerBufferedEvents(");
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

describe("fence subtypes for logs (fenceSubtype, BufferedFenced.sqlstateClass)", () => {
  test("FENCE_SUBTYPES is frozen and holds every reason literal the module fences with, plus the default", () => {
    const src = readFileSync(join(import.meta.dir, "..", "owner-buffered.ts"), "utf8");
    const literals = new Set([...src.matchAll(/(?:new BufferedFenced|\bfail)\("([^"]+)"/g)].map((m) => m[1]!));
    expect(literals.size).toBeGreaterThan(10);
    for (const r of literals) expect(ownerBuffered.FENCE_SUBTYPES).toContain(r);
    expect(ownerBuffered.FENCE_SUBTYPES).toContain("fenced");
    expect(Object.isFrozen(ownerBuffered.FENCE_SUBTYPES)).toBe(true);
    for (const r of ownerBuffered.FENCE_SUBTYPES) expect(ownerBuffered.fenceSubtype(r)).toBe(r);
    expect(ownerBuffered.fenceSubtype(new BufferedFenced().reason)).toBe("fenced");
  });

  test("anything outside the set is fence_other", () => {
    for (const r of ["", "gate_", "GATE_SHAPE", "gate_shape ", "fixture_timeout", `rpc_error${"x".repeat(300)}`, undefined, null, 42, {}, ["gate_shape"]])
      expect(ownerBuffered.fenceSubtype(r)).toBe("fence_other");
  });

  test("RPC failures carry only a validated two-char SQLSTATE class", async () => {
    const reason = async (err: unknown) => {
      const run = (async (_o: string, _w: string, work: (q: unknown) => Promise<unknown>) =>
        work({ async query() { throw err; } })) as never;
      try {
        await sqlOwnerBufferedReads(run).list(SCOPE, authority("7"), { limit: 1 });
      } catch (e) {
        return e;
      }
      throw new Error("expected a fence");
    };
    const cases: Array<[unknown, string, string | undefined]> = [
      [Object.assign(new Error("m"), { code: "55006" }), "rpc_fenced", "55"],
      [Object.assign(new Error("m"), { code: "57014" }), "rpc_timeout", "57"],
      [Object.assign(new Error("secret message"), { code: "42501" }), "rpc_error", "42"],
      [Object.assign(new Error("m"), { code: "x" }), "rpc_error", undefined],
      [Object.assign(new Error("m"), { code: "a1234" }), "rpc_error", undefined],
      [Object.assign(new Error("m"), { code: "ECONNRESET" }), "rpc_error", undefined],
      [Object.assign(new Error("m"), { code: "42-this-is-not-sqlstate" }), "rpc_error", undefined],
      [Object.assign(new Error("m"), { code: "42" }), "rpc_error", undefined],
      [Object.assign(new Error("m"), { code: "4250" }), "rpc_error", undefined],
      [Object.assign(new Error("m"), { code: "42501x" }), "rpc_error", undefined],
      [Object.assign(new Error("m"), { code: "22P02" }), "rpc_error", "22"],
      [Object.assign(new Error("m"), { code: 42501 }), "rpc_error", undefined],
      [new Error("connection terminated"), "rpc_error", undefined],
      ["thrown string", "rpc_error", undefined],
    ];
    for (const [err, r, cls] of cases) {
      const e = await reason(err);
      expect(e).toBeInstanceOf(BufferedFenced);
      expect((e as BufferedFenced).reason).toBe(r);
      expect((e as BufferedFenced).sqlstateClass).toBe(cls);
      expect(JSON.stringify({ ...(e as object) })).not.toContain("secret message");
    }
    expect(new BufferedFenced("gate_shape").sqlstateClass).toBeUndefined();
  });
});
