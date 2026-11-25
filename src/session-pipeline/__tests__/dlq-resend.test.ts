import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import {
  DlqResendRefusal,
  identityOfBody,
  identityOfKey,
  resolveOptions,
  runDlqResend,
  type DlqResendOptions,
  type ParsedArgs,
  type SqsClient,
  type SqsMessage,
  type SqsMessageAttribute,
} from "../dlq-resend.ts";

// Synthetic fixtures only: no production identifier anywhere.
const DLQ = "https://sqs.us-east-1.amazonaws.com/000000000000/fixture-index-dlq";
const TARGET = "https://sqs.us-east-1.amazonaws.com/000000000000/fixture-index";
const ROOT = "fixture-root";
const sha = (s: string) => createHash("sha256").update(s, "utf8").digest("hex");

interface Fixture { org: string; ws: string; m: string; h: string; s: string }
const key = (f: Fixture, rest = "manifest.json") => `${ROOT}/${f.org}/v1/ws=${f.ws}/m=${f.m}/h=${f.h}/d=2026/11/20/s=${f.s}/${rest}`;
const identity = (f: Fixture) => sha(`${f.org}/${f.ws}/${f.m}/${f.h}/${f.s}`);
/** S3 event bodies carry the key URL-encoded (`=` as %3D). */
const s3Body = (f: Fixture) =>
  JSON.stringify({ Records: [{ eventSource: "aws:s3", eventName: "ObjectCreated:Put", s3: { bucket: { name: "fixture-bucket" }, object: { key: encodeURIComponent(key(f)).replace(/%2F/g, "/"), eTag: "e" } } }] });
const reconcileBody = (f: Fixture) => JSON.stringify({ reconcile: { key: key(f), etag: "e" } });

const A: Fixture = { org: "org-alpha-fixture", ws: "ws-alpha-fixture", m: "mach-alpha", h: "claude", s: "sess-alpha-fixture-0001" };
const B: Fixture = { org: "org-bravo-fixture", ws: "ws-bravo-fixture", m: "mach-bravo", h: "codex", s: "sess-bravo-fixture-0002" };
const C: Fixture = { org: "org-charlie-fixture", ws: "ws-charlie-fixture", m: "mach-charlie", h: "claude", s: "sess-charlie-fixture-0003" };
const ORIGIN: Fixture = { org: "org-alpha-fixture", ws: "ws-alpha-fixture", m: "mach-alpha", h: "_none", s: "_origin" };

type Call = { op: "send" | "delete" | "visibility" | "receive" | "size"; queue: string; id?: string; body?: string; seconds?: number; attributes?: Record<string, SqsMessageAttribute> };

/** An in-memory DLQ with SQS visibility semantics: a received message is hidden until released, deleted or its hold ends. */
function fakeSqs(bodies: { body: string; attributes?: SqsMessage["MessageAttributes"] }[], opts: { failSend?: Set<number>; failDelete?: Set<number> } = {}) {
  const messages = bodies.map((b, i) => ({ id: `m-${i}`, body: b.body, attributes: b.attributes, visible: true, deleted: false, receives: 0 }));
  const calls: Call[] = [];
  let handle = 0;
  const handles = new Map<string, number>();
  const byHandle = (h: string) => messages[handles.get(h)!]!;
  const sqs: SqsClient = {
    async approximateSize(queue) {
      calls.push({ op: "size", queue });
      return messages.filter((m) => !m.deleted).length;
    },
    async receive(queue, { max, visibilitySeconds }) {
      calls.push({ op: "receive", queue, seconds: visibilitySeconds });
      const out: SqsMessage[] = [];
      for (const [i, m] of messages.entries()) {
        if (out.length >= max) break;
        if (m.deleted || !m.visible) continue;
        m.visible = false;
        m.receives++;
        const h = `rh-${++handle}`;
        handles.set(h, i);
        out.push({ MessageId: m.id, ReceiptHandle: h, Body: m.body, ...(m.attributes ? { MessageAttributes: m.attributes } : {}) });
      }
      return out;
    },
    async send(queue, body, attributes) {
      const index = messages.findIndex((m) => m.body === body);
      calls.push({ op: "send", queue, body, id: messages[index]?.id, ...(attributes ? { attributes } : {}) });
      if (opts.failSend?.has(index)) throw Object.assign(new Error("boom"), { code: "ServiceUnavailable" });
    },
    async delete(queue, h) {
      const m = byHandle(h);
      calls.push({ op: "delete", queue, id: m.id });
      if (opts.failDelete?.has(messages.indexOf(m))) throw Object.assign(new Error("boom"), { code: "ReceiptHandleIsInvalid" });
      m.deleted = true;
    },
    async setVisibility(queue, h, seconds) {
      const m = byHandle(h);
      calls.push({ op: "visibility", queue, id: m.id, seconds });
      if (seconds === 0) m.visible = true;
    },
  };
  return { sqs, calls, messages };
}

function options(over: Partial<DlqResendOptions> = {}): DlqResendOptions {
  return {
    dlqUrl: DLQ,
    targetUrl: TARGET,
    selection: new Map([[identity(A), "now_indexes"]]),
    max: 10,
    ratePerSecond: 5,
    execute: false,
    visibilitySeconds: 120,
    emptyReceivesToStop: 3,
    waitSeconds: 0,
    ...over,
  };
}

async function run(opts: DlqResendOptions, sqs: SqsClient) {
  const lines: string[] = [];
  let t = 0;
  const summary = await runDlqResend(opts, {
    sqs,
    emit: (r) => lines.push(JSON.stringify(r)),
    now: () => t,
    sleep: async (ms) => { t += ms; },
  });
  return { summary, lines };
}

describe("identity", () => {
  test("from an S3 event body with a URL-encoded key", () => {
    const body = s3Body(A);
    expect(body).toContain("ws%3D");
    expect(identityOfBody(body)).toEqual({ ok: true, identity: identity(A) });
  });
  test("from a reconcile body", () => {
    expect(identityOfBody(reconcileBody(B))).toEqual({ ok: true, identity: identity(B) });
  });
  test("the date and the file under the session do not change it", () => {
    expect(identityOfKey(key(A, "raw/0000000001-0-abcdef01.jsonl.zst"))).toEqual({ ok: true, identity: identity(A) });
    expect(identityOfKey(key(A).replace("d=2026/11/20", "d=2025/01/02"))).toEqual({ ok: true, identity: identity(A) });
  });
  test("the org is the segment after the key root, also under an upload generation", () => {
    const gen = `${ROOT}/${A.org}/upload-generations/g=00000000-0000-4000-8000-000000000000/v1/ws=${A.ws}/m=${A.m}/h=${A.h}/d=2026/11/20/s=${A.s}/manifest.json`;
    expect(identityOfKey(gen)).toEqual({ ok: true, identity: identity(A) });
  });
  test("the origin bucket (h=_none) has no identity", () => {
    expect(identityOfBody(s3Body(ORIGIN))).toEqual({ ok: false, reason: "origin_bucket" });
    expect(identityOfBody(reconcileBody(ORIGIN))).toEqual({ ok: false, reason: "origin_bucket" });
  });
  test("unparseable and foreign bodies", () => {
    expect(identityOfBody("not json")).toEqual({ ok: false, reason: "unparseable_body" });
    expect(identityOfBody(JSON.stringify({ Event: "s3:TestEvent" }))).toEqual({ ok: false, reason: "no_key" });
    expect(identityOfBody(JSON.stringify({ reconcile: { key: "elsewhere/file.txt" } }))).toEqual({ ok: false, reason: "not_session_key" });
    expect(identityOfKey(`v1/ws=${A.ws}/m=${A.m}/h=${A.h}/d=2026/11/20/s=${A.s}/manifest.json`)).toEqual({ ok: false, reason: "no_org" });
    const mixed = JSON.parse(s3Body(A));
    mixed.Records.push(JSON.parse(s3Body(B)).Records[0]);
    expect(identityOfBody(JSON.stringify(mixed))).toEqual({ ok: false, reason: "mixed_records" });
  });
});

describe("dry run", () => {
  test("never sends or deletes, resets visibility for every message, reports what would be resent", async () => {
    const f = fakeSqs([{ body: s3Body(A) }, { body: reconcileBody(B) }, { body: s3Body(ORIGIN) }, { body: "junk" }]);
    const { summary, lines } = await run(options({ maxScan: 1000 }), f.sqs);
    expect(f.calls.filter((c) => c.op === "send" || c.op === "delete")).toEqual([]);
    // Every receipt got visibility 0 (each received message was released).
    const received = f.messages.reduce((n, m) => n + m.receives, 0);
    const resets = f.calls.filter((c) => c.op === "visibility");
    expect(resets.length).toBe(received);
    expect(resets.every((c) => c.seconds === 0 && c.queue === DLQ)).toBe(true);
    expect(f.messages.every((m) => m.visible && !m.deleted)).toBe(true);
    expect(summary).toMatchObject({ mode: "dry_run", wouldResend: 1, resent: 0, selected: 1, unique: 4, stop: "empty_receives", perClass: { now_indexes: 1 } });
    expect(summary.skipReasons).toMatchObject({ not_selected: 1, origin_bucket: 1, unparseable_body: 1 });
    expect(lines.filter((l) => l.includes('"would_resend"'))).toEqual([
      JSON.stringify({ event: "would_resend", identity: identity(A), bodySha256: sha(s3Body(A)).slice(0, 16), class: "now_indexes" }),
    ]);
  });
});

describe("execute", () => {
  test("resends only selected messages, preserving body and attributes, sending before deleting", async () => {
    const attrs = { trace: { DataType: "String", StringValue: "t-1", StringListValues: [] } };
    const f = fakeSqs([{ body: reconcileBody(B) }, { body: s3Body(A), attributes: attrs }, { body: reconcileBody(C) }, { body: reconcileBody(A) }]);
    const sel = new Map([[identity(A), "now_indexes"], [identity(C), "other"]]);
    const { summary } = await run(options({ execute: true, selection: sel }), f.sqs);
    const sends = f.calls.filter((c) => c.op === "send");
    expect(sends.map((c) => c.id).sort()).toEqual(["m-1", "m-2", "m-3"]);
    expect(sends.every((c) => c.queue === TARGET)).toBe(true);
    expect(sends.find((c) => c.id === "m-1")).toMatchObject({ body: s3Body(A), attributes: { trace: { DataType: "String", StringValue: "t-1" } } });
    expect(sends.find((c) => c.id === "m-1")!.attributes!.trace).not.toHaveProperty("StringListValues");
    for (const id of ["m-1", "m-2", "m-3"]) {
      const sendAt = f.calls.findIndex((c) => c.op === "send" && c.id === id);
      const deleteAt = f.calls.findIndex((c) => c.op === "delete" && c.id === id);
      expect(sendAt).toBeGreaterThanOrEqual(0);
      expect(deleteAt).toBeGreaterThan(sendAt);
    }
    expect(f.calls.filter((c) => c.op === "delete").every((c) => c.queue === DLQ)).toBe(true);
    // The non-selected message is released (visibility 0) and never sent or deleted.
    expect(f.calls.filter((c) => c.id === "m-0").every((c) => c.op === "visibility" && c.seconds === 0)).toBe(true);
    expect(f.messages[0]!.deleted).toBe(false);
    expect(summary).toMatchObject({ mode: "execute", resent: 3, errors: 0, duplicateRisk: 0, perClass: { now_indexes: 2, other: 1 } });
  });

  test("a send failure never deletes and is counted", async () => {
    const f = fakeSqs([{ body: s3Body(A) }, { body: reconcileBody(A) }], { failSend: new Set([0]) });
    const { summary, lines } = await run(options({ execute: true }), f.sqs);
    expect(f.calls.some((c) => c.op === "delete" && c.id === "m-0")).toBe(false);
    expect(f.messages[0]!.deleted).toBe(false);
    expect(f.messages[1]!.deleted).toBe(true);
    expect(summary).toMatchObject({ resent: 1, errors: 1, errorCodes: { "send:ServiceUnavailable": 1 } });
    expect(lines.some((l) => l.includes('"send_failed"'))).toBe(true);
  });

  test("a delete failure after a successful send is a counted duplicate risk", async () => {
    const f = fakeSqs([{ body: s3Body(A) }], { failDelete: new Set([0]) });
    const { summary } = await run(options({ execute: true }), f.sqs);
    expect(summary).toMatchObject({ resent: 1, duplicateRisk: 1 });
    // Never resent twice in one run, even though it is still in the DLQ.
    expect(f.calls.filter((c) => c.op === "send").length).toBe(1);
  });

  test("the cap is respected and non-selected messages get visibility 0", async () => {
    const bodies = [{ body: s3Body(B) }, ...Array.from({ length: 6 }, (_, i) => ({ body: JSON.stringify({ reconcile: { key: key(A), etag: `e${i}` } }) })), { body: s3Body(C) }];
    const f = fakeSqs(bodies);
    const { summary } = await run(options({ execute: true, max: 2 }), f.sqs);
    expect(f.calls.filter((c) => c.op === "send").length).toBe(2);
    expect(summary).toMatchObject({ resent: 2, stop: "cap_reached" });
    for (const id of ["m-0", "m-7"]) {
      const own = f.calls.filter((c) => c.id === id);
      expect(own.length).toBeGreaterThan(0);
      expect(own.every((c) => c.op === "visibility" && c.seconds === 0)).toBe(true);
    }
    // Selected messages past the cap are released, not held.
    expect(summary.skipReasons.after_cap).toBe(4);
    expect(f.messages.filter((m) => !m.deleted).every((m) => m.visible)).toBe(true);
  });

  test("sends are paced by --rate", async () => {
    const f = fakeSqs(Array.from({ length: 3 }, (_, i) => ({ body: JSON.stringify({ reconcile: { key: key(A), etag: `e${i}` } }) })));
    const slept: number[] = [];
    let t = 0;
    await runDlqResend(options({ execute: true, ratePerSecond: 2 }), { sqs: f.sqs, emit: () => {}, now: () => t, sleep: async (ms) => { slept.push(ms); t += ms; } });
    expect(slept).toEqual([500, 500]);
  });

  test("a hold about to lapse is not sent", async () => {
    const f = fakeSqs([{ body: s3Body(A) }, { body: reconcileBody(A) }]);
    let t = 0;
    const summary = await runDlqResend(options({ execute: true, ratePerSecond: 0.01, visibilitySeconds: 30 }), { sqs: f.sqs, emit: () => {}, now: () => t, sleep: async (ms) => { t += ms; } });
    expect(f.calls.filter((c) => c.op === "send").length).toBe(1);
    expect(summary.errorCodes.hold_expiring).toBe(1);
  });
});

describe("stopping", () => {
  test("after K consecutive empty receives", async () => {
    const f = fakeSqs([]);
    const { summary } = await run(options({ emptyReceivesToStop: 4, maxScan: 1000 }), f.sqs);
    expect(summary.stop).toBe("empty_receives");
    expect(f.calls.filter((c) => c.op === "receive").length).toBe(4);
  });
  test("receives hide each message for visibility-seconds", async () => {
    const f = fakeSqs([{ body: s3Body(B) }]);
    await run(options({ visibilitySeconds: 77 }), f.sqs);
    expect(f.calls.filter((c) => c.op === "receive").every((c) => c.seconds === 77)).toBe(true);
  });
  test("after the max scan (default 2x the approximate size, at least 10)", async () => {
    const f = fakeSqs(Array.from({ length: 25 }, () => ({ body: s3Body(B) })));
    const { summary } = await run(options(), f.sqs);
    expect(summary.maxScan).toBe(50);
    expect(summary.scanned).toBeLessThanOrEqual(50);
    const g = fakeSqs(Array.from({ length: 25 }, () => ({ body: s3Body(B) })));
    expect((await run(options({ maxScan: 7 }), g.sqs)).summary).toMatchObject({ stop: "max_scan", scanned: 7 });
  });
});

describe("refusals", () => {
  const files: Record<string, string> = {
    "sel.txt": `# canary\n${identity(A)}\n\n`,
    "classes.json": JSON.stringify({ [identity(A)]: "now_indexes", [identity(B)]: "pointer_move_pending" }),
    "empty.txt": "",
    "bad.txt": "not-a-hash\n",
  };
  const read = (p: string) => {
    if (!(p in files)) throw new Error("missing");
    return files[p]!;
  };
  const base: ParsedArgs = { "dlq-url": DLQ, "target-url": TARGET, select: "sel.txt", max: "5" };
  const reason = (args: ParsedArgs, env: Record<string, string | undefined> = {}) => {
    try {
      resolveOptions(args, env, read);
      return "accepted";
    } catch (e) {
      if (e instanceof DlqResendRefusal) return e.reason;
      throw e;
    }
  };

  test("accepts a valid dry run and a confirmed execute", () => {
    expect(reason(base)).toBe("accepted");
    expect(reason({ ...base, execute: true }, { DLQ_RESEND_CONFIRM: "resend" })).toBe("accepted");
    const opts = resolveOptions({ ...base, classes: "classes.json" }, {}, read);
    expect([...opts.selection]).toEqual([[identity(A), "now_indexes"]]);
    expect(opts).toMatchObject({ ratePerSecond: 1, visibilitySeconds: 120, emptyReceivesToStop: 3, execute: false });
    const byClass = resolveOptions({ ...base, select: undefined, "select-class": "pointer_move_pending", classes: "classes.json" }, {}, read);
    expect([...byClass.selection.keys()]).toEqual([identity(B)]);
  });
  test("missing selection", () => {
    expect(reason({ ...base, select: undefined })).toBe("select_or_select_class_required");
    expect(reason({ ...base, select: undefined, "select-class": "x" })).toBe("select_class_needs_classes_file");
    expect(reason({ ...base, select: undefined, "select-class": "nope", classes: "classes.json" })).toBe("selection_empty");
    expect(reason({ ...base, select: "empty.txt" })).toBe("selection_empty");
    expect(reason({ ...base, select: "bad.txt" })).toBe("select_file_line_1_not_a_hash");
  });
  test("missing or excessive max", () => {
    expect(reason({ ...base, max: undefined })).toBe("max_required");
    expect(reason({ ...base, max: "101" })).toBe("max_out_of_range_1_100");
    expect(reason({ ...base, max: "0" })).toBe("max_out_of_range_1_100");
    expect(reason({ ...base, max: "100" })).toBe("accepted");
  });
  test("the same URL", () => {
    expect(reason({ ...base, "target-url": DLQ })).toBe("dlq_url_equals_target_url");
    expect(reason({ ...base, "target-url": `${DLQ.replace("https://sqs", "https://SQS")}/` })).toBe("dlq_url_equals_target_url");
  });
  test("execute without DLQ_RESEND_CONFIRM=resend", () => {
    expect(reason({ ...base, execute: true })).toBe("execute_needs_DLQ_RESEND_CONFIRM");
    expect(reason({ ...base, execute: true }, { DLQ_RESEND_CONFIRM: "yes" })).toBe("execute_needs_DLQ_RESEND_CONFIRM");
  });
  test("rate above 5", () => {
    expect(reason({ ...base, rate: "6" })).toBe("rate_out_of_range_0.01_5");
    expect(reason({ ...base, rate: "5" })).toBe("accepted");
  });
});

describe("output", () => {
  test("carries no raw key, org, workspace, machine or session id", async () => {
    const f = fakeSqs([{ body: s3Body(A) }, { body: reconcileBody(B) }, { body: s3Body(C) }, { body: s3Body(ORIGIN) }], { failSend: new Set([2]) });
    const sel = new Map([[identity(A), "now_indexes"], [identity(B), "now_indexes"], [identity(C), "now_indexes"]]);
    const { lines } = await run(options({ execute: true, selection: sel }), f.sqs);
    const text = lines.join("\n");
    expect(lines.length).toBeGreaterThan(2);
    for (const fx of [A, B, C]) {
      for (const raw of [fx.org, fx.ws, fx.m, fx.s, key(fx), encodeURIComponent(key(fx))]) expect(text).not.toContain(raw);
    }
    for (const raw of [ROOT, "fixture-bucket", "fixture-index", "000000000000"]) expect(text).not.toContain(raw);
  });
});
