import { describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { parseSessionKey } from "@rhc-object-store";
import { prefixOrgResolver } from "@rhc/lib/pipeline/adapter.ts";
import { AwsCallError } from "../aws-json.ts";
import { indexPointer, type PointerDeps } from "../pointer.ts";
import { parseQueueBody } from "../s3-events.ts";
import {
  classCode,
  DELETE_TIMEOUT_MS,
  DlqResendRefusal,
  identityOfBody,
  identityOfKey,
  operatorCredentials,
  parseQueueUrl,
  resolveOptions,
  runDlqResend,
  SEND_TIMEOUT_MS,
  type DlqResendOptions,
  type ParsedArgs,
  type SendAck,
  type SqsClient,
  type SqsMessage,
  type SqsMessageAttribute,
} from "../dlq-resend.ts";

// Synthetic fixtures only: no production identifier anywhere.
const ACCOUNT = "000000000000";
const DLQ = `https://sqs.us-east-1.amazonaws.com/${ACCOUNT}/fixture-index-dlq`;
const TARGET = `https://sqs.us-east-1.amazonaws.com/${ACCOUNT}/fixture-index`;
const OTHER_DLQ = `https://sqs.us-east-1.amazonaws.com/${ACCOUNT}/fixture-other-dlq`;
const arn = (url: string) => parseQueueUrl(url).arn;
const ROOT = "fixture-root";
const sha = (s: string) => createHash("sha256").update(s, "utf8").digest("hex");
const md5 = (s: string) => createHash("md5").update(s, "utf8").digest("hex");

interface Fixture { org: string; ws: string; m: string; h: string; s: string }
const key = (f: Fixture, rest = "manifest.json") => `${ROOT}/${f.org}/v1/ws=${f.ws}/m=${f.m}/h=${f.h}/d=2026/11/20/s=${f.s}/${rest}`;
const identity = (f: Fixture) => sha(`${f.org}/${f.ws}/${f.m}/${f.h}/${f.s}`);
/** S3 encodes keys like form values (`=` as %3D). */
const encodeKey = (k: string) => encodeURIComponent(k).replace(/%2F/g, "/");
const record = (k: string, over: Record<string, unknown> = {}) => ({ eventSource: "aws:s3", eventName: "ObjectCreated:Put", s3: { bucket: { name: "fixture-bucket" }, object: { key: encodeKey(k), eTag: "e" } }, ...over });
const s3Body = (f: Fixture, rest?: string) => JSON.stringify({ Records: [record(key(f, rest))] });
const reconcileBody = (f: Fixture, etag = "e") => JSON.stringify({ reconcile: { key: key(f), etag } });

const A: Fixture = { org: "org-alpha-fixture", ws: "ws-alpha-fixture", m: "mach-alpha", h: "claude", s: "sess-alpha-fixture-0001" };
const B: Fixture = { org: "org-bravo-fixture", ws: "ws-bravo-fixture", m: "mach-bravo", h: "codex", s: "sess-bravo-fixture-0002" };
const C: Fixture = { org: "org-charlie-fixture", ws: "ws-charlie-fixture", m: "mach-charlie", h: "claude", s: "sess-charlie-fixture-0003" };
const ORIGIN: Fixture = { org: "org-alpha-fixture", ws: "ws-alpha-fixture", m: "mach-alpha", h: "_none", s: "_origin" };

type Call = { op: "attributes" | "send" | "delete" | "visibility" | "receive"; queue: string; id?: string; body?: string; seconds?: number; timeoutMs?: number; at: number; attributes?: Record<string, SqsMessageAttribute> };
type SendBehaviour = "ok" | "lost" | "http500" | "http400" | "no_message_id" | "bad_md5";

interface FakeOptions {
  sendBehaviour?: (index: number) => SendBehaviour;
  failDelete?: Set<number>;
  failExtend?: boolean;
  receiveDelayMs?: number;
  sendDelayMs?: number;
  queueAttrs?: Record<string, Record<string, string>>;
}

const goodAttrs = () => ({
  [DLQ]: { QueueArn: arn(DLQ), ApproximateNumberOfMessages: "0" },
  [TARGET]: { QueueArn: arn(TARGET), RedrivePolicy: JSON.stringify({ deadLetterTargetArn: arn(DLQ), maxReceiveCount: "5" }), ApproximateNumberOfMessages: "0" },
});

/** An in-memory DLQ with SQS visibility semantics on a fake monotonic clock. */
function fakeSqs(bodies: { body: string; attributes?: SqsMessage["MessageAttributes"] }[], opts: FakeOptions = {}) {
  const clock = { t: 0 };
  const messages = bodies.map((b, i) => ({ id: `m-${i}`, body: b.body, attributes: b.attributes, visible: true, deleted: false, receives: 0 }));
  const calls: Call[] = [];
  let handle = 0;
  const handles = new Map<string, number>();
  const byHandle = (h: string) => messages[handles.get(h)!]!;
  const attrs = opts.queueAttrs ?? goodAttrs();
  const sqs: SqsClient = {
    async attributes(queue) {
      calls.push({ op: "attributes", queue, at: clock.t });
      const out = { ...(attrs[queue] ?? {}) };
      if (queue === DLQ) out.ApproximateNumberOfMessages = String(messages.filter((m) => !m.deleted).length);
      return out;
    },
    async receive(queue, { max, visibilitySeconds }) {
      calls.push({ op: "receive", queue, seconds: visibilitySeconds, at: clock.t });
      clock.t += opts.receiveDelayMs ?? 0;
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
    async send(queue, body, attributes, timeoutMs): Promise<SendAck> {
      const index = messages.findIndex((m) => m.body === body);
      calls.push({ op: "send", queue, body, id: messages[index]?.id, timeoutMs, at: clock.t, ...(attributes ? { attributes } : {}) });
      clock.t += opts.sendDelayMs ?? 0;
      switch (opts.sendBehaviour?.(index) ?? "ok") {
        case "lost": throw Object.assign(new Error("timed out"), { name: "TimeoutError" });
        case "http500": throw new AwsCallError("sqs", 500, "InternalError");
        case "http400": throw new AwsCallError("sqs", 400, "InvalidParameterValue");
        case "no_message_id": return { MD5OfMessageBody: md5(body) };
        case "bad_md5": return { MessageId: "sent-1", MD5OfMessageBody: md5(body + "x") };
        default: return { MessageId: `sent-${index}`, MD5OfMessageBody: md5(body) };
      }
    },
    async delete(queue, h, timeoutMs) {
      const m = byHandle(h);
      calls.push({ op: "delete", queue, id: m.id, timeoutMs, at: clock.t });
      if (opts.failDelete?.has(messages.indexOf(m))) throw Object.assign(new Error("timed out"), { name: "TimeoutError" });
      m.deleted = true;
    },
    async setVisibility(queue, h, seconds) {
      const m = byHandle(h);
      calls.push({ op: "visibility", queue, id: m.id, seconds, at: clock.t });
      if (seconds > 0 && opts.failExtend) throw new AwsCallError("sqs", 400, "ReceiptHandleIsInvalid");
      if (seconds === 0) m.visible = true;
    },
  };
  return { sqs, calls, messages, clock };
}

function options(over: Partial<DlqResendOptions> = {}): DlqResendOptions {
  return {
    dlq: parseQueueUrl(DLQ),
    target: parseQueueUrl(TARGET),
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

async function run(opts: DlqResendOptions, f: ReturnType<typeof fakeSqs>) {
  const lines: string[] = [];
  const summary = await runDlqResend(opts, {
    sqs: f.sqs,
    emit: (r) => lines.push(JSON.stringify(r)),
    now: () => f.clock.t,
    sleep: async (ms) => { f.clock.t += ms; },
  });
  return { summary, lines };
}

async function refusal(p: Promise<unknown>): Promise<string> {
  try {
    await p;
    return "accepted";
  } catch (e) {
    if (e instanceof DlqResendRefusal) return e.reason;
    throw e;
  }
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
  test("reconcile keys are used literally (no URL-decode)", () => {
    expect(identityOfBody(JSON.stringify({ reconcile: { key: encodeKey(key(A)) } }))).toEqual({ ok: false, reason: "not_session_key" });
  });
  test("the date does not change it", () => {
    expect(identityOfKey(key(A).replace("d=2026/11/20", "d=2025/01/02"))).toEqual({ ok: true, identity: identity(A) });
  });
  test("the origin bucket (h=_none) is skipped", () => {
    expect(identityOfBody(s3Body(ORIGIN))).toEqual({ ok: false, reason: "origin_bucket" });
    expect(identityOfBody(reconcileBody(ORIGIN))).toEqual({ ok: false, reason: "origin_bucket" });
  });
  test("TestEvent, malformed and mixed bodies are rejected", () => {
    expect(identityOfBody(JSON.stringify({ Event: "s3:TestEvent", Records: [record(key(A))] }))).toEqual({ ok: false, reason: "test_event" });
    expect(identityOfBody("not json")).toEqual({ ok: false, reason: "invalid_body" });
    expect(identityOfBody(JSON.stringify({ reconcile: { key: "" } }))).toEqual({ ok: false, reason: "invalid_body" });
    expect(identityOfBody(JSON.stringify({ Records: [] }))).toEqual({ ok: false, reason: "no_events" });
    // A record the consumer drops (wrong source, unknown event name) rejects the whole body.
    expect(identityOfBody(JSON.stringify({ Records: [record(key(A)), record(key(A), { eventSource: "aws:sns" })] }))).toEqual({ ok: false, reason: "malformed_records" });
    expect(identityOfBody(JSON.stringify({ Records: [record(key(A), { eventName: "ObjectRestore:Post" })] }))).toEqual({ ok: false, reason: "no_events" });
    expect(identityOfBody(JSON.stringify({ Records: [record(key(A)), record(key(B))] }))).toEqual({ ok: false, reason: "mixed_records" });
    expect(identityOfBody(JSON.stringify({ Records: [record(key(A)), record(key(A).replace("d=2026/11/20", "d=2026/11/21"))] }))).toEqual({ ok: true, identity: identity(A) });
  });
});

describe("selector parity with the consumer", () => {
  /** What the consumer would do with a body: parseQueueBody, then indexPointer's pre-lookup ignores. */
  async function consumerView(body: string): Promise<{ ignored: string } | { identity: string }[]> {
    const parsed = parseQueueBody(body);
    if (parsed.kind !== "events") return { ignored: parsed.kind };
    const out: { identity: string }[] = [];
    for (const event of parsed.events) {
      let org: string | null = null;
      const deps = {
        skipOriginBucket: true,
        reader: { bucket: undefined },
        orgs: { kind: "probe", orgFor: async (loc: { workspaceId: string; prefix?: string }) => { org = await prefixOrgResolver.orgFor(loc); return null; } },
      } as unknown as PointerDeps;
      const outcome = await indexPointer(event, deps);
      if (outcome.status !== "ignored" || outcome.reason !== "org_unresolved" || !org) return { ignored: outcome.status === "ignored" ? outcome.reason : "processed" };
      const loc = parseSessionKey(event.key)!;
      out.push({ identity: sha(`${org}/${loc.workspaceId}/${loc.machineId}/${loc.harness}/${loc.sessionId}`) });
    }
    return out;
  }
  const fixtures: [string, string][] = [
    ["s3 put", s3Body(A)],
    ["s3 delete", JSON.stringify({ Records: [record(key(B), { eventName: "ObjectRemoved:Delete" })] })],
    ["s3 key with + for space", JSON.stringify({ Records: [{ ...record(""), s3: { object: { key: encodeKey(key({ ...C, s: "sess c" })).replace(/%20/g, "+") } } }] })],
    ["reconcile", reconcileBody(C)],
    ["reconcile, literal encoded key", JSON.stringify({ reconcile: { key: encodeKey(key(A)) } })],
    ["origin bucket", s3Body(ORIGIN)],
    ["retention bucket", s3Body({ ...A, h: "_babysitter" })],
    ["raw chunk", s3Body(A, "raw/0000000001-0-abcdef01.jsonl.zst")],
    ["upload generation hint", JSON.stringify({ reconcile: { key: `${ROOT}/${A.org}/upload-generations/g=00000000-0000-4000-8000-000000000000/v1/ws=${A.ws}/m=${A.m}/h=${A.h}/d=2026/11/20/s=${A.s}/manifest.json` } })],
    ["no org prefix", JSON.stringify({ reconcile: { key: `v1/ws=${A.ws}/m=${A.m}/h=${A.h}/d=2026/11/20/s=${A.s}/manifest.json` } })],
    ["not a session key", JSON.stringify({ reconcile: { key: "elsewhere/file.txt" } })],
    ["test event", JSON.stringify({ Event: "s3:TestEvent" })],
    ["invalid", "{"],
  ];
  for (const [name, body] of fixtures) {
    test(`agree: ${name}`, async () => {
      const consumer = await consumerView(body);
      const selector = identityOfBody(body);
      if (Array.isArray(consumer)) {
        expect(consumer.length).toBe(1);
        expect(selector).toEqual({ ok: true, identity: consumer[0]!.identity });
      } else {
        expect(selector.ok).toBe(false);
      }
    });
  }
});

describe("queue identity, before any receive", () => {
  const noReceive = (f: ReturnType<typeof fakeSqs>) => expect(f.calls.some((c) => c.op === "receive")).toBe(false);
  test("an alias, VPC, custom or unknown host is refused without a fallback region", () => {
    for (const url of [
      `https://us-east-1.queue.amazonaws.com/${ACCOUNT}/fixture-index-dlq`,
      `https://vpce-0.sqs.us-east-1.vpce.amazonaws.com/${ACCOUNT}/fixture-index-dlq`,
      `https://sqs.example.com/${ACCOUNT}/fixture-index-dlq`,
      `https://sqs.cn-north-1.amazonaws.com.cn/${ACCOUNT}/fixture-index-dlq`,
      `http://sqs.us-east-1.amazonaws.com/${ACCOUNT}/fixture-index-dlq`,
    ]) {
      expect(() => parseQueueUrl(url)).toThrow(DlqResendRefusal);
      try { parseQueueUrl(url); } catch (e) { expect((e as DlqResendRefusal).reason).toMatch(/^queue_(host_unsupported|url_invalid)$/); }
    }
    expect(() => parseQueueUrl(`https://sqs.us-east-1.amazonaws.com/123/x`)).toThrow(DlqResendRefusal);
    expect(parseQueueUrl(DLQ)).toMatchObject({ region: "us-east-1", account: ACCOUNT, name: "fixture-index-dlq", arn: `arn:aws:sqs:us-east-1:${ACCOUNT}:fixture-index-dlq` });
  });
  test("a QueueArn that does not match the URL (alias resolution) is refused", async () => {
    const attrs = goodAttrs();
    attrs[DLQ].QueueArn = arn(OTHER_DLQ);
    const f = fakeSqs([{ body: s3Body(A) }], { queueAttrs: attrs });
    expect(await refusal(run(options(), f))).toBe("dlq_arn_does_not_match_url");
    noReceive(f);
  });
  test("a target that dead-letters elsewhere (wrong pair) is refused", async () => {
    const attrs = goodAttrs();
    attrs[TARGET].RedrivePolicy = JSON.stringify({ deadLetterTargetArn: arn(OTHER_DLQ), maxReceiveCount: "5" });
    const f = fakeSqs([{ body: s3Body(A) }], { queueAttrs: attrs });
    expect(await refusal(run(options(), f))).toBe("target_redrive_is_not_this_dlq");
    noReceive(f);
  });
  test("a target without a redrive policy is refused", async () => {
    const attrs = goodAttrs();
    delete (attrs[TARGET] as Record<string, string>).RedrivePolicy;
    const f = fakeSqs([{ body: s3Body(A) }], { queueAttrs: attrs });
    expect(await refusal(run(options(), f))).toBe("target_has_no_redrive_policy");
    noReceive(f);
  });
  test("a DLQ with an onward redrive policy is refused (dry run included)", async () => {
    const attrs = goodAttrs();
    (attrs[DLQ] as Record<string, string>).RedrivePolicy = JSON.stringify({ deadLetterTargetArn: arn(OTHER_DLQ), maxReceiveCount: "3" });
    const f = fakeSqs([{ body: s3Body(A) }], { queueAttrs: attrs });
    expect(await refusal(run(options({ execute: false }), f))).toBe("dlq_has_redrive_policy");
    noReceive(f);
  });
  test("the same queue under two spellings is refused", () => {
    expect(() => resolveOptions({ "dlq-url": DLQ, "target-url": `${DLQ}/`, select: "x", max: "1" }, {}, () => "")).toThrow("dlq_url_equals_target_url");
  });
});

describe("dry run", () => {
  test("never sends or deletes, resets visibility for every message, reports what would be resent", async () => {
    const f = fakeSqs([{ body: s3Body(A) }, { body: reconcileBody(B) }, { body: s3Body(ORIGIN) }, { body: "junk" }]);
    const { summary, lines } = await run(options({ maxScan: 1000 }), f);
    expect(f.calls.filter((c) => c.op === "send" || c.op === "delete")).toEqual([]);
    const received = f.messages.reduce((n, m) => n + m.receives, 0);
    const resets = f.calls.filter((c) => c.op === "visibility");
    expect(resets.length).toBe(received);
    expect(resets.every((c) => c.seconds === 0 && c.queue === DLQ)).toBe(true);
    expect(f.messages.every((m) => m.visible && !m.deleted)).toBe(true);
    expect(summary).toMatchObject({ mode: "dry_run", wouldResend: 1, resent: 0, selected: 1, unique: 4, stop: "empty_receives", perClass: { [classCode("now_indexes")]: 1 } });
    expect(summary.skipReasons).toMatchObject({ not_selected: 1, origin_bucket: 1, invalid_body: 1 });
    expect(lines.filter((l) => l.includes('"would_resend"'))).toEqual([
      JSON.stringify({ event: "would_resend", identity: identity(A), bodySha256: sha(s3Body(A)).slice(0, 16), class: classCode("now_indexes") }),
    ]);
  });
});

describe("execute", () => {
  test("resends only selected messages with a valid ack, preserving body and attributes, sending before deleting", async () => {
    const attrs = { trace: { DataType: "String", StringValue: "t-1", StringListValues: [] } };
    const f = fakeSqs([{ body: reconcileBody(B) }, { body: s3Body(A), attributes: attrs }, { body: reconcileBody(C) }, { body: reconcileBody(A) }]);
    const sel = new Map([[identity(A), "now_indexes"], [identity(C), "other"]]);
    const { summary } = await run(options({ execute: true, selection: sel }), f);
    const sends = f.calls.filter((c) => c.op === "send");
    expect(sends.map((c) => c.id).sort()).toEqual(["m-1", "m-2", "m-3"]);
    expect(sends.every((c) => c.queue === TARGET && c.timeoutMs === SEND_TIMEOUT_MS)).toBe(true);
    expect(sends.find((c) => c.id === "m-1")).toMatchObject({ body: s3Body(A), attributes: { trace: { DataType: "String", StringValue: "t-1" } } });
    expect(sends.find((c) => c.id === "m-1")!.attributes!.trace).not.toHaveProperty("StringListValues");
    for (const id of ["m-1", "m-2", "m-3"]) {
      const sendAt = f.calls.findIndex((c) => c.op === "send" && c.id === id);
      const deleteAt = f.calls.findIndex((c) => c.op === "delete" && c.id === id);
      expect(sendAt).toBeGreaterThanOrEqual(0);
      expect(deleteAt).toBeGreaterThan(sendAt);
    }
    expect(f.calls.filter((c) => c.op === "delete").every((c) => c.queue === DLQ && c.timeoutMs === DELETE_TIMEOUT_MS)).toBe(true);
    expect(f.calls.filter((c) => c.id === "m-0").every((c) => c.op === "visibility" && c.seconds === 0)).toBe(true);
    expect(f.messages[0]!.deleted).toBe(false);
    expect(summary).toMatchObject({ mode: "execute", resent: 3, errors: 0, duplicateRisk: 0, perClass: { [classCode("now_indexes")]: 2, [classCode("other")]: 1 } });
  });

  test("a refused send (HTTP 4xx) never deletes, is counted, and the run continues", async () => {
    const f = fakeSqs([{ body: s3Body(A) }, { body: reconcileBody(A) }], { sendBehaviour: (i) => (i === 0 ? "http400" : "ok") });
    const { summary, lines } = await run(options({ execute: true }), f);
    expect(f.calls.some((c) => c.op === "delete" && c.id === "m-0")).toBe(false);
    expect(f.messages[1]!.deleted).toBe(true);
    expect(summary).toMatchObject({ resent: 1, errors: 1, duplicateRisk: 0, errorCodes: { "send:InvalidParameterValue": 1 } });
    expect(lines.some((l) => l.includes('"send_failed"'))).toBe(true);
  });

  const ambiguousCases: [string, SendBehaviour, string][] = [
    ["accepted but the response was lost (timeout)", "lost", "send_unconfirmed:TimeoutError"],
    ["a server error that may have committed", "http500", "send_unconfirmed:InternalError"],
    ["a success without a MessageId", "no_message_id", "send_ack_invalid"],
    ["a success whose MD5 does not match the body", "bad_md5", "send_ack_invalid"],
  ];
  for (const [name, behaviour, outcome] of ambiguousCases) {
    test(`ambiguous send: ${name} -> duplicate risk, no delete, halt, rest of batch released`, async () => {
      const bodies = [{ body: reconcileBody(A, "e0") }, { body: reconcileBody(A, "e1") }, { body: s3Body(B) }, { body: reconcileBody(A, "e2") }];
      const f = fakeSqs(bodies, { sendBehaviour: (i) => (i === 0 ? behaviour : "ok") });
      const { summary, lines } = await run(options({ execute: true }), f);
      expect(f.calls.filter((c) => c.op === "send").map((c) => c.id)).toEqual(["m-0"]);
      expect(f.calls.filter((c) => c.op === "delete")).toEqual([]);
      expect(summary).toMatchObject({ stop: "halted_ambiguous_send", duplicateRisk: 1, resent: 0, skipReasons: { halted: 3 } });
      expect(summary.reconcile).toEqual([{ identity: identity(A), bodySha256: sha(bodies[0]!.body).slice(0, 16), class: classCode("now_indexes"), outcome }]);
      // The untouched remainder goes back at visibility 0; the ambiguous one stays held in the DLQ.
      for (const id of ["m-1", "m-2", "m-3"]) expect(f.calls.filter((c) => c.id === id && c.op === "visibility").map((c) => c.seconds)).toEqual([0]);
      expect(f.messages[0]).toMatchObject({ deleted: false, visible: false });
      expect(f.calls.filter((c) => c.op === "receive").length).toBe(1);
      expect(lines.some((l) => l.includes('"duplicate_risk"'))).toBe(true);
    });
  }

  test("a delete failure after a valid send is a duplicate risk and halts", async () => {
    const f = fakeSqs([{ body: reconcileBody(A, "e0") }, { body: reconcileBody(A, "e1") }, { body: s3Body(C) }], { failDelete: new Set([0]) });
    const { summary } = await run(options({ execute: true }), f);
    expect(summary).toMatchObject({ stop: "halted_delete_failure", resent: 1, duplicateRisk: 1, skipReasons: { halted: 2 } });
    expect(f.calls.filter((c) => c.op === "send").length).toBe(1);
    expect(summary.reconcile[0]!.outcome).toBe("delete_unconfirmed:TimeoutError");
    for (const id of ["m-1", "m-2"]) expect(f.calls.filter((c) => c.id === id).map((c) => [c.op, c.seconds])).toEqual([["visibility", 0]]);
  });

  test("a delayed receive leaves too little hold: the hold is extended before the send", async () => {
    // The receive itself takes 100 s of the 120 s hold (deadline measured from before the call).
    const f = fakeSqs([{ body: s3Body(A) }], { receiveDelayMs: 100_000 });
    const { summary } = await run(options({ execute: true }), f);
    const own = f.calls.filter((c) => c.id === "m-0").map((c) => [c.op, c.seconds ?? null]);
    expect(own).toEqual([["visibility", 120], ["send", null], ["delete", null]]);
    expect(summary).toMatchObject({ resent: 1, holdExtended: 1, errors: 0 });
  });

  test("a send delayed by pacing gets the hold extended; one with ample hold does not", async () => {
    const f = fakeSqs([{ body: reconcileBody(A, "e0") }, { body: reconcileBody(A, "e1") }]);
    const { summary } = await run(options({ execute: true, ratePerSecond: 0.01 }), f);
    // The first send starts at t=0 (120 s left); the second waits 100 s (20 s left < send+delete+margin).
    expect(f.calls.filter((c) => c.op === "visibility" && c.seconds! > 0).map((c) => c.id)).toEqual(["m-1"]);
    const extendAt = f.calls.findIndex((c) => c.op === "visibility" && c.id === "m-1");
    expect(f.calls.findIndex((c) => c.op === "send" && c.id === "m-1")).toBeGreaterThan(extendAt);
    expect(summary).toMatchObject({ resent: 2, holdExtended: 1 });
  });

  test("a failed extension skips the send (left held in the DLQ)", async () => {
    const f = fakeSqs([{ body: s3Body(A) }], { receiveDelayMs: 100_000, failExtend: true });
    const { summary } = await run(options({ execute: true }), f);
    expect(f.calls.some((c) => c.op === "send")).toBe(false);
    expect(summary).toMatchObject({ resent: 0, errors: 1, duplicateRisk: 0 });
  });

  test("the cap is respected and non-selected messages get visibility 0", async () => {
    const bodies = [{ body: s3Body(B) }, ...Array.from({ length: 6 }, (_, i) => ({ body: reconcileBody(A, `e${i}`) })), { body: s3Body(C) }];
    const f = fakeSqs(bodies);
    const { summary } = await run(options({ execute: true, max: 2 }), f);
    expect(f.calls.filter((c) => c.op === "send").length).toBe(2);
    expect(summary).toMatchObject({ resent: 2, stop: "cap_reached" });
    for (const id of ["m-0", "m-7"]) {
      const own = f.calls.filter((c) => c.id === id);
      expect(own.length).toBeGreaterThan(0);
      expect(own.every((c) => c.op === "visibility" && c.seconds === 0)).toBe(true);
    }
    expect(summary.skipReasons.after_cap).toBe(4);
    expect(f.messages.filter((m) => !m.deleted).every((m) => m.visible)).toBe(true);
  });

  test("sends are paced by --rate", async () => {
    const f = fakeSqs(Array.from({ length: 3 }, (_, i) => ({ body: reconcileBody(A, `e${i}`) })));
    await run(options({ execute: true, ratePerSecond: 2 }), f);
    expect(f.calls.filter((c) => c.op === "send").map((c) => c.at)).toEqual([0, 500, 1000]);
  });
});

describe("stopping", () => {
  test("after K consecutive empty receives", async () => {
    const f = fakeSqs([]);
    const { summary } = await run(options({ emptyReceivesToStop: 4, maxScan: 1000 }), f);
    expect(summary.stop).toBe("empty_receives");
    expect(f.calls.filter((c) => c.op === "receive").length).toBe(4);
  });
  test("receives hide each message for visibility-seconds", async () => {
    const f = fakeSqs([{ body: s3Body(B) }]);
    await run(options({ visibilitySeconds: 77 }), f);
    expect(f.calls.filter((c) => c.op === "receive").every((c) => c.seconds === 77)).toBe(true);
  });
  test("after the max scan (default 2x the approximate size, at least 10)", async () => {
    const f = fakeSqs(Array.from({ length: 25 }, () => ({ body: s3Body(B) })));
    const { summary } = await run(options(), f);
    expect(summary.maxScan).toBe(50);
    expect(summary.scanned).toBeLessThanOrEqual(50);
    const g = fakeSqs(Array.from({ length: 25 }, () => ({ body: s3Body(B) })));
    expect((await run(options({ maxScan: 7 }), g)).summary).toMatchObject({ stop: "max_scan", scanned: 7 });
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
  });
  test("queues in different accounts or regions", () => {
    expect(reason({ ...base, "target-url": TARGET.replace("us-east-1", "us-west-2") })).toBe("queues_not_in_one_account_and_region");
  });
  test("execute without DLQ_RESEND_CONFIRM=resend", () => {
    expect(reason({ ...base, execute: true })).toBe("execute_needs_DLQ_RESEND_CONFIRM");
    expect(reason({ ...base, execute: true }, { DLQ_RESEND_CONFIRM: "yes" })).toBe("execute_needs_DLQ_RESEND_CONFIRM");
  });
  test("rate above 5", () => {
    expect(reason({ ...base, rate: "6" })).toBe("rate_out_of_range_0.01_5");
    expect(reason({ ...base, rate: "5" })).toBe("accepted");
  });
  test("a hold too short for a bounded send and delete", () => {
    expect(reason({ ...base, "visibility-seconds": "30" })).toBe("visibility_seconds_out_of_range_60_43200");
  });
});

describe("credentials", () => {
  const full = { AWS_ACCESS_KEY_ID: "AKIDFIXTURE", AWS_SECRET_ACCESS_KEY: "secret-fixture", AWS_SESSION_TOKEN: "token-fixture" };
  const never = () => { throw new Error("profile must not be read"); };
  test("a complete environment session is used as is", () => {
    expect(operatorCredentials({ ...full, AWS_PROFILE: "p" }, never)).toEqual({ accessKeyId: "AKIDFIXTURE", secretAccessKey: "secret-fixture", sessionToken: "token-fixture" });
  });
  test("an incomplete environment is refused, never falling through to the profile", () => {
    for (const drop of Object.keys(full)) {
      const env: Record<string, string | undefined> = { ...full, AWS_PROFILE: "p" };
      delete env[drop];
      expect(() => operatorCredentials(env, never)).toThrow("env_credentials_incomplete");
    }
    expect(() => operatorCredentials({ AWS_SESSION_TOKEN: "t", AWS_PROFILE: "p" }, never)).toThrow("env_credentials_incomplete");
  });
  test("no environment keys: the profile is exported into memory", () => {
    const seen: string[] = [];
    const creds = operatorCredentials({ AWS_PROFILE: "fixture-profile" }, (p) => { seen.push(p); return JSON.stringify({ Version: 1, AccessKeyId: "AKIDP", SecretAccessKey: "sp", SessionToken: "tp" }); });
    expect(seen).toEqual(["fixture-profile"]);
    expect(creds).toEqual({ accessKeyId: "AKIDP", secretAccessKey: "sp", sessionToken: "tp" });
    expect(() => operatorCredentials({}, never)).toThrow("no_aws_credentials");
    expect(() => operatorCredentials({ AWS_PROFILE: "p" }, () => "{}")).toThrow("profile_credentials_incomplete");
  });
});

describe("output", () => {
  test("carries no raw key, org, workspace, machine or session id, queue identifier or class name", async () => {
    const f = fakeSqs([{ body: s3Body(A) }, { body: reconcileBody(B) }, { body: s3Body(C) }, { body: s3Body(ORIGIN) }], { sendBehaviour: (i) => (i === 2 ? "http400" : "ok") });
    const sel = new Map([[identity(A), "now_indexes"], [identity(B), "pointer_move_pending"], [identity(C), "now_indexes"]]);
    const { lines } = await run(options({ execute: true, selection: sel }), f);
    const text = lines.join("\n");
    expect(lines.length).toBeGreaterThan(2);
    for (const fx of [A, B, C]) {
      for (const raw of [fx.org, fx.ws, fx.m, fx.s, key(fx), encodeKey(key(fx))]) expect(text).not.toContain(raw);
    }
    for (const raw of [ROOT, "fixture-bucket", "fixture-index", ACCOUNT, "now_indexes", "pointer_move_pending"]) expect(text).not.toContain(raw);
  });
});
