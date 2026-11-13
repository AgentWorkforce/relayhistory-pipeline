/** The hosted upload-admission runtime (upload-admission-hosted.ts): config, secret, Cloudflare queue send, handler wiring. */
import { describe, expect, it } from "bun:test";
import { cloudflareQueueSender, hostedUploadAdmissionConfig, hostedUploadAdmissionRuntime, parseUploadAdmissionSecret, UploadAdmissionRuntimeError } from "../upload-admission-hosted.ts";
import { createUploadAdmissionHandler, handler } from "../upload-admission-lambda.ts";
import { uploadDirectConfig } from "../upload-admission.ts";

// Synthetic values only. The witness key is the contract's test-only key (bytes 0x00..0x1f).
const ACCT = "0123456789abcdef0123456789abcdef", QUEUE = "fedcba9876543210fedcba9876543210";
const TOKEN = "synthetic_token_0123456789abcdefghijklmn";
const WITNESS = Buffer.from(Array.from({ length: 32 }, (_, i) => i)).toString("base64url");
const env = {
  AWS_REGION: "us-east-1",
  AWS_ACCESS_KEY_ID: "ASIASYNTHETIC0000000",
  AWS_SECRET_ACCESS_KEY: "synthetic",
  AWS_SESSION_TOKEN: "synthetic",
  UPLOAD_ADMISSION_BUCKET: "96bulls-aws-synthetic-transcripts-111111111111",
  UPLOAD_ADMISSION_SECRET_ID: "96bulls/aws-synthetic/upload-admission",
  UPLOAD_ADMISSION_CF_ACCOUNT_ID: ACCT,
  UPLOAD_ADMISSION_CF_QUEUE_ID: QUEUE,
  INGEST_KEY_ROOT: "96bulls",
  INGEST_ACCEPTANCE_MODE: "buffered",
};
const code = (f: () => unknown) => { try { f(); } catch (e) { return (e as UploadAdmissionRuntimeError).code; } return "ok"; };

describe("hostedUploadAdmissionConfig", () => {
  it("reads the stack's environment, with Worker-parity buffered deliver deps", () => {
    const c = hostedUploadAdmissionConfig(env);
    expect(c).toMatchObject({ region: "us-east-1", bucket: env.UPLOAD_ADMISSION_BUCKET, keyRoot: "96bulls", secretId: env.UPLOAD_ADMISSION_SECRET_ID, cloudflareAccountId: ACCT, cloudflareQueueId: QUEUE });
    expect(c.deliver).toEqual({ acceptanceMode: "buffered", inlineMaxSessions: 0 });
    const g = "a".repeat(64);
    expect(hostedUploadAdmissionConfig({ ...env, INGEST_BABYSITTER_RETENTION_ROUTING: "on", INGEST_SUBREQUEST_CAP: "1000", INGEST_SUBREQUEST_REDIRECTS: "3", INGEST_EXPECT_GENERATION: g }).deliver)
      .toEqual({ acceptanceMode: "buffered", inlineMaxSessions: 0, babysitterRetention: true, subrequestCap: 1000, subrequestRedirects: 3, retentionGeneration: g });
  });

  it("refuses by code, never echoing a value", () => {
    for (const k of ["AWS_REGION", "UPLOAD_ADMISSION_BUCKET", "UPLOAD_ADMISSION_SECRET_ID", "UPLOAD_ADMISSION_CF_ACCOUNT_ID", "UPLOAD_ADMISSION_CF_QUEUE_ID"])
      expect(code(() => hostedUploadAdmissionConfig({ ...env, [k]: undefined }))).toBe("config_required");
    expect(code(() => hostedUploadAdmissionConfig({ ...env, UPLOAD_ADMISSION_SECRET_ID: "96bulls/x/other" }))).toBe("target_invalid");
    expect(code(() => hostedUploadAdmissionConfig({ ...env, UPLOAD_ADMISSION_CF_QUEUE_ID: "96bulls-session-ingest-admission" }))).toBe("queue_target_invalid");
    expect(code(() => hostedUploadAdmissionConfig({ ...env, INGEST_ACCEPTANCE_MODE: "canonical" }))).toBe("acceptance_mode_invalid");
    expect(code(() => hostedUploadAdmissionConfig({ ...env, SCRUB_ENGINE: "wasm" }))).toBe("scrub_engine_unsupported");
    expect(code(() => hostedUploadAdmissionConfig({ ...env, INGEST_SUBREQUEST_CAP: "0" }))).toBe("worker_parity_invalid");
    const arn = "arn:aws:secretsmanager:us-east-1:111111111111:secret:96bulls/aws-synthetic/upload-admission-AbC123";
    expect(hostedUploadAdmissionConfig({ ...env, UPLOAD_ADMISSION_SECRET_ID: arn }).secretId).toBe(arn);
    try { hostedUploadAdmissionConfig({ ...env, UPLOAD_ADMISSION_CF_QUEUE_ID: "leaky-value" }); } catch (e) { expect(String(e)).not.toContain("leaky-value"); }
  });
});

describe("parseUploadAdmissionSecret", () => {
  it("exact keys only", () => {
    expect(parseUploadAdmissionSecret(JSON.stringify({ upload_witness_key: WITNESS, cloudflare_queue_token: TOKEN }))).toEqual({ witnessKey: WITNESS, queueToken: TOKEN });
    for (const bad of ["", "{", "[]", JSON.stringify({ upload_witness_key: WITNESS }), JSON.stringify({ upload_witness_key: WITNESS, cloudflare_queue_token: TOKEN, extra: 1 }),
      JSON.stringify({ upload_witness_key: WITNESS, cloudflare_queue_token: "bad token" }), JSON.stringify({ upload_witness_key: 1, cloudflare_queue_token: TOKEN })])
      expect(code(() => parseUploadAdmissionSecret(bad))).toBe("secret_invalid");
  });
});

describe("cloudflareQueueSender", () => {
  const message = { v: 1 as const, kind: "ingest-admission" as const, key: "96bulls/.admission-inbox/org-a/v1/ws=w1/m=srv_x/r=y/a=1.ndjson.zst" };
  it("posts one json message to exactly the admission queue with the bearer token", async () => {
    const calls: { url: string; init: RequestInit }[] = [];
    const send = cloudflareQueueSender({ accountId: ACCT, queueId: QUEUE, token: TOKEN, fetcher: (async (url: string, init: RequestInit) => { calls.push({ url, init }); return Response.json({ success: true, errors: [], result: null }); }) as never });
    await send(message);
    expect(calls).toHaveLength(1);
    expect(calls[0]!.url).toBe(`https://api.cloudflare.com/client/v4/accounts/${ACCT}/queues/${QUEUE}/messages`);
    expect(calls[0]!.init.method).toBe("POST");
    expect((calls[0]!.init.headers as Record<string, string>).authorization).toBe(`Bearer ${TOKEN}`);
    expect(JSON.parse(String(calls[0]!.init.body))).toEqual({ body: message, content_type: "json" });
  });

  it("throws a coded error (so promoteUpload retries) on a non-2xx, success:false, or a network failure", async () => {
    const run = async (f: () => Promise<Response>) => {
      try { await cloudflareQueueSender({ accountId: ACCT, queueId: QUEUE, token: TOKEN, fetcher: f as never })(message); return "ok"; } catch (e) { return (e as UploadAdmissionRuntimeError).code; }
    };
    expect(await run(async () => new Response("nope", { status: 403 }))).toBe("admission_queue_http_403");
    expect(await run(async () => Response.json({ success: false }))).toBe("admission_queue_http_200");
    expect(await run(async () => { throw new Error("down"); })).toBe("admission_queue_unreachable");
  });
});

describe("hostedUploadAdmissionRuntime + handler", () => {
  const secret = async () => JSON.stringify({ upload_witness_key: WITNESS, cloudflare_queue_token: TOKEN });
  it("builds deps with the store, Worker-parity deliver deps, the queue send and the secret's witness key", async () => {
    const seen: unknown[] = [];
    const deps = await hostedUploadAdmissionRuntime({ secret, store: (c) => { seen.push(c); return {} as never; } })(env as never, uploadDirectConfig({ HISTORY_UPLOAD_DIRECT: "on" }));
    expect(deps.witnessKey).toBe(WITNESS);
    expect(deps.deliver).toMatchObject({ acceptanceMode: "buffered", inlineMaxSessions: 0, target: { keyRoot: "96bulls" } });
    expect(typeof deps.enqueue).toBe("function");
    // No SSE header on PUTs: the bucket default (the transcripts CMK) applies.
    expect(seen[0]).toMatchObject({ bucket: env.UPLOAD_ADMISSION_BUCKET, region: "us-east-1" });
    expect((seen[0] as Record<string, unknown>).serverSideEncryption).toBeUndefined();
  });

  it("a runtime failure fails every record with its code (never a secret), and the next invocation retries the build", async () => {
    const lines: Record<string, unknown>[] = [];
    let attempts = 0;
    const h = createUploadAdmissionHandler(async () => { attempts++; throw new UploadAdmissionRuntimeError("secret_invalid"); }, (l) => void lines.push(l));
    const prev = process.env.HISTORY_UPLOAD_DIRECT;
    process.env.HISTORY_UPLOAD_DIRECT = "on";
    try {
      expect(await h({ Records: [{ messageId: "m1", body: "{}" }] })).toEqual({ batchItemFailures: [{ itemIdentifier: "m1" }] });
      await h({ Records: [{ messageId: "m2", body: "{}" }] });
    } finally {
      if (prev === undefined) delete process.env.HISTORY_UPLOAD_DIRECT; else process.env.HISTORY_UPLOAD_DIRECT = prev;
    }
    expect(attempts).toBe(2);
    expect(lines[0]).toEqual({ outcome: "runtime_unavailable", records: 1, cause: "secret_invalid" });
  });

  it("the deployed handler with the flag off reads nothing (no secret, no store)", async () => {
    const prev = process.env.HISTORY_UPLOAD_DIRECT;
    delete process.env.HISTORY_UPLOAD_DIRECT;
    try {
      expect(await handler({ Records: [{ messageId: "m1", body: "{}" }] })).toEqual({ batchItemFailures: [{ itemIdentifier: "m1" }] });
    } finally {
      if (prev !== undefined) process.env.HISTORY_UPLOAD_DIRECT = prev;
    }
  });
});
