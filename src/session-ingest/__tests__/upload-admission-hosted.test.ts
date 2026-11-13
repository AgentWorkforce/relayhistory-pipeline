/** The hosted upload-admission runtime (upload-admission-hosted.ts): config, secret, the S3-ledger inline admission, handler wiring. */
import { describe, expect, it } from "bun:test";
import { hostedUploadAdmissionConfig, hostedUploadAdmissionRuntime, parseUploadAdmissionSecret, UploadAdmissionRuntimeError } from "../upload-admission-hosted.ts";
import { createUploadAdmissionHandler, handler } from "../upload-admission-lambda.ts";
import { uploadDirectConfig } from "../upload-admission.ts";

// Synthetic values only. The witness key is the contract's test-only key (bytes 0x00..0x1f).
const WITNESS = Buffer.from(Array.from({ length: 32 }, (_, i) => i)).toString("base64url");
const env = {
  AWS_REGION: "us-east-1",
  AWS_ACCESS_KEY_ID: "ASIASYNTHETIC0000000",
  AWS_SECRET_ACCESS_KEY: "synthetic",
  AWS_SESSION_TOKEN: "synthetic",
  UPLOAD_ADMISSION_BUCKET: "96bulls-aws-synthetic-transcripts-111111111111",
  UPLOAD_ADMISSION_SECRET_ID: "96bulls/aws-synthetic/upload-admission",
  INGEST_KEY_ROOT: "96bulls",
  INGEST_LEDGER: "s3",
};
const code = (f: () => unknown) => { try { f(); } catch (e) { return (e as UploadAdmissionRuntimeError).code; } return "ok"; };

describe("hostedUploadAdmissionConfig", () => {
  it("reads the stack's environment", () => {
    expect(hostedUploadAdmissionConfig(env)).toEqual({ region: "us-east-1", bucket: env.UPLOAD_ADMISSION_BUCKET, keyRoot: "96bulls", secretId: env.UPLOAD_ADMISSION_SECRET_ID });
    expect(hostedUploadAdmissionConfig({ ...env, INGEST_ACCEPTANCE_MODE: "canonical", INGEST_UPLOAD_CONSENT: "off", INGEST_REPLICA_MODE: "off" }).keyRoot).toBe("96bulls");
  });

  it("refuses to start unless INGEST_LEDGER=s3 (decision 0005: never the SQL ledger)", () => {
    expect(code(() => hostedUploadAdmissionConfig({ ...env, INGEST_LEDGER: undefined }))).toBe("ingest_ledger_s3_required");
    expect(code(() => hostedUploadAdmissionConfig({ ...env, INGEST_LEDGER: "sql" }))).toBe("ingest_ledger_s3_required");
    expect(code(() => hostedUploadAdmissionConfig({ ...env, INGEST_LEDGER: "S3" }))).toBe("ingest_ledger_invalid");
  });

  it("refuses what it cannot honour without SQL, by code, never echoing a value", () => {
    for (const k of ["AWS_REGION", "UPLOAD_ADMISSION_BUCKET", "UPLOAD_ADMISSION_SECRET_ID"])
      expect(code(() => hostedUploadAdmissionConfig({ ...env, [k]: undefined }))).toBe("config_required");
    expect(code(() => hostedUploadAdmissionConfig({ ...env, UPLOAD_ADMISSION_SECRET_ID: "96bulls/x/other" }))).toBe("target_invalid");
    expect(code(() => hostedUploadAdmissionConfig({ ...env, INGEST_ACCEPTANCE_MODE: "buffered" }))).toBe("acceptance_mode_invalid");
    expect(code(() => hostedUploadAdmissionConfig({ ...env, INGEST_INLINE_MAX_SESSIONS: "0" }))).toBe("inline_max_sessions_unsupported");
    expect(code(() => hostedUploadAdmissionConfig({ ...env, INGEST_REPLICA_MODE: "required" }))).toBe("replica_unsupported");
    expect(code(() => hostedUploadAdmissionConfig({ ...env, INGEST_BABYSITTER_RETENTION_ROUTING: "on" }))).toBe("babysitter_retention_unsupported");
    expect(code(() => hostedUploadAdmissionConfig({ ...env, INGEST_UPLOAD_CONSENT: "enforce" }))).toBe("upload_consent_unsupported");
    expect(code(() => hostedUploadAdmissionConfig({ ...env, INGEST_UPLOAD_CONSENT: "maybe" }))).toBe("upload_consent_invalid");
    expect(code(() => hostedUploadAdmissionConfig({ ...env, SCRUB_ENGINE: "wasm" }))).toBe("scrub_engine_unsupported");
    const arn = "arn:aws:secretsmanager:us-east-1:111111111111:secret:96bulls/aws-synthetic/upload-admission-AbC123";
    expect(hostedUploadAdmissionConfig({ ...env, UPLOAD_ADMISSION_SECRET_ID: arn }).secretId).toBe(arn);
    try { hostedUploadAdmissionConfig({ ...env, UPLOAD_ADMISSION_BUCKET: "Leaky_Value" }); } catch (e) { expect(String(e)).not.toContain("Leaky_Value"); }
  });
});

describe("parseUploadAdmissionSecret", () => {
  it("exact keys only: the witness key (no queue token any more)", () => {
    expect(parseUploadAdmissionSecret(JSON.stringify({ upload_witness_key: WITNESS }))).toEqual({ witnessKey: WITNESS });
    for (const bad of ["", "{", "[]", JSON.stringify({}), JSON.stringify({ upload_witness_key: WITNESS, cloudflare_queue_token: "synthetic_token_0123456789" }),
      JSON.stringify({ upload_witness_key: 1 })])
      expect(code(() => parseUploadAdmissionSecret(bad))).toBe("secret_invalid");
  });
});

describe("hostedUploadAdmissionRuntime + handler", () => {
  const secret = async () => JSON.stringify({ upload_witness_key: WITNESS });
  it("builds inline deliver deps over the S3 ledger, the in-invocation admission and the secret's witness key; never fetches", async () => {
    const seen: unknown[] = [];
    let fetched = 0;
    const deps = await hostedUploadAdmissionRuntime({ secret, fetcher: (async () => { fetched++; throw new Error("no network"); }) as never, store: (c) => { seen.push(c); return {} as never; } })(env as never, uploadDirectConfig({ HISTORY_UPLOAD_DIRECT: "on" }));
    expect(deps.witnessKey).toBe(WITNESS);
    expect(deps.deliver).toMatchObject({ acceptanceMode: "canonical", target: { keyRoot: "96bulls" } });
    expect(deps.deliver.inlineMaxSessions).toBeUndefined();
    expect(deps.deliver.slots).toBeUndefined();
    expect(deps.deliver.replica).toBeUndefined();
    // The S3 ledger: no buffered stage, every SQL-only call refuses.
    await expect(deps.deliver.ledger.prepareBufferStage!({} as never)).rejects.toMatchObject({ code: "ledger_s3_unsupported_buffered" });
    expect(await deps.deliver.ledger.acquireSlot({} as never)).toEqual({ ok: true });
    expect(typeof deps.enqueue).toBe("function");
    expect(seen[0]).toMatchObject({ bucket: env.UPLOAD_ADMISSION_BUCKET, region: "us-east-1" });
    expect((seen[0] as Record<string, unknown>).serverSideEncryption).toBeUndefined();
    expect(fetched).toBe(0);
  });

  it("refuses to build with INGEST_LEDGER=sql, before reading the secret", async () => {
    let read = 0;
    const build = hostedUploadAdmissionRuntime({ secret: async () => { read++; return secret(); }, store: () => ({}) as never });
    await expect(build({ ...env, INGEST_LEDGER: "sql" } as never, uploadDirectConfig({ HISTORY_UPLOAD_DIRECT: "on" }))).rejects.toMatchObject({ code: "ingest_ledger_s3_required" });
    expect(read).toBe(0);
  });

  it("a runtime failure fails every record with its code (never a secret), and the next invocation retries the build", async () => {
    const lines: Record<string, unknown>[] = [];
    let attempts = 0;
    const h = createUploadAdmissionHandler(async () => { attempts++; throw new UploadAdmissionRuntimeError("ingest_ledger_s3_required"); }, (l) => void lines.push(l));
    const prev = process.env.HISTORY_UPLOAD_DIRECT;
    process.env.HISTORY_UPLOAD_DIRECT = "on";
    try {
      expect(await h({ Records: [{ messageId: "m1", body: "{}" }] })).toEqual({ batchItemFailures: [{ itemIdentifier: "m1" }] });
      await h({ Records: [{ messageId: "m2", body: "{}" }] });
    } finally {
      if (prev === undefined) delete process.env.HISTORY_UPLOAD_DIRECT; else process.env.HISTORY_UPLOAD_DIRECT = prev;
    }
    expect(attempts).toBe(2);
    expect(lines[0]).toEqual({ outcome: "runtime_unavailable", records: 1, cause: "ingest_ledger_s3_required" });
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
