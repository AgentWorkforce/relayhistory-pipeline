/**
 * AWS plumbing for the benchmark client, on the same SigV4 code and object-store package ingest-edge uses.
 * No SDK: the point is to measure the production client stack. Nothing here reads or writes a credential
 * file; credentials come from EKS Pod Identity (or the process environment for a local dry run) and stay
 * in memory.
 */
import { cachedProvider } from "../../packages/object-store/src/core.ts";
import { sha256Hex } from "../../packages/object-store/src/crypto.ts";
import { createS3Store } from "../../packages/object-store/src/s3.ts";
import { signRequest, type AwsCredentials } from "../../packages/object-store/src/sigv4.ts";
import type { ObjectStore } from "../../packages/object-store/src/types.ts";
import { xmlText } from "../../packages/object-store/src/xml.ts";
import { podIdentityCredentials } from "../../src/shared/locker.ts";
import { Tally } from "./stats.ts";

export type CredentialProvider = () => Promise<AwsCredentials>;

/** Tagged so {@link errorClass} can report a closed class instead of a message. */
export class BenchError extends Error {
  constructor(
    readonly errorClass: string,
    message: string,
  ) {
    super(`ledger-bench: ${message}`);
    this.name = "BenchError";
  }
}

/** Thrown by {@link RequestMeter} once the run's request budget is spent: the hard cost cap. */
export class BudgetExhausted extends BenchError {
  constructor(limit: number) {
    super("budget_exhausted", `the request budget of ${limit} is spent; stopping`);
  }
}

/**
 * Counts every HTTP request the run makes and refuses to make more than `limit`. The limit is what bounds
 * the run's cost (cost.ts turns it into dollars), independently of how fast the targets answer.
 */
export class RequestMeter {
  #used = 0;
  #bytes = 0;
  readonly statuses = new Tally();
  /** `byteLimit` bounds what is priced per byte: NAT processing, cross-region transfer, S3 Express per-GiB charges. */
  constructor(
    readonly limit: number,
    readonly byteLimit: number = Number.MAX_SAFE_INTEGER,
  ) {
    if (!Number.isSafeInteger(limit) || limit < 1) throw new Error("ledger-bench: the request budget must be a positive integer");
    if (!Number.isSafeInteger(byteLimit) || byteLimit < 1) throw new Error("ledger-bench: the byte budget must be a positive integer");
  }
  get used(): number {
    return this.#used;
  }
  get bytes(): number {
    return this.#bytes;
  }
  get exhausted(): boolean {
    return this.#used >= this.limit || this.#bytes >= this.byteLimit;
  }
  /** Bytes sent or received by a request that was allowed to start. */
  addBytes(n: number): void {
    if (Number.isFinite(n) && n > 0) this.#bytes += n;
  }
  /** Spend `n` requests; `n` above 1 reserves a child process's whole allowance up front. */
  take(n = 1): void {
    if (this.#used + n > this.limit || this.#bytes >= this.byteLimit) {
      if (this.#used + n > this.limit) this.#used = this.limit;
      throw new BudgetExhausted(this.limit);
    }
    this.#used += n;
  }
}

/** A fetch that spends the meter and tallies response statuses under `label` (e.g. `R2 PUT 412`). */
export function meteredFetch(meter: RequestMeter, label: string, inner: typeof fetch = fetch): typeof fetch {
  const wrapped = async (input: Parameters<typeof fetch>[0], init?: RequestInit): Promise<Response> => {
    meter.take();
    const method = (init?.method ?? "GET").toUpperCase();
    const body = init?.body;
    if (body instanceof Uint8Array) meter.addBytes(body.byteLength);
    else if (typeof body === "string") meter.addBytes(body.length);
    try {
      const res = await inner(input, init);
      if (method !== "HEAD") meter.addBytes(Number(res.headers.get("content-length") ?? 0));
      meter.statuses.add(`${label} ${method} ${res.status}`);
      return res;
    } catch (error) {
      meter.statuses.add(`${label} ${method} failed`);
      throw error;
    }
  };
  return wrapped as typeof fetch;
}

/**
 * Base credentials: Pod Identity in the cluster. `LEDGER_BENCH_LOCAL_CREDENTIALS=env` (a local dry run
 * against an emulator or an operator's short-lived session) reads the standard AWS_* variables instead.
 */
export function baseCredentials(env: NodeJS.ProcessEnv = process.env, fetcher: typeof fetch = fetch): CredentialProvider {
  if (env.LEDGER_BENCH_LOCAL_CREDENTIALS === "env") {
    const accessKeyId = env.AWS_ACCESS_KEY_ID;
    const secretAccessKey = env.AWS_SECRET_ACCESS_KEY;
    if (!accessKeyId || !secretAccessKey) throw new Error("ledger-bench: LEDGER_BENCH_LOCAL_CREDENTIALS=env needs AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY");
    const sessionToken = env.AWS_SESSION_TOKEN;
    return () => Promise.resolve({ accessKeyId, secretAccessKey, ...(sessionToken ? { sessionToken } : {}) });
  }
  return cachedProvider(() => podIdentityCredentials(env, fetcher));
}

const ROLE_ARN = /^arn:aws:iam::\d{12}:role\/[\w+=,.@/-]{1,128}$/;

/**
 * Cross-account hop: the pod's role may do nothing but assume the bench role in the throwaway account,
 * which holds every data permission. One signed STS call; the result is cached until 5 minutes before it
 * expires.
 */
export function assumeRole(base: CredentialProvider, args: { roleArn: string; region: string; sessionName: string; fetch?: typeof fetch }): CredentialProvider {
  if (!ROLE_ARN.test(args.roleArn)) throw new Error("ledger-bench: assumeRole needs an IAM role ARN");
  if (!/^[\w+=,.@-]{2,64}$/.test(args.sessionName)) throw new Error("ledger-bench: invalid role session name");
  const fetcher = args.fetch ?? fetch;
  return cachedProvider(async () => {
    const body = new URLSearchParams({ Action: "AssumeRole", Version: "2011-06-15", RoleArn: args.roleArn, RoleSessionName: args.sessionName, DurationSeconds: "3600" }).toString();
    const url = new URL(`https://sts.${args.region}.amazonaws.com/`);
    const headers = new Headers({ "content-type": "application/x-www-form-urlencoded; charset=utf-8" });
    const bytes = new TextEncoder().encode(body);
    await signRequest({ method: "POST", url, headers, payloadSha256: await sha256Hex(bytes), region: args.region, credentials: await base(), service: "sts" });
    const res = await fetcher(url, { method: "POST", headers, body });
    const text = await res.text();
    // The body of a failure names the caller and role; only the status and the error code are reported.
    if (!res.ok) throw new BenchError(`sts_${res.status}_${(xmlText(text, "Code") ?? "error").replace(/[^A-Za-z0-9]/g, "")}`, `sts AssumeRole: http ${res.status}`);
    const accessKeyId = xmlText(text, "AccessKeyId");
    const secretAccessKey = xmlText(text, "SecretAccessKey");
    const sessionToken = xmlText(text, "SessionToken");
    const expiration = xmlText(text, "Expiration");
    if (!accessKeyId || !secretAccessKey || !sessionToken) throw new BenchError("sts_malformed", "sts AssumeRole: no credentials in the response");
    return { accessKeyId, secretAccessKey, sessionToken, ...(expiration ? { expiresAt: new Date(expiration) } : {}) };
  });
}

export interface S3Target {
  bucket: string;
  region: string;
  /** The bucket's customer-managed key; sent with every put, as a bucket policy that pins the key requires. */
  kmsKeyArn?: string;
  /** Local emulator endpoint (path-style). Never set for a real run. */
  endpoint?: string;
}

/** A store on a general-purpose bucket, exactly as ingest-edge builds its Locker store. `sse` overrides the put header. */
export function standardStore(target: S3Target, credentials: CredentialProvider, fetcher: typeof fetch, sse: "aws:kms" | "AES256" | "bucket-default" = "aws:kms"): ObjectStore {
  return createS3Store({
    bucket: target.bucket,
    region: target.region,
    ...(target.endpoint ? { endpoint: target.endpoint } : {}),
    credentials,
    fetch: fetcher,
    ...(sse === "bucket-default" ? {} : { serverSideEncryption: sse }),
    ...(sse === "aws:kms" && target.kmsKeyArn ? { sseKmsKeyId: target.kmsKeyArn } : {}),
  });
}

export interface ExpressTarget {
  /** `<base>--<zone-id>--x-s3`. */
  bucket: string;
  region: string;
}

const EXPRESS_BUCKET = /^[a-z0-9][a-z0-9-]{1,40}--([a-z0-9]+-az\d+)--x-s3$/;

export function expressZone(bucket: string): string {
  const zone = EXPRESS_BUCKET.exec(bucket)?.[1];
  if (!zone) throw new Error("ledger-bench: not a directory bucket name (<base>--<zone-id>--x-s3)");
  return zone;
}

export interface ExpressSessionStats {
  createSessionCalls: number;
  createSessionMs: number[];
  createSessionFailures: number;
}

export interface ExpressOptions {
  /**
   * Refresh the 5-minute session this long before it expires. The object-store package's own credential
   * cache refreshes 300 s early, which for a 300 s session means "always"; step 4 measures both.
   */
  refreshBeforeSeconds?: number;
  /** Name this key on every put (the single-key probe); default: no encryption headers, the bucket's own key. */
  sseKmsKeyArn?: string;
  now?: () => number;
}

/**
 * S3 Express One Zone through the object-store package. The package signs for service `s3` with
 * `x-amz-security-token`; a directory bucket wants a CreateSession credential, service `s3express` and
 * `x-amz-s3session-token`. This wraps the package's fetch and re-signs each request, so everything above
 * the signature (request shape, conditional headers, error mapping, body handling) is the production code.
 */
export function expressStore(target: ExpressTarget, credentials: CredentialProvider, fetcher: typeof fetch, options: ExpressOptions = {}): { store: ObjectStore; stats: ExpressSessionStats; createSession: () => Promise<AwsCredentials> } {
  const zone = expressZone(target.bucket);
  const host = `s3express-${zone}.${target.region}.amazonaws.com`;
  const now = options.now ?? (() => Date.now());
  const refreshBefore = (options.refreshBeforeSeconds ?? 60) * 1000;
  const stats: ExpressSessionStats = { createSessionCalls: 0, createSessionMs: [], createSessionFailures: 0 };

  const createSession = async (): Promise<AwsCredentials> => {
    const url = new URL(`https://${target.bucket}.${host}/?session`);
    const headers = new Headers({ "x-amz-create-session-mode": "ReadWrite" });
    await signRequest({ method: "GET", url, headers, payloadSha256: await sha256Hex(new Uint8Array()), region: target.region, credentials: await credentials(), service: "s3express" });
    stats.createSessionCalls++;
    const t0 = performance.now();
    let res: Response;
    try {
      res = await fetcher(url, { method: "GET", headers });
    } catch (error) {
      stats.createSessionFailures++;
      throw error;
    }
    const text = await res.text();
    stats.createSessionMs.push(Math.round((performance.now() - t0) * 100) / 100);
    if (!res.ok) {
      stats.createSessionFailures++;
      throw new BenchError(`create_session_${res.status}_${(xmlText(text, "Code") ?? "error").replace(/[^A-Za-z0-9]/g, "")}`, `CreateSession: http ${res.status}`);
    }
    const accessKeyId = xmlText(text, "AccessKeyId");
    const secretAccessKey = xmlText(text, "SecretAccessKey");
    const sessionToken = xmlText(text, "SessionToken");
    const expiration = xmlText(text, "Expiration");
    if (!accessKeyId || !secretAccessKey || !sessionToken) {
      stats.createSessionFailures++;
      throw new BenchError("create_session_malformed", "CreateSession: no credentials in the response");
    }
    return { accessKeyId, secretAccessKey, sessionToken, ...(expiration ? { expiresAt: new Date(expiration) } : {}) };
  };

  let session: AwsCredentials | undefined;
  let inflight: Promise<AwsCredentials> | undefined;
  const current = async (): Promise<AwsCredentials> => {
    if (session && (!session.expiresAt || session.expiresAt.getTime() - refreshBefore > now())) return session;
    inflight ??= createSession().finally(() => {
      inflight = undefined;
    });
    session = await inflight;
    return session;
  };

  const resign = async (input: Parameters<typeof fetch>[0], init?: RequestInit): Promise<Response> => {
    const url = new URL(typeof input === "string" || input instanceof URL ? input : input.url);
    // signRequest re-encodes the path for any service but `s3`; S3 Express signs it once. With only
    // unreserved characters in the path the two agree, so the harness keeps its keys inside that set.
    if (!/^[A-Za-z0-9\-._~/]*$/.test(url.pathname)) throw new BenchError("express_key_charset", "directory-bucket keys in this harness use only unreserved characters");
    const s = await current();
    const headers = new Headers(init?.headers);
    const payloadSha256 = headers.get("x-amz-content-sha256") ?? (await sha256Hex(new Uint8Array()));
    for (const name of ["authorization", "x-amz-security-token", "x-amz-date"]) headers.delete(name);
    headers.set("x-amz-s3session-token", s.sessionToken!);
    await signRequest({
      method: (init?.method ?? "GET").toUpperCase(),
      url,
      headers,
      payloadSha256,
      region: target.region,
      credentials: { accessKeyId: s.accessKeyId, secretAccessKey: s.secretAccessKey },
      service: "s3express",
    });
    return fetcher(url, { ...init, headers });
  };

  const store = createS3Store({
    bucket: target.bucket,
    region: target.region,
    endpoint: `https://${host}`,
    virtualHostedStyle: true,
    // Replaced by the session credential in `resign`; the package only needs something to sign with.
    credentials: { accessKeyId: "REPLACED-BY-SESSION", secretAccessKey: "replaced-by-session" },
    fetch: resign as typeof fetch,
    ...(options.sseKmsKeyArn ? { serverSideEncryption: "aws:kms" as const, sseKmsKeyId: options.sseKmsKeyArn } : {}),
  });
  return { store, stats, createSession };
}

export interface DynamoTarget {
  table: string;
  region: string;
  /** Local emulator endpoint. Never set for a real run. */
  endpoint?: string;
}

export type AttributeValue = { S: string } | { N: string } | { B: string } | { BOOL: boolean };
export type Item = Record<string, AttributeValue>;

/** A DynamoDB failure: `errorClass` is `ddb_<Exception>` plus the cancellation reasons of a cancelled transaction. */
export class DynamoError extends BenchError {
  constructor(
    readonly exception: string,
    readonly status: number,
    readonly reasons: string[],
  ) {
    super(`ddb_${exception}${reasons.length > 0 ? `_${[...new Set(reasons.filter((r) => r !== "None"))].sort().join("+") || "None"}` : ""}`, `dynamodb ${exception} (http ${status})`);
  }
}

/** The four calls the benchmark needs, over the DynamoDB JSON 1.0 protocol. */
export class Dynamo {
  readonly #url: URL;
  constructor(
    readonly target: DynamoTarget,
    readonly credentials: CredentialProvider,
    readonly fetcher: typeof fetch,
  ) {
    this.#url = new URL(target.endpoint ?? `https://dynamodb.${target.region}.amazonaws.com/`);
  }

  async call<T = Record<string, unknown>>(operation: string, input: Record<string, unknown>): Promise<T> {
    const body = new TextEncoder().encode(JSON.stringify(input));
    const headers = new Headers({ "content-type": "application/x-amz-json-1.0", "x-amz-target": `DynamoDB_20120810.${operation}` });
    const url = new URL(this.#url);
    await signRequest({ method: "POST", url, headers, payloadSha256: await sha256Hex(body), region: this.target.region, credentials: await this.credentials(), service: "dynamodb" });
    const res = await this.fetcher(url, { method: "POST", headers, body });
    const text = await res.text();
    if (res.ok) return (text ? JSON.parse(text) : {}) as T;
    let parsed: { __type?: string; CancellationReasons?: { Code?: string }[] } = {};
    try {
      parsed = JSON.parse(text) as typeof parsed;
    } catch {
      /* a non-JSON failure body is reported by status alone */
    }
    const exception = (parsed.__type ?? `Http${res.status}`).split("#").pop()!.replace(/[^A-Za-z0-9]/g, "");
    throw new DynamoError(exception, res.status, (parsed.CancellationReasons ?? []).map((r) => (r.Code ?? "None").replace(/[^A-Za-z0-9]/g, "")));
  }

  transactWrite(items: Record<string, unknown>[], clientRequestToken?: string): Promise<Record<string, unknown>> {
    return this.call("TransactWriteItems", { TransactItems: items, ...(clientRequestToken ? { ClientRequestToken: clientRequestToken } : {}) });
  }

  async get(key: Item, consistent = true): Promise<Item | undefined> {
    return (await this.call<{ Item?: Item }>("GetItem", { TableName: this.target.table, Key: key, ConsistentRead: consistent })).Item;
  }

  /** Every item of one partition, strongly consistent, following pagination. */
  async queryPartition(pk: string, skPrefix?: string): Promise<Item[]> {
    const out: Item[] = [];
    let start: Item | undefined;
    do {
      const page = await this.call<{ Items?: Item[]; LastEvaluatedKey?: Item }>("Query", {
        TableName: this.target.table,
        ConsistentRead: true,
        KeyConditionExpression: skPrefix === undefined ? "pk = :pk" : "pk = :pk AND begins_with(sk, :sk)",
        ExpressionAttributeValues: { ":pk": { S: pk }, ...(skPrefix === undefined ? {} : { ":sk": { S: skPrefix } }) },
        ...(start ? { ExclusiveStartKey: start } : {}),
      });
      out.push(...(page.Items ?? []));
      start = page.LastEvaluatedKey;
    } while (start);
    return out;
  }
}
