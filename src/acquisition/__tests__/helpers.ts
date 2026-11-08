/**
 * Test fixtures. Keys are the public RFC 8032 test vectors (key 1 = the cloud
 * issuer of the §3 vectors, key 2 = the native issuer): never deployed keys.
 */
import { createPrivateKey, createPublicKey, randomBytes, sign } from "node:crypto";
import {
  acquisitionAssertionBytes,
  acquisitionAssertionDigests,
  acquisitionPayload,
  type TrustedAcquisitionIssuer,
} from "../../session-ingest/acquisition-assertion.ts";
import { createAcquisitionApp, type AcquisitionPrincipal, type Authenticator } from "../app.ts";
import { AcquisitionSql, Installer, type AcquisitionSqlExecutor, type SqlFunction } from "../installer.ts";
import { MemoryAcquisitionStore } from "../intent-store.ts";
import { createIssuerSigner, rawPublicKey, registryOfClass } from "../key-registry.ts";
import { NativeIssuer } from "../native-issuer.ts";
import { RelaycontrolRejectedError } from "@rhc/auth/relaycontrol.ts";

const PKCS8 = "302e020100300506032b657004220420";
export const CLOUD_SEED_HEX = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60";
export const NATIVE_SEED_HEX = "4ccd089b28ff96da9db6c346ec114e0f5b8a319f35aba624da8cf6ed4fb8a6fb";
export const cloudKey = createPrivateKey({ format: "der", type: "pkcs8", key: Buffer.from(PKCS8 + CLOUD_SEED_HEX, "hex") });
export const nativeKey = createPrivateKey({ format: "der", type: "pkcs8", key: Buffer.from(PKCS8 + NATIVE_SEED_HEX, "hex") });
export const CLOUD_KEY_ID = "test-key-1";
export const NATIVE_KEY_ID = "native-key-1";

export const registry = new Map<string, TrustedAcquisitionIssuer>([
  [CLOUD_KEY_ID, { ownerKind: "cloud", publicKey: createPublicKey(cloudKey) }],
  [NATIVE_KEY_ID, { ownerKind: "native", publicKey: createPublicKey(nativeKey) }],
]);
export const pinnedKeysJson = JSON.stringify([
  { issuerKeyId: CLOUD_KEY_ID, issuerClass: "cloud", publicKeyBase64: rawPublicKey(createPublicKey(cloudKey)).toString("base64") },
  { issuerKeyId: NATIVE_KEY_ID, issuerClass: "native", publicKeyBase64: rawPublicKey(createPublicKey(nativeKey)).toString("base64") },
]);
export const intentKeyBytes = Buffer.alloc(32, 7);

export const ORG = "org_test";
export const WS = "ws_test";
export const USER = "user_test";
export const ORIGIN = "https://history.example.test";

/** Every secret a log line must never contain, filled in as tests run. */
export const secrets = new Set<string>([CLOUD_SEED_HEX, NATIVE_SEED_HEX, intentKeyBytes.toString("base64"), intentKeyBytes.toString("hex")]);

export function newProof(): string {
  const proof = randomBytes(32).toString("base64url");
  secrets.add(proof);
  return proof;
}

// ---------------------------------------------------------------- signed envelopes

/** An ops envelope signed by `key` (default: the cloud test key). */
export function envelope(kind: "install" | "revoke" | "rebind", payload: Record<string, string>, key = cloudKey) {
  const A = acquisitionAssertionBytes(kind, payload);
  const S = sign(null, A, key);
  return { payload: { ...acquisitionPayload(kind, payload), ...acquisitionAssertionDigests(A, S) }, signature: S.toString("base64url") };
}

export const cloudInstall = (over: Record<string, string> = {}) => ({
  operationUuid: "00000000-0000-4000-8000-000000000001", orgId: ORG, workspaceId: WS, source: "droid", sessionId: "sess-0001",
  revision: "1", expectedRevision: "0", expectedOwnerEpoch: "0", expectedPredecessorGrantId: "", ownerKind: "cloud",
  ownerUserId: USER, ownerOriginId: "origin_test", bindingUuid: "00000000-0000-4000-8000-000000000002", installationId: "",
  grantSha256: "eca3f7669845e8b736db441074fcbaa51183bfb167f7cc71003d4d564496d227", expiresAt: "", issuerKeyId: CLOUD_KEY_ID, ...over,
});
export const cloudRevoke = (over: Record<string, string> = {}) => ({
  operationUuid: "00000000-0000-4000-8000-000000000011", orgId: ORG, workspaceId: WS, source: "droid", sessionId: "sess-0001",
  revision: "2", targetGrantId: "00000000-0000-4000-8000-000000000001", targetOwnerEpoch: "1", targetOwnerKind: "cloud",
  targetBindingUuid: "00000000-0000-4000-8000-000000000002", targetInstallationId: "", issuerKeyId: CLOUD_KEY_ID, ...over,
});

// ---------------------------------------------------------------- fake SQL

type Row = { state: string; revision: string; tombstoneRevision: string | null; ownerEpoch: string; ownerKind: string | null; grantId: string | null };

/**
 * A tiny in-memory model of the four §3.1 functions. Returns jsonb TEXT as
 * Postgres prints it (bigints as bare numbers). `failNext` injects a driver
 * error ({code: sqlstate, message}) for the next call to a function (not
 * applied); `loseNext` applies the next call and then loses its response;
 * `hold` parks the next call until released. Install is idempotent by
 * operationUuid like the SQL: a resend answers the same result as `replay`.
 */
export class FakeSql implements AcquisitionSqlExecutor {
  readonly calls: Array<{ fn: SqlFunction; p: Readonly<Record<string, unknown>> }> = [];
  readonly sessions = new Map<string, Row>();
  readonly failures: Array<{ fn?: SqlFunction; error: unknown }> = [];
  /** Raw text override for status (bigint tests). */
  statusText: string | null = null;
  /** The epoch an install lands at (default current + 1, the design contract); returning null omits ownerEpoch from the result. */
  installEpoch: ((current: bigint) => bigint | null) | null = null;
  /** Install results by operationUuid (the SQL's idempotency). */
  readonly installed = new Map<string, string>();
  private readonly losses: SqlFunction[] = [];
  private readonly holds: Array<{ fn: SqlFunction; arrived: () => void; gate: Promise<void> }> = [];

  failNext(error: unknown, fn?: SqlFunction) {
    this.failures.push({ fn, error });
  }

  /** The next call to `fn` commits, then its response is lost (a connection reset). */
  loseNext(fn: SqlFunction) {
    this.losses.push(fn);
  }

  /** Park the next call to `fn` before it applies: `reached` resolves when it arrives, `release` lets it run. */
  hold(fn: SqlFunction): { reached: Promise<void>; release: () => void } {
    let arrived!: () => void, release!: () => void;
    const reached = new Promise<void>((r) => (arrived = r));
    const gate = new Promise<void>((r) => (release = r));
    this.holds.push({ fn, arrived, gate });
    return { reached, release };
  }

  async call(fn: SqlFunction, p: Readonly<Record<string, unknown>>): Promise<string> {
    this.calls.push({ fn, p });
    const h = this.holds.findIndex((x) => x.fn === fn);
    if (h >= 0) {
      const held = this.holds.splice(h, 1)[0]!;
      held.arrived();
      await held.gate;
    }
    const i = this.failures.findIndex((f) => f.fn === undefined || f.fn === fn);
    if (i >= 0) throw this.failures.splice(i, 1)[0]!.error;
    const out = this.apply(fn, p);
    const l = this.losses.indexOf(fn);
    if (l >= 0) {
      this.losses.splice(l, 1);
      throw Object.assign(new Error("connection reset"), { code: "ECONNRESET" });
    }
    return out;
  }

  private apply(fn: SqlFunction, p: Readonly<Record<string, unknown>>): string {
    const key = (sessionId: unknown) => `${String(p.source)}/${String(sessionId)}`;
    if (fn === "status") {
      if (this.statusText !== null) return this.statusText;
      const rows = (p.sessionIds as string[]).map((sessionId) => {
        const r = this.sessions.get(key(sessionId));
        return r
          ? `{"sessionId":${JSON.stringify(sessionId)},"state":"${r.state}","revision":${r.revision},"tombstoneRevision":${r.tombstoneRevision ?? "null"},"ownerEpoch":${r.ownerEpoch},"ownerKind":${JSON.stringify(r.ownerKind)},"grantId":${JSON.stringify(r.grantId)}}`
          : `{"sessionId":${JSON.stringify(sessionId)},"state":"unowned","revision":0,"tombstoneRevision":null,"ownerEpoch":0,"ownerKind":null,"grantId":null}`;
      });
      return `[${rows.join(",")}]`;
    }
    const k = key(p.sessionId);
    const current = this.sessions.get(k);
    if (fn === "install") {
      const prior = this.installed.get(String(p.operationUuid));
      if (prior) return prior.replace(`"outcome":"installed"`, `"outcome":"replay"`);
      const landed = this.installEpoch ? this.installEpoch(BigInt(current?.ownerEpoch ?? "0")) : BigInt(current?.ownerEpoch ?? "0") + 1n;
      const epoch = (landed ?? BigInt(current?.ownerEpoch ?? "0") + 1n).toString();
      this.sessions.set(k, { state: "active", revision: String(p.revision), tombstoneRevision: null, ownerEpoch: epoch, ownerKind: String(p.ownerKind), grantId: String(p.operationUuid) });
      const epochField = landed === null ? "" : `"ownerEpoch":${epoch},`;
      const result = `{"outcome":"installed","grantId":${JSON.stringify(p.operationUuid)},${epochField}"revision":${p.revision},"state":"active"}`;
      this.installed.set(String(p.operationUuid), result);
      return result;
    }
    if (fn === "revoke") {
      const revision = (BigInt(current?.revision ?? "0") > BigInt(String(p.revision)) ? current!.revision : String(p.revision));
      if (current) this.sessions.set(k, { ...current, state: "draining", revision });
      return `{"outcome":"draining","revision":${revision},"ownerEpoch":${current?.ownerEpoch ?? 0},"state":"draining"}`;
    }
    if (current) this.sessions.set(k, { ...current, revision: String(p.revision) });
    return `{"outcome":"rebound","revision":${p.revision},"ownerEpoch":${current?.ownerEpoch ?? 0}}`;
  }
}

export const pgError = (sqlstate: string, code: string) => Object.assign(new Error(`acquisition: ${code}`), { code: sqlstate });

// ---------------------------------------------------------------- auth + log

export const principal = (over: Partial<AcquisitionPrincipal> = {}): AcquisitionPrincipal => ({
  userId: USER, orgId: ORG, workspaceId: WS, scopes: ["rth:acquire"], principalType: "desktop-user", ...over,
});
export const opsPrincipal = (over: Partial<AcquisitionPrincipal> = {}): AcquisitionPrincipal =>
  principal({ userId: "svc_cloud", scopes: ["acquisition:operate"], principalType: "service", ...over });

/** Tokens -> principals; "sh_at_down" simulates an outage; anything else is rejected. */
export class FakeAuthenticator implements Authenticator {
  readonly tokens = new Map<string, AcquisitionPrincipal>();
  async authenticate(token: string): Promise<AcquisitionPrincipal> {
    if (token.endsWith("_down")) throw new Error("introspection timeout");
    const p = this.tokens.get(token);
    if (!p) throw new RelaycontrolRejectedError("inactive");
    return p;
  }
}

export type Captured = { level: string; message: string; fields: Record<string, unknown> };
export function captureLog() {
  const lines: Captured[] = [];
  return { lines, log: (level: string, message: string, fields: Record<string, unknown>) => void lines.push({ level, message, fields }) };
}

// ---------------------------------------------------------------- harness

export function nativeHarness() {
  const sql = new FakeSql();
  const clock = { now: 1_000_000 };
  const store = new MemoryAcquisitionStore(() => clock.now);
  const signer = createIssuerSigner(NATIVE_KEY_ID, nativeKey, registry);
  const issuer = new NativeIssuer({ store, sql: new AcquisitionSql(sql), signer, intentKey: intentKeyBytes, now: () => clock.now });
  const auth = new FakeAuthenticator();
  auth.tokens.set("sh_at_alice", principal());
  const captured = captureLog();
  const app = createAcquisitionApp({ enabled: true, group: "native", groupEnabled: true, authenticator: auth, issuer, log: captured.log });
  return { sql, store, issuer, auth, app, captured, signer, clock };
}

export function opsHarness() {
  const sql = new FakeSql();
  const installer = new Installer(new AcquisitionSql(sql), registryOfClass(registry, "cloud"));
  const auth = new FakeAuthenticator();
  auth.tokens.set("sh_wk_ops", opsPrincipal());
  const captured = captureLog();
  const app = createAcquisitionApp({ enabled: true, group: "ops", groupEnabled: true, authenticator: auth, installer, log: captured.log });
  return { sql, installer, auth, app, captured };
}

export async function call(app: { request: (path: string, init?: RequestInit) => Response | Promise<Response> }, method: string, path: string, opts: { token?: string; body?: unknown; proof?: string; headers?: Record<string, string> } = {}) {
  const headers: Record<string, string> = { ...(opts.headers ?? {}) };
  if (opts.token) headers.Authorization = `Bearer ${opts.token}`;
  if (opts.proof) headers["X-Acquisition-Installation-Proof"] = opts.proof;
  if (opts.body !== undefined) headers["Content-Type"] ??= "application/json";
  const res = await app.request(path, { method, headers, body: opts.body === undefined ? undefined : typeof opts.body === "string" ? opts.body : JSON.stringify(opts.body) });
  const text = await res.text();
  return { status: res.status, text, json: text ? JSON.parse(text) : null, headers: res.headers };
}

/** Assert no captured line (or any extra text) contains a secret. */
export function leaks(texts: string[]): string[] {
  const found: string[] = [];
  for (const t of texts) for (const s of secrets) if (s && t.includes(s)) found.push(s.slice(0, 6));
  return found;
}
