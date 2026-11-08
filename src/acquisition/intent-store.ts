/**
 * Durable state of the native issuer (§4.2 committed intents, P3a
 * installations). One interface, two implementations: MemoryAcquisitionStore
 * (tests) and PgAcquisitionStore over the proposed DDL in
 * supabase/acquisition-intents.proposed.sql (the SQL lane folds it into the
 * migration; nothing here applies it).
 *
 * Rules:
 * - An intent (A, S, digests, encrypted G) is committed BEFORE its SQL call
 *   and is never re-signed; a retry re-sends the stored bytes.
 * - An install's RESERVED revoke (pre-signed, targeting the grant installed)
 *   is signed against the ownerEpoch the install RESULT reports and commits
 *   in ONE transaction with the install's `applied` mark (correction 5). A
 *   result with no usable epoch gets no reservation, recorded on the install
 *   (`reservation`); an explicit revoke then signs against the authority row.
 * - A pending intent that keeps failing (transport, session_busy, a bad stored
 *   intent) is retried with bounded exponential backoff and, after
 *   MAX_SEND_ATTEMPTS, becomes `stuck`: terminal for the reconciler, not
 *   outstanding for key retention, and no longer blocks a later revoke/rebind.
 * - A refused intent is never edited again; a new attempt is a new operationUuid.
 * - G is stored only as AES-256-GCM ciphertext under ACQUISITION_INTENT_KEY,
 *   with the operation and installation as associated data.
 * - Installations store only proofSha256, never the possession proof (P3a).
 */
import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";

export type IntentKind = "install" | "revoke" | "rebind";
export type IntentStatus = "reserved" | "pending" | "applied" | "refused" | "void" | "stuck";
/** install only: whether its reserved revoke was pre-signed (null until applied). */
export type Reservation = "signed" | "no_epoch" | "sign_failed";
export type ActivationReason = "explicit_exclude" | "installation_retired" | "session_removed";

export interface InstallationRecord {
  readonly installationId: string;
  readonly requestUuid: string;
  readonly orgId: string;
  readonly workspaceId: string;
  readonly userId: string;
  readonly originId: string;
  readonly label: string | null;
  readonly proofSha256: string;
  readonly retiredAt: string | null;
}

export interface IntentRecord {
  readonly operationUuid: string;
  readonly kind: IntentKind;
  readonly installationId: string;
  readonly orgId: string;
  readonly workspaceId: string;
  readonly source: string;
  readonly sessionId: string;
  /** revoke/rebind: the grant targeted; install: null (its grant id is its operationUuid). */
  readonly targetGrantId: string | null;
  /** install: ownerOriginId; rebind: newOriginId; revoke: null. */
  readonly originId: string | null;
  readonly assertion: Uint8Array;
  readonly signature: Uint8Array;
  readonly evidenceSha256: string;
  readonly requestSha256: string;
  readonly grantCiphertext: Uint8Array | null;
  readonly grantNonce: Uint8Array | null;
  readonly status: IntentStatus;
  readonly refusalCode: string | null;
  /** The SQL result as text (bigints exact). */
  readonly result: string | null;
  readonly clientOperationUuid: string | null;
  readonly activationReason: ActivationReason | null;
  /** Failed sends counted toward MAX_SEND_ATTEMPTS. */
  readonly attempts: number;
  /** Epoch ms before which the reconciler does not resend. */
  readonly nextAttemptAt: number;
  readonly reservation: Reservation | null;
}

export type NewIntent = Omit<IntentRecord, "status" | "refusalCode" | "result" | "clientOperationUuid" | "activationReason" | "attempts" | "nextAttemptAt" | "reservation">;

/** Bounded retry policy for pending intents (reconciler and client retries alike). */
export const MAX_SEND_ATTEMPTS = 8;
export const RETRY_BASE_MS = 30_000;
export const RETRY_CAP_MS = 3_600_000;
/** Backoff after the n-th failure (n >= 1): base * 2^(n-1), capped. */
export const retryDelayMs = (failures: number) => Math.min(RETRY_BASE_MS * 2 ** Math.max(0, failures - 1), RETRY_CAP_MS);

export interface AcquisitionStore {
  // Installations (P3a). Reading an installation is the proof check itself.
  /** Insert, or return the existing row for the same requestUuid (`created: false`). */
  registerInstallation(record: InstallationRecord): Promise<{ record: InstallationRecord; created: boolean }>;
  getInstallation(installationId: string): Promise<InstallationRecord | null>;

  // Intents (§4.2).
  getIntent(operationUuid: string): Promise<IntentRecord | null>;
  /** A pending install. Returns the existing intent on a duplicate operationUuid. */
  createInstallIntent(install: NewIntent): Promise<{ record: IntentRecord; created: boolean }>;
  /**
   * One transaction: pending install -> applied with `result` and the
   * reservation outcome, and (when signed) its reserved revoke inserted.
   * No-op unless the install is still pending.
   */
  markInstallApplied(operationUuid: string, result: string, reservedRevoke: NewIntent | null, reservation: Reservation): Promise<void>;
  /**
   * An explicit revoke signed now (the install has no usable reservation).
   * Pending, activated by `clientOperationUuid`. Returns the grant's existing
   * revoke instead when one already exists (one revoke per grant).
   */
  createRevokeIntent(revoke: NewIntent, clientOperationUuid: string): Promise<IntentRecord>;
  /** A pending rebind. Returns the existing intent on a duplicate operationUuid. */
  createRebindIntent(rebind: NewIntent): Promise<{ record: IntentRecord; created: boolean }>;
  reservedRevokeFor(grantId: string): Promise<IntentRecord | null>;
  findByClientOperation(clientOperationUuid: string): Promise<IntentRecord | null>;
  /** reserved -> pending (idempotent when already pending/applied). No network, no private key. */
  activateRevoke(operationUuid: string, reason: ActivationReason, clientOperationUuid: string | null): Promise<IntentRecord>;
  /** pending -> applied (revoke/rebind; installs use markInstallApplied). */
  markApplied(operationUuid: string, result: string): Promise<void>;
  /**
   * A failed send of a pending intent: attempts += 1, next attempt after
   * retryDelayMs(attempts); at MAX_SEND_ATTEMPTS it becomes `stuck`.
   * Returns the status after the update.
   */
  markRetry(operationUuid: string): Promise<IntentStatus | null>;
  /** Push a pending intent's next attempt out without counting an attempt (reconciler skip). */
  deferIntent(operationUuid: string, delayMs: number): Promise<void>;
  /** pending -> refused (terminal). A refused install voids its reserved revoke in the same transaction. */
  markRefused(operationUuid: string, code: string): Promise<void>;
  /** Pending rebind for this grant other than `exceptOperationUuid`, if any. */
  pendingRebindFor(grantId: string, exceptOperationUuid: string): Promise<IntentRecord | null>;
  /** The origin of the latest applied install/rebind of this grant. */
  currentOrigin(grantId: string): Promise<string | null>;
  /** Which of `grantIds` were installed by `installationId` through this issuer. */
  grantsOfInstallation(installationId: string, grantIds: readonly string[]): Promise<Set<string>>;
  /** Mark retired and activate every reserved revoke of its grants, in one transaction. Returns the count activated. */
  retireInstallation(installationId: string): Promise<number>;
  /**
   * Pending intents of every kind that are due (nextAttemptAt <= now), by
   * nextAttemptAt then creation: FIFO, and a failing intent moves behind the
   * queue (backoff), so a poisoned backlog cannot starve later intents.
   */
  dueIntents(limit: number): Promise<IntentRecord[]>;
  /**
   * Distinct issuerKeyId (the last A field) of every intent that may still be
   * sent or replayed: reserved revokes and pending operations (`stuck` is
   * terminal and excluded). Key rotation must retain these public keys
   * (§9 key retention).
   */
  outstandingIssuerKeyIds(): Promise<Set<string>>;
}

/** issuerKeyId is the last U+001F field of every assertion kind (F1). */
export function assertionIssuerKeyId(assertion: Uint8Array): string {
  const text = Buffer.from(assertion).toString("utf8");
  return text.slice(text.lastIndexOf("\u001f") + 1);
}

export class StoreConflict extends Error {
  constructor(readonly code: "operation_conflict" | "installation_conflict") {
    super(code);
    this.name = "StoreConflict";
  }
}

// ---------------------------------------------------------------- grant encryption

const GRANT_AAD_DOMAIN = "96bulls:acquisition-grant-intent-v1";

function grantAad(operationUuid: string, installationId: string): Buffer {
  return Buffer.from([GRANT_AAD_DOMAIN, operationUuid, installationId].join("\u001f"), "utf8");
}

/** AES-256-GCM; ciphertext = encrypted bytes || 16-byte tag. */
export function encryptGrant(key: Uint8Array, G: string, operationUuid: string, installationId: string): { ciphertext: Buffer; nonce: Buffer } {
  if (key.byteLength !== 32) throw new Error("intent key must be 32 bytes");
  const nonce = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", key, nonce);
  cipher.setAAD(grantAad(operationUuid, installationId));
  const ciphertext = Buffer.concat([cipher.update(G, "utf8"), cipher.final(), cipher.getAuthTag()]);
  return { ciphertext, nonce };
}

export function decryptGrant(key: Uint8Array, ciphertext: Uint8Array, nonce: Uint8Array, operationUuid: string, installationId: string): string {
  const data = Buffer.from(ciphertext);
  if (data.byteLength < 17) throw new Error("grant ciphertext is malformed");
  const decipher = createDecipheriv("aes-256-gcm", key, nonce);
  decipher.setAAD(grantAad(operationUuid, installationId));
  decipher.setAuthTag(data.subarray(data.byteLength - 16));
  return Buffer.concat([decipher.update(data.subarray(0, data.byteLength - 16)), decipher.final()]).toString("utf8");
}

// ---------------------------------------------------------------- memory

const INSTALLATION_METHODS = new Set(["registerInstallation", "getInstallation"]);

/** In-memory store for tests. `intentCalls` counts every non-installation method call. */
export class MemoryAcquisitionStore implements AcquisitionStore {
  /** `now`: injectable clock (ms) for backoff tests. */
  constructor(private readonly now: () => number = Date.now) {}
  readonly installations = new Map<string, InstallationRecord>();
  readonly intents = new Map<string, IntentRecord>();
  readonly calls: string[] = [];
  get intentCalls(): number {
    return this.calls.filter((m) => !INSTALLATION_METHODS.has(m)).length;
  }

  private note(method: string) {
    this.calls.push(method);
  }
  private update(operationUuid: string, patch: Partial<IntentRecord>) {
    const current = this.intents.get(operationUuid);
    if (!current) throw new Error("no such intent");
    this.intents.set(operationUuid, Object.freeze({ ...current, ...patch }));
  }

  async registerInstallation(record: InstallationRecord) {
    this.note("registerInstallation");
    for (const existing of this.installations.values())
      if (existing.requestUuid === record.requestUuid) return { record: existing, created: false };
    if (this.installations.has(record.installationId)) throw new StoreConflict("installation_conflict");
    this.installations.set(record.installationId, Object.freeze({ ...record }));
    return { record: this.installations.get(record.installationId)!, created: true };
  }
  async getInstallation(installationId: string) {
    this.note("getInstallation");
    return this.installations.get(installationId) ?? null;
  }
  async getIntent(operationUuid: string) {
    this.note("getIntent");
    return this.intents.get(operationUuid) ?? null;
  }
  private fresh(intent: NewIntent, status: IntentStatus): IntentRecord {
    return Object.freeze({ ...intent, status, refusalCode: null, result: null, clientOperationUuid: null, activationReason: null, attempts: 0, nextAttemptAt: this.now(), reservation: null });
  }
  async createInstallIntent(install: NewIntent) {
    this.note("createInstallIntent");
    const existing = this.intents.get(install.operationUuid);
    if (existing) return { record: existing, created: false };
    this.intents.set(install.operationUuid, this.fresh(install, "pending"));
    return { record: this.intents.get(install.operationUuid)!, created: true };
  }
  async markInstallApplied(operationUuid: string, result: string, reservedRevoke: NewIntent | null, reservation: Reservation) {
    this.note("markInstallApplied");
    const current = this.intents.get(operationUuid);
    if (!current || current.kind !== "install" || current.status !== "pending") return;
    if (reservedRevoke && (this.intents.has(reservedRevoke.operationUuid) || [...this.intents.values()].some((i) => i.kind === "revoke" && i.targetGrantId === operationUuid)))
      throw new StoreConflict("operation_conflict"); // the transaction rolls back: still pending
    this.update(operationUuid, { status: "applied", result, reservation });
    if (reservedRevoke) this.intents.set(reservedRevoke.operationUuid, this.fresh(reservedRevoke, "reserved"));
  }
  async createRevokeIntent(revoke: NewIntent, clientOperationUuid: string) {
    this.note("createRevokeIntent");
    const existing = [...this.intents.values()].find((i) => i.kind === "revoke" && i.targetGrantId === revoke.targetGrantId);
    if (existing) return existing;
    if (this.intents.has(revoke.operationUuid) || [...this.intents.values()].some((i) => i.clientOperationUuid === clientOperationUuid))
      throw new StoreConflict("operation_conflict");
    this.intents.set(revoke.operationUuid, Object.freeze({ ...this.fresh(revoke, "pending"), clientOperationUuid, activationReason: "explicit_exclude" as const }));
    return this.intents.get(revoke.operationUuid)!;
  }
  async createRebindIntent(rebind: NewIntent) {
    this.note("createRebindIntent");
    const existing = this.intents.get(rebind.operationUuid);
    if (existing) return { record: existing, created: false };
    this.intents.set(rebind.operationUuid, this.fresh(rebind, "pending"));
    return { record: this.intents.get(rebind.operationUuid)!, created: true };
  }
  async reservedRevokeFor(grantId: string) {
    this.note("reservedRevokeFor");
    return [...this.intents.values()].find((i) => i.kind === "revoke" && i.targetGrantId === grantId) ?? null;
  }
  async findByClientOperation(clientOperationUuid: string) {
    this.note("findByClientOperation");
    return [...this.intents.values()].find((i) => i.clientOperationUuid === clientOperationUuid) ?? null;
  }
  async activateRevoke(operationUuid: string, reason: ActivationReason, clientOperationUuid: string | null) {
    this.note("activateRevoke");
    const current = this.intents.get(operationUuid);
    if (!current || current.kind !== "revoke") throw new Error("no such revoke");
    if (clientOperationUuid !== null) {
      const other = [...this.intents.values()].find((i) => i.clientOperationUuid === clientOperationUuid);
      if (other && other.operationUuid !== operationUuid) throw new StoreConflict("operation_conflict");
    }
    if (current.status === "reserved")
      this.update(operationUuid, { status: "pending", activationReason: reason, clientOperationUuid: clientOperationUuid ?? current.clientOperationUuid });
    else if (current.clientOperationUuid === null && clientOperationUuid !== null) this.update(operationUuid, { clientOperationUuid });
    return this.intents.get(operationUuid)!;
  }
  async markApplied(operationUuid: string, result: string) {
    this.note("markApplied");
    const current = this.intents.get(operationUuid);
    if (current && (current.status === "pending" || current.status === "applied")) this.update(operationUuid, { status: "applied", result });
  }
  async markRetry(operationUuid: string) {
    this.note("markRetry");
    const current = this.intents.get(operationUuid);
    if (!current) return null;
    if (current.status !== "pending") return current.status;
    const attempts = current.attempts + 1;
    this.update(operationUuid, { attempts, nextAttemptAt: this.now() + retryDelayMs(attempts), status: attempts >= MAX_SEND_ATTEMPTS ? "stuck" : "pending" });
    return this.intents.get(operationUuid)!.status;
  }
  async deferIntent(operationUuid: string, delayMs: number) {
    this.note("deferIntent");
    const current = this.intents.get(operationUuid);
    if (current?.status === "pending") this.update(operationUuid, { nextAttemptAt: this.now() + delayMs });
  }
  async markRefused(operationUuid: string, code: string) {
    this.note("markRefused");
    const current = this.intents.get(operationUuid);
    if (!current || current.status !== "pending") return;
    this.update(operationUuid, { status: "refused", refusalCode: code });
    if (current.kind === "install")
      for (const i of this.intents.values())
        if (i.kind === "revoke" && i.targetGrantId === operationUuid && i.status === "reserved") this.update(i.operationUuid, { status: "void" });
  }
  async pendingRebindFor(grantId: string, exceptOperationUuid: string) {
    this.note("pendingRebindFor");
    return [...this.intents.values()].find((i) => i.kind === "rebind" && i.targetGrantId === grantId && i.status === "pending" && i.operationUuid !== exceptOperationUuid) ?? null;
  }
  async currentOrigin(grantId: string) {
    this.note("currentOrigin");
    let origin: string | null = null;
    for (const i of this.intents.values()) // insertion order = creation order
      if (i.status === "applied" && ((i.kind === "install" && i.operationUuid === grantId) || (i.kind === "rebind" && i.targetGrantId === grantId))) origin = i.originId;
    return origin;
  }
  async grantsOfInstallation(installationId: string, grantIds: readonly string[]) {
    this.note("grantsOfInstallation");
    const wanted = new Set(grantIds);
    return new Set([...this.intents.values()].filter((i) => i.kind === "install" && i.installationId === installationId && wanted.has(i.operationUuid)).map((i) => i.operationUuid));
  }
  async retireInstallation(installationId: string) {
    this.note("retireInstallation");
    const inst = this.installations.get(installationId);
    if (!inst) return 0;
    if (!inst.retiredAt) this.installations.set(installationId, Object.freeze({ ...inst, retiredAt: new Date().toISOString() }));
    let count = 0;
    for (const i of [...this.intents.values()])
      if (i.kind === "revoke" && i.installationId === installationId && i.status === "reserved") {
        this.update(i.operationUuid, { status: "pending", activationReason: "installation_retired" });
        count += 1;
      }
    return count;
  }
  async dueIntents(limit: number) {
    this.note("dueIntents");
    const now = this.now();
    // Array.prototype.sort is stable: equal nextAttemptAt keeps insertion (creation) order.
    return [...this.intents.values()].filter((i) => i.status === "pending" && i.nextAttemptAt <= now).sort((a, b) => a.nextAttemptAt - b.nextAttemptAt).slice(0, limit);
  }
  async outstandingIssuerKeyIds() {
    this.note("outstandingIssuerKeyIds");
    const ids = new Set<string>();
    for (const i of this.intents.values()) if (i.status === "reserved" || i.status === "pending") ids.add(assertionIssuerKeyId(i.assertion));
    return ids;
  }
}

// ---------------------------------------------------------------- postgres

/** The minimal client both pg (via pgSqlClient) and PGlite satisfy. */
export interface SqlClient {
  query<R = Record<string, unknown>>(text: string, params?: unknown[]): Promise<{ rows: R[] }>;
  transaction<T>(work: (tx: Pick<SqlClient, "query">) => Promise<T>): Promise<T>;
}

/** A pg Pool as a SqlClient (one pooled connection per transaction). */
export function pgSqlClient(pool: { query: (text: string, params?: unknown[]) => Promise<{ rows: unknown[] }>; connect: () => Promise<{ query: (text: string, params?: unknown[]) => Promise<{ rows: unknown[] }>; release: () => void }> }): SqlClient {
  return {
    query: async <R>(text: string, params?: unknown[]) => ({ rows: (await pool.query(text, params)).rows as R[] }),
    async transaction<T>(work: (tx: Pick<SqlClient, "query">) => Promise<T>) {
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        const result = await work({ query: async <R>(text: string, params?: unknown[]) => ({ rows: (await client.query(text, params)).rows as R[] }) });
        await client.query("COMMIT");
        return result;
      } catch (error) {
        await client.query("ROLLBACK").catch(() => {});
        throw error;
      } finally {
        client.release();
      }
    },
  };
}

const INSTALLATIONS = "sessions.ingest_acquisition_native_installations";
const INTENTS = "sessions.ingest_acquisition_native_intents";
const INTENT_COLUMNS =
  "operation_uuid::text, kind, installation_id::text, org_id, workspace_id, source, session_id, target_grant_id::text, origin_id, assertion, signature, evidence_sha256, request_sha256, grant_ciphertext, grant_nonce, status, refusal_code, result, client_operation_uuid::text, activation_reason, attempts, (extract(epoch FROM next_attempt_at) * 1000)::float8 AS next_attempt_ms, reservation";
const INSTALLATION_COLUMNS = "installation_id::text, request_uuid::text, org_id, workspace_id, user_id, origin_id, label, proof_sha256, retired_at::text";

type Row = Record<string, unknown>;
const bytes = (v: unknown): Uint8Array => (v instanceof Uint8Array ? v : Buffer.from(v as string));
const bytesOrNull = (v: unknown): Uint8Array | null => (v === null || v === undefined ? null : bytes(v));
const strOrNull = (v: unknown): string | null => (v === null || v === undefined ? null : String(v));

function intentRow(r: Row): IntentRecord {
  return Object.freeze({
    operationUuid: String(r.operation_uuid), kind: r.kind as IntentKind, installationId: String(r.installation_id),
    orgId: String(r.org_id), workspaceId: String(r.workspace_id), source: String(r.source), sessionId: String(r.session_id),
    targetGrantId: strOrNull(r.target_grant_id), originId: strOrNull(r.origin_id),
    assertion: bytes(r.assertion), signature: bytes(r.signature),
    evidenceSha256: String(r.evidence_sha256), requestSha256: String(r.request_sha256),
    grantCiphertext: bytesOrNull(r.grant_ciphertext), grantNonce: bytesOrNull(r.grant_nonce),
    status: r.status as IntentStatus, refusalCode: strOrNull(r.refusal_code), result: strOrNull(r.result),
    clientOperationUuid: strOrNull(r.client_operation_uuid), activationReason: strOrNull(r.activation_reason) as ActivationReason | null,
    attempts: Number(r.attempts), nextAttemptAt: Number(r.next_attempt_ms), reservation: strOrNull(r.reservation) as Reservation | null,
  });
}
function installationRow(r: Row): InstallationRecord {
  return Object.freeze({
    installationId: String(r.installation_id), requestUuid: String(r.request_uuid), orgId: String(r.org_id),
    workspaceId: String(r.workspace_id), userId: String(r.user_id), originId: String(r.origin_id),
    label: strOrNull(r.label), proofSha256: String(r.proof_sha256), retiredAt: strOrNull(r.retired_at),
  });
}

const INSERT_INTENT = `INSERT INTO ${INTENTS}(operation_uuid, kind, installation_id, org_id, workspace_id, source, session_id, target_grant_id, origin_id,
  assertion, signature, evidence_sha256, request_sha256, grant_ciphertext, grant_nonce, status)
  VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) ON CONFLICT (operation_uuid) DO NOTHING RETURNING operation_uuid`;
function intentParams(i: NewIntent, status: IntentStatus): unknown[] {
  return [i.operationUuid, i.kind, i.installationId, i.orgId, i.workspaceId, i.source, i.sessionId, i.targetGrantId, i.originId,
    Buffer.from(i.assertion), Buffer.from(i.signature), i.evidenceSha256, i.requestSha256,
    i.grantCiphertext ? Buffer.from(i.grantCiphertext) : null, i.grantNonce ? Buffer.from(i.grantNonce) : null, status];
}

/** Postgres implementation (role bulls96_acquisition_installer; DDL: supabase/acquisition-intents.proposed.sql). */
export class PgAcquisitionStore implements AcquisitionStore {
  constructor(private readonly db: SqlClient) {}

  private async one(text: string, params: unknown[]): Promise<IntentRecord | null> {
    const r = await this.db.query<Row>(text, params);
    return r.rows[0] ? intentRow(r.rows[0]) : null;
  }

  async registerInstallation(record: InstallationRecord) {
    const inserted = await this.db.query<Row>(
      `INSERT INTO ${INSTALLATIONS}(installation_id, request_uuid, org_id, workspace_id, user_id, origin_id, label, proof_sha256)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (request_uuid) DO NOTHING RETURNING ${INSTALLATION_COLUMNS}`,
      [record.installationId, record.requestUuid, record.orgId, record.workspaceId, record.userId, record.originId, record.label, record.proofSha256],
    );
    if (inserted.rows[0]) return { record: installationRow(inserted.rows[0]), created: true };
    const existing = await this.db.query<Row>(`SELECT ${INSTALLATION_COLUMNS} FROM ${INSTALLATIONS} WHERE request_uuid=$1`, [record.requestUuid]);
    if (!existing.rows[0]) throw new StoreConflict("installation_conflict");
    return { record: installationRow(existing.rows[0]), created: false };
  }
  async getInstallation(installationId: string) {
    const r = await this.db.query<Row>(`SELECT ${INSTALLATION_COLUMNS} FROM ${INSTALLATIONS} WHERE installation_id=$1`, [installationId]);
    return r.rows[0] ? installationRow(r.rows[0]) : null;
  }
  getIntent(operationUuid: string) {
    return this.one(`SELECT ${INTENT_COLUMNS} FROM ${INTENTS} WHERE operation_uuid=$1`, [operationUuid]);
  }
  async createInstallIntent(install: NewIntent) {
    const r = await this.db.query(INSERT_INTENT, intentParams(install, "pending"));
    return { record: (await this.getIntent(install.operationUuid))!, created: r.rows.length > 0 };
  }
  async markInstallApplied(operationUuid: string, result: string, reservedRevoke: NewIntent | null, reservation: Reservation) {
    await this.db.transaction(async (tx) => {
      const r = await tx.query(
        `UPDATE ${INTENTS} SET status='applied', result=$2, reservation=$3, updated_at=clock_timestamp()
         WHERE operation_uuid=$1 AND kind='install' AND status='pending' RETURNING operation_uuid`,
        [operationUuid, result, reservation],
      );
      if (r.rows.length === 0 || !reservedRevoke) return;
      // A conflict (the grant already has a revoke) rolls the applied mark back: still pending.
      const rv = await tx.query(INSERT_INTENT, intentParams(reservedRevoke, "reserved"));
      if (rv.rows.length === 0) throw new StoreConflict("operation_conflict");
    });
  }
  async createRevokeIntent(revoke: NewIntent, clientOperationUuid: string) {
    try {
      await this.db.query(
        `INSERT INTO ${INTENTS}(operation_uuid, kind, installation_id, org_id, workspace_id, source, session_id, target_grant_id, origin_id,
           assertion, signature, evidence_sha256, request_sha256, grant_ciphertext, grant_nonce, status, client_operation_uuid, activation_reason)
         VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,'explicit_exclude')
         ON CONFLICT (target_grant_id) WHERE kind = 'revoke' DO NOTHING`,
        [...intentParams(revoke, "pending"), clientOperationUuid],
      );
    } catch (error) {
      if ((error as { code?: string }).code === "23505") throw new StoreConflict("operation_conflict");
      throw error;
    }
    const record = await this.reservedRevokeFor(revoke.targetGrantId!);
    if (!record) throw new Error("no such revoke");
    return record;
  }
  async createRebindIntent(rebind: NewIntent) {
    const r = await this.db.query(INSERT_INTENT, intentParams(rebind, "pending"));
    return { record: (await this.getIntent(rebind.operationUuid))!, created: r.rows.length > 0 };
  }
  reservedRevokeFor(grantId: string) {
    return this.one(`SELECT ${INTENT_COLUMNS} FROM ${INTENTS} WHERE kind='revoke' AND target_grant_id=$1`, [grantId]);
  }
  findByClientOperation(clientOperationUuid: string) {
    return this.one(`SELECT ${INTENT_COLUMNS} FROM ${INTENTS} WHERE client_operation_uuid=$1`, [clientOperationUuid]);
  }
  async activateRevoke(operationUuid: string, reason: ActivationReason, clientOperationUuid: string | null) {
    try {
      await this.db.query(
        `UPDATE ${INTENTS} SET status=CASE WHEN status='reserved' THEN 'pending' ELSE status END,
           activation_reason=CASE WHEN status='reserved' THEN $2 ELSE activation_reason END,
           client_operation_uuid=COALESCE(client_operation_uuid, $3::uuid), updated_at=clock_timestamp()
         WHERE operation_uuid=$1 AND kind='revoke'`,
        [operationUuid, reason, clientOperationUuid],
      );
    } catch (error) {
      if ((error as { code?: string }).code === "23505") throw new StoreConflict("operation_conflict");
      throw error;
    }
    const record = await this.getIntent(operationUuid);
    if (!record) throw new Error("no such revoke");
    return record;
  }
  async markApplied(operationUuid: string, result: string) {
    await this.db.query(`UPDATE ${INTENTS} SET status='applied', result=$2, updated_at=clock_timestamp() WHERE operation_uuid=$1 AND status IN('pending','applied')`, [operationUuid, result]);
  }
  async markRetry(operationUuid: string) {
    // SET expressions read the pre-update row: attempts there is the count before this failure.
    const r = await this.db.query<Row>(
      `UPDATE ${INTENTS} SET attempts = attempts + 1,
         status = CASE WHEN attempts + 1 >= $2 THEN 'stuck' ELSE status END,
         next_attempt_at = clock_timestamp() + make_interval(secs => least($3::float8 * power(2, attempts), $4::float8) / 1000),
         updated_at = clock_timestamp()
       WHERE operation_uuid=$1 AND status='pending' RETURNING status`,
      [operationUuid, MAX_SEND_ATTEMPTS, RETRY_BASE_MS, RETRY_CAP_MS],
    );
    if (r.rows[0]) return r.rows[0].status as IntentStatus;
    return (await this.getIntent(operationUuid))?.status ?? null;
  }
  async deferIntent(operationUuid: string, delayMs: number) {
    await this.db.query(
      `UPDATE ${INTENTS} SET next_attempt_at = clock_timestamp() + make_interval(secs => $2::float8 / 1000), updated_at = clock_timestamp()
       WHERE operation_uuid=$1 AND status='pending'`,
      [operationUuid, delayMs],
    );
  }
  async markRefused(operationUuid: string, code: string) {
    await this.db.transaction(async (tx) => {
      const r = await tx.query<Row>(
        `UPDATE ${INTENTS} SET status='refused', refusal_code=$2, updated_at=clock_timestamp() WHERE operation_uuid=$1 AND status='pending' RETURNING kind`,
        [operationUuid, code],
      );
      if (r.rows[0]?.kind === "install")
        await tx.query(`UPDATE ${INTENTS} SET status='void', updated_at=clock_timestamp() WHERE kind='revoke' AND target_grant_id=$1 AND status='reserved'`, [operationUuid]);
    });
  }
  pendingRebindFor(grantId: string, exceptOperationUuid: string) {
    return this.one(
      `SELECT ${INTENT_COLUMNS} FROM ${INTENTS} WHERE kind='rebind' AND target_grant_id=$1 AND status='pending' AND operation_uuid<>$2 LIMIT 1`,
      [grantId, exceptOperationUuid],
    );
  }
  async currentOrigin(grantId: string) {
    const r = await this.db.query<Row>(
      `SELECT origin_id FROM ${INTENTS} WHERE status='applied' AND ((kind='install' AND operation_uuid=$1) OR (kind='rebind' AND target_grant_id=$1))
       ORDER BY updated_at DESC, created_at DESC LIMIT 1`,
      [grantId],
    );
    return strOrNull(r.rows[0]?.origin_id);
  }
  async grantsOfInstallation(installationId: string, grantIds: readonly string[]) {
    if (grantIds.length === 0) return new Set<string>();
    const r = await this.db.query<Row>(
      `SELECT operation_uuid::text AS g FROM ${INTENTS} WHERE kind='install' AND installation_id=$1 AND operation_uuid = ANY($2::uuid[])`,
      [installationId, grantIds],
    );
    return new Set(r.rows.map((row) => String(row.g)));
  }
  async retireInstallation(installationId: string) {
    return this.db.transaction(async (tx) => {
      await tx.query(`UPDATE ${INSTALLATIONS} SET retired_at=COALESCE(retired_at, clock_timestamp()) WHERE installation_id=$1`, [installationId]);
      const r = await tx.query(
        `UPDATE ${INTENTS} SET status='pending', activation_reason='installation_retired', updated_at=clock_timestamp()
         WHERE kind='revoke' AND installation_id=$1 AND status='reserved' RETURNING operation_uuid`,
        [installationId],
      );
      return r.rows.length;
    });
  }
  async dueIntents(limit: number) {
    const r = await this.db.query<Row>(
      `SELECT ${INTENT_COLUMNS} FROM ${INTENTS} WHERE status='pending' AND next_attempt_at <= clock_timestamp()
       ORDER BY next_attempt_at, created_at, operation_uuid LIMIT $1`,
      [limit],
    );
    return r.rows.map(intentRow);
  }
  async outstandingIssuerKeyIds() {
    const r = await this.db.query<Row>(
      `SELECT DISTINCT substring(convert_from(assertion, 'UTF8') from '[^' || chr(31) || ']*$') AS issuer_key_id
         FROM ${INTENTS} WHERE status IN ('reserved','pending')`, []);
    return new Set(r.rows.map((row) => String(row.issuer_key_id)));
  }
}
