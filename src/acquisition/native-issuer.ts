/**
 * The native issuer (contract P3 + addendum P3a, design §4.1/§4.2/§4.7).
 *
 * History-owned: derives org/workspace/user from the introspected token only,
 * mints installationId and G, keeps the lineage state (revision, epoch,
 * predecessor) itself and signs A with its own pinned native-class Ed25519 key.
 *
 * Installation possession (P3a): registration stores only
 *   proofSha256 = SHA-256("96bulls:acquisition-installation-proof-v1" US orgId US workspaceId US userId US proofBytes)
 * and every other route proves the installation (constant-time) BEFORE any
 * SQL or intent read. The proof, G and the private key are never returned
 * (except G to its own installation), echoed or logged.
 *
 * Durable cleanup (correction 5): when an install applies, a RESERVED revoke
 * of the same grant is pre-signed against the ownerEpoch the install RESULT
 * reports (never an assumed epoch+1) and commits with the applied mark. A
 * result without a usable epoch gets no reservation (recorded); an explicit
 * revoke then signs against the authority's current row. Activation
 * (explicit exclude, session removal, installation retirement) is a local
 * row update. The reconciler resends every due pending intent (install,
 * rebind, revoke; revokes never while still selected) with capped backoff. A
 * failed send is never terminal (the SQL may have committed): after
 * MAX_SEND_ATTEMPTS the intent is only stalled (alerted; still retried, still
 * holding its key; an explicit revoke of a stalled install may proceed).
 * Every send holds the intent's store lease (claimSend), so concurrent
 * replays and reconcilers never send one intent twice at once; every
 * completion presents the lease token it claimed, so a sender whose lease
 * expired and was re-claimed cannot clear the successor's lease (a stale
 * completion is a no-op, counted and reported via onStaleCompletion).
 * An install whose result yields no reserved revoke is not applied: it stays
 * pending as cleanup debt and is resent until its revoke is reserved.
 *
 * Cancellation ordering: retirement and session removal of a still-pending
 * install record a cancel on it (store-serialized with its apply), so the
 * revoke is created active when its outcome arrives; a claim whose
 * installation is retired after proof is refused by createInstallIntent.
 */
import { createHash, randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import {
  AcquisitionAssertionError,
  acquisitionAssertionBytes,
  acquisitionAssertionDigests,
  acquisitionGrantDigest,
  acquisitionPayload,
} from "../session-ingest/acquisition-assertion.ts";
import { isIdentifier, isUuid, UNIT_SEPARATOR, type AcquisitionKind, type GrantScope } from "./validation.ts";
import {
  AcquisitionSql,
  AcquisitionSqlError,
  AcquisitionUnavailableError,
  isRetryableCode,
  MAX_STATUS_SESSIONS,
  SQL_ERROR_CODES,
  type StatusRow,
} from "./installer.ts";
import type { IssuerSigner } from "./key-registry.ts";
import {
  decryptGrant,
  encryptGrant,
  MAX_SEND_ATTEMPTS,
  RETRY_BASE_MS,
  SEND_LEASE_MS,
  StoreConflict,
  type AcquisitionStore,
  type ActivationReason,
  type InstallationRecord,
  type IntentKind,
  type IntentRecord,
  type NewIntent,
  type Reservation,
} from "./intent-store.ts";

export const INSTALLATION_PROOF_DOMAIN = "96bulls:acquisition-installation-proof-v1";
export const INSTALLATION_PROOF_HEADER = "x-acquisition-installation-proof";

/** The caller as introspected (never from the body). */
export interface NativeIdentity {
  readonly orgId: string;
  readonly workspaceId: string;
  readonly userId: string;
}

/** A native-route refusal with its exact HTTP answer. */
export class NativeRefusal extends Error {
  constructor(readonly status: 400 | 403 | 409, readonly body: { error: Record<string, unknown> }) {
    super(String(body.error.code));
    this.name = "NativeRefusal";
  }
  get code(): string {
    return String(this.body.error.code);
  }
}
const invalidRequest = () => new NativeRefusal(400, { error: { code: "invalid_request" } });
const notProven = () => new NativeRefusal(403, { error: { code: "installation_not_proven" } });
// Route-level ownership refusals (F8): `not_authorized` is the bearer scope check only (app.ts).
/** The grant was not issued to this installation (unknown, not an install, another installation). */
const notInstaller = () => new NativeRefusal(403, { error: { code: "not_installer" } });
/** The installation cannot prove ownership of this grant (not applied/refused, no revocable state, wrong G). */
const notOwner = () => new NativeRefusal(403, { error: { code: "not_owner" } });
/** A SQL refusal on a native route: 22023 -> 400 {code}, 42501 -> 403 {code}, else 409 acquisition_refused. */
const refused = (reason: string) =>
  SQL_ERROR_CODES["22023"]!.includes(reason)
    ? new NativeRefusal(400, { error: { code: reason } })
    : SQL_ERROR_CODES["42501"]!.includes(reason)
      ? new NativeRefusal(403, { error: { code: reason } })
      : new NativeRefusal(409, { error: { code: "acquisition_refused", reason, retryable: isRetryableCode(reason) } });

// ---------------------------------------------------------------- proof

/** base64url (unpadded, canonical) of exactly 32 bytes, else null. */
export function decodeInstallationProof(value: unknown): Buffer | null {
  if (typeof value !== "string" || value.length !== 43 || !/^[A-Za-z0-9_-]+$/.test(value)) return null;
  const bytes = Buffer.from(value, "base64url");
  return bytes.byteLength === 32 && bytes.toString("base64url") === value ? bytes : null;
}

export function installationProofSha256(identity: NativeIdentity, proof: Uint8Array): string {
  const prefix = Buffer.from([INSTALLATION_PROOF_DOMAIN, identity.orgId, identity.workspaceId, identity.userId, ""].join(UNIT_SEPARATOR), "utf8");
  return createHash("sha256").update(Buffer.concat([prefix, proof])).digest("hex");
}

function sameHex(a: string, b: string): boolean {
  const x = Buffer.from(a, "utf8");
  const y = Buffer.from(b, "utf8");
  return x.byteLength === y.byteLength && timingSafeEqual(x, y);
}

// ---------------------------------------------------------------- bodies

function exactBody(body: unknown, required: readonly string[], optional: readonly string[] = []): Record<string, unknown> {
  if (body === null || typeof body !== "object" || Array.isArray(body)) throw invalidRequest();
  const b = body as Record<string, unknown>;
  const keys = Object.keys(b);
  if (!required.every((k) => k in b) || !keys.every((k) => required.includes(k) || optional.includes(k))) throw invalidRequest();
  return b;
}
const uuidField = (b: Record<string, unknown>, k: string): string => {
  const v = b[k];
  if (typeof v !== "string" || !isUuid(v)) throw invalidRequest();
  return v;
};
const idField = (b: Record<string, unknown>, k: string): string => {
  const v = b[k];
  if (typeof v !== "string" || !isIdentifier(v)) throw invalidRequest();
  return v;
};

/** The installationId a route names, validated before the proof check (no store access). */
export function requestedInstallationId(body: unknown): string {
  if (body === null || typeof body !== "object" || Array.isArray(body)) throw invalidRequest();
  return uuidField(body as Record<string, unknown>, "installationId");
}

/** A's field order (§3, §4.7); payloadOf re-encodes through the peer codec, so drift fails closed. */
const A_FIELDS: Record<AcquisitionKind, readonly string[]> = {
  install: ["operationUuid", "orgId", "workspaceId", "source", "sessionId", "revision", "expectedRevision", "expectedOwnerEpoch",
    "expectedPredecessorGrantId", "ownerKind", "ownerUserId", "ownerOriginId", "bindingUuid", "installationId", "grantSha256", "expiresAt", "issuerKeyId"],
  revoke: ["operationUuid", "orgId", "workspaceId", "source", "sessionId", "revision", "targetGrantId", "targetOwnerEpoch",
    "targetOwnerKind", "targetBindingUuid", "targetInstallationId", "issuerKeyId"],
  rebind: ["operationUuid", "orgId", "workspaceId", "source", "sessionId", "revision", "targetGrantId", "targetOwnerEpoch",
    "ownerUserId", "installationId", "oldOriginId", "newOriginId", "issuerKeyId"],
};

/** The stored A back to its payload (the stored bytes are the only source on retry). */
function payloadOf(kind: AcquisitionKind, A: Uint8Array): Record<string, string> {
  const parts = Buffer.from(A).toString("utf8").split(UNIT_SEPARATOR);
  const fields = A_FIELDS[kind];
  if (parts.length !== fields.length + 1) throw new Error("stored assertion is malformed");
  const payload = Object.fromEntries(fields.map((f, i) => [f, parts[i + 1]!]));
  if (!acquisitionAssertionBytes(kind, payload).equals(Buffer.from(A))) throw new Error("stored assertion is malformed");
  return payload;
}

const MAX_BIGINT = 9223372036854775807n;
const plusOne = (decimal: string) => (BigInt(decimal) + 1n).toString();
const asString = (v: unknown): string | null => (typeof v === "string" ? v : null);
/** A canonical non-negative int64 decimal string, else null. */
const int64 = (v: unknown): string | null => (typeof v === "string" && /^(0|[1-9][0-9]{0,18})$/.test(v) && BigInt(v) <= MAX_BIGINT ? v : null);
/** A canonical int64 decimal > 0, else null (0 is "never owned", not an epoch). */
const positiveEpoch = (v: unknown): string | null => {
  const e = int64(v);
  return e === null || e === "0" ? null : e;
};
/** A pending intent past MAX_SEND_ATTEMPTS failures: alerted and revocable, still retried (never terminal). */
const isStalled = (record: IntentRecord) => record.status === "pending" && record.attempts >= MAX_SEND_ATTEMPTS;

export interface NativeIssuerOptions {
  readonly store: AcquisitionStore;
  readonly sql: AcquisitionSql;
  readonly signer: IssuerSigner;
  /** ACQUISITION_INTENT_KEY, 32 bytes. */
  readonly intentKey: Uint8Array;
  /** Clock (ms) for the retry gate; must match the store's clock. Default Date.now. */
  readonly now?: () => number;
  /** A completion whose lease was superseded (no-op). For logging/metrics; must not throw. */
  readonly onStaleCompletion?: (event: StaleCompletion) => void;
}

/** A send completion that found its lease superseded by another sender; nothing was written. */
export interface StaleCompletion {
  readonly kind: IntentKind;
  readonly completion: "applied" | "retry" | "refused";
}

interface SendOutcome {
  /** The intent after the completion (re-read). */
  readonly record: IntentRecord;
  /** The completion was refused by the lease CAS (another sender holds the intent now). */
  readonly stale: boolean;
  /** The send failed (counted by markRetry unless stale); rethrown on the request path. */
  readonly failure: { error: unknown } | null;
}

export class NativeIssuer {
  private readonly store: AcquisitionStore;
  private readonly sql: AcquisitionSql;
  private readonly now: () => number;
  private readonly signer: IssuerSigner;
  private readonly intentKey: Uint8Array;
  private readonly onStaleCompletion: ((event: StaleCompletion) => void) | null;
  private staleCount = 0;

  constructor(options: NativeIssuerOptions) {
    if (options.intentKey.byteLength !== 32) throw new Error("96bulls: ACQUISITION_INTENT_KEY must be 32 bytes");
    this.store = options.store;
    this.sql = options.sql;
    this.signer = options.signer;
    this.intentKey = options.intentKey;
    this.now = options.now ?? Date.now;
    this.onStaleCompletion = options.onStaleCompletion ?? null;
  }

  /** Completions dropped by the lease CAS since start (each a no-op). */
  get staleCompletions(): number {
    return this.staleCount;
  }

  get issuerKeyId(): string {
    return this.signer.issuerKeyId;
  }

  // ------------------------------------------------------------ installations

  async registerInstallation(identity: NativeIdentity, body: unknown): Promise<{ installationId: string; originId: string; issuerKeyId: string }> {
    const b = exactBody(body, ["requestUuid", "originId", "installationProof"], ["label"]);
    const requestUuid = uuidField(b, "requestUuid");
    const originId = idField(b, "originId");
    const label = b.label === undefined ? null : idField(b, "label");
    const proof = decodeInstallationProof(b.installationProof);
    if (!proof) throw invalidRequest();
    const proofSha256 = installationProofSha256(identity, proof);
    const { record } = await this.store.registerInstallation({
      installationId: randomUUID(), requestUuid, orgId: identity.orgId, workspaceId: identity.workspaceId, userId: identity.userId,
      originId, label, proofSha256, retiredAt: null,
    });
    // A replay must match the whole binding; the proof hash covers org/ws/user.
    if (!sameHex(record.proofSha256, proofSha256) || record.originId !== originId)
      throw new NativeRefusal(409, { error: { code: "installation_proof_mismatch", retryable: false } });
    return { installationId: record.installationId, originId: record.originId, issuerKeyId: this.signer.issuerKeyId };
  }

  /**
   * P3a: the only store read allowed before the proof is verified is the
   * installation row itself. Unknown, retired, other-principal or mismatched
   * proof are all the same 403.
   */
  async proveInstallation(identity: NativeIdentity, installationId: string, proofHeader: string | null | undefined): Promise<InstallationRecord> {
    const proof = decodeInstallationProof(proofHeader);
    if (!proof || !isUuid(installationId)) throw notProven();
    const expected = installationProofSha256(identity, proof);
    const record = await this.store.getInstallation(installationId);
    if (!record) {
      sameHex(expected, expected); // keep the unknown path's work comparable
      throw notProven();
    }
    const match = sameHex(record.proofSha256, expected);
    if (!match || record.retiredAt !== null || record.orgId !== identity.orgId || record.workspaceId !== identity.workspaceId || record.userId !== identity.userId)
      throw notProven();
    return record;
  }

  // ------------------------------------------------------------ claims

  async claim(identity: NativeIdentity, installation: InstallationRecord, body: unknown): Promise<Record<string, unknown>> {
    const b = exactBody(body, ["operationUuid", "installationId", "source", "sessionId", "originId"]);
    const operationUuid = uuidField(b, "operationUuid");
    const source = idField(b, "source");
    const sessionId = idField(b, "sessionId");
    const originId = idField(b, "originId");
    if (b.installationId !== installation.installationId) throw notProven();
    const scope: GrantScope = { orgId: identity.orgId, workspaceId: identity.workspaceId, source, sessionId };

    const existing = await this.store.getIntent(operationUuid);
    if (existing) return this.replayClaim(existing, installation, scope, originId);

    const [row] = await this.sql.status({ ...scope, sessionIds: [sessionId] });
    const currentRevision = row?.revision ?? "0";
    // Server-internal: a never-owned session (status epoch null) is expected at epoch 0 by the SQL.
    const currentEpoch = row?.ownerEpoch ?? "0";
    const revision = plusOne(currentRevision);
    // The install advances the epoch; at int64 max it cannot (nor could its revoke target it).
    if (BigInt(currentEpoch) >= MAX_BIGINT) throw invalidRequest();
    const G = `rhg_${randomBytes(32).toString("hex")}`;
    const install = acquisitionPayload("install", {
      operationUuid, orgId: scope.orgId, workspaceId: scope.workspaceId, source, sessionId, revision,
      expectedRevision: currentRevision, expectedOwnerEpoch: currentEpoch, expectedPredecessorGrantId: row?.grantId ?? "",
      ownerKind: "native", ownerUserId: identity.userId, ownerOriginId: originId, bindingUuid: "",
      installationId: installation.installationId, grantSha256: acquisitionGrantDigest(scope, operationUuid, G), expiresAt: "",
      issuerKeyId: this.signer.issuerKeyId,
    });
    const sealed = encryptGrant(this.intentKey, G, operationUuid, installation.installationId);
    // The reserved revoke is signed only once the install result names the epoch (send -> reserveRevoke).
    let created: { record: IntentRecord; created: boolean };
    try {
      // Refused if the installation was retired while status was awaited (after proveInstallation).
      created = await this.store.createInstallIntent(this.newIntent("install", install, scope, installation.installationId, null, originId, sealed));
    } catch (error) {
      if (error instanceof StoreConflict && error.code === "installation_retired") throw notProven();
      throw error;
    }
    if (!created.created) return this.replayClaim(created.record, installation, scope, originId);
    return this.claimAnswer(await this.sendLeased(created.record), installation);
  }

  private async replayClaim(existing: IntentRecord, installation: InstallationRecord, scope: GrantScope, originId: string) {
    const same =
      existing.kind === "install" && existing.installationId === installation.installationId && existing.orgId === scope.orgId &&
      existing.workspaceId === scope.workspaceId && existing.source === scope.source && existing.sessionId === scope.sessionId &&
      existing.originId === originId;
    if (!same) throw refused("operation_conflict");
    const wasApplied = existing.status === "applied";
    return this.claimAnswer(await this.sendLeased(existing), installation, wasApplied);
  }

  /** `replay`: the install had already been applied before this request, so the answer is an exact replay. */
  private claimAnswer(record: IntentRecord, installation: InstallationRecord, replay = false): Record<string, unknown> {
    if (record.status === "refused") throw refused(record.refusalCode ?? "operation_conflict");
    if (record.status !== "applied" || !record.result || !record.grantCiphertext || !record.grantNonce) throw new AcquisitionUnavailableError();
    if (record.installationId !== installation.installationId) throw refused("operation_conflict");
    const result = JSON.parse(record.result) as Record<string, unknown>;
    // A success epoch is strictly positive (Desktop rejects 0); an applied install without one is not answered.
    if (positiveEpoch(result.ownerEpoch) === null) throw new AcquisitionUnavailableError();
    const G = decryptGrant(this.intentKey, record.grantCiphertext, record.grantNonce, record.operationUuid, record.installationId);
    return {
      outcome: replay ? "replay" : asString(result.outcome) ?? "installed",
      grantId: asString(result.grantId) ?? record.operationUuid,
      ownerEpoch: asString(result.ownerEpoch),
      revision: asString(result.revision),
      expiresAt: null,
      grant: G,
    };
  }

  // ------------------------------------------------------------ revocations

  async revoke(identity: NativeIdentity, installation: InstallationRecord, body: unknown): Promise<Record<string, unknown>> {
    const b = exactBody(body, ["operationUuid", "installationId", "source", "sessionId", "targetGrantId"]);
    const clientOperationUuid = uuidField(b, "operationUuid");
    const source = idField(b, "source");
    const sessionId = idField(b, "sessionId");
    const targetGrantId = uuidField(b, "targetGrantId");
    if (b.installationId !== installation.installationId) throw notProven();

    const prior = await this.store.findByClientOperation(clientOperationUuid);
    if (prior && (prior.targetGrantId !== targetGrantId || prior.installationId !== installation.installationId)) throw refused("operation_conflict");
    if (!prior && (await this.store.getIntent(clientOperationUuid))) throw refused("operation_conflict");

    const install = await this.ownedInstall(identity, installation, targetGrantId, source, sessionId, true);
    const reserved = await this.store.reservedRevokeFor(install.operationUuid);
    let activated: IntentRecord;
    try {
      if (reserved && reserved.status !== "void") activated = await this.store.activateRevoke(reserved.operationUuid, "explicit_exclude", clientOperationUuid);
      else if (!reserved && (install.status === "pending" || install.reservation !== "signed")) activated = await this.unreservedRevoke(install, clientOperationUuid);
      else throw notOwner(); // void reservation (refused install) or applied without a revoke row: nothing to revoke
    } catch (error) {
      if (error instanceof StoreConflict) throw refused("operation_conflict");
      // A stalled install whose grant has not landed (yet): record the exclude so a late apply
      // creates its revoke active, then answer grant_not_installed as before.
      if (error instanceof NativeRefusal && error.body.error.reason === "grant_not_installed" && install.status === "pending")
        await this.store.cancelGrant(install.installationId, install.operationUuid, "explicit_exclude");
      throw error;
    }
    const record = await this.sendLeased(activated);
    if (record.status === "refused") throw refused(record.refusalCode ?? "operation_conflict");
    if (record.status !== "applied" || !record.result) throw new AcquisitionUnavailableError();
    const r = JSON.parse(record.result) as Record<string, unknown>;
    const outcome = prior ? "replay" : asString(r.outcome);
    return { outcome, revision: asString(r.revision), ownerEpoch: asString(r.ownerEpoch), state: asString(r.state) };
  }

  /**
   * The applied install of `grantId` issued to this installation for this
   * session, else 403/409. `allowStalled` (revocation only): a stalled pending
   * install is returned too, since its grant may have landed and must stay
   * revocable; a pending one not yet stalled is 409 session_busy (retryable).
   */
  private async ownedInstall(identity: NativeIdentity, installation: InstallationRecord, grantId: string, source: string, sessionId: string, allowStalled = false): Promise<IntentRecord> {
    const install = await this.store.getIntent(grantId);
    if (!install || install.kind !== "install" || install.installationId !== installation.installationId) throw notInstaller();
    if (install.orgId !== identity.orgId || install.workspaceId !== identity.workspaceId || install.source !== source || install.sessionId !== sessionId)
      throw new NativeRefusal(403, { error: { code: "target_mismatch" } });
    if (install.status === "pending" && allowStalled && isStalled(install)) return install;
    if (install.status === "pending") throw refused("session_busy");
    if (install.status !== "applied") throw notOwner();
    return install;
  }

  /**
   * Revoke a grant that has no reserved revoke (its install is stalled, or its
   * result carried no epoch): sign now against the authority's current row,
   * only if that row is this grant (else 409 grant_not_installed).
   */
  private async unreservedRevoke(install: IntentRecord, clientOperationUuid: string): Promise<IntentRecord> {
    const scope: GrantScope = { orgId: install.orgId, workspaceId: install.workspaceId, source: install.source, sessionId: install.sessionId };
    const [row] = await this.sql.status({ ...scope, sessionIds: [install.sessionId] });
    if (!row || row.grantId !== install.operationUuid || row.ownerEpoch === null) throw refused("grant_not_installed");
    const payload = acquisitionPayload("revoke", {
      operationUuid: randomUUID(), orgId: scope.orgId, workspaceId: scope.workspaceId, source: scope.source, sessionId: scope.sessionId,
      revision: plusOne(row.revision), targetGrantId: install.operationUuid, targetOwnerEpoch: row.ownerEpoch,
      targetOwnerKind: "native", targetBindingUuid: "", targetInstallationId: install.installationId, issuerKeyId: this.signer.issuerKeyId,
    });
    return this.store.createRevokeIntent(this.newIntent("revoke", payload, scope, install.installationId, install.operationUuid, null, null), clientOperationUuid);
  }

  // ------------------------------------------------------------ rebinds (§4.7)

  async rebind(identity: NativeIdentity, installation: InstallationRecord, body: unknown): Promise<Record<string, unknown>> {
    const b = exactBody(body, ["operationUuid", "installationId", "source", "sessionId", "grantId", "originId", "grant"]);
    const operationUuid = uuidField(b, "operationUuid");
    const source = idField(b, "source");
    const sessionId = idField(b, "sessionId");
    const grantId = uuidField(b, "grantId");
    const newOriginId = idField(b, "originId");
    if (typeof b.grant !== "string" || !/^rhg_[0-9a-f]{64}$/.test(b.grant)) throw invalidRequest();
    if (b.installationId !== installation.installationId) throw notProven();

    const install = await this.ownedInstall(identity, installation, grantId, source, sessionId);
    const G = decryptGrant(this.intentKey, install.grantCiphertext!, install.grantNonce!, install.operationUuid, install.installationId);
    if (!sameHex(G, b.grant)) throw notOwner(); // §4.7: the installation must prove G

    const existing = await this.store.getIntent(operationUuid);
    if (existing) {
      if (existing.kind !== "rebind" || existing.installationId !== installation.installationId || existing.targetGrantId !== grantId || existing.originId !== newOriginId)
        throw refused("operation_conflict");
      return this.rebindAnswer(await this.sendLeased(existing), true);
    }
    if (await this.store.pendingRebindFor(grantId, operationUuid)) throw refused("session_busy");
    const oldOriginId = await this.store.currentOrigin(grantId);
    if (!oldOriginId) throw new AcquisitionUnavailableError();
    if (oldOriginId === newOriginId) throw invalidRequest();

    const scope: GrantScope = { orgId: identity.orgId, workspaceId: identity.workspaceId, source, sessionId };
    const [row] = await this.sql.status({ ...scope, sessionIds: [sessionId] });
    if (!row || row.ownerEpoch === null) throw new AcquisitionUnavailableError(); // never owned: nothing to rebind yet
    const payload = acquisitionPayload("rebind", {
      operationUuid, orgId: scope.orgId, workspaceId: scope.workspaceId, source, sessionId, revision: plusOne(row.revision),
      targetGrantId: grantId, targetOwnerEpoch: row.ownerEpoch, ownerUserId: identity.userId, installationId: installation.installationId,
      oldOriginId, newOriginId, issuerKeyId: this.signer.issuerKeyId,
    });
    const { record, created } = await this.store.createRebindIntent(this.newIntent("rebind", payload, scope, installation.installationId, grantId, newOriginId, null));
    if (!created && (record.installationId !== installation.installationId || record.originId !== newOriginId)) throw refused("operation_conflict");
    return this.rebindAnswer(await this.sendLeased(record), !created);
  }

  private rebindAnswer(record: IntentRecord, replay: boolean): Record<string, unknown> {
    if (record.status === "refused") throw refused(record.refusalCode ?? "operation_conflict");
    if (record.status !== "applied" || !record.result) throw new AcquisitionUnavailableError();
    const r = JSON.parse(record.result) as Record<string, unknown>;
    if (positiveEpoch(r.ownerEpoch) === null) throw new AcquisitionUnavailableError(); // success epochs are strictly positive
    return { outcome: replay ? "replay" : asString(r.outcome), revision: asString(r.revision), ownerEpoch: asString(r.ownerEpoch) };
  }

  // ------------------------------------------------------------ status

  /** Query: installationId, source, and one id per repeated sessionIds (or sessionId) parameter; never comma-split. */
  async status(identity: NativeIdentity, installation: InstallationRecord, query: { source: string | null; sessionIds: string[] }): Promise<{ sessions: Array<StatusRow & { mine: boolean }> }> {
    const { source, sessionIds } = query;
    if (!source || !isIdentifier(source)) throw invalidRequest();
    if (sessionIds.length === 0 || sessionIds.length > MAX_STATUS_SESSIONS || new Set(sessionIds).size !== sessionIds.length || !sessionIds.every(isIdentifier))
      throw invalidRequest();
    const rows = await this.sql.status({ orgId: identity.orgId, workspaceId: identity.workspaceId, source, sessionIds });
    const grantIds = rows.map((r) => r.grantId).filter((g): g is string => typeof g === "string");
    const mine = await this.store.grantsOfInstallation(installation.installationId, grantIds);
    return {
      sessions: rows.map((r) => {
        // A null (never-owned) epoch never qualifies as this installation's active grant.
        const own = r.grantId !== null && r.ownerEpoch !== null && mine.has(r.grantId);
        // Another installation's or principal's grant id is not disclosed (§4.7 "owner undisclosed").
        return { ...r, grantId: own ? r.grantId : null, mine: own };
      }),
    };
  }

  // ------------------------------------------------------------ durable cleanup

  /**
   * Membership removal: activate the grant's reserved revoke, or, while its
   * install is still pending (in flight, lost response, stalled), record the
   * removal on the install so its revoke is created active when the outcome
   * arrives. Local only: no network, no private key.
   */
  async removeSession(installationId: string, grantId: string): Promise<boolean> {
    return (await this.store.cancelGrant(installationId, grantId, "session_removed")) !== "none";
  }

  /** Installation retirement, one transaction: retire, cancel its pending installs, activate its reserved revokes. */
  retireInstallation(installationId: string): Promise<number> {
    return this.store.retireInstallation(installationId);
  }

  /**
   * Resend due pending intents (install, rebind, revoke) exactly as stored,
   * oldest-due first, at most `limit` per pass, one attempt each. A failure
   * backs the intent off behind the queue (capped) and keeps it pending;
   * `stalled` counts those of this pass that have failed MAX_SEND_ATTEMPTS or
   * more times (the caller alerts). An intent leased by another sender is
   * skipped. A pending install is resent even if cancelled (its SQL may have
   * committed; a resend replays the outcome and activates its revoke). A
   * revoke whose installation is still selected for that session is skipped
   * (deferred, not counted as a failure): the default `stillSelected` is true
   * unless the installation is retired or the revoke was activated by an
   * explicit exclude or a session removal.
   */
  async reconcile(options: { limit?: number; stillSelected?: (record: IntentRecord) => Promise<boolean> } = {}): Promise<{ sent: number; skipped: number; retry: number; stalled: number; stale: number }> {
    const stillSelected = options.stillSelected ?? (async (record: IntentRecord) => {
      if (record.activationReason === "explicit_exclude" || record.activationReason === "session_removed") return false;
      const installation = await this.store.getInstallation(record.installationId);
      return installation !== null && installation.retiredAt === null;
    });
    let sent = 0, skipped = 0, retry = 0, stalled = 0, stale = 0;
    for (const record of await this.store.dueIntents(options.limit ?? 100)) {
      if (record.status !== "pending") continue;
      if (record.kind === "revoke" && (await stillSelected(record))) {
        await this.store.deferIntent(record.operationUuid, RETRY_BASE_MS); // behind the queue: no head-of-line block
        skipped += 1;
        continue;
      }
      const leased = await this.store.claimSend(record.operationUuid, SEND_LEASE_MS);
      if (!leased) {
        skipped += 1; // another sender holds it (or it was answered meanwhile)
        continue;
      }
      let outcome: SendOutcome;
      try {
        outcome = await this.send(leased);
      } catch {
        // A store error while recording (e.g. a reserved-revoke uuid collision): same bytes next pass.
        const current = await this.store.getIntent(record.operationUuid);
        outcome = { record: current ?? leased, stale: false, failure: null };
      }
      if (outcome.stale) {
        stale += 1; // another sender took the lease over after ours expired: its completion counts, not ours
        continue;
      }
      const after = outcome.record;
      if (after.status === "pending") {
        retry += 1; // a failed send, or cleanup debt (no reserved revoke yet); same bytes next pass
        if (isStalled(after)) stalled += 1;
      } else sent += 1;
    }
    return { sent, skipped, retry, stalled, stale };
  }

  // ------------------------------------------------------------ internals

  private newIntent(kind: AcquisitionKind, payload: Record<string, string>, scope: GrantScope, installationId: string, targetGrantId: string | null, originId: string | null, sealed: { ciphertext: Buffer; nonce: Buffer } | null): NewIntent {
    const assertion = acquisitionAssertionBytes(kind, payload);
    const signature = this.signer.sign(assertion);
    const digests = acquisitionAssertionDigests(assertion, signature);
    return {
      operationUuid: payload.operationUuid!, kind, installationId, orgId: scope.orgId, workspaceId: scope.workspaceId,
      source: scope.source, sessionId: scope.sessionId, targetGrantId, originId, assertion, signature,
      evidenceSha256: digests.evidenceSha256, requestSha256: digests.requestSha256,
      grantCiphertext: sealed?.ciphertext ?? null, grantNonce: sealed?.nonce ?? null,
    };
  }

  /**
   * Request-path send under the intent's store lease (claimSend). Not sent, and
   * answered 503 acquisition_unavailable (retryable) without counting an
   * attempt, when another request or reconciler holds the lease, or when it
   * already failed and is not yet due (client replays must not outrun the
   * backoff). If another sender answered it meanwhile, that answer is returned.
   */
  private async sendLeased(record: IntentRecord): Promise<IntentRecord> {
    if (record.status !== "pending") return record;
    const leased = await this.store.claimSend(record.operationUuid, SEND_LEASE_MS);
    if (!leased) {
      const current = await this.store.getIntent(record.operationUuid);
      if (current && current.status !== "pending") return current;
      throw new AcquisitionUnavailableError();
    }
    const outcome = await this.send(leased);
    if (outcome.failure) throw outcome.failure.error;
    return outcome.record;
  }

  /**
   * Send a pending intent's stored A/S unchanged (never re-signed) and record
   * the outcome: a result -> applied (an install also reserves its revoke);
   * a non-retryable refusal -> refused (terminal); anything else (a bad
   * stored intent, session_busy, transport) is counted by markRetry (backoff,
   * still pending) and surfaced. An install whose result yields no reserved
   * revoke stays pending (cleanup debt, counted and backed off by the store).
   * The caller holds the intent's lease (`record.leaseToken`, from claimSend);
   * every completion presents it, and a superseded one is a no-op (stale).
   */
  private async send(record: IntentRecord): Promise<SendOutcome> {
    const token = record.leaseToken;
    if (record.status !== "pending" || token === null) throw new Error("send requires a leased pending intent");
    const done = async (completion: StaleCompletion["completion"], written: string, failure: SendOutcome["failure"]): Promise<SendOutcome> => {
      const stale = written === "stale";
      if (stale) this.noteStale({ kind: record.kind, completion });
      return { record: (await this.store.getIntent(record.operationUuid))!, stale, failure };
    };
    let result: Record<string, unknown>;
    try {
      const payload = payloadOf(record.kind, record.assertion);
      const digests = acquisitionAssertionDigests(record.assertion, record.signature);
      if (digests.evidenceSha256 !== record.evidenceSha256 || digests.requestSha256 !== record.requestSha256) throw new Error("stored intent digests do not match");
      result = await this.sql.mutate(record.kind, { ...payload, evidenceSha256: digests.evidenceSha256, requestSha256: digests.requestSha256 });
    } catch (error) {
      if (error instanceof AcquisitionSqlError && !isRetryableCode(error.code))
        return done("refused", await this.store.markRefused(record.operationUuid, token, error.code), null);
      return done("retry", await this.store.markRetry(record.operationUuid, token), { error });
    }
    if (record.kind === "install") {
      const { revoke, reservation } = this.reserveRevoke(record, result);
      return done("applied", await this.store.markInstallApplied(record.operationUuid, token, JSON.stringify(result), revoke, reservation), null);
    }
    return done("applied", await this.store.markApplied(record.operationUuid, token, JSON.stringify(result)), null);
  }

  private noteStale(event: StaleCompletion): void {
    this.staleCount += 1;
    try {
      this.onStaleCompletion?.(event);
    } catch {
      // observability only
    }
  }

  /**
   * The install's reserved revoke, pre-signed against the epoch the install
   * RESULT reports (correction: never an assumed epoch+1). No usable epoch ->
   * no pre-signature, recorded as `no_epoch`; a signer failure -> `sign_failed`.
   * Revision: one past the result's (else the install's); the SQL takes
   * greatest(revision, p.revision), so a trailing value is harmless.
   */
  private reserveRevoke(install: IntentRecord, result: Record<string, unknown>): { revoke: NewIntent | null; reservation: Reservation } {
    const epoch = positiveEpoch(result.ownerEpoch); // an install always lands at an epoch >= 1
    if (epoch === null) return { revoke: null, reservation: "no_epoch" };
    const payload = payloadOf("install", install.assertion);
    const revision = int64(result.revision) ?? payload.revision!;
    if (BigInt(revision) >= MAX_BIGINT) return { revoke: null, reservation: "no_epoch" };
    const scope: GrantScope = { orgId: install.orgId, workspaceId: install.workspaceId, source: install.source, sessionId: install.sessionId };
    try {
      const revoke = acquisitionPayload("revoke", {
        operationUuid: randomUUID(), orgId: scope.orgId, workspaceId: scope.workspaceId, source: scope.source, sessionId: scope.sessionId,
        revision: plusOne(revision), targetGrantId: install.operationUuid, targetOwnerEpoch: epoch,
        targetOwnerKind: "native", targetBindingUuid: "", targetInstallationId: install.installationId, issuerKeyId: this.signer.issuerKeyId,
      });
      return { revoke: this.newIntent("revoke", revoke, scope, install.installationId, install.operationUuid, null, null), reservation: "signed" };
    } catch {
      return { revoke: null, reservation: "sign_failed" };
    }
  }
}

export { AcquisitionAssertionError };
export type { ActivationReason };
