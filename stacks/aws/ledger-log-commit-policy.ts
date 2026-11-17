/**
 * IAM for the numbered-log ledger commit (INGEST_LEDGER_LOG_COMMIT, docs/design/s3-ledger-log-commit.md, Remaining 10),
 * written down in code behind the same flag, and NOT rendered today: the hosted runtime refuses
 * INGEST_LEDGER_LOG_COMMIT=on (src/session-ingest/upload-admission-hosted.ts, ledger_log_commit_unsupported), and the
 * stack input `transcriptStore.uploadAdmission.ledgerLogCommit` refuses "on" the same way (upload-inbox.ts
 * assertUploadAdmissionInputs). Lift both refusals together, in one reviewed change, and only once log mode is cleared
 * for production; then uploadAdmissionBucketStatements renders the consumer's statements below, and this file's other
 * exports say what else that change must carry. Nothing here is applied by hand (infra lives in code only).
 *
 * Who needs what (key layout: src/session-ingest/ledger-log.ts logKeys, ledger-log-materialize.ts, async-admission.ts):
 *   - the admission consumer AND the materializer (one process, the upload-admission consumer role on hosted):
 *       GET/PUT  every object of a stream: `<user dir>/ledger/log/o=<h>/` entries e/, checkpoints c/, the head hint,
 *                payloads p/, materializer markers m/, skip records s/, closed.json and the stream identity doc
 *                (stream.json); the user's stream index beside the streams (`<user dir>/ledger/log-index/o=<h>.json`,
 *                written create-if-absent before entry 0 and by every registration, ledger-log.ts logUserIndex); the
 *                receipt outcomes (`r=<receipt>/outcome-*.json`); the stream registry (`.ledger/log-streams/`);
 *       DELETE   payloads p/ (applied, erased or orphaned) and the admitted outcome a final skip retires;
 *       LIST     the bucket (the entry directories, m/, p/, the stream index, the registry): the consumer already holds
 *                UploadAdmissionListsForMissingKeys on the whole bucket;
 *       KMS      already covered by UploadAdmissionEncryptsAndReadsViaS3 (every object under TENANT_KEY_BASE).
 *     Its existing UploadAdmissionPromotes grant (GET/PUT on the m=srv_* layout) already covers the GET/PUTs; they are
 *     listed again here so this grant stands on its own if that one narrows.
 *   - the operator tool (scripts/ledger-log-stream.ts, an operator role outside the stack):
 *       GET the legacy batch docs (`ledger/batches/`, the drain check) and the log's control objects (entries,
 *       checkpoints, the head hint, closed.json, the identity doc; never the payloads p/, which hold session records);
 *       LIST the bucket, unconditionally: S3 answers 404 for a missing key only to a caller holding s3:ListBucket, and a
 *       GET/HEAD carries no s3:prefix, so a prefix-conditioned ListBucket would turn every missing read (entry 0 of a
 *       legacy stream, closed.json, the markers) into 403. Key names only; the GET grants stay narrow (never p/);
 *       GET/PUT the user's stream index (`ledger/log-index/`: the switch writes it before entry 0, the registry migration
 *       and `backfill-index` write it per registered stream; a HEAD of a missing index object needs the GET grant);
 *       PUT entry 0 (e/), closed.json, the identity doc, the registry; DELETE registry docs (the v1 registry migration);
 *       GET the deployment markers `.ledger/log-commit-on.json` and `.ledger/log-mode-nonprod.json`;
 *       KMS GenerateDataKey/Decrypt via S3 for those keys.
 *   - the bucket's OnlyTheEraserDeletes deny refuses every delete outside the inbox carve-out to anyone but the eraser
 *     and the GC. LEDGER_LOG_DELETE_CARVE_OUT is what it must additionally exempt for the consumer (and, for the
 *     registry, the operator role) before log mode can run on hosted; until then every payload delete would be denied.
 */
import { TENANT_KEY_BASE } from "./tenants.ts";

/** A user's admission directory (async-admission.ts admissionUserDir): `.admission-inbox/<org>/v1/ws=<ws>/m=srv_<h>`. */
export const LEDGER_LOG_USER_DIR = `${TENANT_KEY_BASE}/.admission-inbox/*/v1/ws=*/m=srv_*`;
/** Every object of every stream (entries, checkpoints, head hint, payloads, markers, skips, closed.json, stream.json). */
export const LEDGER_LOG_OBJECTS = `${LEDGER_LOG_USER_DIR}/ledger/log/*`;
/**
 * The user's stream index (ledger-log.ts logUserIndex, logKeysAt(...).index): one `o=<hash>.json` per stream, `{"v":1}`,
 * no id inside. Beside `ledger/log/`, not under it, so LEDGER_LOG_OBJECTS does not cover it.
 */
export const LEDGER_LOG_INDEX = `${LEDGER_LOG_USER_DIR}/ledger/log-index/*`;
/** Content-addressed payloads: deleted once applied, erased or orphaned. */
export const LEDGER_LOG_PAYLOADS = `${LEDGER_LOG_USER_DIR}/ledger/log/*/p/*`;
/** Entries, written by the consumer (every seq) and the operator tool (entry 0 only). */
export const LEDGER_LOG_ENTRIES = `${LEDGER_LOG_USER_DIR}/ledger/log/*/e/*`;
/** The close marker and the stream identity doc (written by the operator tool). */
export const LEDGER_LOG_CLOSED = `${LEDGER_LOG_USER_DIR}/ledger/log/*/closed.json`;
export const LEDGER_LOG_IDENTITY = `${LEDGER_LOG_USER_DIR}/ledger/log/*/stream.json`;
/** The stream registry the sweeper lists: hash-keyed pointers only (no raw user or origin id). */
export const LEDGER_LOG_REGISTRY = `${TENANT_KEY_BASE}/.ledger/log-streams/*`;
/** A receipt's outcome markers (async-admission.ts receiptDir / outcomeKey; liveMachineId is srv_-prefixed). */
export const LEDGER_LOG_OUTCOMES = `${TENANT_KEY_BASE}/.admission-inbox/*/v1/ws=*/m=srv_*/r=*/outcome-*.json`;
export const LEDGER_LOG_ADMITTED_OUTCOMES = `${TENANT_KEY_BASE}/.admission-inbox/*/v1/ws=*/m=srv_*/r=*/outcome-admitted.json`;
/** The legacy batch docs the operator's drain check reads (ledger-s3.ts s3LedgerKeys.batch). */
export const LEDGER_LOG_LEGACY_BATCHES = `${LEDGER_LOG_USER_DIR}/ledger/batches/*`;
/** The log's control objects the operator tool reads (status, the switch, the close, the migration): never p/. */
export const LEDGER_LOG_OPERATOR_READS = ["e/*", "c/*", "head.json", "closed.json", "stream.json"].map((x) => `${LEDGER_LOG_USER_DIR}/ledger/log/*/${x}`);
/** The operator-written deployment markers (ledger-log-stream-ops.ts logCommitProofKey, logModeNonProdKey). */
export const LEDGER_LOG_MARKERS = [`${TENANT_KEY_BASE}/.ledger/log-commit-on.json`, `${TENANT_KEY_BASE}/.ledger/log-mode-nonprod.json`] as const;
/** What OnlyTheEraserDeletes must exempt for log mode (see the header): payloads, the admitted outcome, the registry. */
export const LEDGER_LOG_DELETE_CARVE_OUT = [LEDGER_LOG_PAYLOADS, LEDGER_LOG_ADMITTED_OUTCOMES, LEDGER_LOG_REGISTRY] as const;

export const LEDGER_LOG_CONSUMER_SIDS = ["LedgerLogReadsWritesStreams", "LedgerLogDeletesPayloadsAndAdmittedOutcomes", "LedgerLogRegistersStreams"] as const;
export const LEDGER_LOG_OPERATOR_SIDS = ["LedgerLogOperatorLists", "LedgerLogOperatorReads", "LedgerLogOperatorWrites", "LedgerLogOperatorMigratesRegistry"] as const;

/** The consumer's bucket-policy statements (admission and the materializer). Rendered only with ledgerLogCommit on. */
export function ledgerLogCommitConsumerStatements(i: { bucketArn: string; consumerRoleArn: string }) {
  const Principal = { AWS: i.consumerRoleArn };
  const o = (glob: string) => `${i.bucketArn}/${glob}`;
  return [
    { Sid: "LedgerLogReadsWritesStreams", Effect: "Allow" as const, Principal, Action: ["s3:GetObject", "s3:PutObject"], Resource: [o(LEDGER_LOG_OBJECTS), o(LEDGER_LOG_INDEX), o(LEDGER_LOG_OUTCOMES)] },
    { Sid: "LedgerLogDeletesPayloadsAndAdmittedOutcomes", Effect: "Allow" as const, Principal, Action: ["s3:DeleteObject"], Resource: [o(LEDGER_LOG_PAYLOADS), o(LEDGER_LOG_ADMITTED_OUTCOMES)] },
    { Sid: "LedgerLogRegistersStreams", Effect: "Allow" as const, Principal, Action: ["s3:GetObject", "s3:PutObject"], Resource: [o(LEDGER_LOG_REGISTRY)] },
  ];
}

/** The operator tool's statements, for a role outside the stack (documented; no stack input renders them yet). */
export function ledgerLogOperatorStatements(i: { bucketArn: string; operatorRoleArn: string }) {
  const Principal = { AWS: i.operatorRoleArn };
  const o = (glob: string) => `${i.bucketArn}/${glob}`;
  return [
    // No s3:prefix condition, on purpose: S3 returns 404 for a missing object only when the caller holds s3:ListBucket,
    // and a GET/HEAD request carries no s3:prefix, so a conditioned grant never applies to it and every missing read
    // would come back 403 (packages/object-store maps that to unauthorized): readZero on a legacy stream, the close
    // marker, the non-production and proof markers, the migration's dropped path. Listing reveals key names only.
    { Sid: "LedgerLogOperatorLists", Effect: "Allow" as const, Principal, Action: ["s3:ListBucket"], Resource: i.bucketArn },
    { Sid: "LedgerLogOperatorReads", Effect: "Allow" as const, Principal, Action: ["s3:GetObject"],
      Resource: [o(LEDGER_LOG_LEGACY_BATCHES), ...LEDGER_LOG_OPERATOR_READS.map(o), o(LEDGER_LOG_INDEX), o(LEDGER_LOG_REGISTRY), ...LEDGER_LOG_MARKERS.map(o)] },
    { Sid: "LedgerLogOperatorWrites", Effect: "Allow" as const, Principal, Action: ["s3:PutObject"],
      Resource: [o(LEDGER_LOG_ENTRIES), o(LEDGER_LOG_CLOSED), o(LEDGER_LOG_IDENTITY), o(LEDGER_LOG_INDEX), o(LEDGER_LOG_REGISTRY)] },
    { Sid: "LedgerLogOperatorMigratesRegistry", Effect: "Allow" as const, Principal, Action: ["s3:DeleteObject"], Resource: [o(LEDGER_LOG_REGISTRY)] },
  ];
}

/** The operator's key statement: data keys and decrypt via S3, the objects it reads or writes only (never payloads). */
export function ledgerLogOperatorKeyStatement(i: { region: string; accountId: string; bucketName: string; operatorRoleArn: string }) {
  const arn = (glob: string) => `arn:aws:s3:::${i.bucketName}/${glob}`;
  return {
    Sid: "LedgerLogOperatorViaS3", Effect: "Allow" as const, Principal: { AWS: i.operatorRoleArn }, Action: ["kms:GenerateDataKey", "kms:Decrypt"], Resource: "*",
    Condition: {
      StringEquals: { "kms:ViaService": `s3.${i.region}.amazonaws.com`, "kms:CallerAccount": i.accountId },
      StringLike: { "kms:EncryptionContext:aws:s3:arn": [arn(LEDGER_LOG_LEGACY_BATCHES), ...LEDGER_LOG_OPERATOR_READS.map(arn), arn(LEDGER_LOG_INDEX), arn(LEDGER_LOG_REGISTRY), ...LEDGER_LOG_MARKERS.map(arn)] },
    },
  };
}
