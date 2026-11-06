/**
 * PROPOSED (staged draft rev 2, not committed): src/migration/frozen-copy-resume.ts
 *
 * Commit-aware, source-collation-bound PER-PAGE RESUME for `scripts/frozen-source-copy.ts --copy --resume-dir D`.
 * No schema change: no new table, no G2 boundary regeneration, no migration apply.
 *
 * Invariant P (prefix): for one org and one stream (records | receipts), the copy only ever inserts consecutive
 * keyset pages in NEON order, starting either at the empty key or at a validated checkpoint. Each page is ONE
 * autocommit INSERT ... ON CONFLICT DO NOTHING statement, so it is atomic. Therefore the committed target rows of that
 * org+stream are always a prefix of Neon's order for that org. (Every existing copy revision starts at "" and is
 * sequential per org, so P already holds for rows copied before this change.)
 *
 * Checkpoint ordering (commit-aware): a checkpoint naming page tail T is written (tmp, fsync, rename, fsync dir)
 * strictly AFTER the INSERT that contained T has returned success, i.e. after the target COMMIT ack. Hence:
 *   crash before COMMIT          -> no checkpoint for that page: resume from the previous one; the gap is rescanned
 *   COMMIT outcome unknown       -> no checkpoint (the query rejected): rescanned; ON CONFLICT makes either outcome safe
 *   crash after COMMIT, before the checkpoint write/rename/fsync -> checkpoint lags: rescanned as conflicts
 * A checkpoint is therefore never AHEAD of committed data on its own; the only way it could be is target data loss
 * (restore). That is covered by validation below. Nothing is ever skipped on an unvalidated checkpoint.
 *
 * Source-collation binding: the checkpoint stores the tail key as Neon returned it and the resume predicate is the
 * unchanged Neon keyset `(workspace_id,origin_id,record_id) > ($2,$3,$4)` evaluated ON NEON. Neon is C.UTF-8 and the
 * target ICU en-US, so the target's order is never consulted. The checkpoint is bound to a fingerprint of Neon's key
 * column collations (+ collation versions + database collate/ctype + server major), the cutover epoch, privacy
 * baseline, target project, the org's control-head set, the stream, and the page query version. Any difference
 * discards the checkpoint (rescan from "").
 *
 * Resume validation (all must hold, else rescan from ""): checkpoint parses exactly; binding equal; the tail row
 * exists on Neon AND on the target and their canonical texts are byte-identical (this includes both accepted_seq
 * columns, so source accepted_seq values are verified, never re-derived); the stored tail-row sha256 equals both.
 * Values are always read from Neon and inserted unchanged; resume never writes a value of its own.
 *
 * REV 3 (lead P1): `complete` is NEVER trusted. A checkpoint (complete or not) only yields its validated tail key; the
 * copy ALWAYS resumes the Neon keyset scan from that tail. The scan's first Neon query `> tail LIMIT page` returning
 * fewer than `page` rows (zero for a finished stream) IS the source EOF/successor proof, evaluated live on Neon under
 * Neon's collation on every run. A tampered or stale `complete=true` therefore costs one page query and can skip
 * nothing. The flag is kept in the file only as an informational record ("EOF was observed when written").
 * Trust that remains in the checkpoint: only the tail key, and only after the tail row is proven present and
 * byte-identical on BOTH sides; under invariant P a committed tail implies its whole Neon prefix is committed.
 *
 * The global completeness seal is unchanged and still verifies EVERY org end to end.
 */
import { createHash } from "node:crypto";

export const CHECKPOINT_VERSION = 1;
export const PAGE_QUERY_VERSION = "neon-page-v1"; // bump whenever NEON_PAGE / NEON_RECEIPTS text changes
export type Stream = "record" | "receipt";
export type Key = [string, string, string];

export interface Binding {
  project: string;           // target project ref
  epoch: string;             // delivery_cutover_status().epoch
  privacyBaseline: string;   // delivery_cutover_status().privacyBaselineSha256
  collation: string;         // sha256 of the Neon collation fingerprint rows
  headsSha256: string;       // sha256 of this org's Neon control heads (version, digest, pending), C order
  pageQuery: string;         // PAGE_QUERY_VERSION
}

export interface Checkpoint extends Binding {
  v: number; kind: "frozen-copy-cursor"; stream: Stream; org: string;
  pages: number; rowsThrough: number; tail: Key; tailRowSha256: string; complete: boolean;
}

export const sha256 = (s: string) => createHash("sha256").update(s).digest("hex");
export const checkpointName = (stream: Stream, org: string) => sha256(stream + "\0" + org) + ".json";
const HEX64 = /^[0-9a-f]{64}$/u;
const isKey = (k: unknown): k is Key => Array.isArray(k) && k.length === 3 && k.every((x) => typeof x === "string" && x.length <= 4096);

export function parseCheckpoint(text: string, stream: Stream, org: string): Checkpoint | null {
  if (text.length > 65536) return null;
  let d: any;
  try { d = JSON.parse(text); } catch { return null; }
  const keys = ["v", "kind", "stream", "org", "pages", "rowsThrough", "tail", "tailRowSha256", "complete",
    "project", "epoch", "privacyBaseline", "collation", "headsSha256", "pageQuery"];
  if (!d || typeof d !== "object" || Array.isArray(d) || Object.keys(d).sort().join() !== [...keys].sort().join()) return null;
  if (d.v !== CHECKPOINT_VERSION || d.kind !== "frozen-copy-cursor" || d.stream !== stream || d.org !== org) return null;
  if (!Number.isSafeInteger(d.pages) || d.pages < 1 || !Number.isSafeInteger(d.rowsThrough) || d.rowsThrough < 1) return null;
  if (!isKey(d.tail) || !HEX64.test(d.tailRowSha256) || typeof d.complete !== "boolean") return null;
  for (const k of ["project", "epoch", "privacyBaseline", "collation", "headsSha256", "pageQuery"]) if (typeof d[k] !== "string" || !d[k]) return null;
  return d as Checkpoint;
}

export function sameBinding(a: Binding, b: Binding): boolean {
  return a.project === b.project && a.epoch === b.epoch && a.privacyBaseline === b.privacyBaseline
    && a.collation === b.collation && a.headsSha256 === b.headsSha256 && a.pageQuery === b.pageQuery;
}

export type Decision =
  | { from: "start"; reason: string }
  | { from: "checkpoint"; tail: Key; pages: number; rowsThrough: number };  // REV 3: no `complete`: never a skip

/** Pure resume decision. `neonTail`/`targetTail` are the canonical row texts of the checkpoint tail key fetched by
 * exact key from each side (null = absent). Anything short of full agreement rescans from the empty key. */
export function decide(cp: Checkpoint | null, binding: Binding, neonTail: string | null, targetTail: string | null): Decision {
  if (!cp) return { from: "start", reason: "no_checkpoint" };
  if (!sameBinding(cp, binding)) return { from: "start", reason: "binding_changed" };
  if (neonTail === null) return { from: "start", reason: "tail_absent_on_source" };
  if (targetTail === null) return { from: "start", reason: "tail_absent_on_target" };
  if (neonTail !== targetTail) return { from: "start", reason: "tail_row_differs" };
  if (sha256(neonTail) !== cp.tailRowSha256) return { from: "start", reason: "tail_digest_differs" };
  return { from: "checkpoint", tail: cp.tail, pages: cp.pages, rowsThrough: cp.rowsThrough };
}

export function makeCheckpoint(binding: Binding, stream: Stream, org: string, pages: number, rowsThrough: number,
  tail: Key, tailRowText: string, complete: boolean): Checkpoint {
  return { v: CHECKPOINT_VERSION, kind: "frozen-copy-cursor", stream, org, pages, rowsThrough, tail,
    tailRowSha256: sha256(tailRowText), complete, ...binding };
}

/** Minimal fs surface (node:fs sync API) so tests can inject crashes between every step. */
export interface Fs {
  lstatSync(p: string): { isDirectory(): boolean; isSymbolicLink(): boolean; uid: number; mode: number };
  openSync(p: string, flags: string | number, mode?: number): number;
  writeSync(fd: number, data: string): number;
  fsyncSync(fd: number): void;
  closeSync(fd: number): void;
  renameSync(a: string, b: string): void;
  readFileSync(p: string, enc: "utf8"): string;
  existsSync(p: string): boolean;
}

/** The resume dir is fixed by the controller: absolute, a real directory (not a symlink), owned by us, mode 0700. */
export function checkResumeDir(dir: string, fs: Fs, uid: number): void {
  if (!/^\/[A-Za-z0-9._/-]{1,200}$/u.test(dir) || dir.includes("..")) throw new Error("resume_dir_refused");
  const st = fs.lstatSync(dir);
  if (!st.isDirectory() || st.isSymbolicLink() || st.uid !== uid || (st.mode & 0o777) !== 0o700) throw new Error("resume_dir_refused");
}

/** Durable replace: tmp (O_CREAT|O_TRUNC, 0600) -> write -> fsync -> close -> rename -> fsync(dir). */
export function writeCheckpoint(dir: string, cp: Checkpoint, fs: Fs): void {
  const name = checkpointName(cp.stream, cp.org), tmp = `${dir}/${name}.next`, dst = `${dir}/${name}`;
  const fd = fs.openSync(tmp, "w", 0o600);
  try { fs.writeSync(fd, JSON.stringify(cp) + "\n"); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
  fs.renameSync(tmp, dst);
  const dfd = fs.openSync(dir, "r");
  try { fs.fsyncSync(dfd); } finally { fs.closeSync(dfd); }
}

export function readCheckpoint(dir: string, stream: Stream, org: string, fs: Fs): Checkpoint | null {
  const p = `${dir}/${checkpointName(stream, org)}`;
  if (!fs.existsSync(p)) return null;
  try { return parseCheckpoint(fs.readFileSync(p, "utf8"), stream, org); } catch { return null; }
}

/** Neon collation fingerprint rows (query result) -> stable sha256. */
export function collationSha256(rows: Record<string, unknown>[]): string {
  return sha256(JSON.stringify(rows.map((r) => Object.keys(r).sort().map((k) => [k, r[k] === null || r[k] === undefined ? null : String(r[k])]))));
}

export const NEON_COLLATION_FINGERPRINT = `SELECT 'db' AS scope, d.datcollate AS a, d.datctype AS b, current_setting('server_version_num') AS c, '' AS d
   FROM pg_database d WHERE d.datname = current_database()
 UNION ALL
 SELECT cl.relname || '.' || a.attname, coalesce(c.collname, ''), coalesce(c.collprovider::text, '') || ':' || coalesce(c.collcollate, ''),
        coalesce(pg_collation_actual_version(a.attcollation), ''), a.attcollation::text
   FROM pg_attribute a JOIN pg_class cl ON cl.oid = a.attrelid LEFT JOIN pg_collation c ON c.oid = a.attcollation
  WHERE a.attrelid IN ('sessions.delivery_records'::regclass, 'sessions.delivery_receipts'::regclass)
    AND a.attname IN ('org_id', 'workspace_id', 'origin_id', 'record_id', 'batch_id') AND NOT a.attisdropped
 ORDER BY 1`;

export const NEON_ORG_HEADS = `SELECT workspace_id, user_id, version::text AS version, digest, pending::text AS pending
 FROM sessions.delivery_cutover_control_heads WHERE org_id = $1 ORDER BY workspace_id COLLATE "C", user_id COLLATE "C"`;

export function headsSha256(rows: Record<string, unknown>[]): string {
  return sha256(JSON.stringify(rows.map((r) => [r.workspace_id, r.user_id, r.version, r.digest, r.pending].map((v) => v === null || v === undefined ? null : String(v)))));
}
