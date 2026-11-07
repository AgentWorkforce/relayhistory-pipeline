/**
 * The exporter's local watermark: one SQLite file (ids, revisions, counts and
 * object keys; never content). It is NOT disposable: it also holds the durable
 * unresolved-write obligations (every exporter PUT recorded before it is sent),
 * ownership claims and erasure reservations (write-obligation.ts v3). A lost or
 * recreated file forgets outstanding PUTs, so a new file is UNATTESTED for good:
 * the erasure job never marks an exporter prefix or object erased on it (fails
 * closed). Nothing attests it: no timer, PID or lock-file check ever settles an
 * unknown PUT; a reviewed recovery contract must come first.
 *
 * - `groups`: every `(org, session)` the last sweeps saw, with its Neon
 *   revision and activity, and the sweep id that last saw it.
 * - `exported`: what this exporter last wrote for a group: the revision it
 *   read before reading the content, and the manifest keys it owns.
 */
import { DatabaseSync } from "node:sqlite";

export interface ExportedRow {
  org_id: string;
  session_id: string;
  revision: number;
  exported_at: number;
  keys: string[];
  error: string | null;
  attempts: number;
}

export interface WorkItem {
  org_id: string;
  session_id: string;
  revision: number;
  kind: "changed" | "recent" | "history" | "retry" | "reverify";
  events: number;
}

export interface Backlog {
  groups: number;
  exported: number;
  pending: number;
  pendingEvents: number;
  recentPending: number;
  recentPendingEvents: number;
  changed: number;
  errored: number;
  pendingDeletes: number;
}

export class SyncState {
  readonly db: DatabaseSync;

  constructor(path: string) {
    this.db = new DatabaseSync(path);
    // Shared by the exporter and the erasure job (two processes): WAL plus a busy wait.
    this.db.exec("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA busy_timeout = 10000;");
    // A file that already holds the legacy exporter's schema (opened before obligations existed) vs. a new / recreated one.
    const legacyFile = !!this.db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'meta'").get();
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS groups (
        org_id TEXT NOT NULL, session_id TEXT NOT NULL,
        revision INTEGER NOT NULL DEFAULT 0, last_ms INTEGER, events INTEGER NOT NULL DEFAULT 0,
        rev_sweep INTEGER NOT NULL DEFAULT 0, act_sweep INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (org_id, session_id));
      CREATE TABLE IF NOT EXISTS exported (
        org_id TEXT NOT NULL, session_id TEXT NOT NULL,
        revision INTEGER NOT NULL, exported_at INTEGER NOT NULL, keys TEXT NOT NULL,
        error TEXT, attempts INTEGER NOT NULL DEFAULT 0, next_try INTEGER NOT NULL DEFAULT 0,
        failed_revision INTEGER,
        PRIMARY KEY (org_id, session_id));
      CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS erasures (
        key TEXT PRIMARY KEY, kind TEXT NOT NULL CHECK (kind IN ('object', 'prefix')),
        org_id TEXT NOT NULL, reason TEXT NOT NULL, enqueued_at INTEGER NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0, next_try INTEGER NOT NULL DEFAULT 0, error TEXT);
      CREATE TABLE IF NOT EXISTS pending_keys (org_id TEXT NOT NULL, session_id TEXT NOT NULL, key TEXT NOT NULL,
        PRIMARY KEY (org_id, session_id, key));
      CREATE TABLE IF NOT EXISTS leases (prefix TEXT PRIMARY KEY, holder TEXT NOT NULL, at INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS erasure_seen (org_id TEXT NOT NULL, session_id TEXT NOT NULL, sweep INTEGER NOT NULL,
        PRIMARY KEY (org_id, session_id));
      -- Unresolved-write obligations (shared write-obligation v3, write-obligation.ts), E3's own tables:
      -- one row per exporter PUT invocation, recorded BEFORE it is sent; the versions the exporter
      -- currently publishes (owns); and the versions the eraser reserved for deletion. Every mutation
      -- takes a fresh seq; a prefix's revision is the max seq under it (the compare-and-set token).
      CREATE TABLE IF NOT EXISTS write_invocations (id TEXT PRIMARY KEY, key TEXT NOT NULL, holder TEXT NOT NULL,
        outcome TEXT NOT NULL CHECK (outcome IN ('dispatched', 'acked')), version_id TEXT, seq INTEGER NOT NULL);
      CREATE INDEX IF NOT EXISTS write_invocations_key ON write_invocations (key);
      CREATE TABLE IF NOT EXISTS write_published (key TEXT NOT NULL, version_id TEXT NOT NULL, seq INTEGER NOT NULL,
        PRIMARY KEY (key, version_id));
      CREATE TABLE IF NOT EXISTS erase_reservations (key TEXT NOT NULL, version_id TEXT NOT NULL, scope TEXT NOT NULL,
        seq INTEGER NOT NULL, PRIMARY KEY (key, version_id));
      -- PROTECTED terminal history, never deleted or reopened: every invocation's terminal outcome, and every
      -- version the eraser proved erased (moved here when its reservation is cleared).
      CREATE TABLE IF NOT EXISTS write_terminal (id TEXT PRIMARY KEY, key TEXT NOT NULL, version_id TEXT,
        outcome TEXT NOT NULL CHECK (outcome IN ('erased', 'published', 'refused')), at INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS erased_versions (key TEXT NOT NULL, version_id TEXT NOT NULL, at INTEGER NOT NULL,
        PRIMARY KEY (key, version_id));
      CREATE TABLE IF NOT EXISTS erase_checkpoints (
        scope TEXT PRIMARY KEY, checkpoint TEXT NOT NULL, updated_at INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS truncations (
        org_id TEXT NOT NULL, session_id TEXT NOT NULL, manifest_key TEXT NOT NULL,
        strings INTEGER NOT NULL, recorded_at INTEGER NOT NULL,
        PRIMARY KEY (org_id, session_id, manifest_key));
    `);
    // Added after the first production start: migrate in place.
    const cols = this.db.prepare("PRAGMA table_info(truncations)").all() as { name: string }[];
    if (!cols.some((c) => c.name === "max_chars"))
      try {
        this.db.exec("ALTER TABLE truncations ADD COLUMN max_chars INTEGER NOT NULL DEFAULT 0");
      } catch (error) {
        // The other process (exporter or erasure job) added it first.
        if (!/duplicate column/i.test(String((error as Error).message))) throw error;
      }
    this.db.exec(`
      CREATE INDEX IF NOT EXISTS groups_last ON groups (last_ms DESC);
    `);
    // Lease generation: minted on every NEW acquisition (absent, or taken from another holder); a renew keeps it.
    if (!(this.db.prepare("SELECT 1 FROM pragma_table_info('leases') WHERE name = 'gen'").get())) this.db.exec("ALTER TABLE leases ADD COLUMN gen TEXT");
    // Obligation genesis (once per file, never reset), from EVIDENCE only, never a timer, a PID or a lock file:
    // A file has no evidence that writes dispatched before it are terminal (a dead process, an absent lock or any
    // wait proves nothing; the legacy exporter even cleared pending_keys on a failed group whose PUT timed out), so
    // EVERY genesis is UNATTESTED and the erasure job never marks an exporter prefix or object erased on it. There is
    // no unlock: a separately reviewed recovery contract (old effects proven terminal, or a genuinely never-used
    // scope) is required first. A legacy file's surviving pending_keys are still imported as PREFIX-level unresolved
    // obligations (dispatched, nonterminal): more fail-closed information, never evidence.
    if (!this.meta("obligations_genesis")) {
      this.#tx(() => {
        if (this.meta("obligations_genesis")) return;
        let imported = 0;
        if (legacyFile) {
          const rows = this.db.prepare("SELECT DISTINCT key FROM pending_keys").all() as { key: string }[];
          const ins = this.db.prepare("INSERT INTO write_invocations (id, key, holder, outcome, version_id, seq) VALUES (?, ?, 'legacy-exporter', 'dispatched', NULL, ?)");
          for (const r of rows) {
            const prefix = r.key.endsWith("manifest.json") ? r.key.slice(0, -"manifest.json".length) : r.key;
            ins.run(crypto.randomUUID(), prefix.endsWith("/") ? prefix : `${prefix}/`, this.#seq());
            imported += 1;
          }
        }
        this.setMeta("obligations_genesis", JSON.stringify({ at: Date.now(), attestedAt: null, basis: legacyFile ? "legacy-unattested" : "none", imported }));
      });
    }
  }

  /** The obligation genesis: when this file began tracking exporter PUTs, and whether an operator attested it. */
  obligationGenesis(): { at: number; attestedAt: null; basis?: string; imported?: number } {
    // Any stored attestedAt (old, forged or otherwise) is IGNORED: there is no attestation policy, so it is always null.
    const g = JSON.parse(this.meta("obligations_genesis") ?? '{"at":0}') as { at: number; basis?: string; imported?: number };
    return { at: g.at, attestedAt: null, ...(g.basis !== undefined ? { basis: g.basis } : {}), ...(g.imported !== undefined ? { imported: g.imported } : {}) };
  }
  /** Erasures may be marked done only after the genesis is attested (fail closed on a lost / recreated file). */
  obligationsAttested(): boolean {
    return false; // always: no metadata, old or forged, ever unlocks completion
  }

  #tx(fn: () => void) {
    // IMMEDIATE: two processes share this file; a deferred read-then-write could
    // fail with SQLITE_BUSY_SNAPSHOT, which busy_timeout does not retry.
    this.db.exec("BEGIN IMMEDIATE");
    try {
      fn();
      this.db.exec("COMMIT");
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  meta(k: string): string | undefined {
    return (this.db.prepare("SELECT v FROM meta WHERE k = ?").get(k) as { v: string } | undefined)?.v;
  }

  setMeta(k: string, v: string) {
    this.db.prepare("INSERT INTO meta (k, v) VALUES (?, ?) ON CONFLICT (k) DO UPDATE SET v = excluded.v").run(k, v);
  }

  /** Record a sweep page of `session_revisions`. */
  seeRevisions(sweep: number, rows: { org_id: string; session_id: string; revision: string }[]) {
    const q = this.db.prepare(
      `INSERT INTO groups (org_id, session_id, revision, rev_sweep) VALUES (?, ?, ?, ?)
       ON CONFLICT (org_id, session_id) DO UPDATE SET revision = excluded.revision, rev_sweep = excluded.rev_sweep`,
    );
    this.#tx(() => {
      for (const r of rows) q.run(r.org_id, r.session_id, Number(r.revision), sweep);
    });
  }

  /** Record a sweep page of rollup activity. */
  seeActivity(sweep: number, rows: { org_id: string; session_id: string; last_ms: string; events: string }[], catalog = false) {
    // A catalog row only marks the group seen (and fills last_ms when the rollups gave none);
    // it never overwrites the rollups' activity and event count.
    const q = this.db.prepare(
      catalog
        ? `INSERT INTO groups (org_id, session_id, last_ms, events, act_sweep) VALUES (?, ?, ?, ?, ?)
           ON CONFLICT (org_id, session_id) DO UPDATE SET
             last_ms = CASE WHEN groups.act_sweep = excluded.act_sweep THEN max(coalesce(groups.last_ms, 0), excluded.last_ms) ELSE excluded.last_ms END,
             events = CASE WHEN groups.act_sweep = excluded.act_sweep THEN groups.events ELSE excluded.events END,
             act_sweep = excluded.act_sweep`
        : `INSERT INTO groups (org_id, session_id, last_ms, events, act_sweep) VALUES (?, ?, ?, ?, ?)
           ON CONFLICT (org_id, session_id) DO UPDATE SET last_ms = excluded.last_ms, events = excluded.events,
             act_sweep = excluded.act_sweep`,
    );
    this.#tx(() => {
      for (const r of rows) q.run(r.org_id, r.session_id, Number(r.last_ms), Number(r.events), sweep);
    });
  }

  /**
   * After complete sweeps of both sources: a group neither saw is gone from
   * Neon. Its row is dropped; its exported keys stay until the delete runs.
   */
  dropUnseen(revSweep: number, actSweep: number): number {
    return this.db
      .prepare("DELETE FROM groups WHERE rev_sweep < ? AND act_sweep < ?")
      .run(revSweep, actSweep).changes as number;
  }

  exported(org: string, session: string): ExportedRow | undefined {
    const row = this.db.prepare("SELECT * FROM exported WHERE org_id = ? AND session_id = ?").get(org, session) as
      | (Omit<ExportedRow, "keys"> & { keys: string })
      | undefined;
    return row ? { ...row, keys: JSON.parse(row.keys) as string[] } : undefined;
  }

  /** A key the exporter is about to write under, recorded before the write survives a crash. */
  recordPendingKey(org: string, session: string, key: string) {
    this.db.prepare("INSERT OR IGNORE INTO pending_keys (org_id, session_id, key) VALUES (?, ?, ?)").run(org, session, key);
  }

  /** On start: fold keys of runs that died mid-group into their group's watermark (as failed, to retry). */
  recoverPendingKeys(at = Date.now()): number {
    const rows = this.db.prepare("SELECT org_id, session_id, key FROM pending_keys").all() as { org_id: string; session_id: string; key: string }[];
    const groups = new Map<string, { org: string; session: string; keys: string[] }>();
    for (const r of rows) {
      const k = JSON.stringify([r.org_id, r.session_id]);
      const g = groups.get(k) ?? { org: r.org_id, session: r.session_id, keys: [] };
      g.keys.push(r.key);
      groups.set(k, g);
    }
    for (const g of groups.values()) this.markFailed(g.org, g.session, "interrupted", g.keys, -1, at);
    return groups.size;
  }

  markExported(org: string, session: string, revision: number, keys: string[], at = Date.now()) {
    this.db.prepare("DELETE FROM pending_keys WHERE org_id = ? AND session_id = ?").run(org, session);
    if (keys.length === 0 && !this.db.prepare("SELECT 1 FROM groups WHERE org_id = ? AND session_id = ?").get(org, session)) {
      this.db.prepare("DELETE FROM exported WHERE org_id = ? AND session_id = ?").run(org, session);
      return;
    }
    this.db
      .prepare(
        `INSERT INTO exported (org_id, session_id, revision, exported_at, keys, error, attempts, next_try)
         VALUES (?, ?, ?, ?, ?, NULL, 0, 0)
         ON CONFLICT (org_id, session_id) DO UPDATE SET revision = excluded.revision,
           exported_at = excluded.exported_at, keys = excluded.keys, error = NULL, attempts = 0, next_try = 0,
           failed_revision = NULL`,
      )
      .run(org, session, revision, at, JSON.stringify([...keys].sort()));
  }

  /**
   * A failed export keeps its old keys plus every key it may have written
   * before failing (so a later erasure still finds them), and is retried with
   * a backoff of at most 4 h, or at once when Neon's revision moves again.
   */
  markFailed(org: string, session: string, code: string, keys: string[], revision: number, at = Date.now()) {
    const prior = this.exported(org, session);
    // Waiting for the other process's lease, or an interrupted run, is not the group's failure.
    const transientWait = code === "prefix_busy" || code === "interrupted";
    const attempts = (prior?.attempts ?? 0) + (transientWait ? 0 : 1);
    const nextTry = at + (transientWait ? 2 * 60 * 1000 : Math.min(4, 2 ** Math.max(0, attempts - 1)) * 60 * 60 * 1000);
    const all = JSON.stringify([...new Set([...(prior?.keys ?? []), ...keys])].sort());
    this.db
      .prepare(
        `INSERT INTO exported (org_id, session_id, revision, exported_at, keys, error, attempts, next_try, failed_revision)
         VALUES (?, ?, -1, 0, ?, ?, ?, ?, ?)
         ON CONFLICT (org_id, session_id) DO UPDATE SET keys = excluded.keys, error = excluded.error,
           attempts = excluded.attempts, next_try = excluded.next_try, failed_revision = excluded.failed_revision`,
      )
      .run(org, session, all, code, attempts, nextTry, revision);
    this.db.prepare("DELETE FROM pending_keys WHERE org_id = ? AND session_id = ?").run(org, session);
  }

  /**
   * The truncation ledger: every exported manifest that holds a string cut to
   * the scrub bound, so it can be re-exported in full once the sealer takes
   * large strings. Replaced on each export of the group; T0 waits on it being empty.
   */
  recordTruncations(org: string, session: string, byKey: { key: string; strings: number; maxChars?: number }[], at = Date.now()) {
    this.#tx(() => {
      this.db.prepare("DELETE FROM truncations WHERE org_id = ? AND session_id = ?").run(org, session);
      const q = this.db.prepare(
        "INSERT INTO truncations (org_id, session_id, manifest_key, strings, recorded_at, max_chars) VALUES (?, ?, ?, ?, ?, ?)",
      );
      for (const t of byKey) if (t.strings > 0) q.run(org, session, t.key, t.strings, at, t.maxChars ?? 0);
    });
  }

  // ---------------------------------------------------------------- per-session leases

  /**
   * The exporter and the erasure job never work on one session prefix at the
   * same time: otherwise the eraser could delete a chunk the exporter has just
   * decided to reuse. A lease older than `ttlMs` (a crashed holder) is taken over.
   */
  acquireLease(prefix: string, holder: string, ttlMs = 2 * 60 * 60 * 1000, now = Date.now()): boolean {
    let ok = false;
    this.#tx(() => {
      const row = this.db.prepare("SELECT holder, at FROM leases WHERE prefix = ?").get(prefix) as
        | { holder: string; at: number }
        | undefined;
      if (row && row.holder !== holder && now - Number(row.at) < ttlMs) return;
      // The same holder keeps its generation; a new acquisition (absent, or taken over) mints a fresh one.
      this.db
        .prepare(`INSERT INTO leases (prefix, holder, at, gen) VALUES (?, ?, ?, ?) ON CONFLICT (prefix) DO UPDATE SET at = excluded.at,
          gen = CASE WHEN leases.holder = excluded.holder AND leases.gen IS NOT NULL THEN leases.gen ELSE excluded.gen END, holder = excluded.holder`)
        .run(prefix, holder, now, crypto.randomUUID());
      ok = true;
    });
    return ok;
  }

  /** The lease's current generation (minted at acquisition), or undefined when no one holds it. */
  leaseGen(prefix: string): string | undefined {
    return (this.db.prepare("SELECT gen FROM leases WHERE prefix = ?").get(prefix) as { gen: string | null } | undefined)?.gen ?? undefined;
  }

  /** Extend a lease this holder still has; false if it was taken over. */
  renewLease(prefix: string, holder: string, now = Date.now()): boolean {
    return Number(this.db.prepare("UPDATE leases SET at = ? WHERE prefix = ? AND holder = ?").run(now, prefix, holder).changes) === 1;
  }

  /** Leases of a holder kind other than `current` (a previous run of the same single process). */
  releaseStaleLeases(kind: string, current: string): number {
    return Number(this.db.prepare("DELETE FROM leases WHERE holder LIKE ? AND holder <> ?").run(`${kind}%`, current).changes);
  }

  releaseLeases(holder: string, prefixes: Iterable<string>) {
    const q = this.db.prepare("DELETE FROM leases WHERE prefix = ? AND holder = ?");
    this.#tx(() => {
      for (const p of prefixes) q.run(p, holder);
    });
  }

  /** Erasures the exporter has just made unnecessary: objects it references again, prefixes it writes again. */
  cancelErasures(keys: Iterable<string>) {
    const q = this.db.prepare("DELETE FROM erasures WHERE key = ?");
    this.#tx(() => {
      for (const k of keys) q.run(k);
    });
  }

  // ---------------------------------------------------------------- erasure queue

  /** Queue objects (or whole session prefixes) for the erasure job. Idempotent per key. */
  enqueueErasure(items: { key: string; kind: "object" | "prefix"; org_id: string; reason: string }[], at = Date.now()) {
    const q = this.db.prepare(
      `INSERT INTO erasures (key, kind, org_id, reason, enqueued_at) VALUES (?, ?, ?, ?, ?)
       ON CONFLICT (key) DO UPDATE SET next_try = 0,
         kind = CASE WHEN excluded.kind = 'prefix' THEN 'prefix' ELSE erasures.kind END`,
    );
    // Queued for erasure: no longer owned (the eraser may then reserve its versions), scoped by the item's KIND: an
    // object item unpublishes EXACTLY its key (never another key that merely starts with it); a prefix item, the
    // keys under its "/"-terminated prefix.
    const unpubKey = this.db.prepare("DELETE FROM write_published WHERE key = ?");
    const unpubPrefix = this.db.prepare(`DELETE FROM write_published WHERE ${SyncState.#under}`);
    this.#tx(() => {
      for (const i of items) {
        q.run(i.key, i.kind, i.org_id, i.reason, at);
        if (i.kind === "object") unpubKey.run(i.key);
        else unpubPrefix.run(...SyncState.#u(i.key));
      }
    });
  }

  nextErasures(now: number, limit: number): { key: string; kind: "object" | "prefix"; org_id: string; reason: string; enqueued_at: number }[] {
    // Prefixes first (whole sessions), then the oldest objects.
    return this.db
      .prepare(
        `SELECT key, kind, org_id, reason, enqueued_at FROM erasures WHERE next_try <= ?
          ORDER BY kind = 'prefix' DESC, enqueued_at LIMIT ?`,
      )
      .all(now, limit) as unknown as { key: string; kind: "object" | "prefix"; org_id: string; reason: string; enqueued_at: number }[];
  }

  erasureQueued(key: string, enqueuedAt: number): boolean {
    return !!this.db.prepare("SELECT 1 FROM erasures WHERE key = ? AND enqueued_at = ?").get(key, enqueuedAt);
  }

  erasureDone(key: string, enqueuedAt: number) {
    // Only if not re-queued meanwhile.
    this.db.prepare("DELETE FROM erasures WHERE key = ? AND enqueued_at = ?").run(key, enqueuedAt);
  }

  /**
   * The shared paged eraser's durable resume point (eraser.ts EraseCheckpoint), keyed by the job's scope digest
   * (operation, authority, tenant/session or queued item identity). Progress only: a checkpoint is never proof of
   * erasure; completion needs a fresh strict zero inventory (verifyErased).
   */
  eraseCheckpoint(scope: string): unknown | null {
    const row = this.db.prepare("SELECT checkpoint FROM erase_checkpoints WHERE scope = ?").get(scope) as { checkpoint: string } | undefined;
    return row ? JSON.parse(row.checkpoint) : null;
  }
  setEraseCheckpoint(scope: string, checkpoint: unknown, at = Date.now()) {
    this.db.prepare("INSERT INTO erase_checkpoints(scope, checkpoint, updated_at) VALUES(?, ?, ?) ON CONFLICT(scope) DO UPDATE SET checkpoint = excluded.checkpoint, updated_at = excluded.updated_at")
      .run(scope, JSON.stringify(checkpoint), at);
  }
  clearEraseCheckpoint(scope: string) {
    this.db.prepare("DELETE FROM erase_checkpoints WHERE scope = ?").run(scope);
  }

  // ---------------------------------------------------------------- unresolved-write obligations (v3)

  #seq(): number {
    const n = Number(this.meta("write_seq") ?? 0) + 1;
    this.setMeta("write_seq", String(n));
    return n;
  }
  /**
   * Prefix membership that can use the key index: a BINARY range (byte order of UTF-8 = code point order; SQLite's
   * default collation, stated explicitly) [prefix, successor(prefix)), where successor increments the prefix's last
   * code point, so every string starting with the prefix sorts inside it; plus an exact residual prefix test.
   * Parameters: SyncState.#u(prefix).
   */
  static #under = "key COLLATE BINARY >= ? AND key COLLATE BINARY < ? AND substr(key, 1, length(?)) = ?";
  static #u(prefix: string): [string, string | Uint8Array, string, string] {
    // Range membership is for "/"-terminated directory / session prefixes only; an item KEY is matched exactly.
    if (!prefix.endsWith("/")) throw Object.assign(new Error("a prefix range needs a /-terminated prefix"), { code: "prefix_shape" });
    // The least string greater than every string starting with `p`: increment its last code point (dropping trailing
    // U+10FFFF first). None (empty, or all U+10FFFF): a BLOB, which SQLite orders after every TEXT value.
    const succ = (p: string): string | Uint8Array => {
      const cps = [...p];
      while (cps.length && cps[cps.length - 1]!.codePointAt(0) === 0x10ffff) cps.pop();
      const last = cps.pop()?.codePointAt(0);
      if (last === undefined) return new Uint8Array([0xff]);
      const next = last + 1 >= 0xd800 && last + 1 <= 0xdfff ? 0xe000 : last + 1; // never a lone surrogate
      return cps.join("") + String.fromCodePoint(next);
    };
    return [prefix, succ(prefix), prefix, prefix];
  }

  /** Writer: a NEW invocation id for one PUT at `key`, durable BEFORE the PUT is sent. */
  dispatchWrite(key: string, holder: string): string {
    const id = crypto.randomUUID();
    this.#tx(() => {
      this.db.prepare("INSERT INTO write_invocations (id, key, holder, outcome, version_id, seq) VALUES (?, ?, ?, 'dispatched', NULL, ?)").run(id, key, holder, this.#seq());
    });
    return id;
  }
  /** Writer: its OWN confirmed response, with the exact version id it created ("null" on an unversioned store). */
  ackWrite(id: string, versionId: string) {
    this.#tx(() => {
      this.db.prepare("UPDATE write_invocations SET outcome = 'acked', version_id = ?, seq = ? WHERE id = ? AND outcome = 'dispatched'").run(versionId, this.#seq(), id);
    });
  }
  /** Writer: a definite refusal of this very request (no write happened): terminal, so the row goes. */
  refuseWrite(id: string) {
    this.#tx(() => {
      const r = this.db.prepare("INSERT OR IGNORE INTO write_terminal (id, key, version_id, outcome, at) SELECT id, key, NULL, 'refused', ? FROM write_invocations WHERE id = ? AND outcome = 'dispatched'").run(Date.now(), id);
      if (Number(r.changes) === 1) this.db.prepare("DELETE FROM write_invocations WHERE id = ?").run(id);
    });
  }
  /**
   * Writer publish: the versions this group now references become owned, in ONE transaction under the lock the
   * eraser's reserve takes, and only while the writer can prove it is still the writer:
   *  - lease: every entry lies under one of `lease.prefixes`, each still held by `lease.holder` (an eraser that took
   *    an expired lease, or finished and released it, makes a delayed writer's publish fail);
   *  - an exact version must CONSUME its own still-live acked invocation (or already be owned exactly: idempotent);
   *    once the eraser resolved that invocation (the version was seen gone), a late publish fails;
   *  - a key-level claim ("*": reused / unchanged objects whose version id this run never saw) conflicts with ANY
   *    reservation at the key; an exact one with that version's reservation.
   * Any refusal records nothing (the group then fails; nothing is marked exported).
   */
  publishWrites(entries: { key: string; versionId: string }[], lease: { holder: string; gens: Readonly<Record<string, string>> }) {
    this.#tx(() => {
      const held = this.db.prepare("SELECT holder, gen FROM leases WHERE prefix = ?");
      const erased = this.db.prepare("SELECT 1 FROM erased_versions WHERE key = ? AND version_id = ?");
      const reserved = this.db.prepare("SELECT 1 FROM erase_reservations WHERE key = ? AND (version_id = ? OR ? = '*')");
      const live = this.db.prepare("SELECT id FROM write_invocations WHERE key = ? AND outcome = 'acked' AND version_id = ? AND holder = ?");
      const owned = this.db.prepare("SELECT 1 FROM write_published WHERE key = ? AND version_id = ?");
      const refuse = (code: string, msg: string) => { throw Object.assign(new Error(msg), { code }); };
      const consume: string[] = [];
      for (const e of entries) {
        const p = Object.keys(lease.gens).find((x) => e.key.startsWith(x));
        const h = p ? (held.get(p) as { holder: string; gen: string | null } | undefined) : undefined;
        // Holder AND generation: a lease that expired and was taken (even if re-acquired later) never validates.
        if (!p || !h || h.holder !== lease.holder || h.gen === null || h.gen !== lease.gens[p]) refuse("publish_lease_lost", "the writer no longer holds this lease generation");
        if (reserved.get(e.key, e.versionId, e.versionId)) refuse("publish_reserved", "a published version is reserved for erasure");
        if (e.versionId === "*") continue;
        if (erased.get(e.key, e.versionId)) refuse("publish_erased", "the version was erased");
        const inv = live.get(e.key, e.versionId, lease.holder) as { id: string } | undefined;
        if (inv) consume.push(inv.id);
        else if (!owned.get(e.key, e.versionId)) refuse("publish_unresolved", "the version's own invocation is no longer live (it may have been erased)");
      }
      const seq = this.#seq();
      const ins = this.db.prepare("INSERT INTO write_published (key, version_id, seq) VALUES (?, ?, ?) ON CONFLICT DO NOTHING");
      for (const e of entries) ins.run(e.key, e.versionId, seq);
      const term = this.db.prepare("INSERT OR IGNORE INTO write_terminal (id, key, version_id, outcome, at) SELECT id, key, version_id, 'published', ? FROM write_invocations WHERE id = ?");
      const done = this.db.prepare("DELETE FROM write_invocations WHERE id = ?");
      for (const id of consume) {
        term.run(Date.now(), id);
        done.run(id);
      }
    });
  }
  /** Ownership ends for everything under a "/"-terminated prefix (a prefix-kind erasure or a forgotten session). */
  unpublishPrefix(prefix: string) {
    this.db.prepare(`DELETE FROM write_published WHERE ${SyncState.#under}`).run(...SyncState.#u(prefix));
  }
  /** The prefix's compare-and-set revision: the newest seq of any invocation, publication or reservation under it. */
  obligationRev(prefix: string): number {
    const q = (t: string) => Number((this.db.prepare(`SELECT coalesce(max(seq), 0) AS n FROM ${t} WHERE ${SyncState.#under}`).get(...SyncState.#u(prefix)) as { n: number }).n);
    return Math.max(q("write_invocations"), q("write_published"), q("erase_reservations"));
  }
  /** The unresolved invocations under a prefix (dispatched, or acked whose version was not yet seen gone). */
  unresolvedWrites(prefix: string): { id: string; key: string; outcome: "dispatched" | "acked"; versionId?: string }[] {
    return (this.db.prepare(`SELECT id, key, outcome, version_id FROM write_invocations WHERE ${SyncState.#under} ORDER BY id`).all(...SyncState.#u(prefix)) as
      { id: string; key: string; outcome: "dispatched" | "acked"; version_id: string | null }[]).map((r) => ({ id: r.id, key: r.key, outcome: r.outcome, ...(r.version_id ? { versionId: r.version_id } : {}) }));
  }
  /** EXPLAIN QUERY PLAN of the prefix predicate on `table` (tests: the key index must be used). */
  prefixPlan(table: "write_invocations" | "write_published" | "erase_reservations", prefix: string): string {
    return (this.db.prepare(`EXPLAIN QUERY PLAN SELECT max(seq) FROM ${table} WHERE ${SyncState.#under}`).all(...SyncState.#u(prefix)) as { detail: string }[]).map((r) => r.detail).join(" | ");
  }
  /** Prefix-level unresolved obligations (legacy imports: key ends in "/") that COVER `prefix` (an ancestor or itself). */
  coveringWrites(prefix: string): { id: string; key: string; outcome: "dispatched" | "acked"; versionId?: string }[] {
    return (this.db.prepare("SELECT id, key, outcome, version_id FROM write_invocations WHERE substr(key, -1) = '/' AND substr(?, 1, length(key)) = key").all(prefix) as
      { id: string; key: string; outcome: "dispatched" | "acked"; version_id: string | null }[]).map((r) => ({ id: r.id, key: r.key, outcome: r.outcome, ...(r.version_id ? { versionId: r.version_id } : {}) }));
  }
  /** Versions the writer currently owns under a prefix. */
  publishedUnder(prefix: string): { key: string; versionId: string }[] {
    return (this.db.prepare(`SELECT key, version_id FROM write_published WHERE ${SyncState.#under}`).all(...SyncState.#u(prefix)) as { key: string; version_id: string }[]).map((r) => ({ key: r.key, versionId: r.version_id }));
  }
  /**
   * Eraser reserve (compare-and-set on the prefix revision): atomically reserve EVERY version about to be
   * deleted. Refused if the prefix changed since `rev` (a writer dispatched/acked/published there) or if any
   * doomed version is owned now; after it succeeds, a publish of any reserved version is refused.
   */
  reserveErase(prefix: string, rev: number, scope: string, versions: { key: string; versionId: string }[]): { ok: true; rev: number } | { ok: false } {
    let out: { ok: true; rev: number } | { ok: false } = { ok: false };
    this.#tx(() => {
      if (this.obligationRev(prefix) !== rev) return;
      // Owned: that exact version, or the whole key (a key-level claim).
      const owned = this.db.prepare("SELECT 1 FROM write_published WHERE key = ? AND (version_id = ? OR version_id = '*')");
      for (const v of versions) {
        if (!v.key.startsWith(prefix)) return;
        if (owned.get(v.key, v.versionId)) return;
      }
      const seq = this.#seq();
      const ins = this.db.prepare("INSERT INTO erase_reservations (key, version_id, scope, seq) VALUES (?, ?, ?, ?) ON CONFLICT (key, version_id) DO UPDATE SET scope = excluded.scope, seq = excluded.seq");
      for (const v of versions) ins.run(v.key, v.versionId, scope, seq);
      out = { ok: true, rev: this.obligationRev(prefix) };
    });
    return out;
  }
  /** Clear exactly these invocations (each on its own terminal evidence), compare-and-set on the prefix revision. */
  resolveWrites(prefix: string, rev: number, ids: readonly string[]): { ok: true; rev: number } | { ok: false } {
    let out: { ok: true; rev: number } | { ok: false } = { ok: false };
    this.#tx(() => {
      if (this.obligationRev(prefix) !== rev) return;
      const term = this.db.prepare(`INSERT OR IGNORE INTO write_terminal (id, key, version_id, outcome, at) SELECT id, key, version_id, 'erased', ? FROM write_invocations WHERE id = ? AND ${SyncState.#under}`);
      const del = this.db.prepare(`DELETE FROM write_invocations WHERE id = ? AND ${SyncState.#under}`);
      for (const id of ids) {
        term.run(Date.now(), id, ...SyncState.#u(prefix));
        del.run(id, ...SyncState.#u(prefix));
      }
      out = { ok: true, rev: this.obligationRev(prefix) };
    });
    return out;
  }
  /** After a proven erasure: the prefix's reservations are no longer needed (its versions are gone). */
  /**
   * Retire, in ONE bounded page (at most `limit` rows, one IMMEDIATE transaction), the reservations made under
   * `scope` at exactly `exactKey` (or under the "/"-terminated `prefix`) that a complete strict proof showed absent.
   * Each moves to the protected erased history; everything else (other scopes' reservations, kept versions, siblings)
   * is untouched. The filter is in SQL (index range on key); `absent` decides per row. Resumable: rows are visited in
   * (key, version_id) order after the DURABLE cursor (`retire_cursor:<scope>`), so a crash resumes instead of
   * rescanning; the cursor is cleared when the scan is complete.
   */
  retireReservationsPage(prefix: string, scope: string, exactKey: string | undefined, absent: (key: string, versionId: string) => boolean, limit = 1000,
    maxSeq = Number.MAX_SAFE_INTEGER, proofId = "unbound", expectRev?: number): { retired: number; done: boolean; ok: boolean; rev: number } {
    let out = { retired: 0, done: false, ok: false, rev: -1 };
    this.#tx(() => {
      // Under the writer/retire lock (this IMMEDIATE transaction): the prefix revision must still be the one the caller
      // proved against; a dispatch / publish / reservation that landed meanwhile refuses this page (nothing retired).
      if (expectRev !== undefined && this.obligationRev(prefix) !== expectRev) {
        out = { retired: 0, done: false, ok: false, rev: this.obligationRev(prefix) };
        return;
      }
      const ck = `retire_cursor:${scope}`;
      // The cursor is bound to the proof/workset it was taken under ({proof, pos}). A different proof (new reservations,
      // a changed listing, another key or selection) DISCARDS it: retirement restarts from the start (idempotent),
      // so a row that now sorts before an old cursor is never skipped.
      const saved = JSON.parse(this.meta(ck) ?? "null") as { proof: string; pos: [string, string] } | null;
      const cur = saved && saved.proof === proofId ? saved.pos : null;
      const after = cur ? "AND (key > ? OR (key = ? AND version_id > ?))" : "";
      const where = exactKey !== undefined ? "key = ?" : SyncState.#under;
      // Bound to the PROOF's generation: only reservations at or below the high-water seq captured when the proof completed
      // (a reservation added later was not covered by that proof and stays reserved).
      const params: (string | number | Uint8Array)[] = [...(exactKey !== undefined ? [exactKey] : SyncState.#u(prefix)), scope, maxSeq, ...(cur ? [cur[0], cur[0], cur[1]] : []), limit];
      const rows = this.db.prepare(`SELECT key, version_id FROM erase_reservations WHERE ${where} AND scope = ? AND seq <= ? ${after} ORDER BY key, version_id LIMIT ?`).all(...params) as { key: string; version_id: string }[];
      const hist = this.db.prepare("INSERT OR IGNORE INTO erased_versions (key, version_id, at) VALUES (?, ?, ?)");
      const del = this.db.prepare("DELETE FROM erase_reservations WHERE key = ? AND version_id = ?");
      let retired = 0;
      for (const r of rows) {
        if (!absent(r.key, r.version_id)) continue;
        hist.run(r.key, r.version_id, Date.now());
        del.run(r.key, r.version_id);
        retired += 1;
      }
      const done = rows.length < limit;
      if (done) this.db.prepare("DELETE FROM meta WHERE k = ?").run(ck);
      else this.setMeta(ck, JSON.stringify({ proof: proofId, pos: [rows[rows.length - 1]!.key, rows[rows.length - 1]!.version_id] }));
      out = { retired, done, ok: true, rev: this.obligationRev(prefix) };
    });
    return out;
  }
  /**
   * Read-only bounded page: how many of this scope's reservations (<= maxSeq, at exactKey or under prefix) the current
   * proof says are absent but are still reserved. Used as the final assertion (must be 0) before any done claim.
   */
  eligibleRemainingPage(prefix: string, scope: string, exactKey: string | undefined, absent: (key: string, versionId: string) => boolean,
    maxSeq: number, after: [string, string] | null, limit = 1000): { eligible: number; next: [string, string] | null } {
    const where = exactKey !== undefined ? "key = ?" : SyncState.#under;
    const cond = after ? "AND (key > ? OR (key = ? AND version_id > ?))" : "";
    const params: (string | number | Uint8Array)[] = [...(exactKey !== undefined ? [exactKey] : SyncState.#u(prefix)), scope, maxSeq, ...(after ? [after[0], after[0], after[1]] : []), limit];
    const rows = this.db.prepare(`SELECT key, version_id FROM erase_reservations WHERE ${where} AND scope = ? AND seq <= ? ${cond} ORDER BY key, version_id LIMIT ?`).all(...params) as { key: string; version_id: string }[];
    let eligible = 0;
    for (const r of rows) if (absent(r.key, r.version_id)) eligible += 1;
    return { eligible, next: rows.length < limit ? null : [rows[rows.length - 1]!.key, rows[rows.length - 1]!.version_id] };
  }
  /**
   * The terminal claim, atomically with its re-validation: the queue row is removed (erasure done) only if the prefix
   * revision is still `expectRev` in the SAME transaction; otherwise nothing changes and the item stays queued.
   */
  completeErasure(key: string, enqueuedAt: number, prefix: string, expectRev: number): boolean {
    let ok = false;
    this.#tx(() => {
      if (this.obligationRev(prefix) !== expectRev) return;
      this.db.prepare("DELETE FROM erasures WHERE key = ? AND enqueued_at = ?").run(key, enqueuedAt);
      ok = true;
    });
    return ok;
  }
  /** Unbounded convenience over the paged retirement (tests): every page, in order. */
  retireReservations(prefix: string, absent: (key: string, versionId: string) => boolean, scope?: string) {
    const scopes = scope !== undefined ? [scope] : (this.db.prepare(`SELECT DISTINCT scope FROM erase_reservations WHERE ${SyncState.#under}`).all(...SyncState.#u(prefix)) as { scope: string }[]).map((r) => r.scope);
    for (const sc of scopes) while (!this.retireReservationsPage(prefix, sc, undefined, absent, 1000, Number.MAX_SAFE_INTEGER, "convenience").done);
  }
  /** The reservations' high-water seq under a prefix (the generation a completed proof covers). */
  reservationHighWater(prefix: string): number {
    return Number((this.db.prepare(`SELECT coalesce(max(seq), 0) AS n FROM erase_reservations WHERE ${SyncState.#under}`).get(...SyncState.#u(prefix)) as { n: number }).n);
  }
  /** The durable retirement cursor of a scope (tests / audits). */
  retireCursor(scope: string): [string, string] | null {
    return (JSON.parse(this.meta(`retire_cursor:${scope}`) ?? "null") as { pos: [string, string] } | null)?.pos ?? null;
  }
  /** EXPLAIN QUERY PLAN of the exact-key retirement scan (tests: an index on key, no prefix range). */
  retirePlan(key: string, scope: string): string {
    return (this.db.prepare("EXPLAIN QUERY PLAN SELECT key, version_id FROM erase_reservations WHERE key = ? AND scope = ? ORDER BY key, version_id LIMIT 1000").all(key, scope) as { detail: string }[]).map((r) => r.detail).join(" | ");
  }
  /** Versions in the protected erased history at one key (tests / audits). */
  erasedAt(key: string): string[] {
    return (this.db.prepare("SELECT version_id FROM erased_versions WHERE key = ? ORDER BY version_id").all(key) as { version_id: string }[]).map((r) => r.version_id);
  }
  /** The reservations at one key (tests / audits). */
  reservationsAt(key: string): string[] {
    return (this.db.prepare("SELECT version_id FROM erase_reservations WHERE key = ? ORDER BY version_id").all(key) as { version_id: string }[]).map((r) => r.version_id);
  }
  /** The protected terminal history of one invocation (tests / audits). */
  writeTerminal(id: string): { outcome: string; versionId: string | null } | undefined {
    return (this.db.prepare("SELECT outcome, version_id AS versionId FROM write_terminal WHERE id = ?").get(id) as { outcome: string; versionId: string | null } | undefined) ?? undefined;
  }

  erasureFailed(key: string, code: string, at = Date.now()) {
    this.db
      .prepare("UPDATE erasures SET attempts = attempts + 1, error = ?, next_try = ? + min(3600000, 60000 * (1 << min(attempts, 6))) WHERE key = ?")
      .run(code, at, key);
  }

  erasureBacklog(now = Date.now()): { queued: number; oldestAgeH: number | null; failing: number } {
    const r = this.db.prepare("SELECT count(*) AS n, min(enqueued_at) AS o, count(error) AS f FROM erasures").get() as {
      n: number;
      o: number | null;
      f: number;
    };
    return { queued: Number(r.n), oldestAgeH: r.o === null ? null : +((now - Number(r.o)) / 3_600_000).toFixed(2), failing: Number(r.f) };
  }

  /** The erasure job's own rollup sweep: every group Neon still has activity for. */
  seeForErasure(sweep: number, rows: { org_id: string; session_id: string }[]) {
    const q = this.db.prepare(
      `INSERT INTO erasure_seen (org_id, session_id, sweep) VALUES (?, ?, ?)
       ON CONFLICT (org_id, session_id) DO UPDATE SET sweep = excluded.sweep`,
    );
    this.#tx(() => {
      for (const r of rows) q.run(r.org_id, r.session_id, sweep);
    });
  }

  /**
   * After a complete erasure sweep: exported groups Neon no longer has (the
   * exporter's units come only from rollups, so no rollup means nothing to keep).
   */
  vanishedExports(sweep: number, sweepStartedAt: number, prune = true): { org_id: string; session_id: string; keys: string[] }[] {
    // Only exports older than the sweep: a group created behind the sweep's cursor and
    // exported meanwhile is not missing from Neon, only from this sweep.
    const rows = this.db
      .prepare(
        `SELECT e.org_id, e.session_id, e.keys FROM exported e
          LEFT JOIN erasure_seen s ON s.org_id = e.org_id AND s.session_id = e.session_id AND s.sweep = ?
          WHERE s.org_id IS NULL AND e.keys <> '[]' AND e.exported_at < ?
            AND NOT EXISTS (SELECT 1 FROM pending_keys p WHERE p.org_id = e.org_id AND p.session_id = e.session_id)`,
      )
      .all(sweep, sweepStartedAt) as { org_id: string; session_id: string; keys: string }[];
    if (prune) this.db.prepare("DELETE FROM erasure_seen WHERE sweep < ?").run(sweep);
    return rows.map((r) => ({ ...r, keys: JSON.parse(r.keys) as string[] }));
  }

  /** A group whose session prefixes are queued for erasure forgets them. */
  forgetKeys(org: string, session: string, keys: string[]) {
    this.#tx(() => {
      const prior = this.exported(org, session);
      if (!prior) return;
      const left = prior.keys.filter((k) => !keys.includes(k));
      this.db.prepare("UPDATE exported SET keys = ? WHERE org_id = ? AND session_id = ?").run(JSON.stringify(left), org, session);
    });
  }

  /** Exported groups stuck in error while they still own keys (an SLA risk the queue age does not show). */
  stuckWithKeys(): number {
    return Number((this.db.prepare("SELECT count(*) AS n FROM exported WHERE error IS NOT NULL AND keys <> '[]' AND attempts >= 6").get() as { n: number }).n);
  }

  /**
   * After the sealer takes long strings (scrub rule set 2): make every group in
   * the truncation ledger look changed, so it is re-read and stored whole. The
   * ledger rows clear as each group re-exports without a cut.
   */
  forceReexportTruncated(): number {
    let n = 0;
    this.#tx(() => {
      // Healthy groups: look changed. Failing ones (even past the attempt cap):
      // retry now, so no ledgered group is left holding T0 shut.
      n =
        Number(
          this.db
            .prepare(
              `UPDATE exported SET revision = -2, exported_at = 0
                WHERE error IS NULL AND (org_id, session_id) IN (SELECT DISTINCT org_id, session_id FROM truncations)`,
            )
            .run().changes,
        ) +
        Number(
          this.db
            .prepare(
              `UPDATE exported SET failed_revision = -2, attempts = 0, next_try = 0
                WHERE error IS NOT NULL AND (org_id, session_id) IN (SELECT DISTINCT org_id, session_id FROM truncations)`,
            )
            .run().changes,
        );
    });
    return n;
  }

  truncationTotals(): { sessions: number; manifests: number; strings: number; maxChars?: number } {
    const r = this.db
      .prepare(
        "SELECT count(DISTINCT org_id || '/' || session_id) AS s, count(*) AS m, coalesce(sum(strings), 0) AS n, coalesce(max(max_chars), 0) AS x FROM truncations",
      )
      .get() as { s: number; m: number; n: number; x: number };
    return { sessions: Number(r.s), manifests: Number(r.m), strings: Number(r.n), ...(Number(r.x) ? { maxChars: Number(r.x) } : {}) };
  }

  /**
   * A deletion fence (tombstone or reset) changed without bumping the
   * session's revision: make the exported group look changed, past its debounce.
   */
  forceChanged(groups: { org_id: string; session_id: string }[]): number {
    const ok = this.db.prepare(
      `UPDATE exported SET revision = -2, exported_at = 0
        WHERE org_id = ? AND session_id = ? AND error IS NULL AND keys <> '[]' AND revision <> -2`,
    );
    // A failed group past its attempt cap is revived (once per cap); others
    // keep their backoff, which already retries them.
    const failed = this.db.prepare(
      `UPDATE exported SET failed_revision = -2, attempts = 0, next_try = 0
        WHERE org_id = ? AND session_id = ? AND error IS NOT NULL AND keys <> '[]'
          AND attempts >= 6`,
    );
    let n = 0;
    this.#tx(() => {
      for (const g of groups)
        n += Number(ok.run(g.org_id, g.session_id).changes) + Number(failed.run(g.org_id, g.session_id).changes);
    });
    return n;
  }

  /**
   * The next groups to export, in priority order:
   * (A group gone from Neon is the erasure job's, not the exporter's.)
   *  1. changed since their export and past their debounce (bigger sessions wait longer, at most 24 h);
   *  2. never exported, active in the last `recentMs`, newest first;
   *  3. never exported, the rest of history, newest first;
   *  4. failed ones due for a retry, or whose revision moved since;
   *  5. when nothing else waits, exports older than `reverifyMs`, oldest first.
   */
  nextWork(now: number, recentMs: number, limit: number, reverifyMs = 0): WorkItem[] {
    const out: WorkItem[] = [];
    const take = (sql: string, kind: WorkItem["kind"], ...params: (number | string)[]) => {
      if (out.length >= limit) return;
      const rows = this.db.prepare(sql).all(...params, limit - out.length) as Omit<WorkItem, "kind">[];
      for (const r of rows) out.push({ ...r, kind });
    };
    take(
      `SELECT g.org_id, g.session_id, g.revision, g.events FROM groups g JOIN exported e USING (org_id, session_id)
        WHERE e.error IS NULL AND g.revision <> e.revision
          AND e.exported_at + min(86400000, 1800000 + g.events * 600) <= ?
        ORDER BY g.last_ms DESC NULLS FIRST LIMIT ?`,
      "changed",
      now,
    );
    take(
      `SELECT g.org_id, g.session_id, g.revision, g.events FROM groups g
        LEFT JOIN exported e USING (org_id, session_id)
        WHERE e.org_id IS NULL AND (g.last_ms IS NULL OR g.last_ms >= ?)
        ORDER BY g.last_ms DESC NULLS LAST LIMIT ?`,
      "recent",
      now - recentMs,
    );
    take(
      `SELECT g.org_id, g.session_id, g.revision, g.events FROM groups g
        LEFT JOIN exported e USING (org_id, session_id)
        WHERE e.org_id IS NULL AND g.last_ms < ? ORDER BY g.last_ms DESC LIMIT ?`,
      "history",
      now - recentMs,
    );
    take(
      `SELECT g.org_id, g.session_id, g.revision, g.events FROM groups g JOIN exported e USING (org_id, session_id)
        WHERE e.error IS NOT NULL
          AND ((e.next_try <= ? AND e.attempts < 6) OR g.revision <> coalesce(e.failed_revision, g.revision))
        ORDER BY e.next_try LIMIT ?`,
      "retry",
      now,
    );
    // Last: a rolling re-read of old exports, for changes Neon records without a
    // revision bump (delivery records that never project to an event). Only when
    // nothing else is waiting; a re-read of an unchanged session writes nothing.
    if (out.length === 0 && reverifyMs > 0)
      take(
        `SELECT g.org_id, g.session_id, g.revision, g.events FROM groups g JOIN exported e USING (org_id, session_id)
          WHERE e.error IS NULL AND e.exported_at < ? ORDER BY e.exported_at LIMIT ?`,
        "reverify",
        now - reverifyMs,
      );
    // A group can match two queries only across them (e.g. never twice); dedupe defensively.
    const seen = new Set<string>();
    return out.filter((w) => {
      const k = `${w.org_id}\u0000${w.session_id}`;
      if (seen.has(k)) return false;
      seen.add(k);
      return true;
    });
  }

  backlog(now: number, recentMs: number): Backlog {
    const one = (sql: string, ...p: number[]) =>
      Object.values(this.db.prepare(sql).get(...p) as Record<string, number>).map((v) => Number(v ?? 0));
    const [groups, exported] = one(
      `SELECT (SELECT count(*) FROM groups), (SELECT count(*) FROM exported WHERE error IS NULL)`,
    );
    const [pending, pendingEvents] = one(
      `SELECT count(*), sum(g.events) FROM groups g LEFT JOIN exported e USING (org_id, session_id) WHERE e.org_id IS NULL`,
    );
    const [recentPending, recentPendingEvents] = one(
      `SELECT count(*), sum(g.events) FROM groups g LEFT JOIN exported e USING (org_id, session_id)
        WHERE e.org_id IS NULL AND (g.last_ms IS NULL OR g.last_ms >= ?)`,
      now - recentMs,
    );
    const [changed] = one(
      `SELECT count(*) FROM groups g JOIN exported e USING (org_id, session_id) WHERE e.error IS NULL AND g.revision <> e.revision`,
    );
    const [errored] = one(`SELECT count(*) FROM exported WHERE error IS NOT NULL`);
    const [pendingDeletes] = one(
      `SELECT count(*) FROM exported e LEFT JOIN groups g USING (org_id, session_id) WHERE g.org_id IS NULL AND e.keys <> '[]'`,
    );
    return {
      groups: groups!,
      exported: exported!,
      pending: pending!,
      pendingEvents: pendingEvents!,
      recentPending: recentPending!,
      recentPendingEvents: recentPendingEvents!,
      changed: changed!,
      errored: errored!,
      pendingDeletes: pendingDeletes!,
    };
  }

  errorCounts(): Record<string, number> {
    const rows = this.db.prepare("SELECT error, count(*) AS n FROM exported WHERE error IS NOT NULL GROUP BY error").all() as {
      error: string;
      n: number;
    }[];
    return Object.fromEntries(rows.map((r) => [r.error, r.n]));
  }

  close() {
    this.db.close();
  }
}
