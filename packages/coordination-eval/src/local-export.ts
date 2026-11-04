import { Database } from "bun:sqlite";
import { createHash } from "node:crypto";
import {
  readFile,
  mkdir,
  writeFile,
  mkdtemp,
  link,
  lstat,
  unlink,
  rm,
} from "node:fs/promises";
import { dirname, join } from "node:path";
import { scrubText, MAX_SCRUB_CHARS } from "../../pipeline/src/lib/scrub.ts";
import {
  prepareAssessment,
  type ExportEvent,
  type SessionExport,
} from "./intake.ts";

export const LOCAL_EXPORT_VERSION = "coordination-local-export/0.1.0";
const MAX_EVENTS = 10000;
const MAX_SESSIONS = 1000;
const MAX_METADATA_ROWS = 100000;
const MAX_BYTES = 16 * 1024 * 1024;

export interface LocalExportOptions {
  dbPath: string;
  organizationId: string;
  workspaceId: string;
  since: string;
  until: string;
  projectPrefix: string;
  machineLabel: string;
  sessionKeys?: string[];
  perSessionLimit?: number;
  fileEditLimit?: number;
  excerptChars?: number;
  timeoutMs?: number;
  exportedAt?: string;
}

interface CatalogRow {
  source: string;
  session_id: string;
  title: string | null;
  project_key: string;
  cwd: string | null;
  discovery_state: string;
  revision: number;
}
interface EventRow {
  id: number;
  event_uid: string;
  ts_ms: number;
  text: string;
  role: string;
  payload_truncated: number | null;
  revision: number;
}
interface EditRow {
  id: number;
  tool_use_id: string;
  ts_ms: number;
  file_path: string;
  cwd: string | null;
  tool_name: string;
  revision: number;
}
interface SelectedSession {
  key: string;
  discoveryState: string;
  conversationCandidates: number;
  fileEditCandidates: number;
  selectedConversations: number;
  selectedFileEdits: number;
  emptySelectedConversations: number;
  omittedConversations: number;
  omittedFileEdits: number;
}

const digest = (value: unknown) =>
  createHash("sha256").update(JSON.stringify(value)).digest("hex");
const sessionKey = (source: string, id: string) =>
  `session-${digest([source, id])}`;
function fail(message: string): never {
  throw new Error(`local export: ${message}`);
}
function clean(value: string): string {
  return scrubText(value).replace(
    /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g,
    "\ufffd",
  );
}
function metadata(value: string): string {
  return clean(value).replace(/\s+/g, " ").trim().slice(0, 1024);
}
function identity(value: string): string {
  if (
    !/^[a-zA-Z0-9][a-zA-Z0-9_.:/@-]{0,255}$/.test(value) ||
    clean(value) !== value
  )
    fail("source identity is unsafe or unsupported");
  return value;
}
function date(value: string): number {
  const ms = Date.parse(value);
  if (!Number.isFinite(ms) || new Date(ms).toISOString() !== value)
    fail("timestamps must be UTC ISO strings with milliseconds");
  return ms;
}
function bounded(value: number, min: number, max: number): number {
  if (!Number.isInteger(value) || value < min || value > max)
    fail("unsupported numeric limit");
  return value;
}
function sample<T>(rows: T[], limit: number): T[] {
  if (rows.length <= limit) return rows;
  if (limit === 0) return [];
  if (limit === 1) return [rows[0]!];
  return Array.from(
    { length: limit },
    (_, i) => rows[Math.floor((i * (rows.length - 1)) / (limit - 1))]!,
  );
}

/** Explicit opt-in local pilot adapter over ai-hist's normalized tables, never provider files. */
export async function exportLocalWorkspace(options: LocalExportOptions) {
  const since = date(options.since),
    until = date(options.until);
  if (since >= until) fail("reporting window is empty");
  const exportedAt = options.exportedAt ?? new Date().toISOString();
  if (date(exportedAt) < until) fail("export precedes the reporting cutoff");
  const limit = bounded(options.perSessionLimit ?? 20, 1, 1000);
  const editLimit = bounded(options.fileEditLimit ?? 6, 0, limit);
  const excerptChars = bounded(options.excerptChars ?? 1200, 100, 12000);
  const timeoutMs = bounded(options.timeoutMs ?? 120000, 1000, 300000);
  const organizationId = identity(options.organizationId),
    workspaceId = identity(options.workspaceId);
  if (!/^[a-zA-Z0-9.-]+\/[a-zA-Z0-9_.-]+\/$/.test(options.projectPrefix))
    fail("project prefix must identify one remote organization");
  const label = metadata(options.machineLabel);
  if (!label) fail("machine coverage label is required");
  const configPath = join(dirname(options.dbPath), "config.json");
  let config: Record<string, unknown>;
  try {
    const raw = await readFile(configPath, "utf8");
    if (raw.length > 65536) fail("scope metadata exceeds its supported bound");
    const parsed: unknown = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
      fail("invalid probe scope metadata");
    config = parsed as Record<string, unknown>;
  } catch {
    fail("could not read credential-free sibling probe scope metadata");
  }
  if (
    config.version !== 1 ||
    config.site_url !== "https://agentrelay.com" ||
    config.org_id !== organizationId ||
    config.workspace_id !== workspaceId
  )
    fail("requested scope does not match the production probe configuration");
  if (config.sharing_mode !== "new" && config.sharing_mode !== "all")
    fail(
      "only explicit all/new sharing modes are supported by this pilot adapter",
    );
  if (typeof config.job_id !== "string")
    fail("probe delivery job metadata is missing");
  const requested = options.sessionKeys ? new Set(options.sessionKeys) : null;
  if (
    requested &&
    (requested.size !== options.sessionKeys!.length ||
      requested.size > MAX_SESSIONS)
  )
    fail("invalid selected session keys");
  const deadline = Date.now() + timeoutMs;
  const db = new Database(options.dbPath, { readonly: true, strict: true });
  const checkBudget = () => {
    if (Date.now() > deadline)
      fail("read budget exceeded; narrow the selected sessions or window");
    return 1;
  };
  const events: ExportEvent[] = [];
  const sessionStats: SelectedSession[] = [];
  const locators: Array<{
    evidenceId: string;
    table: string;
    rowId: number;
    revision: number;
    role: string | null;
  }> = [];
  try {
    db.exec("PRAGMA query_only=ON; PRAGMA busy_timeout=2000; BEGIN");
    db.function("coordination_export_budget", checkBudget);
    const job = db
      .query<{ state: string }, [string]>(
        "SELECT state FROM delivery_jobs WHERE id=?",
      )
      .get(config.job_id);
    if (!job || !["active", "paused"].includes(job.state))
      fail("configured sharing job is not active or paused");
    const store = db
      .query<{ epoch: string; schema_digest: string | null }, []>(
        "SELECT CAST(epoch AS TEXT) epoch, export_schema_digest schema_digest FROM change_feed_store LIMIT 1",
      )
      .get();
    if (!store) fail("local store identity metadata is missing");
    const storeIdentity = `local-store-${digest(store.epoch).slice(0, 24)}`;
    const rows = db
      .query<CatalogRow, [number, number, string, string, number]>(
        `
      SELECT s.source,s.session_id,s.title,s.project_key,s.cwd,s.discovery_state,s.revision
      FROM sessions s
      WHERE s.last_activity_ms>=? AND s.first_activity_ms<?
        AND s.project_key_method='remote' AND substr(s.project_key,1,length(?))=?
        AND NOT EXISTS (SELECT 1 FROM delivery_exclusions x WHERE x.source=s.source AND x.session_id=s.session_id)
        AND coordination_export_budget()=1
      ORDER BY s.source,s.session_id LIMIT ?
    `,
      )
      .all(
        since,
        until,
        options.projectPrefix,
        options.projectPrefix,
        MAX_SESSIONS + 1,
      );
    if (rows.length > MAX_SESSIONS)
      fail("catalog exceeds the pilot session bound");
    const selected = rows.filter(
      (row) =>
        !requested || requested.has(sessionKey(row.source, row.session_id)),
    );
    if (requested && selected.length !== requested.size)
      fail("a selected session is outside the admitted catalog");
    if (selected.length * limit > MAX_EVENTS)
      fail(
        "selection exceeds the total event cap; reduce the per-session limit",
      );
    const sessions = selected.map((row) => ({
      key: sessionKey(row.source, row.session_id),
      organizationId,
      workspaceId,
      source: identity(row.source),
      sessionId: identity(row.session_id),
      title: metadata(row.title ?? "Untitled session") || "Untitled session",
      projectKey: metadata(row.project_key),
      taskRefs: [],
      parentSessionKey: null as string | null,
    }));
    const keys = new Set(sessions.map((s) => s.key));
    let unexpandedDelegations = 0;
    for (const row of selected) {
      checkBudget();
      const key = sessionKey(row.source, row.session_id);
      const conversationRows = db
        .query<{ id: number }, [string, string, number, number, number]>(
          `
        SELECT id FROM session_events
        WHERE source=? AND session_id=? AND ts_ms>=? AND ts_ms<?
          AND role IN ('user','assistant') AND kind='text' AND text IS NOT NULL
          AND coalesce(is_meta,0)=0 AND control_kind IS NULL AND location='local'
          AND coordination_export_budget()=1
        ORDER BY ts_ms,id LIMIT ?
      `,
        )
        .all(row.source, row.session_id, since, until, MAX_METADATA_ROWS + 1);
      const edits = db
        .query<{ id: number }, [string, string, number, number, number]>(
          `
        SELECT id FROM file_edits WHERE source=? AND session_id=? AND ts_ms>=? AND ts_ms<?
          AND location='local' AND coordination_export_budget()=1
        ORDER BY ts_ms,id LIMIT ?
      `,
        )
        .all(row.source, row.session_id, since, until, MAX_METADATA_ROWS + 1);
      if (
        conversationRows.length > MAX_METADATA_ROWS ||
        edits.length > MAX_METADATA_ROWS
      )
        fail("one session exceeds the metadata scan bound");
      const chosenEdits = sample(edits, Math.min(editLimit, edits.length));
      const chosenConversations = sample(
        conversationRows,
        limit - chosenEdits.length,
      );
      let empty = 0;
      for (const picked of chosenConversations) {
        const e = db
          .query<EventRow, [number]>(
            "SELECT id,event_uid,ts_ms,text,role,payload_truncated,revision FROM session_events WHERE id=?",
          )
          .get(picked.id)!;
        const scrubbed = clean(e.text);
        if (!scrubbed.trim()) {
          empty++;
          continue;
        }
        const id = `event-${digest([row.source, row.session_id, "session_events", e.event_uid])}`;
        events.push({
          id,
          sessionKey: key,
          machineId: storeIdentity,
          eventId: identity(e.event_uid),
          kind: "text",
          occurredAt: new Date(e.ts_ms).toISOString(),
          availableAt: null,
          text: `${e.role === "user" ? "User" : "Assistant"}: ${scrubbed.slice(0, excerptChars)}`,
          truncated:
            Boolean(e.payload_truncated) ||
            scrubbed.length > excerptChars ||
            e.text.length > MAX_SCRUB_CHARS,
          filesTouched: [],
        });
        locators.push({
          evidenceId: id,
          table: "session_events",
          rowId: e.id,
          revision: e.revision,
          role: e.role,
        });
      }
      for (const picked of chosenEdits) {
        const e = db
          .query<EditRow, [number]>(
            "SELECT id,tool_use_id,ts_ms,file_path,cwd,tool_name,revision FROM file_edits WHERE id=?",
          )
          .get(picked.id)!;
        // Keep the recorded path; no inferred repository/worktree-root equivalence.
        const fullPath = clean(e.file_path);
        const file =
          fullPath.length <= 1024 && !/[\u0000-\u001f\u007f]/.test(fullPath)
            ? fullPath
            : null;
        const id = `event-${digest([row.source, row.session_id, "file_edits", e.tool_use_id])}`;
        events.push({
          id,
          sessionKey: key,
          machineId: storeIdentity,
          eventId: identity(e.tool_use_id),
          kind: "file_edit",
          occurredAt: new Date(e.ts_ms).toISOString(),
          availableAt: null,
          text: `Normalized file edit: ${metadata(e.tool_name)}; path: ${metadata(e.file_path)}. The edit record does not establish correctness or task completion.`,
          truncated: file === null || clean(e.tool_name).length > 1024,
          filesTouched: file ? [file] : [],
        });
        locators.push({
          evidenceId: id,
          table: "file_edits",
          rowId: e.id,
          revision: e.revision,
          role: null,
        });
      }
      const children = db
        .query<{ child_session_id: string }, [string, string]>(
          "SELECT child_session_id FROM session_relationships WHERE source=? AND parent_session_id=? AND relationship='delegated' AND child_session_id IS NOT NULL AND coordination_export_budget()=1",
        )
        .all(row.source, row.session_id);
      for (const child of children) {
        const childKey = sessionKey(row.source, child.child_session_id);
        if (!keys.has(childKey)) {
          unexpandedDelegations++;
          continue;
        }
        const target = sessions.find((s) => s.key === childKey)!;
        if (target.parentSessionKey && target.parentSessionKey !== key)
          fail("ambiguous delegated parent in selected catalog");
        target.parentSessionKey = key;
      }
      sessionStats.push({
        key,
        discoveryState: row.discovery_state,
        conversationCandidates: conversationRows.length,
        fileEditCandidates: edits.length,
        selectedConversations: chosenConversations.length - empty,
        selectedFileEdits: chosenEdits.length,
        emptySelectedConversations: empty,
        omittedConversations:
          conversationRows.length - chosenConversations.length + empty,
        omittedFileEdits: edits.length - chosenEdits.length,
      });
    }
    events.sort(
      (a, b) =>
        a.occurredAt.localeCompare(b.occurredAt) || a.id.localeCompare(b.id),
    );
    const omittedConversations = sessionStats.reduce(
      (n, s) => n + s.omittedConversations,
      0,
    );
    const omittedFileEdits = sessionStats.reduce(
      (n, s) => n + s.omittedFileEdits,
      0,
    );
    const exported: SessionExport = {
      schemaVersion: 1,
      synthetic: false,
      scope: { organizationId, workspaceId },
      window: { since: options.since, until: options.until },
      coverage: {
        status: "partial",
        notes: `Local evidence from ${label} only; not team-wide coverage. ${sessions.length} selected canonical remote-project catalog sessions; ${events.length} sampled conversational/file-edit records. At most ${limit} records per session, reserving up to ${editLimit} for file edits; evenly spaced chronological sampling includes first/last when the allocation is at least two. Omitted ${omittedConversations} conversational records and ${omittedFileEdits} file edits. Excerpts capped at ${excerptChars} characters after redaction. Other event kinds, unknown-timestamp edits, and ${unexpandedDelegations} delegated relationships outside the selected catalog are not expanded. Missing parentage is not proof of session independence. File references retain recorded paths; equivalence across worktrees is not inferred. Sharing eligibility is checked against current all/new exclusions; upload completeness and historical permissions are not established. machineId identifies this local store, not a hosted machine. Historical availability is unknown.`,
      },
      provenance: {
        source: `${LOCAL_EXPORT_VERSION}; normalized ai-hist tables; store=${storeIdentity}; schema=${digest(store.schema_digest)}; no provider parsing`,
        exportedAt,
        redacted: true,
      },
      sessions,
      events,
    };
    prepareAssessment(exported);
    if (Buffer.byteLength(JSON.stringify(exported, null, 2)) > MAX_BYTES)
      fail("export exceeds 16 MiB; narrow the selection or excerpts");
    return {
      exported,
      inventory: {
        exporterVersion: LOCAL_EXPORT_VERSION,
        scope: exported.scope,
        window: exported.window,
        machineLabel: label,
        catalogEligibleSessions: rows.length,
        selectedSessions: sessions.length,
        selectedEvents: events.length,
        storeIdentity,
        storeEpoch: store.epoch,
        sourceSchemaDigest: digest(store.schema_digest),
        selection: {
          method:
            "evenly-spaced chronological conversational and file-edit samples",
          perSessionLimit: limit,
          fileEditLimit: editLimit,
          excerptChars,
          maxEvents: MAX_EVENTS,
          projectPrefix: options.projectPrefix,
          sharingMode: config.sharing_mode,
        },
        omittedConversations,
        omittedFileEdits,
        unexpandedDelegations,
        sessions: sessionStats,
        locators,
      },
    };
  } catch (error) {
    if (error instanceof Error && error.message.startsWith("local export:"))
      throw error;
    fail(
      "read failed or normalized schema is unsupported; no source mutation was attempted",
    );
  } finally {
    if (db.inTransaction) db.exec("ROLLBACK");
    db.close();
  }
}

export async function writeLocalExport(
  options: LocalExportOptions,
  outputPath: string,
) {
  const result = await exportLocalWorkspace(options);
  await mkdir(dirname(outputPath), { recursive: true, mode: 0o700 });
  const staging = await mkdtemp(
    join(dirname(outputPath), ".coordination-export-"),
  );
  const stagedExport = join(staging, "export.json");
  const stagedInventory = join(staging, "inventory.json");
  const inventoryPath = `${outputPath}.inventory.json`;
  let publishedInventory = false;
  let publishedExport = false;
  try {
    await writeFile(
      stagedExport,
      JSON.stringify(result.exported, null, 2) + "\n",
      { flag: "wx", mode: 0o600 },
    );
    await writeFile(
      stagedInventory,
      JSON.stringify(result.inventory, null, 2) + "\n",
      { flag: "wx", mode: 0o600 },
    );
    // The complete export is the commit point. Hard links publish atomically
    // without replacing another invocation's files, unlike rename().
    await link(stagedInventory, inventoryPath);
    publishedInventory = true;
    await link(stagedExport, outputPath);
    publishedExport = true;
  } finally {
    try {
      if (publishedInventory && !publishedExport) {
        const owned = await lstat(stagedInventory);
        const current = await lstat(inventoryPath).catch((error) => {
          if (error.code === "ENOENT") return null;
          throw error;
        });
        if (current?.ino === owned.ino && current.dev === owned.dev)
          await unlink(inventoryPath);
      }
    } finally {
      await rm(staging, { recursive: true, force: true });
    }
  }
  return result.inventory;
}
