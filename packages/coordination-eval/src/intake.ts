import { createHash } from "node:crypto";
import { scrubText } from "../../pipeline/src/lib/scrub.ts";
import {
  parseAssessment,
  type Assessment,
  type Evidence,
  type Observation,
  type Session,
  type Workstream,
} from "./contract.ts";

export const INTAKE_VERSION = "coordination-intake/0.1.0";
const PROXIMITY_MS = 60 * 60 * 1000;
const MAX_OBSERVATIONS = 200;

export interface ExportSession extends Session {
  title: string;
  projectKey: string | null;
  taskRefs: string[];
  parentSessionKey: string | null;
}

export interface ExportEvent extends Evidence {
  filesTouched: string[];
}

export interface SessionExport {
  schemaVersion: 1;
  synthetic: boolean;
  scope: Assessment["scope"];
  window: Assessment["window"];
  coverage: Pick<Assessment["coverage"], "status" | "notes">;
  provenance: Assessment["provenance"];
  sessions: ExportSession[];
  events: ExportEvent[];
}

export interface IntakeInventory {
  intakeVersion: string;
  sessions: number;
  sessionsWithWindowEvidence: number;
  sessionsWithoutWindowEvidence: string[];
  declaredSessionRoots: number;
  events: number;
  windowEvents: number;
  truncatedEvents: number;
  unknownAvailabilityEvents: number;
  sources: Array<{ source: string; sessions: number; events: number }>;
  workstreams: Array<{
    id: string;
    grouping: "task" | "project" | "session";
    projectKey: string | null;
    taskRef: string | null;
  }>;
  observations: {
    count: number;
    limit: number;
    limitReached: boolean;
    proximityMinutes: number;
  };
  limitations: string[];
}

function fail(message: string): never {
  throw new Error(`session export: ${message}`);
}

function object(value: unknown, keys: string[]): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value))
    fail("expected object");
  const result = value as Record<string, unknown>;
  if (Object.keys(result).some((key) => !keys.includes(key)))
    fail("unknown field");
  return result;
}

function list(value: unknown, maximum: number): unknown[] {
  if (!Array.isArray(value) || value.length > maximum)
    fail("array exceeds the supported bound or is not an array");
  return value;
}

function text(value: unknown): string {
  if (
    typeof value !== "string" ||
    !value.trim() ||
    value.length > 1024 ||
    /[\u0000-\u001f\u007f]/.test(value)
  )
    fail("expected bounded metadata text");
  if (scrubText(value) !== value) fail("requires upstream redaction");
  return value;
}

function strings(value: unknown, maximum: number): string[] {
  const result = list(value, maximum).map(text);
  if (new Set(result).size !== result.length)
    fail("duplicate metadata reference");
  return result.sort();
}

function stableId(prefix: string, values: unknown[]): string {
  return `${prefix}-${createHash("sha256").update(JSON.stringify(values)).digest("hex")}`;
}

function compare(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}

function parseExport(input: unknown): SessionExport {
  const raw = object(input, [
    "schemaVersion",
    "synthetic",
    "scope",
    "window",
    "coverage",
    "provenance",
    "sessions",
    "events",
  ]);
  const coverage = object(raw.coverage, ["status", "notes"]);
  const sessions = list(raw.sessions, 10000).map((item) =>
    object(item, [
      "key",
      "organizationId",
      "workspaceId",
      "source",
      "sessionId",
      "title",
      "projectKey",
      "taskRefs",
      "parentSessionKey",
    ]),
  );
  const events = list(raw.events, 10000).map((item) =>
    object(item, [
      "id",
      "sessionKey",
      "machineId",
      "eventId",
      "kind",
      "occurredAt",
      "availableAt",
      "text",
      "truncated",
      "filesTouched",
    ]),
  );
  const base = parseAssessment({
    schemaVersion: raw.schemaVersion,
    synthetic: raw.synthetic,
    scope: raw.scope,
    window: raw.window,
    coverage: {
      ...coverage,
      baselineSelection:
        "Census of exported workstream groups with evidence in the reporting window; groups have not been reviewed.",
    },
    provenance: raw.provenance,
    sessions: sessions.map(
      ({ key, organizationId, workspaceId, source, sessionId }) => ({
        key,
        organizationId,
        workspaceId,
        source,
        sessionId,
      }),
    ),
    evidence: events.map(
      ({
        id,
        sessionKey,
        machineId,
        eventId,
        kind,
        occurredAt,
        availableAt,
        text,
        truncated,
      }) => ({
        id,
        sessionKey,
        machineId,
        eventId,
        kind,
        occurredAt,
        availableAt,
        text,
        truncated,
      }),
    ),
    workstreams: [],
    findings: [],
  });
  return {
    schemaVersion: 1,
    synthetic: base.synthetic,
    scope: base.scope,
    window: base.window,
    coverage: { status: base.coverage.status, notes: base.coverage.notes },
    provenance: base.provenance,
    sessions: base.sessions
      .map((s, i) => ({
        ...s,
        title: text(sessions[i]!.title),
        projectKey:
          sessions[i]!.projectKey === null
            ? null
            : text(sessions[i]!.projectKey),
        taskRefs: strings(sessions[i]!.taskRefs, 64),
        parentSessionKey:
          sessions[i]!.parentSessionKey === null
            ? null
            : text(sessions[i]!.parentSessionKey),
      }))
      .sort((a, b) => compare(a.key, b.key)),
    events: base.evidence
      .map((e, i) => ({
        ...e,
        filesTouched: strings(events[i]!.filesTouched, 256),
      }))
      .sort(
        (a, b) => compare(a.occurredAt, b.occurredAt) || compare(a.id, b.id),
      ),
  };
}

interface Group {
  id: string;
  grouping: "task" | "project" | "session";
  projectKey: string | null;
  taskRef: string | null;
  title: string;
  sessions: Set<string>;
  events: ExportEvent[];
}

/** Uses declared metadata and event proximity; never interprets a shared file as waste. */
export function prepareAssessment(input: unknown): {
  assessment: Assessment;
  inventory: IntakeInventory;
} {
  const exported = parseExport(input);
  const sessions = new Map(exported.sessions.map((s) => [s.key, s]));
  const roots = new Map<string, string>();
  for (const session of exported.sessions) {
    const visited = new Set<string>();
    let cursor = session;
    while (cursor.parentSessionKey !== null) {
      if (visited.has(cursor.key)) fail("cyclic parent session references");
      visited.add(cursor.key);
      const parent = sessions.get(cursor.parentSessionKey);
      if (!parent) fail("parent session is missing from the scoped export");
      cursor = parent;
    }
    roots.set(session.key, cursor.key);
  }
  const project = (key: string) =>
    sessions.get(key)!.projectKey ?? sessions.get(roots.get(key)!)!.projectKey;
  const taskRefs = (key: string) => {
    const own = sessions.get(key)!.taskRefs;
    return own.length ? own : sessions.get(roots.get(key)!)!.taskRefs;
  };
  const windowEvents = exported.events.filter(
    (e) => e.occurredAt >= exported.window.since,
  );
  const activeSessions = new Set(windowEvents.map((e) => e.sessionKey));
  const groups = new Map<string, Group>();
  const membership = new Map<string, Set<string>>();
  for (const event of windowEvents) {
    const p = project(event.sessionKey);
    const tasks = taskRefs(event.sessionKey);
    const specifications = tasks.length
      ? tasks.map((taskRef) => ({
          grouping: "task" as const,
          taskRef,
          identity: ["task", p ?? roots.get(event.sessionKey), taskRef],
          title: `${p ? `${p} · ` : ""}${taskRef}`,
        }))
      : [
          {
            grouping: p ? ("project" as const) : ("session" as const),
            taskRef: null,
            identity: p
              ? ["project", p]
              : ["session", roots.get(event.sessionKey)],
            title: p ?? sessions.get(roots.get(event.sessionKey)!)!.title,
          },
        ];
    for (const spec of specifications) {
      const id = stableId("workstream", spec.identity);
      let group = groups.get(id);
      if (!group) {
        group = {
          id,
          grouping: spec.grouping,
          projectKey: p,
          taskRef: spec.taskRef,
          title: spec.title,
          sessions: new Set(),
          events: [],
        };
        groups.set(id, group);
      }
      group.sessions.add(event.sessionKey);
      group.events.push(event);
      const current = membership.get(event.sessionKey) ?? new Set<string>();
      current.add(id);
      membership.set(event.sessionKey, current);
    }
  }
  const orderedGroups = [...groups.values()].sort((a, b) =>
    compare(a.id, b.id),
  );
  const workstreams: Workstream[] = orderedGroups.map((group) => ({
    id: group.id,
    title: group.title,
    selection: "baseline",
    sessionKeys: [...group.sessions].sort(),
    state: "unknown",
    summary: `${group.sessions.size} exported session(s), ${group.events.length} event(s) in this ${group.grouping} group. Activity spans ${group.events[0]!.occurredAt} to ${group.events.at(-1)!.occurredAt}. Progress, blockers and outcomes require review.`,
    summaryEvidenceIds: [
      ...new Set([group.events[0]!.id, group.events.at(-1)!.id]),
    ],
    assessment: "pending",
  }));

  const observations: Observation[] = [];
  const seen = new Set<string>();
  let limitReached = false;
  const observe = (
    kind: "shared_surface" | "shared_task",
    signal: string[],
    first: ExportEvent,
    second: ExportEvent,
    description: string,
  ): boolean => {
    if (roots.get(first.sessionKey) === roots.get(second.sessionKey))
      return true;
    const pair = [
      roots.get(first.sessionKey)!,
      roots.get(second.sessionKey)!,
    ].sort();
    const id = stableId("observation", [kind, signal, pair]);
    if (seen.has(id)) return true;
    seen.add(id);
    if (observations.length === MAX_OBSERVATIONS) {
      limitReached = true;
      return false;
    }
    observations.push({
      id,
      kind,
      workstreamIds: [
        ...new Set([
          ...(membership.get(first.sessionKey) ?? []),
          ...(membership.get(second.sessionKey) ?? []),
        ]),
      ].sort(),
      sessionKeys: [first.sessionKey, second.sessionKey].sort(),
      description,
      evidenceIds: [first.id, second.id],
    });
    return true;
  };
  const proximityPairs = (
    events: ExportEvent[],
    visit: (a: ExportEvent, b: ExportEvent) => boolean,
  ): boolean => {
    const latestByRoot = new Map<string, ExportEvent>();
    for (const event of events) {
      const root = roots.get(event.sessionKey)!;
      const oldest = Date.parse(event.occurredAt) - PROXIMITY_MS;
      for (const [otherRoot, previous] of latestByRoot) {
        if (Date.parse(previous.occurredAt) < oldest)
          latestByRoot.delete(otherRoot);
        else if (otherRoot !== root && !visit(previous, event)) return false;
      }
      latestByRoot.set(root, event);
    }
    return true;
  };
  const surfaces = new Map<
    string,
    { project: string; file: string; events: ExportEvent[] }
  >();
  for (const event of windowEvents) {
    const p = project(event.sessionKey);
    if (p === null) continue;
    for (const file of event.filesTouched) {
      const key = JSON.stringify([p, file]);
      const surface = surfaces.get(key) ?? { project: p, file, events: [] };
      surface.events.push(event);
      surfaces.set(key, surface);
    }
  }
  for (const [, surface] of [...surfaces].sort(([a], [b]) => compare(a, b))) {
    if (
      !proximityPairs(surface.events, (a, b) =>
        observe(
          "shared_surface",
          [surface.project, surface.file],
          a,
          b,
          `Two distinct declared session roots reference ${surface.file} in ${surface.project} within 60 minutes. Independence is unverified where relationship metadata is incomplete. Review whether the work was complementary, intentionally duplicated, or conflicting. File references alone do not establish edits, concurrent execution, or rework.`,
        ),
      )
    )
      break;
  }
  if (!limitReached)
    for (const group of orderedGroups.filter((g) => g.grouping === "task")) {
      if (
        !proximityPairs(group.events, (a, b) =>
          observe(
            "shared_task",
            [group.id],
            a,
            b,
            `Two distinct declared session roots have events within 60 minutes under the explicit task reference ${group.taskRef}. Independence is unverified where relationship metadata is incomplete. Review ownership and handoffs; metadata and timing alone do not establish a missed connection.`,
          ),
        )
      )
        break;
    }

  const assessment = parseAssessment({
    schemaVersion: 1,
    synthetic: exported.synthetic,
    scope: exported.scope,
    window: exported.window,
    coverage: {
      ...exported.coverage,
      notes: `${exported.coverage.notes}${limitReached ? " Structural observation generation reached its 200-item limit; this is not an exhaustive overlap inventory." : ""}`,
      baselineSelection:
        "Census of exported metadata groups with evidence in the reporting window, selected before overlap analysis. Task references group within a project; missing task references fall back to project, then declared session root. Groups and outcomes remain unreviewed.",
    },
    provenance: exported.provenance,
    sessions: exported.sessions.map(
      ({
        key,
        organizationId,
        workspaceId,
        source,
        sessionId,
        title,
        projectKey,
        taskRefs,
        parentSessionKey,
      }) => ({
        key,
        organizationId,
        workspaceId,
        source,
        sessionId,
        context: { title, projectKey, taskRefs, parentSessionKey },
      }),
    ),
    evidence: exported.events,
    workstreams,
    findings: [],
    observations,
  });
  const sourceNames = [
    ...new Set(exported.sessions.map((s) => s.source)),
  ].sort();
  return {
    assessment,
    inventory: {
      intakeVersion: INTAKE_VERSION,
      sessions: exported.sessions.length,
      sessionsWithWindowEvidence: activeSessions.size,
      sessionsWithoutWindowEvidence: exported.sessions
        .filter((s) => !activeSessions.has(s.key))
        .map((s) => s.key),
      declaredSessionRoots: new Set(
        [...activeSessions].map((s) => roots.get(s)),
      ).size,
      events: exported.events.length,
      windowEvents: windowEvents.length,
      truncatedEvents: exported.events.filter((e) => e.truncated).length,
      unknownAvailabilityEvents: exported.events.filter(
        (e) => e.availableAt === null,
      ).length,
      sources: sourceNames.map((source) => ({
        source,
        sessions: exported.sessions.filter((s) => s.source === source).length,
        events: exported.events.filter(
          (e) => sessions.get(e.sessionKey)!.source === source,
        ).length,
      })),
      workstreams: orderedGroups.map(
        ({ id, grouping, projectKey, taskRef }) => ({
          id,
          grouping,
          projectKey,
          taskRef,
        }),
      ),
      observations: {
        count: observations.length,
        limit: MAX_OBSERVATIONS,
        limitReached,
        proximityMinutes: PROXIMITY_MS / 60000,
      },
      limitations: [
        "Grouping uses explicit exported metadata, not semantic understanding. A project fallback may contain multiple workstreams; a session with multiple task references may appear in multiple groups.",
        "All workstreams are pending review and have unknown progress. No semantic findings, verified outcomes, savings, or causal claims are generated.",
        "File references and events within an hour are structural review signals, not proof of conflict or simultaneous execution. Delegated sessions under the same declared parent root do not create overlap observations with one another.",
        "This inventory covers the supplied export only. Authorization, complete pagination, tombstones, redaction and historical source availability must be established by the upstream reader.",
      ],
    },
  };
}
