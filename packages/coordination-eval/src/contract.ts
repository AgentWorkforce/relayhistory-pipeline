import { scrubText } from "../../pipeline/src/lib/scrub.ts";

export const KINDS = [
  "repeated_investigation",
  "conflicting_decisions",
  "missed_handoff",
  "human_context_transfer",
  "successful_coordination",
] as const;
export type Kind = (typeof KINDS)[number];
export type Verdict = "pending" | "confirmed" | "rejected" | "uncertain";
export const INTERVENTIONS = [
  "live_communication",
  "shared_history",
  "human_visibility",
  "unknown",
] as const;
export type Intervention = (typeof INTERVENTIONS)[number];

export interface Observation {
  id: string;
  kind: "shared_surface" | "concurrent_activity" | "shared_task";
  workstreamIds: string[];
  sessionKeys: string[];
  description: string;
  evidenceIds: string[];
}

export interface Session {
  key: string;
  organizationId: string;
  workspaceId: string;
  source: string;
  sessionId: string;
  context?: {
    title: string;
    projectKey: string | null;
    taskRefs: string[];
    parentSessionKey: string | null;
  } | null;
}

export interface Evidence {
  id: string;
  sessionKey: string;
  machineId: string;
  eventId: string;
  kind: string;
  occurredAt: string;
  availableAt: string | null;
  text: string;
  truncated: boolean;
  filesTouched?: string[];
}

export interface Workstream {
  id: string;
  title: string;
  selection: "baseline" | "targeted";
  sessionKeys: string[];
  state: "active" | "blocked" | "done" | "unknown";
  summary: string;
  summaryEvidenceIds: string[];
  assessment: "pending" | "reviewed";
}

export interface Finding {
  id: string;
  incidentId: string;
  workstreamId: string;
  kind: Kind;
  description: string;
  checkpoint: string;
  factEvidenceIds: string[];
  actionEvidenceIds: string[];
  outcomeEvidenceIds: string[];
  proposedMessage: string | null;
  intervention: Intervention;
  communication: "observed" | "not_observed" | "unknown";
  review: { verdict: Verdict; reviewer: string | null; reason: string };
}

export interface Assessment {
  schemaVersion: 1;
  synthetic: boolean;
  scope: { organizationId: string; workspaceId: string };
  window: { since: string; until: string };
  coverage: {
    status: "complete" | "partial" | "unknown";
    notes: string;
    baselineSelection: string;
  };
  provenance: { source: string; exportedAt: string; redacted: true };
  sessions: Session[];
  evidence: Evidence[];
  workstreams: Workstream[];
  findings: Finding[];
  observations: Observation[];
}

function fail(path: string, message: string): never {
  // Paths are fixed schema labels, never input values or parser messages.
  throw new Error(`${path}: ${message}`);
}

function object(
  value: unknown,
  path: string,
  keys: string[],
): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value))
    fail(path, "expected object");
  const result = value as Record<string, unknown>;
  if (Object.keys(result).some((key) => !keys.includes(key)))
    fail(path, "unknown field");
  return result;
}

function string(value: unknown, path: string): string {
  if (
    typeof value !== "string" ||
    !value.trim() ||
    value.length > 16000 ||
    /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/.test(value)
  )
    fail(path, "expected bounded nonempty text");
  if (scrubText(value) !== value) fail(path, "requires upstream redaction");
  return value;
}

function id(value: unknown, path: string): string {
  const result = string(value, path);
  if (result.length > 256 || !/^[a-zA-Z0-9][a-zA-Z0-9_.:/@-]*$/.test(result))
    fail(path, "invalid identifier");
  return result;
}

function choice<T extends string>(
  value: unknown,
  path: string,
  choices: readonly T[],
): T {
  if (typeof value !== "string" || !choices.includes(value as T))
    fail(path, "invalid choice");
  return value as T;
}

function boolean(value: unknown, path: string): boolean {
  if (typeof value !== "boolean") fail(path, "expected boolean");
  return value;
}

function timestamp(value: unknown, path: string): string {
  const result = string(value, path);
  if (
    !/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/.test(result) ||
    !Number.isFinite(Date.parse(result)) ||
    new Date(result).toISOString() !== result
  )
    fail(path, "expected UTC ISO timestamp with milliseconds");
  return result;
}

function list<T>(
  value: unknown,
  path: string,
  parse: (item: unknown) => T,
): T[] {
  if (!Array.isArray(value) || value.length > 10000)
    fail(path, "expected array with at most 10000 entries");
  return value.map(parse);
}

function ids(value: unknown, path: string): string[] {
  const result = list(value, path, (item) => id(item, path));
  if (new Set(result).size !== result.length) fail(path, "duplicate reference");
  return result;
}

function unique<T>(
  rows: T[],
  key: (row: T) => string,
  path: string,
): Map<string, T> {
  const result = new Map<string, T>();
  for (const row of rows) {
    const k = key(row);
    if (result.has(k)) fail(path, "duplicate identity");
    result.set(k, row);
  }
  return result;
}

export function parseAssessment(value: unknown): Assessment {
  const a = object(value, "assessment", [
    "schemaVersion",
    "synthetic",
    "scope",
    "window",
    "coverage",
    "provenance",
    "sessions",
    "evidence",
    "workstreams",
    "findings",
    "observations",
  ]);
  if (a.schemaVersion !== 1) fail("schemaVersion", "unsupported schema");
  const scope = object(a.scope, "scope", ["organizationId", "workspaceId"]);
  const window = object(a.window, "window", ["since", "until"]);
  const coverage = object(a.coverage, "coverage", [
    "status",
    "notes",
    "baselineSelection",
  ]);
  const provenance = object(a.provenance, "provenance", [
    "source",
    "exportedAt",
    "redacted",
  ]);
  if (provenance.redacted !== true)
    fail("provenance", "requires a redacted export");
  const result: Assessment = {
    schemaVersion: 1,
    synthetic: boolean(a.synthetic, "synthetic"),
    scope: {
      organizationId: id(scope.organizationId, "scope.organizationId"),
      workspaceId: id(scope.workspaceId, "scope.workspaceId"),
    },
    window: {
      since: timestamp(window.since, "window.since"),
      until: timestamp(window.until, "window.until"),
    },
    coverage: {
      status: choice(coverage.status, "coverage.status", [
        "complete",
        "partial",
        "unknown",
      ]),
      notes: string(coverage.notes, "coverage.notes"),
      baselineSelection: string(
        coverage.baselineSelection,
        "coverage.baselineSelection",
      ),
    },
    provenance: {
      source: string(provenance.source, "provenance.source"),
      exportedAt: timestamp(provenance.exportedAt, "provenance.exportedAt"),
      redacted: true,
    },
    sessions: list(a.sessions, "sessions", (item) => {
      const s = object(item, "session", [
        "key",
        "organizationId",
        "workspaceId",
        "source",
        "sessionId",
        "context",
      ]);
      const context =
        s.context == null
          ? null
          : object(s.context, "session.context", [
              "title",
              "projectKey",
              "taskRefs",
              "parentSessionKey",
            ]);
      return {
        key: id(s.key, "session.key"),
        organizationId: id(s.organizationId, "session.organizationId"),
        workspaceId: id(s.workspaceId, "session.workspaceId"),
        source: id(s.source, "session.source"),
        sessionId: id(s.sessionId, "session.sessionId"),
        context:
          context === null
            ? null
            : {
                title: string(context.title, "session.context.title"),
                projectKey:
                  context.projectKey === null
                    ? null
                    : string(context.projectKey, "session.context.projectKey"),
                taskRefs: list(
                  context.taskRefs,
                  "session.context.taskRefs",
                  (v) => string(v, "session.context.taskRefs"),
                ),
                parentSessionKey:
                  context.parentSessionKey === null
                    ? null
                    : id(
                        context.parentSessionKey,
                        "session.context.parentSessionKey",
                      ),
              },
      };
    }),
    evidence: list(a.evidence, "evidence", (item) => {
      const e = object(item, "evidence", [
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
      ]);
      return {
        id: id(e.id, "evidence.id"),
        sessionKey: id(e.sessionKey, "evidence.sessionKey"),
        machineId: id(e.machineId, "evidence.machineId"),
        eventId: id(e.eventId, "evidence.eventId"),
        kind: id(e.kind, "evidence.kind"),
        occurredAt: timestamp(e.occurredAt, "evidence.occurredAt"),
        availableAt:
          e.availableAt === null
            ? null
            : timestamp(e.availableAt, "evidence.availableAt"),
        text: string(e.text, "evidence.text"),
        truncated: boolean(e.truncated, "evidence.truncated"),
        filesTouched: list(e.filesTouched ?? [], "evidence.filesTouched", (v) =>
          string(v, "evidence.filesTouched"),
        ),
      };
    }),
    workstreams: list(a.workstreams, "workstreams", (item) => {
      const w = object(item, "workstream", [
        "id",
        "title",
        "selection",
        "sessionKeys",
        "state",
        "summary",
        "summaryEvidenceIds",
        "assessment",
      ]);
      return {
        id: id(w.id, "workstream.id"),
        title: string(w.title, "workstream.title"),
        selection: choice(w.selection, "workstream.selection", [
          "baseline",
          "targeted",
        ]),
        sessionKeys: ids(w.sessionKeys, "workstream.sessionKeys"),
        state: choice(w.state, "workstream.state", [
          "active",
          "blocked",
          "done",
          "unknown",
        ]),
        summary: string(w.summary, "workstream.summary"),
        summaryEvidenceIds: ids(
          w.summaryEvidenceIds,
          "workstream.summaryEvidenceIds",
        ),
        assessment: choice(w.assessment, "workstream.assessment", [
          "pending",
          "reviewed",
        ]),
      };
    }),
    observations: list(a.observations ?? [], "observations", (item) => {
      const o = object(item, "observation", [
        "id",
        "kind",
        "workstreamIds",
        "sessionKeys",
        "description",
        "evidenceIds",
      ]);
      return {
        id: id(o.id, "observation.id"),
        kind: choice(o.kind, "observation.kind", [
          "shared_surface",
          "concurrent_activity",
          "shared_task",
        ]),
        workstreamIds: ids(o.workstreamIds, "observation.workstreamIds"),
        sessionKeys: ids(o.sessionKeys, "observation.sessionKeys"),
        description: string(o.description, "observation.description"),
        evidenceIds: ids(o.evidenceIds, "observation.evidenceIds"),
      };
    }),
    findings: list(a.findings, "findings", (item) => {
      const f = object(item, "finding", [
        "id",
        "incidentId",
        "workstreamId",
        "kind",
        "description",
        "checkpoint",
        "factEvidenceIds",
        "actionEvidenceIds",
        "outcomeEvidenceIds",
        "proposedMessage",
        "intervention",
        "communication",
        "review",
      ]);
      const r = object(f.review, "finding.review", [
        "verdict",
        "reviewer",
        "reason",
      ]);
      return {
        id: id(f.id, "finding.id"),
        incidentId: id(f.incidentId, "finding.incidentId"),
        workstreamId: id(f.workstreamId, "finding.workstreamId"),
        kind: choice(f.kind, "finding.kind", KINDS),
        description: string(f.description, "finding.description"),
        checkpoint: timestamp(f.checkpoint, "finding.checkpoint"),
        factEvidenceIds: ids(f.factEvidenceIds, "finding.factEvidenceIds"),
        actionEvidenceIds: ids(
          f.actionEvidenceIds,
          "finding.actionEvidenceIds",
        ),
        outcomeEvidenceIds: ids(
          f.outcomeEvidenceIds,
          "finding.outcomeEvidenceIds",
        ),
        proposedMessage:
          f.proposedMessage === null
            ? null
            : string(f.proposedMessage, "finding.proposedMessage"),
        intervention: choice(
          f.intervention ?? "unknown",
          "finding.intervention",
          INTERVENTIONS,
        ),
        communication: choice(f.communication, "finding.communication", [
          "observed",
          "not_observed",
          "unknown",
        ]),
        review: {
          verdict: choice(r.verdict, "finding.review.verdict", [
            "pending",
            "confirmed",
            "rejected",
            "uncertain",
          ]),
          reviewer:
            r.reviewer === null
              ? null
              : string(r.reviewer, "finding.review.reviewer"),
          reason: string(r.reason, "finding.review.reason"),
        },
      };
    }),
  };
  validateReferences(result);
  return result;
}

function validateReferences(a: Assessment): void {
  if (a.window.since >= a.window.until)
    fail("window", "since must precede until");
  if (a.provenance.exportedAt < a.window.until)
    fail("provenance", "export must be at or after the reporting cutoff");
  const sessions = unique(a.sessions, (s) => s.key, "sessions");
  unique(
    a.sessions,
    (s) => JSON.stringify([s.source, s.sessionId]),
    "sessions",
  );
  for (const s of a.sessions) {
    if (
      s.organizationId !== a.scope.organizationId ||
      s.workspaceId !== a.scope.workspaceId
    )
      fail("sessions", "cross-scope session");
    const seen = new Set<string>();
    let current = s;
    while (current.context?.parentSessionKey) {
      if (seen.has(current.key))
        fail("sessions", "cyclic context parent reference");
      seen.add(current.key);
      const parent = sessions.get(current.context.parentSessionKey);
      if (!parent) fail("sessions", "unknown context parent session");
      current = parent;
    }
  }
  const evidence = unique(a.evidence, (e) => e.id, "evidence");
  unique(
    a.evidence,
    (e) => JSON.stringify([e.sessionKey, e.machineId, e.kind, e.eventId]),
    "evidence",
  );
  for (const e of a.evidence) {
    if (!sessions.has(e.sessionKey)) fail("evidence", "unknown session");
    if (e.occurredAt >= a.window.until)
      fail("evidence", "event is beyond the reporting cutoff");
    if (
      e.availableAt !== null &&
      (e.availableAt < e.occurredAt || e.availableAt > a.provenance.exportedAt)
    )
      fail("evidence", "invalid availability timestamp");
  }
  const workstreams = unique(a.workstreams, (w) => w.id, "workstreams");
  const resolve = (references: string[], w: Workstream): Evidence[] =>
    references.map((ref) => {
      const e = evidence.get(ref);
      if (!e || !w.sessionKeys.includes(e.sessionKey))
        fail("references", "evidence is missing or outside the workstream");
      return e;
    });
  for (const w of a.workstreams) {
    if (!w.sessionKeys.length || w.sessionKeys.some((s) => !sessions.has(s)))
      fail("workstreams", "requires known sessions");
    if (!w.summaryEvidenceIds.length)
      fail("workstreams", "summary requires evidence");
    resolve(w.summaryEvidenceIds, w);
  }
  unique(a.findings, (f) => f.id, "findings");
  unique(a.observations, (o) => o.id, "observations");
  for (const o of a.observations) {
    if (
      !o.workstreamIds.length ||
      o.sessionKeys.length < 2 ||
      !o.evidenceIds.length
    )
      fail(
        "observations",
        "requires workstreams, distinct sessions and evidence",
      );
    const members = new Set<string>();
    for (const wid of o.workstreamIds) {
      const w = workstreams.get(wid);
      if (!w) fail("observations", "unknown workstream");
      for (const key of w.sessionKeys) members.add(key);
      if (!w.sessionKeys.some((key) => o.sessionKeys.includes(key)))
        fail("observations", "unrelated workstream");
    }
    if (o.sessionKeys.some((key) => !members.has(key)))
      fail("observations", "session outside workstreams");
    const supported = new Set<string>();
    for (const eid of o.evidenceIds) {
      const e = evidence.get(eid);
      if (!e || !o.sessionKeys.includes(e.sessionKey))
        fail("observations", "evidence outside observed sessions");
      supported.add(e.sessionKey);
    }
    if (o.sessionKeys.some((key) => !supported.has(key)))
      fail("observations", "each observed session requires evidence");
  }
  unique(a.findings, (f) => f.incidentId, "findings.incidentId");
  for (const f of a.findings) {
    const w = workstreams.get(f.workstreamId);
    if (!w) fail("findings", "unknown workstream");
    if (f.checkpoint < a.window.since || f.checkpoint >= a.window.until)
      fail("findings", "checkpoint outside reporting window");
    const facts = resolve(f.factEvidenceIds, w);
    const actions = resolve(f.actionEvidenceIds, w);
    const outcomes = resolve(f.outcomeEvidenceIds, w);
    if (!facts.length || !actions.length || !outcomes.length)
      fail("findings", "requires fact, action and outcome evidence");
    if (facts.some((e) => e.occurredAt > f.checkpoint))
      fail("findings", "fact occurs after checkpoint");
    if ([...actions, ...outcomes].some((e) => e.occurredAt < f.checkpoint))
      fail("findings", "action or outcome precedes checkpoint");
    const lastAction = actions.reduce(
      (latest, e) => (e.occurredAt > latest ? e.occurredAt : latest),
      f.checkpoint,
    );
    if (outcomes.some((e) => e.occurredAt < lastAction))
      fail("findings", "outcome precedes cited action");
    if (f.review.verdict !== "pending" && !f.review.reviewer)
      fail("findings.review", "reviewed verdict requires reviewer");
    if (f.review.verdict === "confirmed") {
      if ([...facts, ...actions, ...outcomes].some((e) => e.truncated))
        fail("findings", "confirmed evidence cannot be truncated");
      if (
        !facts.some((fact) =>
          actions.some((action) => fact.sessionKey !== action.sessionKey),
        )
      )
        fail("findings", "confirmed connection requires distinct sessions");
      if (f.kind !== "successful_coordination" && !f.proposedMessage)
        fail("findings", "confirmed opportunity requires a proposed message");
      if (
        f.kind === "successful_coordination" &&
        f.communication !== "observed"
      )
        fail(
          "findings",
          "successful coordination requires observed communication",
        );
    }
  }
  for (const w of a.workstreams) {
    if (
      w.assessment === "reviewed" &&
      a.findings.some(
        (f) => f.workstreamId === w.id && f.review.verdict === "pending",
      )
    )
      fail("workstreams", "reviewed workstream has pending findings");
  }
}
