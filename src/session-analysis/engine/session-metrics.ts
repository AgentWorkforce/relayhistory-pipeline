// Ported from Skip session-analysis.ts at 07a16203ccc708fe26c0caca3c77cdde87d55b33.
import type {
  SessionAnalysisMetrics,
  SessionToolUsage,
  SessionFileChangeSummary,
  SessionUsageSummary,
  SessionInteractionPeriod,
} from "./analysis-contract.ts";
import type {
  CollectedEvidence,
  NormalizedEvent,
  NormalizedToolCall,
  NormalizedFileEdit,
} from "../../session-brief/engine/normalized-history.ts";
import { eventEvidenceId } from "../../session-brief/engine/normalized-history.ts";

export function sessionMetrics(
  rootSessionId: string,
  evidence: CollectedEvidence,
): SessionAnalysisMetrics {
  const usage = aggregateUsage(evidence.events);
  const tools = aggregateTools(evidence.tools);
  const filesChanged = aggregateFiles(evidence.edits);
  const interactionPeriods = buildInteractionPeriods(
    rootSessionId,
    evidence.events,
  );
  return {
    models: [
      ...new Set(
        evidence.events
          .map((event) => event.model)
          .filter((model): model is string => Boolean(model)),
      ),
    ].sort(),
    usage,
    tools,
    filesChanged,
    interactionPeriods,
    longestInteractionPeriodMs: interactionPeriods.length
      ? Math.max(...interactionPeriods.map((period) => period.durationMs))
      : null,
  };
}

function aggregateUsage(events: NormalizedEvent[]): SessionUsageSummary {
  const seen = new Set<string>();
  let inputTokens = 0;
  let outputTokens = 0;
  let cachedInputTokens = 0;
  let totalTokens = 0;
  let costUSD = 0;
  let observed = false;
  let observedCost = false;
  for (const event of events) {
    if (!event.tokenUsage) continue;
    const key =
      event.machineId && event.sourceKind
        ? JSON.stringify([
            event.source,
            event.sessionId,
            event.machineId,
            event.sourceKind,
            event.messageId ?? event.eventUid,
          ])
        : `${event.source}:${event.sessionId}:${event.messageId ?? event.eventUid}`;
    if (seen.has(key)) continue;
    seen.add(key);
    const input =
      numberAt(event.tokenUsage, "input_tokens", "inputTokens") ?? 0;
    const output =
      numberAt(event.tokenUsage, "output_tokens", "outputTokens") ?? 0;
    const cached =
      numberAt(
        event.tokenUsage,
        "cached_input_tokens",
        "cachedInputTokens",
        "cache_read_input_tokens",
      ) ?? 0;
    const total =
      numberAt(event.tokenUsage, "total_tokens", "totalTokens") ??
      input + output;
    const cost = numberAt(
      event.tokenUsage,
      "cost_usd",
      "costUSD",
      "total_cost_usd",
    );
    if (input || output || cached || total) observed = true;
    inputTokens += input;
    outputTokens += output;
    cachedInputTokens += cached;
    totalTokens += total;
    if (cost !== undefined) {
      observedCost = true;
      costUSD += cost;
    }
  }
  return {
    observed,
    inputTokens,
    outputTokens,
    cachedInputTokens,
    totalTokens,
    costUSD: observedCost ? costUSD : null,
  };
}

function aggregateTools(tools: NormalizedToolCall[]): SessionToolUsage[] {
  const groups = new Map<string, SessionToolUsage>();
  for (const tool of tools) {
    const group = groups.get(tool.name) ?? {
      name: tool.name,
      count: 0,
      errorCount: 0,
      targets: [],
    };
    group.count += 1;
    if (tool.isError === true) group.errorCount += 1;
    if (
      tool.target &&
      !group.targets.includes(tool.target) &&
      group.targets.length < 10
    )
      group.targets.push(tool.target);
    groups.set(tool.name, group);
  }
  return [...groups.values()].sort(
    (a, b) => b.count - a.count || a.name.localeCompare(b.name),
  );
}

function aggregateFiles(
  edits: NormalizedFileEdit[],
): SessionFileChangeSummary[] {
  const groups = new Map<string, SessionFileChangeSummary>();
  for (const edit of edits) {
    const group = groups.get(edit.filePath) ?? {
      path: edit.filePath,
      editCount: 0,
      linesAdded: null,
      linesRemoved: null,
      userModified: null,
    };
    group.editCount += 1;
    if (edit.linesAdded !== null)
      group.linesAdded = (group.linesAdded ?? 0) + edit.linesAdded;
    if (edit.linesRemoved !== null)
      group.linesRemoved = (group.linesRemoved ?? 0) + edit.linesRemoved;
    if (edit.userModified !== null)
      group.userModified = group.userModified === true || edit.userModified;
    groups.set(edit.filePath, group);
  }
  return [...groups.values()].sort(
    (a, b) => b.editCount - a.editCount || a.path.localeCompare(b.path),
  );
}

function buildInteractionPeriods(
  rootSessionId: string,
  events: NormalizedEvent[],
): SessionInteractionPeriod[] {
  const humanEvents = events.filter(
    (event) =>
      event.sessionId === rootSessionId &&
      event.role === "user" &&
      event.kind === "text" &&
      event.text,
  );
  const lastEventAt = events.at(-1)?.tsMs;
  return humanEvents.flatMap((event, index): SessionInteractionPeriod[] => {
    const end = humanEvents[index + 1]?.tsMs ?? lastEventAt;
    if (end === undefined || end < event.tsMs) return [];
    return [
      {
        startedAt: new Date(event.tsMs).toISOString(),
        endedAt: new Date(end).toISOString(),
        durationMs: end - event.tsMs,
        triggerEvidenceId: eventEvidenceId(event),
      },
    ];
  });
}

function numberAt(
  value: Record<string, unknown>,
  ...keys: string[]
): number | undefined {
  for (const key of keys) {
    const candidate = value[key];
    if (
      typeof candidate === "number" &&
      Number.isFinite(candidate) &&
      candidate >= 0
    )
      return candidate;
  }
  return undefined;
}
