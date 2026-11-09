import {
  EVIDENCE_KINDS,
  validateEvidenceBundle,
  type EvidenceKind,
  type SessionEvidenceReference,
  type SessionAnalysisMetrics,
  type SessionSemanticInput,
} from "./analysis-contract.ts";
import type {
  CollectedEvidence,
  NormalizedEvent,
  NormalizedHydration,
  NormalizedToolCall,
} from "../../session-brief/engine/normalized-history.ts";
import { eventEvidenceId, toolEvidenceId } from "../../session-brief/engine/normalized-history.ts";

// Leave room for the request envelope below the API's 512 KiB limit.
const MAXIMUM_INPUT_BYTES = 480 * 1024;
const MAXIMUM_REFERENCES = 400;
const MAXIMUM_PER_KIND = 100;

export function buildSemanticInput(
  source: string,
  rootSessionId: string,
  evidence: CollectedEvidence,
  hydration: NormalizedHydration,
  metrics: SessionAnalysisMetrics,
): SessionSemanticInput {
  const candidates = collectReferences(rootSessionId, evidence);
  // Round-robin across kinds keeps large tool logs from starving human requests or results.
  const groups = EVIDENCE_KINDS.map((kind) =>
    sample(
      candidates.filter((reference) => reference.kind === kind),
      MAXIMUM_PER_KIND,
    ),
  );
  const references: SessionEvidenceReference[] = [];
  for (
    let index = 0;
    index < MAXIMUM_PER_KIND && references.length < MAXIMUM_REFERENCES;
    index++
  ) {
    for (const group of groups)
      if (group[index] && references.length < MAXIMUM_REFERENCES)
        references.push(group[index]!);
  }
  const boundedMetrics = {
    ...metrics,
    models: metrics.models
      .slice(0, 32)
      .map((model) => excerpt(model, 128).text),
    tools: metrics.tools.slice(0, 100).map((tool) => ({
      ...tool,
      name: excerpt(tool.name, 256).text,
      targets: tool.targets.map((target) => excerpt(target, 1_024).text),
    })),
    filesChanged: metrics.filesChanged
      .slice(0, 500)
      .map((file) => ({ ...file, path: excerpt(file.path, 2_048).text })),
    interactionPeriods: sample(metrics.interactionPeriods, 100),
  };
  const input: SessionSemanticInput = {
    source,
    sessionId: rootSessionId,
    capability: hydration.capability,
    references,
    metrics: boundedMetrics,
    coverage: {
      byKind: [],
      metricsOmitted: {
        models: 0,
        tools: 0,
        filesChanged: 0,
        interactionPeriods: 0,
      },
      metricsShortened: 0,
      treeTruncated: evidence.tree.truncated,
      unlinkedDelegationCount: evidence.tree.unlinked.length,
      hydrationStatus: hydration.status,
      diagnosticCount:
        hydration.diagnostics.length + evidence.tree.diagnostics.length,
      sourceStamp: hydration.indexedThrough.sourceStamp,
      indexedThroughMs: hydration.indexedThrough.lastEventAtMs,
    },
  };
  const updateCoverage = () => {
    const includedIds = new Set(
      input.references.map((reference) => reference.id),
    );
    for (const reference of input.references)
      if (
        reference.toolCallId !== null &&
        !includedIds.has(reference.toolCallId)
      )
        reference.toolCallId = null;
    input.coverage.byKind = EVIDENCE_KINDS.map((kind) => {
      const included = input.references.filter(
        (reference) => reference.kind === kind,
      );
      return {
        kind,
        available: candidates.filter((reference) => reference.kind === kind)
          .length,
        included: included.length,
        truncated: included.filter((reference) => reference.truncated).length,
      };
    });
    for (const key of [
      "models",
      "tools",
      "filesChanged",
      "interactionPeriods",
    ] as const)
      input.coverage.metricsOmitted[key] =
        metrics[key].length - boundedMetrics[key].length;
    input.coverage.metricsShortened =
      boundedMetrics.models.filter(
        (model, index) => model !== metrics.models[index],
      ).length +
      boundedMetrics.tools.filter(
        (tool, index) =>
          JSON.stringify(tool) !== JSON.stringify(metrics.tools[index]),
      ).length +
      boundedMetrics.filesChanged.filter(
        (file, index) => file.path !== metrics.filesChanged[index]?.path,
      ).length;
  };
  updateCoverage();
  // Account for UTF-8, escaping and metric size, not just character counts.
  while (byteLength(input) > MAXIMUM_INPUT_BYTES) {
    const arrays = [
      boundedMetrics.filesChanged,
      boundedMetrics.tools,
      boundedMetrics.interactionPeriods,
      boundedMetrics.models,
    ];
    const largest = arrays.reduce((a, b) =>
      byteLength(a) >= byteLength(b) ? a : b,
    );
    if (byteLength(largest) > 32 * 1024) largest.pop();
    else if (input.references.length > 0) {
      // Remove from the most represented kind while keeping its first and latest item when possible.
      const group = EVIDENCE_KINDS.map((kind) =>
        input.references.filter((reference) => reference.kind === kind),
      ).sort((a, b) => b.length - a.length)[0]!;
      const remove = group[Math.floor(group.length / 2)]!;
      input.references.splice(input.references.indexOf(remove), 1);
    } else throw new Error("Session metadata exceeds the analysis input limit");
    updateCoverage();
  }
  validateEvidenceBundle(
    { references: input.references, coverage: input.coverage },
    source,
    rootSessionId,
  );
  return input;
}

function collectReferences(
  rootSessionId: string,
  evidence: CollectedEvidence,
): SessionEvidenceReference[] {
  const references: SessionEvidenceReference[] = [];
  for (const event of evidence.events) {
    if (!event.text?.trim()) continue;
    let kind: EvidenceKind;
    if (event.kind === "tool_result" && event.role === "tool_result")
      kind = "tool-result";
    else if (event.kind === "text" && event.role === "user")
      kind =
        event.sessionId === rootSessionId
          ? "human-message"
          : "delegated-instruction";
    else if (event.kind === "text" && event.role === "assistant")
      kind = "assistant-message";
    else continue;
    const summary = excerpt(event.text, 1_200);
    const detail = excerpt(event.text, 4_000);
    const tool =
      kind === "tool-result" ? linkedTool(event, evidence.tools) : undefined;
    references.push({
      id: eventId(event),
      source: event.source,
      sessionId: event.sessionId,
      kind,
      at: iso(event.tsMs),
      summary: summary.text,
      detail: detail.text,
      truncated: detail.truncated,
      isError: tool?.isError ?? null,
      toolCallId: tool ? toolId(tool) : null,
      locator: {
        eventUid: event.eventUid,
        toolUseId: tool?.toolUseId ?? null,
        fileEditId: null,
      },
    });
  }
  for (const tool of evidence.tools) {
    const summary = excerpt(
      `${tool.name}${tool.target ? `: ${tool.target}` : ""}`,
      1_200,
    );
    const args = allowedArguments(tool.args);
    const detail = args === null ? null : excerpt(args, 4_000);
    references.push({
      id: toolId(tool),
      source: tool.source,
      sessionId: tool.sessionId,
      kind: "tool-call",
      at: iso(tool.tsMs),
      summary: summary.text,
      detail: detail?.text ?? null,
      truncated: summary.truncated || Boolean(detail?.truncated),
      isError: tool.isError,
      toolCallId: null,
      locator: { eventUid: null, toolUseId: tool.toolUseId, fileEditId: null },
    });
  }
  for (const edit of evidence.edits) {
    const summary = excerpt(
      `${edit.toolName ?? "edit"}: ${edit.filePath}`,
      1_200,
    );
    const patch =
      edit.structuredPatch === null
        ? edit.structuredPatchJson
        : JSON.stringify(edit.structuredPatch);
    const detail = patch?.trim() ? excerpt(patch, 4_000) : null;
    const tool = evidence.tools.find((tool) =>
      edit.toolEvidenceId !== undefined
        ? edit.toolEvidenceId !== null && toolId(tool) === edit.toolEvidenceId
        : tool.source === edit.source &&
          tool.sessionId === edit.sessionId &&
          tool.toolUseId === edit.toolUseId,
    );
    references.push({
      id: `edit:${edit.source}:${edit.sessionId}:${edit.id}`,
      source: edit.source,
      sessionId: edit.sessionId,
      kind: "file-edit",
      at: iso(edit.tsMs),
      summary: summary.text,
      detail: detail?.text ?? null,
      truncated: summary.truncated || Boolean(detail?.truncated),
      isError: null,
      toolCallId: tool ? toolId(tool) : null,
      locator: {
        eventUid: null,
        toolUseId: edit.toolUseId,
        fileEditId: edit.id,
      },
    });
  }
  for (const node of evidence.tree.nodes.slice(1)) {
    if (!node.relationship) continue;
    const summary = excerpt(
      `Delegated to ${node.relationship.childAgentName ?? node.relationship.childAgentType ?? node.sessionId}`,
      1_200,
    );
    references.push({
      id: `relationship:${node.source}:${node.relationship.relationshipUid}`,
      source: node.source,
      sessionId: node.sessionId,
      kind: "delegation",
      at: iso(node.relationship.spawnedAtMs),
      summary: summary.text,
      detail: null,
      truncated: summary.truncated,
      isError: null,
      toolCallId: null,
      locator: { eventUid: null, toolUseId: null, fileEditId: null },
    });
  }
  return references;
}

function linkedTool(
  event: NormalizedEvent,
  tools: NormalizedToolCall[],
): NormalizedToolCall | undefined {
  // The normalized event contract has no dedicated tool-use ID. Never infer a link from proximity.
  const matches = tools.filter(
    (tool) =>
      tool.source === event.source &&
      tool.sessionId === event.sessionId &&
      event.parentId === tool.toolUseId,
  );
  return matches.length === 1 ? matches[0] : undefined;
}

function allowedArguments(args: NormalizedToolCall["args"]): string | null {
  if (!args || typeof args !== "object" || Array.isArray(args)) return null;
  const entries = Object.entries(args).filter(
    ([key, value]) =>
      [
        "cmd",
        "command",
        "path",
        "file_path",
        "description",
        "pattern",
        "query",
      ].includes(key) &&
      typeof value === "string" &&
      value.trim(),
  );
  return entries.length
    ? JSON.stringify(Object.fromEntries(entries), null, 2)
    : null;
}

function excerpt(
  text: string,
  maximum: number,
): { text: string; truncated: boolean } {
  if (text.length <= maximum) return { text, truncated: false };
  const marker = "\n[… omitted …]\n";
  const head = Math.ceil((maximum - marker.length) / 2);
  return {
    text: `${text.slice(0, head)}${marker}${text.slice(-(maximum - head - marker.length))}`,
    truncated: true,
  };
}
function sample<T>(values: readonly T[], maximum: number): T[] {
  if (values.length <= maximum) return [...values];
  const head = Math.ceil(maximum / 3);
  return [...values.slice(0, head), ...values.slice(-(maximum - head))];
}
function byteLength(value: unknown): number {
  return Buffer.byteLength(JSON.stringify(value), "utf8");
}
function iso(value: number | null): string | null {
  return value === null ? null : new Date(value).toISOString();
}
function eventId(event: NormalizedEvent): string {
  return eventEvidenceId(event);
}
function toolId(tool: NormalizedToolCall): string {
  return toolEvidenceId(tool);
}
