import {
  SESSION_ANALYSIS_TITLE_SCHEMA,
  SESSION_ANALYSIS_TIMEOUT_MS,
  validateAnalysisTitle,
  validateSemanticRequest,
  type SessionSemanticInput,
  type SessionSemanticAnalysis,
} from "./analysis-contract.ts";
import { prepareAnalysisEvidence } from "./analysis-evidence.ts";
import { ANALYSIS_INSTRUCTIONS } from "./analysis-prompt.ts";
import {
  SESSION_SUMMARY_JSON_SCHEMA,
  SESSION_SUMMARY_TIMEOUT_MS,
  validateSummaryInput,
  validateSummaryText,
  type SessionSummaryInput,
  type SessionSummaryText,
} from "../../session-brief/engine/summary-contract.ts";
import { SUMMARY_INSTRUCTIONS } from "../../session-brief/engine/summary-prompt.ts";

/** Skip API session-analysis.ts, commit 07a16203, prompt/contract v2.4. */
export const ANALYZER_VERSION = "session-analysis-v2.4";

export interface ModelTransportRequest {
  model: string;
  instructions: string;
  input: string;
  schemaName: "session_analysis" | "session_summary";
  schema: Record<string, unknown>;
  timeoutMs: number;
  /** Summary model parameters; absent for full analysis. */
  store?: false;
  maxOutputTokens?: 256;
  reasoningEffort?: "none";
}
export interface ModelTransportResponse {
  outputText: string;
  inputTokens: number;
  outputTokens: number;
  providerRequestId: string | null;
  model?: string;
}
export interface AnalysisModelTransport {
  complete(request: ModelTransportRequest): Promise<ModelTransportResponse>;
}

export interface SessionAnalysisModelResult {
  title: string;
  description: string;
  analyzerVersion: typeof ANALYZER_VERSION;
  analysis: SessionSemanticAnalysis;
  metering: {
    inputTokens: number;
    outputTokens: number;
    totalTokens: number;
    model: string;
    providerRequestId: string | null;
  };
}

export async function analyzeSessionInput(
  input: SessionSemanticInput,
  transport: AnalysisModelTransport,
  model: string,
): Promise<SessionAnalysisModelResult> {
  validateSemanticRequest(input);
  const evidence = prepareAnalysisEvidence(input);
  const modelSchema = {
    ...evidence.schema,
    properties: {
      title: SESSION_ANALYSIS_TITLE_SCHEMA,
      ...(evidence.schema.properties as Record<string, unknown>),
    },
    required: ["title", ...(evidence.schema.required as string[])],
  };
  const result = await transport.complete({
    model,
    instructions: ANALYSIS_INSTRUCTIONS,
    input: JSON.stringify(evidence.input),
    schemaName: "session_analysis",
    schema: modelSchema,
    timeoutMs: SESSION_ANALYSIS_TIMEOUT_MS,
  });
  let candidate: unknown;
  try {
    candidate = JSON.parse(result.outputText);
  } catch {
    throw new TypeError("model output is not valid JSON");
  }
  if (!candidate || typeof candidate !== "object" || Array.isArray(candidate))
    throw new TypeError("model analysis must be an object");
  const { title, ...semantic } = candidate as Record<string, unknown>;
  let analysis: SessionSemanticAnalysis;
  try {
    analysis = evidence.decode(semantic);
  } catch {
    throw new TypeError("model analysis has an invalid shape or citation");
  }
  return {
    title: validateAnalysisTitle(title),
    description: analysis.description.text,
    analyzerVersion: ANALYZER_VERSION,
    analysis,
    metering: {
      inputTokens: result.inputTokens,
      outputTokens: result.outputTokens,
      totalTokens: result.inputTokens + result.outputTokens,
      model: result.model ?? model,
      providerRequestId: result.providerRequestId,
    },
  };
}

export async function summarizeSessionInput(
  input: SessionSummaryInput,
  transport: AnalysisModelTransport,
  model: string,
): Promise<{
  summary: SessionSummaryText;
  model: string;
  inputTokens: number;
  outputTokens: number;
}> {
  validateSummaryInput(input);
  const result = await transport.complete({
    model,
    instructions: SUMMARY_INSTRUCTIONS,
    input: JSON.stringify(input),
    schemaName: "session_summary",
    schema: SESSION_SUMMARY_JSON_SCHEMA as unknown as Record<string, unknown>,
    timeoutMs: SESSION_SUMMARY_TIMEOUT_MS,
    store: false,
    maxOutputTokens: 256,
    reasoningEffort: "none",
  });
  let summary: SessionSummaryText;
  try {
    summary = validateSummaryText(JSON.parse(result.outputText));
  } catch {
    throw new TypeError("model summary has an invalid shape");
  }
  return {
    summary,
    model: result.model ?? model,
    inputTokens: result.inputTokens,
    outputTokens: result.outputTokens,
  };
}
