import {
  SESSION_ANALYSIS_JSON_SCHEMA,
  validateSemanticAnalysis,
  type SessionSemanticInput,
} from "./analysis-contract.ts";

/** Short IDs are private to one model call; persisted citations retain their original locators. */
export function prepareAnalysisEvidence(input: SessionSemanticInput) {
  const aliases = new Map(
    input.references.map((reference, index) => [reference.id, `e${index + 1}`]),
  );
  const originalIDs = new Map(
    [...aliases].map(([original, alias]) => [alias, original]),
  );
  const references = input.references.map((reference) => ({
    ...reference,
    id: aliases.get(reference.id)!,
    toolCallId:
      reference.toolCallId === null
        ? null
        : (aliases.get(reference.toolCallId) ?? null),
  }));
  const modelInput = {
    ...input,
    references,
    metrics: {
      ...input.metrics,
      interactionPeriods: input.metrics.interactionPeriods.map((period) => ({
        ...period,
        // Metrics can refer to events omitted by evidence sampling. Those are not citable.
        triggerEvidenceId: aliases.get(period.triggerEvidenceId) ?? null,
      })),
    },
  };
  // Define the enum once: repeating it for every claim exceeds provider schema limits.
  // With no evidence, maxItems: 0 permits only empty citation arrays without an empty enum.
  const schema: Record<string, unknown> = JSON.parse(
    JSON.stringify(SESSION_ANALYSIS_JSON_SCHEMA, (key, value: unknown) =>
      key === "evidenceIds"
        ? {
            type: "array",
            items: aliases.size
              ? { $ref: "#/$defs/evidenceId" }
              : { type: "string" },
            maxItems: aliases.size ? 100 : 0,
          }
        : value,
    ),
  );
  if (aliases.size)
    schema.$defs = {
      evidenceId: { type: "string", enum: [...aliases.values()] },
    };

  return {
    input: modelInput,
    schema,
    decode(candidate: unknown) {
      const analysis = validateSemanticAnalysis(candidate, references);
      const decoded: unknown = JSON.parse(
        JSON.stringify(analysis),
        (key, value: unknown) =>
          key === "evidenceIds" && Array.isArray(value)
            ? value.map((id: string) => originalIDs.get(id)!)
            : value,
      );
      return validateSemanticAnalysis(decoded, input.references);
    },
  };
}
