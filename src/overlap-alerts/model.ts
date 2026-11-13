import { type Judge, record, sessionKey, hash } from "./contracts.ts";
import { JUDGE_INSTRUCTIONS, validateVerdict } from "./judgment.ts";
import { boundedJson, endpoint } from "./http.ts";

/** Explicit destination only. No hosted model or Cloud endpoint fallback. */
export function modelJudge(config: { url: string; model: string; apiKey: string; fetch?: typeof fetch }): Judge {
  const url = endpoint("OVERLAP_MODEL_URL", config.url);
  if (!config.model.trim() || !config.apiKey.trim()) throw new Error("model_not_configured");
  return {
    contextLimits: { eventsPerSession: 12, charsPerEvent: 2000 },
    version: `semantic/1/${config.model}/${hash(JUDGE_INSTRUCTIONS).slice(0, 12)}`,
    async assess(candidate) {
      const sessions = [candidate.left, candidate.right].map(s => ({
        sessionKey: sessionKey(s), repository: s.repository, complete: s.complete,
        omittedEvidence: Math.max(0, s.evidence.length - 12),
        evidence: [...s.evidence].sort((a, b) => Date.parse(a.at) - Date.parse(b.at)).slice(-12).map(e => ({ ...e, text: e.text.slice(0, 2000), truncated: e.text.length > 2000 })),
      }));
      let response: Response;
      try {
        response = await (config.fetch ?? fetch)(url, { method: "POST", redirect: "error", signal: AbortSignal.timeout(30_000),
          headers: { Authorization: `Bearer ${config.apiKey}`, "Content-Type": "application/json" },
          body: JSON.stringify({ model: config.model, store: false, max_output_tokens: 1800,
            instructions: JUDGE_INSTRUCTIONS, input: JSON.stringify({ asOf: candidate.asOf ?? null, signals: candidate.signals, sessions }), text: { format: { type: "json_object" } } }),
        });
      } catch { throw new Error("model_request_failed"); }
      const payload = await boundedJson(response, 128_000);
      if (!record(payload) || (payload.status !== undefined && payload.status !== "completed")) throw new Error("model_incomplete");
      let text = typeof payload.output_text === "string" ? payload.output_text : "";
      if (!text && Array.isArray(payload.output)) for (const item of payload.output) {
        if (!record(item) || !Array.isArray(item.content)) continue;
        for (const part of item.content) if (record(part) && part.type === "output_text" && typeof part.text === "string") text += part.text;
      }
      return validateVerdict(JSON.parse(text), candidate);
    },
  };
}
