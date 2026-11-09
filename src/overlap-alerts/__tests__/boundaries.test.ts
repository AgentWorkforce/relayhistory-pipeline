import { describe, expect, test } from "bun:test";
import { mkdtemp, stat, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import data from "../fixtures/synthetic.json";
import { parseDataset, KINDS, sessionKey } from "../contracts.ts";
import { modelJudge } from "../model.ts";
import { findCandidates } from "../candidates.ts";
import { fileBaseline } from "../judgment.ts";
import { importEvidence } from "../source.ts";
import { eraseRun, purgeExpiredRuns, readJson, requireUnexpired, writeRun } from "../artifacts.ts";
import { collectAlerts } from "../collector.ts";

const d = () => parseDataset(structuredClone(data));
const candidate = () => findCandidates(d().scope, d().sessions)[0]!;
const fetcher = (f: (url: string | URL | Request, init?: RequestInit) => Promise<Response>) => f as typeof fetch;
describe("semantic assessment boundary", () => {
  test("all six opportunity kinds work through the optional transport with checked evidence", async () => {
    const c = { ...candidate(), asOf: "2026-11-07T12:00:00Z" }, citations = (await fileBaseline.assess(c)).citations;
    for (const kind of KINDS) {
      const judge = modelJudge({ url: "https://models.example.test/v1/responses", apiKey: "fixture-only", model: "enterprise-model", fetch: fetcher(async (url, init) => {
        expect(String(url)).toBe("https://models.example.test/v1/responses");
        expect(init?.redirect).toBe("error");
        const body = JSON.parse(String(init?.body));
        expect(JSON.parse(body.input).asOf).toBe(c.asOf);
        expect(body.store).toBe(false); expect(body.instructions).toContain("untrusted evidence");
        return Response.json({ status: "completed", output: [{ content: [{ type: "output_text", text: JSON.stringify({ decision: "suggest", kind, confidence: "high", reason: "A possible coordination opportunity", citations }) }] }] });
      }) });
      expect((await judge.assess(c)).kind).toBe(kind);
    }
  });
  test("refuses incomplete, oversized and invented model output", async () => {
    for (const payload of [{ status: "incomplete", output_text: "{}" }, { output_text: "x".repeat(130000) }, { output_text: JSON.stringify({ decision: "suggest", kind: "dependency", confidence: "high", reason: "Invented", citations: [] }) }]) {
      const judge = modelJudge({ url: "https://models.example.test/v1/responses", apiKey: "fixture-only", model: "m", fetch: fetcher(async () => Response.json(payload)) });
      await expect(judge.assess(candidate())).rejects.toThrow();
    }
  });
  test("never falls back to a public model endpoint", () => {
    for (const url of ["", "http://models.example.test/v1/responses", "https://secret@example.test/v1/responses"])
      expect(() => modelJudge({ url, model: "m", apiKey: "fixture-only" })).toThrow();
  });
});
describe("authorized evidence intake", () => {
  const now = new Date("2026-11-08T00:00:00Z");
  const production = () => {
    const input = d(); input.provenance.kind = "production";
    input.provenance.capturedAt = now.toISOString();
    input.provenance.expiresAt = "2026-11-15T00:00:00Z";
    return input;
  };
  test("scrubs content without inventing arrival time or write operations", () => {
    const input = production(), e = input.sessions[0]!.evidence[0]!;
    e.text = 'password="my-super-secret-password"'; e.availableAt = null;
    e.files = [{ path: "src/a.ts", operation: "unknown" }];
    const output = importEvidence(input, now);
    expect(JSON.stringify(output)).not.toContain("my-super-secret-password");
    expect(output.sessions[0]!.evidence[0]!.availableAt).toBeNull();
    expect(output.sessions[0]!.evidence[0]!.files[0]!.operation).toBe("unknown");
  });
  test("refuses extra fields, future capture and excessive retention", () => {
    expect(() => importEvidence({ ...production(), token: "do-not-persist" }, now)).toThrow();
    const future = production(); future.provenance.capturedAt = "2026-11-09T00:00:00Z";
    expect(() => importEvidence(future, now)).toThrow("future_capture");
    const long = production(); long.provenance.expiresAt = "2027-01-01T00:00:00Z";
    expect(() => importEvidence(long, now)).toThrow();
  });
});
describe("private artifacts", () => {
  test("refuses expired production data, permits reproducible synthetic fixtures", () => {
    const dataset = d(); dataset.provenance.kind = "production"; dataset.provenance.expiresAt = "2026-11-09T00:00:00Z";
    expect(() => requireUnexpired(dataset, new Date("2026-11-10T00:00:00Z"))).toThrow("production_dataset_expired");
    dataset.provenance.kind = "synthetic"; expect(() => requireUnexpired(dataset, new Date("2026-11-10T00:00:00Z"))).not.toThrow();
  });
  test("writes new private runs, refuses overwrite, and erases complete derived runs", async () => {
    const root = await mkdtemp(join(tmpdir(), "overlap-test-")), path = join(root, "run");
    try {
      const dataset = d(); const meta = { expiresAt: dataset.provenance.expiresAt, sessionKeys: dataset.sessions.map(sessionKey) };
      await writeRun(path, { "dataset.json": dataset }, meta);
      expect((await stat(path)).mode & 0o777).toBe(0o700);
      expect((await stat(join(path, "dataset.json"))).mode & 0o777).toBe(0o600);
      expect(await readJson(join(path, "dataset.json"))).toEqual(dataset);
      await expect(writeRun(path, {}, meta)).rejects.toThrow();
      await eraseRun(path); await expect(stat(path)).rejects.toThrow();
      await expect(eraseRun(root)).rejects.toThrow();
    } finally { await rm(root, { recursive: true, force: true }); }
  });
  test("TTL removes expired derived runs without touching current runs or other directories", async () => {
    const root = await mkdtemp(join(tmpdir(), "overlap-ttl-"));
    try {
      await writeRun(join(root, "expired"), {}, { expiresAt: "2026-11-07T00:00:00Z", sessionKeys: [] });
      await writeRun(join(root, "current"), {}, { expiresAt: "2026-11-15T00:00:00Z", sessionKeys: [] });
      expect(await purgeExpiredRuns(root, new Date("2026-11-08T00:00:00Z"))).toBe(1);
      await expect(stat(join(root, "expired"))).rejects.toThrow();
      expect((await stat(join(root, "current"))).isDirectory()).toBe(true);
    } finally { await rm(root, { recursive: true, force: true }); }
  });
  test("low confidence suggestions never become alert rows", async () => {
    const result = await collectAlerts(d(), { version: "low", async assess(c) { const v = await fileBaseline.assess(c); return { ...v, confidence: "low" }; } },
      { asOf: "2026-11-07T12:00:00Z", mode: "replay", lookbackHours: 168, maxPairs: 10 });
    expect(result.alerts).toHaveLength(0);
  });
});
