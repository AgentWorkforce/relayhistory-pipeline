/**
 * READ_API_ANALYSIS_BUCKET on LOCKER_PROVIDER=azure (lambda.ts readApiAnalysisBucket): the S3 result reader does not
 * apply, so analysis stays unconfigured (the route's 501 analysis_not_configured), no S3 client is built, and one
 * startup warning carries the fixed code read_api_analysis_bucket_ignored_on_azure. The Azure store path is S2b.
 * Every other provider keeps the bucket exactly as before.
 */
import { afterEach, expect, test } from "bun:test";
import { readApiAnalysisBucket } from "../lambda.ts";

const write = process.stderr.write.bind(process.stderr);
afterEach(() => { process.stderr.write = write; });

function capture(fn: () => unknown): { value: unknown; lines: string[] } {
  const lines: string[] = [];
  process.stderr.write = ((chunk: string | Uint8Array) => { lines.push(String(chunk)); return true; }) as typeof process.stderr.write;
  try { return { value: fn(), lines }; } finally { process.stderr.write = write; }
}

test("azure: the bucket is ignored with one coded warning (no S3 client, analysis unconfigured)", () => {
  const { value, lines } = capture(() => readApiAnalysisBucket({ LOCKER_PROVIDER: "azure", READ_API_ANALYSIS_BUCKET: "fx-analysis-bucket" }));
  expect(value).toBeUndefined();
  expect(lines).toHaveLength(1);
  const line = JSON.parse(lines[0]!);
  expect(line).toMatchObject({ level: "warn", reason: "read_api_analysis_bucket_ignored_on_azure" });
  expect(lines[0]).not.toContain("fx-analysis-bucket");
});

test("unset LOCKER_PROVIDER, s3, gcs and fs keep the bucket unchanged and log nothing; unset bucket stays unset", () => {
  for (const provider of [undefined, "s3", "gcs", "fs"]) {
    const { value, lines } = capture(() => readApiAnalysisBucket({ ...(provider ? { LOCKER_PROVIDER: provider } : {}), READ_API_ANALYSIS_BUCKET: " fx-analysis-bucket " }));
    expect([provider, value, lines]).toEqual([provider, "fx-analysis-bucket", []]);
  }
  const { value, lines } = capture(() => readApiAnalysisBucket({ LOCKER_PROVIDER: "azure" }));
  expect([value, lines]).toEqual([undefined, []]);
});
