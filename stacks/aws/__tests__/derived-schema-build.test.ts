import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { basename, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { generation } from "../../../src/hoopdb/migrate.ts";
import { derivedSchemaBuildPlugin, packagedDerivedSchemaSource } from "../derived-schema-build.ts";

const root = fileURLToPath(new URL("../../..", import.meta.url));
const verifierNames = [
  ["session_directory_search_worker_verify.sql", "verify:session_directory_search_worker"],
  ["session_token_verifier_verify.sql", "verify:session_token_verifier"],
  ["session_ingest_seeder_verify.sql", "verify:session_ingest_seeder"],
  ["session_ingest_verify.sql", "verify:session_ingest"],
  ["canonical_project_events_verify.sql", "verify:canonical_project_events"],
  ["acquisition_installer_verify.sql", "verify:acquisition_installer"],
] as const;

test("captured build/watch inputs reproduce the actual generation without expanding runtime verifiers", async () => {
  const inputs: string[] = [];
  const code = await packagedDerivedSchemaSource(path => inputs.push(path));
  expect(new Set(inputs).size).toBe(inputs.length);
  const parts: [string, string][] = [];
  for (const path of inputs) {
    const rel = relative(root, path);
    const verifier = verifierNames.find(([file]) => file === basename(path));
    const name = verifier?.[1] ?? (rel.startsWith("supabase/migrations/") ? `upstream:${basename(path)}`
      : rel.startsWith("src/hoopdb/migrations/") ? `96bulls:${basename(path)}` : undefined);
    if (!name) throw new Error(`unexpected schema input: ${rel}`);
    parts.push([name, createHash("sha256").update(await readFile(path, "utf8")).digest("hex")]);
  }
  const ordered = [
    ...parts.filter(([name]) => name.startsWith("upstream:")).sort(([a], [b]) => a.localeCompare(b)),
    ...parts.filter(([name]) => name.startsWith("96bulls:")).sort(([a], [b]) => a.localeCompare(b)),
    ...verifierNames.map(([, name]) => parts.find(([n]) => n === name)),
  ];
  expect(ordered.every(Boolean)).toBe(true);
  expect(createHash("sha256").update(JSON.stringify(ordered)).digest("hex")).toBe(await generation());

  const packaged = await import(`data:text/javascript;base64,${Buffer.from(code).toString("base64")}`);
  expect(Object.keys((await packaged.derivedSchemaAssets()).verifiers).sort()).toEqual([
    "ingest", "ingestSeeder", "searchWorker", "tokenVerifier",
  ]);
  const watched: string[] = [];
  const loaded = await derivedSchemaBuildPlugin().load.call({ addWatchFile(path) { watched.push(path); } },
    fileURLToPath(new URL("../../../src/session-pipeline/derived-schema.ts", import.meta.url)));
  expect(loaded?.code).toBe(code);
  expect(watched).toEqual(inputs);
});
