import { readFile, stat } from "node:fs/promises";
import { writeLocalExport, type LocalExportOptions } from "./local-export.ts";

export async function runLocalExport(configPath: string, outputPath: string) {
  if ((await stat(configPath)).size > 65536)
    throw new Error("Local pilot config exceeds 64 KiB.");
  let input: unknown;
  try {
    input = JSON.parse(await readFile(configPath, "utf8"));
  } catch {
    throw new Error("Local pilot config is not valid JSON.");
  }
  if (!input || typeof input !== "object" || Array.isArray(input))
    throw new Error("Local pilot config must be an object.");
  const c = input as Record<string, unknown>;
  const strings = [
    "dbPath",
    "organizationId",
    "workspaceId",
    "since",
    "until",
    "projectPrefix",
    "machineLabel",
  ] as const;
  const numbers = [
    "perSessionLimit",
    "fileEditLimit",
    "excerptChars",
    "timeoutMs",
  ] as const;
  const keys: string[] = [...strings, ...numbers, "sessionKeys"];
  if (Object.keys(c).some((key) => !keys.includes(key)))
    throw new Error("Local pilot config contains an unsupported field.");
  for (const key of strings)
    if (typeof c[key] !== "string" || !(c[key] as string).trim())
      throw new Error("Local pilot config is missing required metadata.");
  for (const key of numbers)
    if (c[key] !== undefined && typeof c[key] !== "number")
      throw new Error("Local pilot limits must be numbers.");
  if (
    c.sessionKeys !== undefined &&
    (!Array.isArray(c.sessionKeys) ||
      c.sessionKeys.some((key) => typeof key !== "string"))
  )
    throw new Error("Selected sessions must be a list of keys.");
  return writeLocalExport(c as unknown as LocalExportOptions, outputPath);
}

if (import.meta.main) {
  const args = process.argv.slice(2);
  if (args.length !== 4 || args[0] !== "--config" || args[2] !== "--out") {
    process.stderr.write(
      "Usage: bun run coordination:export-local --config PRIVATE_PILOT_CONFIG.json --out NEW_EXPORT.json\n",
    );
    process.exitCode = 1;
  } else {
    try {
      const result = await runLocalExport(args[1]!, args[3]!);
      process.stdout.write(
        JSON.stringify({
          sessions: result.selectedSessions,
          sampledEvents: result.selectedEvents,
          omittedConversations: result.omittedConversations,
          omittedFileEdits: result.omittedFileEdits,
        }) + "\n",
      );
    } catch (error) {
      const message =
        error instanceof Error && !("code" in error)
          ? error.message
          : "Could not read pilot metadata or write a new private export. Existing files are not overwritten.";
      process.stderr.write(message + "\n");
      process.exitCode = 1;
    }
  }
}
