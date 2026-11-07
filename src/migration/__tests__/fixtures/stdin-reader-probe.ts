// Test-only: read credentials from this process's real stdin (fd 0) through the production entry point (no options),
// and print one JSON line. No credentials are involved.
import { readStdinCredentials } from "../../stdin-credentials.ts";
const started = Date.now();
try {
  const doc = await readStdinCredentials(["databaseUrl"], ["databaseUrl"]);
  process.stdout.write(JSON.stringify({ ok: true, keys: Object.keys(doc), ms: Date.now() - started }) + "\n");
} catch (e) {
  process.stdout.write(JSON.stringify({ ok: false, code: (e as { code?: string }).code ?? String((e as Error).name), ms: Date.now() - started }) + "\n");
}
process.exit(0);
