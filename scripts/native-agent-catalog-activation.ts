/** Native operator command. URL only from environment; never print connection errors. */
import { expectedSchema } from "../src/hoopdb/migrate.ts";
import { nativeAgentCatalogActivation } from "../src/hoopdb/native-agent-catalog-activation.ts";
import { inTransaction, openHoopDb, schemaVerdict } from "../src/shared/db.ts";

const argv = process.argv.slice(2);
// Opt-in only: accepts an empty install as complete. Plan and apply must agree;
// the mode is in the fingerprint, so a mismatched apply refuses plan_changed.
const freshInstall = argv[argv.length - 1] === "--fresh-install";
const args = freshInstall ? argv.slice(0, -1) : argv;
const apply = args[0] === "--apply";
if (!(args.length === 0 || (args.length === 1 && args[0] === "--plan") ||
      (args.length === 2 && apply && /^[0-9a-f]{64}$/.test(args[1]!)))) {
  console.error("usage: bun scripts/native-agent-catalog-activation.ts [--plan | --apply PLAN_FINGERPRINT] [--fresh-install]");
  process.exit(64);
}
if (process.env.HOOPDB_DB_MODE !== "selfhosted") {
  console.error("native_catalog_selfhosted_only"); process.exit(64);
}
// The same verify-full URL semantics as the native services. Custom CA trust is
// configured via NODE_EXTRA_CA_CERTS, never by turning certificate checks off.
let secure = false;
try {
  const url = new URL(process.env.HOOPDB_URL ?? "");
  secure = ["postgres:", "postgresql:"].includes(url.protocol) &&
    url.searchParams.getAll("sslmode").length === 1 &&
    url.searchParams.get("sslmode") === "verify-full" && !url.searchParams.has("uselibpqcompat");
} catch { /* credentials must not reach diagnostics */ }
if (!secure || process.env.NODE_TLS_REJECT_UNAUTHORIZED === "0") {
  console.error("native_catalog_verify_full_required"); process.exit(64);
}
const db = openHoopDb();
try {
  const expected = await expectedSchema();
  const transact = <T>(work: Parameters<typeof inTransaction<T>>[1]) => inTransaction(db, work);
  const schema = await schemaVerdict(expected, transact);
  if (!schema.ok) throw new Error("native_catalog_schema_validation_required");
  const result = await nativeAgentCatalogActivation(transact, {
    mode: "selfhosted", generation: expected.generation,
    ...(apply ? { applyFingerprint: args[1]! } : {}),
    ...(freshInstall ? { freshInstall: true } : {}),
  });
  console.log(JSON.stringify(result));
  if (!result.eligible) process.exitCode = 2;
} catch (error) {
  const message = error instanceof Error && /^native_catalog_[a-z_]+$/.test(error.message)
    ? error.message : "native_catalog_operation_failed";
  console.error(message); process.exitCode = 1;
} finally { await db.end(); }
