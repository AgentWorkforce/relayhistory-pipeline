/** Parser for the `supabase db dump --dry-run` login script printed by the operator's
 * Supabase CLI. The project ref is explicit configuration (no default target). */
import { SUPABASE_PROJECT_REF } from "./deploy-target.ts";
function refuse(code: string): never { throw Object.assign(new Error(code), { code }); }
/** Data parser only. Never execute or source the shell script printed by the CLI. */
export function parseCliLogin(script: string, projectRef: string): { password: string } {
  if (!SUPABASE_PROJECT_REF.test(projectRef)) refuse("cli_target_config");
  if (Buffer.byteLength(script) > 65536) refuse("cli_reply_limit");
  const fields = new Map<string, string>();
  for (const raw of script.split("\n")) {
    const match = /^(?:export )?(PGHOST|PGPORT|PGUSER|PGPASSWORD|PGDATABASE)=(.*)$/u.exec(raw.trim());
    if (!match) continue;
    const key = match[1]!, encoded = match[2]!.trim();
    if (fields.has(key)) refuse("cli_duplicate_field");
    let value: string;
    if (encoded.startsWith('"')) {
      try { value = JSON.parse(encoded); } catch { return refuse("cli_field_parse"); }
    } else if (encoded.startsWith("'")) {
      if (!/^'[^']*'$/u.test(encoded)) refuse("cli_field_parse");
      value = encoded.slice(1, -1);
    } else {
      if (!/^[A-Za-z0-9_./:+-]+$/u.test(encoded)) refuse("cli_field_parse");
      value = encoded;
    }
    if (typeof value !== "string" || /[\x00-\x1f\x7f]/u.test(value)) refuse("cli_field_parse");
    fields.set(key, value);
  }
  if (fields.size !== 5 || fields.get("PGHOST") !== `db.${projectRef}.supabase.co` || fields.get("PGPORT") !== "5432" || fields.get("PGUSER") !== "cli_login_postgres" || fields.get("PGDATABASE") !== "postgres") refuse("cli_target_pin");
  const password = fields.get("PGPASSWORD")!;
  if (password.length < 16 || /redact|your-password|\*\*\*/iu.test(password)) refuse("cli_credential_unavailable");
  return { password };
}
