/**
 * 20261105062250 (mode 'p', the pre-planned staged ACK): the exact text it adds to 062210's sessions.ingest_lazy_ack_batch
 * and to sessions.lazy_ack_t2_set_mode, and a strict inverse. stripPreplan(062250 body) must equal 062210's body byte for
 * byte; every piece must match exactly once, so any other drift fails the suites that use it.
 */
import { readFileSync } from "node:fs";

export const PREPLAN_MIG = "supabase/migrations/20261105062250_lazy_ack_preplan.sql";
export const T2_MIG = "supabase/migrations/20261105062210_lazy_ack_t2.sql";
export const PREPLAN_DOWN = "supabase/rollbacks/20261105062250_lazy_ack_preplan.down.sql";
export const PREPLAN_STEP = "upstream:20261105062250_lazy_ack_preplan.sql";

export const ackBodyOf = (sql: string) => {
  const m = sql.match(/CREATE OR REPLACE FUNCTION sessions\.ingest_lazy_ack_batch\(p jsonb\)[\s\S]*?AS \$lazy_ack_batch\$([\s\S]*?)\$lazy_ack_batch\$;/);
  if (!m) throw Error("ACK body not found");
  return m[1]!;
};
export const setModeBodyOf = (sql: string) => {
  const m = sql.match(/FUNCTION sessions\.lazy_ack_t2_set_mode\(p_mode text\)[\s\S]*?AS \$lazy_ack_tx_set_mode\$([\s\S]*?)\$lazy_ack_tx_set_mode\$;/);
  if (!m) throw Error("set_mode body not found");
  return m[1]!;
};
const once = (s: string, find: string | RegExp, repl: string) => {
  const n = typeof find === "string" ? s.split(find).length - 1 : (s.match(new RegExp(find.source, "g")) ?? []).length;
  if (n !== 1) throw Error(`062250 strip: ${String(find).slice(0, 60)} matched ${n} times`);
  return s.replace(find, repl);
};
/** The pre-plan block, from its first comment line to the END IF that closes it (exact, from the migration). */
export function preplanBlock(body: string): string {
  const a = body.indexOf(" -- 062250 mode 'p' (pre-planned ACK;"), b = body.indexOf("  END;\n END IF;\n", a);
  if (a < 0 || b < 0) throw Error("062250 pre-plan block not found");
  return body.slice(a, b + "  END;\n END IF;\n".length);
}
/** 062250's ACK body with exactly its additions removed: 062210's body. */
export function stripPreplan(body: string): string {
  let x = body;
  x = once(x, "v_t2 boolean;v_p boolean;v_pre boolean:=false;v_pstage uuid;v_pclaim uuid;v_pgroups jsonb;v_porg text;v_pws text;v_puser text;v_porigin text;\n v_precords jsonb;v_pepochs jsonb;v_preset bigint;v_pranks jsonb;v_pce text;v_psi text;v_precs jsonb;v_pok boolean;v_gate text;", "v_t2 boolean;v_gate text;");
  x = once(x, preplanBlock(x), "");
  x = once(x, " IF NOT (v_t2 OR v_p) THEN\n FOR r IN", " IF NOT v_t2 THEN\n FOR r IN");
  x = once(x, /  -- 062250 mode 'p': the pre-plan's \(A\) only if[\s\S]*?   v_recs:=v_precs;\n  ELSE\n/, "");
  x = once(x, "    FROM sessions.ingest_receipt_seat_records(t.claim_token,t.group_ordinals) q)z;\n  END IF;\n", "    FROM sessions.ingest_receipt_seat_records(t.claim_token,t.group_ordinals) q)z;\n");
  x = once(x, "  -- 062250 mode 'p': no upgrade; the clock is held NO KEY UPDATE since the main statement above and v_last came from it.\n  IF v_t2 THEN\n", "");
  x = once(x, "ck.singleton FOR NO KEY UPDATE;\n  END IF;\n", "ck.singleton FOR NO KEY UPDATE;\n");
  return x;
}
/** 062250's set_mode body with exactly its changes undone: 062210's. */
export function stripPreplanSetMode(body: string): string {
  let x = body;
  x = once(x, "p_mode NOT IN('main','t2','p') THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='T2 staged ACK: mode must be main, t2 or p';", "p_mode NOT IN('main','t2') THEN RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='T2 staged ACK: mode must be main or t2';");
  x = once(x, " IF v_old IN('t2','p') AND p_mode IN('t2','p') AND p_mode<>v_old THEN\n  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='T2 staged ACK: switch between t2 and p only through main; mode unchanged';END IF;\n", "");
  x = once(x, "IF p_mode IN('t2','p') AND v_gate<>'fenced' THEN", "IF p_mode='t2' AND v_gate<>'fenced' THEN");
  x = once(x, "MESSAGE='T2 staged ACK: mode t2 or p requires the gate fenced first", "MESSAGE='T2 staged ACK: mode t2 requires the gate fenced first");
  return x;
}
export const read = (p: string) => readFileSync(p, "utf8");
