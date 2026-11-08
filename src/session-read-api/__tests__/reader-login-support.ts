/**
 * Test support: the live rows a production read login answers to the startup admission
 * (lambda.ts READER_LOGIN_SQL / READER_PG16_SQL), shaped exactly as session_read_api_role.sql
 * provisions bulls96_read_api on PostgreSQL 17, logged in with SCRAM.
 */
import { READER_LOGIN_SQL, READER_PG16_SQL } from "../lambda.ts";

export const READER_LOGIN_ROW: Readonly<Record<string, unknown>> = Object.freeze({
  session_user: "bulls96_read_api",
  current_user: "bulls96_read_api",
  role_setting: "none",
  server_version: 170004,
  statement_timeout: "2s",
  default_transaction_read_only: "on",
  rolsuper: false,
  rolbypassrls: false,
  rolcreaterole: false,
  rolcreatedb: false,
  rolreplication: false,
  rolinherit: false,
  rolcanlogin: true,
  rolconfig: ["statement_timeout=2s", "default_transaction_read_only=on"],
  memberships: 0,
  members: 0,
  backend_user: "bulls96_read_api",
});

export const READER_PG16_ROW: Readonly<Record<string, unknown>> = Object.freeze({
  system_user: "scram-sha-256:bulls96_read_api",
  member_edges: [],
});

/** Rows for the admission queries; undefined for any other statement. */
export function readerLoginAnswer(text: string, login: Record<string, unknown> = READER_LOGIN_ROW, pg16: Record<string, unknown> = READER_PG16_ROW) {
  if (text === READER_LOGIN_SQL) return [login];
  if (text === READER_PG16_SQL) return [pg16];
  return undefined;
}
