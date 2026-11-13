import { readFileSync } from "node:fs";
import type pg from "pg";
import { advanceReset } from "../account-reset-runner.ts";

export const cloudSchema = readFileSync(
  new URL("../../../scripts/ops/account-reset/cloud-neon.sql", import.meta.url),
  "utf8",
);
export const id = (n: number) =>
  `10000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
export const alice = id(1),
  bob = id(2),
  own = id(10),
  shared = id(11),
  former = id(12),
  foreign = id(13);
export const base = `
CREATE TABLE users(id uuid PRIMARY KEY,primary_email text,name text,avatar_url text,last_organization_id uuid,last_workspace_id uuid,cloud_agent_spawn_quota_override int,updated_at timestamptz DEFAULT now());
CREATE TABLE organizations(id uuid PRIMARY KEY,created_by_user_id uuid);
CREATE TABLE organization_memberships(organization_id uuid,user_id uuid,status text,updated_at timestamptz,PRIMARY KEY(organization_id,user_id));
CREATE TABLE workspaces(id uuid PRIMARY KEY,organization_id uuid,relay_workspace_id text);
CREATE TABLE relay_workspaces(id text PRIMARY KEY,owner_user_id uuid,relaycast_workspace_id text UNIQUE,relaycast_api_key text,updated_at timestamptz);
CREATE TABLE auth_identities(user_id uuid);
CREATE TABLE api_token_sessions(user_id uuid,revoked_at timestamptz,revoked_reason text,updated_at timestamptz);
INSERT INTO users(id,primary_email) VALUES ('${alice}','alice@test'),('${bob}','bob@test');
INSERT INTO organizations VALUES ('${own}','${alice}'),('${shared}','${alice}'),('${former}','${alice}'),('${foreign}','${bob}');
INSERT INTO organization_memberships VALUES ('${own}','${alice}','active',now()),('${shared}','${alice}','active',now()),('${shared}','${bob}','active',now()),
('${former}','${alice}','active',now()),('${former}','${bob}','revoked',now()),('${foreign}','${bob}','active',now());
INSERT INTO relay_workspaces VALUES ('r1','${alice}','provider-alice','key-a',now()),('r2','${bob}','provider-bob','key-b',now()),('r3','${alice}','provider-shared','key-s',now());
INSERT INTO workspaces VALUES ('${id(20)}','${own}','r1'),('${id(21)}','${foreign}','r2'),('${id(22)}','${shared}','r3');
INSERT INTO auth_identities VALUES ('${alice}'),('${bob}');
INSERT INTO api_token_sessions(user_id) VALUES ('${alice}'),('${bob}');`;
export const legacyFixture = `CREATE SCHEMA sessions;
CREATE TABLE sessions.delivery_records(org_id text,workspace_id text,user_id text,session_id text,content text);
INSERT INTO sessions.delivery_records VALUES ('${own}','${id(20)}','${alice}','collision','alice content'),('${own}','${id(20)}','${bob}','collision','bob content'),('${foreign}','${id(21)}','${bob}','collision','bob other org');
CREATE TABLE sessions.auth_sessions(org_id text,workspace_id text,user_id text,revoked_at timestamptz,updated_at timestamptz,revoked_reason text);
INSERT INTO sessions.auth_sessions(org_id,workspace_id,user_id) SELECT org_id,workspace_id,user_id FROM sessions.delivery_records;
CREATE TABLE sessions.probe_connections(org_id text,workspace_id text,user_id text);
INSERT INTO sessions.probe_connections SELECT org_id,workspace_id,user_id FROM sessions.delivery_records;
CREATE TABLE sessions.calls(org_id text,ws text,owner text,request text);
CREATE FUNCTION sessions.reset_uploaded_sessions(text,text,text,text) RETURNS int LANGUAGE sql AS $$
  INSERT INTO sessions.calls VALUES ($1,$2,$3,$4); DELETE FROM sessions.delivery_records WHERE org_id=$1 AND workspace_id=$2 AND user_id=$3; SELECT 0
$$;`;
export async function enqueue(db: pg.Client, email = "alice@test") {
  return (
    await db.query(
      "SELECT * FROM public.admin_enqueue_account_reset($1,$2,'staff')",
      [alice, email],
    )
  ).rows;
}
export async function job(db: pg.Client) {
  return (
    await db.query<Parameters<typeof advanceReset>[1]>(
      "SELECT * FROM public.admin_account_resets WHERE user_id=$1",
      [alice],
    )
  ).rows[0]!;
}
export const verified = async () => ({
  enabled: true as const,
  state: "bytes_verified" as const,
  verified: true,
  deleted: {},
  tokensRevoked: 1,
  steps: 1,
  leaseWaits: 0,
  prefixes: 1,
  versionsDeleted: 1,
  byteRounds: 1,
});
