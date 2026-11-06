/** Synthetic frozen-source fixture for the seal tests: a Neon-side model of relayhistory 0078
 * (state, status, permits, guard triggers, control heads, begin/complete) plus the legacy delivery
 * tables, and the actual Supabase target tables extracted from the migrations. No real data. */
import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";

export interface Q { query(sql: string, params?: unknown[]): Promise<{ rows: any[] }>; exec?(sql: string): Promise<unknown> }
export const EPOCH = "3".repeat(64), BASELINE = "a".repeat(64), IDENTITY = "c".repeat(64);
const hex = (s: string) => createHash("sha256").update(s).digest("hex");

// Neon model. Guard/permit/begin/complete bodies follow relayhistory 0078 (PRE snapshot stubbed).
export const SOURCE_SQL = `
CREATE SCHEMA sessions;
CREATE TABLE sessions.delivery_cutover_state(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 phase text NOT NULL CHECK(phase IN ('legacy','seeding','active','halted')),epoch text,privacy_baseline_sha256 text,updated_at timestamptz NOT NULL DEFAULT clock_timestamp());
INSERT INTO sessions.delivery_cutover_state(singleton,phase) VALUES(true,'legacy');
CREATE TABLE sessions.delivery_cutover_permits(xid bigint NOT NULL,backend_pid integer NOT NULL,epoch text NOT NULL,org_id text NOT NULL,workspace_id text NOT NULL);
CREATE FUNCTION sessions.delivery_cutover_status() RETURNS jsonb LANGUAGE sql VOLATILE SET search_path=pg_catalog,sessions,pg_temp AS $status$
  SELECT jsonb_build_object('version',1,'phase',phase,'epoch',epoch,'privacyBaselineSha256',privacy_baseline_sha256) FROM sessions.delivery_cutover_state WHERE singleton;
$status$;
CREATE TABLE sessions.delivery_origins(org_id text NOT NULL,workspace_id text NOT NULL,origin_id text NOT NULL,PRIMARY KEY(org_id,workspace_id,origin_id));
CREATE TABLE sessions.delivery_records(org_id text NOT NULL,workspace_id text NOT NULL,origin_id text NOT NULL,record_id text NOT NULL,
 revision_id text,revision bigint,digest text,kind text,source text,session_id text,operation text,user_id text,payload jsonb,
 PRIMARY KEY(org_id,workspace_id,origin_id,record_id));
CREATE TABLE sessions.delivery_projection_acceptance(org_id text,workspace_id text,origin_id text,record_id text,accepted_seq bigint,PRIMARY KEY(org_id,workspace_id,origin_id,record_id));
CREATE TABLE sessions.delivery_catalog_acceptance(org_id text,workspace_id text,origin_id text,record_id text,accepted_seq bigint,PRIMARY KEY(org_id,workspace_id,origin_id,record_id));
CREATE TABLE sessions.delivery_receipts(org_id text NOT NULL,workspace_id text NOT NULL,origin_id text NOT NULL,batch_id text NOT NULL,digest text NOT NULL,receipt jsonb NOT NULL,
 PRIMARY KEY(org_id,workspace_id,origin_id,batch_id));
CREATE TABLE sessions.deleted_upload_sessions(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,origin_id text NOT NULL,source text NOT NULL,session_id text NOT NULL,
 PRIMARY KEY(org_id,workspace_id,user_id,origin_id,source,session_id));
CREATE TABLE sessions.session_resets(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,request_id text NOT NULL,deleted bigint NOT NULL,
 PRIMARY KEY(org_id,workspace_id,user_id,request_id));
CREATE TABLE sessions.delivery_cutover_control_heads(org_id text NOT NULL,workspace_id text NOT NULL,user_id text NOT NULL,
 epoch text NOT NULL,version bigint NOT NULL CHECK(version>0),digest text NOT NULL CHECK(digest ~ '^[0-9a-f]{64}$'),
 kind text NOT NULL CHECK(kind IN ('selection','reset')),payload jsonb NOT NULL,pending boolean NOT NULL,deleted bigint,
 before_privacy jsonb,privacy_version bigint NOT NULL DEFAULT 0,updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(org_id,workspace_id,user_id));
CREATE FUNCTION sessions.guard_delivery_cutover_v1() RETURNS trigger LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,sessions,pg_temp AS $guard$
DECLARE s sessions.delivery_cutover_state%ROWTYPE; old_row jsonb; new_row jsonb;
BEGIN
  SELECT * INTO STRICT s FROM sessions.delivery_cutover_state WHERE singleton FOR SHARE;
  IF s.phase='legacy' THEN IF TG_LEVEL='STATEMENT' THEN RETURN NULL; END IF; RETURN COALESCE(NEW,OLD); END IF;
  IF TG_LEVEL='ROW' THEN
    IF TG_OP<>'INSERT' THEN old_row:=to_jsonb(OLD); END IF;
    IF TG_OP<>'DELETE' THEN new_row:=to_jsonb(NEW); END IF;
  END IF;
  IF s.phase='active' AND EXISTS (SELECT 1 FROM sessions.delivery_cutover_permits p
      WHERE p.xid=txid_current() AND p.backend_pid=pg_backend_pid() AND p.epoch=s.epoch
        AND (TG_LEVEL='STATEMENT' OR
          ((old_row IS NULL OR (old_row->>'org_id'=p.org_id AND COALESCE(old_row->>'workspace_id','')=p.workspace_id))
           AND (new_row IS NULL OR (new_row->>'org_id'=p.org_id AND COALESCE(new_row->>'workspace_id','')=p.workspace_id))))) THEN
    IF TG_LEVEL='STATEMENT' THEN RETURN NULL; END IF;
    RETURN COALESCE(NEW,OLD);
  END IF;
  RAISE EXCEPTION 'delivery_cutover_held' USING ERRCODE='55000';
END $guard$;
DO $t$ DECLARE name text; BEGIN
  FOREACH name IN ARRAY ARRAY['delivery_origins','delivery_receipts','delivery_records','deleted_upload_sessions','session_resets',
      'delivery_catalog_acceptance','delivery_projection_acceptance'] LOOP
    EXECUTE format('CREATE TRIGGER delivery_cutover_statement BEFORE INSERT OR UPDATE OR DELETE ON sessions.%I FOR EACH STATEMENT EXECUTE FUNCTION sessions.guard_delivery_cutover_v1()',name);
    EXECUTE format('CREATE TRIGGER delivery_cutover_row BEFORE INSERT OR UPDATE OR DELETE ON sessions.%I FOR EACH ROW EXECUTE FUNCTION sessions.guard_delivery_cutover_v1()',name);
  END LOOP; END $t$;
CREATE FUNCTION sessions.require_delivery_cutover_control_v1(p_epoch text,p_org text,p_workspace text) RETURNS void LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,sessions,pg_temp AS $permit$
DECLARE s sessions.delivery_cutover_state%ROWTYPE;
BEGIN
  SELECT * INTO STRICT s FROM sessions.delivery_cutover_state WHERE singleton FOR SHARE;
  IF s.phase<>'active' OR s.epoch IS DISTINCT FROM p_epoch OR s.privacy_baseline_sha256 IS NULL OR p_org IS NULL OR length(p_org)=0 OR p_workspace IS NULL THEN
    RAISE EXCEPTION 'delivery_cutover_held' USING ERRCODE='55000'; END IF;
  INSERT INTO sessions.delivery_cutover_permits VALUES(txid_current(),pg_backend_pid(),p_epoch,p_org,p_workspace);
END $permit$;
CREATE FUNCTION sessions.begin_delivery_cutover_control_v1(p_epoch text,p_org text,p_workspace text,p_user text,p_kind text,p_payload jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,sessions,pg_temp AS $begin_control$
DECLARE h sessions.delivery_cutover_control_heads%ROWTYPE; d text; fresh boolean;
BEGIN
  PERFORM sessions.require_delivery_cutover_control_v1(p_epoch,p_org,p_workspace);
  d:=encode(sha256(convert_to(jsonb_build_array('delivery-cutover-control-v1',p_epoch,p_org,p_workspace,p_user,p_kind,p_payload)::text,'UTF8')),'hex');
  INSERT INTO sessions.delivery_cutover_control_heads(org_id,workspace_id,user_id,epoch,version,digest,kind,payload,pending)
    VALUES(p_org,p_workspace,p_user,p_epoch,1,d,p_kind,p_payload,true) ON CONFLICT DO NOTHING RETURNING true INTO fresh;
  SELECT * INTO STRICT h FROM sessions.delivery_cutover_control_heads WHERE org_id=p_org AND workspace_id=p_workspace AND user_id=p_user FOR UPDATE;
  IF COALESCE(fresh,false) THEN
    UPDATE sessions.delivery_cutover_control_heads SET before_privacy='{}'::jsonb,privacy_version=0
      WHERE org_id=p_org AND workspace_id=p_workspace AND user_id=p_user RETURNING * INTO h;
  END IF;
  IF NOT COALESCE(fresh,false) THEN
    IF h.pending THEN
      IF h.digest IS DISTINCT FROM d OR h.kind IS DISTINCT FROM p_kind THEN RAISE EXCEPTION 'delivery_control_pending' USING ERRCODE='55000'; END IF;
      RETURN jsonb_build_object('epoch',h.epoch,'version',h.version::text,'digest',h.digest,'kind',h.kind,'pending',true,'apply',false);
    END IF;
    UPDATE sessions.delivery_cutover_control_heads SET version=h.version+1,digest=d,kind=p_kind,payload=p_payload,pending=true,deleted=NULL,
      before_privacy=NULL,privacy_version=h.version,updated_at=clock_timestamp()
      WHERE org_id=p_org AND workspace_id=p_workspace AND user_id=p_user RETURNING * INTO h;
  END IF;
  RETURN jsonb_build_object('epoch',h.epoch,'version',h.version::text,'digest',h.digest,'kind',h.kind,'pending',true,'apply',true);
END $begin_control$;
-- Fixture equivalents of set_session_uploads_cutover_v1(enabled=false) and a zero-session reset,
-- each followed by its completion (relayhistory completes in a second call).
CREATE FUNCTION sessions.fixture_disable_session_v1(p_org text,p_ws text,p_user text,p_origin text,p_source text,p_session text) RETURNS jsonb
LANGUAGE plpgsql SET search_path=pg_catalog,sessions,pg_temp AS $f$
DECLARE i jsonb; e text;
BEGIN
  SELECT epoch INTO e FROM sessions.delivery_cutover_state;
  i:=sessions.begin_delivery_cutover_control_v1(e,p_org,p_ws,p_user,'selection',jsonb_build_object('origin_id',p_origin,'session',p_session,'enabled',false));
  IF (i->>'apply')::boolean THEN
    INSERT INTO sessions.deleted_upload_sessions VALUES(p_org,p_ws,p_user,p_origin,p_source,p_session) ON CONFLICT DO NOTHING;
    UPDATE sessions.delivery_records SET operation='delete',payload=NULL WHERE org_id=p_org AND workspace_id=p_ws AND origin_id=p_origin AND source=p_source AND session_id=p_session;
  END IF;
  DELETE FROM sessions.delivery_cutover_permits WHERE xid=txid_current() AND backend_pid=pg_backend_pid();
  UPDATE sessions.delivery_cutover_control_heads SET pending=false WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user;
  RETURN (SELECT to_jsonb(h) FROM sessions.delivery_cutover_control_heads h WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user);
END $f$;
CREATE FUNCTION sessions.fixture_empty_reset_v1(p_org text,p_ws text,p_user text,p_request text) RETURNS jsonb
LANGUAGE plpgsql SET search_path=pg_catalog,sessions,pg_temp AS $f$
DECLARE i jsonb; e text;
BEGIN
  SELECT epoch INTO e FROM sessions.delivery_cutover_state;
  i:=sessions.begin_delivery_cutover_control_v1(e,p_org,p_ws,p_user,'reset',jsonb_build_object('request_id',p_request));
  IF (i->>'apply')::boolean THEN INSERT INTO sessions.session_resets VALUES(p_org,p_ws,p_user,p_request,0); END IF;
  DELETE FROM sessions.delivery_cutover_permits WHERE xid=txid_current() AND backend_pid=pg_backend_pid();
  UPDATE sessions.delivery_cutover_control_heads SET pending=false WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user;
  RETURN (SELECT to_jsonb(h) FROM sessions.delivery_cutover_control_heads h WHERE org_id=p_org AND workspace_id=p_ws AND user_id=p_user);
END $f$;
`;

/** The actual Supabase table definitions, extracted from the migrations (no hand copy). */
export function targetSql(): string {
  const root = fileURLToPath(new URL("../../../../supabase/migrations/", import.meta.url));
  const custody = readFileSync(root + "20261105010000_frozen_source_custody.sql", "utf8");
  const lazy = readFileSync(root + "20261104055000_lazy_current_custody.sql", "utf8");
  const table = (src: string, name: string) => {
    const m = new RegExp(`CREATE TABLE sessions\\.${name}\\((?:.|\\n)*?\\n\\);`).exec(src);
    if (!m) throw Error("fixture: table not found in migration: " + name);
    return m[0];
  };
  return ["CREATE SCHEMA IF NOT EXISTS sessions;", table(lazy, "ingest_lazy_policy"),
    ...["ingest_frozen_source_records", "ingest_frozen_source_deletions", "ingest_frozen_source_resets", "ingest_frozen_source_org_seals",
      "ingest_frozen_source_completeness", "ingest_source_control_heads", "ingest_frozen_source_receipts"].map((n) => table(custody, n)),
    `INSERT INTO sessions.ingest_lazy_policy(baseline_mode,generation,source_identity_sha256,control_epoch,privacy_baseline_sha256,retention_contract_sha256,source_proof_sha256,issuer,valid_until)
     VALUES('lazy-current','${"e".repeat(64)}','${IDENTITY}','${EPOCH}','${BASELINE}','${"b".repeat(64)}','${"d".repeat(64)}','relayhistory-cloud',now()+interval '1 day');`].join("\n");
}

// Orgs whose C order differs from ICU order; one org has origins but no rows.
export const ORGS = { "Org-B": 40, "org-a": 63, "org-c-empty": 0, "org-z": 25, "örg-ü": 18 } as const;
export const ORG_ORDER_C = Object.keys(ORGS).sort((a, b) => (Buffer.compare(Buffer.from(a), Buffer.from(b))));
const PREFIX = ["a", "A", "_", "!", "á", "z", "Z", "a-b", "a_b"];
const WS = ["", "ws-1", "WS-2"];

/** Rows for the source; deterministic. */
export function sourceRows() {
  const origins: unknown[][] = [], records: unknown[][] = [], proj: unknown[][] = [], cat: unknown[][] = [], receipts: unknown[][] = [];
  for (const [org, n] of Object.entries(ORGS)) {
    origins.push([org, "ws-1", "origin-1"], [org, "", "Origin-2"]);
    for (let i = 0; i < n; i++) {
      const ws = WS[i % 3]!, origin = i % 2 ? "origin-1" : "Origin-2", rid = PREFIX[i % PREFIX.length]! + i;
      records.push([org, ws, origin, rid, "rev-" + i, i + 1, hex(org + rid), "event", i % 2 ? "claude" : "codex", i % 7 === 0 ? null : "s" + (i % 5), "upsert", i % 2 ? "u1" : "u2"]);
      if (i % 3 === 0) proj.push([org, ws, origin, rid, i * 10 + 1]);
      if (i % 4 === 0) cat.push([org, ws, origin, rid, i * 10 + 2]);
    }
    for (let j = 0; j < Math.min(5, Math.ceil(n / 10)); j++) receipts.push([org, WS[j % 3]!, "origin-1", (j % 2 ? "B" : "b") + j, hex(org + "b" + j), JSON.stringify({ ok: true, n: j, label: "réçu" })]);
  }
  return { origins, records, proj, cat, receipts };
}

/** Initializes the frozen Neon model: loads in phase legacy, then freezes (phase active). */
export async function initSource(db: Q, run: (sql: string) => Promise<unknown>) {
  await run(SOURCE_SQL);
  const r = sourceRows();
  for (const x of r.origins) await db.query("INSERT INTO sessions.delivery_origins VALUES($1,$2,$3)", x);
  for (const x of r.records) await db.query("INSERT INTO sessions.delivery_records(org_id,workspace_id,origin_id,record_id,revision_id,revision,digest,kind,source,session_id,operation,user_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)", x);
  for (const x of r.proj) await db.query("INSERT INTO sessions.delivery_projection_acceptance VALUES($1,$2,$3,$4,$5)", x);
  for (const x of r.cat) await db.query("INSERT INTO sessions.delivery_catalog_acceptance VALUES($1,$2,$3,$4,$5)", x);
  for (const x of r.receipts) await db.query("INSERT INTO sessions.delivery_receipts VALUES($1,$2,$3,$4,$5,$6::jsonb)", x);
  await db.query("INSERT INTO sessions.deleted_upload_sessions VALUES('org-a','ws-1','u1','origin-1','claude','s1'),('org-z','','u2','Origin-2','codex','s2'),('Org-B','WS-2','u1','origin-1','claude','s3')");
  await db.query("INSERT INTO sessions.session_resets VALUES('org-a','ws-1','u1','req-1',3),('örg-ü','','u2','req-2',0)");
  await db.query(`INSERT INTO sessions.delivery_cutover_control_heads(org_id,workspace_id,user_id,epoch,version,digest,kind,payload,pending,privacy_version)
    VALUES('org-a','ws-1','u1','${EPOCH}',2,'${hex("head")}','selection','{"enabled":false}',false,1)`);
  await db.query(`UPDATE sessions.delivery_cutover_state SET phase='active',epoch='${EPOCH}',privacy_baseline_sha256='${BASELINE}'`);
}
export async function initTarget(run: (sql: string) => Promise<unknown>) { await run(targetSql()); }

/** Seal rows that carry the attestation (timestamps excluded). */
export async function sealRows(db: Q) {
  const seals = (await db.query("SELECT org_id,record_rows::text r,record_sum::text s,privacy_rows::text pr,privacy_sum::text ps FROM sessions.ingest_frozen_source_org_seals ORDER BY org_id COLLATE \"C\"")).rows;
  const c = (await db.query("SELECT source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count::text,record_rows::text,manifest_sha256,neon_snapshot FROM sessions.ingest_frozen_source_completeness")).rows;
  return { seals: seals.map((r) => ({ ...r })), completeness: c.map((r) => ({ ...r })) };
}
