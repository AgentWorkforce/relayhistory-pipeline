-- Slot 20261105062360 (registry reservation receipt 252139230903828480): the operator session erase replay-target
-- manifest, per docs/design/operator-erase-replay-target-manifest.md (draft 3, 37be39c5). Code only: applying it to any
-- real database needs Khaliq's explicit OK. Raw-identifier retention is an open owner decision (design section 8): this
-- slot adds no TTL, GC or cleanup, and claims no retention policy.
--
-- Why: the SQL erase discovers aliases, owners and origins before it removes them. The 062320 hold release cannot
-- rebuild that set afterwards, so the bridge answers session_manifest_missing for every session erasure. This slot
-- persists the exact finite target set inside the erase transaction, under the erase's own locks, and seals it before
-- the first destructive selection. The seal commits with the SQL effects or not at all. Prospective only: no backfill.
--
-- Objects (all owner-only, RLS on, immutable; this slot's down KEEPS the four tables as the replay witness):
--   sessions.session_erasure_replay_requests     intent: manifest UUID, the accepted receipt's request_id, the tenant,
--                                                source and requested session, capture version.
--   sessions.session_erasure_replay_aliases      the original alias declaration A0; one row per fence identity.
--   sessions.session_erasure_replay_targets      T = {(u, o, s, a) | u in U, o in O(u), a in A0}.
--   sessions.session_erasure_replay_completions  one seal per manifest: counts and the canonical digest. Only for a
--                                                complete, nonempty capture.
--   sessions.session_erasure_replay_read(...)    the owner reader (design section 6.1, wire v1), READ ONLY.
--
-- Eligibility (design section 3, default "a retry cannot seal"): an intent is written only by the call that inserted the
-- receipt (the first attempt), and only when no alias of its closure is already bound to another manifest. That same
-- uninterrupted call may seal it. A retried (failed) receipt never creates or seals an intent; a caught RH010 rolls the
-- capture back with the cleanup and leaves the intent unsealed. The existing erase behaviour is unchanged on every path.
--
-- Lock order is unchanged: 062320's org check (SHARED, in ingest_operator_runtime_begin) first, then the global erase
-- gate (961996,50000), alias writer locks, the receipt row, the sorted owner fence locks. Capture takes no new lock but
-- the parent request row (FOR SHARE by a child insert, FOR UPDATE by the seal), which only this transaction writes.
--
-- The four tables are not 062340 erasure-history sources: a manifest write is not an erasure and does not change the
-- release count (count of history rows), which stays exactly 062340's.

-- A superset of the receipt primary key: the compound target the intent's foreign key names, so the intent's request_id
-- is provably the accepted receipt's. No row is written.
DO $receipt_binding$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='sessions.session_erasure_receipts'::regclass
   AND conname='session_erasure_receipts_request_binding') THEN
  ALTER TABLE sessions.session_erasure_receipts
   ADD CONSTRAINT session_erasure_receipts_request_binding UNIQUE (org_id, workspace_id, source, session_id, request_id);
 END IF;
END $receipt_binding$;

CREATE TABLE IF NOT EXISTS sessions.session_erasure_replay_requests (
  manifest_id uuid NOT NULL DEFAULT gen_random_uuid(),
  request_id text NOT NULL,
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  source text NOT NULL,
  requested_session_id text NOT NULL,
  capture_version smallint NOT NULL CHECK (capture_version = 1),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT session_erasure_replay_requests_pkey PRIMARY KEY (manifest_id),
  CONSTRAINT session_erasure_replay_requests_receipt UNIQUE (org_id, workspace_id, source, requested_session_id),
  CONSTRAINT session_erasure_replay_requests_request UNIQUE (request_id),
  CONSTRAINT session_erasure_replay_requests_scope UNIQUE (manifest_id, org_id, workspace_id, source),
  CONSTRAINT session_erasure_replay_requests_receipt_fk FOREIGN KEY (org_id, workspace_id, source, requested_session_id, request_id)
    REFERENCES sessions.session_erasure_receipts (org_id, workspace_id, source, session_id, request_id)
);
CREATE TABLE IF NOT EXISTS sessions.session_erasure_replay_aliases (
  manifest_id uuid NOT NULL,
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  source text NOT NULL,
  alias_session_id text NOT NULL,
  CONSTRAINT session_erasure_replay_aliases_pkey PRIMARY KEY (org_id, workspace_id, source, alias_session_id),
  CONSTRAINT session_erasure_replay_aliases_member UNIQUE (org_id, workspace_id, source, alias_session_id, manifest_id),
  CONSTRAINT session_erasure_replay_aliases_request_fk FOREIGN KEY (manifest_id, org_id, workspace_id, source)
    REFERENCES sessions.session_erasure_replay_requests (manifest_id, org_id, workspace_id, source),
  CONSTRAINT session_erasure_replay_aliases_fence_fk FOREIGN KEY (org_id, workspace_id, source, alias_session_id)
    REFERENCES sessions.session_erasure_fences (org_id, workspace_id, source, session_id)
);
CREATE INDEX IF NOT EXISTS session_erasure_replay_aliases_manifest
  ON sessions.session_erasure_replay_aliases (manifest_id, alias_session_id COLLATE "C");
CREATE TABLE IF NOT EXISTS sessions.session_erasure_replay_targets (
  manifest_id uuid NOT NULL,
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  user_id text NOT NULL,
  origin_id text NOT NULL,
  source text NOT NULL,
  session_id text NOT NULL,
  CONSTRAINT session_erasure_replay_targets_pkey PRIMARY KEY (manifest_id, user_id, origin_id, source, session_id),
  CONSTRAINT session_erasure_replay_targets_request_fk FOREIGN KEY (manifest_id, org_id, workspace_id, source)
    REFERENCES sessions.session_erasure_replay_requests (manifest_id, org_id, workspace_id, source),
  CONSTRAINT session_erasure_replay_targets_alias_fk FOREIGN KEY (org_id, workspace_id, source, session_id, manifest_id)
    REFERENCES sessions.session_erasure_replay_aliases (org_id, workspace_id, source, alias_session_id, manifest_id)
);
-- The reader's keyset order: bytewise (user_id, origin_id, source, session_id) within one manifest.
CREATE UNIQUE INDEX IF NOT EXISTS session_erasure_replay_targets_order ON sessions.session_erasure_replay_targets
  (manifest_id, user_id COLLATE "C", origin_id COLLATE "C", source COLLATE "C", session_id COLLATE "C");
CREATE TABLE IF NOT EXISTS sessions.session_erasure_replay_completions (
  manifest_id uuid NOT NULL,
  org_id text NOT NULL,
  workspace_id text NOT NULL,
  source text NOT NULL,
  derivation_version smallint NOT NULL CHECK (derivation_version = 1),
  alias_count integer NOT NULL CHECK (alias_count BETWEEN 1 AND 256),
  owner_count integer NOT NULL CHECK (owner_count BETWEEN 1 AND 64),
  pair_count bigint NOT NULL CHECK (pair_count >= owner_count),
  target_count bigint NOT NULL CHECK (target_count > 0 AND target_count = pair_count * alias_count),
  manifest_sha256 text NOT NULL CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
  completed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT session_erasure_replay_completions_pkey PRIMARY KEY (manifest_id),
  CONSTRAINT session_erasure_replay_completions_request_fk FOREIGN KEY (manifest_id, org_id, workspace_id, source)
    REFERENCES sessions.session_erasure_replay_requests (manifest_id, org_id, workspace_id, source)
);
ALTER TABLE sessions.session_erasure_replay_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.session_erasure_replay_aliases ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.session_erasure_replay_targets ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.session_erasure_replay_completions ENABLE ROW LEVEL SECURITY;

-- All four are insert-only: no UPDATE, DELETE or TRUNCATE.
CREATE OR REPLACE FUNCTION sessions.session_erasure_replay_guard() RETURNS trigger
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $replay_guard$
BEGIN
 RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session erasure replay manifest: records are immutable';
END $replay_guard$;

-- The canonical digest (design section 4): SHA-256 over the raw ASCII domain tag, then every field framed as its
-- 4-byte big-endian UTF-8 octet length then its bytes; each list is its count (framed canonical decimal text, bigint
-- range) then its members in bytewise ("C") order. Fields after the tag: capture_version (decimal), manifest_id
-- (canonical lowercase), request_id, org, workspace, source, requested session, the aliases, then each target as
-- (user_id, origin_id, source, session_id). The same encoding is src/hoopdb/session-erasure-replay-digest.ts (test vectors pinned there and checked against this function).
CREATE OR REPLACE FUNCTION sessions.session_erasure_replay_digest(p_manifest uuid) RETURNS text
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $replay_digest$
 WITH r AS (SELECT * FROM sessions.session_erasure_replay_requests WHERE manifest_id=p_manifest),
 a AS (SELECT coalesce(string_agg(int4send(octet_length(convert_to(x.alias_session_id,'UTF8')))||convert_to(x.alias_session_id,'UTF8'),
        ''::bytea ORDER BY x.alias_session_id COLLATE "C"),''::bytea) AS b, count(*) AS n
       FROM sessions.session_erasure_replay_aliases AS x WHERE x.manifest_id=p_manifest),
 t AS (SELECT coalesce(string_agg(
          int4send(octet_length(convert_to(x.user_id,'UTF8')))||convert_to(x.user_id,'UTF8')
        ||int4send(octet_length(convert_to(x.origin_id,'UTF8')))||convert_to(x.origin_id,'UTF8')
        ||int4send(octet_length(convert_to(x.source,'UTF8')))||convert_to(x.source,'UTF8')
        ||int4send(octet_length(convert_to(x.session_id,'UTF8')))||convert_to(x.session_id,'UTF8'),
        ''::bytea ORDER BY x.user_id COLLATE "C",x.origin_id COLLATE "C",x.source COLLATE "C",x.session_id COLLATE "C"),''::bytea) AS b,
        count(*) AS n
       FROM sessions.session_erasure_replay_targets AS x WHERE x.manifest_id=p_manifest)
 SELECT encode(sha256(
    convert_to('rh-erase-replay-manifest/v1','UTF8')
  ||(SELECT string_agg(int4send(octet_length(convert_to(v,'UTF8')))||convert_to(v,'UTF8'),''::bytea ORDER BY o)
      FROM r, LATERAL (VALUES (0,r.capture_version::text),(1,r.manifest_id::text),(2,r.request_id),(3,r.org_id),(4,r.workspace_id),
                              (5,r.source),(6,r.requested_session_id)) AS v(o,v))
  ||int4send(octet_length(a.n::text))||convert_to(a.n::text,'UTF8')||a.b
  ||int4send(octet_length(t.n::text))||convert_to(t.n::text,'UTF8')||t.b),'hex')
 FROM a, t WHERE EXISTS(SELECT 1 FROM r)
$replay_digest$;

-- The size bound (equivalence review, finding 2). PostgreSQL caps one value at 1 GiB, and the digest hashes the whole
-- framed message as one bytea; origins per owner are not capped by 050000. So a manifest whose framed message would
-- exceed this source cap is never sealed: the witness stays unsealed (unproven) and the erase proceeds exactly as
-- before. The cap is far below the ceiling so that capture, seal and the reader's per-page recompute stay bounded.
CREATE OR REPLACE FUNCTION sessions.session_erasure_replay_digest_cap() RETURNS bigint
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pg_temp AS $replay_cap$
 SELECT 67108864::bigint -- 64 MiB of framed message
$replay_cap$;

-- The exact octet length of the digest's framed message, summed without building it (one value never materializes).
CREATE OR REPLACE FUNCTION sessions.session_erasure_replay_framed_bytes(p_manifest uuid) RETURNS bigint
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $replay_bytes$
 WITH r AS (SELECT * FROM sessions.session_erasure_replay_requests WHERE manifest_id=p_manifest),
 a AS (SELECT count(*) AS n,coalesce(sum(4+octet_length(convert_to(x.alias_session_id,'UTF8'))),0) AS b
       FROM sessions.session_erasure_replay_aliases AS x WHERE x.manifest_id=p_manifest),
 t AS (SELECT count(*) AS n,coalesce(sum(16+octet_length(convert_to(x.user_id,'UTF8'))+octet_length(convert_to(x.origin_id,'UTF8'))
         +octet_length(convert_to(x.source,'UTF8'))+octet_length(convert_to(x.session_id,'UTF8'))),0) AS b
       FROM sessions.session_erasure_replay_targets AS x WHERE x.manifest_id=p_manifest)
 SELECT (octet_length(convert_to('rh-erase-replay-manifest/v1','UTF8'))
   +(SELECT sum(4+octet_length(convert_to(v,'UTF8'))) FROM r, LATERAL (VALUES (r.capture_version::text),(r.manifest_id::text),(r.request_id),
       (r.org_id),(r.workspace_id),(r.source),(r.requested_session_id)) AS v(v))
   +4+octet_length(a.n::text)+a.b+4+octet_length(t.n::text)+t.b)::bigint
 FROM a, t WHERE EXISTS(SELECT 1 FROM r)
$replay_bytes$;

-- A child row (alias or target) may not join a sealed manifest. FOR SHARE on the parent conflicts with the seal's
-- FOR UPDATE, so a child insert racing a seal either commits first (and the seal's recount sees it) or waits and sees
-- the seal: each later statement takes a fresh READ COMMITTED snapshot. Both guards refuse any other isolation level.
CREATE OR REPLACE FUNCTION sessions.session_erasure_replay_child_guard() RETURNS trigger
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $replay_child$
BEGIN
 -- READ COMMITTED only (40001 otherwise): the check below must see a seal committed while this insert waited on the
 -- parent lock, which a REPEATABLE READ or SERIALIZABLE snapshot taken earlier would not.
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session erasure replay manifest: read-committed transaction required';END IF;
 PERFORM 1 FROM sessions.session_erasure_replay_requests WHERE manifest_id=NEW.manifest_id FOR SHARE;
 IF EXISTS(SELECT 1 FROM sessions.session_erasure_replay_completions WHERE manifest_id=NEW.manifest_id) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session erasure replay manifest: manifest is sealed';END IF;
 RETURN NEW;
END $replay_child$;

-- The seal recomputes everything it claims from the rows under the parent's FOR UPDATE lock: a nonempty, exactly
-- (owner, origin) x alias target set, the requested session among the aliases, and the digest.
CREATE OR REPLACE FUNCTION sessions.session_erasure_replay_seal_guard() RETURNS trigger
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $replay_seal$
DECLARE r sessions.session_erasure_replay_requests%ROWTYPE;v_aliases bigint;v_owners bigint;v_pairs bigint;v_targets bigint;
BEGIN
 -- READ COMMITTED only (40001 otherwise): the recount below must see every child committed before this seal took the
 -- parent lock, which a REPEATABLE READ or SERIALIZABLE snapshot taken earlier would not.
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session erasure replay manifest: read-committed transaction required';END IF;
 SELECT * INTO r FROM sessions.session_erasure_replay_requests WHERE manifest_id=NEW.manifest_id FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='23503',MESSAGE='session erasure replay manifest: no intent';END IF;
 SELECT count(*) INTO v_aliases FROM sessions.session_erasure_replay_aliases WHERE manifest_id=NEW.manifest_id;
 SELECT count(DISTINCT user_id),count(DISTINCT (user_id,origin_id)),count(*) INTO v_owners,v_pairs,v_targets
  FROM sessions.session_erasure_replay_targets WHERE manifest_id=NEW.manifest_id;
 IF v_targets=0 OR v_aliases=0
  OR NOT EXISTS(SELECT 1 FROM sessions.session_erasure_replay_aliases WHERE manifest_id=NEW.manifest_id
    AND alias_session_id=r.requested_session_id)
  -- Every (owner, origin) pair names every alias: with the primary key and the alias foreign key, the count is enough.
  OR v_targets<>v_pairs*v_aliases
  OR (NEW.alias_count,NEW.owner_count,NEW.pair_count,NEW.target_count)IS DISTINCT FROM(v_aliases,v_owners,v_pairs,v_targets)
  OR NEW.derivation_version IS DISTINCT FROM r.capture_version
  OR sessions.session_erasure_replay_framed_bytes(NEW.manifest_id)>sessions.session_erasure_replay_digest_cap()
  OR NEW.manifest_sha256 IS DISTINCT FROM sessions.session_erasure_replay_digest(NEW.manifest_id) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session erasure replay manifest: seal does not match its rows';END IF;
 RETURN NEW;
END $replay_seal$;

DO $replay_triggers$
DECLARE t text;
BEGIN
 FOREACH t IN ARRAY ARRAY['session_erasure_replay_requests','session_erasure_replay_aliases','session_erasure_replay_targets',
   'session_erasure_replay_completions'] LOOP
  EXECUTE format('DROP TRIGGER IF EXISTS session_erasure_replay_guard ON sessions.%I',t);
  EXECUTE format('DROP TRIGGER IF EXISTS session_erasure_replay_truncate ON sessions.%I',t);
  EXECUTE format('CREATE TRIGGER session_erasure_replay_guard BEFORE UPDATE OR DELETE ON sessions.%I FOR EACH ROW EXECUTE FUNCTION sessions.session_erasure_replay_guard()',t);
  EXECUTE format('CREATE TRIGGER session_erasure_replay_truncate BEFORE TRUNCATE ON sessions.%I FOR EACH STATEMENT EXECUTE FUNCTION sessions.session_erasure_replay_guard()',t);
 END LOOP;
 FOREACH t IN ARRAY ARRAY['session_erasure_replay_aliases','session_erasure_replay_targets'] LOOP
  EXECUTE format('DROP TRIGGER IF EXISTS session_erasure_replay_unsealed ON sessions.%I',t);
  EXECUTE format('CREATE TRIGGER session_erasure_replay_unsealed BEFORE INSERT ON sessions.%I FOR EACH ROW EXECUTE FUNCTION sessions.session_erasure_replay_child_guard()',t);
 END LOOP;
 DROP TRIGGER IF EXISTS session_erasure_replay_seal ON sessions.session_erasure_replay_completions;
 CREATE TRIGGER session_erasure_replay_seal BEFORE INSERT ON sessions.session_erasure_replay_completions
  FOR EACH ROW EXECUTE FUNCTION sessions.session_erasure_replay_seal_guard();
END $replay_triggers$;

-- The 050000 fence body, with one addition: given an eligible manifest, the target set T is materialized ONCE after
-- owner-lock convergence, sealed, and then drives the destructive selection loop. With p_manifest NULL the body is the
-- legacy derivation, statement for statement. Both paths select the same (owner, origin) pairs in the same order.
CREATE FUNCTION sessions.session_erasure_replay_fence(p_org text,p_ws text,p_source text,p_session text,p_ids text[],p_manifest uuid) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $replay_fence$
DECLARE v_locked text[]:='{}';v_users text[];v_user text;v_origin text;v_pairs jsonb;v_round integer:=0;
 v_ledger bigint:=0;v_pointers bigint:=0;v_dirs bigint:=0;v_n bigint;v_live boolean;v_artifacts jsonb;
 v_manifest uuid:=p_manifest;v_targets bigint;v_owner_origins bigint;
 c_erased_revision CONSTANT bigint:=9007199254740991;
 c_turn_origin CONSTANT text:='relayhistory:conversation_turns:v1';
 c_max_owners CONSTANT integer:=64;
BEGIN
 v_pairs:=(SELECT jsonb_agg(jsonb_build_array(p_source,i) ORDER BY i COLLATE "C") FROM unnest(p_ids) AS i);
 -- Lock, then read ownership again under the locks: an owner whose claim committed between the
 -- read and the lock is caught by the next round. Locks are taken in user id order per round.
 LOOP
  v_round:=v_round+1;
  v_users:=sessions.ingest_operator_erase_owners(p_org,p_ws,p_source,p_ids);
  IF cardinality(v_users)>c_max_owners THEN
   RAISE EXCEPTION USING ERRCODE='RH010',MESSAGE='operator erase: owner cap exceeded';END IF;
  EXIT WHEN v_users<@v_locked;
  IF v_round>3 THEN RAISE EXCEPTION USING ERRCODE='RH010',MESSAGE='operator erase: owners did not settle';END IF;
  FOREACH v_user IN ARRAY v_users LOOP
   IF NOT v_user=ANY(v_locked) THEN
    -- A newly visible owner sorting before a held lock would reverse the global lock order.
    -- Abort this attempt rather than deadlock with another erase; its session fence is kept.
    IF EXISTS(SELECT 1 FROM unnest(v_locked) AS held WHERE held COLLATE "C">v_user COLLATE "C") THEN
     RAISE EXCEPTION USING ERRCODE='RH010',MESSAGE='operator erase: owner lock order changed';END IF;
    PERFORM sessions.ingest_lock_fences(p_org,p_ws,v_user);
    v_locked:=v_locked||v_user;
   END IF;
  END LOOP;
 END LOOP;
 -- Capture (062360): only when the intent's original alias declaration is exactly this call's closure, and the
 -- converged owner set is nonempty (an empty set is never sealed: design section 2).
 IF v_manifest IS NOT NULL AND (cardinality(v_users)=0 OR ARRAY(SELECT x.alias_session_id FROM sessions.session_erasure_replay_aliases AS x
    WHERE x.manifest_id=v_manifest ORDER BY x.alias_session_id COLLATE "C") IS DISTINCT FROM
    ARRAY(SELECT i FROM unnest(p_ids) AS i ORDER BY i COLLATE "C")) THEN
  v_manifest:=NULL;
 END IF;
 IF v_manifest IS NOT NULL THEN
  INSERT INTO sessions.session_erasure_replay_targets(manifest_id,org_id,workspace_id,user_id,origin_id,source,session_id)
  SELECT v_manifest,p_org,p_ws,u.user_id,o.origin_id,p_source,i
   FROM unnest(v_users) AS u(user_id)
   CROSS JOIN LATERAL (SELECT q.origin_id FROM sessions.ingest_session_ownership AS q
     WHERE q.org_id=p_org AND q.workspace_id=p_ws AND q.user_id=u.user_id AND q.source=p_source AND q.session_id=ANY(p_ids)
    UNION SELECT c_turn_origin) AS o
   CROSS JOIN unnest(p_ids) AS i;
  GET DIAGNOSTICS v_targets=ROW_COUNT;
  v_owner_origins:=v_targets/cardinality(p_ids);
 END IF;
 -- Over the size bound: no seal (unproven), and the legacy derivation below, exactly as before.
 IF v_manifest IS NOT NULL AND sessions.session_erasure_replay_framed_bytes(v_manifest)>sessions.session_erasure_replay_digest_cap() THEN
  v_manifest:=NULL;
 END IF;
 IF v_manifest IS NOT NULL THEN
  INSERT INTO sessions.session_erasure_replay_completions(manifest_id,org_id,workspace_id,source,derivation_version,
    alias_count,owner_count,pair_count,target_count,manifest_sha256)
  VALUES(v_manifest,p_org,p_ws,p_source,1,cardinality(p_ids),cardinality(v_users),v_owner_origins,v_targets,
    sessions.session_erasure_replay_digest(v_manifest));
  -- The selection is driven by the sealed set itself, in the legacy order (owner, then origin, bytewise).
  FOR v_user,v_origin IN
   SELECT t.user_id,t.origin_id FROM sessions.session_erasure_replay_targets AS t WHERE t.manifest_id=v_manifest
   GROUP BY t.user_id,t.origin_id ORDER BY t.user_id COLLATE "C",t.origin_id COLLATE "C"
  LOOP
   v_ledger:=v_ledger+sessions.ingest_apply_selection(p_org,p_ws,v_user,v_origin,v_pairs,false);
  END LOOP;
 ELSE
  FOREACH v_user IN ARRAY v_users LOOP
   FOR v_origin IN
    SELECT u.origin_id FROM (SELECT o.origin_id FROM sessions.ingest_session_ownership AS o
      WHERE o.org_id=p_org AND o.workspace_id=p_ws AND o.user_id=v_user AND o.source=p_source AND o.session_id=ANY(p_ids)
     UNION SELECT c_turn_origin) AS u
    ORDER BY u.origin_id COLLATE "C"
   LOOP
    -- Same owner-only selection primitive as the public ingest entry point, after
    -- this helper's complete tenant validation, ordered-mode refusal and owner locks.
    v_ledger:=v_ledger+sessions.ingest_apply_selection(p_org,p_ws,v_user,v_origin,v_pairs,false);
   END LOOP;
  END LOOP;
 END IF;
 -- Every copy's pointer row: the marker revision keeps upsert_session_index from undeleting it.
 UPDATE sessions.session_index AS s SET deleted_at=coalesce(s.deleted_at,clock_timestamp()),indexed_at=clock_timestamp(),
   revision=greatest(s.revision,c_erased_revision)
  WHERE s.org_id=p_org AND s.workspace_id=p_ws AND s.harness=p_source
    AND (s.session_id=ANY(p_ids) OR s.canonical_session_id=ANY(p_ids));
 GET DIAGNOSTICS v_pointers=ROW_COUNT;
 -- A guarded pointer (a live managed publication head) refuses the write by skipping it; it stays
 -- pending for the publication retirement path rather than being reported done.
 v_live:=EXISTS(SELECT 1 FROM sessions.session_index AS s WHERE s.org_id=p_org AND s.workspace_id=p_ws AND s.harness=p_source
   AND (s.session_id=ANY(p_ids) OR s.canonical_session_id=ANY(p_ids))
   AND (s.deleted_at IS NULL OR s.revision<c_erased_revision));
 UPDATE sessions.session_directory AS d SET short_summary=NULL,short_summary_scrub_ruleset=NULL,short_summary_scrub_version=NULL,
   short_summary_source=NULL,short_summary_generated_at=NULL,
   search_text=sessions.session_directory_search_text(d.title,d.task_title,d.agent_name,d.project_id,d.git_repo,d.git_branch,d.model,NULL),deleted_at=coalesce(d.deleted_at,clock_timestamp())
  WHERE d.org_id=p_org AND d.workspace_id=p_ws AND d.harness=p_source AND d.session_id=ANY(p_ids);
 GET DIAGNOSTICS v_dirs=ROW_COUNT;
 SELECT artifacts INTO STRICT v_artifacts FROM sessions.session_erasure_receipts
  WHERE org_id=p_org AND workspace_id=p_ws AND source=p_source AND session_id=p_session;
 v_artifacts:=v_artifacts
  ||jsonb_build_object('pointerRows',jsonb_build_object('count',v_pointers,'state',CASE WHEN v_live THEN 'pending' ELSE 'done' END))
  ||jsonb_build_object('ledgerRows',jsonb_build_object('count',v_ledger,'state','done'))
  ||jsonb_build_object('directoryRows',jsonb_build_object('count',v_dirs,'state','done'));
 UPDATE sessions.session_erasure_receipts SET artifacts=v_artifacts,uploads_fenced=true,updated_at=clock_timestamp()
  WHERE org_id=p_org AND workspace_id=p_ws AND source=p_source AND session_id=p_session;
 v_n:=cardinality(v_users);
 INSERT INTO sessions.session_erasure_events(org_id,workspace_id,source,session_id,request_id,kind,count)
 SELECT p_org,p_ws,p_source,p_session,r.request_id,'fenced',v_n FROM sessions.session_erasure_receipts AS r
  WHERE r.org_id=p_org AND r.workspace_id=p_ws AND r.source=p_source AND r.session_id=p_session;
END $replay_fence$;

-- sessions.ingest_operator_erase_fence: latest body from 20261105050000_operator_session_erase.sql, now the legacy
-- (uncaptured) path of the single fence body above. -- 062360 session erasure replay manifest
CREATE OR REPLACE FUNCTION sessions.ingest_operator_erase_fence(p_org text,p_ws text,p_source text,p_session text,p_ids text[]) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $fence$
BEGIN
 PERFORM sessions.session_erasure_replay_fence(p_org,p_ws,p_source,p_session,p_ids,NULL); -- 062360 session erasure replay manifest
END $fence$;

-- sessions.ingest_operator_erase_session: latest body from 20261105050000_operator_session_erase.sql, plus 062320's marked
-- erasure check first (it records an erasure; re-taking the org key SHARED that ingest_operator_runtime_begin already
-- holds is reentrant, and a direct owner call now takes it before the global gate, as the runtime entry does), the
-- intent (first attempt only) and the captured fence.
CREATE OR REPLACE FUNCTION sessions.ingest_operator_erase_session(p_org text,p_ws text,p_source text,p_session text,
  p_request text,p_actor_kind text,p_actor_id text) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $operator_erase$
DECLARE v_org text;v_ws text;v_source text;v_session text;v_request text;v_actor text;
 r sessions.session_erasure_receipts%ROWTYPE;v_ids text[];v_code text;
 v_fresh boolean:=false;v_manifest uuid; -- 062360 session erasure replay manifest
BEGIN
 PERFORM sessions.ingest_org_admission_hold_check(p_org,'erasure'); -- 062320 org admission hold
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='operator erase: read-committed transaction required';END IF;
 v_org:=sessions.ingest_identifier(to_jsonb(p_org),false);
 v_ws:=sessions.ingest_identifier(to_jsonb(p_ws),true);
 v_source:=sessions.ingest_identifier(to_jsonb(p_source),false);
 v_session:=sessions.ingest_identifier(to_jsonb(p_session),false);
 v_request:=sessions.ingest_identifier(to_jsonb(p_request),false);
 v_actor:=sessions.ingest_identifier(to_jsonb(p_actor_id),false);
 IF p_actor_kind IS NULL OR p_actor_kind NOT IN('user','bootstrap') THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='operator erase: actor kind is malformed';END IF;
 IF EXISTS(SELECT 1 FROM sessions.session_erasure_receipts AS x WHERE x.request_id=v_request
   AND (x.org_id,x.workspace_id,x.source,x.session_id) IS DISTINCT FROM (v_org,v_ws,v_source,v_session)) THEN
  RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='operator erase: request id belongs to another session';END IF;

 -- Global statement gate: writers share it before row locks; an erase owns it exclusively.
 -- This covers direct UPDATE and INSERT/ON CONFLICT as well as the composed consumer.
 PERFORM pg_advisory_xact_lock(961996,50000);
 -- Reject lossy alias collisions and oversized closure before receipt/fence writes.
 v_ids:=sessions.ingest_operator_erase_ids(v_org,v_ws,v_source,v_session);
 -- A repeat (any request id) answers from the existing receipt; only a failed one is retried.
 SELECT * INTO r FROM sessions.session_erasure_receipts
  WHERE org_id=v_org AND workspace_id=v_ws AND source=v_source AND session_id=v_session FOR UPDATE;
 IF FOUND AND r.status<>'failed' THEN
  INSERT INTO sessions.session_erasure_events(org_id,workspace_id,source,session_id,request_id,kind,actor_kind,actor_id)
  VALUES(v_org,v_ws,v_source,v_session,v_request,'repeated',p_actor_kind,v_actor);
  RETURN sessions.ingest_operator_erase_render(v_org,v_ws,v_source,v_session);
 END IF;

 IF sessions.ingest_operator_erase_ordered(v_org) THEN
  RAISE EXCEPTION USING ERRCODE='RH011',MESSAGE='operator erase: lazy, buffered or ordered org is not supported';END IF;

 IF FOUND THEN
  UPDATE sessions.session_erasure_receipts SET status='in_progress',failure=NULL,attempts=attempts+1,updated_at=clock_timestamp()
   WHERE org_id=v_org AND workspace_id=v_ws AND source=v_source AND session_id=v_session;
  INSERT INTO sessions.session_erasure_events(org_id,workspace_id,source,session_id,request_id,kind,actor_kind,actor_id)
  VALUES(v_org,v_ws,v_source,v_session,v_request,'retried',p_actor_kind,v_actor);
 ELSE
  -- The primary key decides a race between two first calls: the loser waits, then repeats.
  INSERT INTO sessions.session_erasure_receipts(org_id,workspace_id,source,session_id,request_id,artifacts,actor_kind,actor_id)
  SELECT v_org,v_ws,v_source,v_session,v_request,
    jsonb_object_agg(k,jsonb_build_object('count',0,'state','pending')),p_actor_kind,v_actor
   FROM unnest(sessions.ingest_operator_erase_artifact_keys()) AS k
  ON CONFLICT ON CONSTRAINT session_erasure_receipts_pkey DO NOTHING;
  IF NOT FOUND THEN
   INSERT INTO sessions.session_erasure_events(org_id,workspace_id,source,session_id,request_id,kind,actor_kind,actor_id)
   VALUES(v_org,v_ws,v_source,v_session,v_request,'repeated',p_actor_kind,v_actor);
   RETURN sessions.ingest_operator_erase_render(v_org,v_ws,v_source,v_session);
  END IF;
  v_fresh:=true; -- 062360 session erasure replay manifest
  INSERT INTO sessions.session_erasure_events(org_id,workspace_id,source,session_id,request_id,kind,actor_kind,actor_id)
  VALUES(v_org,v_ws,v_source,v_session,v_request,'requested',p_actor_kind,v_actor);
 END IF;
 SELECT * INTO STRICT r FROM sessions.session_erasure_receipts
  WHERE org_id=v_org AND workspace_id=v_ws AND source=v_source AND session_id=v_session;

 -- Resolve aliases before writing any fence.
 v_ids:=sessions.ingest_operator_erase_ids(v_org,v_ws,v_source,v_session);
 -- 062360: an alias fenced BEFORE this call (by an erase without a manifest binding, or by another manifest) is proof
 -- debt this call cannot discharge from the owners that survive it: read under the gate, before the fences below.
 IF v_fresh AND EXISTS(SELECT 1 FROM sessions.session_erasure_fences AS x
   WHERE x.org_id=v_org AND x.workspace_id=v_ws AND x.source=v_source AND x.session_id=ANY(v_ids)) THEN
  v_fresh:=false; -- 062360 session erasure replay manifest
 END IF;
 -- First, and outside the block below: kept even when owner fencing fails.
 PERFORM nextval('sessions.session_erasure_writer_epoch'::regclass);
 INSERT INTO sessions.session_erasure_fences(org_id,workspace_id,source,session_id)
 SELECT v_org,v_ws,v_source,i FROM unnest(v_ids) AS i ON CONFLICT DO NOTHING;
 -- 062360: the capture intent and its original alias declaration, at the fences' boundary (kept by a caught RH010,
 -- never sealed by a later attempt). Bound to the accepted receipt's persisted request_id. Only the first attempt, only
 -- when none of its aliases was fenced before this call, and (implied, kept explicit) none is bound to another
 -- manifest: an overlapping or pre-fenced closure is never proof (design sections 5 and 12).
 IF v_fresh AND NOT EXISTS(SELECT 1 FROM sessions.session_erasure_replay_aliases AS x
   WHERE x.org_id=v_org AND x.workspace_id=v_ws AND x.source=v_source AND x.alias_session_id=ANY(v_ids)) THEN
  INSERT INTO sessions.session_erasure_replay_requests(request_id,org_id,workspace_id,source,requested_session_id,capture_version)
  VALUES(r.request_id,v_org,v_ws,v_source,v_session,1) RETURNING manifest_id INTO v_manifest;
  INSERT INTO sessions.session_erasure_replay_aliases(manifest_id,org_id,workspace_id,source,alias_session_id)
  SELECT v_manifest,v_org,v_ws,v_source,i FROM unnest(v_ids) AS i;
 END IF;

 IF NOT r.uploads_fenced THEN
  BEGIN
   PERFORM sessions.session_erasure_replay_fence(v_org,v_ws,v_source,v_session,v_ids,v_manifest); -- 062360 session erasure replay manifest
  EXCEPTION WHEN SQLSTATE 'RH010' THEN
   GET STACKED DIAGNOSTICS v_code=RETURNED_SQLSTATE;
   UPDATE sessions.session_erasure_receipts SET status='failed',
     failure=jsonb_build_object('step','fence','code',v_code,'retryable',true),updated_at=clock_timestamp()
    WHERE org_id=v_org AND workspace_id=v_ws AND source=v_source AND session_id=v_session;
   INSERT INTO sessions.session_erasure_events(org_id,workspace_id,source,session_id,request_id,kind,step,error_code)
   VALUES(v_org,v_ws,v_source,v_session,v_request,'fence_failed','fence',v_code);
   RETURN sessions.ingest_operator_erase_render(v_org,v_ws,v_source,v_session);
  END;
 END IF;
 PERFORM sessions.ingest_operator_erase_settle(v_org,v_ws,v_source,v_session);
 RETURN sessions.ingest_operator_erase_render(v_org,v_ws,v_source,v_session);
END $operator_erase$;

-- The owner reader (design section 6.1, wire v1). One page of the sealed set for the manifest the queried fence alias
-- resolves to, in bytewise keyset order, with the full binding, count and digest on every page. Anything short of a
-- valid seal is a closed unproven page with no targets. A plain SELECT: no write, lock or sequence, so it runs in a
-- READ ONLY transaction (the bridge's), and equally in a read-write one.
CREATE OR REPLACE FUNCTION sessions.session_erasure_replay_read(org text,workspace text,source text,fence_session text,
  after_cursor jsonb DEFAULT NULL,page_size integer DEFAULT 512) RETURNS jsonb
LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog,pg_temp AS $replay_read$
-- The design's parameter names (org, workspace, source) are also column names: every column reference is qualified.
#variable_conflict use_variable
DECLARE v_alias sessions.session_erasure_replay_aliases%ROWTYPE;r sessions.session_erasure_replay_requests%ROWTYPE;
 c sessions.session_erasure_replay_completions%ROWTYPE;v_receipt text;v_aliases text[];v_count bigint;v_sha text;
 v_after jsonb;v_targets jsonb;v_n integer;v_last jsonb;v_more boolean;
BEGIN
 -- Identifiers as the erase accepts them (sessions.ingest_identifier): text, at most 512 octets, no control characters,
 -- non-empty except the workspace.
 IF page_size IS NULL OR page_size NOT BETWEEN 1 AND 512
  OR EXISTS(SELECT 1 FROM unnest(ARRAY[org,workspace,source,fence_session]) WITH ORDINALITY AS i(v,o)
    WHERE i.v IS NULL OR (i.v='' AND i.o<>2) OR octet_length(i.v)>512 OR i.v~'[\x01-\x1f\x7f]')
  OR (after_cursor IS NOT NULL AND jsonb_typeof(after_cursor)<>'object') THEN
  RETURN sessions.session_erasure_replay_unproven('reader_input_invalid');END IF;
 SELECT * INTO v_alias FROM sessions.session_erasure_replay_aliases AS x
  WHERE x.org_id=org AND x.workspace_id=workspace AND x.source=source AND x.alias_session_id=fence_session;
 IF NOT FOUND THEN RETURN sessions.session_erasure_replay_unproven('manifest_missing');END IF;
 SELECT * INTO r FROM sessions.session_erasure_replay_requests AS x WHERE x.manifest_id=v_alias.manifest_id;
 SELECT x.request_id INTO v_receipt FROM sessions.session_erasure_receipts AS x
  WHERE x.org_id=r.org_id AND x.workspace_id=r.workspace_id AND x.source=r.source AND x.session_id=r.requested_session_id;
 IF r.manifest_id IS NULL OR (r.org_id,r.workspace_id,r.source)IS DISTINCT FROM(org,workspace,source)
  OR v_receipt IS DISTINCT FROM r.request_id
  OR NOT EXISTS(SELECT 1 FROM sessions.session_erasure_fences AS x WHERE x.org_id=org AND x.workspace_id=workspace
    AND x.source=source AND x.session_id=fence_session) THEN
  RETURN sessions.session_erasure_replay_unproven('manifest_binding_invalid');END IF;
 IF r.capture_version<>1 THEN RETURN sessions.session_erasure_replay_unproven('manifest_format_unsupported');END IF;
 SELECT * INTO c FROM sessions.session_erasure_replay_completions AS x WHERE x.manifest_id=r.manifest_id;
 IF NOT FOUND THEN RETURN sessions.session_erasure_replay_unproven('manifest_unsealed');END IF;
 IF (c.org_id,c.workspace_id,c.source)IS DISTINCT FROM(r.org_id,r.workspace_id,r.source) THEN
  RETURN sessions.session_erasure_replay_unproven('manifest_binding_invalid');END IF;
 IF c.derivation_version<>1 THEN RETURN sessions.session_erasure_replay_unproven('manifest_format_unsupported');END IF;
 -- Integrity on every page: the alias declaration, the counts and the full digest, recomputed from the rows.
 v_aliases:=ARRAY(SELECT x.alias_session_id FROM sessions.session_erasure_replay_aliases AS x WHERE x.manifest_id=r.manifest_id
   ORDER BY x.alias_session_id COLLATE "C");
 SELECT count(*) INTO v_count FROM sessions.session_erasure_replay_targets AS x WHERE x.manifest_id=r.manifest_id;
 v_sha:=sessions.session_erasure_replay_digest(r.manifest_id);
 IF v_count=0 OR c.target_count=0 THEN RETURN sessions.session_erasure_replay_unproven('manifest_empty_unproven');END IF;
 IF cardinality(v_aliases) NOT BETWEEN 1 AND 256 OR cardinality(v_aliases)<>c.alias_count OR v_count<>c.target_count
  OR NOT r.requested_session_id=ANY(v_aliases) OR v_sha IS DISTINCT FROM c.manifest_sha256
  OR EXISTS(SELECT 1 FROM sessions.session_erasure_replay_targets AS x WHERE x.manifest_id=r.manifest_id
    AND (x.source<>r.source OR NOT x.session_id=ANY(v_aliases))) THEN
  RETURN sessions.session_erasure_replay_unproven('manifest_integrity_invalid');END IF;
 -- The cursor: exactly {v, manifest_id, manifest_sha256, after{4 keys}}, this manifest and digest, and naming a target
 -- of this set that is not its last (the terminal page carries no cursor).
 IF after_cursor IS NOT NULL THEN
  v_after:=after_cursor->'after';
  IF (SELECT array_agg(k ORDER BY k) FROM jsonb_object_keys(after_cursor) AS k) IS DISTINCT FROM ARRAY['after','manifest_id','manifest_sha256','v']
   OR after_cursor->'v' IS DISTINCT FROM '1'::jsonb
   OR after_cursor->'manifest_id' IS DISTINCT FROM to_jsonb(r.manifest_id::text)
   OR after_cursor->'manifest_sha256' IS DISTINCT FROM to_jsonb(c.manifest_sha256)
   OR jsonb_typeof(v_after) IS DISTINCT FROM 'object'
   OR (SELECT array_agg(k ORDER BY k) FROM jsonb_object_keys(v_after) AS k) IS DISTINCT FROM ARRAY['origin_id','session_id','source','user_id']
   OR EXISTS(SELECT 1 FROM jsonb_each(v_after) AS e WHERE jsonb_typeof(e.value)<>'string')
   OR NOT EXISTS(SELECT 1 FROM sessions.session_erasure_replay_targets AS x WHERE x.manifest_id=r.manifest_id
     AND x.user_id=v_after->>'user_id' AND x.origin_id=v_after->>'origin_id' AND x.source=v_after->>'source'
     AND x.session_id=v_after->>'session_id')
   OR NOT EXISTS(SELECT 1 FROM sessions.session_erasure_replay_targets AS x WHERE x.manifest_id=r.manifest_id
     AND (x.user_id COLLATE "C",x.origin_id COLLATE "C",x.source COLLATE "C",x.session_id COLLATE "C")
       >(v_after->>'user_id' COLLATE "C",v_after->>'origin_id' COLLATE "C",v_after->>'source' COLLATE "C",v_after->>'session_id' COLLATE "C")) THEN
   RETURN sessions.session_erasure_replay_unproven('manifest_cursor_invalid');END IF;
 END IF;
 -- One parameterized keyset SELECT: page_size+1 rows tell whether a later page exists.
 SELECT coalesce(jsonb_agg(jsonb_build_object('user_id',p.user_id,'origin_id',p.origin_id,'source',p.source,'session_id',p.session_id)
   ORDER BY p.user_id COLLATE "C",p.origin_id COLLATE "C",p.source COLLATE "C",p.session_id COLLATE "C") FILTER (WHERE p.rn<=page_size),'[]'::jsonb),
   count(*)>page_size
  INTO v_targets,v_more
  FROM (SELECT x.user_id,x.origin_id,x.source,x.session_id,
     row_number() OVER (ORDER BY x.user_id COLLATE "C",x.origin_id COLLATE "C",x.source COLLATE "C",x.session_id COLLATE "C") AS rn
    FROM sessions.session_erasure_replay_targets AS x WHERE x.manifest_id=r.manifest_id
     AND (v_after IS NULL OR (x.user_id COLLATE "C",x.origin_id COLLATE "C",x.source COLLATE "C",x.session_id COLLATE "C")
       >(v_after->>'user_id' COLLATE "C",v_after->>'origin_id' COLLATE "C",v_after->>'source' COLLATE "C",v_after->>'session_id' COLLATE "C"))
    ORDER BY x.user_id COLLATE "C",x.origin_id COLLATE "C",x.source COLLATE "C",x.session_id COLLATE "C"
    LIMIT page_size+1) AS p;
 v_n:=jsonb_array_length(v_targets);
 IF v_n=0 THEN RETURN sessions.session_erasure_replay_unproven('manifest_cursor_invalid');END IF;
 v_last:=v_targets->(v_n-1);
 RETURN jsonb_build_object('v',1,'complete',true,'reason','complete','manifest_id',r.manifest_id::text,'request_id',r.request_id,
  'scope',jsonb_build_object('org_id',r.org_id,'workspace_id',r.workspace_id,'source',r.source,'requested_session_id',r.requested_session_id),
  'capture_version',1,'alias_ids',to_jsonb(v_aliases),'target_count',c.target_count::text,'manifest_sha256',c.manifest_sha256,
  'targets',v_targets,
  'next_cursor',CASE WHEN v_more THEN jsonb_build_object('v',1,'manifest_id',r.manifest_id::text,'manifest_sha256',c.manifest_sha256,'after',v_last)
   ELSE 'null'::jsonb END);
END $replay_read$;

-- The closed unproven page: every key present, no identity, no targets, no cursor.
CREATE OR REPLACE FUNCTION sessions.session_erasure_replay_unproven(p_reason text) RETURNS jsonb
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pg_temp AS $replay_unproven$
 SELECT jsonb_build_object('v',1,'complete',false,'reason',p_reason,'manifest_id',NULL,'request_id',NULL,'scope',NULL,
  'capture_version',NULL,'alias_ids','[]'::jsonb,'target_count',NULL,'manifest_sha256',NULL,'targets','[]'::jsonb,'next_cursor',NULL)
$replay_unproven$;

-- The retained witness's exact shape: owner, RLS, columns, constraints (NOT NULL is the column flag; PostgreSQL 18's
-- separate not-null constraint rows are not counted), indexes, triggers, plus the receipts binding constraint the
-- intent's foreign key needs. A reapply over a retained manifest (this slot's down keeps a populated one) must find
-- exactly this, else 55000 and nothing applied; the down verifies it before removing its ledger row.
CREATE OR REPLACE FUNCTION sessions.session_erasure_replay_shape() RETURNS text
LANGUAGE sql STABLE SET search_path=pg_catalog,pg_temp AS $replay_shape$
 SELECT string_agg(c.relname
   ||E'\n owner='||CASE WHEN c.relowner=(SELECT r.relowner FROM pg_class r WHERE r.oid='sessions.session_erasure_receipts'::regclass) THEN 'erase' ELSE 'other' END
   ||' rls='||c.relrowsecurity::text||' force='||c.relforcerowsecurity::text
   ||E'\n cols='||(SELECT string_agg(a.attname||' '||format_type(a.atttypid,a.atttypmod)||CASE WHEN a.attnotnull THEN ' NOT NULL' ELSE '' END
       ||coalesce(' DEFAULT '||pg_get_expr(d.adbin,d.adrelid),''),', ' ORDER BY a.attnum)
     FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped)
   ||E'\n cons='||(SELECT string_agg(k.conname||' '||pg_get_constraintdef(k.oid),'; ' ORDER BY k.conname)
     FROM pg_constraint k WHERE k.conrelid=c.oid AND k.contype<>'n')
   ||E'\n idx='||(SELECT string_agg(pg_get_indexdef(x.indexrelid),'; ' ORDER BY pg_get_indexdef(x.indexrelid)) FROM pg_index x WHERE x.indrelid=c.oid)
   ||E'\n trg='||coalesce((SELECT string_agg(pg_get_triggerdef(t.oid),'; ' ORDER BY t.tgname) FROM pg_trigger t WHERE t.tgrelid=c.oid AND NOT t.tgisinternal),''),
   E'\n' ORDER BY c.relname)
  ||E'\nreceipts='||coalesce((SELECT pg_get_constraintdef(k.oid) FROM pg_constraint k WHERE k.conrelid='sessions.session_erasure_receipts'::regclass
     AND k.conname='session_erasure_receipts_request_binding'),'missing')
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='sessions' AND c.relkind='r' AND c.relname IN('session_erasure_replay_requests','session_erasure_replay_aliases',
   'session_erasure_replay_targets','session_erasure_replay_completions')
$replay_shape$;

-- A new or retained manifest must be exactly the reviewed one (after the triggers above are recreated).
DO $replay_shape_check$
BEGIN
 IF sessions.session_erasure_replay_shape() IS DISTINCT FROM
-- replay manifest shape begin
$shape$session_erasure_replay_aliases
 owner=erase rls=true force=false
 cols=manifest_id uuid NOT NULL, org_id text NOT NULL, workspace_id text NOT NULL, source text NOT NULL, alias_session_id text NOT NULL
 cons=session_erasure_replay_aliases_fence_fk FOREIGN KEY (org_id, workspace_id, source, alias_session_id) REFERENCES sessions.session_erasure_fences(org_id, workspace_id, source, session_id); session_erasure_replay_aliases_member UNIQUE (org_id, workspace_id, source, alias_session_id, manifest_id); session_erasure_replay_aliases_pkey PRIMARY KEY (org_id, workspace_id, source, alias_session_id); session_erasure_replay_aliases_request_fk FOREIGN KEY (manifest_id, org_id, workspace_id, source) REFERENCES sessions.session_erasure_replay_requests(manifest_id, org_id, workspace_id, source)
 idx=CREATE INDEX session_erasure_replay_aliases_manifest ON sessions.session_erasure_replay_aliases USING btree (manifest_id, alias_session_id COLLATE "C"); CREATE UNIQUE INDEX session_erasure_replay_aliases_member ON sessions.session_erasure_replay_aliases USING btree (org_id, workspace_id, source, alias_session_id, manifest_id); CREATE UNIQUE INDEX session_erasure_replay_aliases_pkey ON sessions.session_erasure_replay_aliases USING btree (org_id, workspace_id, source, alias_session_id)
 trg=CREATE TRIGGER session_erasure_replay_guard BEFORE DELETE OR UPDATE ON sessions.session_erasure_replay_aliases FOR EACH ROW EXECUTE FUNCTION sessions.session_erasure_replay_guard(); CREATE TRIGGER session_erasure_replay_truncate BEFORE TRUNCATE ON sessions.session_erasure_replay_aliases FOR EACH STATEMENT EXECUTE FUNCTION sessions.session_erasure_replay_guard(); CREATE TRIGGER session_erasure_replay_unsealed BEFORE INSERT ON sessions.session_erasure_replay_aliases FOR EACH ROW EXECUTE FUNCTION sessions.session_erasure_replay_child_guard()
session_erasure_replay_completions
 owner=erase rls=true force=false
 cols=manifest_id uuid NOT NULL, org_id text NOT NULL, workspace_id text NOT NULL, source text NOT NULL, derivation_version smallint NOT NULL, alias_count integer NOT NULL, owner_count integer NOT NULL, pair_count bigint NOT NULL, target_count bigint NOT NULL, manifest_sha256 text NOT NULL, completed_at timestamp with time zone NOT NULL DEFAULT clock_timestamp()
 cons=session_erasure_replay_completions_alias_count_check CHECK (((alias_count >= 1) AND (alias_count <= 256))); session_erasure_replay_completions_check CHECK ((pair_count >= owner_count)); session_erasure_replay_completions_check1 CHECK (((target_count > 0) AND (target_count = (pair_count * alias_count)))); session_erasure_replay_completions_derivation_version_check CHECK ((derivation_version = 1)); session_erasure_replay_completions_manifest_sha256_check CHECK ((manifest_sha256 ~ '^[0-9a-f]{64}$'::text)); session_erasure_replay_completions_owner_count_check CHECK (((owner_count >= 1) AND (owner_count <= 64))); session_erasure_replay_completions_pkey PRIMARY KEY (manifest_id); session_erasure_replay_completions_request_fk FOREIGN KEY (manifest_id, org_id, workspace_id, source) REFERENCES sessions.session_erasure_replay_requests(manifest_id, org_id, workspace_id, source)
 idx=CREATE UNIQUE INDEX session_erasure_replay_completions_pkey ON sessions.session_erasure_replay_completions USING btree (manifest_id)
 trg=CREATE TRIGGER session_erasure_replay_guard BEFORE DELETE OR UPDATE ON sessions.session_erasure_replay_completions FOR EACH ROW EXECUTE FUNCTION sessions.session_erasure_replay_guard(); CREATE TRIGGER session_erasure_replay_seal BEFORE INSERT ON sessions.session_erasure_replay_completions FOR EACH ROW EXECUTE FUNCTION sessions.session_erasure_replay_seal_guard(); CREATE TRIGGER session_erasure_replay_truncate BEFORE TRUNCATE ON sessions.session_erasure_replay_completions FOR EACH STATEMENT EXECUTE FUNCTION sessions.session_erasure_replay_guard()
session_erasure_replay_requests
 owner=erase rls=true force=false
 cols=manifest_id uuid NOT NULL DEFAULT gen_random_uuid(), request_id text NOT NULL, org_id text NOT NULL, workspace_id text NOT NULL, source text NOT NULL, requested_session_id text NOT NULL, capture_version smallint NOT NULL, created_at timestamp with time zone NOT NULL DEFAULT clock_timestamp()
 cons=session_erasure_replay_requests_capture_version_check CHECK ((capture_version = 1)); session_erasure_replay_requests_pkey PRIMARY KEY (manifest_id); session_erasure_replay_requests_receipt UNIQUE (org_id, workspace_id, source, requested_session_id); session_erasure_replay_requests_receipt_fk FOREIGN KEY (org_id, workspace_id, source, requested_session_id, request_id) REFERENCES sessions.session_erasure_receipts(org_id, workspace_id, source, session_id, request_id); session_erasure_replay_requests_request UNIQUE (request_id); session_erasure_replay_requests_scope UNIQUE (manifest_id, org_id, workspace_id, source)
 idx=CREATE UNIQUE INDEX session_erasure_replay_requests_pkey ON sessions.session_erasure_replay_requests USING btree (manifest_id); CREATE UNIQUE INDEX session_erasure_replay_requests_receipt ON sessions.session_erasure_replay_requests USING btree (org_id, workspace_id, source, requested_session_id); CREATE UNIQUE INDEX session_erasure_replay_requests_request ON sessions.session_erasure_replay_requests USING btree (request_id); CREATE UNIQUE INDEX session_erasure_replay_requests_scope ON sessions.session_erasure_replay_requests USING btree (manifest_id, org_id, workspace_id, source)
 trg=CREATE TRIGGER session_erasure_replay_guard BEFORE DELETE OR UPDATE ON sessions.session_erasure_replay_requests FOR EACH ROW EXECUTE FUNCTION sessions.session_erasure_replay_guard(); CREATE TRIGGER session_erasure_replay_truncate BEFORE TRUNCATE ON sessions.session_erasure_replay_requests FOR EACH STATEMENT EXECUTE FUNCTION sessions.session_erasure_replay_guard()
session_erasure_replay_targets
 owner=erase rls=true force=false
 cols=manifest_id uuid NOT NULL, org_id text NOT NULL, workspace_id text NOT NULL, user_id text NOT NULL, origin_id text NOT NULL, source text NOT NULL, session_id text NOT NULL
 cons=session_erasure_replay_targets_alias_fk FOREIGN KEY (org_id, workspace_id, source, session_id, manifest_id) REFERENCES sessions.session_erasure_replay_aliases(org_id, workspace_id, source, alias_session_id, manifest_id); session_erasure_replay_targets_pkey PRIMARY KEY (manifest_id, user_id, origin_id, source, session_id); session_erasure_replay_targets_request_fk FOREIGN KEY (manifest_id, org_id, workspace_id, source) REFERENCES sessions.session_erasure_replay_requests(manifest_id, org_id, workspace_id, source)
 idx=CREATE UNIQUE INDEX session_erasure_replay_targets_order ON sessions.session_erasure_replay_targets USING btree (manifest_id, user_id COLLATE "C", origin_id COLLATE "C", source COLLATE "C", session_id COLLATE "C"); CREATE UNIQUE INDEX session_erasure_replay_targets_pkey ON sessions.session_erasure_replay_targets USING btree (manifest_id, user_id, origin_id, source, session_id)
 trg=CREATE TRIGGER session_erasure_replay_guard BEFORE DELETE OR UPDATE ON sessions.session_erasure_replay_targets FOR EACH ROW EXECUTE FUNCTION sessions.session_erasure_replay_guard(); CREATE TRIGGER session_erasure_replay_truncate BEFORE TRUNCATE ON sessions.session_erasure_replay_targets FOR EACH STATEMENT EXECUTE FUNCTION sessions.session_erasure_replay_guard(); CREATE TRIGGER session_erasure_replay_unsealed BEFORE INSERT ON sessions.session_erasure_replay_targets FOR EACH ROW EXECUTE FUNCTION sessions.session_erasure_replay_child_guard()
receipts=UNIQUE (org_id, workspace_id, source, session_id, request_id)$shape$
-- replay manifest shape end
 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session erasure replay manifest: retained manifest drifted; nothing applied';END IF;
END $replay_shape_check$;

-- Owner-only, from the catalog: every non-owner grantee on the four tables (and their columns) and on the slot's
-- functions is revoked, so no default or prior grant survives a down and reapply. No runtime grant is added.
DO $replay_acl$
DECLARE r record;
BEGIN
 FOR r IN SELECT c.relname,x.grantee FROM pg_class c,aclexplode(c.relacl) x
           WHERE c.oid IN('sessions.session_erasure_replay_requests'::regclass,'sessions.session_erasure_replay_aliases'::regclass,
             'sessions.session_erasure_replay_targets'::regclass,'sessions.session_erasure_replay_completions'::regclass)
             AND x.grantee<>c.relowner GROUP BY c.relname,x.grantee LOOP
   EXECUTE format('REVOKE ALL ON sessions.%I FROM %s',r.relname,CASE WHEN r.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(r.grantee)) END);
 END LOOP;
 FOR r IN SELECT c.relname,a.attname,x.grantee FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid,aclexplode(a.attacl) x
           WHERE a.attrelid IN('sessions.session_erasure_replay_requests'::regclass,'sessions.session_erasure_replay_aliases'::regclass,
             'sessions.session_erasure_replay_targets'::regclass,'sessions.session_erasure_replay_completions'::regclass)
             AND a.attnum>0 AND x.grantee<>c.relowner GROUP BY c.relname,a.attname,x.grantee LOOP
   EXECUTE format('REVOKE ALL (%I) ON sessions.%I FROM %s',r.attname,r.relname,CASE WHEN r.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(r.grantee)) END);
 END LOOP;
 FOR r IN SELECT p.oid::regprocedure AS fn,x.grantee FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace,
             aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) x
           WHERE n.nspname='sessions' AND p.proname LIKE 'session\_erasure\_replay\_%' AND x.grantee<>p.proowner GROUP BY p.oid,x.grantee LOOP
   EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %s',r.fn,CASE WHEN r.grantee=0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(r.grantee)) END);
 END LOOP;
END $replay_acl$;
