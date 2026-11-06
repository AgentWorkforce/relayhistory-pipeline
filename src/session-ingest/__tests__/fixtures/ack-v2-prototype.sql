-- TEST-ONLY PROTOTYPE of ledger design v8 Phase 1 (packed ACK vectors). NOT a migration.
-- Applied by src/session-ingest/__tests__/ack-v2-prototype.test.ts on top of supabase/migrations in a
-- disposable PGlite / PG17 database. Never applied to any deployed database. No migration number.
-- Design: relay-briefs ledger-out-of-postgres-design-20261106.md v8 (§8, §9, §10, §11, §12, §13, §14).

-- ---------- isolation contract (§9.3) ----------
CREATE FUNCTION sessions.ack_require_rc() RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: READ COMMITTED required';END IF;
END $$;

-- ---------- persisted mode (§8.1) ----------
CREATE TABLE sessions.ingest_ack_format(
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 epoch bigint NOT NULL DEFAULT 0, mode text NOT NULL DEFAULT 'v1' CHECK(mode IN('v1','v2')),
 cutover_seat bigint, released_at timestamptz NOT NULL DEFAULT clock_timestamp(), changed_at timestamptz);
INSERT INTO sessions.ingest_ack_format(singleton) VALUES(true);
-- format eras: every seat number belongs to exactly one era (first_seat..next era's first_seat-1); P2/T6c rollback support
CREATE TABLE sessions.ingest_ack_format_epochs(epoch bigint PRIMARY KEY,mode text NOT NULL CHECK(mode IN('v1','v2')),first_seat numeric NOT NULL CHECK(first_seat>=0 AND first_seat<=9223372036854775808),
 changed_at timestamptz NOT NULL DEFAULT clock_timestamp());
INSERT INTO sessions.ingest_ack_format_epochs VALUES(0,'v1',0);   -- genesis (before the guard exists)
CREATE FUNCTION sessions.ack_era_guard() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
DECLARE m sessions.ingest_ack_format%ROWTYPE;v_last bigint;v_prev numeric;
BEGIN
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: format eras are append-only';END IF;
 SELECT * INTO STRICT m FROM sessions.ingest_ack_format WHERE singleton;
 SELECT last_seat INTO STRICT v_last FROM sessions.ingest_receipt_seat_clock WHERE singleton;
 SELECT max(first_seat) INTO v_prev FROM sessions.ingest_ack_format_epochs;
 IF NEW.epoch<>m.epoch OR NEW.mode<>m.mode OR NEW.first_seat<>v_last::numeric+1 OR (v_prev IS NOT NULL AND NEW.first_seat<v_prev) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: format era must match the format row and clock';END IF;
 RETURN NEW;END $$;
CREATE FUNCTION sessions.ack_seat_era(p_seat bigint) RETURNS text LANGUAGE sql STABLE AS
$$ SELECT mode FROM sessions.ingest_ack_format_epochs WHERE first_seat<=p_seat::numeric ORDER BY first_seat DESC,epoch DESC LIMIT 1 $$;

-- ---------- parent discriminator (§9.1) ----------
ALTER TABLE sessions.ingest_claim_attempts ADD COLUMN ack_format smallint CHECK(ack_format IN(0,1,2));   -- 0 = legacy, no ACK rows

-- ---------- canonical preimages (§9.4) ----------
CREATE FUNCTION sessions.ack_f(p_tag int,p bytea) RETURNS bytea LANGUAGE sql IMMUTABLE AS
$$ SELECT CASE WHEN p IS NULL THEN '\x0000000000'::bytea ELSE set_byte('\x00'::bytea,0,p_tag)||int4send(octet_length(p))||p END $$;
CREATE FUNCTION sessions.ack_ft(p text) RETURNS bytea LANGUAGE sql IMMUTABLE AS $$ SELECT sessions.ack_f(1,convert_to(p,'UTF8')) $$;
CREATE FUNCTION sessions.ack_fi(p bigint) RETURNS bytea LANGUAGE sql IMMUTABLE AS $$ SELECT sessions.ack_f(2,int8send(p)) $$;
CREATE FUNCTION sessions.ack_fu(p uuid) RETURNS bytea LANGUAGE sql IMMUTABLE AS $$ SELECT sessions.ack_f(3,decode(replace(p::text,'-',''),'hex')) $$;
CREATE FUNCTION sessions.ack_fj(p jsonb) RETURNS bytea LANGUAGE sql IMMUTABLE AS $$ SELECT sessions.ack_f(4,convert_to(p::text,'UTF8')) $$;
CREATE FUNCTION sessions.ack_attempt_sha(a sessions.ingest_claim_attempts) RETURNS bytea LANGUAGE sql STABLE AS $$
 SELECT sha256(convert_to('96bulls/ack-attempt/v1','UTF8')||'\x00'::bytea||sessions.ack_fu(a.claim_token)||sessions.ack_ft(a.org_id)
  ||sessions.ack_ft(a.workspace_id)||sessions.ack_ft(a.user_id)||sessions.ack_ft(a.origin_id)||sessions.ack_ft(a.batch_id)||sessions.ack_ft(a.digest)
  ||sessions.ack_fi(a.reset_epoch)||sessions.ack_ft(a.schema_generation)||sessions.ack_fi(a.source_control_version)
  ||sessions.ack_ft(a.source_identity_sha256)||sessions.ack_ft(a.source_witness_sha256)||sessions.ack_fj(a.records)
  ||sessions.ack_fj(a.epochs)||sessions.ack_fj(a.source_processing_ranks)) $$;
CREATE FUNCTION sessions.ack_clock_sha() RETURNS bytea LANGUAGE sql STABLE AS $$
 SELECT sha256(convert_to('96bulls/ack-clock/v1','UTF8')||'\x00'::bytea||sessions.ack_ft(c.control_epoch)||sessions.ack_ft(c.source_identity_sha256)
  ||sessions.ack_ft(c.baseline->>'sourceProofSha256')||sessions.ack_ft(c.initial_generation)||sessions.ack_fi(c.allocated_high_water))
 FROM sessions.ingest_receipt_seat_clock c WHERE c.singleton $$;

-- ---------- processing ranks must be a total order: a permutation of 0..n-1 (also proved at claim time, lazy:277-281) ----------
CREATE FUNCTION sessions.ack_require_rank_permutation(p_ranks jsonb,p_n int) RETURNS void LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
 IF jsonb_typeof(p_ranks) IS DISTINCT FROM 'array' OR jsonb_array_length(p_ranks)<>p_n
  OR EXISTS(SELECT 1 FROM jsonb_array_elements(p_ranks) r WHERE CASE WHEN jsonb_typeof(r)<>'number' OR r::text!~'^(0|[1-9][0-9]{0,2})$' THEN true ELSE r::text::int>=p_n END)
  OR (SELECT count(DISTINCT r::text) FROM jsonb_array_elements(p_ranks) r)<>p_n THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: processing ranks are not a permutation';END IF;
END $$;

-- ---------- vector table (§9.4 inclusive numrange, §8.2.1) ----------
CREATE TABLE sessions.ingest_ack_vectors(
 claim_token uuid PRIMARY KEY REFERENCES sessions.ingest_claim_attempts(claim_token) ON DELETE RESTRICT,
 ack_stage_token uuid NOT NULL REFERENCES sessions.ingest_stage_intents(stage_token) ON DELETE RESTRICT,
 format_version smallint NOT NULL CHECK(format_version=1),
 n smallint NOT NULL CHECK(n BETWEEN 1 AND 500),
 allocated_count smallint NOT NULL CHECK(allocated_count BETWEEN 0 AND 500 AND allocated_count<=n),
 clock_before bigint NOT NULL CHECK(clock_before>=0),
 seat_lo bigint, seat_hi bigint,
 attempt_sha256 bytea NOT NULL CHECK(octet_length(attempt_sha256)=32),
 clock_identity_sha256 bytea NOT NULL CHECK(octet_length(clock_identity_sha256)=32),
 vec bytea NOT NULL CHECK(octet_length(vec) BETWEEN 6 AND 65536),
 vec_sha256 bytea NOT NULL CHECK(octet_length(vec_sha256)=32),
 acked_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK((allocated_count=0 AND seat_lo IS NULL AND seat_hi IS NULL)
  OR (allocated_count>0 AND seat_lo::numeric=clock_before::numeric+1 AND seat_hi::numeric-seat_lo::numeric+1=allocated_count)),
 EXCLUDE USING gist(numrange(seat_lo,seat_hi,'[]') WITH &&) WHERE(allocated_count>0));
ALTER TABLE sessions.ingest_ack_vectors ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON sessions.ingest_ack_vectors,sessions.ingest_ack_format FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

-- ---------- byte helpers (§8.2.2: big-endian, canonical LEB128) ----------
CREATE FUNCTION sessions.ack_u16(p int) RETURNS bytea LANGUAGE sql IMMUTABLE AS $$ SELECT substring(int4send(p) FROM 3 FOR 2) $$;
CREATE FUNCTION sessions.ack_varint(p bigint) RETURNS bytea LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE o bytea:=''::bytea;v bigint:=p;b int;
BEGIN
 IF v<0 THEN RAISE EXCEPTION USING ERRCODE='22003',MESSAGE='ack v2: negative varint';END IF;
 LOOP b:=(v & 127)::int;v:=v>>7;
  IF v<>0 THEN o:=o||set_byte('\x00'::bytea,0,b|128);ELSE o:=o||set_byte('\x00'::bytea,0,b);EXIT;END IF;END LOOP;
 RETURN o;
END $$;

-- ---------- decoder: full structural + hash verification (§8.2.3, §9.4, §11.1) ----------
CREATE FUNCTION sessions.ingest_ack_entries(p_claim uuid)
RETURNS TABLE(idx int,ord int,accepted boolean,allocated boolean,reused boolean,lineage bigint,seat bigint,
 owner_kind int,seat_key text,owner_claim uuid,owner_ord int,first_digest text,native_key text,record_id text,
 revision bigint,candidate_digest text,source text,session_id text,reset_epoch bigint,fence_epoch bigint,candidate_key text,key_material jsonb)
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $dec$
DECLARE v sessions.ingest_ack_vectors%ROWTYPE;a sessions.ingest_claim_attempts%ROWTYPE;t sessions.ingest_stage_intents%ROWTYPE;
 b bytea;pos int;len int;i int;f int;lin bigint;off bigint;sh int;x int;k int:=0;o_seat bigint;o_kind int;o_key text;o_claim uuid;o_ord int;fd text;
 j int;nret int:=0;ords int[]:='{}';e record;q_ord int[];q_key text[];q_km jsonb[];q_cd text[];q_re bigint[];q_fe bigint[];a_f int[]:='{}';a_lin bigint[]:='{}';a_off bigint[]:='{}';a_os bigint[]:='{}';a_ok int[]:='{}';a_key text[]:='{}';a_oc uuid[]:='{}';a_oo int[]:='{}';a_fd text[]:='{}';
BEGIN
 SELECT * INTO v FROM sessions.ingest_ack_vectors WHERE claim_token=p_claim;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: vector missing';END IF;
 SELECT * INTO STRICT a FROM sessions.ingest_claim_attempts WHERE claim_token=p_claim;
 SELECT * INTO STRICT t FROM sessions.ingest_stage_intents WHERE stage_token=v.ack_stage_token;
 b:=v.vec;len:=octet_length(b);
 IF sha256(b)<>v.vec_sha256 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: vector hash';END IF;
 IF sessions.ack_attempt_sha(a)<>v.attempt_sha256 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: attempt identity changed';END IF;
 IF sessions.ack_clock_sha()<>v.clock_identity_sha256 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: clock identity mismatch';END IF;
 IF a.ack_format IS DISTINCT FROM 2 OR t.claim_token<>p_claim THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: discriminator';END IF;
 PERFORM sessions.ack_require_rank_permutation(a.source_processing_ranks,jsonb_array_length(a.records));
 IF len<6 OR get_byte(b,0)<>1 OR get_byte(b,1)<>1 OR (get_byte(b,2)<<8|get_byte(b,3))<>v.n
  OR (get_byte(b,4)<<8|get_byte(b,5))<>v.allocated_count THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: header';END IF;
 pos:=6;
 FOR i IN 1..v.n LOOP
  IF pos+3>len THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: truncated';END IF;
  x:=get_byte(b,pos)<<8|get_byte(b,pos+1);f:=get_byte(b,pos+2);pos:=pos+3;
  IF x>499 OR x=ANY(ords) THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: ord';END IF;ords:=ords||x;
  IF (f & 248)<>0 OR ((f>>1)&1)+((f>>2)&1)<>1 OR ((f&1)=1 AND (f&2)=0) THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: flags';END IF;
  -- canonical LEB128
  lin:=0;sh:=0;LOOP
   IF pos>=len OR sh>56 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: varint';END IF;
   x:=get_byte(b,pos);pos:=pos+1;
   IF sh=56 AND x>127 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: varint';END IF;
   lin:=lin|((x&127)::bigint<<sh);
   IF x<128 THEN IF x=0 AND sh>0 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: noncanonical varint';END IF;EXIT;END IF;
   sh:=sh+7;END LOOP;
  off:=NULL;o_seat:=NULL;o_kind:=NULL;o_key:=NULL;o_claim:=NULL;o_ord:=NULL;fd:=NULL;
  IF (f&2)=2 THEN
   off:=0;sh:=0;LOOP
    IF pos>=len OR sh>14 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: offset';END IF;
    x:=get_byte(b,pos);pos:=pos+1;off:=off|((x&127)::bigint<<sh);
    IF x<128 THEN IF x=0 AND sh>0 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: noncanonical offset';END IF;EXIT;END IF;sh:=sh+7;END LOOP;
   k:=k+1;IF off<>k THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: offsets not 1..k';END IF;
  ELSE
   IF pos+9+32>len THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: truncated reuse';END IF;
   o_seat:=('x'||encode(substring(b FROM pos+1 FOR 8),'hex'))::bit(64)::bigint;pos:=pos+8;
   o_kind:=get_byte(b,pos);pos:=pos+1;
   o_key:=encode(substring(b FROM pos+1 FOR 32),'hex');pos:=pos+32;
   IF o_seat<1 OR o_kind NOT IN(1,2) THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: reused seat bounds';END IF;
   IF o_kind=2 THEN
    IF pos+18>len THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: truncated owner';END IF;
    o_claim:=encode(substring(b FROM pos+1 FOR 16),'hex')::uuid;pos:=pos+16;o_ord:=get_byte(b,pos)<<8|get_byte(b,pos+1);pos:=pos+2;
   END IF;
   IF o_kind=2 AND o_claim=p_claim THEN
    -- in-claim reuse (v1 in-loop overlay): must name an EARLIER entry of this vector that allocated this seat and was accepted
    IF NOT (o_ord=ANY(ords[1:i-1])) OR v.allocated_count=0 OR o_seat<v.seat_lo OR o_seat>v.seat_hi THEN
     RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: in-claim reuse must name an earlier allocation';END IF;
    j:=array_position(ords,o_ord);
    IF (a_f[j]&3)<>3 OR v.seat_lo+(a_off[j]-1)<>o_seat THEN
     RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: in-claim reuse must name an earlier accepted allocation';END IF;
   ELSIF (v.allocated_count>0 AND o_seat>=v.seat_lo) OR (v.allocated_count=0 AND o_seat>v.clock_before) THEN
    RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: reused seat bounds';END IF;
  END IF;
  IF (f&1)=0 THEN
   IF pos+32>len THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: truncated digest';END IF;
   fd:=encode(substring(b FROM pos+1 FOR 32),'hex');pos:=pos+32;END IF;
  a_f:=a_f||f;a_lin:=a_lin||lin;a_off:=a_off||off;a_os:=a_os||o_seat;a_ok:=a_ok||o_kind;a_key:=a_key||o_key;a_oc:=a_oc||o_claim;a_oo:=a_oo||o_ord;a_fd:=a_fd||fd;
 END LOOP;
 IF pos<>len THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: trailing bytes';END IF;
 IF k<>v.allocated_count THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: allocated count';END IF;
 -- exact ord set AND exact effective v1 visit order (source_processing_ranks of the immutable attempt): idx is the
 -- v1 processing order, so final_winners selected by idx are the v1 winners (§15)
 SELECT array_agg(q.ord ORDER BY (a.source_processing_ranks->>q.ord)::int),array_agg(q.key_sha256 ORDER BY (a.source_processing_ranks->>q.ord)::int),
  array_agg(q.key_material ORDER BY (a.source_processing_ranks->>q.ord)::int),array_agg(q.candidate_digest ORDER BY (a.source_processing_ranks->>q.ord)::int),
  array_agg(q.reset_epoch ORDER BY (a.source_processing_ranks->>q.ord)::int),array_agg(q.fence_epoch ORDER BY (a.source_processing_ranks->>q.ord)::int)
  INTO q_ord,q_key,q_km,q_cd,q_re,q_fe FROM sessions.ingest_receipt_seat_records(p_claim,t.group_ordinals) q;   -- the ONLY call (profiled hot spot)
 IF q_ord IS DISTINCT FROM ords THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: entry order is not the processing order';END IF;
 -- entry i and seat-record i are the same ord (order check above), so they align by position
 FOR e IN SELECT d.*,q.ck,q.km,q.cd,q.re,q.fe,
   CASE WHEN d.o_claim=p_claim THEN encode(sha256(convert_to(jsonb_build_array('lazy-ack-seat-v1',q_key[array_position(q_ord,d.o_ord)],d.o_seat::text)::text,'UTF8')),'hex') END own_key
  FROM unnest(ords,a_f,a_lin,a_off,a_os,a_ok,a_key,a_oc,a_oo,a_fd) WITH ORDINALITY d(ord,flags,lineage,off,o_seat,o_kind,o_key,o_claim,o_ord,fd,idx)
  JOIN unnest(q_ord,q_key,q_km,q_cd,q_re,q_fe) WITH ORDINALITY q(ord,ck,km,cd,re,fe,qi) ON q.qi=d.idx AND q.ord=d.ord ORDER BY d.idx LOOP
  IF e.o_claim=p_claim AND (e.own_key IS DISTINCT FROM e.o_key OR (q_km[array_position(q_ord,e.o_ord)]->>7) IS DISTINCT FROM (e.km->>7)
   OR e.fd IS DISTINCT FROM q_cd[array_position(q_ord,e.o_ord)]) THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: in-claim reuse key mismatch';END IF;
  IF (e.flags&4)=4 AND (e.o_claim IS DISTINCT FROM p_claim) AND EXISTS(SELECT 1 FROM generate_series(1,e.idx::int-1) gs(gk)
    WHERE (a_f[gs.gk]&1)=1 AND (q_km[gs.gk]->>7)=(e.km->>7)) THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: reuse must name the latest prior in-claim accept';END IF;
  IF e.o_claim=p_claim AND EXISTS(SELECT 1 FROM generate_series(array_position(ords,e.o_ord)+1,e.idx::int-1) gs(gk)
    WHERE (a_f[gs.gk]&1)=1 AND (q_km[gs.gk]->>7)=(e.km->>7)) THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: in-claim reuse must name the latest prior accept';END IF;
  nret:=nret+1;
  idx:=e.idx;ord:=e.ord;accepted:=(e.flags&1)=1;allocated:=(e.flags&2)=2;reused:=(e.flags&4)=4;lineage:=e.lineage;
  seat:=CASE WHEN allocated THEN v.seat_lo+(e.off-1) ELSE e.o_seat END;owner_kind:=e.o_kind;owner_claim:=e.o_claim;owner_ord:=e.o_ord;
  candidate_key:=e.ck;key_material:=e.km;candidate_digest:=e.cd;reset_epoch:=e.re;fence_epoch:=e.fe;
  seat_key:=CASE WHEN allocated THEN encode(sha256(convert_to(jsonb_build_array('lazy-ack-seat-v1',e.ck,seat::text)::text,'UTF8')),'hex') ELSE e.o_key END;
  first_digest:=CASE WHEN accepted THEN e.cd ELSE e.fd END;
  record_id:=e.km->>7;revision:=(e.km->>8)::bigint;source:=e.km->>10;session_id:=e.km->>11;
  native_key:=encode(sha256(convert_to(jsonb_build_array('global-native-custody-v1',a.org_id,a.workspace_id,a.origin_id,e.km->>7)::text,'UTF8')),'hex');
  RETURN NEXT;
 END LOOP;
 IF nret<>v.n THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: decoded row count';END IF;
END $dec$;

-- Owner of a seat number: v2 vector entry (exactly one, allocated). §11.2 caller requires accepted.
CREATE FUNCTION sessions.ingest_ack_seat_owner(p_seat bigint)
RETURNS TABLE(claim_token uuid,ord int,accepted boolean,allocated boolean,seat_key text,first_digest text,key_material jsonb,source text,session_id text,fence_epoch bigint)
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $$
DECLARE v_claim uuid;v_n int;
BEGIN
 SELECT count(*),min(x.claim_token::text)::uuid INTO v_n,v_claim FROM sessions.ingest_ack_vectors x
  WHERE x.allocated_count>0 AND numrange(x.seat_lo,x.seat_hi,'[]')@>p_seat::numeric;
 IF v_n<>1 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: seat owner not unique';END IF;
 RETURN QUERY SELECT v_claim,e.ord,e.accepted,e.allocated,e.seat_key,e.first_digest,e.key_material,e.source,e.session_id,e.fence_epoch FROM sessions.ingest_ack_entries(v_claim) e WHERE e.allocated AND e.seat=p_seat;
END $$;

-- ---------- guards (§12.1) ----------
CREATE FUNCTION sessions.ack_mode() RETURNS text LANGUAGE sql VOLATILE AS $$ SELECT mode FROM sessions.ingest_ack_format WHERE singleton FOR SHARE $$;
ALTER TABLE sessions.ingest_receipt_seats ADD COLUMN compat_vector_claim uuid REFERENCES sessions.ingest_claim_attempts(claim_token);
CREATE FUNCTION sessions.ack_guard_seat() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
DECLARE ow record;
BEGIN PERFORM sessions.ack_require_rc();
 IF sessions.ack_mode()='v2' THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: v1 seat insert refused in mode v2';END IF;
 IF NEW.compat_vector_claim IS NOT NULL THEN  -- compat alias must equal an existing v2 accepted allocation exactly
  SELECT * INTO ow FROM sessions.ingest_ack_seat_owner(NEW.seat);
  IF NOT FOUND OR ow.claim_token<>NEW.compat_vector_claim OR NOT ow.accepted OR NOT ow.allocated OR ow.seat_key<>NEW.key_sha256
   OR ow.first_digest<>NEW.first_digest OR ow.key_material IS DISTINCT FROM NEW.key_material
   OR ow.source IS DISTINCT FROM NEW.source OR ow.session_id IS DISTINCT FROM NEW.session_id OR ow.fence_epoch IS DISTINCT FROM NEW.fence_epoch THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: compat seat must alias an accepted v2 allocation';END IF;
 ELSIF sessions.ack_seat_era(NEW.seat)<>'v1' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: v1 seat outside a v1 era';END IF;
 RETURN NEW;END $$;
CREATE TRIGGER ack_guard_seat BEFORE INSERT ON sessions.ingest_receipt_seats FOR EACH ROW EXECUTE FUNCTION sessions.ack_guard_seat();
CREATE FUNCTION sessions.ack_guard_decision() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
DECLARE f smallint;
BEGIN PERFORM sessions.ack_require_rc();
 IF sessions.ack_mode()='v2' THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: v1 decision insert refused in mode v2';END IF;
 SELECT ack_format INTO f FROM sessions.ingest_claim_attempts WHERE claim_token=NEW.claim_token FOR UPDATE;
 IF f IS NULL THEN UPDATE sessions.ingest_claim_attempts SET ack_format=1 WHERE claim_token=NEW.claim_token;
 ELSIF f<>1 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: claim already has another format';END IF;
 RETURN NEW;END $$;
CREATE TRIGGER ack_guard_decision BEFORE INSERT ON sessions.ingest_lazy_ack_decisions FOR EACH ROW EXECUTE FUNCTION sessions.ack_guard_decision();
CREATE FUNCTION sessions.ack_guard_vector() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
DECLARE f smallint;m sessions.ingest_ack_format%ROWTYPE;
BEGIN PERFORM sessions.ack_require_rc();
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: vectors are immutable';END IF;
 SELECT * INTO STRICT m FROM sessions.ingest_ack_format WHERE singleton FOR SHARE;
 IF m.mode<>'v2' OR (NEW.allocated_count>0 AND (NEW.seat_lo<=m.cutover_seat OR sessions.ack_seat_era(NEW.seat_lo)<>'v2')) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: vector insert refused outside mode v2';END IF;
 SELECT ack_format INTO f FROM sessions.ingest_claim_attempts WHERE claim_token=NEW.claim_token FOR UPDATE;
 IF f IS NULL THEN UPDATE sessions.ingest_claim_attempts SET ack_format=2 WHERE claim_token=NEW.claim_token;
 ELSIF f<>2 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: claim already has another format';END IF;
 RETURN NEW;END $$;
CREATE TRIGGER ack_guard_vector BEFORE INSERT OR UPDATE OR DELETE ON sessions.ingest_ack_vectors FOR EACH ROW EXECUTE FUNCTION sessions.ack_guard_vector();
CREATE FUNCTION sessions.ack_refuse_truncate() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: truncate refused';END $$;
CREATE TRIGGER ack_vector_truncate BEFORE TRUNCATE ON sessions.ingest_ack_vectors FOR EACH STATEMENT EXECUTE FUNCTION sessions.ack_refuse_truncate();
-- write-once discriminator + hashed-field immutability after stamping (§9.1, §12.2.4)
CREATE FUNCTION sessions.ack_attempt_immutable() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF OLD.ack_format IS NOT NULL AND (NEW.ack_format IS DISTINCT FROM OLD.ack_format OR sessions.ack_attempt_sha(NEW)<>sessions.ack_attempt_sha(OLD)) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: stamped attempt is immutable';END IF;
 RETURN NEW;END $$;
CREATE TRIGGER ack_attempt_immutable BEFORE UPDATE ON sessions.ingest_claim_attempts FOR EACH ROW EXECUTE FUNCTION sessions.ack_attempt_immutable();
CREATE FUNCTION sessions.ack_clock_immutable() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF ROW(NEW.control_epoch,NEW.source_identity_sha256,NEW.baseline->>'sourceProofSha256',NEW.initial_generation,NEW.allocated_high_water)
  IS DISTINCT FROM ROW(OLD.control_epoch,OLD.source_identity_sha256,OLD.baseline->>'sourceProofSha256',OLD.initial_generation,OLD.allocated_high_water) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: clock identity is immutable';END IF;
 IF NEW.last_seat<OLD.last_seat THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: seat clock never moves backwards';END IF;
 RETURN NEW;END $$;
CREATE TRIGGER ack_clock_immutable BEFORE UPDATE ON sessions.ingest_receipt_seat_clock FOR EACH ROW EXECUTE FUNCTION sessions.ack_clock_immutable();

-- ---------- slots: fourth pointer + three-pointer invariant (§9.2) ----------
ALTER TABLE sessions.ingest_lazy_native_slots ADD COLUMN current_seat bigint;
ALTER TABLE sessions.ingest_lazy_native_slots ADD CONSTRAINT ack_slot_pointer CHECK(
 (current_owner IS NULL AND num_nonnulls(current_seat,current_seat_key,current_legacy_key)=0)
 OR (current_owner IS NOT NULL AND num_nonnulls(current_seat,current_seat_key,current_legacy_key)=1)) NOT VALID;
ALTER TABLE sessions.ingest_lazy_native_slots VALIDATE CONSTRAINT ack_slot_pointer;

-- ---------- statement-level transition-table validator (§12.2, §10.1, §11.1) ----------
CREATE FUNCTION sessions.ack_slot_validate() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $val$
DECLARE c sessions.ingest_receipt_seat_clock%ROWTYPE;pol sessions.ingest_lazy_policy%ROWTYPE;m sessions.ingest_ack_format%ROWTYPE;v_bad bigint;
BEGIN
 SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton;
 IF TG_OP='UPDATE' THEN
  -- Pointer/owner transition state machine (v9 §15.10), for EVERY updated slot row. States:
  --   U unowned (owner NULL, no pointer) | L legacy (current_legacy_key) | K v1 seat key | S v2 seat
  -- Allowed (with lineage rule):  X -> X unchanged custody            : lineage unchanged (no-op)
  --   U|L|K|S -> K|S  (accept)  : same owner, or U -> new owner; lineage strictly increases; effective seat
  --                               strictly increases when the old state had one
  --   L|K|S -> U      (clear)   : full clear shape (every current_* NULL); lineage = old + 1
  -- Forbidden: anything -> L; U -> U with a lineage change; owner change without passing through U; any other edge.
  WITH t AS(SELECT o.key_sha256,
    CASE WHEN o.current_owner IS NULL THEN 'U' WHEN o.current_legacy_key IS NOT NULL THEN 'L' WHEN o.current_seat_key IS NOT NULL THEN 'K' ELSE 'S' END f,
    CASE WHEN nr.current_owner IS NULL THEN 'U' WHEN nr.current_legacy_key IS NOT NULL THEN 'L' WHEN nr.current_seat_key IS NOT NULL THEN 'K' ELSE 'S' END g,
    o.lineage ol,nr.lineage nl,o.current_owner oo,nr.current_owner no,
    coalesce(o.current_seat,so.seat) os,coalesce(nr.current_seat,sn.seat) ns,
    ROW(o.current_owner,o.current_source,o.current_session,o.current_revision,o.current_digest,o.current_reset_epoch,o.current_fence_epoch,
        o.current_seat,o.current_seat_key,o.current_legacy_key) IS NOT DISTINCT FROM
    ROW(nr.current_owner,nr.current_source,nr.current_session,nr.current_revision,nr.current_digest,nr.current_reset_epoch,nr.current_fence_epoch,
        nr.current_seat,nr.current_seat_key,nr.current_legacy_key) same,
    num_nonnulls(nr.current_source,nr.current_session,nr.current_revision,nr.current_digest,nr.current_reset_epoch,nr.current_fence_epoch)=0 clear_shape,
    -- K accept: the receipt seat is a FRESH (above the committed clock; v1/v1r advance the clock only after their loop),
    -- non-compat seat whose key material binds this slot's tenant/owner/record and every custody field (v9 review H1)
    o.current_revision o_rev,nr.current_revision nr_rev,
    (sn.key_sha256 IS NOT NULL AND sn.compat_vector_claim IS NULL AND sn.seat>c.last_seat
     AND sn.key_material->>0='buffer-first-ack-seat-v1' AND sn.key_material->>12=nr.current_fence_epoch::text
     AND jsonb_array_length(sn.key_material)=13
     AND sn.key_sha256=encode(sha256(convert_to(jsonb_build_array('lazy-ack-seat-v1',encode(sha256(convert_to(sn.key_material::text,'UTF8')),'hex'),sn.seat::text)::text,'UTF8')),'hex')
     AND sn.key_material->>1=c.control_epoch AND sn.key_material->>2=c.source_identity_sha256
     AND sn.key_material->>3=nr.org_id AND sn.key_material->>4=nr.workspace_id AND sn.key_material->>5=nr.current_owner
     AND sn.key_material->>6=nr.origin_id AND sn.key_material->>7=nr.record_id AND sn.key_material->>8=nr.current_revision::text
     AND sn.key_material->>9=nr.current_reset_epoch::text AND sn.key_material->>10 IS NOT DISTINCT FROM nr.current_source
     AND sn.key_material->>11 IS NOT DISTINCT FROM nr.current_session AND sn.first_digest=nr.current_digest
     AND sn.fence_epoch=nr.current_fence_epoch AND nr.source_identity_sha256=c.source_identity_sha256 AND nr.control_epoch=c.control_epoch) k_bound
   FROM nr JOIN orows o ON o.key_sha256=nr.key_sha256
   LEFT JOIN sessions.ingest_receipt_seats sn ON sn.key_sha256=nr.current_seat_key
   LEFT JOIN sessions.ingest_receipt_seats so ON so.key_sha256=o.current_seat_key)
  SELECT count(*) INTO v_bad FROM t WHERE (
       (same AND nl=ol)
    OR (NOT same AND g='S' AND (oo IS NULL OR oo=no) AND nl>ol AND (os IS NULL OR ns>os))        -- S is bound to its final winner below
    OR (NOT same AND g='K' AND (oo IS NULL OR oo=no) AND nl=ol+1 AND (os IS NULL OR ns>os) AND k_bound
        AND (oo IS NULL OR nr_rev>o_rev))
    OR (NOT same AND f IN('L','K','S') AND g='U' AND clear_shape AND nl=ol+1)) IS NOT TRUE;   -- NULL refuses
  IF v_bad>0 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: slot regression refused';END IF;
 ELSIF EXISTS(SELECT 1 FROM nr WHERE current_seat IS NOT NULL OR current_seat_key IS NOT NULL OR lineage<>0) THEN
  -- slots are created only by lazy_records: unowned or legacy, lineage 0; seats are only ever installed by UPDATE
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: slot insert with a v2 seat refused';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM nr WHERE current_seat IS NOT NULL) THEN RETURN NULL;END IF;
 SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton;
 SELECT * INTO STRICT pol FROM sessions.ingest_lazy_policy WHERE singleton;
 SELECT * INTO STRICT m FROM sessions.ingest_ack_format WHERE singleton;
 WITH ch AS MATERIALIZED(SELECT * FROM nr WHERE current_seat IS NOT NULL),
 own AS MATERIALIZED(SELECT DISTINCT v.claim_token FROM ch JOIN sessions.ingest_ack_vectors v
   ON v.allocated_count>0 AND numrange(v.seat_lo,v.seat_hi,'[]')@>ch.current_seat::numeric),
 ent AS MATERIALIZED(SELECT o.claim_token AS ct,e.* FROM own o CROSS JOIN LATERAL sessions.ingest_ack_entries(o.claim_token) e),
 -- final_winners: per owning vector and native key, the accepted+allocated entry with the greatest vector idx
 -- (immutable entry order = executed v1 processing order). Only a final winner may be a slot's current seat.
 fw AS MATERIALIZED(SELECT DISTINCT ON(ct,native_key) * FROM ent WHERE accepted AND allocated ORDER BY ct,native_key,idx DESC)
 SELECT count(*) INTO v_bad FROM ch LEFT JOIN fw e ON e.seat=ch.current_seat
  LEFT JOIN sessions.ingest_claim_attempts a ON a.claim_token=e.ct
 WHERE e.idx IS NULL OR NOT e.accepted OR e.native_key<>ch.key_sha256
  OR a.org_id<>ch.org_id OR a.workspace_id<>ch.workspace_id OR a.origin_id<>ch.origin_id OR e.record_id<>ch.record_id
  OR a.user_id IS DISTINCT FROM ch.current_owner OR e.revision IS DISTINCT FROM ch.current_revision
  OR e.candidate_digest IS DISTINCT FROM ch.current_digest OR e.source IS DISTINCT FROM ch.current_source
  OR e.session_id IS DISTINCT FROM ch.current_session OR e.reset_epoch IS DISTINCT FROM ch.current_reset_epoch
  OR e.fence_epoch IS DISTINCT FROM ch.current_fence_epoch OR e.lineage IS DISTINCT FROM ch.lineage
  OR ch.source_identity_sha256<>c.source_identity_sha256 OR ch.control_epoch<>c.control_epoch
  OR pol.source_identity_sha256<>c.source_identity_sha256 OR pol.control_epoch<>c.control_epoch
  OR sessions.ack_seat_era(ch.current_seat) IS DISTINCT FROM 'v2';
 IF v_bad>0 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: slot current seat binding refused';END IF;
 RETURN NULL;
END $val$;
CREATE FUNCTION sessions.ack_slot_identity() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF TG_OP IN('DELETE','TRUNCATE') THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: native slots are never deleted';END IF;
 IF ROW(NEW.key_sha256,NEW.org_id,NEW.workspace_id,NEW.origin_id,NEW.record_id,NEW.key_material,NEW.source_identity_sha256,NEW.control_epoch,NEW.initialized_witness_sha256,NEW.initialized_at) IS DISTINCT FROM
    ROW(OLD.key_sha256,OLD.org_id,OLD.workspace_id,OLD.origin_id,OLD.record_id,OLD.key_material,OLD.source_identity_sha256,OLD.control_epoch,OLD.initialized_witness_sha256,OLD.initialized_at) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: native slot identity is immutable';END IF;
 RETURN NEW;END $$;
CREATE TRIGGER ack_slot_identity BEFORE UPDATE OR DELETE ON sessions.ingest_lazy_native_slots FOR EACH ROW EXECUTE FUNCTION sessions.ack_slot_identity();
CREATE TRIGGER ack_slot_truncate BEFORE TRUNCATE ON sessions.ingest_lazy_native_slots FOR EACH STATEMENT EXECUTE FUNCTION sessions.ack_slot_identity();
CREATE TRIGGER ack_slot_validate_ins AFTER INSERT ON sessions.ingest_lazy_native_slots REFERENCING NEW TABLE AS nr
 FOR EACH STATEMENT EXECUTE FUNCTION sessions.ack_slot_validate();
CREATE TRIGGER ack_slot_validate_upd AFTER UPDATE ON sessions.ingest_lazy_native_slots REFERENCING OLD TABLE AS orows NEW TABLE AS nr
 FOR EACH STATEMENT EXECUTE FUNCTION sessions.ack_slot_validate();

-- ---------- bridge: keep the v1 bodies under new names; dispatch after the clock lock (§8.1) ----------
ALTER FUNCTION sessions.ingest_lazy_ack_batch(jsonb) RENAME TO ingest_lazy_ack_batch_v1;
ALTER FUNCTION sessions.ingest_lazy_stage_seats(uuid) RENAME TO ingest_lazy_stage_seats_v1;
ALTER FUNCTION sessions.ingest_buffer_gc_ordinals(uuid) RENAME TO ingest_buffer_gc_ordinals_v1;

-- v2 writer (§13.1): v1 decision logic, entries in memory, one vector, one set-based slot UPDATE, coverage check.
CREATE FUNCTION sessions.ingest_lazy_ack_batch_v2(p jsonb) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $v2$
DECLARE c sessions.ingest_receipt_seat_clock%ROWTYPE;a sessions.ingest_claim_attempts%ROWTYPE;
 t sessions.ingest_stage_intents%ROWTYPE;slot sessions.ingest_lazy_native_slots%ROWTYPE;s sessions.ingest_receipt_seats%ROWTYPE;
 r record;ow record;v_last bigint;v_before bigint;v_key text;v_material jsonb;v_accept boolean;v_first text;v_lin bigint;v_seat bigint;
 v_alloc int:=0;v_n int:=0;v_ent bytea:=''::bytea;v_flags int;v_vec bytea;
 v_want bigint;v_want_d bigint;v_upd bigint;v_upd_d bigint;v_mis bigint;v_kind int;v_okey text;v_oclaim uuid;v_oord int;
 pk text[]:='{}';prev bigint[]:='{}';pdig text[]:='{}';plin bigint[]:='{}';pseat bigint[]:='{}';pord int[]:='{}';pskey text[]:='{}';j int;
 psrc text[]:='{}';psess text[]:='{}';pre bigint[]:='{}';pfe bigint[]:='{}';
BEGIN
 PERFORM sessions.ingest_lazy_guard(p->>'org_id');
 PERFORM sessions.ingest_lock_fences(p->>'org_id',p->>'workspace_id',p->>'user_id');
 PERFORM 1 FROM sessions.ingest_lazy_user_seals u JOIN sessions.ingest_seed_fence_baselines b USING(org_id,workspace_id,user_id)
  WHERE u.org_id=p->>'org_id' AND u.workspace_id=p->>'workspace_id' AND u.user_id=p->>'user_id' AND u.privacy_fingerprint=b.fingerprint;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: real user seal required before any receipt';END IF;
 SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE;v_last:=c.last_seat;v_before:=v_last;
 IF p->>'buffer_receipt_kind'='suppressed' THEN RETURN;END IF;
 IF p->>'buffer_receipt_kind' IS DISTINCT FROM 'staged' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: unexpected receipt authority';END IF;
 SELECT i.* INTO t FROM sessions.ingest_buffer_outbox o JOIN sessions.ingest_stage_intents i ON i.stage_token=o.active_stage_token
  WHERE o.org_id=p->>'org_id' AND o.workspace_id=p->>'workspace_id' AND o.user_id=p->>'user_id' AND o.origin_id=p->>'origin_id'
   AND o.batch_id=p->>'batch_id' AND o.digest=p->>'digest' AND o.state='buffered' FOR UPDATE OF i;
 IF NOT FOUND OR t.parent_stage_token IS NOT NULL OR t.receipt_state<>'acknowledged' OR t.cleanup_state<>'live'
  OR t.expires_at<=clock_timestamp() THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: current original stage required';END IF;
 PERFORM sessions.ingest_lazy_attempt(t.claim_token);
 SELECT * INTO STRICT a FROM sessions.ingest_claim_attempts WHERE claim_token=t.claim_token;
 IF a.org_id<>t.org_id OR a.workspace_id<>t.workspace_id OR a.user_id<>t.user_id OR a.origin_id<>t.origin_id
  OR a.batch_id<>t.batch_id OR a.digest<>t.digest OR a.reset_epoch<>t.reset_epoch
  OR jsonb_array_length(a.source_processing_ranks)<>jsonb_array_length(a.records) THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: original candidate order changed';END IF;
 PERFORM sessions.ack_require_rank_permutation(a.source_processing_ranks,jsonb_array_length(a.records));
 FOR r IN SELECT q.* FROM sessions.ingest_receipt_seat_records(t.claim_token,t.group_ordinals) q
  ORDER BY (a.source_processing_ranks->>q.ord)::integer LOOP
  IF a.source_processing_ranks->>r.ord IS NULL THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: source processing rank missing';END IF;
  v_material:=jsonb_build_array('global-native-custody-v1',a.org_id,a.workspace_id,a.origin_id,r.key_material->>7);
  v_key:=encode(sha256(convert_to(v_material::text,'UTF8')),'hex');
  SELECT * INTO slot FROM sessions.ingest_lazy_native_slots WHERE key_sha256=v_key FOR UPDATE;
  IF NOT FOUND OR slot.key_material IS DISTINCT FROM v_material OR slot.source_identity_sha256<>c.source_identity_sha256
   OR slot.control_epoch<>c.control_epoch THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: initialized native custody slot required';END IF;
  j:=array_position(pk,v_key);
  IF j IS NOT NULL THEN  -- overlay an earlier accepted entry of this same claim (v1 updated the slot in-loop)
   slot.current_owner:=a.user_id;slot.current_revision:=prev[j];slot.current_digest:=pdig[j];
   slot.lineage:=plin[j];slot.current_seat:=pseat[j];slot.current_seat_key:=NULL;slot.current_legacy_key:=NULL;
  END IF;
  IF slot.current_owner IS NOT NULL AND slot.current_owner<>a.user_id THEN
   RAISE EXCEPTION USING ERRCODE='P0W01',MESSAGE='session ingest: native key currently belongs to another user';END IF;
  v_accept:=slot.current_owner IS NULL OR slot.current_revision<(r.key_material->>8)::bigint;
  v_first:=CASE WHEN v_accept THEN r.candidate_digest ELSE slot.current_digest END;
  v_kind:=NULL;v_okey:=NULL;v_oclaim:=NULL;v_oord:=NULL;
  IF NOT v_accept AND (slot.current_seat_key IS NOT NULL OR slot.current_seat IS NOT NULL) THEN
   IF slot.current_seat_key IS NOT NULL THEN          -- owner_kind 1: v1 seat (accepted by construction, §11.2)
    SELECT * INTO STRICT s FROM sessions.ingest_receipt_seats WHERE key_sha256=slot.current_seat_key;
    IF s.first_digest<>v_first THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: current custody seat drift';END IF;
    v_seat:=s.seat;v_kind:=1;v_okey:=s.key_sha256;
   ELSIF j IS NOT NULL THEN                            -- owner_kind 2: this claim's own earlier accepted entry
    v_seat:=pseat[j];v_kind:=2;v_okey:=pskey[j];v_oclaim:=a.claim_token;v_oord:=pord[j];
    IF pdig[j]<>v_first THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: current custody seat drift';END IF;
   ELSE                                                -- owner_kind 2: another claim's accepted allocated entry
    SELECT * INTO ow FROM sessions.ingest_ack_seat_owner(slot.current_seat);
    IF NOT FOUND OR NOT ow.accepted OR NOT ow.allocated OR ow.first_digest<>v_first THEN
     RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: current custody seat drift';END IF;
    v_seat:=slot.current_seat;v_kind:=2;v_okey:=ow.seat_key;v_oclaim:=ow.claim_token;v_oord:=ow.ord;
   END IF;
   v_lin:=slot.lineage;v_flags:=4;
  ELSE
   IF v_last=9223372036854775807 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: ACK seats exhausted';END IF;
   v_last:=v_last+1;v_alloc:=v_alloc+1;v_seat:=v_last;
   v_flags:=2|CASE WHEN v_accept THEN 1 ELSE 0 END;
   IF v_accept THEN
    IF slot.lineage=9223372036854775807 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: native lineage exhausted';END IF;
    v_lin:=slot.lineage+1;
    IF j IS NULL THEN pk:=pk||v_key;j:=cardinality(pk);END IF;
    prev[j]:=(r.key_material->>8)::bigint;pdig[j]:=r.candidate_digest;plin[j]:=v_lin;pseat[j]:=v_seat;pord[j]:=r.ord;
    psrc[j]:=r.key_material->>10;psess[j]:=r.key_material->>11;pre[j]:=r.reset_epoch;pfe[j]:=r.fence_epoch;
    pskey[j]:=encode(sha256(convert_to(jsonb_build_array('lazy-ack-seat-v1',r.key_sha256,v_seat::text)::text,'UTF8')),'hex');
   ELSE v_lin:=slot.lineage;END IF;
  END IF;
  v_n:=v_n+1;
  v_ent:=v_ent||sessions.ack_u16(r.ord)||set_byte('\x00'::bytea,0,v_flags)||sessions.ack_varint(v_lin)
   ||CASE WHEN v_flags&2=2 THEN sessions.ack_varint(v_alloc) ELSE int8send(v_seat)||set_byte('\x00'::bytea,0,v_kind)||decode(v_okey,'hex')
     ||CASE WHEN v_kind=2 THEN decode(replace(v_oclaim::text,'-',''),'hex')||sessions.ack_u16(v_oord) ELSE ''::bytea END END
   ||CASE WHEN v_flags&1=0 THEN decode(v_first,'hex') ELSE ''::bytea END;
 END LOOP;
 v_vec:='\x0101'::bytea||sessions.ack_u16(v_n)||sessions.ack_u16(v_alloc)||v_ent;
 INSERT INTO sessions.ingest_ack_vectors(claim_token,ack_stage_token,format_version,n,allocated_count,clock_before,seat_lo,seat_hi,
  attempt_sha256,clock_identity_sha256,vec,vec_sha256)
 VALUES(a.claim_token,t.stage_token,1,v_n,v_alloc,v_before,CASE WHEN v_alloc>0 THEN v_before+1 END,CASE WHEN v_alloc>0 THEN v_last END,
  sessions.ack_attempt_sha(a),sessions.ack_clock_sha(),v_vec,sha256(v_vec));
 -- one set-based slot UPDATE + exact coverage (§13.1 as amended by v9 §15): final_winners = last accepted entry per native key in vector idx order.
 -- writer-side final_winners come from the writer's own visit-order overlay (last accepted entry per native key);
 -- the AFTER STATEMENT validator independently re-derives them from the stored vector bytes (two derivations).
 WITH want AS MATERIALIZED(SELECT x.native_key,x.seat,x.lineage,x.revision,x.candidate_digest,x.source,x.session_id,x.reset_epoch,x.fence_epoch
   FROM unnest(pk,pseat,plin,prev,pdig,psrc,psess,pre,pfe) x(native_key,seat,lineage,revision,candidate_digest,source,session_id,reset_epoch,fence_epoch)),
 upd AS(UPDATE sessions.ingest_lazy_native_slots n SET lineage=w.lineage,current_owner=a.user_id,current_seat=w.seat,current_seat_key=NULL,
   current_legacy_key=NULL,current_revision=w.revision,current_digest=w.candidate_digest,current_source=w.source,current_session=w.session_id,
   current_reset_epoch=w.reset_epoch,current_fence_epoch=w.fence_epoch
  FROM want w WHERE n.key_sha256=w.native_key RETURNING n.key_sha256,n.current_seat,n.lineage,n.current_owner,n.current_revision,n.current_digest,
   n.current_source,n.current_session,n.current_reset_epoch,n.current_fence_epoch)
 SELECT (SELECT count(*) FROM want),(SELECT count(DISTINCT native_key) FROM want),(SELECT count(*) FROM upd),
  (SELECT count(DISTINCT key_sha256) FROM upd),
  (SELECT count(*) FROM upd u FULL JOIN want w ON u.key_sha256=w.native_key AND u.current_seat=w.seat AND u.lineage=w.lineage
    AND u.current_owner=a.user_id AND u.current_revision=w.revision AND u.current_digest=w.candidate_digest
    AND u.current_source IS NOT DISTINCT FROM w.source AND u.current_session IS NOT DISTINCT FROM w.session_id
    AND u.current_reset_epoch=w.reset_epoch AND u.current_fence_epoch=w.fence_epoch
    WHERE u.key_sha256 IS NULL OR w.native_key IS NULL)
 INTO v_want,v_want_d,v_upd,v_upd_d,v_mis;
 IF v_want<>v_want_d OR v_upd<>v_upd_d OR v_upd<>v_want OR v_mis<>0 THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: accepted/update coverage mismatch';END IF;
 UPDATE sessions.ingest_receipt_seat_clock SET last_seat=v_last WHERE singleton;
 IF t.expires_at<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: stage expired while assigning seats';END IF;
 PERFORM sessions.ingest_lazy_attempt(t.claim_token);
END $v2$;

-- P2/T6c dual-aware v1-format rollback writer: the original v1 body (lazy:616-693) with exactly the [v1r change N] edits.
CREATE FUNCTION sessions.ingest_lazy_ack_batch_v1r(p jsonb) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $v1r$
DECLARE c sessions.ingest_receipt_seat_clock%ROWTYPE;a sessions.ingest_claim_attempts%ROWTYPE;
 t sessions.ingest_stage_intents%ROWTYPE;slot sessions.ingest_lazy_native_slots%ROWTYPE;s sessions.ingest_receipt_seats%ROWTYPE;
 d sessions.ingest_lazy_ack_decisions%ROWTYPE;r record;ow record;v_last bigint;v_key text;v_material jsonb;v_accept boolean;v_first text;v_new_lineage bigint;
BEGIN
 PERFORM sessions.ingest_lazy_guard(p->>'org_id');
 PERFORM sessions.ingest_lock_fences(p->>'org_id',p->>'workspace_id',p->>'user_id');
 PERFORM 1 FROM sessions.ingest_lazy_user_seals u JOIN sessions.ingest_seed_fence_baselines b USING(org_id,workspace_id,user_id)
  WHERE u.org_id=p->>'org_id' AND u.workspace_id=p->>'workspace_id' AND u.user_id=p->>'user_id' AND u.privacy_fingerprint=b.fingerprint;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: real user seal required before any receipt';END IF;
 SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE;v_last:=c.last_seat;
 IF p->>'buffer_receipt_kind'='suppressed' THEN RETURN;END IF;
 IF p->>'buffer_receipt_kind' IS DISTINCT FROM 'staged' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: unexpected receipt authority';END IF;
 SELECT i.* INTO t FROM sessions.ingest_buffer_outbox o JOIN sessions.ingest_stage_intents i ON i.stage_token=o.active_stage_token
  WHERE o.org_id=p->>'org_id' AND o.workspace_id=p->>'workspace_id' AND o.user_id=p->>'user_id' AND o.origin_id=p->>'origin_id'
   AND o.batch_id=p->>'batch_id' AND o.digest=p->>'digest' AND o.state='buffered' FOR UPDATE OF i;
 IF NOT FOUND OR t.parent_stage_token IS NOT NULL OR t.receipt_state<>'acknowledged' OR t.cleanup_state<>'live'
  OR t.expires_at<=clock_timestamp() THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: current original stage required';END IF;
 PERFORM sessions.ingest_lazy_attempt(t.claim_token);
 SELECT * INTO STRICT a FROM sessions.ingest_claim_attempts WHERE claim_token=t.claim_token;
 IF a.org_id<>t.org_id OR a.workspace_id<>t.workspace_id OR a.user_id<>t.user_id OR a.origin_id<>t.origin_id
  OR a.batch_id<>t.batch_id OR a.digest<>t.digest OR a.reset_epoch<>t.reset_epoch
  OR jsonb_array_length(a.source_processing_ranks)<>jsonb_array_length(a.records) THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: original candidate order changed';END IF;
 PERFORM sessions.ack_require_rank_permutation(a.source_processing_ranks,jsonb_array_length(a.records));   -- [v1r change 3]
 FOR r IN SELECT q.* FROM sessions.ingest_receipt_seat_records(t.claim_token,t.group_ordinals) q
  ORDER BY (a.source_processing_ranks->>q.ord)::integer LOOP
  IF a.source_processing_ranks->>r.ord IS NULL THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: source processing rank missing';END IF;
  v_material:=jsonb_build_array('global-native-custody-v1',a.org_id,a.workspace_id,a.origin_id,r.key_material->>7);
  v_key:=encode(sha256(convert_to(v_material::text,'UTF8')),'hex');
  SELECT * INTO slot FROM sessions.ingest_lazy_native_slots WHERE key_sha256=v_key FOR UPDATE;
  IF NOT FOUND OR slot.key_material IS DISTINCT FROM v_material OR slot.source_identity_sha256<>c.source_identity_sha256
   OR slot.control_epoch<>c.control_epoch THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: initialized native custody slot required';END IF;
  IF slot.current_owner IS NOT NULL AND slot.current_owner<>a.user_id THEN
   RAISE EXCEPTION USING ERRCODE='P0W01',MESSAGE='session ingest: native key currently belongs to another user';END IF;
  v_accept:=slot.current_owner IS NULL OR slot.current_revision<(r.key_material->>8)::bigint;
  v_first:=CASE WHEN v_accept THEN r.candidate_digest ELSE slot.current_digest END;
  -- Every ignored old-source candidate receives a real target order reference;
  -- it never fabricates the source row's publication seat.
  IF NOT v_accept AND (slot.current_seat_key IS NOT NULL OR slot.current_seat IS NOT NULL) THEN   -- [v1r change 1]
   IF slot.current_seat_key IS NULL THEN
    SELECT * INTO ow FROM sessions.ingest_ack_seat_owner(slot.current_seat);
    IF NOT FOUND OR NOT ow.accepted OR NOT ow.allocated THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: current custody seat drift';END IF;
    INSERT INTO sessions.ingest_receipt_seats(key_sha256,key_material,seat,first_digest,source,session_id,fence_epoch,compat_vector_claim)
     VALUES(ow.seat_key,ow.key_material,slot.current_seat,ow.first_digest,ow.source,ow.session_id,ow.fence_epoch,ow.claim_token) ON CONFLICT(key_sha256) DO NOTHING;
    SELECT * INTO STRICT s FROM sessions.ingest_receipt_seats WHERE key_sha256=ow.seat_key;
   ELSE
   SELECT * INTO STRICT s FROM sessions.ingest_receipt_seats WHERE key_sha256=slot.current_seat_key;
   END IF;
   IF s.first_digest<>v_first THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: current custody seat drift';END IF;
  ELSE
   IF v_last=9223372036854775807 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: ACK seats exhausted';END IF;
   v_last:=v_last+1;
   INSERT INTO sessions.ingest_receipt_seats(key_sha256,key_material,seat,first_digest,source,session_id,fence_epoch)
   VALUES(encode(sha256(convert_to(jsonb_build_array('lazy-ack-seat-v1',r.key_sha256,v_last::text)::text,'UTF8')),'hex'),
    r.key_material,v_last,v_first,r.key_material->>10,r.key_material->>11,r.fence_epoch) RETURNING * INTO s;
  END IF;
  v_new_lineage:=slot.lineage;
  IF v_accept THEN
   IF slot.lineage=9223372036854775807 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: native lineage exhausted';END IF;
   v_new_lineage:=slot.lineage+1;
   UPDATE sessions.ingest_lazy_native_slots SET lineage=v_new_lineage,current_owner=a.user_id,
    current_source=r.key_material->>10,current_session=r.key_material->>11,current_revision=(r.key_material->>8)::bigint,
    current_digest=r.candidate_digest,current_reset_epoch=r.reset_epoch,current_fence_epoch=r.fence_epoch,
    current_seat_key=s.key_sha256,current_legacy_key=NULL,current_seat=NULL WHERE key_sha256=v_key;   -- [v1r change 2]
  END IF;
  INSERT INTO sessions.ingest_lazy_ack_decisions(claim_token,ord,native_key_sha256,native_lineage,candidate_key_sha256,candidate_digest,
   first_digest,body_accepted,seat_key_sha256,reset_epoch,fence_epoch,generation,source_control_version,source_witness_sha256)
  VALUES(t.claim_token,r.ord,v_key,v_new_lineage,r.key_sha256,r.candidate_digest,v_first,v_accept,s.key_sha256,r.reset_epoch,r.fence_epoch,
   a.schema_generation,a.source_control_version,a.source_witness_sha256) ON CONFLICT DO NOTHING;
  SELECT * INTO STRICT d FROM sessions.ingest_lazy_ack_decisions WHERE claim_token=t.claim_token AND ord=r.ord;
  IF d.native_key_sha256<>v_key OR d.native_lineage<>v_new_lineage OR d.candidate_key_sha256<>r.key_sha256 OR d.candidate_digest<>r.candidate_digest
   OR d.first_digest<>v_first OR d.body_accepted<>v_accept OR d.seat_key_sha256<>s.key_sha256 OR d.reset_epoch<>r.reset_epoch OR d.fence_epoch<>r.fence_epoch
   OR d.generation<>a.schema_generation OR d.source_control_version<>a.source_control_version OR d.source_witness_sha256<>a.source_witness_sha256 THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: immutable ACK decision adoption refused';END IF;
 END LOOP;
 UPDATE sessions.ingest_receipt_seat_clock SET last_seat=v_last WHERE singleton;
 IF t.expires_at<=clock_timestamp() THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: stage expired while assigning seats';END IF;
 PERFORM sessions.ingest_lazy_attempt(t.claim_token);
END $v1r$;
REVOKE ALL ON FUNCTION sessions.ingest_lazy_ack_batch_v1r(jsonb)
 FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

CREATE FUNCTION sessions.ingest_lazy_ack_batch(p jsonb) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $bridge$
BEGIN
 PERFORM sessions.ack_require_rc();
 PERFORM sessions.ingest_lazy_guard(p->>'org_id');
 PERFORM sessions.ingest_lock_fences(p->>'org_id',p->>'workspace_id',p->>'user_id');
 PERFORM 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE;
 IF sessions.ack_mode()='v2' THEN PERFORM sessions.ingest_lazy_ack_batch_v2(p);
 ELSIF EXISTS(SELECT 1 FROM sessions.ingest_ack_format_epochs WHERE mode='v2') THEN PERFORM sessions.ingest_lazy_ack_batch_v1r(p);
 ELSE PERFORM sessions.ingest_lazy_ack_batch_v1(p);END IF;
END $bridge$;

-- readers dispatch on the per-claim discriminator only (§9.1)
CREATE FUNCTION sessions.ingest_lazy_stage_seats(p_stage uuid) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $stage$
DECLARE c sessions.ingest_receipt_seat_clock%ROWTYPE;t sessions.ingest_stage_intents%ROWTYPE;a sessions.ingest_claim_attempts%ROWTYPE;
 v_rows jsonb;v_n integer;v_expected integer;v_last bigint;
BEGIN
 PERFORM sessions.ack_require_rc();
 SELECT * INTO STRICT t FROM sessions.ingest_stage_intents WHERE stage_token=p_stage;
 SELECT * INTO STRICT a FROM sessions.ingest_claim_attempts WHERE claim_token=t.claim_token;
 IF a.ack_format IS NULL THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: unstamped acknowledged claim';END IF;
 IF a.ack_format IN(0,1) THEN RETURN sessions.ingest_lazy_stage_seats_v1(p_stage);END IF;
 SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton;
 PERFORM sessions.ingest_lazy_guard(t.org_id);
 IF t.receipt_state<>'acknowledged' OR t.cleanup_state<>'live' OR NOT EXISTS(
  SELECT 1 FROM sessions.ingest_buffer_outbox o WHERE o.active_stage_token=t.stage_token AND o.state='buffered'
   AND o.org_id=t.org_id AND o.workspace_id=t.workspace_id AND o.user_id=t.user_id AND o.origin_id=t.origin_id
   AND o.batch_id=t.batch_id AND o.digest=t.digest) THEN
  RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='session ingest: current acknowledged stage required';END IF;
 IF a.org_id<>t.org_id OR a.workspace_id<>t.workspace_id OR a.user_id<>t.user_id OR a.origin_id<>t.origin_id OR a.batch_id<>t.batch_id
  OR a.digest<>t.digest OR a.reset_epoch<>t.reset_epoch OR a.source_identity_sha256 IS DISTINCT FROM c.source_identity_sha256 THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: current original candidate identity changed';END IF;
 WITH wanted AS MATERIALIZED(SELECT * FROM sessions.ingest_receipt_seat_records(t.claim_token,t.group_ordinals)),
 ent AS MATERIALIZED(SELECT * FROM sessions.ingest_ack_entries(t.claim_token))
 SELECT jsonb_agg(jsonb_build_object('ord',r.ord,'seat',e.seat::text,'candidateDigest',r.candidate_digest,'firstDigest',e.first_digest,
  'bodyAccepted',e.accepted,'resetEpoch',r.reset_epoch::text,'fenceEpoch',r.fence_epoch::text) ORDER BY r.ord),
  count(*)::integer,(SELECT count(*)::integer FROM wanted) INTO v_rows,v_n,v_expected
 FROM wanted r JOIN ent e ON e.ord=r.ord AND e.candidate_key=r.key_sha256 AND e.candidate_digest=r.candidate_digest
  AND e.reset_epoch=r.reset_epoch AND e.fence_epoch=r.fence_epoch AND e.seat>c.allocated_high_water
 JOIN sessions.ingest_lazy_native_slots n ON n.key_sha256=e.native_key AND n.org_id=a.org_id AND n.workspace_id=a.workspace_id
  AND n.origin_id=a.origin_id AND n.record_id=r.key_material->>7 AND n.source_identity_sha256=c.source_identity_sha256 AND n.control_epoch=c.control_epoch;
 IF v_n=0 OR v_n<>v_expected THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: original ACK decisions missing';END IF;
 SELECT last_seat INTO STRICT v_last FROM sessions.ingest_receipt_seat_clock WHERE singleton;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v_rows)r WHERE (r->>'seat')::bigint>v_last) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='session ingest: ACK decision clock snapshot incomplete';END IF;
 RETURN jsonb_build_object('controlEpoch',c.control_epoch,'sourceIdentitySha256',c.source_identity_sha256,
  'allocatedHighWater',c.allocated_high_water::text,'lastAssignedSeat',v_last::text,'records',v_rows);
END $stage$;

CREATE FUNCTION sessions.ingest_buffer_gc_ordinals(p_stage uuid) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $gco$
DECLARE s sessions.ingest_stage_intents%ROWTYPE;a sessions.ingest_claim_attempts%ROWTYPE;v_count int;v_result jsonb;
BEGIN
 SELECT * INTO STRICT s FROM sessions.ingest_stage_intents WHERE stage_token=p_stage;
 SELECT * INTO STRICT a FROM sessions.ingest_claim_attempts WHERE claim_token=s.claim_token;
 IF a.ack_format IS DISTINCT FROM 2 OR s.receipt_state='preparing' THEN RETURN sessions.ingest_buffer_gc_ordinals_v1(p_stage);END IF;
 PERFORM sessions.ack_require_rc();
 PERFORM 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR SHARE;
 IF ROW(a.org_id,a.workspace_id,a.user_id,a.origin_id,a.batch_id,a.digest,a.reset_epoch)
  IS DISTINCT FROM ROW(s.org_id,s.workspace_id,s.user_id,s.origin_id,s.batch_id,s.digest,s.reset_epoch) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc: original identity changed';END IF;
 WITH wanted AS MATERIALIZED(SELECT * FROM sessions.ingest_receipt_seat_records(s.claim_token,s.group_ordinals)),
 ent AS MATERIALIZED(SELECT * FROM sessions.ingest_ack_entries(s.claim_token))
 SELECT count(*) INTO v_count FROM wanted r JOIN ent e ON e.ord=r.ord AND e.candidate_key=r.key_sha256 AND e.candidate_digest=r.candidate_digest
  AND e.reset_epoch=r.reset_epoch AND e.fence_epoch=r.fence_epoch;
 IF v_count<>(SELECT count(*) FROM jsonb_array_elements(s.group_ordinals)g CROSS JOIN LATERAL jsonb_array_elements(g)o) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='buffer gc: immutable ACK decisions required';END IF;
 WITH wanted AS MATERIALIZED(SELECT * FROM sessions.ingest_receipt_seat_records(s.claim_token,s.group_ordinals)),
 ent AS MATERIALIZED(SELECT * FROM sessions.ingest_ack_entries(s.claim_token))
 SELECT coalesce(jsonb_agg(r.ord ORDER BY r.ord),'[]') INTO v_result
 FROM wanted r JOIN ent e ON e.ord=r.ord
 JOIN sessions.ingest_lazy_native_slots n ON n.key_sha256=e.native_key
  AND n.org_id=s.org_id AND n.workspace_id=s.workspace_id AND n.origin_id=s.origin_id AND n.record_id=r.key_material->>7
 WHERE e.accepted AND n.current_owner=s.user_id AND n.current_seat=e.seat
  AND n.current_digest=e.candidate_digest AND n.current_revision=(r.key_material->>8)::bigint
  AND n.current_source IS NOT DISTINCT FROM r.key_material->>10
  AND n.current_session IS NOT DISTINCT FROM r.key_material->>11
  AND NOT EXISTS(SELECT 1 FROM sessions.upload_deletions f WHERE f.org_id=s.org_id AND f.workspace_id=s.workspace_id
   AND f.user_id=s.user_id AND f.origin_id=s.origin_id AND f.source=n.current_source AND f.session_id=n.current_session);
 RETURN v_result;
END $gco$;

-- lazy_clear: identical to lazy:733-746 plus current_seat (MAJOR 2 of the first independent review)
CREATE OR REPLACE FUNCTION sessions.ingest_lazy_clear(p jsonb,p_kind text) RETURNS void
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $lazy_clear$
BEGIN
 IF p_kind='selection' AND (p->>'enabled')::boolean THEN RETURN;END IF;
 PERFORM sessions.ack_require_rc();
 PERFORM sessions.ingest_lock_fences(p#>>'{tenant,orgId}',p#>>'{tenant,workspaceId}',p#>>'{tenant,userId}');
 PERFORM 1 FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE;
 UPDATE sessions.ingest_lazy_native_slots n SET lineage=n.lineage+1,current_owner=NULL,current_source=NULL,current_session=NULL,
  current_revision=NULL,current_digest=NULL,current_reset_epoch=NULL,current_fence_epoch=NULL,current_seat_key=NULL,current_legacy_key=NULL,current_seat=NULL
 WHERE n.org_id=p#>>'{tenant,orgId}' AND n.workspace_id=p#>>'{tenant,workspaceId}' AND n.current_owner=p#>>'{tenant,userId}'
  AND n.current_session IS NOT NULL AND(p_kind='reset' OR(n.origin_id=p->>'originId' AND EXISTS(
   SELECT 1 FROM sessions.ingest_session_pairs(p->'sessions')s WHERE s.source=n.current_source AND s.session_id=n.current_session)));
END $lazy_clear$;

-- ---------- stamp history, then the owner flip (§9.1, §8.1, §9.3) ----------
UPDATE sessions.ingest_claim_attempts a SET ack_format=1
 WHERE EXISTS(SELECT 1 FROM sessions.ingest_lazy_ack_decisions d WHERE d.claim_token=a.claim_token) AND a.ack_format IS NULL;
-- committed claims with no ACK rows can never acquire a format later (review of d0239414): stamp them legacy (0)
UPDATE sessions.ingest_claim_attempts a SET ack_format=0
 WHERE a.ack_format IS NULL AND EXISTS(SELECT 1 FROM sessions.ingest_batches b WHERE b.org_id=a.org_id AND b.workspace_id=a.workspace_id
  AND b.origin_id=a.origin_id AND b.batch_id=a.batch_id AND b.digest=a.digest AND b.state='committed');
UPDATE sessions.ingest_ack_format SET released_at=clock_timestamp();

CREATE FUNCTION sessions.ingest_ack_drain_count() RETURNS bigint LANGUAGE sql STABLE AS $$
 SELECT count(*) FROM pg_stat_activity s, sessions.ingest_ack_format f
 WHERE f.singleton AND s.datname=current_database() AND s.pid<>pg_backend_pid() AND s.backend_type='client backend'
  AND (s.state IS NULL OR (s.state<>'idle' AND s.xact_start IS NULL) OR s.xact_start<f.released_at) $$;

CREATE FUNCTION sessions.ingest_ack_format_set(p_mode text) RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $set$
DECLARE c sessions.ingest_receipt_seat_clock%ROWTYPE;m sessions.ingest_ack_format%ROWTYPE;
BEGIN
 PERFORM sessions.ack_require_rc();
 IF NOT pg_has_role(current_user,'pg_read_all_stats','USAGE') THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: drain requires pg_read_all_stats';END IF;
 SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR UPDATE;   -- same lock every seat writer takes
 SELECT * INTO STRICT m FROM sessions.ingest_ack_format WHERE singleton FOR UPDATE;
 IF sessions.ingest_ack_drain_count()<>0 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: bridge drain incomplete';END IF;
 IF p_mode='v2' THEN
  IF EXISTS(SELECT 1 FROM sessions.ingest_claim_attempts a JOIN sessions.ingest_stage_intents t ON t.claim_token=a.claim_token
   WHERE t.receipt_state='acknowledged' AND a.ack_format IS NULL AND EXISTS(SELECT 1 FROM sessions.ingest_lazy_ack_decisions d WHERE d.claim_token=a.claim_token)) THEN
   RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: unstamped acknowledged claim';END IF;
  IF m.mode='v2' THEN RETURN (SELECT to_jsonb(f) FROM sessions.ingest_ack_format f WHERE singleton);END IF;
  UPDATE sessions.ingest_ack_format SET mode='v2',epoch=epoch+1,cutover_seat=c.last_seat,changed_at=clock_timestamp() WHERE singleton;
  INSERT INTO sessions.ingest_ack_format_epochs(epoch,mode,first_seat) SELECT epoch,'v2',c.last_seat::numeric+1 FROM sessions.ingest_ack_format WHERE singleton;
 ELSIF p_mode='v1' THEN
  IF m.mode='v1' THEN RETURN (SELECT to_jsonb(f) FROM sessions.ingest_ack_format f WHERE singleton);END IF;
  -- rollback: the dual-aware v1-format writer (ingest_lazy_ack_batch_v1r) takes over for every later ACK
  UPDATE sessions.ingest_ack_format SET mode='v1',epoch=epoch+1,changed_at=clock_timestamp() WHERE singleton;
  INSERT INTO sessions.ingest_ack_format_epochs(epoch,mode,first_seat) SELECT epoch,'v1',c.last_seat::numeric+1 FROM sessions.ingest_ack_format WHERE singleton;
 ELSE RAISE EXCEPTION USING ERRCODE='22023',MESSAGE='ack v2: mode';END IF;
 RETURN (SELECT to_jsonb(f) FROM sessions.ingest_ack_format f WHERE singleton);
END $set$;

-- ---------- P4 / §9.9: 620 whole-row snapshot stays byte-compatible (prototype stand-in for the forward compat migration) ----------
-- v1-shaped projection: drop current_seat; a v2 seat is presented as the v1 seat key v1 would have produced
-- (same derivation sha256(['lazy-ack-seat-v1',candidate_key,seat]), taken from the owning vector entry).
CREATE FUNCTION sessions.ack_slot_v1_projection(n sessions.ingest_lazy_native_slots) RETURNS jsonb LANGUAGE sql VOLATILE AS $$
 SELECT (to_jsonb(n)-'current_seat')||CASE WHEN n.current_seat IS NULL THEN '{}'::jsonb
  ELSE jsonb_build_object('current_seat_key',(SELECT o.seat_key FROM sessions.ingest_ack_seat_owner(n.current_seat) o)) END $$;
CREATE OR REPLACE FUNCTION sessions.ingest_history_native_snapshot(p_stage uuid)RETURNS jsonb
LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pg_temp AS $native$
DECLARE s sessions.ingest_stage_intents%ROWTYPE;a sessions.ingest_claim_attempts%ROWTYPE;v jsonb;v_expected bigint;v_wanted bigint;
BEGIN
 SELECT *INTO STRICT s FROM sessions.ingest_stage_intents WHERE stage_token=p_stage;
 SELECT *INTO STRICT a FROM sessions.ingest_claim_attempts WHERE claim_token=s.claim_token;
 WITH keep AS MATERIALIZED(SELECT DISTINCT(o::text)::integer ord FROM jsonb_array_elements(s.group_ordinals)g CROSS JOIN LATERAL jsonb_array_elements(g)o),wanted AS MATERIALIZED(
  SELECT DISTINCT x->>'record_id' record_id,encode(sha256(convert_to(jsonb_build_array('global-native-custody-v1',a.org_id,a.workspace_id,a.origin_id,x->>'record_id')::text,'UTF8')),'hex') key
  FROM jsonb_array_elements(a.records)x JOIN keep k ON k.ord=(x->>'ord')::integer
 )SELECT coalesce(jsonb_agg(jsonb_build_object('recordId',w.record_id,'slot',sessions.ack_slot_v1_projection(n))ORDER BY w.record_id COLLATE"C"),'[]'),count(*),(SELECT count(*)FROM wanted)
 INTO v,v_expected,v_wanted FROM wanted w JOIN sessions.ingest_lazy_native_slots n ON n.key_sha256=w.key AND n.org_id=a.org_id
  AND n.workspace_id=a.workspace_id AND n.origin_id=a.origin_id AND n.record_id=w.record_id AND n.current_owner=a.user_id;
 IF v_expected=0 OR v_expected<>v_wanted THEN RAISE EXCEPTION USING ERRCODE='40001',MESSAGE='history: no current qualified native custody';END IF;
 RETURN v;
END $native$;

-- clock preimage over an explicit row (TS golden twin, P13)
CREATE FUNCTION sessions.ack_clock_sha_of(c sessions.ingest_receipt_seat_clock) RETURNS bytea LANGUAGE sql IMMUTABLE AS $$
 SELECT sha256(convert_to('96bulls/ack-clock/v1','UTF8')||'\x00'::bytea||sessions.ack_ft(c.control_epoch)||sessions.ack_ft(c.source_identity_sha256)
  ||sessions.ack_ft(c.baseline->>'sourceProofSha256')||sessions.ack_ft(c.initial_generation)||sessions.ack_fi(c.allocated_high_water)) $$;

-- ---------- P10c hardening: replica-mode (session_replication_role=replica) must not skip guards/validator ----------
ALTER TABLE sessions.ingest_receipt_seats ENABLE ALWAYS TRIGGER ack_guard_seat;
ALTER TABLE sessions.ingest_lazy_ack_decisions ENABLE ALWAYS TRIGGER ack_guard_decision;
ALTER TABLE sessions.ingest_ack_vectors ENABLE ALWAYS TRIGGER ack_guard_vector;
ALTER TABLE sessions.ingest_ack_vectors ENABLE ALWAYS TRIGGER ack_vector_truncate;
ALTER TABLE sessions.ingest_claim_attempts ENABLE ALWAYS TRIGGER ack_attempt_immutable;
ALTER TABLE sessions.ingest_receipt_seat_clock ENABLE ALWAYS TRIGGER ack_clock_immutable;
ALTER TABLE sessions.ingest_lazy_native_slots ENABLE ALWAYS TRIGGER ack_slot_validate_ins;
ALTER TABLE sessions.ingest_lazy_native_slots ENABLE ALWAYS TRIGGER ack_slot_validate_upd;

CREATE TRIGGER ack_era_guard BEFORE INSERT OR UPDATE OR DELETE ON sessions.ingest_ack_format_epochs FOR EACH ROW EXECUTE FUNCTION sessions.ack_era_guard();
CREATE TRIGGER ack_era_truncate BEFORE TRUNCATE ON sessions.ingest_ack_format_epochs FOR EACH STATEMENT EXECUTE FUNCTION sessions.ack_era_guard();
ALTER TABLE sessions.ingest_ack_format_epochs ENABLE ROW LEVEL SECURITY;
ALTER TABLE sessions.ingest_ack_format_epochs ENABLE ALWAYS TRIGGER ack_era_guard;
ALTER TABLE sessions.ingest_ack_format_epochs ENABLE ALWAYS TRIGGER ack_era_truncate;
ALTER TABLE sessions.ingest_lazy_native_slots ENABLE ALWAYS TRIGGER ack_slot_identity;
ALTER TABLE sessions.ingest_lazy_native_slots ENABLE ALWAYS TRIGGER ack_slot_truncate;
REVOKE ALL ON sessions.ingest_ack_format_epochs FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
-- defence in depth (minor 3): no direct EXECUTE on any prototype function for app roles
DO $rv$DECLARE f record;BEGIN
 FOR f IN SELECT p.oid::regprocedure sig FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
  WHERE n.nspname='sessions' AND (p.proname LIKE 'ack\_%' OR p.proname IN('ingest_ack_entries','ingest_ack_seat_owner','ingest_ack_format_set','ingest_ack_drain_count',
   'ingest_lazy_ack_batch','ingest_lazy_ack_batch_v1','ingest_lazy_ack_batch_v1r','ingest_lazy_ack_batch_v2','ingest_lazy_stage_seats','ingest_lazy_stage_seats_v1',
   'ingest_buffer_gc_ordinals','ingest_buffer_gc_ordinals_v1','ingest_lazy_clear'))
 LOOP EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder',f.sig);END LOOP;END$rv$;

-- ---------- receipt evidence is append-only (review v9 MAJOR 3): no UPDATE / DELETE / TRUNCATE ----------
CREATE FUNCTION sessions.ack_receipt_immutable() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: receipt evidence is append-only';END $$;
CREATE TRIGGER ack_receipt_seats_immutable BEFORE UPDATE OR DELETE ON sessions.ingest_receipt_seats FOR EACH ROW EXECUTE FUNCTION sessions.ack_receipt_immutable();
CREATE TRIGGER ack_receipt_seats_truncate BEFORE TRUNCATE ON sessions.ingest_receipt_seats FOR EACH STATEMENT EXECUTE FUNCTION sessions.ack_receipt_immutable();
CREATE TRIGGER ack_decisions_immutable BEFORE UPDATE OR DELETE ON sessions.ingest_lazy_ack_decisions FOR EACH ROW EXECUTE FUNCTION sessions.ack_receipt_immutable();
CREATE TRIGGER ack_decisions_truncate BEFORE TRUNCATE ON sessions.ingest_lazy_ack_decisions FOR EACH STATEMENT EXECUTE FUNCTION sessions.ack_receipt_immutable();
ALTER TABLE sessions.ingest_receipt_seats ENABLE ALWAYS TRIGGER ack_receipt_seats_immutable;
ALTER TABLE sessions.ingest_receipt_seats ENABLE ALWAYS TRIGGER ack_receipt_seats_truncate;
ALTER TABLE sessions.ingest_lazy_ack_decisions ENABLE ALWAYS TRIGGER ack_decisions_immutable;
ALTER TABLE sessions.ingest_lazy_ack_decisions ENABLE ALWAYS TRIGGER ack_decisions_truncate;
REVOKE ALL ON FUNCTION sessions.ack_receipt_immutable() FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

-- ---------- receipt provenance enforced in SQL (migration-completeness addendum 6a09d424) ----------
-- SECURITY DEFINER: deferred constraint triggers fire at COMMIT, outside the SECURITY DEFINER RPC, as the session role
-- (bulls96_session_ingest), which cannot read the ledger tables; argument-less, pinned search_path, owner-only.
-- Commit-time (DEFERRABLE INITIALLY DEFERRED) because the real v1/v1r writers keep their order:
-- seat insert -> slot update -> decision insert -> clock advance (lazy:665/673/678/688).
-- A committed K winner must have exactly ONE body_accepted v1 decision bound to claim/tenant/owner/native key/
-- seat/lineage/digest/epochs, and its seat must be <= the committed clock in a v1 era (no winner ahead of the clock).
CREATE FUNCTION sessions.ack_k_provenance() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $kp$
DECLARE n sessions.ingest_lazy_native_slots%ROWTYPE;s sessions.ingest_receipt_seats%ROWTYPE;v_last bigint;v_ok int;
BEGIN
 SELECT * INTO n FROM sessions.ingest_lazy_native_slots WHERE key_sha256=NEW.key_sha256;   -- the state AT COMMIT
 IF NOT FOUND OR n.current_seat_key IS NULL THEN RETURN NULL;END IF;
 SELECT * INTO STRICT s FROM sessions.ingest_receipt_seats WHERE key_sha256=n.current_seat_key;
 SELECT last_seat INTO STRICT v_last FROM sessions.ingest_receipt_seat_clock WHERE singleton;
 IF s.compat_vector_claim IS NOT NULL OR s.seat>v_last OR sessions.ack_seat_era(s.seat) IS DISTINCT FROM 'v1' THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: K winner ahead of the committed clock or outside a v1 era';END IF;
 SELECT count(*) INTO v_ok FROM sessions.ingest_lazy_ack_decisions d JOIN sessions.ingest_claim_attempts a USING(claim_token)
  WHERE d.seat_key_sha256=n.current_seat_key AND d.body_accepted AND a.ack_format=1
   AND d.native_key_sha256=n.key_sha256 AND d.native_lineage=n.lineage AND d.first_digest=n.current_digest AND d.candidate_digest=n.current_digest
   AND d.reset_epoch=n.current_reset_epoch AND d.fence_epoch=n.current_fence_epoch
   AND a.org_id=n.org_id AND a.workspace_id=n.workspace_id AND a.user_id=n.current_owner AND a.origin_id=n.origin_id
   AND d.candidate_key_sha256=encode(sha256(convert_to(s.key_material::text,'UTF8')),'hex')
   AND d.generation=a.schema_generation AND d.source_control_version=a.source_control_version
   AND d.source_witness_sha256=a.source_witness_sha256 AND d.reset_epoch=a.reset_epoch
   AND EXISTS(SELECT 1 FROM jsonb_array_elements(a.records) x WHERE (x->>'ord')::int=d.ord AND x->>'record_id'=n.record_id
    AND (x->>'revision')::bigint=n.current_revision AND x->>'digest'=n.current_digest)
   AND EXISTS(SELECT 1 FROM sessions.ingest_batches b WHERE b.org_id=a.org_id AND b.workspace_id=a.workspace_id AND b.origin_id=a.origin_id
    AND b.batch_id=a.batch_id AND b.digest=a.digest AND b.user_id=a.user_id AND b.state='committed' AND b.buffer_receipt_kind='staged')
   -- the claim owns the batch's ORIGINAL stage (no parent, acknowledged, full identity), and that stage is the root of the
   -- outbox's current stage chain (GC may have replaced it with a child) or the outbox is erased; a cloned claim owns no stage
   AND EXISTS(SELECT 1 FROM sessions.ingest_stage_intents t JOIN sessions.ingest_buffer_outbox o ON o.org_id=a.org_id AND o.workspace_id=a.workspace_id
     AND o.origin_id=a.origin_id AND o.batch_id=a.batch_id AND o.user_id=a.user_id AND o.digest=a.digest
    LEFT JOIN sessions.ingest_stage_intents c ON c.stage_token=o.active_stage_token
    WHERE t.claim_token=a.claim_token AND t.parent_stage_token IS NULL AND t.receipt_state='acknowledged'
     AND t.org_id=a.org_id AND t.workspace_id=a.workspace_id AND t.user_id=a.user_id AND t.origin_id=a.origin_id
     AND t.batch_id=a.batch_id AND t.digest=a.digest AND t.reset_epoch=a.reset_epoch
     AND (o.state='erased' OR o.active_stage_token=t.stage_token OR c.parent_stage_token=t.stage_token));
 IF v_ok<>1 THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: K winner has no accepted v1 decision';END IF;
 RETURN NULL;
END $kp$;
CREATE CONSTRAINT TRIGGER ack_k_provenance AFTER UPDATE ON sessions.ingest_lazy_native_slots
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.current_seat_key IS NOT NULL) EXECUTE FUNCTION sessions.ack_k_provenance();
-- Same provenance for v2 vectors: at commit the range is <= the committed clock and the batch is committed with
-- this ACK stage active (an owner-inserted vector without the clock advance / receipt cannot become authority).
CREATE FUNCTION sessions.ack_vector_provenance() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $vp$
DECLARE v_last bigint;
BEGIN
 SELECT last_seat INTO STRICT v_last FROM sessions.ingest_receipt_seat_clock WHERE singleton;
 IF (NEW.allocated_count>0 AND NEW.seat_hi>v_last) OR NEW.clock_before>v_last THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: vector ahead of the committed clock';END IF;
 -- full identity binding (review 7e646cf2): the ACK stage belongs to THIS claim, is the original acknowledged live stage,
 -- shares the claim's batch identity, and is the outbox's active stage for that same batch
 IF NOT EXISTS(SELECT 1 FROM sessions.ingest_claim_attempts a
  JOIN sessions.ingest_stage_intents t ON t.stage_token=NEW.ack_stage_token AND t.claim_token=NEW.claim_token AND t.claim_token=a.claim_token
   AND t.org_id=a.org_id AND t.workspace_id=a.workspace_id AND t.user_id=a.user_id AND t.origin_id=a.origin_id
   AND t.batch_id=a.batch_id AND t.digest=a.digest AND t.reset_epoch=a.reset_epoch AND t.parent_stage_token IS NULL
   AND t.receipt_state='acknowledged'
  JOIN sessions.ingest_batches b ON b.org_id=a.org_id AND b.workspace_id=a.workspace_id
   AND b.origin_id=a.origin_id AND b.batch_id=a.batch_id AND b.digest=a.digest AND b.user_id=a.user_id
  JOIN sessions.ingest_buffer_outbox o ON o.org_id=a.org_id AND o.workspace_id=a.workspace_id AND o.origin_id=a.origin_id AND o.batch_id=a.batch_id
   AND o.user_id=a.user_id AND o.digest=a.digest
  WHERE a.claim_token=NEW.claim_token AND b.state='committed' AND b.buffer_receipt_kind='staged' AND o.active_stage_token=NEW.ack_stage_token) THEN
  RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: vector without a committed receipt';END IF;
 RETURN NULL;
END $vp$;
CREATE CONSTRAINT TRIGGER ack_vector_provenance AFTER INSERT ON sessions.ingest_ack_vectors
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION sessions.ack_vector_provenance();
ALTER TABLE sessions.ingest_lazy_native_slots ENABLE ALWAYS TRIGGER ack_k_provenance;
ALTER TABLE sessions.ingest_ack_vectors ENABLE ALWAYS TRIGGER ack_vector_provenance;
REVOKE ALL ON FUNCTION sessions.ack_k_provenance(),sessions.ack_vector_provenance() FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;

-- every non-compat receipt seat (winner or not) must be <= the committed clock at commit (review: stale-on-legacy
-- seats allocate without a slot update; a writer skipping the clock advance would strand a future UNIQUE(seat) collision)
CREATE FUNCTION sessions.ack_seat_provenance() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $sp$
DECLARE v_last bigint;
BEGIN
 IF NEW.compat_vector_claim IS NOT NULL THEN RETURN NULL;END IF;
 SELECT last_seat INTO STRICT v_last FROM sessions.ingest_receipt_seat_clock WHERE singleton;
 IF NEW.seat>v_last THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='ack v2: receipt seat ahead of the committed clock';END IF;
 RETURN NULL;
END $sp$;
CREATE CONSTRAINT TRIGGER ack_seat_provenance AFTER INSERT ON sessions.ingest_receipt_seats
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION sessions.ack_seat_provenance();
ALTER TABLE sessions.ingest_receipt_seats ENABLE ALWAYS TRIGGER ack_seat_provenance;
REVOKE ALL ON FUNCTION sessions.ack_seat_provenance() FROM PUBLIC,anon,authenticated,service_role,bulls96_session_ingest,bulls96_session_ingest_seeder;
-- provenance lookup by seat key (review M2: avoid a full decisions scan per K winner at COMMIT)
CREATE INDEX ingest_lazy_ack_decisions_seat_key_idx ON sessions.ingest_lazy_ack_decisions(seat_key_sha256);
