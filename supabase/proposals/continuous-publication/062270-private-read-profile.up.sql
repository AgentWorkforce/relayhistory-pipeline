-- Component of the proposed post-H receipt capture server. No public RPC,
-- enrollment writer or positive completeness result is installed by this file.
BEGIN;
CREATE SCHEMA frontier_receipt_private;
REVOKE ALL ON SCHEMA frontier_receipt_private FROM PUBLIC;
CREATE TABLE frontier_receipt_private.read_profiles (
 profile_id uuid PRIMARY KEY,
 profile_sha256 text NOT NULL CHECK (profile_sha256 ~ '^[a-f0-9]{64}$'),
 login_oid oid NOT NULL, login_name name NOT NULL,
 tenant jsonb NOT NULL CHECK (jsonb_typeof(tenant)='object'),
 generation text NOT NULL CHECK (generation ~ '^[a-f0-9]{64}$'),
 target jsonb NOT NULL CHECK (jsonb_typeof(target)='object'),
 expected_authority jsonb NOT NULL CHECK (jsonb_typeof(expected_authority)='object'),
 starts_at timestamptz NOT NULL CHECK (isfinite(starts_at)),
 expires_at timestamptz NOT NULL CHECK (isfinite(expires_at) AND expires_at>starts_at),
 page_limit int NOT NULL CHECK (page_limit BETWEEN 1 AND 1000),
 max_decisions int NOT NULL CHECK (max_decisions BETWEEN 0 AND 1000000),
 max_response_bytes int NOT NULL CHECK (max_response_bytes BETWEEN 1024 AND 67108864)
);
-- An owner-controlled revocation record preserves the original profile image.
CREATE TABLE frontier_receipt_private.revoked_profiles (
 profile_id uuid PRIMARY KEY REFERENCES frontier_receipt_private.read_profiles,
 revoked_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
REVOKE ALL ON ALL TABLES IN SCHEMA frontier_receipt_private FROM PUBLIC;
CREATE FUNCTION frontier_receipt_private.immutable_record() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 RAISE EXCEPTION 'receipt capture: append-only profile ledger';
END $$;
CREATE TRIGGER keep_read_profile BEFORE UPDATE OR DELETE OR TRUNCATE ON frontier_receipt_private.read_profiles
 FOR EACH STATEMENT EXECUTE FUNCTION frontier_receipt_private.immutable_record();
CREATE TRIGGER keep_revocation BEFORE UPDATE OR DELETE OR TRUNCATE ON frontier_receipt_private.revoked_profiles
 FOR EACH STATEMENT EXECUTE FUNCTION frontier_receipt_private.immutable_record();
REVOKE ALL ON FUNCTION frontier_receipt_private.immutable_record() FROM PUBLIC;

CREATE FUNCTION frontier_receipt_private.check_request(p jsonb)
RETURNS frontier_receipt_private.read_profiles
LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog,pg_temp AS $$
DECLARE profile frontier_receipt_private.read_profiles; reader record; login record;
BEGIN
 IF current_setting('transaction_read_only')<>'on' OR current_setting('transaction_isolation')<>'repeatable read' OR
    current_setting('role',true) IS DISTINCT FROM 'bulls96_frontier_receipt_reader' THEN
   RAISE EXCEPTION USING ERRCODE='42501', MESSAGE='receipt capture: restricted reader transaction required';
 END IF;
 SELECT * INTO STRICT reader FROM pg_roles WHERE rolname='bulls96_frontier_receipt_reader';
 SELECT * INTO STRICT login FROM pg_roles WHERE rolname=session_user;
 IF reader.rolsuper OR reader.rolinherit OR reader.rolcreaterole OR reader.rolcreatedb OR reader.rolreplication OR
    reader.rolbypassrls OR reader.rolcanlogin OR login.rolsuper OR login.rolinherit OR login.rolcreaterole OR
    login.rolcreatedb OR login.rolreplication OR login.rolbypassrls OR NOT login.rolcanlogin OR
    EXISTS(SELECT FROM pg_auth_members m WHERE member=reader.oid OR
      (member=login.oid AND (roleid<>reader.oid OR admin_option OR
       coalesce((to_jsonb(m)->>'inherit_option')::boolean,false) OR
       NOT coalesce((to_jsonb(m)->>'set_option')::boolean,true)))) THEN
   RAISE EXCEPTION USING ERRCODE='42501', MESSAGE='receipt capture: unexpected role privileges';
 END IF;
 PERFORM sessions.ingest_exact_keys(p,ARRAY['protocol','profileId','profileSha256','tenant','generation','after','limit']);
 IF p->>'protocol' IS DISTINCT FROM 'first-ack-seat-capture-v1' OR jsonb_typeof(p->'limit')<>'number' OR
    p->>'limit' !~ '^[1-9][0-9]{0,3}$' OR octet_length(p::text)>16384 THEN
   RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='receipt capture: invalid request';
 END IF;
 SELECT * INTO profile FROM frontier_receipt_private.read_profiles pr WHERE pr.profile_id=(p->>'profileId')::uuid
   AND (pr.login_oid,pr.login_name)=(login.oid,login.rolname) AND pr.profile_sha256=p->>'profileSha256'
   AND pr.tenant=p->'tenant' AND pr.generation=p->>'generation'
   AND (p->>'limit')::int<=pr.page_limit
   AND pr.starts_at<=clock_timestamp() AND pr.expires_at>clock_timestamp()
   AND NOT EXISTS(SELECT FROM frontier_receipt_private.revoked_profiles r WHERE r.profile_id=pr.profile_id);
 IF NOT FOUND THEN
   RAISE EXCEPTION USING ERRCODE='42501', MESSAGE='receipt capture: no admitted read profile';
 END IF;
 -- The qualified enumerator must validate the cursor and compare LIVE
 -- authority/target, domain count/hash and work/byte bounds on each page.
 -- Nothing in this helper produces or certifies a row of the receipt domain.
 RETURN profile;
END $$;
REVOKE ALL ON FUNCTION frontier_receipt_private.check_request(jsonb) FROM PUBLIC;
COMMIT;
