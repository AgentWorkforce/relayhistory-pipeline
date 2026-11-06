-- Registry v3.1 slot 037000: the directory-document projection pool.
-- Provenance: third pool after the signed-off 030000 event+brief contract.
-- Metadata-only: widens the page-ack pool CHECK and adds a three-terminal
-- canonical_feature_document_finish; the 030000 two-terminal RPC is untouched.
ALTER TABLE sessions.canonical_feature_page_ack
  DROP CONSTRAINT IF EXISTS canonical_feature_page_ack_pool_check;
ALTER TABLE sessions.canonical_feature_page_ack
  ADD CONSTRAINT canonical_feature_page_ack_pool_check
  CHECK (pool IN ('event','brief','document'));
-- Three pools need the v3 RPC: identical chain-walk + CAS as the two-terminal
-- contract, with 'document' as the third pool and a third terminal hash.
CREATE FUNCTION sessions.canonical_feature_document_finish(
  p_revision bigint,
  p_source_authority jsonb,
  p_profile_binding text,
  p_terminal_event text,
  p_terminal_brief text,
  p_terminal_document text,
  p_target_binding text
) RETURNS boolean
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path='' AS $finish$
DECLARE
  v_st sessions.canonical_feature_copy_state%ROWTYPE;
  v_pool text;
  v_cursor text;
  v_next text;
  v_seen text[];
  v_steps int;
  v_count int;
  v_terminal int;
BEGIN
  SELECT * INTO v_st FROM sessions.canonical_feature_copy_state
    WHERE entry_revision=p_revision FOR UPDATE;
  IF NOT FOUND OR NOT v_st.dirty
     OR v_st.source_authority IS DISTINCT FROM p_source_authority
     OR v_st.profile_binding IS DISTINCT FROM p_profile_binding
     OR v_st.target_binding IS DISTINCT FROM p_target_binding
  THEN RETURN false; END IF;
  IF NOT EXISTS(SELECT 1 FROM sessions.canonical_feature_journal j
    WHERE j.revision=p_revision AND j.image_key=v_st.image_key
      AND j.kind=v_st.kind
      AND j.source_authority=v_st.source_authority
      AND j.pointer_set=v_st.pointer_set)
  THEN RETURN false; END IF;
  FOR v_pool IN SELECT pool FROM (VALUES('event'),('brief'),('document'))p(pool)
  LOOP
    IF EXISTS(SELECT 1 FROM sessions.canonical_feature_page_ack
      WHERE entry_revision=p_revision AND kind<>v_st.kind)
    THEN RETURN false; END IF;
    SELECT count(*) INTO v_count FROM sessions.canonical_feature_page_ack
      WHERE entry_revision=p_revision AND kind=v_st.kind AND pool=v_pool;
    IF v_count=0 THEN RETURN false; END IF;
    SELECT count(*) INTO v_terminal FROM sessions.canonical_feature_page_ack
      WHERE entry_revision=p_revision AND kind=v_st.kind
        AND pool=v_pool AND next_cursor IS NULL;
    IF v_terminal<>1 THEN RETURN false; END IF;
    v_cursor:='start'; v_steps:=0; v_seen:=ARRAY[]::text[];
    LOOP
      IF v_cursor=ANY(v_seen) THEN RETURN false; END IF;
      v_seen:=v_seen||v_cursor;
      SELECT next_cursor INTO v_next FROM sessions.canonical_feature_page_ack
        WHERE entry_revision=p_revision AND kind=v_st.kind
          AND pool=v_pool AND cursor=v_cursor;
      IF NOT FOUND THEN RETURN false; END IF;
      v_steps:=v_steps+1;
      EXIT WHEN v_next IS NULL;
      v_cursor:=v_next;
    END LOOP;
    IF v_steps<>v_count THEN RETURN false; END IF;
    IF NOT EXISTS(SELECT 1 FROM sessions.canonical_feature_page_ack
      WHERE entry_revision=p_revision AND kind=v_st.kind AND pool=v_pool
        AND next_cursor IS NULL
        AND page_sha256 = CASE v_pool
          WHEN 'event' THEN p_terminal_event
          WHEN 'brief' THEN p_terminal_brief
          ELSE p_terminal_document END)
    THEN RETURN false; END IF;
  END LOOP;
  UPDATE sessions.canonical_feature_copy_state SET dirty=false
    WHERE entry_revision=p_revision;
  RETURN true;
END;
$finish$;
REVOKE ALL ON FUNCTION sessions.canonical_feature_document_finish(bigint,jsonb,text,text,text,text,text)
  FROM PUBLIC,anon,authenticated,service_role,
  bulls96_session_ingest,bulls96_session_ingest_seeder;
GRANT EXECUTE ON FUNCTION sessions.canonical_feature_document_finish(bigint,jsonb,text,text,text,text,text)
  TO bulls96_canonical_feature_producer;
