-- Disposable PGlite, synthetic single origin. This schema is not deployable migration SQL.
CREATE TEMP TABLE gate(singleton boolean PRIMARY KEY CHECK(singleton), enabled boolean NOT NULL,
  generation text NOT NULL, live_leases integer NOT NULL, fixture_now timestamptz NOT NULL);
CREATE TEMP TABLE approval(nonce uuid PRIMARY KEY, command jsonb NOT NULL, expected_old jsonb, used boolean NOT NULL DEFAULT false);
CREATE TEMP TABLE frontier(singleton boolean PRIMARY KEY CHECK(singleton), image jsonb NOT NULL);
CREATE TEMP TABLE audit(nonce uuid PRIMARY KEY, digest text NOT NULL);
CREATE FUNCTION pg_temp.try_install(command_bytes text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE c jsonb := command_bytes::jsonb; a approval%ROWTYPE; g gate%ROWTYPE; old jsonb; proposed jsonb := c->'row'; d text;
BEGIN
 SELECT * INTO STRICT g FROM gate WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT a FROM approval WHERE nonce=(c->>'nonce')::uuid FOR UPDATE;
 SELECT image INTO old FROM frontier WHERE singleton FOR UPDATE;
 IF NOT g.enabled OR g.live_leases<>0 OR a.used OR a.command IS DISTINCT FROM c OR
    g.generation IS DISTINCT FROM proposed->>'schema_generation' OR a.expected_old IS DISTINCT FROM old OR
    (proposed->>'valid_until')::timestamptz < g.fixture_now + interval '150 seconds' OR
    (old IS NOT NULL AND (proposed->>'valid_until')::timestamptz <= (old->>'valid_until')::timestamptz)
 THEN RAISE EXCEPTION 'fixture gate refused'; END IF;
 INSERT INTO frontier VALUES(true,proposed) ON CONFLICT(singleton) DO UPDATE SET image=EXCLUDED.image;
 UPDATE approval SET used=true WHERE nonce=a.nonce;
 d:=encode(sha256(convert_to(command_bytes,'UTF8')),'hex');
 INSERT INTO audit VALUES(a.nonce,d);
 RETURN d;
END $$;
