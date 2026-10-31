-- Binds PostgREST's login role to the search Worker and nothing else. Applied after
-- the migrations, as a reviewed deployment step. Set the password out of band.
-- NOINHERIT is required: with INHERIT, the role itself would hold EXECUTE on the RPC.
CREATE ROLE session_directory_authenticator LOGIN NOINHERIT NOCREATEDB NOCREATEROLE NOBYPASSRLS;
GRANT session_directory_search_worker TO session_directory_authenticator;
