/**
 * Support codes shared across AgentWorkforce repos (infra, cloud, sessions-pipeline): one
 * code and one doc link per customer-fixable failure, so a log line, a runbook and
 * infra's diagnose-install.py point at the same fix. They are additive: the existing
 * `error_code` stays the machine key; `support_code` and `doc` ride beside it.
 *
 * Closed values only: log.ts emits `support_code` and `doc` as enum fields, so neither
 * can ever carry free text, a URL with credentials or a password.
 */

/** A database login problem: a configured role was not found, or the migration login is not the intended owner. */
export const AR_DB_LOGIN = "AR_DB_LOGIN";
export const AR_DB_LOGIN_DOC =
  "https://agentrelay.com/docs/self-hosting/troubleshooting#database-logins";

export const SUPPORT_CODE_VALUES: ReadonlySet<string> = new Set([AR_DB_LOGIN]);
export const SUPPORT_DOC_VALUES: ReadonlySet<string> = new Set([AR_DB_LOGIN_DOC]);
