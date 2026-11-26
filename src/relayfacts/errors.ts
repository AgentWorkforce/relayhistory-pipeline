/**
 * Errors the relayfacts tools print. A tool's own refusal (UsageError and its kin) carries a message written
 * here, never data; anything else (a database, provider, parse or system error) is reduced to a fixed code,
 * since its message can carry provider, database or session text.
 */
export class UsageError extends Error {
  constructor(message: string) { super(message); this.name = "UsageError"; }
}

/** A fixed error code for a log line: never a message. */
export function errorCode(error: unknown): string {
  const e = error as { reason?: unknown; code?: unknown; name?: unknown; constructor?: { name?: string } };
  for (const v of [e.reason, e.code, e.name, e.constructor?.name]) if (typeof v === "string" && v !== "Object" && /^[A-Za-z0-9_.:-]{1,64}$/u.test(v)) return v;
  return "error";
}

/** What a CLI prints for an error: the message of its own refusals, the code of anything else. */
export function printable(error: unknown, own: ReadonlyArray<abstract new (...args: never[]) => Error> = []): string {
  return error instanceof UsageError || own.some((c) => error instanceof c) ? (error as Error).message : errorCode(error);
}
