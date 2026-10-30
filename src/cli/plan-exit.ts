/** Whether a spawned plan must end this process. `allowFail` stays an explicit opt-out. */
export function planChildMustFailClosed(allowFail: boolean | undefined, status: number | null): boolean {
  return allowFail !== true && status !== 0;
}

/** Exit code for a fail-closed child. A missing status is 1, never 0. */
export function planChildExitCode(status: number | null): number {
  return status ?? 1;
}
