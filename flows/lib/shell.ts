// Every deterministic step runs under /bin/sh; this is the one way a string
// becomes a shell argument in these flows.
export const shellWord = (value: string): string => `'${value.replaceAll("'", "'\\''")}'`;
