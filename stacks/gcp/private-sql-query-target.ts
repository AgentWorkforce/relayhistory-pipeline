import { isIP } from "node:net";

/** In-cluster Cloud SQL Auth Proxy. The stack does not put a DSN in the chart. */
export const PRIVATE_SQL_PROXY_HOST = "cloud-sql-proxy.96bulls.svc";
export const PRIVATE_SQL_PROXY_PORT = 5432;

/** PSA range reserved for the private Cloud SQL endpoint. */
const PRIVATE_SQL_PREFIX = 0x0a500000;
const PRIVATE_SQL_MASK = 0xfffff000;

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;

export interface PrivateSqlQueryTargetInput {
  host: string;
  port: number;
  database: string;
  role: string;
  /** Rejected when present. The password stays in an external Secret. */
  password?: string;
  /** Reported server address from a later probe. Optional at contract time. */
  serverAddress?: string;
}

export interface PrivateSqlQueryTarget {
  host: typeof PRIVATE_SQL_PROXY_HOST;
  port: typeof PRIVATE_SQL_PROXY_PORT;
  database: string;
  role: string;
  serverAddress?: string;
}

/**
 * The only query target a later authenticated check may use.
 * This function does not open a connection and is not called by the stack.
 */
export function privateSqlQueryTarget(input: PrivateSqlQueryTargetInput): PrivateSqlQueryTarget {
  if (input.password !== undefined)
    throw new Error("96bulls: private SQL query target rejects a password");
  if (looksLikeUrl(input.host) || looksLikeUrl(input.database) || looksLikeUrl(input.role))
    throw new Error("96bulls: private SQL query target rejects a URL");
  if (input.host !== PRIVATE_SQL_PROXY_HOST)
    throw new Error("96bulls: private SQL query target host must be cloud-sql-proxy.96bulls.svc");
  if (input.port !== PRIVATE_SQL_PROXY_PORT)
    throw new Error("96bulls: private SQL query target port must be 5432");
  if (!IDENTIFIER.test(input.database))
    throw new Error("96bulls: private SQL database must be an unquoted identifier");
  if (!IDENTIFIER.test(input.role))
    throw new Error("96bulls: private SQL role must be an unquoted identifier");
  if (input.serverAddress !== undefined) requirePrivateSqlServerAddress(input.serverAddress);
  return {
    host: PRIVATE_SQL_PROXY_HOST,
    port: PRIVATE_SQL_PROXY_PORT,
    database: input.database,
    role: input.role,
    ...(input.serverAddress === undefined ? {} : { serverAddress: input.serverAddress }),
  };
}

function looksLikeUrl(value: string): boolean {
  return value.includes("://") || value.includes("@") || value.includes("/") || value.includes("?");
}

function requirePrivateSqlServerAddress(address: string): void {
  if (isIP(address) !== 4)
    throw new Error("96bulls: private SQL server address must be an IPv4 address");
  const [a, b, c, d] = address.split(".").map((part) => Number(part));
  const value = (((a! << 24) | (b! << 16) | (c! << 8) | d!) >>> 0);
  if ((value & PRIVATE_SQL_MASK) !== PRIVATE_SQL_PREFIX)
    throw new Error("96bulls: private SQL server address must be inside 10.80.0.0/20");
}
