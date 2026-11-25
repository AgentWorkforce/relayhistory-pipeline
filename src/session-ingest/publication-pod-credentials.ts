/** Credential boundary for a future hosted publisher target. Account and role are
 * supplied by the reviewed deployment contract. The selected S3/DB target must
 * separately verify bucket, key, region, database role and schema generation. */
import { constants } from "node:fs";
import { open, readlink } from "node:fs/promises";
import { request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";
import { signV4 } from "../session-pipeline/aws-json.ts";
import { xmlElements } from "../../packages/object-store/src/xml.ts";
import type { AwsCredentials } from "../../packages/object-store/src/sigv4.ts";

const TOKEN_DIR = "/var/run/secrets/pods.eks.amazonaws.com/serviceaccount";
const TOKEN_FILE = TOKEN_DIR + "/eks-pod-identity-token";
const POD_URIS = [
  "http://169.254.170.23/v1/credentials",
  "http://[fd00:ec2::23]/v1/credentials",
];
const STS_URL = "https://sts.us-east-1.amazonaws.com/";
const TTL_MARGIN = 300_000;

function refuse(code: string): never {
  throw Object.assign(new Error("publication_pod_" + code), { code: "publication_pod_" + code });
}

async function projectedToken(): Promise<string> {
  const fd = await open(TOKEN_FILE, constants.O_RDONLY | constants.O_NONBLOCK);
  try {
    // Kubernetes rotates projected volumes through symlinks. Check the opened
    // inode, rather than checking a symlink and then reopening the path.
    const actual = await readlink("/proc/self/fd/" + fd.fd);
    const stat = await fd.stat();
    if (!actual.startsWith(TOKEN_DIR + "/") || !stat.isFile() || stat.size < 1 || stat.size > 16384) refuse("token_file");
    const buf = Buffer.alloc(stat.size + 1);
    let offset = 0;
    while (offset < buf.length) {
      const { bytesRead } = await fd.read(buf, offset, buf.length - offset, offset);
      if (!bytesRead) break;
      offset += bytesRead;
    }
    if (offset !== stat.size) refuse("token_file");
    return buf.toString("utf8", 0, offset).trim();
  } finally {
    await fd.close();
  }
}

type Request = (url: string, method: "GET" | "POST", headers: Record<string, string>, body: string, signal: AbortSignal) => Promise<string>;

// Dedicated direct sockets: no environment proxy, SDK chain, redirects or
// response-body logging. STS uses the ordinary verified TLS endpoint.
const directRequest: Request = (url, method, headers, body, signal) => new Promise((resolve, reject) => {
  const transport = url === STS_URL ? httpsRequest : httpRequest;
  const req = transport(url, { method, headers, signal, agent: false, rejectUnauthorized: true, maxHeaderSize: 16384 }, res => {
    const chunks: Buffer[] = [];
    let size = 0;
    res.on("error", reject);
    if (res.statusCode !== 200) { reject(new Error("http")); res.destroy(); return; }
    res.on("data", (chunk: Buffer) => {
      size += chunk.length;
      if (size > 32768) { reject(new Error("body_bound")); res.destroy(); return; }
      chunks.push(chunk);
    });
    res.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
  });
  req.on("error", reject);
  req.end(body || undefined);
});

export function publicationPodCredentials(
  env: NodeJS.ProcessEnv,
  expected: { accountId: string; roleName: string },
  tests: { request?: Request; readToken?: () => Promise<string>; now?: () => number } = {},
): (signal?: AbortSignal) => Promise<AwsCredentials> {
  const uri = env.AWS_CONTAINER_CREDENTIALS_FULL_URI;
  if (!uri || !POD_URIS.includes(uri) || env.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE !== TOKEN_FILE) refuse("binding");
  if (["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_AUTHORIZATION_TOKEN"].some(name => env[name] !== undefined)) refuse("mixed_identity");
  const { accountId, roleName } = expected;
  if (!/^\d{12}$/.test(accountId) || !/^[A-Za-z0-9_+=,.@-]{1,64}$/.test(roleName)) refuse("identity_config");
  const expectedPrefix = "arn:aws:sts::" + accountId + ":assumed-role/" + roleName + "/";
  const request = tests.request ?? directRequest;
  const now = tests.now ?? Date.now;
  type Verified = { value: AwsCredentials; verified: number; expires: number };
  // Export the authority deadline to downstream caches as well. STS identity
  // must be rechecked even when the underlying pod session lasts for hours.
  const snapshot = (entry: Verified): AwsCredentials => ({
    ...entry.value, expiresAt: new Date(Math.min(entry.expires - TTL_MARGIN, entry.verified + TTL_MARGIN)),
  });
  let cached: Verified | undefined;
  let pending: Promise<Verified> | undefined;
  const refresh = async (caller?: AbortSignal): Promise<Verified> => {
    const signal = AbortSignal.any([AbortSignal.timeout(5000), ...(caller ? [caller] : [])]);
    let token: string;
    try { token = await (tests.readToken ?? projectedToken)(); } catch { return refuse("token_file"); }
    if (!/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(token) || token.length > 16384) refuse("token");
    signal.throwIfAborted();
    let v: Record<string, unknown>;
    try { v = JSON.parse(await request(uri, "GET", { authorization: token }, "", signal)); } catch { return refuse("endpoint"); }
    if (!v || typeof v !== "object" || typeof v.AccessKeyId !== "string" || !/^ASIA[A-Z0-9]{16}$/.test(v.AccessKeyId) ||
        typeof v.SecretAccessKey !== "string" || !/^[!-~]{16,256}$/.test(v.SecretAccessKey) ||
        typeof v.Token !== "string" || !/^[!-~]{1,8192}$/.test(v.Token) || typeof v.Expiration !== "string") refuse("credentials");
    const expires = Date.parse(v.Expiration);
    if (!Number.isFinite(expires) || expires <= now() + TTL_MARGIN) refuse("expiration");
    const value = { accessKeyId: v.AccessKeyId, secretAccessKey: v.SecretAccessKey, sessionToken: v.Token };
    const body = "Action=GetCallerIdentity&Version=2011-06-15";
    const headers = signV4({ method: "POST", url: new URL(STS_URL), service: "sts", region: "us-east-1",
      headers: { "content-type": "application/x-www-form-urlencoded" }, body, credentials: value, now: new Date(now()) });
    let xml: string;
    try { xml = await request(STS_URL, "POST", headers, body, signal); } catch { return refuse("sts"); }
    const results = xmlElements(xml, "GetCallerIdentityResult");
    const accounts = xmlElements(results[0] ?? "", "Account"), arns = xmlElements(results[0] ?? "", "Arn");
    const arn = arns[0] ?? "";
    if (results.length !== 1 || accounts.length !== 1 || accounts[0] !== accountId || arns.length !== 1 ||
        !arn.startsWith(expectedPrefix) || !/^[A-Za-z0-9_+=,.@-]{2,64}$/.test(arn.slice(expectedPrefix.length))) refuse("identity");
    signal.throwIfAborted();
    if (expires <= now() + TTL_MARGIN) refuse("expiration");
    cached = { value, expires, verified: now() };
    return cached;
  };
  return async caller => {
    caller?.throwIfAborted();
    const start = now();
    if (cached && start >= cached.verified && start - cached.verified < TTL_MARGIN && cached.expires > start + TTL_MARGIN) return snapshot(cached);
    cached = undefined;
    pending ??= refresh(caller).finally(() => { pending = undefined; });
    const value = await pending;
    caller?.throwIfAborted();
    return snapshot(value);
  };
}
