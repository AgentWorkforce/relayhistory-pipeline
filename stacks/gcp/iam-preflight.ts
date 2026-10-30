const IAM_ORIGIN = "https://iam.googleapis.com";
const SERVICE_ACCOUNT =
  /^[a-z][a-z0-9-]{4,28}[a-z0-9]@[a-z][a-z0-9-]{4,28}[a-z0-9]\.iam\.gserviceaccount\.com$/;

export interface GcpBearerCredential {
  readonly bearer: string;
}

export interface ServiceAccountActAsProof {
  proof: "iam.serviceAccounts.actAs";
  serviceAccountEmail: string;
}

export type IamPreflightFetch = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;

function fail(message: string): never {
  throw new Error(`96bulls: ${message} GKE was not created or changed.`);
}

/** Read-only testIamPermissions check for roles/iam.serviceAccountUser's actAs permission. */
export async function requireServiceAccountActAs(
  projectId: string,
  serviceAccountEmail: string,
  credential: GcpBearerCredential,
  fetcher: IamPreflightFetch = globalThis.fetch,
): Promise<ServiceAccountActAsProof> {
  if (!/^[a-z][a-z0-9-]{4,28}[a-z0-9]$/.test(projectId) || !SERVICE_ACCOUNT.test(serviceAccountEmail))
    fail("node service-account actAs preflight target is invalid.");
  if (
    !credential?.bearer ||
    credential.bearer.length < 16 ||
    credential.bearer.length > 8192 ||
    /\s/.test(credential.bearer)
  )
    fail("node service-account actAs preflight credential is missing.");
  const url = new URL(
    `/v1/projects/${projectId}/serviceAccounts/${encodeURIComponent(serviceAccountEmail)}:testIamPermissions`,
    IAM_ORIGIN,
  );
  let response: Response;
  try {
    response = await fetcher(url, {
      method: "POST",
      headers: {
        accept: "application/json",
        authorization: `Bearer ${credential.bearer}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({ permissions: ["iam.serviceAccounts.actAs"] }),
      redirect: "error",
    });
  } catch {
    fail("could not verify iam.serviceAccounts.actAs on the node service account.");
  }
  if (!response.ok) fail("could not verify iam.serviceAccounts.actAs on the node service account.");
  let body: unknown;
  try {
    body = await response.json();
  } catch {
    fail("node service-account actAs preflight returned malformed evidence.");
  }
  const permissions = (body as { permissions?: unknown })?.permissions;
  if (!Array.isArray(permissions) || !permissions.includes("iam.serviceAccounts.actAs"))
    fail("deployer lacks iam.serviceAccounts.actAs (roles/iam.serviceAccountUser) on the node service account.");
  return { proof: "iam.serviceAccounts.actAs", serviceAccountEmail };
}
