import { expect, test } from "bun:test";
import { publicationPodCredentials } from "../publication-pod-credentials.ts";

const env = {
  AWS_CONTAINER_CREDENTIALS_FULL_URI: "http://169.254.170.23/v1/credentials",
  AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE: "/var/run/secrets/pods.eks.amazonaws.com/serviceaccount/eks-pod-identity-token",
};
const identity = { accountId: "111122223333", roleName: "test-session-publication" };
const role = "arn:aws:sts::111122223333:assumed-role/test-session-publication/test-session";
const sts = (arn = role) => '<GetCallerIdentityResponse><GetCallerIdentityResult><Arn>' + arn +
  '</Arn><Account>111122223333</Account></GetCallerIdentityResult></GetCallerIdentityResponse>';

test("only the exact EKS IPv4 or IPv6 agent URI and projected token path are admitted", () => {
  for (const uri of ["http://169.254.169.254/v1/credentials", "https://example.test/v1/credentials",
    env.AWS_CONTAINER_CREDENTIALS_FULL_URI + "?next=elsewhere", "http://127.0.0.1/v1/credentials"])
    expect(() => publicationPodCredentials({ ...env, AWS_CONTAINER_CREDENTIALS_FULL_URI: uri }, identity)).toThrow("publication_pod_binding");
  expect(() => publicationPodCredentials({ ...env, AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE: "/tmp/token" }, identity)).toThrow("publication_pod_binding");
  expect(typeof publicationPodCredentials({ ...env, AWS_CONTAINER_CREDENTIALS_FULL_URI: "http://[fd00:ec2::23]/v1/credentials" }, identity)).toBe("function");
  for (const key of ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_AUTHORIZATION_TOKEN"])
    expect(() => publicationPodCredentials({ ...env, [key]: "" }, identity)).toThrow("publication_pod_mixed_identity");
});

test("only STS-verified sessions are cached; re-reads projected token on rotation; refuses wrong role without stale fallback", async () => {
  let clock = Date.UTC(2026, 0, 1), keys = 0, stsCalls = 0, tokens = 0, currentRole = role;
  const get = publicationPodCredentials(env, identity, {
    now: () => clock, readToken: async () => "test.synthetic.t" + ++tokens,
    request: async (url, method, headers, body) => {
      if (url === env.AWS_CONTAINER_CREDENTIALS_FULL_URI) {
        expect(method).toBe("GET");
        expect(headers.authorization).toBe("test.synthetic.t" + tokens);
        return JSON.stringify({ AccessKeyId: "ASIA" + String(++keys).padStart(16, "0"), SecretAccessKey: "test-only-secret-shape",
          Token: "synthetic-session-" + keys, Expiration: new Date(clock + 600_000).toISOString() });
      }
      expect(url).toBe("https://sts.us-east-1.amazonaws.com/");
      expect(method).toBe("POST");
      expect(headers.authorization).toContain("/us-east-1/sts/aws4_request");
      expect(headers["x-amz-security-token"]).toBe("synthetic-session-" + keys);
      expect(body).toBe("Action=GetCallerIdentity&Version=2011-06-15");
      stsCalls++;
      return sts(currentRole);
    },
  });
  const [first, concurrent] = await Promise.all([get(), get()]);
  expect(first).toEqual(concurrent);
  first.accessKeyId = "caller-mutated";
  expect((await get()).accessKeyId).toBe("ASIA0000000000000001");
  expect([keys, stsCalls]).toEqual([1, 1]);
  clock += 300_000;
  expect((await get()).accessKeyId).toBe("ASIA0000000000000002");
  clock += 300_000;
  currentRole = role.replace("test-session-publication", "another-role");
  await expect(get()).rejects.toThrow("publication_pod_identity");
  await expect(get()).rejects.toThrow("publication_pod_identity");
  expect([keys, stsCalls, tokens]).toEqual([4, 4, 4]);
});

test("refuses malformed responses, insufficient lifetime, mismatched or ambiguous identity and aborted calls", async () => {
  const clock = Date.UTC(2026, 0, 1);
  const doc = { AccessKeyId: "ASIA1234567890123456", SecretAccessKey: "test-only-secret-shape", Token: "synthetic",
    Expiration: new Date(clock + 600_000).toISOString() };
  const fake = (first: string, second: string) => publicationPodCredentials(env, identity, {
    readToken: async () => "a.b.c", now: () => clock, request: async url => url === env.AWS_CONTAINER_CREDENTIALS_FULL_URI ? first : second,
  });
  await expect(fake("not json", "")()).rejects.toThrow("publication_pod_endpoint");
  await expect(fake(JSON.stringify({ ...doc, Token: "" }), "")()).rejects.toThrow("publication_pod_credentials");
  await expect(fake(JSON.stringify({ ...doc, Expiration: new Date(clock + 300_000).toISOString() }), "")()).rejects.toThrow("publication_pod_expiration");
  for (const bad of [sts() + sts(), sts().replace("<Account>", "<Account>123456789012</Account><Account>"),
    sts().replace("<Account>111122223333", "<Account>111111111111"), sts(role + "/extra")])
    await expect(fake(JSON.stringify(doc), bad)()).rejects.toThrow("publication_pod_identity");
  await expect(fake(JSON.stringify(doc), sts())(AbortSignal.abort())).rejects.toThrow();
});
