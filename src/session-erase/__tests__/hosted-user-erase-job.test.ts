import { expect, test } from "bun:test";

const template = new URL("../../../deploy/operator-user-erase/job.yaml.example", import.meta.url);
const job = Bun.YAML.parse(await Bun.file(template).text()) as {
  kind: string;
  spec: {
    suspend: boolean;
    completions: number;
    parallelism: number;
    backoffLimit: number;
    template: {
      spec: {
        restartPolicy: string;
        automountServiceAccountToken: boolean;
        containers: Array<{
          env: Array<{ name: string; value: string }>;
          volumeMounts: Array<{ name: string; mountPath: string; readOnly: boolean }>;
        }>;
        volumes: Array<{ name: string; csi?: { readOnly: boolean }; secret?: { items: Array<{ path: string }> } }>;
      };
    };
  };
};
const pod = job.spec.template.spec;

test("hosted erase runs once and requires explicit release and enablement", () => {
  expect(job.kind).toBe("Job");
  expect(job.spec).toMatchObject({ suspend: true, completions: 1, parallelism: 1, backoffLimit: 0 });
  expect(pod.restartPolicy).toBe("Never");
  expect(pod.automountServiceAccountToken).toBe(false);
  expect(pod.containers).toHaveLength(1);
  const [container] = pod.containers;
  expect(container).not.toHaveProperty("command");
  expect(container).not.toHaveProperty("args");
  expect(container!.env).toContainEqual({ name: "SERVICE", value: "user-erase" });
  expect(container!.env).toContainEqual({ name: "SUMMARY_DECISION_RECORDS_ENABLED", value: "true" });
  expect(container!.env).toContainEqual({ name: "OPERATOR_USER_ERASE_ENABLED", value: "0" });
});

test("request, owner and complete STS tuple use dedicated read-only mounts", () => {
  const container = pod.containers[0]!;
  const env = Object.fromEntries(container.env.map(({ name, value }) => [name, value]));
  expect(Object.keys(env)).toHaveLength(container.env.length);
  const inputs = [
    ["request", "OPERATOR_USER_ERASE_REQUEST", "request.json"],
    ["owner", "OPERATOR_USER_ERASE_DATABASE_URL", "url"],
    ["sts", "SESSION_ERASE_S3_ACCESS_KEY_ID", "access-key-id"],
    ["sts", "SESSION_ERASE_S3_SECRET_ACCESS_KEY", "secret-access-key"],
    ["sts", "SESSION_ERASE_S3_SESSION_TOKEN", "session-token"],
  ] as const;
  for (const [volume, variable, file] of inputs) {
    const mount = container.volumeMounts.find(({ name }) => name === volume);
    expect(mount?.readOnly).toBe(true);
    expect(env[variable]).toBeUndefined();
    expect(env[variable + "_FILE"]).toBe(mount?.mountPath + "/" + file);
    const source = pod.volumes.find(({ name }) => name === volume);
    if (volume === "sts") expect(source?.csi?.readOnly).toBe(true);
    else expect(source?.secret?.items.map(({ path }) => path)).toContain(file);
  }
});
