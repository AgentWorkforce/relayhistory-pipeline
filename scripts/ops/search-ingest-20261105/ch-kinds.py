# READ ONLY. ClickHouse creds from the session pipeline's Secrets Manager secret (RAM only).
# Lists DISTINCT (org_id, session_id) with trajectory/commit_link rows; writes JSONL to stdout.
import base64, json, os, subprocess, sys, urllib.request
env = {**os.environ, "AWS_CONFIG_FILE": "/Volumes/T7/Projects/AgentWorkforce/artifacts/migration-ops-20261102/pippen-existing-workload-sso.config",
       "AWS_PROFILE": "sessions-prod", "AWS_REGION": "us-east-1"}
for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"): env.pop(k, None)
arn = subprocess.run(["aws", "lambda", "get-function-configuration", "--function-name", "96bulls-aws-sessions-prod-session-pipeline",
                      "--query", "Environment.Variables.SESSION_PIPELINE_SECRET_ARN", "--output", "text"], env=env, capture_output=True, text=True, check=True).stdout.strip()
sec = json.loads(subprocess.run(["aws", "secretsmanager", "get-secret-value", "--secret-id", arn, "--query", "SecretString", "--output", "text"],
                                env=env, capture_output=True, text=True, check=True).stdout)
ch = sec["clickhouse"]; sec = None
auth = "Basic " + base64.b64encode(f"{ch['user']}:{ch['password']}".encode()).decode()
url, db = ch["url"].rstrip("/"), ch["database"]; ch = None
def q(sql, fmt="JSONCompact"):
    r = urllib.request.Request(f"{url}/?database={db}&default_format={fmt}&readonly=2&max_execution_time=60", data=sql.encode(), headers={"Authorization": auth})
    try:
        body = urllib.request.urlopen(r, timeout=120).read().decode()
        return json.loads(body)["data"] if fmt == "JSONCompact" else body
    except urllib.error.HTTPError as e:
        sys.exit("CH ERR " + e.read().decode()[:300])
print(json.dumps({"kinds": q("SELECT kind, count(), uniqExact(org_id, session_id) FROM transcript_events GROUP BY kind ORDER BY 2 DESC"), "types": q("SELECT type, count() FROM transcript_events WHERE positionCaseInsensitive(type,'traject')>0 OR positionCaseInsensitive(type,'commit')>0 GROUP BY type ORDER BY 2 DESC LIMIT 10")}))
