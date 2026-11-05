# Throttled one-off enqueue of {"reconcile":{"key":K}} for each scope key (idempotent: the pipeline re-reads
# the manifest; busy scopes requeue themselves). Stops if the DLQ grows or the Lambda errors.
import json, os, subprocess, sys, time
KEYS = sys.argv[1]; LOG = sys.argv[2]
env = {**os.environ, "AWS_CONFIG_FILE": "/Volumes/T7/Projects/AgentWorkforce/artifacts/migration-ops-20261102/pippen-existing-workload-sso.config",
       "AWS_PROFILE": "sessions-prod", "AWS_REGION": "us-east-1"}
for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"): env.pop(k, None)
def aws(*a): return subprocess.run(["aws", *a], env=env, capture_output=True, text=True)
q = aws("sqs", "get-queue-url", "--queue-name", "96bulls-aws-sessions-prod-session-events", "--query", "QueueUrl", "--output", "text").stdout.strip()
dlq = aws("sqs", "get-queue-url", "--queue-name", "96bulls-aws-sessions-prod-session-events-dlq", "--query", "QueueUrl", "--output", "text").stdout.strip()
def depth(u):
    r = aws("sqs", "get-queue-attributes", "--queue-url", u, "--attribute-names", "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible", "--query", "Attributes", "--output", "json")
    a = json.loads(r.stdout or "{}"); return int(a.get("ApproximateNumberOfMessages", 0)), int(a.get("ApproximateNumberOfMessagesNotVisible", 0))
keys = [l.strip() for l in open(KEYS) if l.strip()]
dlq0 = sum(depth(dlq)); sent = 0
log = open(LOG, "a")
def note(**k): log.write(json.dumps({"at": time.strftime("%H:%M:%SZ", time.gmtime()), **k}) + "\n"); log.flush()
note(start=True, keys=len(keys), dlq0=dlq0)
for i in range(0, len(keys), 10):
    batch = [{"Id": str(j), "MessageBody": json.dumps({"reconcile": {"key": k}})} for j, k in enumerate(keys[i:i + 10])]
    r = aws("sqs", "send-message-batch", "--queue-url", q, "--entries", json.dumps(batch), "--output", "json")
    out = json.loads(r.stdout or "{}")
    if r.returncode != 0 or out.get("Failed"): note(error="send", rc=r.returncode, failed=len(out.get("Failed", []))); time.sleep(5); continue
    sent += len(batch)
    if (i // 10) % 30 == 0:
        vis, inflight = depth(q); d = sum(depth(dlq)); note(sent=sent, queue=vis, inflight=inflight, dlq=d)
        if d > dlq0 + 20: note(stop="dlq_growth", dlq=d); sys.exit(2)
        while vis > 300: time.sleep(10); vis, inflight = depth(q)
    time.sleep(2)
note(done=True, sent=sent, dlq=sum(depth(dlq)))
