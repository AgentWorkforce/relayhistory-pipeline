# Mac side orchestrator: pause the replica queue, run the SF drain+apply, ALWAYS resume the queue.
import json, subprocess, sys
NEON = ["--project-id", "divine-snow-84812652", "--database-name", "neondb"]
def url(role):
    out = subprocess.run(["neonctl", "connection-string", "br-damp-mud-apbr6ifp", *NEON, "--role-name", role],
                         capture_output=True, text=True, check=True).stdout.strip().splitlines()[-1].strip()
    if not out.startswith(f"postgresql://{role}:"): sys.exit("bad url shape for " + role)
    return out
owner_url = url("neondb_owner")
from urllib.parse import urlsplit
host = urlsplit(owner_url).hostname
# The helper reads only the runtime role NAME from DATABASE_URL (apply-neon-migration.mjs:945); no password needed.
creds = json.dumps({"ownerUrl": owner_url, "appUrl": f"postgresql://relayhistory_app_runtime@{host}/neondb"})
env_cf = {"CLOUDFLARE_ACCOUNT_ID": "f7232cb80f6fab86a95426302af243e4"}
import os
env = {**os.environ, **env_cf}
p = subprocess.run(["wrangler", "queues", "pause-delivery", "relayhistory-neon-replica"], env=env, capture_output=True, text=True)
print("pause:", p.returncode, (p.stdout + p.stderr).strip().splitlines()[-1:] )
if p.returncode != 0: sys.exit("queue pause failed; nothing else done")
try:
    r = subprocess.run(["ssh", "-o", "BatchMode=yes", "sf-frame",
        "/home/khaliqgant/.local/share/mise/installs/node/22.22.3/bin/node ~/relay-briefs/bulls-resume-20261104/drain-apply-0078.mjs"],
        input=creds + "\n", capture_output=True, text=True, timeout=900)
    print("\n".join(l for l in r.stdout.splitlines() if "postgres" not in l.lower() or "app_blue_exec" in l))
    print("ssh exit", r.returncode, r.stderr[-400:])
finally:
    q = subprocess.run(["wrangler", "queues", "resume-delivery", "relayhistory-neon-replica"], env=env, capture_output=True, text=True)
    print("resume:", q.returncode, (q.stdout + q.stderr).strip().splitlines()[-1:])
