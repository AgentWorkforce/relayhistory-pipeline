#!/usr/bin/env python3
"""Run ONCE after 1Password is unlocked:  python3 -I ~/relay-briefs/migration-fast-20261103/after-1password-unlock.py

Every step needs a 1Password secret. Values stay in RAM and pass via stdin. Each step is idempotent and
stops on the first failure. Uploads keep running on the legacy path throughout (nothing here flips routing).
  1. Arm the buffered GC (fork's reviewed resume script) -> ARMED_HEALTHY retention contract SHA.
  2. Prove the relayhistory_app_runtime login, then capture DELIVERY_CUSTODY_SOURCE on that exact URL.
  3. Set the relayhistory-cloud secrets NEON_APP_DATABASE_URL (new restricted role) and
     DELIVERY_INGEST_SIGNING_KEY (same key as the private ingest Worker's Secrets Store secret).
  4. Set the variables DELIVERY_CUSTODY_SOURCE and DELIVERY_SOURCE_ROLE_DIAGNOSTIC=1. DELIVERY_CUTOVER and
     DELIVERY_FORWARD stay UNSET, so the deploy is dormant.
  5. Dispatch Deploy Production on main (dormant 299 Worker + DELIVERY_INGEST discovery + new DB binding)
     and wait for it.
Rollback for 3 if the new binding misbehaves: NEON_APP_DATABASE_URL back to app_blue
(`neonctl connection-string br-damp-mud-apbr6ifp --project-id divine-snow-84812652 --role-name app_blue
--database-name neondb --pooled`), then redeploy.
"""
import json, os, subprocess, sys, time
HERE = os.path.dirname(os.path.abspath(__file__))
R = "AgentWorkforce/relayhistory-cloud"
APP_ITEM, HMAC_ITEM = "oxvgx7xbu2d4j62th6iiudt4oe", "p5blz5rzkoexubwtrstatxos3y"
HOST = "ep-floral-pond-apkvat38-pooler.c-7.us-east-1.aws.neon.tech"  # pooled, like the live app_blue binding
def sh(*a, input=None, check=True):
    r = subprocess.run(list(a), input=input, capture_output=True, text=True)
    if check and r.returncode != 0:
        sys.exit(f"FAILED: {a[0]} {a[1] if len(a) > 1 else ''}: {(r.stderr or r.stdout)[-400:]}")
    return r
def op_field(item, label):
    return sh("op", "item", "get", item, "--fields", f"label={label}", "--reveal").stdout.strip()
def step(n, msg): print(f"\n== {n}. {msg}", flush=True)

sh("op", "item", "get", APP_ITEM, "--fields", "label=role")  # non-secret read; fails fast while 1Password is locked

step(1, "arm buffered GC")
_rc = os.path.join(HERE, "retention-contract-20261105.json")
if os.path.exists(_rc) and json.load(open(_rc)).get("status") == "ARMED_HEALTHY":
    print("already ARMED_HEALTHY; skipping")
else:
  r = sh("bash", "-c", "cd " + os.path.join(HERE, "gc-apply-20261105") + " && python3 -I resume-arm-gc.py")
  print(r.stdout[-1500:])

step(2, "prove the new runtime login and capture the custody source")
pw = op_field(APP_ITEM, "app_password")
from urllib.parse import quote
app_url = f"postgresql://relayhistory_app_runtime:{quote(pw, safe='')}@{HOST}/neondb?sslmode=require"
pw = ""
cap = sh("ssh", "-o", "BatchMode=yes", "sf-frame",
         "/home/khaliqgant/.local/share/mise/installs/bun/1.4.2/bin/bun ~/relay-briefs/bulls-resume-20261104/custody-capture.ts",
         input=json.dumps({"appUrl": app_url}) + "\n")
line = [l for l in cap.stdout.splitlines() if l.startswith("{")][-1]
capture = json.loads(line)
assert capture["login"] == {"u": "relayhistory_app_runtime", "s": "relayhistory_app_runtime"}, capture["login"]
custody = capture["custodySource"]
assert custody["hostname"] == HOST and len(custody["sourceIdentitySha256"]) == 64
json.dump({"at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), **capture},
          open(os.path.join(HERE, "custody-source-capture-20261105.json"), "w"), indent=2)
print("custody source:", json.dumps(custody))

step(3, "set the relayhistory-cloud deploy secrets (stdin only)")
sh("gh", "secret", "set", "NEON_APP_DATABASE_URL", "-R", R, input=app_url)
app_url = ""
item = json.loads(sh("op", "item", "get", HMAC_ITEM, "--format", "json").stdout)
secret_fields = [f for f in item.get("fields", []) if f.get("type") == "CONCEALED" and f.get("value")]
if len(secret_fields) != 1: sys.exit(f"HMAC item has {len(secret_fields)} concealed fields; pick one by hand")
key = secret_fields[0]["value"]
item = None
if len(key.encode()) < 32 or not key.strip(): sys.exit("signing key shorter than 32 bytes")
sh("gh", "secret", "set", "DELIVERY_INGEST_SIGNING_KEY", "-R", R, input=key)
key = ""
print("secrets set: NEON_APP_DATABASE_URL, DELIVERY_INGEST_SIGNING_KEY")

step(4, "set dormant deploy variables")
sh("gh", "variable", "set", "DELIVERY_CUSTODY_SOURCE", "-R", R, "--body", json.dumps(custody, separators=(",", ":")))
sh("gh", "variable", "set", "DELIVERY_SOURCE_ROLE_DIAGNOSTIC", "-R", R, "--body", "1")
for v in ("DELIVERY_CUTOVER", "DELIVERY_FORWARD"):
    if sh("gh", "variable", "get", v, "-R", R, check=False).returncode == 0:
        sys.exit(f"{v} is set; refusing a non-dormant deploy from this script")

step(5, "dispatch Deploy Production (dormant) and wait")
main = sh("gh", "api", f"repos/{R}/commits/main", "--jq", ".sha").stdout.strip()
sh("gh", "workflow", "run", "deploy-production.yml", "-R", R, "--ref", "main", "-f", "allow_write_hold=true")
time.sleep(15)
run = json.loads(sh("gh", "run", "list", "-R", R, "--workflow", "deploy-production.yml", "--limit", "1",
                    "--json", "databaseId,headSha").stdout)[0]
assert run["headSha"] == main, (run, main)
print("run", run["databaseId"], "at", main)
w = sh("gh", "run", "watch", str(run["databaseId"]), "-R", R, "--exit-status", "--interval", "60", check=False)
print("deploy:", "SUCCESS" if w.returncode == 0 else "FAILED (see run log; rollback note in this file's docstring)")
