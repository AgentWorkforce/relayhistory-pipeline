# Mac side: build the RAM-only stdin for legacy-drain-proof.mjs and pipe it to sf-frame.
#   python3 -I drain-proof-creds.py [--hold-start ISO] [--expect-version ID]
import json, os, subprocess, sys, tomllib
owner = subprocess.run(["neonctl", "connection-string", "br-damp-mud-apbr6ifp", "--project-id", "divine-snow-84812652",
                        "--role-name", "neondb_owner", "--database-name", "neondb"], capture_output=True, text=True, check=True).stdout.strip().splitlines()[-1]
if not owner.startswith("postgresql://neondb_owner:"): sys.exit("bad owner url shape")
cf = tomllib.load(open(os.path.expanduser("~/Library/Preferences/.wrangler/config/default.toml"), "rb"))["oauth_token"]
env = {k: v for k, v in os.environ.items() if not k.startswith("AWS_")}
env.update(AWS_CONFIG_FILE="/Volumes/T7/Projects/AgentWorkforce/artifacts/migration-ops-20261102/pippen-existing-workload-sso.config", AWS_PROFILE="sessions-prod", AWS_REGION="us-east-1")
aws = json.loads(subprocess.run(["aws", "configure", "export-credentials", "--format", "process"], env=env, capture_output=True, text=True, check=True).stdout)
creds = json.dumps({"ownerUrl": owner, "cfToken": cf, "aws": {"accessKeyId": aws["AccessKeyId"], "secretAccessKey": aws["SecretAccessKey"], "sessionToken": aws.get("SessionToken")}})
cmd = "/home/khaliqgant/.local/share/mise/installs/node/22.22.3/bin/node ~/relay-briefs/bulls-resume-20261104/legacy-drain-proof.mjs " + " ".join(sys.argv[1:])
r = subprocess.run(["ssh", "-o", "BatchMode=yes", "sf-frame", cmd], input=creds + "\n", capture_output=True, text=True, timeout=1200)
sys.stdout.write(r.stdout)
if r.returncode: sys.stderr.write(r.stderr[-800:])
