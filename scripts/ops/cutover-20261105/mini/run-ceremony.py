# Mac side: assemble the ceremony's stdin JSON in RAM (neonctl owner URL + 1Password runtime password) and run it on sf-frame.
# usage: run-ceremony.py --dry-run | --execute
import json, os, shlex, subprocess, sys
from urllib.parse import quote
mode = sys.argv[1]; assert mode in ("--dry-run", "--execute")
HOST = os.environ.get("NEON_RUNTIME_HOST") or sys.exit("NEON_RUNTIME_HOST required")
ENDPOINT = os.environ.get("NEON_ENDPOINT") or sys.exit("NEON_ENDPOINT required")
NEON_PROJECT_ID = os.environ.get("NEON_PROJECT_ID") or sys.exit("NEON_PROJECT_ID required")
NEON_BRANCH_ID = os.environ.get("NEON_BRANCH_ID") or sys.exit("NEON_BRANCH_ID required")
RET = "7b2b3b8245ded3f021d3bbe5033be675db5ec66f7e201d60c8145d850c6831fe"
owner = subprocess.run(["neonctl", "connection-string", NEON_BRANCH_ID, "--project-id", NEON_PROJECT_ID,
                        "--role-name", "neondb_owner", "--database-name", "neondb"], capture_output=True, text=True, check=True).stdout.strip().splitlines()[-1]
assert owner.startswith("postgresql://neondb_owner:") and ENDPOINT in owner
pw = ""
import time
for attempt in range(6):  # 1Password intermittently times out; alternate local and kjg-lap paths
    cmd = ["op", "item", "get", "oxvgx7xbu2d4j62th6iiudt4oe", "--fields", "label=app_password", "--reveal"]
    if attempt % 2: cmd = ["ssh", "-o", "BatchMode=yes", "kjg-lap", "export PATH=/opt/homebrew/bin:$PATH; " + " ".join(cmd)]
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode == 0 and len(r.stdout.strip()) == 64: pw = r.stdout.strip(); break
    time.sleep(3)
if not pw: sys.exit("1Password read failed on both paths")
line = json.dumps({"neonOwnerUrl": owner, "neonRuntimeUrl": f"postgresql://relayhistory_app_runtime:{quote(pw, safe='')}@{HOST}/neondb?sslmode=require"})
pw = owner = ""
args = [mode, "--runtime-host", HOST] + (["--retention-sha", RET] if mode == "--execute" else [])
cmd = ("cd /home/khaliqgant/Projects/AgentWorkforce/sessions-pipeline-worktrees/private-ingest-operator-final-462-20261104 && "
       + " ".join(f"{k}={shlex.quote(v)}" for k, v in (("NEON_ENDPOINT", ENDPOINT), ("NEON_PROJECT_ID", NEON_PROJECT_ID), ("NEON_BRANCH_ID", NEON_BRANCH_ID)))
       + " /home/khaliqgant/.local/share/mise/installs/bun/1.4.2/bin/bun ~/relay-briefs/bulls-resume-20261104/cutover-ceremony.ts " + " ".join(args))
p = subprocess.run(["ssh", "-o", "BatchMode=yes", "sf-frame", cmd], input=line + "\n", capture_output=True, text=True)
line = ""
for l in (p.stdout + "\n" + p.stderr).splitlines():
    if "postgresql://" in l or "password" in l.lower(): continue
    print(l)
print("exit", p.returncode)
