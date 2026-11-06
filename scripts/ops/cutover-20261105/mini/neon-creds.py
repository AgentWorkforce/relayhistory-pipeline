# Mac side: owner URL from the authenticated neonctl (pinned project/branch/db) and the
# prepared app password from 1Password, as one JSON line on stdout (piped straight to ssh).
import json, os, subprocess, sys
NEON_PROJECT_ID = os.environ.get("NEON_PROJECT_ID") or sys.exit("NEON_PROJECT_ID required")
NEON_BRANCH_ID = os.environ.get("NEON_BRANCH_ID") or sys.exit("NEON_BRANCH_ID required")
owner = subprocess.run(["neonctl", "connection-string", NEON_BRANCH_ID, "--project-id", NEON_PROJECT_ID,
                        "--role-name", "neondb_owner", "--database-name", "neondb"],
                       capture_output=True, text=True, check=True).stdout.strip().splitlines()[-1].strip()
if not owner.startswith("postgresql://neondb_owner:"):
    sys.exit("unexpected owner url shape")
pw = ""
if sys.argv[1:] != ["--no-app-password"]:
    pw = subprocess.run(["op", "item", "get", "oxvgx7xbu2d4j62th6iiudt4oe", "--fields", "label=app_password", "--reveal"],
                        capture_output=True, text=True, check=True).stdout.strip()
print(json.dumps({"ownerUrl": owner, "appPassword": pw}))
