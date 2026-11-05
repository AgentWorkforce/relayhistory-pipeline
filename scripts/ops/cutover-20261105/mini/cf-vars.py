# Set/replace plain_text vars on a Worker via PATCH /settings, inheriting every other binding unchanged.
# usage: cf-vars.py <script> '<json {NAME: value-string}>' [--dry]
import json, os, sys, tomllib, urllib.request, uuid
ACC = "f7232cb80f6fab86a95426302af243e4"
script, want = sys.argv[1], json.loads(sys.argv[2]); dry = "--dry" in sys.argv
tok = tomllib.load(open(os.path.expanduser("~/Library/Preferences/.wrangler/config/default.toml"), "rb"))["oauth_token"]
base = f"https://api.cloudflare.com/client/v4/accounts/{ACC}/workers/scripts/{script}/settings"
def req(method, data=None, ctype=None):
    h = {"Authorization": "Bearer " + tok}
    if ctype: h["Content-Type"] = ctype
    r = urllib.request.Request(base, method=method, data=data, headers=h)
    try:
        with urllib.request.urlopen(r, timeout=60) as x: return json.load(x)
    except urllib.error.HTTPError as e: return json.load(e)
cur = req("GET")["result"]
before = {b["name"]: b["type"] for b in cur["bindings"]}
bindings = []
for b in cur["bindings"]:
    if b["name"] in want: continue
    bindings.append({"type": "inherit", "name": b["name"]})
for k, v in want.items():
    bindings.append({"type": "plain_text", "name": k, "text": v})
settings = {"bindings": bindings}
if dry: print(json.dumps({"before": before, "set": list(want)}, indent=1)); sys.exit()
bnd = uuid.uuid4().hex
body = (f"--{bnd}\r\nContent-Disposition: form-data; name=\"settings\"\r\nContent-Type: application/json\r\n\r\n"
        + json.dumps(settings) + f"\r\n--{bnd}--\r\n").encode()
r = req("PATCH", body, f"multipart/form-data; boundary={bnd}")
if not r.get("success"): sys.exit("PATCH failed: " + json.dumps(r.get("errors")))
after = {b["name"]: (b["type"], b.get("text") if b["type"] == "plain_text" else None) for b in req("GET")["result"]["bindings"]}
missing = [n for n in before if n not in after]
wrong = [k for k, v in want.items() if after.get(k) != ("plain_text", v)]
changed_types = [n for n in before if n in after and n not in want and after[n][0] != before[n]]
print(json.dumps({"ok": not (missing or wrong or changed_types), "missing": missing, "wrong": wrong, "typeChanged": changed_types, "count": [len(before), len(after)]}))
