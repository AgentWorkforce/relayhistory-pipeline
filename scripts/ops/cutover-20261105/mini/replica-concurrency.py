# Set relayhistory-neon-replica consumer max_concurrency (all other settings unchanged), print before/after.
import json, os, sys, tomllib, urllib.request
ACC = "f7232cb80f6fab86a95426302af243e4"; Q = "b2e92a0b2b984803b71cc3e02f32fac1"; C = "5d5b8bf8b46c416ca5c0449f5b50a7de"
want = int(sys.argv[1]); assert 1 <= want <= 20
tok = tomllib.load(open(os.path.expanduser("~/Library/Preferences/.wrangler/config/default.toml"), "rb"))["oauth_token"]
def call(method, path, body=None):
    r = urllib.request.Request(f"https://api.cloudflare.com/client/v4/accounts/{ACC}{path}", method=method,
        data=None if body is None else json.dumps(body).encode(), headers={"Authorization": "Bearer " + tok, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=30) as x: return json.load(x)
    except urllib.error.HTTPError as e: return json.load(e)
cur = [c for c in call("GET", f"/queues/{Q}/consumers")["result"] if c["consumer_id"] == C][0]
print("before", cur["settings"])
body = {"script_name": cur["script"], "type": "worker", "dead_letter_queue": cur["dead_letter_queue"],
        "settings": {**cur["settings"], "max_concurrency": want}}
r = call("PUT", f"/queues/{Q}/consumers/{C}", body)
print("ok", r.get("success"), r.get("errors"))
after = [c for c in call("GET", f"/queues/{Q}/consumers")["result"] if c["consumer_id"] == C][0]
print("after", after["settings"])
