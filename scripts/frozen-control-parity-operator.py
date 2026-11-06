"""Lead-operated wrapper for scripts/frozen-control-parity.ts (same pattern as the phase2 copy-operator.py).

Runs on the operator Mac. The Neon owner URL comes from the existing native neonctl login into RAM and is
written only to the ssh child's stdin; it is never in argv, a file or a log (all output is redacted). The
remote side refuses unless the sf-frame worktree is at exactly --head with no tracked changes. Supabase
uses the existing sf-frame CLI temporary login inside the TS process, as frozen-source-copy.ts does.

  python3 -I frozen-control-parity-operator.py --head <40-hex> census [--seed-pending-missing]
  python3 -I frozen-control-parity-operator.py --head <40-hex> apply <planSha256> [--seed-pending-missing]
  python3 -I frozen-control-parity-operator.py --head <40-hex> handoff-census [--settle-seconds N]
  python3 -I frozen-control-parity-operator.py --head <40-hex> handoff-sync <planSha256> [--settle-seconds N]
Handoff modes require the live relayhistory-api binding DELIVERY_CUSTODY_SOURCE_MODE=mirror (the fence) before AND
after the remote run, read with a GET through the existing approved Wrangler OAuth account (as watch-copy-v2.py).
"""
import datetime, json, pathlib, re, shlex, subprocess, sys, time, urllib.request
from urllib.parse import urlsplit, unquote

ROOT = "/home/khaliqgant/Projects/AgentWorkforce/sessions-pipeline-worktrees/control-handoff-optionb-3d6edb0"
BUN = "/home/khaliqgant/.local/share/mise/installs/bun/1.4.2/bin/bun"
NEONCTL = ["/opt/homebrew/bin/neonctl", "connection-string", "br-damp-mud-apbr6ifp", "--project-id", "divine-snow-84812652",
           "--role-name", "neondb_owner", "--database-name", "neondb"]
NEON_HOST = "ep-floral-pond-apkvat38.c-7.us-east-1.aws.neon.tech"


USAGE = "usage: --head <40-hex> census|apply <planSha256> [--seed-pending-missing] | handoff-census|handoff-sync <planSha256> [--settle-seconds N]"
ACCOUNT = "f7232cb80f6fab86a95426302af243e4"


def parse(argv):
    a = list(argv)
    if len(a) < 3 or a[0] != "--head" or not re.fullmatch(r"[0-9a-f]{40}", a[1]):
        raise SystemExit(USAGE)
    head, rest = a[1], a[2:]
    pending = "--seed-pending-missing" in rest
    rest = [x for x in rest if x != "--seed-pending-missing"]
    settle = []
    if "--settle-seconds" in rest:
        i = rest.index("--settle-seconds")
        if i + 1 >= len(rest) or not re.fullmatch(r"[0-9]{2,3}", rest[i + 1]) or not 60 <= int(rest[i + 1]) <= 900:
            raise SystemExit(USAGE)
        settle = ["--settle-seconds", rest[i + 1]]
        rest = rest[:i] + rest[i + 2:]
    if rest == ["census"]:
        args = ["--census"]
    elif len(rest) == 2 and rest[0] == "apply" and re.fullmatch(r"[0-9a-f]{64}", rest[1]):
        args = ["--apply", "--expect-plan", rest[1]]
    elif rest == ["handoff-census"]:
        args = ["--handoff-census"]
    elif len(rest) == 2 and rest[0] == "handoff-sync" and re.fullmatch(r"[0-9a-f]{64}", rest[1]):
        args = ["--handoff-sync", "--expect-plan", rest[1]]
    else:
        raise SystemExit(USAGE)
    handoff = rest[0].startswith("handoff")
    if (settle and not handoff) or (pending and handoff):
        raise SystemExit(USAGE)
    return head, rest[0], args + settle + (["--seed-pending-missing"] if pending else [])


def source_mode():
    """Read-only GET of the live relayhistory-api binding; approved native Wrangler OAuth only, no fallback."""
    r = subprocess.run(["/opt/homebrew/bin/wrangler", "auth", "token", "--json"], capture_output=True, text=True, timeout=20)
    if r.returncode:
        raise SystemExit("wrangler login unavailable; diagnostic output withheld")
    token = json.loads(r.stdout).get("token")
    if not isinstance(token, str) or not 10 < len(token) < 8192:
        raise SystemExit("wrangler login unavailable")

    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *a, **kw):
            raise SystemExit("source probe redirect refused")
    url = "https://api.cloudflare.com/client/v4/accounts/" + ACCOUNT + "/workers/scripts/relayhistory-api/settings"
    req = urllib.request.Request(url, headers={"Authorization": "Bearer " + token})
    with urllib.request.build_opener(NoRedirect).open(req, timeout=20) as response:
        value = json.load(response)
    found = [b for b in (value.get("result") or {}).get("bindings") or [] if b.get("name") == "DELIVERY_CUSTODY_SOURCE_MODE"]
    if value.get("success") is not True or len(found) != 1 or found[0].get("type") != "plain_text":
        return None
    return found[0].get("text")


def remote_command(head, args):
    # Exact head; no tracked, untracked or ignored file other than the dependency trees; no .env* anywhere; bun never loads one.
    return ("cd " + shlex.quote(ROOT) + " && test \"$(git rev-parse HEAD)\" = " + shlex.quote(head)
            + " && git diff --quiet HEAD"
            + " && test -z \"$(git status --porcelain --untracked-files=all --ignored=matching | grep -v -E '^!! (packages/[^/]+/)?node_modules/$')\""
            + " && test -z \"$(find . -name node_modules -prune -o -name '.env*' -print)\""
            + " && env -u SUPABASE_CLI " + BUN + " --no-env-file scripts/frozen-control-parity.ts " + shlex.join(args))


def main():
    head, mode, args = parse(sys.argv[1:])
    if mode.startswith("handoff") and source_mode() != "mirror":
        raise SystemExit("fence not closed: relayhistory-api DELIVERY_CUSTODY_SOURCE_MODE is not mirror; refusing handoff")
    r = subprocess.run(NEONCTL, capture_output=True, text=True, timeout=60)
    if r.returncode:
        raise SystemExit("Neon credential retrieval failed; diagnostic output withheld")
    url = r.stdout.strip().splitlines()[-1]
    u = urlsplit(url)
    if u.hostname != NEON_HOST or u.username != "neondb_owner" or not u.password:
        raise SystemExit("Neon endpoint pin mismatch; refusing")
    secrets = [s for s in (url, u.password, unquote(u.password)) if s]
    run_id = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    # One file per run: evidence is always bound to the run that produced it, never "the last line of a shared log".
    log = pathlib.Path(__file__).with_name("control-parity-" + mode + "-" + run_id + ".jsonl")
    started = time.monotonic()
    with log.open("a") as out:
        def emit(line):
            for s in secrets:
                line = line.replace(s, "<redacted>")
            print(line, end="", flush=True)
            out.write(line)
            out.flush()
        emit(json.dumps({"at": datetime.datetime.now(datetime.timezone.utc).isoformat(), "operator": "start", "mode": mode, "head": head,
                         "runId": run_id, "log": str(log)}) + "\n")
        p = subprocess.Popen(["ssh", "sf-frame", remote_command(head, args)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, text=True)
        p.stdin.write(json.dumps({"neonOwnerUrl": url}) + "\n")
        p.stdin.close()
        try:
            for line in p.stdout:
                emit(line)
            code = p.wait()
        except KeyboardInterrupt:
            p.terminate(); p.wait(); raise
        emit(json.dumps({"at": datetime.datetime.now(datetime.timezone.utc).isoformat(), "operator": "end", "mode": mode,
                         "seconds": round(time.monotonic() - started, 3), "exit": code}) + "\n")
        if mode.startswith("handoff") and source_mode() != "mirror":
            emit(json.dumps({"operator": "FENCE_LOST", "action": "STOP: mirror mode changed during the handoff; result is not evidence"}) + "\n")
            return 1
    return code


if __name__ == "__main__":
    sys.exit(main())
