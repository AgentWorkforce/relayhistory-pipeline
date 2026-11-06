"""Lead-operated wrapper for scripts/frozen-control-parity.ts (same pattern as the phase2 copy-operator.py).

Runs on the operator Mac. The Neon owner URL comes from the existing native neonctl login into RAM and is
written only to the ssh child's stdin; it is never in argv, a file or a log (all output is redacted). The
remote side refuses unless the sf-frame worktree is at exactly --head with no tracked changes. Supabase
uses the existing sf-frame CLI temporary login inside the TS process, as frozen-source-copy.ts does.

  python3 -I frozen-control-parity-operator.py --head <40-hex> census [--seed-pending-missing]
  python3 -I frozen-control-parity-operator.py --head <40-hex> apply <planSha256> [--seed-pending-missing]
"""
import datetime, json, pathlib, re, shlex, subprocess, sys, time
from urllib.parse import urlsplit, unquote

ROOT = "/home/khaliqgant/Projects/AgentWorkforce/sessions-pipeline-worktrees/control-parity-seed-5ada92b"
BUN = "/home/khaliqgant/.local/share/mise/installs/bun/1.4.2/bin/bun"
NEONCTL = ["/opt/homebrew/bin/neonctl", "connection-string", "br-damp-mud-apbr6ifp", "--project-id", "divine-snow-84812652",
           "--role-name", "neondb_owner", "--database-name", "neondb"]
NEON_HOST = "ep-floral-pond-apkvat38.c-7.us-east-1.aws.neon.tech"


def parse(argv):
    a = list(argv)
    if len(a) < 3 or a[0] != "--head" or not re.fullmatch(r"[0-9a-f]{40}", a[1]):
        raise SystemExit("usage: --head <40-hex> census|apply <planSha256> [--seed-pending-missing]")
    head, rest = a[1], a[2:]
    pending = "--seed-pending-missing" in rest
    rest = [x for x in rest if x != "--seed-pending-missing"]
    if rest == ["census"]:
        args = ["--census"]
    elif len(rest) == 2 and rest[0] == "apply" and re.fullmatch(r"[0-9a-f]{64}", rest[1]):
        args = ["--apply", "--expect-plan", rest[1]]
    else:
        raise SystemExit("usage: --head <40-hex> census|apply <planSha256> [--seed-pending-missing]")
    return head, rest[0], args + (["--seed-pending-missing"] if pending else [])


def remote_command(head, args):
    # Exact head; no tracked, untracked or ignored file other than the dependency trees; no .env* anywhere; bun never loads one.
    return ("cd " + shlex.quote(ROOT) + " && test \"$(git rev-parse HEAD)\" = " + shlex.quote(head)
            + " && git diff --quiet HEAD"
            + " && test -z \"$(git status --porcelain --untracked-files=all --ignored=matching | grep -v -E '^!! (packages/[^/]+/)?node_modules/$')\""
            + " && test -z \"$(find . -name node_modules -prune -o -name '.env*' -print)\""
            + " && env -u SUPABASE_CLI " + BUN + " --no-env-file scripts/frozen-control-parity.ts " + shlex.join(args))


def main():
    head, mode, args = parse(sys.argv[1:])
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
    return code


if __name__ == "__main__":
    sys.exit(main())
