#!/usr/bin/env bash
# Reads trust documents from OptimusDB (dstype kbtrust) and prints them as a table, JSON or a count.
# Runs on any host that reaches OptimusDB (host A port 80); needs only python3.
#
#   bash kbtrust-query.sh                                   # latest A1/A3 evidence, every cluster
#   bash kbtrust-query.sh cluster=epm-server attribute_id=A3 subject_kind=pod
#   bash kbtrust-query.sh slo_met=false                     # SLO violations only
#   bash kbtrust-query.sh type=monitoring_snapshot --json   # raw documents
#   bash kbtrust-query.sh type=trust_evidence_history cluster=epm-server --count
#   bash kbtrust-query.sh _id=trust:epm-server:node:/epm-server:A1 --json
#
# Filters are key=value, combined with AND, matched on the top-level document fields. Without type= (or _id=),
# type=trust_evidence is used. true/false and numbers are sent as JSON values, everything else as strings.
# Env: ODB (default http://193.225.250.240/optimusdb1), TIMEOUT (default 30 s).
set -euo pipefail
case "${1:-}" in -h|--help) sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'; exit 0;; esac
ODB="${ODB:-http://193.225.250.240/optimusdb1}" TIMEOUT="${TIMEOUT:-30}" python3 - "$@" <<'PY'
import json, os, sys, urllib.request
ODB, mode, crit = os.environ["ODB"].rstrip("/"), "table", {}
for a in sys.argv[1:]:
    if a in ("--json", "--count", "--table"):
        mode = a[2:]; continue
    if "=" not in a:
        sys.exit(f"unknown argument {a!r}; usage: kbtrust-query.sh [key=value ...] [--json|--count]   (--help)")
    k, v = a.split("=", 1)
    try:
        crit[k] = json.loads(v) if v in ("true", "false") or v.replace(".", "", 1).lstrip("-").isdigit() else v
    except ValueError:
        crit[k] = v
if "_id" not in crit:
    crit.setdefault("type", "trust_evidence")
body = json.dumps({"method": {"cmd": "crudget", "argcnt": 1}, "dstype": "kbtrust", "criteria": [crit]}).encode()
try:
    r = json.load(urllib.request.urlopen(urllib.request.Request(f"{ODB}/swarmkb/command", data=body,
                 headers={"Content-Type": "application/json"}), timeout=float(os.environ["TIMEOUT"])))
except Exception as e:  # noqa: BLE001
    sys.exit(f"cannot query {ODB}/swarmkb/command: {e}")
d = r.get("data") if isinstance(r, dict) else r
d = [] if d is None else d if isinstance(d, list) else [d]
if mode == "json":
    print(json.dumps(d, indent=2)); sys.exit(0)
if mode == "count":
    print(len(d)); sys.exit(0)
print(f"# {len(d)} documents  criteria={json.dumps(crit)}")
if not d:
    sys.exit(0)
print(f"{'TYPE':<23}{'ATTR':<5}{'CLUSTER':<16}{'KIND':<14}{'SUBJECT':<46}{'VALUE':>10}{'SCORE':>7}  SLO  UPDATED")
key = lambda x: (x.get("type", ""), x.get("attribute_id", ""), x.get("cluster", ""), x.get("subject_kind", ""),
                 x.get("subject_name", ""), x.get("updated_at", ""))
for x in sorted(d, key=key):
    name = (x.get("namespace") + "/" if x.get("namespace") else "") + str(x.get("subject_name") or x.get("workload") or x.get("_id", ""))
    v, s = x.get("value"), x.get("score")
    vs = "" if v is None else (f"{v:.4%}" if x.get("attribute_id") == "A1" else f"{v:g}") if isinstance(v, (int, float)) else str(v)[:10]
    ss = f"{s:.3f}" if isinstance(s, (int, float)) else ""
    slo = "" if "slo_met" not in x else ("ok " if x["slo_met"] else "BAD")
    print(f"{x.get('type', '')[:22]:<23}{x.get('attribute_id', ''):<5}{str(x.get('cluster', ''))[:15]:<16}"
          f"{x.get('subject_kind', ''):<14}{name[:45]:<46}{vs:>10}{ss:>7}  {slo:<4} {x.get('updated_at', '')}")
PY
