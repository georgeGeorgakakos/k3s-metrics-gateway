#!/usr/bin/env bash
# Shows the trust evidence (A1 Availability, A3 Operational stability) computed by the EMS sidecar
# and what OptimusDB (kbtrust) holds.   Run on host B:  bash check-trust.sh  [A1|A3]  [pod|node|control-plane]
set -uo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
UI="${UI:-http://localhost/swch-metrics-ui}"
PW="${PW:-$(grep ' UI_PASSWORD:' "$DIR/hostB-ems.yaml" 2>/dev/null | awk '{print $2}')}"
ODB="${ODB:-http://193.225.250.240/optimusdb1}"
UI="$UI" PW="$PW" ODB="$ODB" ATTR="${1:-}" KIND="${2:-}" python3 - <<'PY'
import base64, json, os, urllib.parse, urllib.request
UI, PW, ODB = os.environ["UI"], os.environ["PW"], os.environ["ODB"]
auth = {"Authorization": "Basic " + base64.b64encode(f"admin:{PW}".encode()).decode()}
def ui(path):
    return json.load(urllib.request.urlopen(urllib.request.Request(UI + path, headers=auth), timeout=20))
def odb(criteria):
    body = json.dumps({"method": {"cmd": "crudget", "argcnt": 1}, "dstype": "kbtrust", "criteria": [criteria]}).encode()
    r = json.load(urllib.request.urlopen(urllib.request.Request(f"{ODB}/swarmkb/command", data=body,
                 headers={"Content-Type": "application/json"}), timeout=30))
    d = r.get("data") or []
    return d if isinstance(d, list) else [d]

print("=== SIDECAR TRUST SINK ===========================================")
st = ui("/api/status")
for s in [x for x in st["sinks"] if x["type"] == "optimusdb_trust"] or [None]:
    if not s:
        print("  ✗ no optimusdb_trust sink configured – run enable-trust-hostB.sh"); raise SystemExit(1)
    print(f"  {'✓' if s['enabled'] and not s['last_error'] else '✗'} {s['id']} v{st['version']} -> {s['endpoint']} ({s['dstype']})")
    print(f"    cluster={s['cluster']} subjects={s['subjects']} written={s['sent']} errors={s['errors']} "
          f"last_flush={s['last_flush']} {s['last_error'] or ''}")

q = {k: v for k, v in (("attribute", os.environ["ATTR"]), ("kind", os.environ["KIND"])) if v}
docs = ui("/api/trust" + ("?" + urllib.parse.urlencode(q) if q else ""))
print(f"\n=== TRUST EVIDENCE (latest, {len(docs)} documents) ============================")
print(f"  {'ATTR':<5}{'KIND':<14}{'SUBJECT':<46}{'VALUE':>9}{'SCORE':>8}  SLO   {'EVIDENCE'}")
for d in docs:
    e = d["evidence"]
    ev = (f"{e['checks_ok']}/{e['checks_total']} checks, coverage {e['coverage']:.0%}" if d["attribute_id"] == "A1"
          else f"{e['unplanned_events']} unplanned, {e['warning_events']} warnings {e.get('recent_warning_reasons') or ''}")
    name = (d["namespace"] + "/" if d["namespace"] else "") + d["subject_name"]
    val = f"{d['value']:.4%}" if d["attribute_id"] == "A1" else str(d["value"])
    print(f"  {d['attribute_id']:<5}{d['subject_kind']:<14}{name[:45]:<46}{val:>9}{d['score']:>8.3f}  "
          f"{'ok ' if d['slo_met'] else 'BAD'}   {ev}")

print("\n=== OPTIMUSDB kbtrust =============================================")
try:
    for t in ("trust_evidence", "trust_evidence_history", "monitoring_snapshot"):
        print(f"  {t:<24}{len(odb({'type': t})):>6} documents")
    bad = odb({"type": "trust_evidence", "slo_met": False})
    print(f"  SLO violations stored   {len(bad):>6}  {[d.get('_id') for d in bad][:6]}")
    one = odb({"type": "trust_evidence", "attribute_id": "A3"})[:1]
    if one:
        print("\n  example document:\n" + "\n".join("    " + l for l in json.dumps(one[0], indent=2).splitlines()[:40]))
except Exception as e:  # noqa: BLE001
    print(f"  ✗ cannot query OptimusDB at {ODB}: {e}")
PY
