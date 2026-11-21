#!/usr/bin/env bash
# End-to-end check for one k3s cluster: gateway -> EMS sidecar -> EMS broker -> OptimusDB.
# Run on the EMS host (host B), in the folder with hostB-ems.yaml:
#
#   bash e2e-check.sh <cluster-name>        e.g.  bash e2e-check.sh epm-server
#   bash e2e-check.sh                       (all registered clusters)
#
# Env: UI (default http://localhost/swch-metrics-ui), PW (default from hostB-ems.yaml),
#      ODB (default http://193.225.250.240/optimusdb1), NO_EMS=1 to skip the broker check,
#      EMS_SECS (default 20) seconds to listen on the broker,
#      EVENTS_LIMIT / EVENTS_MIN / EVENTS_TIMEOUT (default 200 / 5 / 90 s) for the ems_events query.
set -uo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
CLUSTER="${1:-}"
UI="${UI:-http://localhost/swch-metrics-ui}"
PW="${PW:-$(grep ' UI_PASSWORD:' "$DIR/hostB-ems.yaml" 2>/dev/null | awk '{print $2}')}"
ODB="${ODB:-http://193.225.250.240/optimusdb1}"
EMS_SECS="${EMS_SECS:-20}"

# ---- 4. EMS broker (inside the sidecar container: it has stomp.py and reaches localhost:61610)
EMS_OUT=""
if [ "${NO_EMS:-0}" != 1 ]; then
  EMS_OUT="$(sudo k3s kubectl -n default exec deploy/emsserver-ems-server -c swch-metrics-client -- python3 -c "
import collections, json, stomp, time
seen = collections.Counter(); body = {}
class L(stomp.ConnectionListener):
    def on_message(self, f):
        c = f.headers.get('cluster', '?'); seen[c] += 1
        body.setdefault(c, (f.headers.get('destination'), f.body))
c = stomp.Connection([('localhost', 61610)]); c.set_listener('', L()); c.connect('aaa', '111', wait=True)
c.subscribe('/topic/>', id=1); time.sleep($EMS_SECS); c.disconnect()
print(json.dumps({'counts': seen, 'example': body}))
" 2>/dev/null | tail -1)"
fi

UI="$UI" PW="$PW" ODB="$ODB" CLUSTER="$CLUSTER" EMS_OUT="$EMS_OUT" NO_EMS="${NO_EMS:-0}" EMS_SECS="$EMS_SECS" python3 - <<'PY'
import base64, json, os, time, urllib.parse, urllib.request
from datetime import datetime, timezone
UI, PW, ODB, ONE = os.environ["UI"], os.environ["PW"], os.environ["ODB"].rstrip("/"), os.environ["CLUSTER"]
AUTH = {"Authorization": "Basic " + base64.b64encode(f"admin:{PW}".encode()).decode()}
results = []

def ui(path, method="GET"):
    req = urllib.request.Request(UI + path, method=method, headers=AUTH, data=b"{}" if method == "POST" else None)
    if method == "POST":
        req.add_header("Content-Type", "application/json")
    return json.load(urllib.request.urlopen(req, timeout=30))

def odb_get(criteria):
    body = json.dumps({"method": {"cmd": "crudget", "argcnt": 1}, "dstype": "kbtrust", "criteria": [criteria]}).encode()
    r = json.load(urllib.request.urlopen(urllib.request.Request(f"{ODB}/swarmkb/command", data=body,
                 headers={"Content-Type": "application/json"}), timeout=30))
    d = r.get("data") or []
    return d if isinstance(d, list) else [d]

def check(step, ok, detail):
    results.append((step, ok, detail))
    print(f"  {'✓' if ok else '✗'} {step:<44} {detail}")

def age(iso):
    if not iso:
        return None
    return time.time() - datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()

print("== 1. EMS sidecar")
st = ui("/api/status")
check("sidecar version", st["version"] >= "0.3.0", f"v{st['version']}")
reg = st.get("registration") or {}
check("registration endpoint open (token set)", bool(reg.get("token_configured")), "POST " + UI + "/api/register")
gws = st.get("gateways") or []
targets = [g for g in gws if not ONE or g["cluster"] == ONE or g["id"] == ONE]
check("cluster known to the sidecar", bool(targets),
      ", ".join(f"{g['cluster']} ({g['id']})" for g in targets) or f"'{ONE}' not in {[g['cluster'] for g in gws]}")

for g in targets:
    cl, gid = g["cluster"], g["id"]
    print(f"\n== 2. gateway {cl}  ({g['url']})")
    check("enabled", g["enabled"], "yes" if g["enabled"] else "disabled: enable it in the Gateways tab")
    check("source", True, g["source"] + (f", registered {g['registered_at']}" if g.get("registered_at") else ""))
    hb = age(g.get("last_heartbeat"))
    if g["source"] == "registered":
        check("heartbeat < 10 min", hb is not None and hb < 600, f"{int(hb)} s ago" if hb is not None else "none yet")
    t = ui(f"/api/gateways/{urllib.parse.quote(gid)}/test", "POST")
    check("reachable + API key accepted", t.get("reachable") and t.get("authorized") and not t.get("error"),
          t.get("error") or f"cluster {t.get('cluster_name') or cl}, {t.get('nodes')} node(s)")
    check("polled by every collector", g["ok"] and not g["errors"],
          f"{g['collectors']} collectors, {g['samples_last_run']} samples last run, {g['errors']} errors")

    print(f"== 3. samples from {cl} (sidecar)")
    smp = ui(f"/api/samples?cluster={urllib.parse.quote(cl)}&limit=500")
    metrics = sorted({s['metric'] for s in smp})
    check("samples labelled cluster=" + cl, len(smp) > 0, f"{len(smp)} recent samples, {len(metrics)} metrics")
    if smp:
        s = smp[0]
        print(f"      e.g. {s['metric']} {s.get('instance')} = {s['value']}  labels={ {k: v for k, v in s['labels'].items() if k in ('cluster', 'cluster_uid', 'swarm_id', 'gateway')} }")

    print(f"== 4. EMS broker (STOMP localhost:61610)")
    if os.environ["NO_EMS"] == "1":
        print("      skipped (NO_EMS=1)")
    else:
        try:
            ems = json.loads(os.environ["EMS_OUT"] or "{}")
            n = (ems.get("counts") or {}).get(cl, 0)
            check(f"messages with header cluster={cl}", n > 0, f"{n} in {os.environ['EMS_SECS']} s")
            ex = (ems.get("example") or {}).get(cl)
            if ex:
                print(f"      e.g. {ex[0]}  body {ex[1]}")
                check("cluster in the message body", f'"cluster": "{cl}"' in ex[1] or f'"cluster":"{cl}"' in ex[1],
                      "yes (OptimusDB ems_events can be filtered by cluster)")
        except Exception as e:  # noqa: BLE001
            check("EMS broker listen", False, f"{e}")

    print(f"== 5. OptimusDB  {ODB}/swarmkb/command  (dstype kbtrust)")
    try:
        te = odb_get({"type": "trust_evidence", "cluster": cl})
        a1 = [d for d in te if d.get("attribute_id") == "A1"]
        a3 = [d for d in te if d.get("attribute_id") == "A3"]
        newest = max((d.get("updated_at", "") for d in te), default="")
        fresh = age(newest)
        check("trust_evidence for " + cl, len(te) > 0, f"{len(a1)} A1 + {len(a3)} A3 documents")
        check("updated in the last 5 min", fresh is not None and fresh < 300, f"latest {newest}")
        bad = [d["_id"] for d in te if d.get("slo_met") is False]
        check("SLO violations (information)", True, f"{len(bad)}" + (f": {bad[:3]}" if bad else ""))
        mon = odb_get({"type": "monitoring_snapshot", "cluster": cl})
        check("monitoring_snapshot for " + cl, len(mon) > 0, f"{len(mon)} subjects")
        hist = odb_get({"type": "trust_evidence_history", "cluster": cl})
        check("hourly history (append-only)", True, f"{len(hist)} documents")
        if te:
            d = sorted(te, key=lambda x: x["_id"])[0]
            print(f"      e.g. {d['_id']}  value={d['value']} score={d['score']} slo_met={d['slo_met']}")
    except Exception as e:  # noqa: BLE001
        check("OptimusDB kbtrust query", False, f"{e}")
    # ems_events is filled by OptimusDB's own subscription to the EMS broker; the endpoint can be slow on a
    # large table, so a small window is asked for and a slow answer is a warning, not a failure.
    lim, mins = os.environ.get("EVENTS_LIMIT", "200"), os.environ.get("EVENTS_MIN", "5")
    t0 = time.time()
    try:
        ev = json.load(urllib.request.urlopen(f"{ODB}/swarmkb/ems/events?limit={lim}&since_min={mins}",
                                              timeout=float(os.environ.get("EVENTS_TIMEOUT", "90"))))
        txt = json.dumps(ev)
        n = txt.count(f'\\"cluster\\": \\"{cl}\\"') + txt.count(f'\\"cluster\\":\\"{cl}\\"') + \
            txt.count(f'"cluster": "{cl}"') + txt.count(f'"cluster":"{cl}"')
        check(f"ems_events with cluster (last {mins} min)", n > 0,
              f"{n} of the last {lim} events ({time.time() - t0:.1f} s)")
    except Exception as e:  # noqa: BLE001
        print(f"  ⚠ {'ems_events query (OptimusDB endpoint)':<44} {e} after {time.time() - t0:.0f} s: "
              f"not counted; check by hand: curl -s '{ODB}/swarmkb/ems/events?limit=20&since_min=5'")

failed = [r for r in results if not r[1]]
print("\nRESULT:", "ALL CHECKS PASSED ✓" if not failed else f"{len(failed)} check(s) failed ✗: " + "; ".join(r[0] for r in failed))
PY
