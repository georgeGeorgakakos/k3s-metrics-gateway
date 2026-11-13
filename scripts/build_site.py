#!/usr/bin/env python3
"""Build the GitHub Pages site into _site/.

- /            project page with the README's Mermaid figures (interactive: zoom/pan)
- /demo/       the real client UI running against an in-browser simulated backend
"""
from __future__ import annotations

import html
import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
UI = ROOT / "src/swch_metrics/client/ui"
OUT = ROOT / "_site"

TITLES = ["Architecture", "What happens on deployment", "Inside the client",
          "Sidecar attachment lifecycle", "Configuration precedence"]

PAGE = """<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Swarmchestrate · k3s Metrics Gateway</title>
<link rel="icon" type="image/png" href="demo/ui/favicon.png">
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Overpass:wght@400;700;800&display=swap">
<link rel="stylesheet" href="demo/ui/style.css">
<style>
 .fig {{ background:#fff; border:1px solid var(--line); border-radius:12px; padding:16px; overflow:hidden; position:relative; }}
 .fig .mermaid {{ cursor:grab; min-height:120px; }}
 .fig .tools {{ position:absolute; right:12px; top:12px; display:flex; gap:6px; z-index:2; }}
 .fig svg {{ max-width:none !important; }}
 .demo-frame {{ width:100%; height:820px; border:1px solid var(--line); border-radius:12px; box-shadow:var(--shadow); background:#fff; }}
 .cta {{ display:flex; gap:10px; flex-wrap:wrap; margin-top:14px; }}
 .cta .btn {{ background:rgba(255,255,255,.12); color:#fff; border-color:rgba(255,255,255,.35); }}
 .cta .btn.primary {{ background:#fff; color:var(--swrm-navy); border-color:#fff; }}
 .lead {{ color:#d4e0ff; max-width:760px; font-size:15px; }}
 section h2 {{ margin:6px 0 10px; }}
 main {{ display:flex; flex-direction:column; gap:22px; }}
</style>
</head><body>
<header class="topbar"><div class="wrap topbar-inner">
  <a class="brand" href="./"><img src="https://www.swarmchestrate.eu/wp-content/uploads/2024/04/logo-horizontal.png" alt="Swarmchestrate"
     onerror="this.hidden=true;this.nextElementSibling.hidden=false"><span class="wordmark" hidden>SWARM<b>CHESTRATE</b></span></a>
  <div class="product"><span class="product-name">k3s Metrics Gateway</span><span class="product-sub">&amp; EMS metrics client</span></div>
  <div class="pills"><a class="pill" href="{repo}">GitHub</a></div>
</div></header>
<section class="hero"><div class="wrap hero-inner" style="padding-bottom:34px"><div>
  <div class="eyebrow">Swarmchestrate · Horizon Europe 101135012</div>
  <h1>Metrics &amp; topology for every Swarmchestrate k3s cluster</h1>
  <p class="lead">A per-cluster gateway publishes the cluster topology and a single metrics API for EMS. A modular client running as a sidecar of EMS polls it on configurable intervals and publishes over STOMP. It is configured from a Swarmchestrate-branded UI.</p>
  <div class="cta"><a class="btn primary" href="demo/">Open the live demo →</a><a class="btn" href="{repo}">Source on GitHub</a><a class="btn" href="{repo}#quick-start">Quick start</a></div>
</div></div></section>
<main class="wrap">
{figures}
<section><h2>Live demo</h2><p class="muted">The real client UI, running against simulated data in your browser.</p>
<iframe class="demo-frame" src="demo/?demo" title="Swarmchestrate metrics client demo" loading="lazy"></iframe></section>
</main>
<footer class="footer"><div class="wrap footer-inner"><div><strong>Swarmchestrate</strong> — Application-level Swarm-based Orchestration Across the Cloud-to-Edge Continuum
<div class="muted-light">Funded by the European Union, Horizon Europe grant agreement No. 101135012. Apache-2.0.</div></div><div class="muted-light">{version}</div></div></footer>
<script type="module">
import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs";
mermaid.initialize({{ startOnLoad: false, theme: "base", securityLevel: "strict",
  themeVariables: {{ primaryColor: "#eef3fd", primaryBorderColor: "#0455bf", primaryTextColor: "#162d73",
                    lineColor: "#162d73", fontFamily: "Segoe UI, Roboto, sans-serif", clusterBkg: "#f8faff", clusterBorder: "#b9c8e6" }} }});
await mermaid.run({{ querySelector: ".mermaid" }});
// minimal pan & zoom per figure
document.querySelectorAll(".fig").forEach((fig) => {{
  const svg = fig.querySelector("svg"); if (!svg) return;
  let s = 1, x = 0, y = 0, drag = null;
  const apply = () => (svg.style.transform = `translate(${{x}}px,${{y}}px) scale(${{s}})`, svg.style.transformOrigin = "0 0");
  fig.querySelector("[data-z=in]").onclick = () => {{ s *= 1.2; apply(); }};
  fig.querySelector("[data-z=out]").onclick = () => {{ s /= 1.2; apply(); }};
  fig.querySelector("[data-z=reset]").onclick = () => {{ s = 1; x = y = 0; apply(); }};
  fig.addEventListener("wheel", (e) => {{ if (!e.ctrlKey) return; e.preventDefault(); s *= e.deltaY < 0 ? 1.1 : 0.9; apply(); }}, {{ passive: false }});
  svg.addEventListener("mousedown", (e) => (drag = [e.clientX - x, e.clientY - y]));
  window.addEventListener("mouseup", () => (drag = null));
  window.addEventListener("mousemove", (e) => {{ if (drag) {{ x = e.clientX - drag[0]; y = e.clientY - drag[1]; apply(); }} }});
}});
</script>
</body></html>
"""


def main() -> None:
    repo = sys.argv[1] if len(sys.argv) > 1 else "https://github.com/georgeGeorgakakos/k3s-metrics-gateway"
    subprocess.run([sys.executable, str(ROOT / "scripts/build_demo_data.py")], check=True)
    if OUT.exists():
        shutil.rmtree(OUT)
    demo = OUT / "demo"
    (demo / "ui").mkdir(parents=True)
    shutil.copy(UI / "index.html", demo / "index.html")
    for f in ("app.js", "style.css", "favicon.png", "demo-data.json"):
        shutil.copy(UI / f, demo / "ui" / f)

    readme = (ROOT / "README.md").read_text()
    blocks = re.findall(r"```mermaid\n(.*?)```", readme, re.S)
    figs = []
    for i, code in enumerate(blocks):
        title = TITLES[i] if i < len(TITLES) else f"Figure {i + 1}"
        figs.append(f'<section><h2>{title}</h2><div class="fig"><div class="tools">'
                    f'<button class="btn sm" data-z="in">+</button><button class="btn sm" data-z="out">−</button>'
                    f'<button class="btn sm" data-z="reset">reset</button></div>'
                    f'<pre class="mermaid">{html.escape(code)}</pre></div></section>')
    sys.path.insert(0, str(ROOT / "src"))
    from swch_metrics import __version__
    (OUT / "index.html").write_text(PAGE.format(repo=repo, figures="\n".join(figs), version=f"v{__version__}"))
    (OUT / ".nojekyll").write_text("")
    print(f"site built in {OUT} ({len(blocks)} figures)")


if __name__ == "__main__":
    main()
