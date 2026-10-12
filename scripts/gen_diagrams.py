# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
# ruff: noqa: E702
"""Generate the README graphics in ``docs/images/diagrams`` (hand-laid-out SVG, system fonts only, self-contained dark cards that read on light and dark GitHub themes).

    python scripts/gen_diagrams.py            # rewrite the SVG files
    python scripts/gen_diagrams.py --check    # exit 1 when a committed file differs from what this script produces

Why a generator: the pictures are part of the documentation, so they must be reproducible and reviewable as code, not hand-edited exports.
"""
import pathlib
import sys
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape as esc

OUT = pathlib.Path(__file__).resolve().parents[1] / "docs" / "images" / "diagrams"
BG, CARD, EDGE, TXT, MUT = "#0b1220", "#121c33", "#2a3a5e", "#e8eefb", "#9fb0d0"
TEAL, BLUE, AMB, VIO, GRN, ROSE = "#2dd4bf", "#60a5fa", "#fbbf24", "#a78bfa", "#34d399", "#fb7185"
FONT = "font-family=\"'Segoe UI',-apple-system,BlinkMacSystemFont,'Helvetica Neue',Arial,sans-serif\""

def head(w, h, title, desc):
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" height="{h}" role="img" aria-labelledby="t d">'
            f'<title id="t">{esc(title)}</title><desc id="d">{esc(desc)}</desc>'
            '<defs>'
            f'<marker id="ar" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="{MUT}"/></marker>'
            f'<marker id="art" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="{TEAL}"/></marker>'
            f'<marker id="arb" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="{BLUE}"/></marker>'
            f'<marker id="ara" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="{AMB}"/></marker>'
            f'<linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#0b1220"/><stop offset="1" stop-color="#14213d"/></linearGradient>'
            '</defs>'
            f'<rect width="{w}" height="{h}" rx="18" fill="url(#g)"/>')

def text(x, y, s, size=14, fill=TXT, weight=400, anchor="start", extra=""):
    return f'<text x="{x}" y="{y}" {FONT} font-size="{size}" fill="{fill}" font-weight="{weight}" text-anchor="{anchor}" {extra}>{esc(s)}</text>'

def box(x, y, w, h, title=None, sub=None, stroke=EDGE, fill=CARD, tcol=TXT, r=12, tsize=15, ssize=12, dash=False):
    d = ' stroke-dasharray="6 5"' if dash else ""
    s = f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{r}" fill="{fill}" stroke="{stroke}" stroke-width="1.6"{d}/>'
    if title:
        s += text(x + w / 2, y + (h / 2 - (6 if sub else -5)), title, tsize, tcol, 600, "middle")
    if sub:
        lines = sub if isinstance(sub, list) else [sub]
        for i, ln in enumerate(lines):
            s += text(x + w / 2, y + h / 2 + 13 + i * 15, ln, ssize, MUT, 400, "middle")
    return s

def panel(x, y, w, h, label, color):
    return (f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="16" fill="{color}" fill-opacity="0.07" stroke="{color}" stroke-opacity="0.55" stroke-width="1.5"/>'
            + text(x + 18, y + 28, label, 13, color, 700, "start", 'letter-spacing="1.2"'))

def arrow(x1, y1, x2, y2, color=MUT, mk="ar", dash=False, both=False, w=2):
    d = ' stroke-dasharray="7 5"' if dash else ""
    ms = f' marker-end="url(#{mk})"' + (f' marker-start="url(#{mk})"' if both else "")
    return f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="{w}"{d}{ms}/>'

def path(dd, color=MUT, mk="ar", dash=False, w=2, both=False):
    d = ' stroke-dasharray="7 5"' if dash else ""
    ms = f' marker-end="url(#{mk})"' + (f' marker-start="url(#{mk})"' if both else "")
    return f'<path d="{dd}" fill="none" stroke="{color}" stroke-width="{w}"{d}{ms}/>'

def pill(x, y, label, color, w=None):
    w = w or (len(label) * 7.2 + 20)
    return (f'<rect x="{x}" y="{y}" width="{w}" height="24" rx="12" fill="{color}" fill-opacity="0.15" stroke="{color}" stroke-opacity="0.7"/>'
            + text(x + w / 2, y + 16, label, 12, color, 600, "middle"))

def node(x, y, label, color=TEAL):
    """a small machine glyph with a status lamp"""
    return (f'<rect x="{x}" y="{y}" width="46" height="34" rx="6" fill="{CARD}" stroke="{color}" stroke-width="1.6"/>'
            f'<rect x="{x+6}" y="{y+8}" width="34" height="5" rx="2" fill="{color}" fill-opacity="0.45"/>'
            f'<rect x="{x+6}" y="{y+18}" width="22" height="5" rx="2" fill="{color}" fill-opacity="0.25"/>'
            f'<circle cx="{x+37}" cy="{y+21}" r="3.5" fill="{GRN}"/>'
            + text(x + 23, y + 50, label, 11, MUT, 500, "middle"))

# ------------------------------------------------------------------------------------------------------------ 1. banner
def banner():
    w, h = 1280, 360
    s = head(w, h, "Open Lakehouse Compute Fabric", "Extend Microsoft Fabric to compute you own, or run a complete lakehouse standalone")
    # decorative node mesh on the right
    pts = [(900, 90), (1010, 60), (1130, 110), (960, 190), (1090, 215), (1200, 175), (930, 290), (1050, 305), (1170, 285)]
    edges = [(0, 1), (1, 2), (0, 3), (1, 3), (2, 4), (3, 4), (4, 5), (2, 5), (3, 6), (4, 7), (6, 7), (7, 8), (4, 8), (5, 8)]
    for a, b in edges:
        s += f'<line x1="{pts[a][0]}" y1="{pts[a][1]}" x2="{pts[b][0]}" y2="{pts[b][1]}" stroke="{TEAL}" stroke-opacity="0.28" stroke-width="1.5"/>'
    cols = [TEAL, BLUE, VIO, TEAL, AMB, BLUE, TEAL, VIO, GRN]
    for (x, y), c in zip(pts, cols, strict=True):
        s += f'<circle cx="{x}" cy="{y}" r="15" fill="{c}" fill-opacity="0.16" stroke="{c}" stroke-width="1.8"/><circle cx="{x}" cy="{y}" r="4.5" fill="{c}"/>'
    s += text(64, 120, "OPEN LAKEHOUSE", 22, TEAL, 700, "start", 'letter-spacing="6"')
    s += text(62, 182, "Compute Fabric", 68, TXT, 800)
    s += text(64, 230, "Extend Microsoft Fabric to the compute you own.", 25, "#cbd8f2", 500)
    s += text(64, 264, "Or run the whole lakehouse standalone, on a laptop, on-prem or in a VM.", 20, MUT, 400)
    x = 64
    for lab, c in [("Apache-2.0", GRN), ("Delta Lake", BLUE), ("DuckDB", AMB), ("Apache Spark", ROSE), ("MCP for AI agents", VIO), ("Lineage and governance", ROSE)]:
        s += pill(x, 296, lab, c); x += len(lab) * 7.2 + 32
    return s + "</svg>"

# ------------------------------------------------------------------------------------------------------------ 2. fabric extension
def fabric():
    w, h = 1280, 760
    s = head(w, h, "How OLCF extends Microsoft Fabric", "A Fabric notebook sends itself as a signed capsule to the OLCF control plane, which runs it on your own Spark workers; workers read and write OneLake directly and the status flows back to the notebook")
    s += text(w / 2, 44, "Extend Microsoft Fabric to the compute you own", 24, TXT, 700, "middle")
    s += text(w / 2, 70, "one call from the notebook: olcf.run_remote()", 15, MUT, 400, "middle")
    s += panel(30, 96, 330, 540, "MICROSOFT FABRIC  (cloud)", BLUE)
    s += panel(400, 96, 400, 540, "OLCF CONTROL PLANE  (your network)", TEAL)
    s += panel(840, 96, 410, 540, "YOUR COMPUTE  (on-prem / VM / Kubernetes)", AMB)
    s += box(58, 146, 274, 84, "Fabric notebook", ["import olcf_fabric as olcf", "olcf.run_remote()"], stroke=BLUE)
    s += box(58, 262, 274, 62, "Fabric pipeline", "notebook activity sees the exit value", stroke=BLUE)
    s += box(58, 366, 274, 62, "Microsoft Entra ID", "tenant, workspace and token scoping", stroke=BLUE)
    s += box(58, 470, 274, 84, "OneLake / ADLS Gen2", ["Lakehouse Files and Tables", "Delta, direct abfss access"], stroke=BLUE)
    s += box(428, 146, 344, 62, "Fabric adapter", "validate, challenge, verify capsule", stroke=TEAL)
    s += box(428, 230, 344, 62, "Compatibility analyzer", "73 rules, shims, blockers rejected up front", stroke=TEAL)
    s += box(428, 314, 344, 62, "Scheduler and job service", "placement, queue, retries, cancel", stroke=TEAL)
    s += box(428, 398, 344, 62, "Worker hub", "workers dial in (outbound WebSocket)", stroke=TEAL)
    s += box(428, 482, 160, 62, "Metadata DB", "SQLite / PostgreSQL", stroke=TEAL)
    s += box(612, 482, 160, 62, "Audit and RBAC", "tamper-evident log", stroke=TEAL)
    s += text(600, 584, "Console, REST API, CLI, MCP endpoint", 13, MUT, 500, "middle")
    s += text(600, 604, "Nothing here needs inbound access to your workers.", 12, MUT, 400, "middle")
    for nx, lab in [(884, "worker A"), (990, "worker B"), (1096, "worker C")]:
        s += node(nx, 146, lab)
    s += box(868, 224, 366, 78, "Spark Standalone cluster", ["driver and executors across machines", "immutable signed runtimes, side by side"], stroke=AMB)
    s += box(868, 330, 170, 62, "Host Python", "notebooks without Spark", stroke=AMB)
    s += box(1064, 330, 170, 62, "DuckDB / delta-rs", "SQL and table reads", stroke=AMB)
    s += box(868, 420, 366, 62, "Local, S3, MinIO, ADLS", "your own storage works the same way", stroke=AMB)
    # notebook -> adapter (capsule) and the way back (status)
    s += arrow(332, 190, 428, 177, TEAL, "art", w=2.6)
    s += text(380, 154, "signed capsule", 11, TEAL, 700, "middle")
    s += text(380, 167, "over HTTPS", 11, TEAL, 700, "middle")
    s += arrow(428, 345, 332, 300, BLUE, "arb", dash=True, w=1.8)
    s += text(380, 364, "status, logs,", 11, BLUE, 600, "middle")
    s += text(380, 378, "exit value", 11, BLUE, 600, "middle")
    # inside the control plane
    s += arrow(600, 208, 600, 230, MUT); s += arrow(600, 292, 600, 314, MUT); s += arrow(600, 376, 600, 398, MUT)
    # workers dial in
    s += arrow(772, 429, 868, 263, TEAL, "art", both=True, w=2.4)
    s += text(838, 350, "outbound", 11, TEAL, 700, "middle", 'transform="rotate(-61 838 350)"')
    s += arrow(1051, 302, 1051, 330, MUT); s += arrow(1051, 392, 1051, 420, MUT)
    # data never detours: out of the compute panel, under every panel, into OneLake
    s += path("M 1051 482 V 660 Q 1051 690 1021 690 H 225 Q 195 690 195 660 V 556", AMB, "ara", w=2.4, both=True)
    s += text(w / 2, 716, "direct reads and writes: executors talk to OneLake themselves, data never detours through the control plane", 13, AMB, 600, "middle")
    s += text(w / 2, 744, "Fabric is an adapter, not the core: every part of this runs without a Microsoft subscription.", 13, MUT, 400, "middle")
    return s + "</svg>"

# ------------------------------------------------------------------------------------------------------------ 3. modes
def modes():
    w, h = 1280, 520
    s = head(w, h, "Three ways to run OLCF", "Standalone on one machine, a multi-machine Spark fabric, or extending Microsoft Fabric")
    s += text(w / 2, 44, "Start on a laptop. Grow into a fabric. Plug in Microsoft Fabric when you want to.", 22, TXT, 700, "middle")
    cw, cy, ch = 392, 80, 400
    xs = [28, 444, 860]
    labs = [("1  STANDALONE", TEAL, "One machine, nothing else needed"), ("2  MULTI-MACHINE FABRIC", AMB, "Every machine is a worker"), ("3  FABRIC-EXTENDED", BLUE, "Your Spark runs Fabric notebooks")]
    for x, (lab, c, sub) in zip(xs, labs, strict=True):
        s += panel(x, cy, cw, ch, lab, c)
        s += text(x + 18, cy + 52, sub, 14, "#cbd8f2", 500)
    # card 1
    x = xs[0]
    s += box(x + 24, 150, 344, 56, "Browser, olcf CLI, Python SDK, AI agent", None, stroke=TEAL, tsize=13)
    s += box(x + 24, 236, 344, 90, "OLCF server + embedded worker", ["console, SQL, notebooks, pipelines", "sample Retail lakehouse included"], stroke=TEAL)
    s += box(x + 24, 356, 344, 56, "Local disk: Delta tables", None, stroke=TEAL, tsize=14)
    s += arrow(x + 196, 206, x + 196, 236, MUT); s += arrow(x + 196, 326, x + 196, 356, MUT, both=True)
    s += text(x + 196, 446, "olcf quickstart  -  no Spark, no Fabric, no cloud", 12, TEAL, 600, "middle")
    # card 2
    x = xs[1]
    s += box(x + 24, 150, 344, 56, "Control plane (1 or more nodes)", "PostgreSQL for HA", stroke=AMB, tsize=14)
    for i, lab in enumerate(["server", "VM", "rack", "cloud"]):
        s += node(x + 40 + i * 82, 238, lab, AMB)
    s += box(x + 24, 310, 344, 56, "Spark Standalone across the machines", None, stroke=AMB, tsize=14)
    s += box(x + 24, 384, 344, 38, "S3, MinIO, ADLS, local", None, stroke=AMB, tsize=13)
    s += arrow(x + 196, 206, x + 196, 238, MUT)
    s += arrow(x + 196, 292, x + 196, 310, MUT); s += arrow(x + 196, 366, x + 196, 384, MUT)
    s += text(x + 196, 446, "olcf worker enrol  -  machines connect outward", 12, AMB, 600, "middle")
    # card 3
    x = xs[2]
    s += box(x + 24, 150, 344, 56, "Fabric notebook: olcf.run_remote()", None, stroke=BLUE, tsize=14)
    s += box(x + 24, 236, 344, 56, "OLCF control plane + analyzer", "signed capsule, single-use nonce", stroke=TEAL, tsize=14)
    s += box(x + 24, 322, 344, 56, "Your Spark workers", None, stroke=AMB, tsize=14)
    s += box(x + 24, 398, 344, 30, "OneLake, direct", None, stroke=BLUE, tsize=13)
    s += arrow(x + 196, 206, x + 196, 236, BLUE, "arb"); s += arrow(x + 196, 292, x + 196, 322, MUT); s += arrow(x + 196, 378, x + 196, 398, AMB, "ara")
    s += text(x + 196, 450, "attach the olcf_fabric wheel to a Fabric Environment", 12, BLUE, 600, "middle")
    s += text(w / 2, 506, "The same server and the same console in all three. Fabric is an adapter, not a dependency.", 13, MUT, 400, "middle")
    return s + "</svg>"

# ------------------------------------------------------------------------------------------------------------ 4. feature map
def features():
    w, h = 1280, 724
    s = head(w, h, "What is in the box", "Capability map: lakehouse, SQL, notebooks, pipelines, compute fabric, Fabric extension, AI agents, security and operations, Intelligence, catalog and glossary, governance, runtimes")
    s += text(w / 2, 44, "What is in the box", 26, TXT, 700, "middle")
    tiles = [
        ("Lakehouse", "Delta, Iceberg discovery, Parquet", "Files/Tables explorer, protocol inspection, maintenance", TEAL),
        ("SQL", "DuckDB and Spark SQL", "editor, history, exports, saved queries, dashboards", BLUE),
        ("Notebooks", "Jupyter-compatible, real kernels", "Python on any worker, PySpark on the fabric", VIO),
        ("Pipelines", "DAGs of typed activities", "cron, webhook, storage triggers, retries, resume", AMB),
        ("Compute fabric", "Spark across machines", "workers dial in; signed immutable runtimes, pools, queues, drain", GRN),
        ("Fabric extension", "olcf.run_remote() from Fabric", "capsule, 73-rule analyzer, notebookutils shim", BLUE),
        ("AI agents (MCP)", "29 tools, read-only by default", "per-call authorisation and audit, ready-made configs", ROSE),
        ("Security and ops", "RBAC, OIDC/Entra, mTLS, audit", "encrypted backup, signed updates, Prometheus/OTLP", TEAL),
        ("Intelligence", "discovery and lineage", "tables, columns, notebooks, pipelines, reports; confidence and gaps on every edge", VIO),
        ("Catalog and glossary", "stable asset ids, search", "terms, domains, data products, owners, classifications", TEAL),
        ("Governance", "observes, never blocks", "classification review, certification, policies, quality score", AMB),
        ("Runtimes", "Fabric 1.3 and 2.0 compatible", "Spark 3.5 and 4.1 side by side, signed, per-job dependency layers", BLUE),
    ]
    tw, th = 288, 184
    for i, (t, a, b, c) in enumerate(tiles):
        x = 32 + (i % 4) * (tw + 24); y = 84 + (i // 4) * (th + 24)
        s += f'<rect x="{x}" y="{y}" width="{tw}" height="{th}" rx="14" fill="{CARD}" stroke="{c}" stroke-opacity="0.7" stroke-width="1.6"/>'
        s += f'<rect x="{x}" y="{y}" width="{tw}" height="6" rx="3" fill="{c}"/>'
        s += f'<circle cx="{x+34}" cy="{y+50}" r="16" fill="{c}" fill-opacity="0.18" stroke="{c}" stroke-width="1.6"/><circle cx="{x+34}" cy="{y+50}" r="5" fill="{c}"/>'
        s += text(x + 62, y + 57, t, 20, TXT, 700)
        s += text(x + 22, y + 106, a, 14, "#cbd8f2", 600)
        for j, part in enumerate(_wrap(b, 36)):
            s += text(x + 22, y + 132 + j * 18, part, 13, MUT, 400)
    return s + "</svg>"

def _wrap(t, n):
    out, cur = [], ""
    for wd in t.split():
        if len(cur) + len(wd) + 1 > n and cur:
            out.append(cur); cur = wd
        else:
            cur = (cur + " " + wd).strip()
    return out + [cur]

# ------------------------------------------------------------------------------------------------------------ 5. agents
def agents():
    w, h = 1280, 440
    s = head(w, h, "AI agents and OLCF", "AI agents connect over MCP with exactly the permissions of the person who created the token, and can also contribute to the code base")
    s += text(w / 2, 42, "Built to be used, and extended, by people and by AI agents", 23, TXT, 700, "middle")
    s += panel(28, 70, 600, 330, "USE IT: MCP ENDPOINT", VIO)
    s += box(52, 112, 160, 70, "Claude Desktop", "Claude Code, any MCP client", stroke=VIO, ssize=11)
    s += box(52, 212, 160, 70, "Your own agent", "script, service, SI", stroke=VIO, ssize=11)
    s += box(244, 140, 170, 110, "/mcp", ["token = the person's", "permissions, audited"], stroke=TEAL)
    s += box(446, 112, 160, 56, "Catalog", "browse, describe", stroke=TEAL, ssize=11)
    s += box(446, 184, 160, 56, "Read-only SQL", "guarded, per call", stroke=TEAL, ssize=11)
    s += box(446, 256, 160, 56, "Jobs and logs", "status, results", stroke=TEAL, ssize=11)
    s += arrow(212, 147, 244, 185, VIO, both=True); s += arrow(212, 247, 244, 215, VIO, both=True)
    s += arrow(414, 195, 446, 140, MUT); s += arrow(414, 195, 446, 212, MUT); s += arrow(414, 195, 446, 284, MUT)
    s += text(328, 350, "olcf mcp config --client claude-code", 13, VIO, 600, "middle")
    s += text(328, 376, "prints a ready-to-paste configuration", 12, MUT, 400, "middle")
    s += panel(652, 70, 600, 330, "BUILD ON IT: AGENT-FRIENDLY REPO", AMB)
    items = [("AGENTS.md", "setup, commands, conventions, guardrails"), ("Contracts", "docs/dev/CONTRACTS.md: the interfaces to keep"), ("Tests first", "one pytest run per tests/ directory, gates in CI"), ("Open work", "release-readiness checklist: acceptance work")]
    for i, (a, b) in enumerate(items):
        y = 112 + i * 66
        s += box(676, y, 552, 54, None, None, stroke=AMB)
        s += text(696, y + 24, a, 15, TXT, 700); s += text(696, y + 43, b, 12.5, MUT, 400)
    s += text(w / 2, 424, "Fork it, point your agent at it, open a pull request. Every change needs a failing test first and a green gate.", 13, MUT, 400, "middle")
    return s + "</svg>"

# ------------------------------------------------------------------------------------------------------------ 6. architecture
def architecture():
    w, h = 1280, 800
    s = head(w, h, "OLCF architecture", "Clients call the control plane; machines in the worker fleet connect outward to it and read and write data directly; Intelligence observes the platform and, optionally, a Fabric tenant")
    s += text(w / 2, 40, "Architecture", 24, TXT, 700, "middle")
    for i, (t, sub, c) in enumerate([("Web console", "Preact, no build step", TEAL), ("olcf CLI", "every feature scriptable", TEAL), ("Python SDK", "olcf.sdk", TEAL),
                                     ("Fabric notebook", "olcf_fabric SDK", BLUE), ("AI agent", "MCP client", VIO)]):
        x = 40 + i * 244
        s += box(x, 64, 216, 64, t, sub, stroke=c, tsize=15, ssize=11.5)
        s += arrow(x + 108, 128, x + 108, 150, c, "ar", w=2)
    s += panel(28, 150, 1224, 322, "CONTROL PLANE", TEAL)
    s += text(250, 178, "one process, or several behind a load balancer", 12, MUT, 400)
    s += box(40, 192, 1192, 44, "REST API /v1  -  identity, RBAC, audit  -  MCP endpoint /mcp", None, stroke=TEAL, tsize=14)
    mods = [("Workspaces and items", ["notebooks, pipelines, SQL,", "environments, connections"], TEAL),
            ("Catalog", ["lakehouses, Delta/Iceberg,", "dashboards, lineage mirror"], TEAL),
            ("Scheduler and jobs", ["placement, queues, retries,", "sessions, cancel"], TEAL),
            ("Intelligence", ["discovery, lineage, glossary,", "governance (observe-only)"], VIO),
            ("Fabric adapter", ["analyzer, capsule,", "diagnostics"], BLUE)]
    for i, (t, sub, c) in enumerate(mods):
        x = 40 + i * 242
        s += box(x, 252, 224, 76, t, sub, stroke=c, tsize=14.5, ssize=11.5)
    for i, (t, sub) in enumerate([("Metadata DB", "SQLite or PostgreSQL"), ("Worker hub", "agents dial in"), ("Runtime catalog", "signed, immutable"), ("Platform event bus", "dataset.read / written / ...")]):
        x = 40 + i * 242
        s += box(x, 372, 224, 60, t, sub, stroke=VIO if i == 3 else TEAL, tsize=14, ssize=11.5)
    s += arrow(878, 372, 878, 328, VIO, "ar", w=2)
    s += text(886, 352, "events", 11, VIO, 600)
    s += text(w / 2, 458, "A single machine runs the same design: an embedded worker agent enrols itself with the control plane.", 12, MUT, 400, "middle")
    # workers
    s += panel(28, 500, 900, 130, "WORKER FLEET  -  machines connect outward", AMB)
    for i, (lab, rt) in enumerate([("worker A", "runtimes 2.0 + 1.3"), ("worker B", "runtime 2.0"), ("worker C", "runtime 1.3")]):
        x = 52 + i * 120
        s += node(x, 540, lab, AMB)
        s += text(x + 23, 612, rt, 10, MUT, 400, "middle")
    s += box(420, 540, 484, 70, "Spark Standalone + Host Python", ["driver and executors across machines; Python notebooks and DuckDB without Spark"], stroke=AMB, tsize=14.5, ssize=11.5)
    s += arrow(394, 432, 394, 500, AMB, "ara", both=True, w=2.4)
    s += text(404, 470, "outbound WebSocket, mTLS optional", 11, AMB, 600)
    # external
    s += panel(952, 500, 300, 130, "OPTIONAL: MICROSOFT FABRIC", BLUE)
    s += text(1102, 560, "Fabric notebooks call run_remote()", 12.5, "#cbd8f2", 500, "middle")
    s += text(1102, 580, "Intelligence can scan the tenant", 12.5, "#cbd8f2", 500, "middle")
    s += text(1102, 600, "and Power BI (read-only)", 12.5, "#cbd8f2", 500, "middle")
    s += arrow(1120, 328, 1120, 500, BLUE, "arb", dash=True, w=1.8, both=True)
    s += text(1130, 420, "run_remote capsule, tenant scans", 11, BLUE, 600)
    # data
    s += panel(28, 654, 1224, 112, "DATA PLANE  -  executors and drivers read and write directly", GRN)
    for i, (t, sub) in enumerate([("Local disk", "Delta, Parquet"), ("S3 / MinIO", "Delta, Iceberg, Parquet"), ("ADLS Gen2", "abfss"), ("OneLake", "abfss, direct")]):
        x = 52 + i * 296
        s += box(x, 692, 264, 56, t, sub, stroke=GRN, tsize=14, ssize=11.5)
    s += arrow(628, 630, 628, 692, GRN, "ar", w=2)
    s += text(w / 2, 788, "Control-plane reads for SQL and previews use DuckDB and delta-rs; nothing in the control plane needs inbound access to a worker.", 12, MUT, 400, "middle")
    return s + "</svg>"

# ------------------------------------------------------------------------------------------------------------ 7. intelligence
def intelligence():
    w, h = 1280, 640
    s = head(w, h, "OLCF Intelligence", "Sources feed connectors and analyzers, which build one asset graph with stable ids, confidence and gaps; the graph serves catalog, lineage, glossary and governance through the console, REST, CLI, SDK and MCP")
    s += text(w / 2, 42, "OLCF Intelligence: what do we have, where does it come from, what depends on it, who owns it", 21, TXT, 700, "middle")
    s += panel(28, 70, 300, 520, "READS", BLUE)
    src = [("This installation", ["workspaces, lakehouses, tables,", "notebooks, pipelines, saved SQL,", "reports and dashboards"], TEAL),
           ("Microsoft Fabric tenant", ["optional: Fabric REST, definitions,", "Delta log through OneLake,", "Power BI admin scanner"], BLUE),
           ("Saved scanner file", ["import a Power BI scanner", "result without any connection"], BLUE),
           ("Run-time events", ["dataset.read / written / statement", "from real jobs, notebooks, SQL"], GRN)]
    for i, (t, sub, c) in enumerate(src):
        s += box(48, 112 + i * 118, 260, 98, t, sub, stroke=c, tsize=14.5, ssize=11.5)
    s += panel(372, 70, 536, 520, "ENGINE  (a module over the unchanged catalog)", TEAL)
    s += box(392, 112, 496, 82, "Connectors and scans", ["facts and definitions; incremental, resumable, leased;", "partial scans never remove anything"], stroke=TEAL, tsize=14.5, ssize=11.5)
    s += box(392, 224, 496, 112, "Lineage analyzers (pure, deterministic, never run your code)", ["SQL, PySpark/Python, notebooks, OLCF and Fabric pipelines,", "Spark job definitions, semantic models (TMDL, DAX, M),", "Power BI reports (PBIR), scanner results"], stroke=VIO, tsize=14, ssize=11.5)
    s += box(392, 366, 496, 120, "Asset graph", ["stable ast_ ids with aliases (the same table is one asset", "whoever names it); edges carry evidence and a confidence:", "verified (run-time) - derived - inferred - unknown"], stroke=VIO, tsize=15, ssize=11.5)
    s += box(392, 512, 496, 56, "Gaps: what could not be seen, why, and the fix", None, stroke=AMB, tsize=13.5)
    s += arrow(308, 160, 392, 153, MUT, "ar"); s += arrow(308, 276, 392, 160, MUT, "ar"); s += arrow(308, 394, 392, 168, MUT, "ar")
    s += arrow(308, 512, 392, 428, GRN, "ar")
    s += arrow(640, 194, 640, 224, MUT); s += arrow(640, 336, 640, 366, MUT); s += arrow(640, 486, 640, 512, AMB, "ara")
    s += panel(952, 70, 300, 520, "SERVES", VIO)
    out = [("Catalog and search", "assets, columns, owners", TEAL), ("Lineage and impact", "table and column level", VIO), ("Glossary and domains", "terms, data products, owners", TEAL),
           ("Governance", "classification review, certification, policies, quality (observe-only)", AMB), ("Console, REST, CLI, SDK, MCP", "the same answers everywhere", BLUE)]
    for i, (t, sub, c) in enumerate(out):
        y = 112 + i * 94
        s += box(972, y, 260, 78, t, _wrap(sub, 38), stroke=c, tsize=14, ssize=11.5)
    s += arrow(888, 426, 972, 340, MUT, "ar")
    s += text(w / 2, 622, "A failure in Intelligence never stops compute. Sampling is off unless you turn it on. Governance reports; it does not enforce.", 13, MUT, 400, "middle")
    return s + "</svg>"

ALL = [("banner", banner), ("fabric-extension", fabric), ("deployment-modes", modes), ("feature-map", features), ("agents", agents), ("architecture", architecture), ("intelligence", intelligence)]

if __name__ == "__main__":
    check = "--check" in sys.argv
    bad = 0
    for name, fn in ALL:
        p = OUT / f"{name}.svg"
        svg = fn()
        if check:
            if not p.exists() or p.read_text(encoding="utf-8") != svg:
                print(f"out of date: {p}"); bad += 1
        else:
            p.write_text(svg, encoding="utf-8"); print(p.name, p.stat().st_size)
        ET.fromstring(svg)  # noqa: S314 - our own generated text          # well-formed XML (a duplicate attribute silently truncates the picture in a browser)
    sys.exit(1 if bad else 0)
