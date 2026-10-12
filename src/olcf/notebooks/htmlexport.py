# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
"""Static, script-free HTML export of a notebook (``GET /v1/notebooks/{id}/export?format=html``).

Notebook outputs are attacker-controlled (a notebook may contain ``text/html`` output from untrusted code), so nothing is passed through
unchecked:

* all text is escaped; Markdown is rendered by the small renderer below (no raw HTML pass-through);
* ``text/html`` outputs go through :func:`sanitize_html`, an *allow-list* sanitizer built on :mod:`html.parser` - unknown tags are removed
  (``script``/``style``/``iframe``/``object``/``embed``/``svg``/``math``/``form`` ... together with their content), event handler and ``style``
  attributes never survive, ``href`` may only be ``http(s):``, ``mailto:`` or a ``#fragment``, ``src`` only a base64 ``data:image/...`` URI;
* images are only embedded as validated base64 ``data:`` URIs (SVG is embedded through ``<img>``, where browsers never run its scripts);
* the page carries a ``Content-Security-Policy`` meta tag (``default-src 'none'``) and the API serves it with the same policy plus ``sandbox``.
"""

from __future__ import annotations

import base64
import binascii
import html
import json
import re
from html.parser import HTMLParser
from typing import Any

ANSI = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")
CSP = "default-src 'none'; img-src data:; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'; sandbox"

_ALLOWED_TAGS = {
    "p", "br", "hr", "div", "span", "h1", "h2", "h3", "h4", "h5", "h6", "b", "i", "em", "strong", "u", "s", "del", "ins", "sub", "sup", "code", "pre",
    "blockquote", "ul", "ol", "li", "dl", "dt", "dd", "table", "thead", "tbody", "tfoot", "tr", "th", "td", "caption", "colgroup", "col", "a", "img",
    "small", "mark", "kbd", "abbr", "figure", "figcaption", "details", "summary", "center", "tt", "samp", "var", "cite", "q",
}
_DROP_WITH_CONTENT = {"script", "style", "iframe", "frame", "frameset", "object", "embed", "applet", "template", "noscript", "svg", "math", "form", "textarea",
                      "select", "option", "button", "input", "link", "meta", "base", "head", "title", "audio", "video", "canvas", "xmp", "plaintext", "noembed"}
_VOID = {"br", "hr", "img", "col"}
_GLOBAL_ATTRS = {"class", "title", "lang", "dir"}
_TAG_ATTRS = {"a": {"href", "rel"}, "img": {"src", "alt", "width", "height"}, "td": {"colspan", "rowspan", "align"}, "th": {"colspan", "rowspan", "align", "scope"},
              "col": {"span"}, "ol": {"start", "type"}, "details": {"open"}}
_CLASS_OK = re.compile(r"^[A-Za-z0-9_\- ]{0,200}$")
_NUM = re.compile(r"^\d{1,5}$")
_DATA_IMG = re.compile(r"^data:image/(png|jpe?g|gif|webp|svg\+xml);base64,([A-Za-z0-9+/=\s]{1,20000000})$", re.I)


def _safe_href(v: str) -> str | None:
    v = v.strip()
    if v.startswith("#") and re.match(r"^#[\w\-.:]{0,200}$", v):
        return v
    if re.match(r"^(https?:|mailto:)", v, re.I) and not re.search(r"[\x00-\x1f\x7f]", v):
        return v
    return None


def _safe_img(v: str) -> str | None:
    v = re.sub(r"\s+", "", v.strip())
    m = _DATA_IMG.match(v)
    if not m:
        return None
    try:
        base64.b64decode(m.group(2), validate=True)
    except (binascii.Error, ValueError):
        return None
    return v


class _Sanitizer(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.out: list[str] = []
        self.stack: list[str] = []
        self.drop_depth = 0
        self.drop_tag: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if self.drop_depth:
            if tag == self.drop_tag:
                self.drop_depth += 1
            return
        if tag in _DROP_WITH_CONTENT:
            if tag not in _VOID:
                self.drop_depth, self.drop_tag = 1, tag
            return
        if tag not in _ALLOWED_TAGS:
            return  # unknown wrapper: drop the tag, keep (escaped) children
        allowed = _GLOBAL_ATTRS | _TAG_ATTRS.get(tag, set())
        rendered = []
        for name, value in attrs:
            name = name.lower()
            if name not in allowed or value is None and name not in ("open",):
                continue
            v = value or ""
            if name == "href":
                v2 = _safe_href(v)
                if v2 is None:
                    continue
                v = v2
            elif name == "src":
                v2 = _safe_img(v)
                if v2 is None:
                    continue
                v = v2
            elif name == "class":
                if not _CLASS_OK.match(v):
                    continue
            elif name in ("colspan", "rowspan", "width", "height", "span", "start"):
                if not _NUM.match(v):
                    continue
            elif name in ("align", "scope", "type", "dir"):
                if not re.match(r"^[A-Za-z0-9]{1,12}$", v):
                    continue
            elif name == "rel":
                continue
            elif name == "lang" and not re.match(r"^[A-Za-z0-9\-]{1,16}$", v):
                continue
            rendered.append(f' {name}="{html.escape(v, quote=True)}"' if name != "open" else " open")
        if tag == "a":
            rendered.append(' rel="noopener noreferrer nofollow" target="_blank"')
        if tag == "img" and not any(a.startswith(" src=") for a in rendered):
            return  # an image without a safe source renders nothing
        self.out.append(f"<{tag}{''.join(rendered)}>")
        if tag not in _VOID:
            self.stack.append(tag)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)
        if tag.lower() not in _VOID and not self.drop_depth:
            self.handle_endtag(tag)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if self.drop_depth:
            if tag == self.drop_tag:
                self.drop_depth -= 1
                if not self.drop_depth:
                    self.drop_tag = None
            return
        if tag in self.stack:
            while self.stack:
                t = self.stack.pop()
                self.out.append(f"</{t}>")
                if t == tag:
                    break

    def handle_data(self, data: str) -> None:
        if not self.drop_depth:
            self.out.append(html.escape(data, quote=False))

    def close_all(self) -> str:
        while self.stack:
            self.out.append(f"</{self.stack.pop()}>")
        return "".join(self.out)


def sanitize_html(markup: str, max_bytes: int = 5 * 1024 * 1024) -> str:
    """Allow-list sanitize an HTML fragment (see module docstring)."""
    if len(markup) > max_bytes:
        return "<p><em>output omitted: too large</em></p>"
    p = _Sanitizer()
    p.feed(markup)
    p.close()
    return p.close_all()


# ============================================================================== markdown (tiny, escaping first)
_INLINE_CODE = re.compile(r"`([^`\n]+)`")
_BOLD = re.compile(r"\*\*(?=\S)(.+?)(?<=\S)\*\*|__(?=\S)(.+?)(?<=\S)__")
_ITALIC = re.compile(r"(?<![\w*])\*(?=\S)([^*\n]+?)(?<=\S)\*(?![\w*])|(?<![\w_])_(?=\S)([^_\n]+?)(?<=\S)_(?![\w_])")
_LINK = re.compile(r"\[([^\]\n]+)\]\(([^)\s]+)(?:\s+&quot;[^&]*&quot;)?\)")
_IMAGE = re.compile(r"!\[([^\]\n]*)\]\(([^)\s]+)\)")


def _inline(text: str) -> str:
    esc = html.escape(text, quote=True)
    codes: list[str] = []

    def stash(m: re.Match[str]) -> str:
        codes.append(f"<code>{m.group(1)}</code>")
        return f"\x00{len(codes) - 1}\x00"

    esc = _INLINE_CODE.sub(stash, esc)

    def image(m: re.Match[str]) -> str:
        src = _safe_img(html.unescape(m.group(2)))
        alt = m.group(1)
        return f'<img src="{html.escape(src, quote=True)}" alt="{alt}">' if src else alt

    esc = _IMAGE.sub(image, esc)

    def link(m: re.Match[str]) -> str:
        href = _safe_href(html.unescape(m.group(2)))
        return f'<a href="{html.escape(href, quote=True)}" rel="noopener noreferrer nofollow" target="_blank">{m.group(1)}</a>' if href else m.group(1)

    esc = _LINK.sub(link, esc)
    esc = _BOLD.sub(lambda m: f"<strong>{m.group(1) or m.group(2)}</strong>", esc)
    esc = _ITALIC.sub(lambda m: f"<em>{m.group(1) or m.group(2)}</em>", esc)
    return re.sub(r"\x00(\d+)\x00", lambda m: codes[int(m.group(1))], esc)


def render_markdown(src: str) -> str:
    lines = src.split("\n")
    out: list[str] = []
    i = 0
    para: list[str] = []

    def flush() -> None:
        if para:
            out.append("<p>" + "<br>".join(_inline(p) for p in para) + "</p>")
            para.clear()

    while i < len(lines):
        ln = lines[i]
        s = ln.strip()
        if s.startswith("```"):
            flush()
            j = i + 1
            buf = []
            while j < len(lines) and not lines[j].strip().startswith("```"):
                buf.append(lines[j])
                j += 1
            out.append("<pre><code>" + html.escape("\n".join(buf)) + "</code></pre>")
            i = j + 1
            continue
        m = re.match(r"^(#{1,6})\s+(.*?)\s*#*\s*$", s)
        if m:
            flush()
            out.append(f"<h{len(m.group(1))}>{_inline(m.group(2))}</h{len(m.group(1))}>")
        elif re.match(r"^([-*_])(\s*\1){2,}$", s):
            flush()
            out.append("<hr>")
        elif re.match(r"^[-*+]\s+", s) or re.match(r"^\d+[.)]\s+", s):
            flush()
            ordered = bool(re.match(r"^\d+[.)]\s+", s))
            items = []
            while i < len(lines) and (re.match(r"^\s*[-*+]\s+", lines[i]) if not ordered else re.match(r"^\s*\d+[.)]\s+", lines[i])):
                items.append(re.sub(r"^\s*([-*+]|\d+[.)])\s+", "", lines[i]))
                i += 1
            tag = "ol" if ordered else "ul"
            out.append(f"<{tag}>" + "".join(f"<li>{_inline(x)}</li>" for x in items) + f"</{tag}>")
            continue
        elif s.startswith(">"):
            flush()
            q = []
            while i < len(lines) and lines[i].strip().startswith(">"):
                q.append(lines[i].strip().lstrip(">").strip())
                i += 1
            out.append("<blockquote>" + render_markdown("\n".join(q)) + "</blockquote>")
            continue
        elif "|" in s and i + 1 < len(lines) and re.match(r"^\s*\|?\s*:?-{2,}:?\s*(\|\s*:?-{2,}:?\s*)*\|?\s*$", lines[i + 1]):
            flush()
            head = [c.strip() for c in s.strip("|").split("|")]
            i += 2
            rows = []
            while i < len(lines) and "|" in lines[i] and lines[i].strip():
                rows.append([c.strip() for c in lines[i].strip().strip("|").split("|")])
                i += 1
            out.append("<table><thead><tr>" + "".join(f"<th>{_inline(h)}</th>" for h in head) + "</tr></thead><tbody>"
                       + "".join("<tr>" + "".join(f"<td>{_inline(c)}</td>" for c in r) + "</tr>" for r in rows) + "</tbody></table>")
            continue
        elif not s:
            flush()
        else:
            para.append(ln.rstrip())
        i += 1
    flush()
    return "\n".join(out)


# ============================================================================== notebook
_CSS = """
body{font-family:system-ui,-apple-system,Segoe UI,sans-serif;max-width:60rem;margin:2rem auto;padding:0 1rem;color:#1c1c1c;background:#fff}
.cell{margin:1rem 0}.prompt{color:#888;font:12px monospace;margin-bottom:2px}
pre{background:#f6f8fa;border:1px solid #e1e4e8;border-radius:6px;padding:.6rem .8rem;overflow:auto;font-size:13px;margin:0}
.out pre{background:#fff;border-style:dashed}.err pre{background:#fff5f5;border-color:#e5b8b8}
table{border-collapse:collapse;margin:.4rem 0}th,td{border:1px solid #d0d7de;padding:.2rem .5rem;text-align:left}th{background:#f6f8fa}
img{max-width:100%}.meta{color:#666;font-size:12px}footer{margin-top:2rem;color:#888;font-size:12px}
@media (prefers-color-scheme: dark){body{background:#111;color:#e6e6e6}pre{background:#1a1a1a;border-color:#333}.out pre{background:#111}th{background:#1a1a1a}th,td{border-color:#333}}
"""


def _pre(text: str, cls: str = "") -> str:
    return f'<pre class="{cls}">{html.escape(text)}</pre>'


def _render_output(o: dict[str, Any]) -> str:
    t = o.get("output_type")
    if t == "stream":
        return _pre(ANSI.sub("", str(o.get("text", ""))), "stream")
    if t == "error":
        tb = "\n".join(ANSI.sub("", str(x)) for x in (o.get("traceback") or [])) or f"{o.get('ename')}: {o.get('evalue')}"
        return f'<div class="err">{_pre(tb)}</div>'
    data = o.get("data") if isinstance(o.get("data"), dict) else {}
    for mime in ("image/png", "image/jpeg", "image/gif", "image/webp"):
        if mime in data:
            raw = data[mime] if isinstance(data[mime], str) else "".join(data[mime])
            uri = _safe_img(f"data:{mime};base64,{raw}")
            if uri:
                return f'<img src="{html.escape(uri, quote=True)}" alt="output image">'
    if "image/svg+xml" in data:
        svg = data["image/svg+xml"] if isinstance(data["image/svg+xml"], str) else "".join(data["image/svg+xml"])
        uri = _safe_img("data:image/svg+xml;base64," + base64.b64encode(svg.encode()).decode())
        if uri:
            return f'<img src="{html.escape(uri, quote=True)}" alt="output svg">'
    if "text/html" in data:
        h = data["text/html"] if isinstance(data["text/html"], str) else "".join(data["text/html"])
        return "<div class=\"html-out\">" + sanitize_html(h) + "</div>"
    if "text/markdown" in data:
        md = data["text/markdown"] if isinstance(data["text/markdown"], str) else "".join(data["text/markdown"])
        return "<div>" + render_markdown(md) + "</div>"
    if "application/vnd.olcf.exit+json" in data:
        return _pre("exit value: " + json.dumps(data["application/vnd.olcf.exit+json"], default=str), "exit")
    if "application/json" in data:
        return _pre(json.dumps(data["application/json"], indent=2, default=str))
    if "text/plain" in data:
        txt = data["text/plain"] if isinstance(data["text/plain"], str) else "".join(data["text/plain"])
        return _pre(ANSI.sub("", txt))
    return ""


def render_html(nb: dict[str, Any], title: str = "Notebook", *, outputs: bool = True) -> str:
    """The full, self-contained and script-free HTML document."""
    parts = [f'<!doctype html><html lang="en"><head><meta charset="utf-8"><meta http-equiv="Content-Security-Policy" content="{html.escape(CSP, quote=True)}">',
             f"<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>{html.escape(title)}</title><style>{_CSS}</style></head><body>",
             f"<h1>{html.escape(title)}</h1>"]
    for c in nb.get("cells", []):
        if c["cell_type"] == "markdown":
            parts.append('<div class="cell md">' + render_markdown(c["source"]) + "</div>")
        elif c["cell_type"] == "raw":
            parts.append('<div class="cell raw">' + _pre(c["source"]) + "</div>")
        else:
            ec = c.get("execution_count")
            tags = (c.get("metadata") or {}).get("tags") or []
            label = f"In [{ec if ec is not None else ' '}]" + (f" &middot; {html.escape(', '.join(map(str, tags)))}" if tags else "")
            parts.append(f'<div class="cell code"><div class="prompt">{label}</div>{_pre(c["source"], "code")}')
            if outputs:
                for o in c.get("outputs") or []:
                    rendered = _render_output(o)
                    if rendered:
                        parts.append(f'<div class="out">{rendered}</div>')
            parts.append("</div>")
    parts.append("<footer>Exported by Open Lakehouse Compute Fabric (author: Justin Dodrill). Static document: no scripts are executed.</footer></body></html>")
    return "\n".join(parts)
