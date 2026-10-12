// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Shared pieces of the Intelligence pages: governance availability, small badges and links, the "Scan now" dialog with its live progress, and the section navigation.
import { html, useState, useEffect, useRef, useAsync } from '../core/h.js';
import { api, can, hasRoute } from '../api/client.js';
import { Btn, Badge, Light, Banner, ErrorBox, Progress, Select, Kv, Loading } from '../ui/basic.js';
import { Icon } from '../ui/icons.js';
import { Dialog, toast } from '../ui/overlay.js';
import { fmtNum, ago, fmtDuration, durationBetween, sleep, plural } from '../core/util.js';
import { typeMeta, typeLabel, confLabel, confBadgeKind, CONF_HELP, scanLight, scanLabel, assetHref, sourceKindLabel } from '../intel/model.js';

// ---------------------------------------------------------------------------------------------------------------- governance availability
let govPromise = null;
/** Is the governance module installed and readable by this user? Cached; a 404 or 403 means "hide the governance parts", never an error. */
export function governanceSummary(force = false) {
  if (!govPromise || force) {
    // the OpenAPI document says whether the module is installed, so a missing module never produces a failing request
    govPromise = hasRoute('/v1/governance/summary', 'get').then((has) => (has ? api.get('/governance/summary') : Promise.reject(Object.assign(new Error('not installed'), { status: 404 }))))
      .then((data) => ({ available: true, data }))
      .catch((e) => { if (e && (e.status === 404 || e.status === 403 || e.status === 405 || e.status === 501 || e.status === 503)) return { available: false, data: null, status: e.status }; govPromise = null; throw e; });
  }
  return govPromise;
}
export function invalidateGovernance() { govPromise = null; decoCache.clear(); }
export function useGovernance() {
  const st = useAsync(() => governanceSummary().catch(() => ({ available: false, data: null })), []);
  return { loading: st.loading && !st.data, available: !!(st.data && st.data.available), summary: st.data && st.data.data, reload: () => { invalidateGovernance(); st.reloadLoud(); } };
}

// ---------------------------------------------------------------------------------------------------------------- small presentational pieces
export const orgCan = (perm) => can(perm);

export function TypeChip({ type, label }) {
  return html`<span class="type-chip"><${Icon} name=${typeMeta(type).icon} size="sm" /><span>${label || typeLabel(type)}</span></span>`;
}
export function AssetLink({ asset, tab, showType, class: cls = '' }) {
  if (!asset) return html`<span class="muted">unknown</span>`;
  if (asset.hidden) return html`<span class="muted" title="You do not have access to this asset">hidden asset</span>`;
  return html`<a class=${'asset-link ' + cls} href=${assetHref(asset.id, tab)} title=${asset.path || asset.name}><${Icon} name=${typeMeta(asset.type).icon} size="sm" /><span class="trunc">${asset.name || asset.id}</span></a>${showType ? html` <span class="small muted">${typeLabel(asset.type)}</span>` : null}`;
}
export function ConfBadge({ level, short }) {
  const l = level || 'unknown';
  return html`<${Badge} kind=${confBadgeKind(l)} title=${CONF_HELP[l]}>${short ? confLabel(l)[0] : confLabel(l)}<//>`;
}
/** Traffic-light status of a scan or source, in the same style as the control panel (colour plus a word). */
export function ScanBadge({ status, running }) {
  const s = running ? 'running' : status;
  const state = scanLight(s);
  return html`<span class=${'badge ' + state}><${Light} state=${state} label="" />${scanLabel(s)}</span>`;
}
export function StatusBadge2({ status }) {
  const kind = status === 'active' ? 'ok' : status === 'removed' ? 'off' : status === 'placeholder' ? 'warn' : '';
  const text = { active: 'Active', removed: 'Removed', placeholder: 'Placeholder' }[status] || status;
  return html`<${Badge} kind=${kind} title=${status === 'placeholder' ? 'Something refers to this, but no scan has found it yet.' : status === 'removed' ? 'A later scan no longer found it. History is kept.' : undefined}>${text}<//>`;
}
export function Ago({ ts }) { return ts ? html`<time datetime=${ts} title=${ts}>${ago(ts)}</time>` : html`<span class="muted">never</span>`; }

/** A stacked bar of confidence levels with a legend: how sure are the recorded connections? */
export function ConfidenceBar({ counts, total }) {
  const t = total != null ? total : Object.values(counts || {}).reduce((a, b) => a + b, 0);
  if (!t) return html`<div class="muted small">No connections recorded yet.</div>`;
  const order = ['verified', 'derived', 'inferred', 'unknown'];
  return html`<div><div class="conf-bar" role="img" aria-label=${order.map((k) => `${counts[k] || 0} ${confLabel(k).toLowerCase()}`).join(', ')}>${order.map((k) => (counts[k] ? html`<i key=${k} class=${'c-' + k} style=${`flex:${counts[k]}`} title=${`${confLabel(k)}: ${counts[k]}`}></i>` : null))}</div>
    <ul class="conf-legend">${order.map((k) => html`<li key=${k}><i class=${'sw c-' + k}></i><b>${fmtNum(counts[k] || 0)}</b> ${confLabel(k).toLowerCase()}</li>`)}</ul></div>`;
}

/** What governance knows about an asset, read defensively from the `governance` decoration that the asset routes carry when the governance module is installed. */
export function govBits(a) {
  const g = (a && a.governance) || {};
  const names = (v) => (Array.isArray(v) ? v : v && typeof v !== 'number' ? [v] : []).map((x) => (typeof x === 'string' ? x : x && (x.name || (x.classification && x.classification.name)) || '')).filter(Boolean);
  const cert = typeof g.certification === 'string' ? g.certification : g.certification && g.certification.state;
  const q = g.quality != null ? g.quality : g.quality_score != null ? g.quality_score : a && a.quality_score;
  const score = typeof q === 'number' ? q : q && typeof q.score === 'number' ? q.score : null;
  const dom = g.domain && typeof g.domain === 'object' ? (g.domain.name || (g.domain.domain && g.domain.domain.name)) : g.domain;
  return { certification: cert && cert !== 'none' ? cert : null, classifications: names(g.classifications), inherited: (Array.isArray(g.classifications) ? g.classifications : []).filter((c) => c && c.inherited).map((c) => c.name), domain: dom || null, owners: names(g.owners), ownerCount: typeof g.owner_count === 'number' ? g.owner_count : typeof g.owners === 'number' ? g.owners : names(g.owners).length, terms: names(g.glossary_terms), product: names(g.data_product)[0] || null, score, violations: Array.isArray(g.violations) ? g.violations.length : typeof g.violations === 'number' ? g.violations : g.violations && typeof g.violations.open === 'number' ? g.violations.open : 0 };
}
/** Governance facts of one asset for a result card, fetched per card (cached, four at a time). Only used when the list item carries no `governance` object of its own (an older server). */
const decoCache = new Map();
let decoActive = 0;
const decoWaiting = [];
function decoPump() {
  while (decoActive < 4 && decoWaiting.length) {
    const job = decoWaiting.shift();
    decoActive += 1;
    api.get(`/governance/assets/${encodeURIComponent(job.id)}`).then((r) => job.done(r), () => job.done(null)).finally(() => { decoActive -= 1; decoPump(); });
  }
}
export function governanceBitsOf(g) {
  if (!g) return null;
  const nm = (v) => (v || []).map((x) => x.name || (x.classification && x.classification.name)).filter(Boolean);
  const direct = ((g.classifications && g.classifications.direct) || []).filter((c) => c.status === 'approved');
  return { certification: g.certification && g.certification.state && g.certification.state !== 'none' ? g.certification.state : null, classifications: nm(direct), domain: g.domain && g.domain.domain ? g.domain.domain.name : null,
    owners: nm(g.owners && g.owners.effective), score: g.quality && typeof g.quality.score === 'number' ? g.quality.score : null, violations: (g.violations || []).length, terms: nm(g.terms), product: ((g.products || [])[0] || {}).name || null };
}
export function useGovernanceBits(asset, enabled) {
  const id = asset && asset.id;
  const [bits, setBits] = useState(() => (id && decoCache.has(id) && decoCache.get(id).bits) || null);
  useEffect(() => {
    if (!enabled || !id) return undefined;
    const hit = decoCache.get(id);
    if (hit && hit.bits !== undefined) { setBits(hit.bits); return undefined; }
    let live = true;
    const job = { id, done: (r) => { const b = governanceBitsOf(r); decoCache.set(id, { bits: b }); if (live) setBits(b); } };
    decoWaiting.push(job); decoPump();
    return () => { live = false; };
  }, [id, enabled]);
  return bits;
}
export const CERT_LABEL = { none: 'Not certified', draft: 'Draft', certified: 'Certified', deprecated: 'Deprecated' };
export function CertBadge({ state }) {
  if (!state || state === 'none') return null;
  const kind = state === 'certified' ? 'ok' : state === 'deprecated' ? 'bad' : 'warn';
  return html`<${Badge} kind=${kind} title=${state === 'certified' ? 'A person with approval rights confirmed this is fit to rely on.' : state === 'deprecated' ? 'Do not start using this. It is being phased out.' : 'Proposed for certification, not yet confirmed.'}><${Icon} name=${state === 'certified' ? 'shield-check' : 'info'} size="sm" />${CERT_LABEL[state] || state}<//>`;
}
const QUALITY_NAMES = { documentation: 'Documented', ownership: 'Has an owner', freshness: 'Fresh', certification: 'Certified', lineage: 'Lineage known', violations: 'No open policy violations', schema_stability: 'Stable structure', classification: 'Sensitivity reviewed' };
/** The plain-words name of a quality score component. */
export const qualityName = (n) => QUALITY_NAMES[n] || (String(n).replace(/_/g, ' ').replace(/^./, (c) => c.toUpperCase()));

export function QualityScore({ score }) {
  if (score == null) return null;
  const s = Math.round(score);
  return html`<span class=${'score ' + (s >= 80 ? '' : s >= 55 ? 'mid' : 'low')} title="Quality score out of 100: documentation, ownership, freshness and more"><span class="bar"><i style=${`width:${s}%`}></i></span>Quality ${s}</span>`;
}

// ---------------------------------------------------------------------------------------------------------------- governance constants
export const GOV_SECTIONS = [
  { id: '', label: 'Overview' }, { id: 'glossary', label: 'Glossary' }, { id: 'domains', label: 'Domains and products' }, { id: 'classifications', label: 'Classifications' },
  { id: 'review', label: 'Review queue' }, { id: 'requests', label: 'Requests' }, { id: 'policies', label: 'Policies and violations' }, { id: 'exposure', label: 'Exposure' }, { id: 'owners', label: 'Owners' },
];

// ---------------------------------------------------------------------------------------------------------------- scans
/** Poll one scan until it ends. Returns the scan row (or null while it is being fetched). */
export function useScan(scanId, { every = 1000 } = {}) {
  const [scan, setScan] = useState(null);
  const [error, setError] = useState(null);
  useEffect(() => {
    if (!scanId) { setScan(null); return undefined; }
    let stop = false;
    (async () => {
      for (let i = 0; i < 100000 && !stop; i += 1) {
        try {
          const s = await api.get(`/intelligence/scans/${encodeURIComponent(scanId)}`);
          if (stop) return;
          setScan(s); setError(null);
          if (s.status !== 'running') return;
        } catch (e) { if (!stop) setError(e); }
        await sleep(every);
      }
    })();
    return () => { stop = true; };
  }, [scanId]);
  return { scan, error };
}

export function scanCounts(scan) {
  const st = (scan && scan.stats) || {};
  const ap = st.apply || {};
  const c = st.counters || {};
  const seen = st.facts && st.facts.assets != null ? st.facts.assets : (ap.assets_created || 0) + (ap.assets_updated || 0) + (ap.assets_unchanged || 0);
  return { seen, assets: (ap.assets_created || 0) + (ap.assets_updated || 0), created: ap.assets_created || 0, updated: ap.assets_updated || 0, edges: (ap.edges_created || 0) + (ap.edges_updated || 0), gaps: st.gaps_reported ?? ap.gaps ?? 0, analysed: ap.analyzed || 0, counters: c, duration: st.duration_seconds };
}

/** A scan counter such as tables_unchanged in words, singular when the number is one ("1 table unchanged"). */
const counterLabel = (name, n) => { const w = String(name).split('_'); if (n === 1) { const i = w.findIndex((x) => x.length > 3 && x.endsWith('s') && !x.endsWith('ss')); if (i >= 0) w[i] = w[i].slice(0, -1); } return w.join(' '); };

/** Progress of one scan: state, counters so far, what was found, errors. Updates every second while the scan runs. */
export function ScanProgress({ scanId, onDone, compact }) {
  const { scan, error } = useScan(scanId);
  const done = useRef(false);
  useEffect(() => { if (scan && scan.status !== 'running' && !done.current) { done.current = true; onDone && onDone(scan); } }, [scan && scan.status]);
  if (error && !scan) return html`<${ErrorBox} error=${error} />`;
  if (!scan) return html`<${Loading} label="Starting the scan" />`;
  const running = scan.status === 'running';
  const k = scanCounts(scan);
  const kind = scan.status === 'succeeded' ? 'ok' : scan.status === 'partial' ? 'warn' : scan.status === 'running' ? 'info' : scan.status === 'cancelled' ? 'info' : 'bad';
  return html`<div class="col gap-3" data-testid="scan-progress">
    <div class="row"><${ScanBadge} status=${scan.status} /><span class="small muted">${scan.mode} scan · started <${Ago} ts=${scan.started_at} />${scan.finished_at ? ` · took ${fmtDuration(durationBetween(scan.started_at, scan.finished_at))}` : ''}</span></div>
    ${running ? html`<${Progress} indeterminate=${true} label="Scan in progress" />` : null}
    ${Object.keys(k.counters).length ? html`<div class="chips">${Object.entries(k.counters).map(([n, v]) => html`<span class="tag" key=${n}>${fmtNum(v)} ${counterLabel(n, v)}</span>`)}</div>` : null}
    ${!running ? html`<${Banner} kind=${kind} title=${scan.status === 'succeeded' ? 'The scan finished' : scan.status === 'partial' ? 'The scan finished, with problems' : scan.status === 'cancelled' ? 'The scan was cancelled' : 'The scan failed'}>
      ${compact ? null : html`${fmtNum(k.created)} new and ${fmtNum(k.updated)} changed assets, ${fmtNum(k.edges)} connections, ${plural(k.gaps, 'thing')} it could not see.`}${scan.error ? html` <div class="mt-2">${typeof scan.error === 'string' ? scan.error : JSON.stringify(scan.error)}</div>` : null}<//>` : null}</div>`;
}

/** Choose a source and a mode, start the scan, watch it run. */
export function ScanDialog({ sources, sourceId, onClose, onDone }) {
  const scannable = sources.filter((s) => s.enabled && s.kind !== 'scanner_import');
  const [src, setSrc] = useState(sourceId || (scannable[0] && scannable[0].id) || '');
  const [mode, setMode] = useState('incremental');
  const [scanId, setScanId] = useState(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  const [finished, setFinished] = useState(false);
  async function start() {
    setBusy(true); setErr(null);
    try { const r = await api.post(`/intelligence/sources/${encodeURIComponent(src)}/scan`, { mode }); setScanId(r.id || r.scan_id); } catch (e) { setErr(e); } finally { setBusy(false); }
  }
  async function cancel() { try { await api.post(`/intelligence/scans/${encodeURIComponent(scanId)}/cancel`); toast.info('Stopping the scan after the batch in progress'); } catch (e) { setErr(e); } }
  const footer = scanId
    ? html`${!finished ? html`<${Btn} kind="danger" icon="stop" onClick=${cancel}>Cancel the scan<//>` : null}<${Btn} kind="primary" onClick=${() => onClose(finished)}>${finished ? 'Close' : 'Hide (it keeps running)'}<//>`
    : html`<${Btn} onClick=${() => onClose(false)}>Not now<//><${Btn} kind="primary" icon="refresh" busy=${busy} disabled=${!src} onClick=${start} data-autofocus=${true}>Start the scan<//>`;
  return html`<${Dialog} title="Scan now" onClose=${() => onClose(finished)} footer=${footer}>
    ${err ? html`<div class="mb-3"><${ErrorBox} error=${err} /></div>` : null}
    ${!scanId ? html`<div class="col gap-2">
      <${Select} label="What to scan" value=${src} onChange=${setSrc} options=${scannable.map((s) => ({ value: s.id, label: `${s.name} (${sourceKindLabel(s.kind)})` }))} />
      <fieldset class="fieldset"><legend>How much</legend>
        <label class="radio-row"><input type="radio" name="scan-mode" checked=${mode === 'incremental'} onChange=${() => setMode('incremental')} /><span><b>Only what changed</b><span class="small muted block">Quick. Re-reads items that changed since the last scan.</span></span></label>
        <label class="radio-row"><input type="radio" name="scan-mode" checked=${mode === 'full'} onChange=${() => setMode('full')} /><span><b>Everything</b><span class="small muted block">Slower. Also notices things that disappeared and marks them as removed (their history is kept).</span></span></label></fieldset>
      <p class="small muted m-0">Scanning reads names, structure and definitions. It does not read the data inside your tables.</p></div>`
      : html`<${ScanProgress} scanId=${scanId} onDone=${(s) => { setFinished(true); onDone && onDone(s); }} />`}</${Dialog}>`;
}

/** Sources list with a short load; shared by the overview and the discovery pages. */
export function useSources() {
  const st = useAsync(() => api.get('/intelligence/sources').then((r) => r.items || []), []);
  return { ...st, items: st.data || [] };
}

export { Kv };
