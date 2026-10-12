// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Intelligence overview and the data catalog (faceted search over every asset the platform knows about).
import { html, useState, useEffect, useAsync, useMemo, useDebounced } from '../core/h.js';
import { api, can } from '../api/client.js';
import { setQuery, useRoute } from '../core/router.js';
import { Btn, Card, Banner, Empty, ErrorBox, Loading, PageHead, Input, Select, Tag } from '../ui/basic.js';
import { Icon } from '../ui/icons.js';
import { StatTile } from '../charts/Chart.js';
import { fmtNum, plural } from '../core/util.js';
import { loadWorkspaces } from './common.js';
import { typeMeta, typeLabel, typePlural, CATALOG_TYPES, gapMeta, catalogHref, discoveryHref, governanceHref, assetHref, sourceKindLabel, pct } from '../intel/model.js';
import { AssetLink, ConfidenceBar, ScanBadge, Ago, TypeChip, StatusBadge2, ScanDialog, useGovernance, useGovernanceBits, useSources, govBits, CertBadge, QualityScore } from './intel_common.js';

const cap = (s) => s.charAt(0).toUpperCase() + s.slice(1);
const CODE_TYPES = ['notebook', 'pipeline', 'activity', 'sql_query', 'spark_job', 'script', 'dataflow'];
const UNREADABLE = new Set(['parse_error', 'unsupported_construct', 'dynamic_reference', 'not_inspected', 'permission_denied', 'api_unavailable']);

// ---------------------------------------------------------------------------------------------------------------- overview
/** The latest scan that has an earlier scan of the same source, and what changed between them. */
async function loadLatestChange() {
  const snaps = (await api.get('/intelligence/snapshots', { limit: 50 })).items || [];
  const done = snaps.filter((s) => s.status === 'succeeded' || s.status === 'partial');
  const bySource = new Map();
  for (const s of done) { const l = bySource.get(s.source_id) || []; l.push(s); bySource.set(s.source_id, l); }
  let best = null;
  for (const l of bySource.values()) if (l.length >= 2 && (!best || String(l[0].finished_at) > String(best[0].finished_at))) best = l;
  if (!best) return { diff: null, scans: done.length };
  const diff = await api.get('/intelligence/snapshots/diff', { from: best[1].id, to: best[0].id, limit: 12 });
  return { diff, scans: done.length };
}

function QuickSearch({ initial = '' }) {
  const [q, setQ] = useState(initial);
  const go = (e) => { e.preventDefault(); window.location.hash = catalogHref({ q: q.trim() }); };
  return html`<form class="intel-search" role="search" onSubmit=${go}>
    <div class="grow"><input class="input" type="search" value=${q} onInput=${(e) => setQ(e.target.value)} placeholder="Search tables, columns, notebooks, reports by name or description" aria-label="Search the catalog" /></div>
    <${Btn} kind="primary" type="submit" icon="search">Search<//></form>`;
}

function ChangeCard({ change }) {
  if (change.loading && !change.data) return html`<${Loading} label="Comparing the last two scans" />`;
  if (change.error) return html`<div class="muted small">The comparison is not available right now.</div>`;
  const { diff, scans } = change.data || {};
  if (!diff) return html`<div class="muted">${scans < 1 ? 'No scan has finished yet.' : 'Only one scan has finished so far. Scan again later and this shows what was added, removed or changed in between.'}</div>`;
  const s = diff.summary || {};
  const a = diff.assets || {};
  const show = (list, cls, sign) => (list || []).slice(0, 5).map((x) => html`<li key=${x.id}><span class=${cls} aria-hidden="true">${sign}</span><${AssetLink} asset=${x} /> <span class="small muted">${typeLabel(x.type)}</span></li>`);
  return html`<div class="col gap-3">
    <div class="small muted">Between the scan of <${Ago} ts=${diff.from && diff.from.finished_at} /> and the scan of <${Ago} ts=${diff.to && diff.to.finished_at} />.</div>
    <div class="chips"><span class="badge ok">${fmtNum(s.assets_added || 0)} assets added</span><span class="badge warn">${fmtNum(s.assets_changed || 0)} changed</span><span class="badge bad">${fmtNum(s.assets_removed || 0)} removed</span>
      <span class="badge info">${fmtNum(s.edges_added || 0)} connections added</span>${s.edges_removed ? html`<span class="badge bad">${fmtNum(s.edges_removed)} connections removed</span>` : null}</div>
    <ul class="change-list">${show(a.added, 'delta-add', '+')}${show(a.removed, 'delta-del', '−')}${show((a.changed || []).map((c) => c.asset || c), 'delta-chg', '~')}</ul>
    <a href=${discoveryHref('snapshots', { from: diff.from && diff.from.scan_id, to: diff.to && diff.to.scan_id })}>See the full comparison</a></div>`;
}

export function IntelOverviewPage() {
  const sum = useAsync(() => api.get('/intelligence/summary'), []);
  const src = useSources();
  const gov = useGovernance();
  const gaps = useAsync(() => api.get('/intelligence/gaps', { limit: 200 }).then((r) => r.items || []).catch(() => []), []);
  const change = useAsync(loadLatestChange, []);
  const [scanning, setScanning] = useState(false);
  const manage = can('intelligence.manage');
  const s = sum.data;
  const reloadAll = () => { sum.reload(); src.reload(); gaps.reload(); change.reload(); };

  const byType = useMemo(() => (s ? Object.entries(s.assets.by_type).sort((a, b) => b[1] - a[1]) : []), [s]);
  const active = s ? s.assets.by_status.active || 0 : 0;
  const placeholders = s ? s.placeholders || 0 : 0;
  const never = s && s.last_scans.length === 0;
  const codeTotal = s ? CODE_TYPES.reduce((n, t) => n + (s.assets.by_type[t] || 0), 0) : 0;
  const codeBad = useMemo(() => {
    const ids = new Set();
    for (const g of gaps.data || []) if (g.asset_id && UNREADABLE.has(g.kind)) ids.add(g.asset_id);
    return ids.size;
  }, [gaps.data]);
  const codeOk = Math.max(0, codeTotal - codeBad);
  const foundPct = s && active + placeholders ? pct(active, active + placeholders) : null;
  const tileCount = 4 + (foundPct != null ? 1 : 0) + (gov.available && gov.summary ? 2 : 0);
  const tileCols = tileCount <= 5 ? tileCount : Math.ceil(tileCount / 2);                     // 7 tiles make 4 + 3, never 5 + 2

  const actions = html`${manage ? html`<${Btn} kind="primary" icon="refresh" onClick=${() => setScanning(true)} disabled=${!src.items.some((x) => x.enabled && x.kind !== 'scanner_import')}>Scan now<//>` : null}<${Btn} icon="search" href=${catalogHref()}>Browse the catalog<//>`;
  return html`<div class="page"><${PageHead} icon="sparkles" title="Intelligence" sub="What data you have, where it comes from and who is affected when it changes. Everything here comes from scans of your sources, and it says plainly what the scans could not see." actions=${actions} />
    ${sum.error && !s ? html`<${ErrorBox} error=${sum.error} onRetry=${sum.reloadLoud} />` : !s ? html`<${Loading} label="Loading the overview" />` : html`
      <${QuickSearch} />
      ${never ? html`<div class="mb-4"><${Banner} kind="info" title="Nothing has been scanned yet" actions=${manage ? html`<${Btn} kind="primary" size="sm" onClick=${() => setScanning(true)}>Scan now<//>` : null}>Intelligence learns about your data by scanning it. A scan reads the names, structure and definitions of what you have. It does not read the data inside your tables.${manage ? '' : ' Ask an administrator to run the first scan.'}<//></div>` : null}
      <div class="intel-tiles" style=${`--cols:${tileCols}`}>
        <${StatTile} label="Assets" value=${fmtNum(active)} sub=${placeholders ? `${fmtNum(placeholders)} more are only mentioned, never found` : 'Tables, columns, notebooks, reports and more'} href=${catalogHref()} />
        <${StatTile} label="Connections" value=${fmtNum(s.edges.total)} sub="Recorded links between assets" href=${lineageIndex()} />
        <${StatTile} label="Open gaps" value=${fmtNum(s.gaps.open)} state=${s.gaps.open ? 'degraded' : 'ok'} sub=${s.gaps.open ? 'Things the scans could not see' : 'Nothing known to be missing'} href=${discoveryHref('gaps')} />
        <${StatTile} label="Code read fully" value=${codeTotal ? `${pct(codeOk, codeTotal)}%` : '–'} state=${codeTotal && codeBad ? 'degraded' : codeTotal ? 'ok' : undefined} sub=${codeTotal ? `${codeOk} of ${codeTotal} notebooks, queries and scripts` : 'No code found yet'} href=${discoveryHref('coverage')} />
        ${foundPct != null ? html`<${StatTile} label="Found by a scan" value=${`${foundPct}%`} sub=${placeholders ? `${plural(placeholders, 'referenced item')} no scan has found` : 'Everything mentioned was found'} href=${catalogHref({ status: 'placeholder' })} />` : null}
        ${gov.available && gov.summary ? html`<${StatTile} label="Waiting for review" value=${fmtNum((gov.summary.review_queue && gov.summary.review_queue.total) || 0)} sub="Suggested labels and links" href=${governanceHref('review')} />
          <${StatTile} label="Policy violations" value=${fmtNum((gov.summary.violations && gov.summary.violations.open) || 0)} state=${gov.summary.violations && gov.summary.violations.open ? 'degraded' : 'ok'} sub="Reported only. Policies do not block anything." href=${governanceHref('policies')} />` : null}
      </div>
      <div class="grid c2 top">
        <${Card} title="Assets by type" sub=${`${fmtNum(active)} active`}>
          ${byType.length ? html`<div class="type-counts">${byType.map(([t, n]) => html`<a key=${t} href=${catalogHref({ type: t })}><${Icon} name=${typeMeta(t).icon} /><div><b>${fmtNum(n)}</b><div><span>${cap(typePlural(t, n))}</span></div></div></a>`)}</div>` : html`<div class="muted">No assets yet.</div>`}</${Card}>
        <${Card} title="Sources and their last scan" actions=${html`<a class="small" href=${discoveryHref()}>Manage sources</a>`}>
          ${src.error && !src.data ? html`<${ErrorBox} error=${src.error} />` : !src.data ? html`<${Loading} />` : html`<ul class="src-list">${src.items.map((x) => html`<li key=${x.id}>
            <div class="grow"><b>${x.name}</b><div class="small muted">${sourceKindLabel(x.kind)}${x.enabled ? '' : ' · switched off'}</div></div>
            <${ScanBadge} status=${x.last_status} running=${x.running} />
            <div class="small muted">${x.last_scan_at ? html`<${Ago} ts=${x.last_scan_at} />` : 'never scanned'}${x.next_due_at && x.enabled && x.kind !== 'scanner_import' ? html`<div>next <${Ago} ts=${x.next_due_at} /></div>` : null}</div>
            ${manage && x.enabled && x.kind !== 'scanner_import' ? html`<${Btn} size="sm" onClick=${() => setScanning(x.id)}>Scan<//>` : null}</li>`)}</ul>`}</${Card}>
        <${Card} title="How sure are the connections?" sub="Every link keeps how it was found">
          <${ConfidenceBar} counts=${s.edges.by_confidence} total=${s.edges.total} />
          <p class="small muted mt-3 m-0">Verified means the platform observed it while data was processed. Derived means read from code or from a definition, such as a pipeline's dependencies or a report's model, and not yet seen running. Inferred is a guess from names and unknown means a dependency exists but how the data moves is not known. Lineage is never claimed to be complete: see the gaps.</p></${Card}>
        <${Card} title="What we could not see" actions=${html`<a class="small" href=${discoveryHref('gaps')}>All gaps</a>`}>
          ${s.gaps.open ? html`<ul class="change-list">${Object.entries(s.gaps.by_kind).sort((a, b) => b[1] - a[1]).map(([k, n]) => html`<li key=${k}><${Icon} name=${gapMeta(k).icon} size="sm" /><a href=${discoveryHref('gaps', { kind: k })}><b>${n}</b> ${gapMeta(k).label.toLowerCase()}</a></li>`)}</ul>
            <p class="small muted mt-3 m-0">Each of these hides part of the picture. Open the gap to see what is missing and how to fix it.</p>` : html`<div class="muted">No open gaps. That only means the scans found nothing they could not read.</div>`}</${Card}>
        <${Card} title="What changed since the previous scan"><${ChangeCard} change=${change} /></${Card}>
        ${gov.available && gov.summary ? html`<${Card} title="Governance" actions=${html`<a class="small" href=${governanceHref()}>Open governance</a>`}>
          <div class="chips mb-3">${Object.entries((gov.summary.classifications && gov.summary.classifications.by_label) || {}).slice(0, 6).map(([k, n]) => html`<span class="tag cls" key=${k}>${n} ${k}</span>`)}</div>
          ${gov.summary.coverage ? html`<div class="small">Owners assigned on <b>${Math.round(gov.summary.coverage.owner_pct || 0)}%</b> of datasets, described on <b>${Math.round(gov.summary.coverage.description_pct || 0)}%</b>, classified and reviewed on <b>${Math.round(gov.summary.coverage.classification_reviewed_pct || 0)}%</b>.</div>` : null}
          ${(gov.summary.top_unowned || []).length ? html`<div class="mt-3"><div class="small muted mb-2">Without an owner</div><ul class="change-list">${gov.summary.top_unowned.slice(0, 4).map((a) => html`<li key=${a.id}><${AssetLink} asset=${a} /></li>`)}</ul></div>` : null}</${Card}>` : null}
      </div>`}
    ${scanning ? html`<${ScanDialog} sources=${src.items} sourceId=${typeof scanning === 'string' ? scanning : undefined} onClose=${() => { setScanning(false); reloadAll(); }} onDone=${reloadAll} />` : null}</div>`;
}
const lineageIndex = () => '#/intelligence/lineage';

// ---------------------------------------------------------------------------------------------------------------- catalog
const FILTER_KEYS = ['q', 'type', 'workspace_id', 'domain', 'classification', 'certification', 'owner', 'tag', 'status'];
const FILTER_LABEL = { q: 'Search', type: 'Type', workspace_id: 'Workspace', domain: 'Domain', classification: 'Classification', certification: 'Certification', owner: 'Owner', tag: 'Tag', status: 'Status' };
const PAGE = 25;

function Facets({ query, set, workspaces, summary, gov }) {
  const [domains, setDomains] = useState([]);
  const [classes, setClasses] = useState([]);
  useEffect(() => {
    if (!gov.available) return;
    api.get('/governance/domains', { limit: 200 }).then((r) => setDomains(r.items || [])).catch(() => {});
    api.get('/governance/classifications', { limit: 200 }).then((r) => setClasses(r.items || [])).catch(() => {});
  }, [gov.available]);
  const [q, setQ] = useState(query.q || '');
  const [tag, setTag] = useState(query.tag || '');
  const [owner, setOwner] = useState(query.owner || '');
  const dq = useDebounced(q, 350);
  useEffect(() => { if ((query.q || '') !== dq) set({ q: dq }); }, [dq]);
  useEffect(() => { setQ(query.q || ''); }, [query.q]);
  const counts = (summary && summary.assets.by_type) || {};
  const types = [...new Set([...CATALOG_TYPES, ...Object.keys(counts)])].filter((t) => counts[t] || t === query.type);
  return html`<aside class="facets" aria-label="Filters">
    <div class="field"><label for="cat-q">Search</label><input id="cat-q" class="input" type="search" value=${q} onInput=${(e) => setQ(e.target.value)} placeholder="Name, path, description, alias" /></div>
    <div><h2 class="mb-2">Type</h2><ul class="facet-types">
      <li><button type="button" aria-pressed=${!query.type} onClick=${() => set({ type: '' })}><span class="grow">Everything</span></button></li>
      ${types.map((t) => html`<li key=${t}><button type="button" aria-pressed=${query.type === t} onClick=${() => set({ type: query.type === t ? '' : t })}><${Icon} name=${typeMeta(t).icon} size="sm" /><span class="grow trunc">${typeLabel(t)}</span><span class="small muted">${counts[t] != null ? fmtNum(counts[t]) : ''}</span></button></li>`)}</ul></div>
    ${workspaces.length > 1 ? html`<${Select} label="Workspace" value=${query.workspace_id || ''} onChange=${(v) => set({ workspace_id: v })} placeholder="Any workspace" options=${workspaces.map((w) => ({ value: w.id, label: w.display_name || w.name }))} />` : null}
    ${gov.available ? html`
      <${Select} label="Domain" value=${query.domain || ''} onChange=${(v) => set({ domain: v })} placeholder="Any domain" options=${domains.map((d) => ({ value: d.id, label: d.name }))} />
      <${Select} label="Classification" value=${query.classification || ''} onChange=${(v) => set({ classification: v })} placeholder="Any" options=${classes.map((c) => ({ value: c.id, label: c.name }))} />
      <${Select} label="Certification" value=${query.certification || ''} onChange=${(v) => set({ certification: v })} placeholder="Any" options=${[{ value: 'certified', label: 'Certified' }, { value: 'draft', label: 'Draft' }, { value: 'deprecated', label: 'Deprecated' }]} />
      <form onSubmit=${(e) => { e.preventDefault(); set({ owner }); }}><${Input} label="Owner" value=${owner} onInput=${setOwner} placeholder="User name or e-mail, then Enter" /></form>` : null}
    <form onSubmit=${(e) => { e.preventDefault(); set({ tag }); }}><${Input} label="Tag" value=${tag} onInput=${setTag} placeholder="Tag, then Enter" /></form>
    <${Select} label="Status" value=${query.status || 'active'} onChange=${(v) => set({ status: v === 'active' ? '' : v })} options=${[{ value: 'active', label: 'Active' }, { value: 'placeholder', label: 'Mentioned but never found' }, { value: 'removed', label: 'Removed by a later scan' }, { value: 'any', label: 'All of them' }]} />
  </aside>`;
}

function ResultCard({ a, govOn }) {
  const own = govBits(a);
  const loaded = useGovernanceBits(a, !!govOn && !a.governance && !['column', 'visual', 'file', 'workspace'].includes(a.type));      // the list item brings its own compact facts; the per-card call is the fallback for a server that does not
  const g = loaded || own;
  const sens = (a.attrs && a.attrs.sensitive_columns) || [];
  const rows = a.attrs && a.attrs.row_count;
  return html`<li class="asset-card"><div class="ic"><${Icon} name=${typeMeta(a.type).icon} /></div>
    <div class="grow">
      <h3><a href=${assetHref(a.id)}>${a.name}</a></h3>
      ${a.path ? html`<div class="path">${a.path}</div>` : null}
      ${a.description ? html`<div class="desc">${a.description}</div>` : null}
      <div class="badges"><${TypeChip} type=${a.type} />${a.status !== 'active' ? html`<${StatusBadge2} status=${a.status} />` : null}<${CertBadge} state=${g.certification} />
        ${g.classifications.map((c) => html`<span class="tag cls" key=${c} title=${(g.inherited || []).includes(c) ? 'Classification that reaches this asset through lineage' : 'Classification'}>${c}${(g.inherited || []).includes(c) ? ' (inherited)' : ''}</span>`)}
        ${!g.classifications.length && sens.length ? html`<span class="tag cls suggested" title="Column names suggest sensitive data. Nothing is classified until a person confirms it.">${sens.length} column${sens.length === 1 ? '' : 's'} may be sensitive</span>` : null}
        ${g.domain ? html`<span class="tag" title="Domain">${g.domain}</span>` : null}
        ${(a.tags || []).slice(0, 4).map((t) => html`<span class="tag" key=${t}>${t}</span>`)}
        ${rows != null ? html`<span class="small muted">${fmtNum(rows)} rows</span>` : null}
        ${g.owners.length ? html`<span class="small muted">Owner: ${g.owners.slice(0, 2).join(', ')}</span>` : g.ownerCount ? html`<span class="small muted">${plural(g.ownerCount, 'owner')}</span>` : null}
        <${QualityScore} score=${g.score} /></div></div></li>`;
}

export function IntelCatalogPage() {
  const loc = useRoute();
  const query = loc.query;
  const gov = useGovernance();
  const sum = useAsync(() => api.get('/intelligence/summary').catch(() => null), []);
  const ws = useAsync(() => loadWorkspaces().catch(() => []), []);
  const [cursors, setCursors] = useState([null]);           // cursor used for each page already visited
  const filterSig = FILTER_KEYS.map((k) => query[k] || '').join('|');
  useEffect(() => { setCursors([null]); }, [filterSig]);
  const page = cursors.length - 1;
  const res = useAsync(() => {
    const p = { limit: PAGE, cursor: cursors[page] || undefined };
    for (const k of FILTER_KEYS) if (query[k]) p[k] = query[k];
    return api.get('/intelligence/assets', p);
  }, [filterSig, cursors.join(',')]);
  const set = (patch) => setQuery(patch);
  const active = FILTER_KEYS.filter((k) => query[k]);
  const label = (k) => {
    const v = query[k];
    if (k === 'type') return typeLabel(v);
    if (k === 'workspace_id') { const w = (ws.data || []).find((x) => x.id === v); return w ? w.display_name || w.name : v; }
    if (k === 'status') return { placeholder: 'Mentioned but never found', removed: 'Removed', any: 'All statuses', active: 'Active' }[v] || v;
    return v;
  };
  const items = res.data ? res.data.items : [];
  const next = res.data && res.data.next_cursor;
  return html`<div class="page"><${PageHead} icon="search" title="Data catalog" sub="Find any table, column, notebook, query or report that scans have discovered. Filters are kept in the address, so you can share a search." />
    <div class="catalog">
      <${Facets} query=${query} set=${set} workspaces=${ws.data || []} summary=${sum.data} gov=${gov} />
      <div>
        ${active.length ? html`<div class="active-filters" aria-label="Active filters">${active.map((k) => html`<span class="filter-chip" key=${k}>${FILTER_LABEL[k]}: ${label(k)}<button type="button" aria-label=${`Remove filter ${FILTER_LABEL[k]}`} onClick=${() => set({ [k]: '' })}><${Icon} name="x" size="sm" /></button></span>`)}<${Btn} size="sm" kind="ghost" onClick=${() => set(Object.fromEntries(FILTER_KEYS.map((k) => [k, ''])))}>Clear all<//></div>` : null}
        ${res.error && !res.data ? html`<${ErrorBox} error=${res.error} onRetry=${res.reloadLoud} />` : !res.data ? html`<${Loading} label="Searching" />` : !items.length ? html`<${Empty} icon="search" title=${active.length ? 'Nothing matches these filters' : 'The catalog is empty'}>${active.length ? 'Try fewer filters or a different word. Names, paths, descriptions and aliases are searched.' : 'Run a scan from the Discovery page. Until a scan has run, there is nothing to list.'}${active.length ? null : html`<div class="mt-3"><${Btn} kind="primary" href=${discoveryHref()}>Go to Discovery<//></div>`}</${Empty}>` : html`
          <div class="small muted mb-3" aria-live="polite">Showing ${page * PAGE + 1} to ${page * PAGE + items.length}${next ? ', more on the next page' : ''}</div>
          <ul class="results" aria-label="Search results">${items.map((a) => html`<${ResultCard} key=${a.id} a=${a} govOn=${gov.available} />`)}</ul>
          <nav class="pager" aria-label="Pages"><${Btn} size="sm" icon="chevron-left" disabled=${page === 0} onClick=${() => setCursors((c) => c.slice(0, -1))}>Previous<//><span>Page ${page + 1}</span><${Btn} size="sm" disabled=${!next} onClick=${() => setCursors((c) => [...c, next])}>Next<//></nav>`}
      </div></div></div>`;
}
