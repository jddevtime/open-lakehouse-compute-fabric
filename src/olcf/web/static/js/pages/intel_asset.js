// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// One asset in depth: overview, columns, lineage, impact, origin, quality, documentation, history, sample rows and governance. Also the stand-alone impact analysis page.
import { html, useState, useEffect, useMemo, useAsync } from '../core/h.js';
import { api, can } from '../api/client.js';
import { Btn, Badge, Banner, Card, CopyBtn, Empty, ErrorBox, Loading, PageHead, Tabs, TabPanel, Select, Input, Textarea, Check, Kv, Code } from '../ui/basic.js';
import { Icon } from '../ui/icons.js';
import { Table } from '../ui/data.js';
import { toast } from '../ui/overlay.js';
import { Markdown } from '../notebook/markdown.js';
import { fmtNum, fmtBytes, ago, fmtDate, downloadText, enc, plural } from '../core/util.js';
import { itemHref } from '../core/links.js';
import { useQueryState } from './common.js';
import { typeMeta, typeLabel, typeNoun, typePlural, assetHref, lineageHref, impactHref, discoveryHref, catalogHref, gapMeta, countsPhrase, pathTail, isProcess } from '../intel/model.js';
import { AssetLink, ConfBadge, TypeChip, StatusBadge2, Ago, useGovernance, govBits, CertBadge, QualityScore, qualityName } from './intel_common.js';

const TABLE_LIKE = new Set(['table', 'view', 'model_table']);
const DATASETS = new Set(['table', 'view', 'model_table', 'semantic_model', 'file']);
const CONF_OPTIONS = [{ value: '', label: 'Show everything' }, { value: 'inferred', label: 'Inferred or better' }, { value: 'derived', label: 'Derived or better' }, { value: 'verified', label: 'Verified only' }];

/** Load a page module on first use so the heavy graph code is only fetched when someone opens a lineage tab. */
function useModule(load) {
  const st = useAsync(load, []);
  return st;
}

// ---------------------------------------------------------------------------------------------------------------- overview
function Facts({ a }) {
  const at = a.attrs || {};
  const rows = [
    ['Type', html`<${TypeChip} type=${a.type} />`],
    ['Where', a.path ? html`<span class="wrap-any">${a.path}</span>` : null],
    a.workspace_id ? ['Workspace', html`<a href=${'#/workspaces/' + enc(a.workspace_id)}>Open the workspace</a>`] : null,
    ['Status', html`<${StatusBadge2} status=${a.status} />`],
    at.format ? ['Format', at.format] : null,
    at.row_count != null ? ['Rows', fmtNum(at.row_count)] : null,
    at.size_bytes != null ? ['Size', fmtBytes(at.size_bytes)] : null,
    at.num_files != null ? ['Files', fmtNum(at.num_files)] : null,
    (at.partition_columns || []).length ? ['Split by', at.partition_columns.join(', ')] : null,
    at.last_modified ? ['Data last changed', html`<${Ago} ts=${at.last_modified} />`] : null,
    at.history && at.history.last_operation ? ['Last operation', `${at.history.last_operation}${at.history.last_commit_at ? ' ' + ago(at.history.last_commit_at) : ''}`] : null,
    at.delta_version != null ? ['Table version', String(at.delta_version)] : null,
    at.tile_count != null ? ['Tiles', fmtNum(at.tile_count)] : null,
    ['First seen', html`<${Ago} ts=${a.created_at} />`],
    ['Last seen in a scan', html`<${Ago} ts=${a.updated_at} />`],
  ];
  return html`<${Kv} items=${rows} />`;
}

function Children({ id, label }) {
  const st = useAsync(() => api.get(`/intelligence/assets/${enc(id)}/children`, { limit: 100 }), [id]);
  if (st.error && !st.data) return html`<${ErrorBox} error=${st.error} compact />`;
  if (!st.data) return html`<${Loading} />`;
  if (!st.data.items.length) return html`<div class="muted">Nothing is inside this ${label}.</div>`;
  return html`<ul class="change-list">${st.data.items.map((c) => html`<li key=${c.id}><${AssetLink} asset=${c} /><span class="small muted">${typeLabel(c.type)}</span></li>`)}</ul>${st.data.next_cursor ? html`<div class="small muted mt-2">Showing the first 100.</div>` : null}`;
}

function OverviewTab({ a, setTab }) {
  const at = a.attrs || {};
  const e = a.lineage_edges || { upstream: 0, downstream: 0 };
  const sens = at.sensitive_columns || [];
  const container = a.child_count > 0 && !TABLE_LIKE.has(a.type);
  const stale = useAsync(() => (DATASETS.has(a.type) && a.type !== 'file' ? api.get(`/intelligence/assets/${enc(a.id)}/staleness`, { depth: 3 }) : Promise.resolve(null)), [a.id]);
  return html`<div class="grid c2 top">
    <${Card} title="About this ${typeNoun(a.type)}"><${Facts} a=${a} />
      ${(a.aliases || []).length ? html`<details class="mt-3"><summary class="small">Other names this is known by (${a.aliases.length})</summary><ul class="small">${a.aliases.map((x, i) => html`<li key=${i}><span class="muted">${x.kind}</span> <span class="mono wrap-any">${x.value}</span></li>`)}</ul></details>` : null}</${Card}>
    <div class="col gap-4">
      <${Card} title="Connections">
        <div class="row-wrap"><a class="btn" href=${lineageHref(a.id)}><${Icon} name="route" size="sm" />Open the lineage</a><a class="btn" href=${impactHref(a.id)}><${Icon} name="activity" size="sm" />What depends on it</a></div>
        <p class="mt-3 m-0"><b>${fmtNum(e.upstream)}</b> connection${e.upstream === 1 ? '' : 's'} feed${e.upstream === 1 ? 's' : ''} it and <b>${fmtNum(e.downstream)}</b> use${e.downstream === 1 ? 's' : ''} it.${!e.upstream && !e.downstream ? ' None were found. That can be true, or the scans could not read how it is used.' : ''}</p>
        ${a.open_gaps ? html`<div class="mt-3"><${Banner} kind="warn" title=${`${a.open_gaps} thing${a.open_gaps === 1 ? '' : 's'} the scans could not read about this`} actions=${html`<${Btn} size="sm" href=${discoveryHref('gaps', { asset: a.id })}>See what<//>`}>The connections shown for it may be incomplete.<//></div>` : null}</${Card}>
      ${stale.data ? html`<${Card} title="Freshness"><${Banner} kind=${stale.data.stale ? 'warn' : 'ok'} title=${stale.data.stale ? 'Looks out of date' : 'Looks fresh'}>${stale.data.explanation}<//><div class="mt-2"><a href=${assetHref(a.id, 'impact')} onClick=${(ev) => { ev.preventDefault(); setTab('impact'); }}>See the cause analysis</a></div></${Card}>` : null}
      ${sens.length ? html`<${Card} title="May hold sensitive data"><p class="m-0">${sens.length} column${sens.length === 1 ? ' has a name' : 's have names'} that suggest${sens.length === 1 ? 's' : ''} sensitive data: <b>${sens.slice(0, 8).join(', ')}</b>${sens.length > 8 ? ' and more' : ''}. These are suggestions from names and types only. No data was read, and nothing is labelled until a person confirms it.</p></${Card}>` : null}
      ${container ? html`<${Card} title="What is inside" sub=${`${fmtNum(a.child_count)} item${a.child_count === 1 ? '' : 's'}`}><${Children} id=${a.id} label=${typeNoun(a.type)} /></${Card}>` : null}
    </div></div>`;
}

// ---------------------------------------------------------------------------------------------------------------- columns
function ColumnsTab({ a, gov }) {
  const cols = (a.attrs && a.attrs.columns) || [];
  const profile = (a.attrs && a.attrs.profile && a.attrs.profile.columns) || {};
  const kids = useAsync(() => api.get(`/intelligence/assets/${enc(a.id)}/children`, { type: 'column', limit: 500 }).then((r) => r.items || []).catch(() => []), [a.id]);
  const byName = useMemo(() => new Map((kids.data || []).map((k) => [k.name, k])), [kids.data]);
  const [q, setQ] = useState('');
  const [onlyFlagged, setOnlyFlagged] = useState(false);
  const rows = cols.filter((c) => (!q || c.name.toLowerCase().includes(q.toLowerCase()) || (c.description || '').toLowerCase().includes(q.toLowerCase())) && (!onlyFlagged || (c.suggested_classifications || []).length));
  const flagged = cols.filter((c) => (c.suggested_classifications || []).length).length;
  const columns = [
    { key: 'ordinal', label: '#', num: true, width: '48px', render: (c) => (c.ordinal ?? 0) + 1 },
    { key: 'name', label: 'Column', render: (c) => { const k = byName.get(c.name); return html`<span class="row-wrap">${k ? html`<a class="cell-main" href=${assetHref(k.id)}>${c.name}</a>` : html`<span class="cell-main">${c.name}</span>`}${c.partition ? html`<${Badge} kind="info" title="The table is split into folders by this column">split<//>` : null}</span>`; } },
    { key: 'type', label: 'Type', mono: true },
    { key: 'nullable', label: 'Empty allowed', sort: (c) => (c.nullable ? 1 : 0), render: (c) => (c.nullable ? 'Yes' : 'No') },
    { key: 'description', label: 'Description', sortable: false, render: (c) => c.description || html`<span class="muted">No description</span>` },
    { key: 'class', label: 'Labels', sortable: false, render: (c) => {
      const k = byName.get(c.name); const g = k ? govBits(k) : { classifications: [], terms: [] };
      const sug = (c.suggested_classifications || []).filter((s) => !g.classifications.includes(s.classification));
      return html`<div class="chips">${g.classifications.map((n) => html`<span class="tag cls" key=${n}>${n}</span>`)}${sug.map((s) => html`<span class="tag cls suggested" key=${s.classification} title=${`Suggested from the ${s.evidence}, ${Math.round(s.confidence * 100)}% sure. Needs a person to review it. No data was read.`}>${s.classification.replace(/_/g, ' ')}?</span>`)}${g.terms.map((n) => html`<span class="tag" key=${n} title="Glossary term">${n}</span>`)}</div>`; } },
    { key: 'nulls', label: 'Empty values', sortable: false, num: true, render: (c) => { const p = profile[c.name]; return p && p.null_count != null ? fmtNum(p.null_count) : html`<span class="muted">–</span>`; } },
    { key: 'range', label: 'Smallest to largest', sortable: false, render: (c) => { const p = profile[c.name]; return p && p.min != null ? html`<span class="small mono">${String(p.min)} to ${String(p.max)}</span>` : html`<span class="muted">–</span>`; } },
  ];
  return html`<div class="col gap-3">
    ${flagged ? html`<${Banner} kind="info" title=${`${flagged} column${flagged === 1 ? '' : 's'} may hold sensitive data`}>Labels with a question mark are suggestions made from column names and types. They stay suggestions until a person reviews them${gov.available ? '' : '. Reviewing needs the governance module'}.<//>` : null}
    <div class="toolbar"><div class="grow"><${Input} label="Find a column" value=${q} onInput=${setQ} placeholder="Name or description" /></div>${flagged ? html`<${Check} label="Only columns with a suggested label" checked=${onlyFlagged} onChange=${setOnlyFlagged} />` : null}</div>
    <${Card} flush=${true} title=${`${fmtNum(cols.length)} columns`}><${Table} caption="Columns" rows=${rows} columns=${columns} rowKey=${(c) => c.name} dense=${true} empty=${html`<${Empty} icon="list" title="No column matches" />`} /></${Card}>
    ${a.attrs && a.attrs.profile && a.attrs.profile.complete === false ? html`<div class="small muted">Smallest, largest and empty counts come from the table's own log and are not available for every column.</div>` : null}</div>`;
}

// ---------------------------------------------------------------------------------------------------------------- lineage (lazy)
function LineageTab({ a }) {
  const mod = useModule(() => import('./intel_lineage.js'));
  if (mod.error && !mod.data) return html`<${ErrorBox} error=${mod.error} onRetry=${mod.reloadLoud} />`;
  if (!mod.data) return html`<${Loading} label="Loading the lineage explorer" />`;
  const { LineageExplorer } = mod.data;
  return html`<${LineageExplorer} assetId=${a.id} root=${a} embedded=${true} height="520px" /><div class="mt-3"><a class="btn" href=${lineageHref(a.id)}><${Icon} name="expand" size="sm" />Open full page</a></div>`;
}

// ---------------------------------------------------------------------------------------------------------------- impact and staleness
function ImpactItem({ x }) {
  return html`<tr><td><${AssetLink} asset=${x} /><div class="small muted wrap-any">${pathTail(x.path, 3)}</div></td>
    <td class="small">${x.level === 0 ? 'Directly' : `${x.level} step${x.level === 1 ? '' : 's'} further on`}</td>
    <td>${(x.owners || []).length ? x.owners.map((o) => o.name || o).join(', ') : html`<span class="muted">Not recorded</span>`}</td>
    <td><${ConfBadge} level=${x.confidence_floor} /></td></tr>`;
}

export function StalenessPanel({ assetId }) {
  const st = useAsync(() => api.get(`/intelligence/assets/${enc(assetId)}/staleness`, { depth: 5 }), [assetId]);
  if (st.error && !st.data) return html`<${ErrorBox} error=${st.error} onRetry=${st.reloadLoud} />`;
  if (!st.data) return html`<${Loading} />`;
  const d = st.data;
  const nodes = (d.nodes || []).filter((n) => n.last_write_at || n.stale || n.last_run);
  return html`<div class="col gap-3" data-testid="staleness">
    <${Banner} kind=${d.stale ? 'warn' : 'ok'} title=${d.stale ? 'This looks out of date' : 'This looks fresh'}>${d.explanation}<span class="small muted block mt-2">Counts as stale after ${d.threshold_hours} hours without a write.</span><//>
    ${(d.root_causes || []).length ? html`<div><h3 class="mb-2" style="font-size:var(--fs-md)">Most likely cause</h3><ul class="stale-chain">${d.root_causes.map((r) => { const n = (d.nodes || []).find((x) => x.asset.id === (r.id || (r.asset && r.asset.id))) || r; const asset = n.asset || r; return html`<li class="root" key=${asset.id}><${Icon} name="alert-triangle" /><div><${AssetLink} asset=${asset} /><div class="small">${(n.reasons || []).join(' ') || 'Stale, and nothing upstream of it is stale.'}</div></div></li>`; })}</ul></div>` : null}
    ${nodes.length ? html`<div class="table-wrap"><table class="table dense"><caption class="sr-only">Freshness of what feeds this</caption><thead><tr><th scope="col">Upstream asset</th><th scope="col">Last write</th><th scope="col">Last run</th><th scope="col">State</th></tr></thead><tbody>
      ${nodes.map((n) => html`<tr key=${n.asset.id}><td><${AssetLink} asset=${n.asset} /></td><td>${n.last_write_at ? html`<${Ago} ts=${n.last_write_at} />` : html`<span class="muted">Not known</span>`}${n.last_write_source ? html`<div class="small muted">${n.last_write_source}</div>` : null}</td><td>${n.last_run ? (n.last_run.status || JSON.stringify(n.last_run)) : html`<span class="muted">Not recorded</span>`}</td><td>${n.root_cause ? html`<${Badge} kind="bad">Probable cause<//>` : n.stale ? html`<${Badge} kind="warn">Stale<//>` : html`<${Badge} kind="ok">Fresh<//>`}</td></tr>`)}</tbody></table></div>` : null}
    ${(d.gaps || []).length ? html`<div class="small muted">${plural(d.gaps.length, 'thing')} could not be read upstream, so this analysis may miss a cause.</div>` : null}</div>`;
}

export function ImpactView({ assetId }) {
  const [depth, setDepth] = useState(5);
  const [conf, setConf] = useState('');
  const st = useAsync(() => api.get(`/intelligence/assets/${enc(assetId)}/impact`, { depth, min_confidence: conf || undefined }), [assetId, depth, conf]);
  const d = st.data;
  const groups = d ? Object.entries(d.by_type || {}).sort((x, y) => y[1].count - x[1].count) : [];
  const direct = d ? (d.nodes || []).filter((n) => n.direction === 'downstream' && n.level === 0) : [];
  const further = d ? (d.nodes || []).filter((n) => n.direction === 'downstream' && n.level > 0 && !isProcess(n.type)) : [];
  const countBy = (list) => { const m = {}; for (const n of list) m[n.type] = (m[n.type] || 0) + 1; return countsPhrase(m); };
  return html`<div class="col gap-4" data-testid="impact">
    <div class="filter-row"><div style="min-width:200px"><${Select} label="How far to look" value=${String(depth)} onChange=${(v) => setDepth(+v)} options=${[1, 2, 3, 5, 8, 10].map((n) => ({ value: String(n), label: `${n} step${n === 1 ? '' : 's'}` }))} /></div>
      <div style="min-width:200px"><${Select} label="How sure" value=${conf} onChange=${setConf} options=${CONF_OPTIONS} /></div></div>
    ${st.error && !d ? html`<${ErrorBox} error=${st.error} onRetry=${st.reloadLoud} />` : !d ? html`<${Loading} label="Working out what depends on this" />` : html`
      <${Card} title="If you change or drop this">
        ${d.total ? html`<p class="m-0" data-testid="impact-summary"><b>${fmtNum(d.total)}</b> thing${d.total === 1 ? '' : 's'} depend${d.total === 1 ? 's' : ''} on it within ${depth} step${depth === 1 ? '' : 's'}.
          ${direct.length ? html` <b>${direct.length}</b> read${direct.length === 1 ? 's' : ''} it directly (${countBy(direct)}), so they would be the first to break.` : ''}
          ${further.length ? html` Further on: ${countBy(further)}.` : ''}
          ${(d.reports || []).length ? html` <b>${d.reports.length}</b> report part${d.reports.length === 1 ? '' : 's'} would show different or missing numbers.` : ''}</p>`
          : html`<p class="m-0">Nothing was found that depends on this. That is only a statement about what scans could read: something outside the scanned sources may still use it.</p>`}
        ${!d.owners_known ? html`<p class="small muted mt-3 m-0">Owners are not shown because none are recorded yet. Governance can assign them.</p>` : null}
        ${d.truncated ? html`<div class="mt-3"><${Banner} kind="warn" title="The count stopped at the analysis limit">There are more than ${fmtNum(d.total)} dependants, so the numbers are a lower bound. Choose fewer steps or a higher confidence to see a complete answer.<//></div>` : null}
        ${d.items_truncated && !d.truncated ? html`<div class="mt-3"><${Banner} kind="info" title="Only the nearest are listed">The counts are complete; the lists show the ${fmtNum((d.stats && d.stats.listed) || 0)} nearest of ${fmtNum(d.total)}. Choose fewer steps to list the rest.<//></div>` : null}
        ${(d.gaps || []).length ? html`<details class="mt-3 blind"><summary><b>${plural(d.gaps.length, 'thing')}</b> could not be read, so consumers may be missing</summary>${d.gaps.map((g) => html`<div class="gap-item" key=${g.id}><div><b>${gapMeta(g.kind).label}</b> · ${g.subject}</div><div class="small">${g.detail}</div><div class="rem">${g.remediation}</div></div>`)}</details>` : null}</${Card}>
      ${groups.map(([type, g]) => html`<${Card} key=${type} title=${`${fmtNum(g.count)} ${typePlural(type, g.count)}`}><div class="table-wrap"><table class="table dense"><caption class="sr-only">${typePlural(type, 2)} that depend on this</caption><thead><tr><th scope="col">Name</th><th scope="col">How it depends</th><th scope="col">Owners</th><th scope="col">How sure</th></tr></thead><tbody>${g.items.map((x) => html`<${ImpactItem} key=${x.id} x=${x} />`)}</tbody></table></div>${g.count > g.items.length ? html`<div class="small muted mt-2">Showing ${g.items.length} of ${g.count}.</div>` : null}</${Card}>`)}
      ${(d.processes || []).length ? html`<${Card} title=${`Code that reads or writes it (${d.processes.length})`}><ul class="change-list">${d.processes.map((p) => html`<li key=${p.id}><${AssetLink} asset=${p} /><span class="small muted">${typeLabel(p.type)}</span></li>`)}</ul></${Card}>` : null}`}
    <${Card} title="Is it fresh? What could be holding it up?"><${StalenessPanel} assetId=${assetId} /></${Card}>
  </div>`;
}

export function ImpactPage({ params }) {
  const id = params.id;
  const a = useAsync(() => api.get(`/intelligence/assets/${enc(id)}`), [id]);
  return html`<div class="page"><${PageHead} icon="activity" title=${a.data ? `What depends on ${a.data.name}` : 'Impact analysis'} sub="Everything downstream that would be affected if this changes, with owners where they are known, and what could be holding it up."
      actions=${a.data ? html`<${Btn} icon="external" href=${assetHref(id)}>Open asset<//><${Btn} icon="route" href=${lineageHref(id)}>Lineage<//>` : null} />
    ${a.error && !a.data ? html`<${ErrorBox} error=${a.error} onRetry=${a.reloadLoud} />` : !a.data ? html`<${Loading} />` : html`<${ImpactView} assetId=${id} />`}</div>`;
}

// ---------------------------------------------------------------------------------------------------------------- origin
function OriginTab({ a }) {
  const [depth, setDepth] = useState(10);
  const [conf, setConf] = useState('');
  const st = useAsync(() => api.get(`/intelligence/assets/${enc(a.id)}/origin`, { depth, min_confidence: conf || undefined }), [a.id, depth, conf]);
  const d = st.data;
  const nodeById = useMemo(() => new Map(((d && d.nodes) || []).map((n) => [n.id, n])), [d]);
  // routes that reach a real origin come first, the longest (the fullest story) ahead of the shorter ones, then in a stable order by where they start
  const isSource = new Map(((d && d.roots) || []).map((r) => [r.asset.id, !!r.is_source]));
  const sorted = d ? [...d.paths].sort((x, y) => (isSource.get(y.source) ? 1 : 0) - (isSource.get(x.source) ? 1 : 0) || y.length - x.length || String(nodeById.get(x.source) ? nodeById.get(x.source).name : x.source).localeCompare(String(nodeById.get(y.source) ? nodeById.get(y.source).name : y.source))) : [];
  // paths that read the same (same names, kinds and confidence, through different assets that happen to share a name) are shown once, with how many there are
  const paths = useMemo(() => {
    const seen = new Map();
    for (const p of sorted) {
      const key = p.nodes.map((id) => { const n = nodeById.get(id); return n ? `${n.type}:${n.name}` : id; }).join('>') + '|' + p.confidence;
      if (seen.has(key)) seen.get(key).same += 1; else seen.set(key, { ...p, same: 1 });
    }
    return [...seen.values()];
  }, [d, nodeById]);
  return html`<div class="col gap-4" data-testid="origin">
    <div class="filter-row"><div style="min-width:200px"><${Select} label="How far back" value=${String(depth)} onChange=${(v) => setDepth(+v)} options=${[1, 2, 3, 5, 8, 10].map((n) => ({ value: String(n), label: `${n} step${n === 1 ? '' : 's'}` }))} /></div>
      <div style="min-width:200px"><${Select} label="How sure" value=${conf} onChange=${setConf} options=${CONF_OPTIONS} /></div></div>
    ${st.error && !d ? html`<${ErrorBox} error=${st.error} onRetry=${st.reloadLoud} />` : !d ? html`<${Loading} label="Tracing the origin" />` : !d.has_upstream ? html`<${Empty} icon="route" title="Nothing upstream was found">Either this is where the data starts, or the scans could not see what produces it. Check the gaps below.${(d.gaps || []).length ? '' : ''}</${Empty}>` : html`
      <${Card} title="Where it starts" sub=${`${d.roots.length} origin${d.roots.length === 1 ? '' : 's'}`}>
        <ul class="change-list">${d.roots.map((r) => html`<li key=${r.asset.id}><${AssetLink} asset=${r.asset} showType=${true} /><span class="small muted">${r.path_count} path${r.path_count === 1 ? '' : 's'}</span><${ConfBadge} level=${r.best_confidence} />${r.is_source ? null : html`<${Badge} kind="warn" title="The trace stopped here because of the step limit or a filter">may go further<//>`}</li>`)}</ul></${Card}>
      <${Card} title=${`How it gets here (${plural(paths.length, 'route')})`} sub=${d.paths.length !== paths.length ? `${d.paths.length} paths, some that read the same are shown once` : undefined}>
        ${paths.map((p, i) => html`<div class="exposure-path" key=${i} data-testid="origin-path">${p.nodes.map((id, j) => html`${j ? html`<span class="arrow" aria-hidden="true"><${Icon} name="arrow-right" size="sm" /></span>` : null}${nodeById.get(id) ? html`<${AssetLink} asset=${nodeById.get(id)} />` : html`<span class="muted">hidden asset</span>`}`)}<span class="grow"></span>${p.same > 1 ? html`<span class="small muted" title="Different assets with the same names make the same journey">x${p.same}</span>` : null}<${ConfBadge} level=${p.confidence} /></div>`)}
        <p class="small muted mt-3 m-0">A path is only as sure as its weakest connection.</p></${Card}>
      ${d.level_switched ? html`<${Banner} kind="info" title="Traced at column level">${d.level_switched.reason}.<//>` : null}
      ${d.truncated ? html`<${Banner} kind="warn" title="Only part of the origin is shown">The trace was cut at the size limit.<//>` : null}`}
    ${d && (d.gaps || []).length ? html`<details class="blind"><summary><b>${plural(d.gaps.length, 'thing')}</b> could not be read upstream</summary>${d.gaps.map((g) => html`<div class="gap-item" key=${g.id}><div><b>${gapMeta(g.kind).label}</b> · ${g.subject}</div><div class="small">${g.detail}</div><div class="rem">${g.remediation}</div></div>`)}</details>` : null}
  </div>`;
}

// ---------------------------------------------------------------------------------------------------------------- quality
function QualityTab({ a, gov }) {
  const st = useAsync(() => (gov.available ? api.get(`/governance/assets/${enc(a.id)}/quality`) : Promise.resolve(null)), [a.id, gov.available]);
  const at = a.attrs || {};
  const cols = at.columns || [];
  const described = cols.filter((c) => c.description).length;
  const checks = [
    { name: 'Has a description', ok: !!(a.description && a.description.length >= 10), detail: a.description ? 'Described.' : 'No description yet.' },
    cols.length ? { name: 'Columns are described', ok: described === cols.length, detail: `${described} of ${cols.length} columns have a description.`, partial: described > 0 } : null,
    { name: 'Has connections recorded', ok: !!(a.lineage_edges && (a.lineage_edges.upstream || a.lineage_edges.downstream)), detail: 'Something feeds it or uses it.' },
    { name: 'No unreadable parts', ok: !a.open_gaps, detail: a.open_gaps ? `${plural(a.open_gaps, 'open gap')}.` : 'No open gaps.' },
    at.sensitive_columns ? { name: 'No unreviewed sensitive-looking columns', ok: !at.sensitive_columns.length, detail: `${plural(at.sensitive_columns.length, 'column')} may be sensitive.` } : null,
  ].filter(Boolean);
  return html`<div class="col gap-4">
    ${gov.available && st.data ? html`<${Card} title="Quality score" actions=${html`<${QualityScore} score=${st.data.score} />`}>
      <div class="table-wrap"><table class="table dense"><caption class="sr-only">Parts of the quality score</caption><thead><tr><th scope="col">Part</th><th scope="col" class="num">Points</th><th scope="col">What cost points</th></tr></thead><tbody>
        ${(st.data.components || []).map((c) => html`<tr key=${c.name}><td>${qualityName(c.name)}${c.applicable ? '' : html` <span class="muted small">(does not apply)</span>`}</td><td class="num">${c.applicable ? `${(+c.points).toFixed(1)} of ${(+c.max_points).toFixed(1)}` : '–'}</td><td class="small">${(c.reasons || []).map((r) => r.reason).join(' ') || html`<span class="muted">Nothing</span>`}</td></tr>`)}</tbody></table></div>
      <p class="small muted mt-3 m-0">Computed ${ago(st.data.computed_at)}. The score is a guide to where to look, not a verdict.</p></${Card}>` : null}
    <${Card} title=${gov.available ? 'Simple checks' : 'Simple checks (no quality score without the governance module)'}>
      <ul class="stale-chain">${checks.map((c) => html`<li key=${c.name} class=${c.ok ? '' : 'stale'}><${Icon} name=${c.ok ? 'check-circle' : 'alert-triangle'} /><div><b>${c.name}</b><div class="small">${c.detail}</div></div></li>`)}</ul></${Card}>
    ${st.error && gov.available ? html`<${ErrorBox} error=${st.error} compact />` : null}</div>`;
}

// ---------------------------------------------------------------------------------------------------------------- documentation
function DocsTab({ a }) {
  const st = useAsync(() => api.get(`/intelligence/assets/${enc(a.id)}/documentation`), [a.id]);
  if (st.error && !st.data) return html`<${ErrorBox} error=${st.error} onRetry=${st.reloadLoud} />`;
  if (!st.data) return html`<${Loading} label="Writing the documentation" />`;
  const md = st.data.markdown;
  const file = `${a.name.replace(/[^A-Za-z0-9_-]+/g, '_')}.md`;
  return html`<div class="col gap-3" data-testid="docs">
    <div class="row-wrap"><${CopyBtn} text=${md} label="Copy as Markdown" size="" /><${Btn} icon="download" onClick=${() => downloadText(md, file, 'text/markdown')}>Download .md<//><span class="small muted">Written from what scans found ${ago(st.data.generated_at)}. It is regenerated each time you open this.</span></div>
    <${Card}><div class="doc-md"><${Markdown} source=${md} /></div></${Card}></div>`;
}

// ---------------------------------------------------------------------------------------------------------------- history
function describeChange(c) {
  const out = [];
  if (c.created) out.push('First recorded');
  if (c.removed) out.push('Marked as removed');
  if (c.restored) out.push('Found again after being removed');
  if ((c.columns_added || []).length) out.push(`Columns added: ${c.columns_added.join(', ')}`);
  if ((c.columns_removed || []).length) out.push(`Columns removed: ${c.columns_removed.join(', ')}`);
  if ((c.columns_retyped || []).length) out.push(`Columns changed type: ${c.columns_retyped.map((x) => (typeof x === 'string' ? x : `${x.name} (${x.from} to ${x.to})`)).join(', ')}`);
  if ((c.attrs_changed || []).length) out.push(`Details changed: ${c.attrs_changed.map((k) => k.replace(/_/g, ' ')).join(', ')}`);
  return out.length ? out : ['Recorded again with no visible change'];
}
function HistoryTab({ a }) {
  const [schemaOnly, setSchemaOnly] = useState(false);
  const [extra, setExtra] = useState([]);
  const [cursor, setCursor] = useState(null);
  const [more, setMore] = useState(false);
  const st = useAsync(() => api.get(`/intelligence/assets/${enc(a.id)}/history`, { limit: 20, schema_only: schemaOnly || undefined }).then((r) => { setCursor(r.next_cursor); setExtra([]); return r; }), [a.id, schemaOnly]);
  const load = async () => { setMore(true); try { const r = await api.get(`/intelligence/assets/${enc(a.id)}/history`, { limit: 20, cursor, schema_only: schemaOnly || undefined }); setExtra((x) => [...x, ...r.items]); setCursor(r.next_cursor); } finally { setMore(false); } };
  const items = st.data ? [...st.data.items, ...extra] : [];
  return html`<div class="col gap-3" data-testid="history">
    <${Check} label="Only show changes to the columns" checked=${schemaOnly} onChange=${setSchemaOnly} />
    ${st.error && !st.data ? html`<${ErrorBox} error=${st.error} onRetry=${st.reloadLoud} />` : !st.data ? html`<${Loading} />` : !items.length ? html`<${Empty} icon="history" title=${schemaOnly ? 'The columns never changed' : 'No history yet'}>${schemaOnly ? 'Every scan saw the same columns.' : 'History is recorded each time a scan sees a change.'}</${Empty}>` : html`
      <div>${items.map((v) => html`<div class="history-item" key=${v.id}><div class="row-wrap"><b><${Ago} ts=${v.created_at} /></b><span class="small muted">${fmtDate(v.created_at)}</span></div>
        <ul class="small m-0">${describeChange(v.changes || {}).map((t, i) => html`<li key=${i}>${t}</li>`)}</ul></div>`)}</div>
      ${cursor ? html`<${Btn} busy=${more} onClick=${load}>Show older changes<//>` : null}`}</div>`;
}

// ---------------------------------------------------------------------------------------------------------------- sample rows
function SampleTab({ a }) {
  const pol = useAsync(() => api.get(`/intelligence/assets/${enc(a.id)}/sampling-policy`), [a.id]);
  const [rowsWanted, setRows] = useState(5);
  const [res, setRes] = useState(null);
  const [err, setErr] = useState(null);
  const [busy, setBusy] = useState(false);
  useEffect(() => { setRes(null); setErr(null); return () => setRes(null); }, [a.id]);     // values live in this component only: they are dropped when the tab or asset changes
  async function fetchRows() {
    setBusy(true); setErr(null);
    try { setRes(await api.get(`/intelligence/assets/${enc(a.id)}/sample`, { rows: Math.min(rowsWanted, (pol.data && pol.data.max_rows) || 5) })); } catch (e) { setErr(e); setRes(null); } finally { setBusy(false); }
  }
  const p = pol.data;
  const manage = can('intelligence.manage');
  if (pol.error && !p) return html`<${ErrorBox} error=${pol.error} onRetry=${pol.reloadLoud} />`;
  if (!p) return html`<${Loading} />`;
  if (!p.allowed) return html`<div class="col gap-3" data-testid="sample-off"><${Banner} kind="info" title="Showing sample rows is switched off" actions=${manage ? html`<${Btn} size="sm" href=${discoveryHref('sampling')}>Sampling policies<//>` : null}>
      OLCF never looks at the values inside your tables unless an administrator allows it. ${p.reason ? p.reason + ' ' : ''}Nothing here is hidden by mistake: this is the default.${manage ? '' : ' Ask an administrator if you need it.'}<//>
    <p class="small muted m-0">When it is allowed, you will see at most a handful of rows with sensitive-looking values masked, fetched on request, never stored and always recorded in the audit log.</p></div>`;
  const max = p.max_rows || 5;
  const rows = Math.min(rowsWanted, max);
  return html`<div class="col gap-3" data-testid="sample">
    <${Banner} kind="warn" title="This reads real data">Up to <b>${max}</b> row${max === 1 ? '' : 's'}, values masked (<b>${p.masking}</b>)${p.exclude_classified ? ', classified columns left out' : ''}. Fetching is recorded in the audit log. The values are shown here only: they are not saved and not kept in your browser.<//>
    <div class="row-wrap"><div style="width:160px"><${Select} label="Rows" value=${String(rows)} onChange=${(v) => setRows(+v)} options=${Array.from({ length: Math.min(max, 100) }, (_, i) => i + 1).filter((n) => n <= 10 || n % 5 === 0 || n === max).map((n) => ({ value: String(n), label: String(n) }))} /></div>
      <${Btn} kind="primary" icon="eye" busy=${busy} onClick=${fetchRows}>${res ? 'Fetch again' : 'Fetch sample rows'}<//>${res ? html`<${Btn} icon="eye-off" onClick=${() => setRes(null)}>Hide the values<//>` : null}</div>
    ${err ? html`<${ErrorBox} error=${err} />` : null}
    ${res ? html`<div class="sample-wrap" data-testid="sample-rows"><table class="table dense"><caption class="sr-only">Masked sample rows</caption><thead><tr>${res.columns.map((c) => html`<th scope="col" key=${c.name}>${c.name}<div class="small muted">${c.type}</div></th>`)}</tr></thead><tbody>${res.rows.map((r, i) => html`<tr key=${i}>${res.columns.map((c) => html`<td class="mono" key=${c.name}>${r[c.name] == null ? html`<span class="muted">empty</span>` : String(r[c.name])}</td>`)}</tr>`)}</tbody></table></div>
      ${(res.excluded_columns || []).length ? html`<div class="small muted">Left out because they are classified: ${res.excluded_columns.join(', ')}.</div>` : null}
      <div class="small muted">${String(res.note || '').replace(/^./, (c) => c.toUpperCase())}</div>` : null}</div>`;
}

// ---------------------------------------------------------------------------------------------------------------- governance (lazy)
function GovernanceTab({ a, gov }) {
  const mod = useModule(() => import('./intel_governance.js'));
  if (mod.error && !mod.data) return html`<${ErrorBox} error=${mod.error} onRetry=${mod.reloadLoud} />`;
  if (!mod.data) return html`<${Loading} />`;
  const { AssetGovernance } = mod.data;
  return html`<${AssetGovernance} asset=${a} />`;
}

// ---------------------------------------------------------------------------------------------------------------- header (editable description)
function Header({ a, etag, onSaved, gov }) {
  const [edit, setEdit] = useState(false);
  const [desc, setDesc] = useState(a.description || '');
  const [tags, setTags] = useState((a.tags || []).join(', '));
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  const canEdit = can('governance.curate', a.workspace_id) && a.status !== 'removed';
  const g = govBits(a);
  useEffect(() => { setDesc(a.description || ''); setTags((a.tags || []).join(', ')); }, [a.id, a.etag]);
  async function save() {
    setBusy(true); setErr(null);
    try {
      const r = await api.full('PATCH', `/intelligence/assets/${enc(a.id)}`, { body: { description: desc, tags: tags.split(',').map((t) => t.trim()).filter(Boolean) }, ifMatch: etag });
      onSaved({ ...a, ...r.data }, r.etag || r.data.etag);
      setEdit(false); toast.ok('Saved');
    } catch (e) { setErr(e); } finally { setBusy(false); }
  }
  const crumbs = (a.path || '').split(' / ').slice(0, -1);
  return html`<header class="page-head asset-head"><div class="ic"><${Icon} name=${typeMeta(a.type).icon} size="lg" /></div>
    <div class="titles">
      ${crumbs.length || a.parent ? html`<div class="crumbs">${a.parent ? html`<a href=${assetHref(a.parent.id)}>${a.parent.name}</a>` : null}${!a.parent ? crumbs.join(' / ') : null}</div>` : null}
      <h1>${a.name}</h1>
      <div class="row-wrap mt-2"><${TypeChip} type=${a.type} /><${StatusBadge2} status=${a.status} /><${CertBadge} state=${g.certification} />${g.classifications.map((c) => html`<span class="tag cls" key=${c}>${c}</span>`)}${g.domain ? html`<span class="tag">${g.domain}</span>` : null}<${QualityScore} score=${g.score} />
        ${g.owners.length ? html`<span class="small muted">Owner: ${g.owners.join(', ')}</span>` : null}${(a.tags || []).map((t) => html`<span class="tag" key=${t}>${t}</span>`)}</div>
      ${a.status === 'removed' ? html`<div class="mt-3"><${Banner} kind="warn" title="A later scan did not find this">It is kept so history and lineage stay readable.<//></div>` : null}
      ${a.status === 'placeholder' ? html`<div class="mt-3"><${Banner} kind="warn" title="Only mentioned, never found">Some code or definition refers to this, but no scan has found the real thing yet.<//></div>` : null}
      <div class="desc-edit mt-3">${edit ? html`<form onSubmit=${(e) => { e.preventDefault(); save(); }}><${Textarea} label="Description" value=${desc} onInput=${setDesc} rows=${4} hint="Plain text or Markdown. Anyone who can see this asset can read it." /><${Input} label="Tags" value=${tags} onInput=${setTags} hint="Separate with commas" />
          ${err ? html`<${ErrorBox} error=${err} />` : null}
          <div class="row-wrap mt-2"><${Btn} kind="primary" type="submit" busy=${busy}>Save<//><${Btn} onClick=${() => { setEdit(false); setErr(null); setDesc(a.description || ''); }}>Cancel<//></div></form>`
        : html`<div>${a.description ? html`<p class="m-0" style="max-width:80ch">${a.description}</p>` : html`<p class="m-0 muted">No description yet.</p>`}${a.description_source && a.description ? html`<div class="small muted">Source: ${a.description_source}</div>` : null}
          ${canEdit ? html`<div class="mt-2"><${Btn} size="sm" icon="edit" onClick=${() => setEdit(true)}>Edit description and tags<//></div>` : null}</div>`}</div>
    </div>
    <div class="row-wrap">${a.attrs && a.attrs.item_id ? html`<${Btn} icon="external" href=${itemHref({ id: a.attrs.item_id, type: a.attrs.item_type, workspace_id: a.workspace_id })}>Open it<//>` : a.attrs && a.attrs.lakehouse_id && TABLE_LIKE.has(a.type) ? html`<${Btn} icon="database" href=${'#/lakehouse/' + enc(a.attrs.lakehouse_id)}>Open the lakehouse<//>` : null}
      <${Btn} icon="route" href=${lineageHref(a.id)}>Lineage<//><${Btn} icon="activity" href=${impactHref(a.id)}>Impact<//></div></header>`;
}

// ---------------------------------------------------------------------------------------------------------------- the page
export function AssetPage({ params }) {
  const id = params.id;
  const [tab, setTab] = useQueryState('tab', 'overview');
  const gov = useGovernance();
  const st = useAsync(async () => { const r = await api.withEtag(`/intelligence/assets/${enc(id)}`); return { asset: r.data, etag: r.etag }; }, [id]);
  const [local, setLocal] = useState(null);
  useEffect(() => { setLocal(null); }, [id, st.data]);
  const a = local ? local.asset : st.data && st.data.asset;
  const etag = local ? local.etag : st.data && st.data.etag;
  if (st.error && !st.data) return html`<div class="page"><${ErrorBox} error=${st.error} onRetry=${st.reloadLoud} /><div class="mt-3"><a class="btn" href=${catalogHref()}>Back to the catalog</a></div></div>`;
  if (!a) return html`<div class="page"><${Loading} label="Loading the asset" /></div>`;
  const cols = (a.attrs && a.attrs.columns) || [];
  const tabs = [
    { id: 'overview', label: 'Overview', icon: 'info' },
    cols.length ? { id: 'columns', label: 'Columns', icon: 'list', count: cols.length } : null,
    { id: 'lineage', label: 'Lineage', icon: 'route' },
    { id: 'impact', label: 'Impact', icon: 'activity' },
    { id: 'origin', label: 'Origin', icon: 'git' },
    { id: 'quality', label: 'Quality', icon: 'gauge' },
    { id: 'docs', label: 'Documentation', icon: 'book' },
    { id: 'history', label: 'History', icon: 'history' },
    TABLE_LIKE.has(a.type) && can('intelligence.sample') ? { id: 'sample', label: 'Sample', icon: 'eye' } : null,
    gov.available ? { id: 'governance', label: 'Governance', icon: 'shield-check' } : null,
  ].filter(Boolean);
  const cur = tabs.some((t) => t.id === tab) ? tab : 'overview';
  return html`<div class="page"><${Header} a=${a} etag=${etag} onSaved=${(asset, e) => setLocal({ asset, etag: e })} gov=${gov} />
    <${Tabs} label="Asset sections" tabs=${tabs} value=${cur} onChange=${setTab} />
    <div class="tab-body">
      <${TabPanel} id="overview" value=${cur}><${OverviewTab} a=${a} setTab=${setTab} /></${TabPanel}>
      <${TabPanel} id="columns" value=${cur}><${ColumnsTab} a=${a} gov=${gov} /></${TabPanel}>
      <${TabPanel} id="lineage" value=${cur}><${LineageTab} a=${a} /></${TabPanel}>
      <${TabPanel} id="impact" value=${cur}><${ImpactView} assetId=${a.id} /></${TabPanel}>
      <${TabPanel} id="origin" value=${cur}><${OriginTab} a=${a} /></${TabPanel}>
      <${TabPanel} id="quality" value=${cur}><${QualityTab} a=${a} gov=${gov} /></${TabPanel}>
      <${TabPanel} id="docs" value=${cur}><${DocsTab} a=${a} /></${TabPanel}>
      <${TabPanel} id="history" value=${cur}><${HistoryTab} a=${a} /></${TabPanel}>
      <${TabPanel} id="sample" value=${cur}><${SampleTab} a=${a} /></${TabPanel}>
      <${TabPanel} id="governance" value=${cur}><${GovernanceTab} a=${a} gov=${gov} /></${TabPanel}>
    </div></div>`;
}
