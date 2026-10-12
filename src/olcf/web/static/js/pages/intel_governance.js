// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Governance: glossary, domains and data products, classifications and their review queue, requests and certification, policies and violations (they report, they never block),
// where labelled data ends up, and who owns what. Also the Governance tab of one asset. Built on /v1/governance/*; see docs/dev/GOVERNANCE_API.md.
import { html, useState, useEffect, useMemo, useAsync, useDebounced } from '../core/h.js';
import { api, can } from '../api/client.js';
import { navigate } from '../core/util.js';
import { Btn, Badge, Banner, Card, Check, Empty, ErrorBox, Input, Loading, PageHead, Select, Switch, Tabs, TabPanel, Textarea, Seg } from '../ui/basic.js';
import { Icon } from '../ui/icons.js';
import { Dialog, Drawer, confirmDialog, openOverlay, toast, act } from '../ui/overlay.js';
import { Table } from '../ui/data.js';
import { StatTile } from '../charts/Chart.js';
import { Markdown } from '../notebook/markdown.js';
import { fmtNum, enc, plural } from '../core/util.js';
import { typeLabel, assetHref, catalogHref, governanceHref, lineageHref, confLabel } from '../intel/model.js';
import { AssetLink, ConfBadge, Ago, CertBadge, CERT_LABEL, QualityScore, qualityName, GOV_SECTIONS, useGovernance, invalidateGovernance } from './intel_common.js';
import { useQueryState } from './common.js';

const GOV = '/governance';
const canCurate = (ws) => can('governance.curate', ws);
const canApprove = (ws) => can('governance.approve', ws);
const names = (list) => (list || []).map((x) => x.name || x.id).join(', ');


function AskText({ title, label, confirmLabel, required, hint, close }) {
  const [v, setV] = useState('');
  return html`<${Dialog} title=${title} onClose=${() => close(null)} footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} kind="primary" disabled=${required && !v.trim()} onClick=${() => close(v.trim())}>${confirmLabel}<//>`}>
    <${Textarea} label=${label} value=${v} onInput=${setV} rows=${3} hint=${hint} data-autofocus=${true} /></${Dialog}>`;
}
/** Ask for a short text (a reason or comment). Resolves to the text, or null when cancelled. Optional comments may be empty. */
const askText = (o) => openOverlay(({ close }) => html`<${AskText} ...${o} close=${close} />`);

/** First page plus "show more" for any governance list route. */
function usePaged(path, query, deps, pageSize = 50) {
  const [more, setMore] = useState([]);
  const [cursor, setCursor] = useState(null);
  const [busy, setBusy] = useState(false);
  const st = useAsync(() => api.get(path, { limit: pageSize, ...query }).then((r) => { setMore([]); setCursor(r.next_cursor || null); return r; }), deps);
  const items = st.data ? [...st.data.items, ...more] : [];
  async function loadMore() {
    setBusy(true);
    try { const r = await api.get(path, { limit: pageSize, ...query, cursor }); setMore((m) => [...m, ...r.items]); setCursor(r.next_cursor || null); } catch (e) { toast.bad('Could not load more', e && e.message); } finally { setBusy(false); }
  }
  return { ...st, items, cursor, loadMore, busy };
}
const MoreBtn = ({ p }) => (p.cursor ? html`<div class="mt-3"><${Btn} busy=${p.busy} onClick=${p.loadMore}>Show more<//></div>` : null);

function Dot({ color }) { return html`<span class="class-dot" style=${`background:${color || 'var(--ink-3)'}`} aria-hidden="true"></span>`; }
function ClassBadge({ c, status, title }) {
  if (!c) return null;
  return html`<span class=${'badge class-badge' + (status === 'suggested' ? ' dashed' : '')} title=${title || `${c.category || ''} (level ${c.level ?? '?'})`}><${Dot} color=${c.color} />${c.name}${status === 'suggested' ? '?' : ''}</span>`;
}
function Title({ children, sub }) { return html`<div class="mb-2"><h3 class="m-0" style="font-size:var(--fs-md)">${children}</h3>${sub ? html`<div class="small muted">${sub}</div>` : null}</div>`; }

/** Search for an asset by name and pick one. */
function AssetPick({ onPick, label = 'Find an asset', types, placeholder = 'Type at least two letters of a name' }) {
  const [q, setQ] = useState('');
  const dq = useDebounced(q, 300);
  const res = useAsync(() => (dq.trim().length >= 2 ? api.get('/intelligence/assets', { q: dq.trim(), type: types, limit: 8 }).then((r) => r.items || []) : Promise.resolve([])), [dq]);
  return html`<div class="field"><label for="asset-pick">${label}</label>
    <input id="asset-pick" class="input" type="search" value=${q} onInput=${(e) => setQ(e.target.value)} placeholder=${placeholder} autocomplete="off" />
    ${dq.trim().length >= 2 && res.data ? (res.data.length ? html`<ul class="pick-list">${res.data.map((a) => html`<li key=${a.id}><button type="button" onClick=${() => { onPick(a); setQ(''); }}><${AssetLink} asset=${a} showType=${true} /></button></li>`)}</ul>` : html`<div class="hint">Nothing found.</div>`) : null}</div>`;
}

// ---------------------------------------------------------------------------------------------------------------- owners and certification (shared by assets, terms, domains and products)
const ROLES = [['owner', 'Owner'], ['steward', 'Steward'], ['custodian', 'Custodian'], ['contact', 'Contact']];
const roleLabel = (r) => (ROLES.find((x) => x[0] === r) || [r, r])[1];

export function OwnersEditor({ prefix, id, canEdit, onChange }) {
  const st = useAsync(() => api.get(`${GOV}/${prefix}/${enc(id)}/owners`), [prefix, id]);
  const [role, setRole] = useState('owner');
  const [who, setWho] = useState('');
  const [ext, setExt] = useState(false);
  const [email, setEmail] = useState('');
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  async function add() {
    setBusy(true); setErr(null);
    try { await api.post(`${GOV}/${prefix}/${enc(id)}/owners`, ext ? { role, name: who, email: email || undefined } : { role, user: who }); setWho(''); setEmail(''); toast.ok('Added'); st.reload(); onChange && onChange(); } catch (e) { setErr(e); } finally { setBusy(false); }
  }
  async function remove(o) {
    const done = await act(() => api.del(`${GOV}/${prefix}/${enc(id)}/owners/${o.id}`), { ok: 'Removed' });
    if (done !== undefined) { st.reload(); onChange && onChange(); }
  }
  if (st.error && !st.data) return html`<${ErrorBox} error=${st.error} onRetry=${st.reloadLoud} />`;
  if (!st.data) return html`<${Loading} />`;
  const eff = st.data.effective || [];
  return html`<div data-testid="owners">
    ${eff.length ? html`<ul class="change-list">${eff.map((o, i) => html`<li key=${o.id ?? i}><${Icon} name="user" size="sm" /><b>${o.name}</b><${Badge}>${roleLabel(o.role)}<//>${o.email ? html`<span class="small muted">${o.email}</span>` : null}
      ${o.inherited_from ? html`<span class="small muted">from <a href=${assetHref(o.inherited_from.asset_id)}>${o.inherited_from.name}</a>${o.inherited_from.distance ? ` (${o.inherited_from.distance} level${o.inherited_from.distance === 1 ? '' : 's'} up)` : ''}</span>` : null}
      ${canEdit && !o.inherited_from && o.id != null ? html`<${Btn} size="sm" kind="ghost" iconOnly icon="trash" label=${`Remove ${o.name}`} onClick=${() => remove(o)} />` : null}</li>`)}</ul>`
      : html`<div class="muted small">Nobody is recorded yet.</div>`}
    ${canEdit ? html`<div class="filter-row mt-3">
      <div style="width:150px"><${Select} label="Role" value=${role} onChange=${setRole} options=${ROLES.map(([value, label]) => ({ value, label }))} /></div>
      <div style="min-width:220px"><${Input} label=${ext ? 'Name of the person' : 'OLCF user (name or e-mail)'} value=${who} onInput=${setWho} /></div>
      ${ext ? html`<div style="min-width:200px"><${Input} label="E-mail (optional)" value=${email} onInput=${setEmail} /></div>` : null}
      <${Btn} busy=${busy} disabled=${!who.trim()} onClick=${add}>Add<//>
      <${Check} label="Someone outside OLCF" checked=${ext} onChange=${setExt} /></div>
      ${err ? html`<div class="mt-2"><${ErrorBox} error=${err} /></div>` : null}` : null}</div>`;
}

export function CertPanel({ prefix, subjectType, id, workspaceId, onChange }) {
  const st = useAsync(() => api.withEtag(`${GOV}/${prefix}/${enc(id)}/certification`).then((r) => ({ ...r.data, etag: r.etag || r.data.etag })), [prefix, id]);
  const org = prefix !== 'assets';
  const mayCurate = canCurate(org ? undefined : workspaceId);
  const mayApprove = canApprove(org ? undefined : workspaceId);
  const [busy, setBusy] = useState(false);
  async function set(state) {
    let comment;
    if (state !== 'none') { comment = await askText({ title: `Mark as ${CERT_LABEL[state].toLowerCase()}`, label: 'Why (optional)', confirmLabel: 'Save' }); if (comment === null) return; }
    setBusy(true);
    try { await api.put(`${GOV}/${prefix}/${enc(id)}/certification`, { state, comment: comment || undefined }, { ifMatch: st.data && st.data.etag }); toast.ok('Saved'); st.reload(); onChange && onChange(); } catch (e) { toast.bad('Could not change the certification', e && e.message); } finally { setBusy(false); }
  }
  async function request(type) {
    const reason = await askText({ title: type === 'certify' ? 'Ask for certification' : 'Ask to deprecate', label: 'Why (optional)', confirmLabel: 'Send the request' });
    if (reason === null) return;
    await act(() => api.post(`${GOV}/changes`, { type, subject_type: subjectType, subject_id: id, payload: type === 'certify' ? { state: 'certified' } : {}, comment: reason || undefined }), { ok: 'Request sent for approval' });
    onChange && onChange();
  }
  if (st.error && !st.data) return html`<${ErrorBox} error=${st.error} onRetry=${st.reloadLoud} />`;
  if (!st.data) return html`<${Loading} />`;
  const c = st.data;
  return html`<div data-testid="certification" class="col gap-2">
    <div class="row-wrap"><${CertBadge} state=${c.state} />${!c.state || c.state === 'none' ? html`<span class="muted">Not certified</span>` : null}
      ${c.certified_at ? html`<span class="small muted">by ${c.certified_by && !/^usr_/.test(c.certified_by) ? c.certified_by : 'a person with approval rights'} <${Ago} ts=${c.certified_at} /></span>` : null}</div>
    ${c.comment ? html`<div class="small">${c.comment}</div>` : null}
    ${c.invalidated_at ? html`<${Banner} kind="warn" title="The certification was withdrawn automatically">${c.invalidated_reason === 'schema_changed' ? 'The structure changed after it was certified, so it needs to be looked at again.' : c.invalidated_reason || 'It changed after it was certified.'}<//>` : null}
    ${c.schema_changed_since && !c.invalidated_at ? html`<div class="small muted">The structure changed since it was certified.</div>` : null}
    <div class="row-wrap">
      ${mayCurate && c.state !== 'draft' && c.state !== 'certified' ? html`<${Btn} size="sm" busy=${busy} onClick=${() => set('draft')}>Mark as draft<//>` : null}
      ${mayApprove ? html`${c.state !== 'certified' ? html`<${Btn} size="sm" kind="primary" icon="shield-check" busy=${busy} onClick=${() => set('certified')}>Certify<//>` : null}${c.state !== 'deprecated' ? html`<${Btn} size="sm" busy=${busy} onClick=${() => set('deprecated')}>Deprecate<//>` : null}`
        : mayCurate ? html`<${Btn} size="sm" onClick=${() => request('certify')}>Ask for certification<//><${Btn} size="sm" kind="ghost" onClick=${() => request('deprecate')}>Ask to deprecate<//>` : null}
      ${mayCurate && c.state && c.state !== 'none' ? html`<${Btn} size="sm" kind="ghost" busy=${busy} onClick=${() => set('none')}>Remove the mark<//>` : null}</div>
    ${!mayCurate ? html`<div class="small muted">Changing this needs the curate or approve permission.</div>` : null}</div>`;
}

// ---------------------------------------------------------------------------------------------------------------- overview
function Coverage({ label, pct: p, hint }) {
  const v = p == null ? null : Math.round(p);
  const cls = v == null ? 'none' : v >= 80 ? '' : v >= 50 ? 'mid' : 'low';
  return html`<div class="cov-row"><div>${label}${hint ? html`<div class="small muted">${hint}</div>` : null}</div><div class=${'cov-bar ' + cls} role="meter" aria-label=${label} aria-valuemin="0" aria-valuemax="100" aria-valuenow=${v ?? undefined}>${v != null ? html`<i style=${`width:${v}%`}></i>` : null}</div><div class="num small">${v == null ? 'n/a' : `${v}%`}</div></div>`;
}
function OverviewSection() {
  const st = useAsync(() => api.get(`${GOV}/summary`), []);
  if (st.error && !st.data) return html`<${ErrorBox} error=${st.error} onRetry=${st.reloadLoud} />`;
  if (!st.data) return html`<${Loading} />`;
  const s = st.data;
  const cv = s.coverage || {};
  const vio = s.violations || {};
  const q = s.review_queue || {};
  const gtiles = 3;                                                                              // 3 + 3 rather than 5 + 1
  return html`<div class="col gap-4" data-testid="gov-overview">
    <div class="intel-tiles" style=${`--cols:${gtiles}`}>
      <${StatTile} label="Glossary terms" value=${fmtNum((s.glossary && s.glossary.terms) || 0)} sub="Agreed business words" href=${governanceHref('glossary')} />
      <${StatTile} label="Domains" value=${fmtNum(s.domains || 0)} sub=${`${fmtNum((s.products && s.products.total) || 0)} data products`} href=${governanceHref('domains')} />
      <${StatTile} label="Waiting for review" value=${fmtNum(q.total || 0)} state=${q.total ? 'degraded' : 'ok'} sub="Suggested labels and links" href=${governanceHref('review')} />
      <${StatTile} label="Open requests" value=${fmtNum(q.change_requests || 0)} sub="Changes waiting for approval" href=${governanceHref('requests')} />
      <${StatTile} label="Policy violations" value=${fmtNum(vio.open || 0)} state=${vio.open ? 'degraded' : 'ok'} sub="Reported only, never blocking" href=${governanceHref('policies')} />
      <${StatTile} label="Labels in use" value=${fmtNum((s.classifications && s.classifications.assignments && s.classifications.assignments.approved) || 0)} sub=${`${fmtNum((s.classifications && s.classifications.labels) || 0)} kinds of label`} href=${governanceHref('classifications')} /></div>
    <div class="grid c2 top">
      <${Card} title="How well is the data looked after?" sub="Share of tables and similar assets">
        <${Coverage} label="Have an owner" pct=${cv.owner_pct} /><${Coverage} label="Have a description" pct=${cv.description_pct} /><${Coverage} label="Belong to a domain" pct=${cv.domain_pct} />
        <${Coverage} label="Linked to a glossary term" pct=${cv.term_pct} /><${Coverage} label="Labels reviewed by a person" pct=${cv.classification_reviewed_pct} /><${Coverage} label="Certified" pct=${cv.certified_pct} />
        <div class="small muted mt-2">Measured over ${fmtNum(cv.datasets || 0)} tables, views, models and reports. ${s.evaluation && s.evaluation.last_evaluated_at ? html`Policies last checked <${Ago} ts=${s.evaluation.last_evaluated_at} />.` : 'Policies have not been checked yet.'}</div></${Card}>
      <div class="col gap-4">
        <${Card} title="No owner yet" sub="Start with these" actions=${html`<a class="small" href=${governanceHref('owners')}>All owners</a>`}>${(s.top_unowned || []).length ? html`<ul class="change-list">${s.top_unowned.map((a) => html`<li key=${a.id}><${AssetLink} asset=${a} showType=${true} /></li>`)}</ul>` : html`<span class="muted">Everything has an owner.</span>`}</${Card}>
        <${Card} title="Nothing labelled yet" sub="Tables that no one has looked at for sensitive data">${(s.top_unclassified || []).length ? html`<ul class="change-list">${s.top_unclassified.map((a) => html`<li key=${a.id}><${AssetLink} asset=${a} showType=${true} /></li>`)}</ul>` : html`<span class="muted">Every table has been reviewed.</span>`}</${Card}></div></div>
    <p class="small muted m-0">Governance here observes and reports. Nothing in a query, a pipeline or a notebook is blocked or changed because of a label, a policy or a certification.</p></div>`;
}

// ---------------------------------------------------------------------------------------------------------------- glossary
const TERM_STATUS = { draft: 'Draft', approved: 'Approved', deprecated: 'Deprecated' };
const statusKind = (s) => (s === 'approved' ? 'ok' : s === 'deprecated' ? 'bad' : 'warn');

function TermDialog({ term, parent, terms, domains, onClose }) {
  const editing = !!term;
  const [name, setName] = useState(term ? term.name : '');
  const [def, setDef] = useState(term ? term.definition || '' : '');
  const [abbr, setAbbr] = useState(term ? term.abbreviation || '' : '');
  const [syn, setSyn] = useState(term ? (term.synonyms || []).join(', ') : '');
  const [status, setStatus] = useState(term ? term.status : 'draft');
  const [parentId, setParentId] = useState(term ? term.parent_id || '' : parent || '');
  const [domainId, setDomainId] = useState(term ? term.domain_id || '' : '');
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  const mayApprove = canApprove();
  async function save() {
    setBusy(true); setErr(null);
    const body = { name: name.trim(), definition: def, abbreviation: abbr.trim() || null, synonyms: syn.split(',').map((x) => x.trim()).filter(Boolean), status, parent_id: parentId || null, domain_id: domainId || null };
    try {
      const r = editing ? await api.patch(`${GOV}/glossary/terms/${enc(term.id)}`, body, { ifMatch: term.etag }) : await api.post(`${GOV}/glossary/terms`, body);
      toast.ok(editing ? 'Term saved' : 'Term added'); onClose(r);
    } catch (e) { setErr(e); } finally { setBusy(false); }
  }
  const choices = terms.filter((t) => !term || t.id !== term.id);
  return html`<${Dialog} title=${editing ? `Edit ${term.name}` : parent ? 'Add a narrower term' : 'Add a glossary term'} size="lg" onClose=${() => onClose(null)}
    footer=${html`<${Btn} onClick=${() => onClose(null)}>Cancel<//><${Btn} kind="primary" busy=${busy} disabled=${!name.trim()} onClick=${save}>Save<//>`}>
    <div class="col gap-1" data-testid="term-dialog">${err ? html`<div class="mb-3"><${ErrorBox} error=${err} /></div>` : null}
      <${Input} label="Name" value=${name} onInput=${setName} required=${true} data-autofocus=${true} maxLength="200" />
      <${Textarea} label="What it means" value=${def} onInput=${setDef} rows=${5} hint="Plain words. Markdown works." />
      <div class="grid c2"><${Input} label="Short form (optional)" value=${abbr} onInput=${setAbbr} /><${Input} label="Other names (comma separated)" value=${syn} onInput=${setSyn} /></div>
      <div class="grid c2"><${Select} label="Sits under" value=${parentId} onChange=${setParentId} placeholder="Top level" options=${choices.map((t) => ({ value: t.id, label: t.name }))} />
        <${Select} label="Domain" value=${domainId} onChange=${setDomainId} placeholder="None" options=${domains.map((d) => ({ value: d.id, label: d.name }))} /></div>
      <${Select} label="Status" value=${status} onChange=${setStatus} options=${Object.entries(TERM_STATUS).filter(([v]) => v !== 'approved' || mayApprove || status === 'approved').map(([value, label]) => ({ value, label }))} hint=${mayApprove ? 'Only approved terms can be certified.' : 'Approving a term needs the approve permission.'} /></div></${Dialog}>`;
}

function TermDetail({ id, terms, onSelect, onEdit, onAddChild, onDeleted }) {
  const st = useAsync(() => api.get(`${GOV}/glossary/terms/${enc(id)}`), [id]);
  const links = usePaged(`${GOV}/glossary/terms/${enc(id)}/links`, {}, [id], 25);
  const mayEdit = canCurate();
  if (st.error && !st.data) return html`<${ErrorBox} error=${st.error} onRetry=${st.reloadLoud} />`;
  if (!st.data) return html`<${Loading} />`;
  const t = st.data;
  async function link(a) { await act(() => api.post(`${GOV}/glossary/terms/${enc(id)}/links`, { asset_id: a.id }), { ok: `Linked to ${a.name}` }); links.reload(); st.reload(); }
  async function unlink(l) { await act(() => api.del(`${GOV}/glossary/terms/${enc(id)}/links/${l.id}`), { ok: 'Unlinked' }); links.reload(); st.reload(); }
  async function remove() {
    const hard = canApprove();
    const ok = await confirmDialog({ title: `Retire ${t.name}?`, danger: true, confirmLabel: 'Retire the term', body: html`<div class="col gap-2"><p class="m-0">The term is marked as deprecated and its links to assets are removed from view. They are kept as history.</p>${hard ? html`<p class="m-0 muted">To delete it for good, use the checkbox below.</p>` : null}</div>` });
    if (!ok) return;
    let hardDelete = false;
    if (hard) hardDelete = await confirmDialog({ title: 'Delete it for good as well?', body: 'Removes the term, its links and every reference to it. Narrower terms move up one level. This cannot be undone.', confirmLabel: 'Delete for good', cancelLabel: 'Keep the history', danger: true });
    const r = await act(() => api.del(`${GOV}/glossary/terms/${enc(id)}`, { hard: hardDelete || undefined }, { ifMatch: t.etag }), { ok: hardDelete ? 'Term deleted' : 'Term retired' });
    if (r !== undefined) onDeleted();
  }
  return html`<div class="col gap-4" data-testid="term-detail">
    <div><div class="row-wrap mb-1">${(t.ancestors || []).map((a) => html`<span key=${a.id} class="small"><a href="#" onClick=${(e) => { e.preventDefault(); onSelect(a.id); }}>${a.name}</a> <span class="muted">/</span></span>`)}</div>
      <div class="row-wrap"><h2 class="m-0" style="font-size:var(--fs-xl)">${t.name}</h2><${Badge} kind=${statusKind(t.status)}>${TERM_STATUS[t.status] || t.status}<//><${CertBadge} state=${t.certification && t.certification.state ? t.certification.state : typeof t.certification === 'string' ? t.certification : null} />
        ${t.abbreviation ? html`<${Badge} kind="outline" mono=${true}>${t.abbreviation}<//>` : null}</div>
      ${(t.synonyms || []).length ? html`<div class="small muted mt-1">Also called: ${t.synonyms.join(', ')}</div>` : null}</div>
    ${mayEdit ? html`<div class="row-wrap"><${Btn} size="sm" icon="edit" onClick=${() => onEdit(t)}>Edit<//><${Btn} size="sm" icon="plus" onClick=${() => onAddChild(t.id)}>Add a narrower term<//><${Btn} size="sm" kind="ghost" icon="trash" onClick=${remove}>Retire<//></div>` : null}
    <${Card} title="What it means">${t.definition ? html`<div class="doc-md"><${Markdown} source=${t.definition} /></div>` : html`<span class="muted">No definition written yet.</span>`}</${Card}>
    ${(t.children || []).length || (t.related_terms || []).length ? html`<div class="grid c2">
      ${(t.children || []).length ? html`<${Card} title="Narrower terms"><ul class="change-list">${t.children.map((c) => html`<li key=${c.id}><a href="#" onClick=${(e) => { e.preventDefault(); onSelect(c.id); }}>${c.name}</a></li>`)}</ul></${Card}>` : null}
      ${(t.related_terms || []).length ? html`<${Card} title="Related terms"><ul class="change-list">${t.related_terms.map((c) => html`<li key=${c.id}><a href="#" onClick=${(e) => { e.preventDefault(); onSelect(c.id); }}>${c.name}</a></li>`)}</ul></${Card}>` : null}</div>` : null}
    <${Card} title=${`Where it is used (${fmtNum(t.link_count || 0)})`} sub="Assets and data products linked to this term">
      ${links.error && !links.data ? html`<${ErrorBox} error=${links.error} onRetry=${links.reloadLoud} />` : !links.data ? html`<${Loading} />` : links.items.length ? html`<ul class="change-list" data-testid="term-links">${links.items.map((l) => html`<li key=${l.id}>${l.asset ? html`<${AssetLink} asset=${l.asset} showType=${true} />` : l.product ? html`<span><${Icon} name="package" size="sm" /> ${l.product.name} <span class="small muted">data product</span></span>` : null}
        ${l.status === 'pending' ? html`<${Badge} kind="warn">Waiting for review<//>` : null}${l.source === 'suggested' ? html`<${Badge} kind="outline">Suggested<//>` : null}
        ${mayEdit ? html`<${Btn} size="sm" kind="ghost" iconOnly icon="x" label="Unlink" onClick=${() => unlink(l)} />` : null}</li>`)}</ul><${MoreBtn} p=${links} />` : html`<span class="muted">Not linked to anything yet.</span>`}
      ${mayEdit ? html`<div class="mt-3" style="max-width:420px"><${AssetPick} label="Link an asset to this term" onPick=${link} /></div>` : null}</${Card}>
    <${Card} title="Owners and stewards"><${OwnersEditor} prefix="glossary/terms" id=${id} canEdit=${mayEdit} /></${Card}>
    <${Card} title="Certification"><${CertPanel} prefix="glossary/terms" subjectType="term" id=${id} onChange=${() => st.reload()} /></${Card}></div>`;
}

function TermNode({ t, depth, kids, open, setOpen, sel, setSel }) {
  const c = kids[t.id] || [];
  const isOpen = open[t.id] !== undefined ? open[t.id] : depth < 1;
  return html`<li role="none"><div class="tree-row"><button type="button" class="tree-twisty" aria-label=${c.length ? (isOpen ? `Collapse ${t.name}` : `Expand ${t.name}`) : undefined} aria-hidden=${c.length ? undefined : 'true'} tabindex=${c.length ? 0 : -1} onClick=${() => setOpen((o) => ({ ...o, [t.id]: !isOpen }))}>${c.length ? html`<${Icon} name=${isOpen ? 'chevron-down' : 'chevron-right'} size="sm" />` : null}</button>
    <button type="button" role="treeitem" class="tree-node" aria-selected=${sel === t.id} aria-expanded=${c.length ? isOpen : undefined} onClick=${() => setSel(t.id)}><span class="trunc">${t.name}</span>${t.status !== 'approved' ? html`<span class="small muted">${TERM_STATUS[t.status] || t.status}</span>` : null}</button></div>
    ${c.length && isOpen ? html`<ul role="group">${c.map((x) => html`<${TermNode} key=${x.id} t=${x} depth=${depth + 1} kids=${kids} open=${open} setOpen=${setOpen} sel=${sel} setSel=${setSel} />`)}</ul>` : null}</li>`;
}

function GlossarySection() {
  const [sel, setSel] = useQueryState('term');
  const [q, setQ] = useState('');
  const dq = useDebounced(q, 250);
  const list = usePaged(`${GOV}/glossary/terms`, {}, [], 500);
  const doms = useAsync(() => api.get(`${GOV}/domains`, { limit: 500 }).then((r) => r.items || []).catch(() => []), []);
  const search = useAsync(() => (dq.trim() ? api.get(`${GOV}/glossary/search`, { q: dq.trim(), limit: 30 }).then((r) => r.items || []) : Promise.resolve(null)), [dq]);
  const [open, setOpen] = useState({});
  const [dialog, setDialog] = useState(null);
  const [rev, setRev] = useState(0);
  const mayEdit = canCurate();
  const terms = list.items;
  const kids = useMemo(() => { const m = {}; for (const t of terms) (m[t.parent_id || ''] ||= []).push(t); for (const k of Object.keys(m)) m[k].sort((a, b) => a.name.localeCompare(b.name)); return m; }, [terms]);
  useEffect(() => { if (!sel && terms.length) setSel(terms[0].id); }, [terms.length]);
  const after = (r) => { setDialog(null); if (r) { list.reload(); setRev((n) => n + 1); if (r.id) setSel(r.id); } };
  return html`<div class="gov-split" data-testid="glossary">
    <div class="col gap-3"><div class="row-wrap"><div class="grow"><input class="input" type="search" value=${q} onInput=${(e) => setQ(e.target.value)} placeholder="Search terms, short forms and other names" aria-label="Search the glossary" /></div>
      ${mayEdit ? html`<${Btn} kind="primary" size="sm" icon="plus" onClick=${() => setDialog({})}>New term<//>` : null}</div>
      <${Card} flush=${true}><div class="tree-wrap">
        ${list.error && !list.data ? html`<${ErrorBox} error=${list.error} onRetry=${list.reloadLoud} />` : !list.data ? html`<${Loading} />`
          : dq.trim() ? (search.data ? (search.data.length ? html`<ul class="tree" role="listbox" aria-label="Search results">${search.data.map((r) => html`<li key=${r.term.id}><button type="button" class="tree-node" aria-selected=${sel === r.term.id} onClick=${() => setSel(r.term.id)}><span class="trunc">${r.term.name}</span><span class="small muted">matched ${String(r.matched_on || '').replace(/_/g, ' ')}</span></button></li>`)}</ul>` : html`<div class="pad muted small">No term matches.</div>`) : html`<${Loading} />`)
          : !terms.length ? html`<${Empty} icon="book" title="The glossary is empty">${mayEdit ? 'Add the first term to start agreeing on what business words mean.' : 'Nobody has added a term yet.'}</${Empty}>`
            : html`<ul class="tree" role="tree" aria-label="Glossary terms">${(kids[''] || []).map((t) => html`<${TermNode} key=${t.id} t=${t} depth=${0} kids=${kids} open=${open} setOpen=${setOpen} sel=${sel} setSel=${setSel} />`)}</ul><${MoreBtn} p=${list} />`}</div></${Card}></div>
    <div>${sel ? html`<${TermDetail} key=${sel + ':' + rev} id=${sel} terms=${terms} onSelect=${setSel} onEdit=${(t) => setDialog({ term: t })} onAddChild=${(pid) => setDialog({ parent: pid })} onDeleted=${() => { setSel(''); list.reload(); }} />` : !terms.length && list.data ? null : html`<${Empty} icon="book" title="Choose a term">Select a term on the left to see what it means and where it is used.</${Empty}>`}</div>
    ${dialog ? html`<${TermDialog} term=${dialog.term} parent=${dialog.parent} terms=${terms} domains=${doms.data || []} onClose=${after} />` : null}</div>`;
}

// ---------------------------------------------------------------------------------------------------------------- domains and data products
function DomainDialog({ domain, domains, onClose }) {
  const [name, setName] = useState(domain ? domain.name : '');
  const [desc, setDesc] = useState(domain ? domain.description || '' : '');
  const [parent, setParent] = useState(domain ? domain.parent_id || '' : '');
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  async function save() {
    setBusy(true); setErr(null);
    const body = { name: name.trim(), description: desc, parent_id: parent || null };
    try { const r = domain ? await api.patch(`${GOV}/domains/${enc(domain.id)}`, body, { ifMatch: domain.etag }) : await api.post(`${GOV}/domains`, body); toast.ok('Saved'); onClose(r); } catch (e) { setErr(e); } finally { setBusy(false); }
  }
  return html`<${Dialog} title=${domain ? `Edit ${domain.name}` : 'Add a domain'} onClose=${() => onClose(null)} footer=${html`<${Btn} onClick=${() => onClose(null)}>Cancel<//><${Btn} kind="primary" busy=${busy} disabled=${!name.trim()} onClick=${save}>Save<//>`}>
    <div class="col gap-1">${err ? html`<${ErrorBox} error=${err} />` : null}<${Input} label="Name" value=${name} onInput=${setName} required=${true} data-autofocus=${true} />
      <${Textarea} label="What this area covers" value=${desc} onInput=${setDesc} rows=${3} />
      <${Select} label="Sits under" value=${parent} onChange=${setParent} placeholder="Top level" options=${domains.filter((d) => !domain || d.id !== domain.id).map((d) => ({ value: d.id, label: d.name }))} /></div></${Dialog}>`;
}

function DomainDrawer({ id, onClose, onChanged, onEdit }) {
  const st = useAsync(() => api.get(`${GOV}/domains/${enc(id)}`), [id]);
  const assets = usePaged(`${GOV}/domains/${enc(id)}/assets`, { include_descendants: true }, [id], 25);
  const mayEdit = canCurate();
  async function remove() {
    if (!(await confirmDialog({ title: `Delete ${st.data.name}?`, body: 'A domain that still has sub-domains, data products, assets or terms cannot be deleted.', confirmLabel: 'Delete', danger: true }))) return;
    const r = await act(() => api.del(`${GOV}/domains/${enc(id)}`, undefined, { ifMatch: st.data.etag }), { ok: 'Domain deleted' });
    if (r !== undefined) { onChanged(); onClose(); }
  }
  const d = st.data;
  return html`<${Drawer} title=${d ? d.name : 'Domain'} onClose=${onClose} wide=${true} actions=${d && mayEdit ? html`<${Btn} size="sm" icon="edit" onClick=${() => onEdit(d)}>Edit<//><${Btn} size="sm" kind="ghost" iconOnly icon="trash" label="Delete the domain" onClick=${remove} />` : null}>
    ${st.error && !d ? html`<${ErrorBox} error=${st.error} />` : !d ? html`<${Loading} />` : html`<div class="col gap-4">
      <div class="small muted">${(d.ancestors || []).map((a) => a.name).join(' / ')}</div>${d.description ? html`<p class="m-0">${d.description}</p>` : null}
      ${(d.children || []).length ? html`<${Card} title="Sub-domains"><ul class="change-list">${d.children.map((c) => html`<li key=${c.id}>${c.name}</li>`)}</ul></${Card}>` : null}
      <${Card} title="Owners and stewards"><${OwnersEditor} prefix="domains" id=${id} canEdit=${mayEdit} /></${Card}>
      <${Card} title="Assets in this domain" sub="Directly, through a parent asset or through a data product">${assets.error && !assets.data ? html`<${ErrorBox} error=${assets.error} />` : !assets.data ? html`<${Loading} />` : assets.items.length ? html`<ul class="change-list">${assets.items.map((a) => html`<li key=${a.id}><${AssetLink} asset=${a.asset || a} showType=${true} />${a.source ? html`<span class="small muted">${a.source}</span>` : null}</li>`)}</ul><${MoreBtn} p=${assets} />` : html`<span class="muted">No assets yet. Assign one from the Governance tab of the asset.</span>`}</${Card}></div>`}</${Drawer}>`;
}

function ProductDialog({ product, domains, onClose }) {
  const [name, setName] = useState(product ? product.name : '');
  const [desc, setDesc] = useState(product ? product.description || '' : '');
  const [domainId, setDomainId] = useState(product && product.domain ? product.domain.id : '');
  const [status, setStatus] = useState(product ? product.status : 'draft');
  const [fresh, setFresh] = useState(product && product.sla && product.sla.freshness_hours != null ? String(product.sla.freshness_hours) : '');
  const [avail, setAvail] = useState(product && product.sla ? product.sla.availability || '' : '');
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  const mayApprove = canApprove();
  async function save() {
    setBusy(true); setErr(null);
    const sla = {}; if (fresh !== '') sla.freshness_hours = Number(fresh); if (avail.trim()) sla.availability = avail.trim();
    const body = { name: name.trim(), description: desc, domain_id: domainId || null, status, sla };
    try { const r = product ? await api.patch(`${GOV}/products/${enc(product.id)}`, body, { ifMatch: product.etag }) : await api.post(`${GOV}/products`, body); toast.ok('Saved'); onClose(r); } catch (e) { setErr(e); } finally { setBusy(false); }
  }
  return html`<${Dialog} title=${product ? `Edit ${product.name}` : 'Add a data product'} size="lg" onClose=${() => onClose(null)} footer=${html`<${Btn} onClick=${() => onClose(null)}>Cancel<//><${Btn} kind="primary" busy=${busy} disabled=${!name.trim()} onClick=${save}>Save<//>`}>
    <div class="col gap-1">${err ? html`<${ErrorBox} error=${err} />` : null}<${Input} label="Name" value=${name} onInput=${setName} required=${true} data-autofocus=${true} />
      <${Textarea} label="What it offers" value=${desc} onInput=${setDesc} rows=${3} hint="A data product is a named set of tables and reports that someone looks after and promises to keep up to date." />
      <div class="grid c2"><${Select} label="Domain" value=${domainId} onChange=${setDomainId} placeholder="None" options=${domains.map((d) => ({ value: d.id, label: d.name }))} />
        <${Select} label="Status" value=${status} onChange=${setStatus} options=${[['draft', 'Draft'], ['published', 'Published'], ['deprecated', 'Deprecated']].filter(([v]) => v !== 'published' || mayApprove || status === 'published').map(([value, label]) => ({ value, label }))} hint=${mayApprove ? undefined : 'Publishing needs the approve permission.'} /></div>
      <div class="grid c2"><${Input} label="Promised freshness, in hours (optional)" type="number" min="0" value=${fresh} onInput=${setFresh} /><${Input} label="Availability promise (optional)" value=${avail} onInput=${setAvail} placeholder="for example 99% on working days" /></div></div></${Dialog}>`;
}

function ProductDrawer({ id, onClose, onChanged, onEdit }) {
  const st = useAsync(() => api.get(`${GOV}/products/${enc(id)}`), [id]);
  const cons = useAsync(() => api.get(`${GOV}/products/${enc(id)}/consumers`, { depth: 3 }).then((r) => r.items || r.consumers || []).catch(() => []), [id]);
  const mayEdit = canCurate();
  async function addMember(a) { await act(() => api.post(`${GOV}/products/${enc(id)}/members`, { asset_id: a.id }), { ok: `${a.name} added` }); st.reload(); onChanged(); }
  async function removeMember(a) { await act(() => api.del(`${GOV}/products/${enc(id)}/members/${enc(a.id)}`), { ok: 'Removed' }); st.reload(); onChanged(); }
  async function remove() {
    if (!(await confirmDialog({ title: `Delete ${st.data.name}?`, body: 'A published product has to be deprecated first.', confirmLabel: 'Delete', danger: true }))) return;
    const r = await act(() => api.del(`${GOV}/products/${enc(id)}`, undefined, { ifMatch: st.data.etag }), { ok: 'Product deleted' });
    if (r !== undefined) { onChanged(); onClose(); }
  }
  const p = st.data;
  const members = (p && p.members) || [];
  return html`<${Drawer} title=${p ? p.name : 'Data product'} onClose=${onClose} wide=${true} actions=${p && mayEdit ? html`<${Btn} size="sm" icon="edit" onClick=${() => onEdit(p)}>Edit<//><${Btn} size="sm" kind="ghost" iconOnly icon="trash" label="Delete the product" onClick=${remove} />` : null}>
    ${st.error && !p ? html`<${ErrorBox} error=${st.error} />` : !p ? html`<${Loading} />` : html`<div class="col gap-4" data-testid="product-detail">
      <div class="row-wrap"><${Badge} kind=${p.status === 'published' ? 'ok' : p.status === 'deprecated' ? 'bad' : 'warn'}>${p.status}<//><${CertBadge} state=${p.certification && p.certification.state ? p.certification.state : typeof p.certification === 'string' ? p.certification : null} />${p.domain ? html`<span class="small">Domain: <b>${p.domain.name}</b></span>` : null}${p.quality && p.quality.score != null ? html`<${QualityScore} score=${p.quality.score} />` : null}</div>
      ${p.description ? html`<p class="m-0">${p.description}</p>` : null}
      ${p.sla && (p.sla.freshness_hours != null || p.sla.availability) ? html`<div class="small">Promise: ${p.sla.freshness_hours != null ? `refreshed at least every ${p.sla.freshness_hours} hours` : ''}${p.sla.freshness_hours != null && p.sla.availability ? ', ' : ''}${p.sla.availability || ''}</div>` : null}
      <${Card} title=${`What is in it (${members.length}${p.hidden_members ? ` and ${p.hidden_members} you cannot see` : ''})`}>${members.length ? html`<ul class="change-list">${members.map((m) => html`<li key=${m.asset ? m.asset.id : m.id}><${AssetLink} asset=${m.asset || m} showType=${true} />${m.score != null ? html`<${QualityScore} score=${m.score} />` : null}${mayEdit ? html`<${Btn} size="sm" kind="ghost" iconOnly icon="x" label="Remove from the product" onClick=${() => removeMember(m.asset || m)} />` : null}</li>`)}</ul>` : html`<span class="muted">Nothing yet.</span>`}
        ${mayEdit ? html`<div class="mt-3" style="max-width:420px"><${AssetPick} label="Add a table or report" types="table,view,model_table,measure,report,semantic_model,dashboard" onPick=${addMember} /></div>` : null}</${Card}>
      <${Card} title="Who uses it" sub="Reports and datasets downstream of its members">${!cons.data ? html`<${Loading} />` : cons.data.length ? html`<ul class="change-list">${cons.data.slice(0, 30).map((c, i) => html`<li key=${i}><${AssetLink} asset=${c.asset || c} showType=${true} />${c.confidence ? html`<${ConfBadge} level=${c.confidence} />` : null}</li>`)}</ul>` : html`<span class="muted">No known consumers. This only counts what lineage knows about.</span>`}</${Card}>
      ${p.contract && ((p.contract.columns || []).length || (p.contract.expectations || []).length) ? html`<${Card} title="Contract" sub="What the product promises">${(p.contract.columns || []).length ? html`<${Table} caption="Contract columns" rows=${p.contract.columns} rowKey=${(c) => c.name} columns=${[{ key: 'name', label: 'Column' }, { key: 'type', label: 'Type', render: (c) => c.type || '' }, { key: 'description', label: 'Meaning', render: (c) => c.description || '' }]} />` : null}${(p.contract.expectations || []).length ? html`<ul class="mt-2">${p.contract.expectations.map((x, i) => html`<li key=${i}><b>${x.name}</b> ${x.description || x.rule || ''}</li>`)}</ul>` : null}</${Card}>` : null}
      <${Card} title="Owners and stewards"><${OwnersEditor} prefix="products" id=${id} canEdit=${mayEdit} /></${Card}>
      <${Card} title="Certification"><${CertPanel} prefix="products" subjectType="product" id=${id} onChange=${() => { st.reload(); onChanged(); }} /></${Card}></div>`}</${Drawer}>`;
}

function DomainsSection() {
  const [view, setView] = useQueryState('view', 'domains');
  const mayEdit = canCurate();
  const domains = usePaged(`${GOV}/domains`, {}, [], 500);
  const products = usePaged(`${GOV}/products`, {}, [], 200);
  const [open, setOpen] = useState(null);
  const [rev, setRev] = useState(0);
  const [dialog, setDialog] = useState(null);
  const nameOf = (id) => (domains.items.find((d) => d.id === id) || {}).name;
  return html`<div class="col gap-3" data-testid="domains">
    <div class="row-wrap"><${Seg} label="Show" options=${[{ value: 'domains', label: 'Domains' }, { value: 'products', label: 'Data products' }]} value=${view} onChange=${setView} />
      ${mayEdit ? html`<${Btn} kind="primary" size="sm" icon="plus" onClick=${() => setDialog(view === 'domains' ? { kind: 'domain' } : { kind: 'product' })}>${view === 'domains' ? 'New domain' : 'New data product'}<//>` : null}</div>
    <p class="small muted m-0">${view === 'domains' ? 'A domain is an area of the business, such as Sales or Finance, that data belongs to.' : 'A data product is a named set of tables and reports that someone looks after.'}</p>
    ${view === 'domains' ? (domains.error && !domains.data ? html`<${ErrorBox} error=${domains.error} onRetry=${domains.reloadLoud} />` : !domains.data ? html`<${Loading} />` : html`<${Card} flush=${true}><${Table} caption="Domains" rows=${domains.items} onRowClick=${(d) => setOpen({ kind: 'domain', id: d.id })} initialSort=${{ key: 'name', dir: 1 }}
        columns=${[{ key: 'name', label: 'Domain', render: (d) => html`<b>${d.name}</b>${d.parent_id ? html` <span class="small muted">under ${nameOf(d.parent_id) || '…'}</span>` : ''}` }, { key: 'description', label: 'About', sortable: false, render: (d) => html`<span class="muted">${d.description || ''}</span>` },
          { key: 'asset_count', label: 'Assets', num: true }, { key: 'product_count', label: 'Products', num: true }, { key: 'child_count', label: 'Sub-domains', num: true }]}
        empty=${html`<${Empty} icon="folder" title="No domains yet">${mayEdit ? 'Add one to group data by business area.' : 'Nobody has added a domain.'}</${Empty}>`} /></${Card}>`)
      : (products.error && !products.data ? html`<${ErrorBox} error=${products.error} onRetry=${products.reloadLoud} />` : !products.data ? html`<${Loading} />` : html`<${Card} flush=${true}><${Table} caption="Data products" rows=${products.items} onRowClick=${(p) => setOpen({ kind: 'product', id: p.id })} initialSort=${{ key: 'name', dir: 1 }}
        columns=${[{ key: 'name', label: 'Product', render: (p) => html`<b>${p.name}</b>` }, { key: 'domain', label: 'Domain', sort: (p) => (p.domain ? p.domain.name : ''), render: (p) => (p.domain ? p.domain.name : html`<span class="muted">none</span>`) },
          { key: 'status', label: 'Status', render: (p) => html`<${Badge} kind=${p.status === 'published' ? 'ok' : p.status === 'deprecated' ? 'bad' : 'warn'}>${p.status}<//>` },
          { key: 'certification', label: 'Certified', sortable: false, render: (p) => html`<${CertBadge} state=${p.certification && p.certification.state ? p.certification.state : typeof p.certification === 'string' ? p.certification : null} />` }, { key: 'member_count', label: 'Members', num: true }]}
        empty=${html`<${Empty} icon="package" title="No data products yet">${mayEdit ? 'Group related tables and reports into a product people can rely on.' : 'Nobody has added a data product.'}</${Empty}>`} /></${Card}>`)}
    ${open && open.kind === 'domain' ? html`<${DomainDrawer} key=${rev} id=${open.id} onClose=${() => setOpen(null)} onChanged=${() => domains.reload()} onEdit=${(d) => setDialog({ kind: 'domain', item: d })} />` : null}
    ${open && open.kind === 'product' ? html`<${ProductDrawer} key=${rev} id=${open.id} onClose=${() => setOpen(null)} onChanged=${() => products.reload()} onEdit=${(p) => setDialog({ kind: 'product', item: p })} />` : null}
    ${dialog && dialog.kind === 'domain' ? html`<${DomainDialog} domain=${dialog.item} domains=${domains.items} onClose=${(r) => { setDialog(null); if (r) { domains.reload(); setRev((x) => x + 1); } }} />` : null}
    ${dialog && dialog.kind === 'product' ? html`<${ProductDialog} product=${dialog.item} domains=${domains.items} onClose=${(r) => { setDialog(null); if (r) { products.reload(); setRev((x) => x + 1); } }} />` : null}</div>`;
}

// ---------------------------------------------------------------------------------------------------------------- classifications
function ClassDialog({ cls, onClose }) {
  const [name, setName] = useState(cls ? cls.name : '');
  const [cat, setCat] = useState(cls ? cls.category || '' : '');
  const [level, setLevel] = useState(cls ? cls.level : 1);
  const [desc, setDesc] = useState(cls ? cls.description || '' : '');
  const [color, setColor] = useState(cls && cls.color ? cls.color : '#6b7280');
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  const builtin = cls && cls.builtin;
  async function save() {
    setBusy(true); setErr(null);
    const body = builtin ? { description: desc, color } : { name: name.trim(), category: cat.trim(), level: Number(level), description: desc, color };
    try { const r = cls ? await api.patch(`${GOV}/classifications/${enc(cls.id)}`, body, { ifMatch: cls.etag }) : await api.post(`${GOV}/classifications`, body); toast.ok('Saved'); invalidateGovernance(); onClose(r); } catch (e) { setErr(e); } finally { setBusy(false); }
  }
  return html`<${Dialog} title=${cls ? `Edit ${cls.name}` : 'Add a label'} onClose=${() => onClose(null)} footer=${html`<${Btn} onClick=${() => onClose(null)}>Cancel<//><${Btn} kind="primary" busy=${busy} disabled=${!name.trim() || (!builtin && !cat.trim())} onClick=${save}>Save<//>`}>
    <div class="col gap-1">${err ? html`<${ErrorBox} error=${err} />` : null}
      ${builtin ? html`<p class="small muted m-0">This is one of the labels that come with OLCF. Only its description and colour can change.</p>` : null}
      <${Input} label="Name" value=${name} onInput=${setName} required=${true} disabled=${builtin} data-autofocus=${!builtin} />
      <div class="grid c2"><${Input} label="Kind of data" value=${cat} onInput=${setCat} disabled=${builtin} placeholder="for example personal, financial" />
        <${Select} label="How sensitive" value=${String(level)} onChange=${setLevel} disabled=${builtin} options=${[['0', '0 Public'], ['1', '1 Internal'], ['2', '2 Confidential'], ['3', '3 Sensitive'], ['4', '4 Highly sensitive'], ['5', '5 Restricted']].map(([value, label]) => ({ value, label }))} /></div>
      <${Textarea} label="What it is for" value=${desc} onInput=${setDesc} rows=${3} />
      <div class="field"><label for="cls-color">Colour</label><input id="cls-color" type="color" value=${color} onInput=${(e) => setColor(e.target.value)} /></div></div></${Dialog}>`;
}

function ClassDrawer({ cls, onClose, onChanged, onEdit }) {
  const assets = usePaged(`${GOV}/classifications/${enc(cls.id)}/assets`, { include_inherited: false }, [cls.id], 25);
  const mayCurate = canCurate();
  async function remove() {
    if (!(await confirmDialog({ title: `Delete the label ${cls.name}?`, body: 'A label that is still assigned to something cannot be deleted.', confirmLabel: 'Delete', danger: true }))) return;
    const r = await act(() => api.del(`${GOV}/classifications/${enc(cls.id)}`, undefined, { ifMatch: cls.etag }), { ok: 'Label deleted' });
    if (r !== undefined) { invalidateGovernance(); onChanged(); onClose(); }
  }
  return html`<${Drawer} title=${cls.name} onClose=${onClose} wide=${true} actions=${html`${mayCurate ? html`<${Btn} size="sm" icon="edit" onClick=${() => onEdit(cls)}>Edit<//>` : null}${canApprove() && !cls.builtin ? html`<${Btn} size="sm" kind="ghost" iconOnly icon="trash" label="Delete the label" onClick=${remove} />` : null}`}>
    <div class="col gap-4"><div class="row-wrap"><${ClassBadge} c=${cls} /><span class="small muted">${cls.category}, sensitivity ${cls.level}</span>${cls.builtin ? html`<${Badge} kind="outline">Comes with OLCF<//>` : null}</div>
      ${cls.description ? html`<p class="m-0">${cls.description}</p>` : null}
      <div class="row-wrap"><${Btn} size="sm" icon="route" href=${governanceHref('exposure', { cls: cls.id })}>Where does it end up?<//><${Btn} size="sm" icon="search" href=${catalogHref({ classification: cls.name })}>Find in the catalog<//></div>
      <${Card} title=${`Assets carrying this label`}>${assets.error && !assets.data ? html`<${ErrorBox} error=${assets.error} />` : !assets.data ? html`<${Loading} />` : assets.items.length ? html`<ul class="change-list">${assets.items.map((a, i) => html`<li key=${i}><${AssetLink} asset=${a.asset} showType=${true} /><span class="small muted">${a.source === 'auto' ? 'suggested by a scan' : a.source || ''}${a.status && a.status !== 'approved' ? `, ${a.status}` : ''}</span></li>`)}</ul><${MoreBtn} p=${assets} />` : html`<span class="muted">Nothing carries this label yet.</span>`}</${Card}></div></${Drawer}>`;
}

function ClassificationsSection() {
  const st = usePaged(`${GOV}/classifications`, {}, [], 200);
  const [open, setOpen] = useState(null);
  const [dialog, setDialog] = useState(null);
  const mayEdit = canApprove() || canCurate();
  const [busy, setBusy] = useState(false);
  async function scanNow() {
    setBusy(true);
    try { const r = await api.post(`${GOV}/classifications/scan`, {}); toast.ok('Looked at the latest scans', (({ n }) => `${plural(n, 'new suggestion')} ${n === 1 ? 'is' : 'are'} waiting in the review queue.`)({ n: (r && (r.suggested ?? r.created ?? r.queued)) || 0 })); invalidateGovernance(); } catch (e) { toast.bad('Could not look for suggestions', e && e.message); } finally { setBusy(false); }
  }
  return html`<div class="col gap-3" data-testid="classifications">
    <${Banner} kind="info" title="Labels are suggestions until a person agrees">Scans suggest labels from column names and types only. They never read your data. A suggestion becomes a label only when someone approves it in the review queue.<//>
    <div class="row-wrap">${mayEdit ? html`<${Btn} kind="primary" size="sm" icon="plus" onClick=${() => setDialog({})}>Add a label<//><${Btn} size="sm" icon="refresh" busy=${busy} onClick=${scanNow}>Look for suggestions now<//>` : null}<${Btn} size="sm" icon="check-circle" href=${governanceHref('review')}>Open the review queue<//></div>
    ${st.error && !st.data ? html`<${ErrorBox} error=${st.error} onRetry=${st.reloadLoud} />` : !st.data ? html`<${Loading} />` : html`<${Card} flush=${true}><${Table} caption="Labels" rows=${st.items} onRowClick=${(c) => setOpen(c)} initialSort=${{ key: 'level', dir: -1 }}
      columns=${[{ key: 'name', label: 'Label', sort: (c) => c.name, render: (c) => html`<${ClassBadge} c=${c} />` }, { key: 'category', label: 'Kind of data' }, { key: 'level', label: 'Sensitivity', num: true }, { key: 'description', label: 'About', sortable: false, render: (c) => html`<span class="muted">${c.description || ''}</span>` },
        { key: 'assignment_count', label: 'In use', num: true }, { key: 'builtin', label: '', sortable: false, render: (c) => (c.builtin ? html`<${Badge} kind="outline">built in<//>` : '') }]}
      empty=${html`<${Empty} icon="tag" title="No labels yet">Add one to start marking sensitive data.</${Empty}>`} /></${Card}>`}
    ${open ? html`<${ClassDrawer} cls=${open} onClose=${() => setOpen(null)} onChanged=${() => st.reload()} onEdit=${(c) => setDialog({ cls: c })} />` : null}
    ${dialog ? html`<${ClassDialog} cls=${dialog.cls} onClose=${(r) => { setDialog(null); if (r) { st.reload(); setOpen(null); } }} />` : null}</div>`;
}

// ---------------------------------------------------------------------------------------------------------------- review queue
function evidenceText(it) {
  const e = it.evidence || {};
  const parts = [];
  if (e.rule) parts.push(`rule ${String(e.rule).replace(/_/g, ' ')}`);
  if (e.detector_label) parts.push(`looks like ${String(e.detector_label).replace(/_/g, ' ')}`);
  if (e.match) parts.push(`matched ${e.match}`);
  if (e.matched_on) parts.push(`matched on ${String(e.matched_on).replace(/_/g, ' ')}`);
  if (Array.isArray(e.evidence_kinds) && e.evidence_kinds.length) parts.push(`from ${e.evidence_kinds.join(' and ').replace(/_/g, ' ')}`);
  return parts.join(', ');
}
function ReviewSection() {
  const [kind, setKind] = useState('');
  const [minc, setMinc] = useState('');
  const [q, setQ] = useState('');
  const dq = useDebounced(q, 300);
  const list = usePaged(`${GOV}/review-queue`, { kind: kind || undefined, min_confidence: minc || undefined, q: dq || undefined }, [kind, minc, dq], 50);
  const sum = useAsync(() => api.get(`${GOV}/review-queue/summary`), [list.data]);
  const [picked, setPicked] = useState({});
  const [busy, setBusy] = useState(false);
  const ids = Object.keys(picked).filter((k) => picked[k]);
  const items = list.items;
  useEffect(() => { setPicked({}); }, [kind, minc, dq]);
  async function decide(action, which) {
    const comment = await askText({ title: action === 'approve' ? `Approve ${which.length === 1 ? 'this suggestion' : `${which.length} suggestions`}` : `Reject ${which.length === 1 ? 'this suggestion' : `${which.length} suggestions`}`, label: 'Comment (optional)', confirmLabel: action === 'approve' ? 'Approve' : 'Reject', hint: action === 'reject' ? 'A rejected suggestion is not proposed again.' : undefined });
    if (comment === null) return;
    setBusy(true);
    try {
      if (which.length === 1) await api.post(`${GOV}/review-queue/${enc(which[0])}/${action}`, { comment: comment || undefined });
      else {
        const r = await api.post(`${GOV}/review-queue/bulk`, { action, ids: which.slice(0, 500), comment: comment || undefined });
        if (r.failed) toast.warn(`${r.ok} done, ${r.failed} could not be decided`, (r.results.find((x) => !x.ok) || {}).error);
        else toast.ok(`${r.ok} ${action === 'approve' ? 'approved' : 'rejected'}`);
      }
      if (which.length === 1) toast.ok(action === 'approve' ? 'Approved' : 'Rejected');
      setPicked({}); invalidateGovernance(); list.reload();
    } catch (e) { toast.bad('Could not decide', e && (e.message || String(e))); } finally { setBusy(false); }
  }
  const s = sum.data || {};
  return html`<div class="col gap-3" data-testid="review">
    <${Banner} kind="info" title="Nothing here is a fact yet">These are guesses from names and links. Check each one. Approving makes it a label or link people can rely on; rejecting stops it being suggested again.<//>
    <div class="filter-row"><div style="width:220px"><${Select} label="Kind" value=${kind} onChange=${setKind} placeholder=${`All (${fmtNum(s.total || 0)})`} options=${[{ value: 'classification', label: `Labels (${fmtNum(s.classification || 0)})` }, { value: 'term_link', label: `Glossary links (${fmtNum(s.term_link || 0)})` }]} /></div>
      <div style="width:220px"><${Select} label="How sure" value=${minc} onChange=${setMinc} placeholder="Any" options=${[{ value: '0.5', label: 'At least half sure' }, { value: '0.8', label: 'Very sure (80% or more)' }]} /></div>
      <div style="min-width:220px"><${Input} label="Asset name" value=${q} onInput=${setQ} type="search" /></div></div>
    ${ids.length ? html`<div class="row-wrap bulk-bar" role="status"><b>${ids.length} selected</b><${Btn} size="sm" kind="primary" busy=${busy} onClick=${() => decide('approve', ids)}>Approve<//><${Btn} size="sm" busy=${busy} onClick=${() => decide('reject', ids)}>Reject<//><${Btn} size="sm" kind="ghost" onClick=${() => setPicked({})}>Clear<//></div>` : null}
    ${list.error && !list.data ? html`<${ErrorBox} error=${list.error} onRetry=${list.reloadLoud} />` : !list.data ? html`<${Loading} />` : !items.length ? html`<${Empty} icon="check-circle" title="Nothing waiting">New suggestions appear after a scan. You can also ask for them on the Classifications page.</${Empty}>` : html`<${Card} flush=${true}>
      <div class="queue-head"><${Check} label=${`Select all ${items.length} shown`} checked=${ids.length === items.length} onChange=${(v) => setPicked(v ? Object.fromEntries(items.map((i) => [i.id, true])) : {})} /></div>
      ${items.map((it) => html`<div class="queue-row" key=${it.id} data-testid="queue-item"><input type="checkbox" aria-label=${`Select ${it.asset ? it.asset.name : it.id}`} checked=${!!picked[it.id]} onChange=${(e) => setPicked((p) => ({ ...p, [it.id]: e.target.checked }))} />
        <div class="col gap-1" style="min-width:0"><div class="row-wrap">${it.kind === 'classification' ? html`<${ClassBadge} c=${it.classification} status="suggested" /><span class="small muted">on</span>` : html`<${Badge} kind="outline"><${Icon} name="book" size="sm" />${it.term ? it.term.name : 'term'}<//><span class="small muted">linked to</span>`}<${AssetLink} asset=${it.asset} showType=${true} /></div>
          <div class="small muted">${it.confidence != null ? `${Math.round(it.confidence * 100)}% sure` : ''}${evidenceText(it) ? ` (${evidenceText(it)})` : ''} · suggested <${Ago} ts=${it.created_at} /></div></div>
        <div class="row"><${Btn} size="sm" kind="primary" disabled=${busy || !canCurate(it.asset && it.asset.workspace_id)} onClick=${() => decide('approve', [it.id])}>Approve<//><${Btn} size="sm" disabled=${busy || !canCurate(it.asset && it.asset.workspace_id)} onClick=${() => decide('reject', [it.id])}>Reject<//></div></div>`)}</${Card}>
      <${MoreBtn} p=${list} />`}</div>`;
}

// ---------------------------------------------------------------------------------------------------------------- requests and certifications
const CHANGE_LABEL = { certify: 'Certify', deprecate: 'Deprecate', classification_change: 'Change labels', term_link: 'Link a term', ownership_change: 'Change owners', policy_exception: 'Allow an exception' };
const CHANGE_STATUS = { pending: ['warn', 'Waiting'], approved: ['ok', 'Approved'], rejected: ['bad', 'Rejected'], withdrawn: ['off', 'Withdrawn'] };

function ChangeDrawer({ id, onClose, onChanged }) {
  const st = useAsync(() => api.get(`${GOV}/changes/${enc(id)}`), [id]);
  const [text, setText] = useState('');
  const c = st.data;
  const ws = c && c.workspace_id;
  async function act1(action, comment) {
    try { await api.post(`${GOV}/changes/${enc(id)}/${action}`, comment != null ? { comment } : {}); toast.ok(action === 'approve' ? 'Approved and applied' : action === 'reject' ? 'Rejected' : action === 'withdraw' ? 'Withdrawn' : 'Done'); st.reload(); onChanged(); invalidateGovernance(); } catch (e) { toast.bad('Could not do that', e && (e.message || String(e))); }
  }
  async function approve() { const cm = await askText({ title: 'Approve and apply this change', label: 'Comment (optional)', confirmLabel: 'Approve' }); if (cm !== null) act1('approve', cm || undefined); }
  async function reject() { const cm = await askText({ title: 'Reject this change', label: 'Why', confirmLabel: 'Reject', required: true }); if (cm) act1('reject', cm); }
  async function comment() { if (!text.trim()) return; try { await api.post(`${GOV}/changes/${enc(id)}/comments`, { text }); setText(''); st.reload(); } catch (e) { toast.bad('Could not comment', e && e.message); } }
  return html`<${Drawer} title=${c ? `${CHANGE_LABEL[c.type] || c.type}` : 'Request'} onClose=${onClose} wide=${true}>
    ${st.error && !c ? html`<${ErrorBox} error=${st.error} />` : !c ? html`<${Loading} />` : html`<div class="col gap-4" data-testid="change-detail">
      <div class="row-wrap"><${Badge} kind=${CHANGE_STATUS[c.status][0]}>${CHANGE_STATUS[c.status][1]}<//><span class="small muted">asked by ${c.requester ? c.requester.name : 'someone'} <${Ago} ts=${c.created_at} /></span></div>
      <${Card} title="About"><div class="col gap-2">${c.subject && c.subject.id && c.subject_type === 'asset' ? html`<div><${AssetLink} asset=${c.subject} showType=${true} /></div>` : html`<div>${c.subject_type}: ${(c.subject && c.subject.name) || c.subject_id}</div>`}
        <pre class="payload">${JSON.stringify(c.payload, null, 2)}</pre>
        ${c.decision_comment ? html`<div><b>Decision note: </b>${c.decision_comment}</div>` : null}${c.applied_at ? html`<div class="small muted">Applied <${Ago} ts=${c.applied_at} /></div>` : null}
        ${c.apply_result && c.apply_result.error ? html`<${Banner} kind="bad" title="It was approved but could not be applied">${typeof c.apply_result.error === 'string' ? c.apply_result.error : JSON.stringify(c.apply_result.error)}<//>` : null}</div></${Card}>
      ${c.status === 'pending' ? html`<div class="row-wrap">${canApprove(ws) ? html`<${Btn} kind="primary" onClick=${approve}>Approve and apply<//><${Btn} onClick=${reject}>Reject<//>` : html`<span class="small muted">Approving needs the approve permission.</span>`}<${Btn} kind="ghost" onClick=${() => act1('withdraw')}>Withdraw<//></div>` : null}
      ${c.status === 'approved' && !c.applied_at && canApprove(ws) ? html`<${Btn} onClick=${() => act1('apply')}>Apply it again<//>` : null}
      <${Card} title="Discussion">${(c.comments || []).length ? html`<ul class="change-list">${c.comments.map((m, i) => html`<li key=${i}><div><b>${m.by || 'someone'}</b> <span class="small muted"><${Ago} ts=${m.at} /></span><div>${m.text}</div></div></li>`)}</ul>` : html`<span class="muted">No comments yet.</span>`}
        ${canCurate(ws) ? html`<div class="row mt-3"><div class="grow"><input class="input" value=${text} onInput=${(e) => setText(e.target.value)} placeholder="Add a comment" aria-label="Add a comment" onKeyDown=${(e) => { if (e.key === 'Enter') comment(); }} /></div><${Btn} onClick=${comment} disabled=${!text.trim()}>Send<//></div>` : null}</${Card}></div>`}</${Drawer}>`;
}

function RequestsSection() {
  const [view, setView] = useQueryState('view', 'inbox');
  const [status, setStatus] = useState('pending');
  const [open, setOpen] = useState(null);
  const q = view === 'mine' ? { requester: 'me' } : view === 'review' ? { reviewer: 'me' } : {};
  const ch = usePaged(`${GOV}/changes`, { status: status || undefined, ...q }, [status, view], 50);
  const certs = usePaged(`${GOV}/certifications`, {}, [], 50);
  return html`<div class="col gap-3" data-testid="requests">
    <div class="row-wrap"><${Seg} label="Show" options=${[{ value: 'inbox', label: 'All requests' }, { value: 'review', label: 'Waiting for me' }, { value: 'mine', label: 'Asked by me' }, { value: 'certified', label: 'Certified things' }]} value=${view} onChange=${setView} /></div>
    ${view === 'certified' ? html`
      ${certs.error && !certs.data ? html`<${ErrorBox} error=${certs.error} onRetry=${certs.reloadLoud} />` : !certs.data ? html`<${Loading} />` : html`<${Card} flush=${true}><${Table} caption="Certified and deprecated" rows=${certs.items} rowKey=${(c) => c.subject_type + c.subject_id}
        columns=${[{ key: 'subject', label: 'What', sortable: false, render: (c) => (c.asset ? html`<${AssetLink} asset=${c.asset} showType=${true} />` : html`<span>${(c.subject && c.subject.name) || c.subject_id} <span class="small muted">${c.subject_type}</span></span>`) },
          { key: 'state', label: 'State', render: (c) => html`<${CertBadge} state=${c.state} />` }, { key: 'certified_by', label: 'By', render: (c) => (c.certified_by && !/^usr_/.test(c.certified_by) ? c.certified_by : '') }, { key: 'certified_at', label: 'When', render: (c) => html`<${Ago} ts=${c.certified_at} />` }]}
        empty=${html`<${Empty} icon="shield-check" title="Nothing is certified yet">Certify an asset from its Governance tab, or approve a request.</${Empty}>`} /></${Card}><${MoreBtn} p=${certs} />`}`
    : html`<div class="filter-row"><div style="width:200px"><${Select} label="State" value=${status} onChange=${setStatus} placeholder="Any" options=${Object.entries(CHANGE_STATUS).map(([value, v]) => ({ value, label: v[1] }))} /></div></div>
      ${ch.error && !ch.data ? html`<${ErrorBox} error=${ch.error} onRetry=${ch.reloadLoud} />` : !ch.data ? html`<${Loading} />` : html`<${Card} flush=${true}><${Table} caption="Change requests" rows=${ch.items} onRowClick=${(c) => setOpen(c.id)}
        columns=${[{ key: 'type', label: 'Request', sort: (c) => c.type, render: (c) => CHANGE_LABEL[c.type] || c.type }, { key: 'subject', label: 'About', sortable: false, render: (c) => (c.subject && c.subject.id && c.subject_type === 'asset' ? html`<${AssetLink} asset=${c.subject} />` : (c.subject && c.subject.name) || c.subject_id) },
          { key: 'requester', label: 'Asked by', render: (c) => (c.requester ? c.requester.name : '') }, { key: 'created_at', label: 'When', sort: (c) => c.created_at, render: (c) => html`<${Ago} ts=${c.created_at} />` },
          { key: 'status', label: 'State', render: (c) => html`<${Badge} kind=${CHANGE_STATUS[c.status][0]}>${CHANGE_STATUS[c.status][1]}<//>` }]}
        empty=${html`<${Empty} icon="inbox" title="No requests here">Requests appear when someone asks to certify something or to change labels or owners that need approval.</${Empty}>`} /></${Card}><${MoreBtn} p=${ch} />`}`}
    ${open ? html`<${ChangeDrawer} id=${open} onClose=${() => setOpen(null)} onChanged=${() => ch.reload()} />` : null}</div>`;
}

// ---------------------------------------------------------------------------------------------------------------- policies and violations
const SEVERITY = { low: ['off', 'Low'], medium: ['warn', 'Medium'], high: ['bad', 'High'], critical: ['bad', 'Critical'] };
const VSTATE = { open: ['warn', 'Open'], acknowledged: ['info', 'Seen'], excepted: ['off', 'Allowed for now'], resolved: ['ok', 'Resolved'] };
/** The main parameters of each rule type as plain fields; anything else goes in the advanced box. */
const RULE_FIELDS = {
  classification_flow: [{ key: 'classification', kind: 'class', label: 'Data carrying this label', required: true }, { key: 'include_possible', kind: 'bool', label: 'Also report possible exposures that lineage does not prove' }],
  required_owner: [{ key: 'types', kind: 'list', label: 'Kinds of asset (comma separated, empty for the usual ones)' }],
  required_description: [{ key: 'min_length', kind: 'number', label: 'Shortest description, in characters', dflt: 10 }, { key: 'types', kind: 'list', label: 'Kinds of asset (comma separated)' }],
  required_classification: [{ key: 'name_patterns', kind: 'lines', label: 'Column names that must carry a label (one pattern per line, * allowed)', required: true }],
  certified_sources_only: [{ key: 'depth', kind: 'number', label: 'How many steps upstream to look', dflt: 5 }],
  freshness: [{ key: 'max_age_hours', kind: 'number', label: 'Oldest allowed, in hours', required: true, dflt: 24 }, { key: 'types', kind: 'list', label: 'Kinds of asset (comma separated)' }],
  quality_min: [{ key: 'min_score', kind: 'number', label: 'Lowest allowed quality score (0 to 100)', required: true, dflt: 60 }, { key: 'types', kind: 'list', label: 'Kinds of asset (comma separated)' }],
};
const RULE_NAME = { classification_flow: 'Labelled data must not reach certain places', required_owner: 'Things must have an owner', required_description: 'Things must be described', required_classification: 'Sensitive-looking columns must carry a label', certified_sources_only: 'Reports must use certified sources', freshness: 'Data must be fresh', quality_min: 'Quality score must stay above a limit' };

function PolicyDialog({ policy, ruleTypes, classes, onClose }) {
  const editing = !!policy;
  const [name, setName] = useState(policy ? policy.name : '');
  const [desc, setDesc] = useState(policy ? policy.description || '' : '');
  const [rule, setRule] = useState(policy ? policy.rule_type : (ruleTypes[0] && ruleTypes[0].rule_type) || 'required_owner');
  const [sev, setSev] = useState(policy ? policy.severity : 'medium');
  const [enabled, setEnabled] = useState(policy ? policy.enabled : true);
  const fields = RULE_FIELDS[rule] || [];
  const params0 = policy && policy.rule_type === rule ? policy.params || {} : {};
  const [vals, setVals] = useState(() => { const v = {}; for (const f of fields) { const x = params0[f.key]; v[f.key] = f.kind === 'lines' || f.kind === 'list' ? (x || []).join(f.kind === 'lines' ? '\n' : ', ') : f.kind === 'bool' ? !!x : x != null ? String(x) : f.dflt != null && !editing ? String(f.dflt) : ''; } return v; });
  const known = new Set(fields.map((f) => f.key));
  const [adv, setAdv] = useState(() => { const o = {}; for (const [k, v] of Object.entries(params0)) if (!known.has(k)) o[k] = v; return Object.keys(o).length ? JSON.stringify(o, null, 2) : ''; });
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  const [preview, setPreview] = useState(null);
  const doc = (ruleTypes.find((r) => r.rule_type === rule) || {});
  function changeRule(r) { setRule(r); const v = {}; for (const f of RULE_FIELDS[r] || []) v[f.key] = f.kind === 'bool' ? false : f.dflt != null ? String(f.dflt) : ''; setVals(v); setAdv(''); setPreview(null); }
  function params() {
    const out = {};
    for (const f of RULE_FIELDS[rule] || []) {
      const x = vals[f.key];
      if (f.kind === 'bool') { if (x) out[f.key] = true; } else if (f.kind === 'number') { if (x !== '' && x != null) out[f.key] = Number(x); } else if (f.kind === 'lines') { const l = String(x || '').split('\n').map((s) => s.trim()).filter(Boolean); if (l.length) out[f.key] = l; } else if (f.kind === 'list') { const l = String(x || '').split(',').map((s) => s.trim()).filter(Boolean); if (l.length) out[f.key] = l; } else if (x) out[f.key] = x;
    }
    if (adv.trim()) { let extra; try { extra = JSON.parse(adv); } catch { throw new Error('The advanced settings are not valid JSON.'); } if (!extra || typeof extra !== 'object' || Array.isArray(extra)) throw new Error('The advanced settings must be a JSON object.'); Object.assign(out, extra); }
    return out;
  }
  async function run(kind) {
    setErr(null); setPreview(null);
    let p; try { p = params(); } catch (e) { setErr(e); return; }
    setBusy(kind);
    try {
      if (kind === 'preview') setPreview(await api.post(`${GOV}/policies/preview`, { rule_type: rule, params: p, severity: sev, limit: 20 }));
      else { const body = { name: name.trim(), description: desc, rule_type: rule, params: p, severity: sev, enabled, mode: 'observe' }; const r = editing ? await api.patch(`${GOV}/policies/${enc(policy.id)}`, body, { ifMatch: policy.etag }) : await api.post(`${GOV}/policies`, body); toast.ok('Policy saved'); onClose(r); }
    } catch (e) { setErr(e); } finally { setBusy(false); }
  }
  const pv = preview && (preview.findings || preview.items || preview.violations || []);
  return html`<${Dialog} title=${editing ? `Edit ${policy.name}` : 'Add a policy'} size="lg" onClose=${() => onClose(null)} footer=${html`<${Btn} onClick=${() => onClose(null)}>Cancel<//><${Btn} icon="eye" busy=${busy === 'preview'} onClick=${() => run('preview')}>Preview what it would report<//><${Btn} kind="primary" busy=${busy === 'save'} disabled=${!name.trim()} onClick=${() => run('save')}>Save<//>`}>
    <div class="col gap-1" data-testid="policy-dialog">${err ? html`<div class="mb-3"><${ErrorBox} error=${err} /></div>` : null}
      <div class="warn-box mb-3">A policy only <b>reports</b>. It lists what breaks the rule; it never blocks a query, a pipeline or a notebook.</div>
      <${Input} label="Name" value=${name} onInput=${setName} required=${true} data-autofocus=${true} />
      <${Select} label="The rule" value=${rule} onChange=${changeRule} disabled=${editing} options=${ruleTypes.map((r) => ({ value: r.rule_type, label: RULE_NAME[r.rule_type] || r.rule_type }))} hint=${doc.description} />
      ${fields.map((f) => (f.kind === 'class' ? html`<${Select} key=${f.key} label=${f.label} value=${vals[f.key] || ''} onChange=${(v) => setVals((x) => ({ ...x, [f.key]: v }))} placeholder="Choose a label" options=${classes.map((c) => ({ value: c.name, label: c.name }))} />`
        : f.kind === 'bool' ? html`<${Check} key=${f.key} label=${f.label} checked=${!!vals[f.key]} onChange=${(v) => setVals((x) => ({ ...x, [f.key]: v }))} />`
          : f.kind === 'lines' ? html`<${Textarea} key=${f.key} label=${f.label} value=${vals[f.key] || ''} rows=${3} onInput=${(v) => setVals((x) => ({ ...x, [f.key]: v }))} />`
            : html`<${Input} key=${f.key} label=${f.label} type=${f.kind === 'number' ? 'number' : 'text'} value=${vals[f.key] ?? ''} onInput=${(v) => setVals((x) => ({ ...x, [f.key]: v }))} />`))}
      <div class="grid c2"><${Select} label="How serious" value=${sev} onChange=${setSev} options=${Object.entries(SEVERITY).map(([value, v]) => ({ value, label: v[1] }))} /><div style="padding-top:22px"><${Switch} label="Check it regularly" checked=${enabled} onChange=${setEnabled} /></div></div>
      <${Textarea} label="Why it exists (optional)" value=${desc} onInput=${setDesc} rows=${2} />
      <details class="mt-2"><summary class="small">Advanced settings</summary><p class="small muted">Extra settings as JSON, for example a selector that limits the rule to some workspaces or names. Known settings: ${Object.keys(doc.params || {}).join(', ')}.</p><${Textarea} label="Extra settings (JSON)" mono=${true} value=${adv} onInput=${setAdv} rows=${4} /></details>
      ${preview ? html`<div class="mt-3" data-testid="policy-preview"><${Banner} kind=${pv && pv.length ? 'warn' : 'ok'} title=${pv && pv.length ? `It would report ${plural(preview.total ?? pv.length, 'thing')}` : 'It would report nothing today'}>${preview.evaluated != null ? `Looked at ${fmtNum(preview.evaluated)}. ` : ''}${preview.unknown ? `${fmtNum(preview.unknown)} could not be judged. ` : ''}Nothing was stored.<//>
        ${pv && pv.length ? html`<ul class="change-list mt-2">${pv.slice(0, 20).map((f, i) => html`<li key=${i}>${f.asset ? html`<${AssetLink} asset=${f.asset} />` : null}<span class="small wrap-any">${f.message}</span></li>`)}</ul>` : null}</div>` : null}</div></${Dialog}>`;
}

function ViolationDrawer({ id, onClose, onChanged }) {
  const st = useAsync(() => api.get(`${GOV}/violations/${enc(id)}`), [id]);
  const v = st.data;
  const ws = v && v.workspace_id;
  async function ack() { const c = await askText({ title: 'Mark as seen', label: 'Comment (optional)', confirmLabel: 'Mark as seen' }); if (c === null) return; await act(() => api.post(`${GOV}/violations/${enc(id)}/acknowledge`, { comment: c || undefined }), { ok: 'Marked as seen' }); st.reload(); onChanged(); }
  async function reopen() { await act(() => api.post(`${GOV}/violations/${enc(id)}/reopen`, {}), { ok: 'Reopened' }); st.reload(); onChanged(); }
  async function except() {
    const reason = await askText({ title: 'Ask for an exception', label: 'Why is this acceptable?', confirmLabel: 'Send the request', required: true, hint: 'A person with approval rights decides. The violation stays reported until then.' });
    if (!reason) return;
    await act(() => api.post(`${GOV}/changes`, { type: 'policy_exception', subject_type: 'violation', subject_id: id, payload: { reason } }), { ok: 'Request sent for approval' });
  }
  const ev = v && v.evidence;
  const path = ev && ev.path && ev.path.nodes;
  return html`<${Drawer} title=${v ? v.policy.name : 'Violation'} onClose=${onClose} wide=${true}>
    ${st.error && !v ? html`<${ErrorBox} error=${st.error} />` : !v ? html`<${Loading} />` : html`<div class="col gap-4" data-testid="violation-detail">
      <div class="row-wrap"><${Badge} kind=${SEVERITY[v.severity][0]}>${SEVERITY[v.severity][1]}<//><${Badge} kind=${VSTATE[v.status][0]}>${VSTATE[v.status][1]}<//><span class="small muted">first seen <${Ago} ts=${v.first_seen_at} />, last seen <${Ago} ts=${v.last_seen_at} /></span></div>
      <p class="m-0">${v.message}</p>${v.asset ? html`<div><${AssetLink} asset=${v.asset} showType=${true} /> <a class="small" href=${lineageHref(v.asset.id)}>Show lineage</a></div>` : v.product ? html`<div>Data product ${v.product.name}</div>` : null}
      ${path && path.length ? html`<${Card} title="How the data gets there" sub="Along the recorded lineage"><div class="exposure-path">${path.map((n, i) => html`<span key=${i} class="row">${i ? html`<span class="arrow" aria-hidden="true">→</span>` : null}<${AssetLink} asset=${n} /></span>`)}</div></${Card}>`
        : ev && Object.keys(ev).length ? html`<${Card} title="Evidence"><pre class="payload">${JSON.stringify(ev, null, 2)}</pre></${Card}>` : null}
      ${v.exception ? html`<${Banner} kind="info" title="An exception applies">${v.exception.reason || ''}${v.exception.until ? ` Until ${v.exception.until}.` : ''}<//>` : null}
      <div class="row-wrap">${v.status === 'open' && canCurate(ws) ? html`<${Btn} onClick=${ack}>Mark as seen<//>` : null}${(v.status === 'acknowledged' || v.status === 'excepted') && canCurate(ws) ? html`<${Btn} onClick=${reopen}>Reopen<//>` : null}
        ${(v.status === 'open' || v.status === 'acknowledged') && canCurate(ws) ? html`<${Btn} onClick=${except}>Ask for an exception<//>` : null}</div></div>`}</${Drawer}>`;
}

function PoliciesSection() {
  const [view, setView] = useQueryState('view', 'violations');
  const pol = usePaged(`${GOV}/policies`, {}, [], 100);
  const types = useAsync(() => api.get(`${GOV}/policies/rule-types`).then((r) => r.items || []), []);
  const classes = useAsync(() => api.get(`${GOV}/classifications`, { limit: 200 }).then((r) => r.items || []).catch(() => []), []);
  const [status, setStatus] = useState('open,acknowledged');
  const [sev, setSev] = useState('');
  const [policyId, setPolicyId] = useState('');
  const vio = usePaged(`${GOV}/violations`, { status: status === 'any' ? 'any' : status, severity: sev || undefined, policy_id: policyId || undefined }, [status, sev, policyId], 50);
  const [dialog, setDialog] = useState(null);
  const [open, setOpen] = useState(null);
  const [busy, setBusy] = useState(false);
  const mayApprove = canApprove();
  async function evaluate(p) {
    setBusy(true);
    try { const r = p ? await api.post(`${GOV}/policies/${enc(p.id)}/evaluate`, {}) : await api.post(`${GOV}/policies/evaluate`, {}); toast.ok('Checked', p ? `${fmtNum(r.evaluated || 0)} looked at, ${fmtNum(r.opened || 0)} newly reported, ${fmtNum(r.resolved || 0)} resolved.` : 'All enabled policies were checked.'); pol.reload(); vio.reload(); invalidateGovernance(); } catch (e) { toast.bad('Could not check', e && e.message); } finally { setBusy(false); }
  }
  async function remove(p) {
    if (!(await confirmDialog({ title: `Delete ${p.name}?`, body: 'Its reported violations are deleted with it. Nothing else changes.', confirmLabel: 'Delete', danger: true }))) return;
    await act(() => api.del(`${GOV}/policies/${enc(p.id)}`, undefined, { ifMatch: p.etag }), { ok: 'Policy deleted' }); pol.reload(); vio.reload();
  }
  return html`<div class="col gap-3" data-testid="policies">
    <${Banner} kind="info" title="Policies report, they do not block">A policy lists what breaks a rule, such as an unowned table or a report built on uncertified data. Nothing is stopped, changed or hidden because of it. Use the list to decide what to fix.<//>
    <div class="row-wrap"><${Seg} label="Show" options=${[{ value: 'violations', label: 'Violations' }, { value: 'policies', label: 'Policies' }]} value=${view} onChange=${setView} />
      ${mayApprove ? html`<${Btn} size="sm" icon="refresh" busy=${busy} onClick=${() => evaluate(null)}>Check all policies now<//><${Btn} size="sm" kind="primary" icon="plus" disabled=${!types.data} onClick=${() => setDialog({})}>Add a policy<//>` : null}</div>
    ${view === 'violations' ? html`
      <div class="filter-row"><div style="width:220px"><${Select} label="State" value=${status} onChange=${setStatus} options=${[{ value: 'open,acknowledged', label: 'Open and seen' }, { value: 'open', label: 'Open' }, { value: 'resolved', label: 'Resolved' }, { value: 'excepted', label: 'Allowed for now' }, { value: 'any', label: 'Everything' }]} /></div>
        <div style="width:180px"><${Select} label="How serious" value=${sev} onChange=${setSev} placeholder="Any" options=${Object.entries(SEVERITY).map(([value, v]) => ({ value, label: v[1] }))} /></div>
        <div style="min-width:240px"><${Select} label="Policy" value=${policyId} onChange=${setPolicyId} placeholder="All policies" options=${pol.items.map((p) => ({ value: p.id, label: p.name }))} /></div></div>
      ${vio.error && !vio.data ? html`<${ErrorBox} error=${vio.error} onRetry=${vio.reloadLoud} />` : !vio.data ? html`<${Loading} />` : html`<${Card} flush=${true}><${Table} caption="Violations" rows=${vio.items} onRowClick=${(v) => setOpen(v.id)} initialSort=${{ key: 'first_seen_at', dir: -1 }}
        columns=${[{ key: 'severity', label: 'How serious', sort: (v) => ['low', 'medium', 'high', 'critical'].indexOf(v.severity), render: (v) => html`<${Badge} kind=${SEVERITY[v.severity][0]}>${SEVERITY[v.severity][1]}<//>` },
          { key: 'policy', label: 'Rule', sort: (v) => v.policy.name, render: (v) => v.policy.name }, { key: 'asset', label: 'Where', sortable: false, render: (v) => (v.asset ? html`<${AssetLink} asset=${v.asset} />` : v.product ? v.product.name : '') },
          { key: 'message', label: 'What is wrong', sortable: false, render: (v) => html`<span class="small wrap-any">${v.message}</span>` }, { key: 'status', label: 'State', render: (v) => html`<${Badge} kind=${VSTATE[v.status][0]}>${VSTATE[v.status][1]}<//>` },
          { key: 'first_seen_at', label: 'First seen', sort: (v) => v.first_seen_at, render: (v) => html`<${Ago} ts=${v.first_seen_at} />` }]}
        empty=${html`<${Empty} icon="check-circle" title="Nothing reported">No policy is currently broken. If you have no policies yet, add one and preview it first.</${Empty}>`} /></${Card}><${MoreBtn} p=${vio} />`}`
    : pol.error && !pol.data ? html`<${ErrorBox} error=${pol.error} onRetry=${pol.reloadLoud} />` : !pol.data ? html`<${Loading} />` : html`<${Card} flush=${true}><${Table} caption="Policies" rows=${pol.items}
        columns=${[{ key: 'name', label: 'Policy', render: (p) => html`<b>${p.name}</b><div class="small muted">${RULE_NAME[p.rule_type] || p.rule_type}</div>` }, { key: 'severity', label: 'How serious', render: (p) => html`<${Badge} kind=${SEVERITY[p.severity][0]}>${SEVERITY[p.severity][1]}<//>` },
          { key: 'enabled', label: 'State', render: (p) => (p.enabled ? html`<${Badge} kind="ok">Checked regularly<//>` : html`<${Badge}>Paused<//>`) }, { key: 'open_violations', label: 'Open', num: true }, { key: 'last_evaluated_at', label: 'Last checked', render: (p) => html`<${Ago} ts=${p.last_evaluated_at} />` },
          mayApprove ? { key: 'act', label: '', sortable: false, render: (p) => html`<span class="row"><${Btn} size="sm" onClick=${() => evaluate(p)}>Check<//><${Btn} size="sm" onClick=${() => setDialog({ policy: p })}>Edit<//><${Btn} size="sm" kind="ghost" iconOnly icon="trash" label=${`Delete ${p.name}`} onClick=${() => remove(p)} /></span>` } : null].filter(Boolean)}
        empty=${html`<${Empty} icon="shield" title="No policies yet">${mayApprove ? 'Add one to start reporting.' : 'Adding policies needs the approve permission.'}</${Empty}>`} /></${Card}>`}
    ${dialog && types.data ? html`<${PolicyDialog} policy=${dialog.policy} ruleTypes=${types.data} classes=${classes.data || []} onClose=${(r) => { setDialog(null); if (r) { pol.reload(); setView('policies'); } }} />` : null}
    ${open ? html`<${ViolationDrawer} id=${open} onClose=${() => setOpen(null)} onChanged=${() => { vio.reload(); pol.reload(); }} />` : null}</div>`;
}

// ---------------------------------------------------------------------------------------------------------------- exposure
function ExposureSection() {
  const [cls, setCls] = useQueryState('cls');
  const [depth, setDepth] = useState('6');
  const [possible, setPossible] = useState(true);
  const classes = useAsync(() => api.get(`${GOV}/classifications`, { limit: 200 }).then((r) => r.items || []), []);
  const res = useAsync(() => (cls ? api.get(`${GOV}/classifications/${enc(cls)}/exposure`, { depth, include_possible: possible }) : Promise.resolve(null)), [cls, depth, possible]);
  const d = res.data;
  const groups = useMemo(() => { const m = {}; for (const e of (d && d.exposures) || []) (m[e.kind || e.asset.type] ||= []).push(e); return m; }, [d]);
  return html`<div class="col gap-3" data-testid="exposure">
    <p class="m-0">Pick a label to see where data carrying it ends up, following the recorded lineage downstream. This is only as complete as the lineage: anything outside what scans could see is not shown.</p>
    <div class="filter-row"><div style="min-width:260px"><${Select} label="Label" value=${cls} onChange=${setCls} placeholder="Choose a label" options=${(classes.data || []).map((c) => ({ value: c.id, label: c.name }))} /></div>
      <div style="width:160px"><${Select} label="How far" value=${depth} onChange=${setDepth} options=${[2, 3, 4, 6, 8, 10].map((n) => ({ value: String(n), label: `${n} steps` }))} /></div>
      <${Check} label="Include possible exposure" checked=${possible} onChange=${setPossible} /></div>
    ${!cls ? html`<${Empty} icon="route" title="Where does sensitive data end up?">Choose a label such as PII to see the reports, tables and data products downstream of everything labelled with it.</${Empty}>`
      : res.error && !d ? html`<${ErrorBox} error=${res.error} onRetry=${res.reloadLoud} />` : !d ? html`<${Loading} />` : html`<div class="col gap-4">
        <${Banner} kind=${d.exposures.length ? 'warn' : 'info'} title=${d.exposures.length ? `${plural(d.exposures.length, 'place')} downstream of ${plural(d.sources.length, 'labelled source')}` : 'Nothing downstream was found'}>
          ${d.summary && d.summary.reports ? `${plural(d.summary.reports, 'report')}. ` : ''}${d.summary && d.summary.products ? `${plural(d.summary.products, 'data product')}. ` : ''}${d.exposures.length ? 'Possible exposure means lineage says the table feeds this, but it does not prove that this column does.' : 'That does not prove the data stays put: lineage may be missing links.'}<//>
        ${d.truncated ? html`<${Banner} kind="warn" title="The list is cut short">Too many places to follow. Choose a smaller depth or label.<//>` : null}
        ${(d.notes || []).length ? html`<ul class="small muted">${d.notes.map((n, i) => html`<li key=${i}>${n}</li>`)}</ul>` : null}
        <${Card} title="Where the label is" sub="The labelled sources">${d.sources.length ? html`<ul class="change-list">${d.sources.map((s, i) => html`<li key=${i}><${AssetLink} asset=${s.asset} showType=${true} /></li>`)}</ul>` : html`<span class="muted">Nothing carries this label yet.</span>`}</${Card}>
        ${Object.entries(groups).map(([kind, list]) => html`<${Card} key=${kind} title=${`${typeLabel(kind)} (${list.length})`}>${list.map((e, i) => html`<div class="exposure-path" key=${i}>
          <${AssetLink} asset=${e.asset} showType=${false} /><span class="small muted">from</span><${AssetLink} asset=${e.from_asset} />
          ${e.proven === false ? html`<${Badge} kind="warn" title="Lineage links the tables but not this column, so this is possible, not proven">Possible<//>` : html`<${Badge} kind="ok">Proven<//>`}<${ConfBadge} level=${e.confidence_floor || e.confidence} />
          ${e.path && e.path.nodes && e.path.nodes.length > 2 ? html`<details class="grow"><summary class="small">Path (${e.path.nodes.length} steps)</summary><div class="exposure-path">${e.path.nodes.map((n, j) => html`<span key=${j} class="row">${j ? html`<span class="arrow" aria-hidden="true">→</span>` : null}<${AssetLink} asset=${n} /></span>`)}</div></details>` : null}</div>`)}</${Card}>`)}
        ${(d.data_products || []).length ? html`<${Card} title="Data products it reaches"><ul class="change-list">${d.data_products.map((p, i) => html`<li key=${i}><b>${p.product.name}</b> <span class="small muted">through</span> <${AssetLink} asset=${p.via_asset} /></li>`)}</ul></${Card}>` : null}</div>`}</div>`;
}

// ---------------------------------------------------------------------------------------------------------------- owners overview
function OwnersSection() {
  const st = useAsync(() => api.get(`${GOV}/summary`), []);
  const [asset, setAsset] = useState(null);
  const cv = st.data && st.data.coverage;
  return html`<div class="col gap-4" data-testid="owners-section">
    <p class="m-0">An owner is the person who answers for an asset. Owners can be set on a workspace or a table and are inherited by what it contains, so one entry often covers a lot.</p>
    ${st.error && !st.data ? html`<${ErrorBox} error=${st.error} onRetry=${st.reloadLoud} />` : !st.data ? html`<${Loading} />` : html`<div class="grid c2">
      <${Card} title="Coverage"><${Coverage} label="Tables and similar with an owner" pct=${cv && cv.owner_pct} hint=${`${fmtNum((cv && cv.datasets) || 0)} measured`} /></${Card}>
      <${Card} title="Without an owner" sub="Most useful first">${(st.data.top_unowned || []).length ? html`<ul class="change-list">${st.data.top_unowned.map((a) => html`<li key=${a.id}><${AssetLink} asset=${a} showType=${true} /><${Btn} size="sm" onClick=${() => setAsset(a)}>Assign<//></li>`)}</ul>` : html`<span class="muted">Everything measured has an owner.</span>`}</${Card}></div>`}
    <${Card} title="Look at or change one asset's owners"><div style="max-width:420px"><${AssetPick} label="Find an asset" onPick=${setAsset} /></div>
      ${asset ? html`<div class="mt-3"><div class="mb-2"><${AssetLink} asset=${asset} showType=${true} /></div><${OwnersEditor} key=${asset.id} prefix="assets" id=${asset.id} canEdit=${canCurate(asset.workspace_id)} onChange=${() => st.reload()} /></div>` : null}</${Card}></div>`;
}

// ---------------------------------------------------------------------------------------------------------------- the Governance tab of one asset
export function AssetGovernance({ asset }) {
  const id = asset.id;
  const ws = asset.workspace_id;
  const st = useAsync(() => api.get(`${GOV}/assets/${enc(id)}`), [id]);
  const q = useAsync(() => api.get(`${GOV}/assets/${enc(id)}/quality`).catch(() => null), [id]);
  const classes = useAsync(() => api.get(`${GOV}/classifications`, { limit: 200 }).then((r) => r.items || []).catch(() => []), []);
  const domains = useAsync(() => api.get(`${GOV}/domains`, { limit: 500 }).then((r) => r.items || []).catch(() => []), []);
  const mayCurate = canCurate(ws);
  const [pickClass, setPickClass] = useState('');
  const [pickTerm, setPickTerm] = useState('');
  const termSearch = useDebounced(pickTerm, 250);
  const terms = useAsync(() => (termSearch.trim() ? api.get(`${GOV}/glossary/search`, { q: termSearch.trim(), limit: 6 }).then((r) => r.items || []) : Promise.resolve([])), [termSearch]);
  if (st.error && !st.data) return html`<${ErrorBox} error=${st.error} onRetry=${st.reloadLoud} />`;
  if (!st.data) return html`<${Loading} />`;
  const g = st.data;
  const cl = g.classifications || {};
  const reload = () => { st.reload(); invalidateGovernance(); };
  async function addClass() {
    const r = await act(() => api.post(`${GOV}/assets/${enc(id)}/classifications`, { classification: pickClass }), {});
    if (r !== undefined) { toast.ok(r.status === 'suggested' ? 'Suggested. It waits for approval in the review queue' : 'Label added'); setPickClass(''); reload(); }
  }
  async function removeClass(a) { const r = await act(() => api.del(`${GOV}/assets/${enc(id)}/classifications/${enc(a.classification.id)}`), { ok: 'Label removed' }); if (r !== undefined) reload(); }
  async function setDomain(v) { const r = await act(() => api.put(`${GOV}/assets/${enc(id)}/domain`, { domain_id: v || null }), { ok: 'Domain saved' }); if (r !== undefined) reload(); }
  async function addTerm(t) { const r = await act(() => api.post(`${GOV}/assets/${enc(id)}/terms`, { term_id: t.id }), { ok: `Linked to ${t.name}` }); if (r !== undefined) { setPickTerm(''); reload(); } }
  async function removeTerm(t) { const r = await act(() => api.del(`${GOV}/assets/${enc(id)}/terms/${enc(t.id)}`), { ok: 'Unlinked' }); if (r !== undefined) reload(); }
  const dom = g.domain && g.domain.domain;
  const quality = q.data;
  return html`<div class="col gap-4" data-testid="asset-governance">
    <div class="grid c2 top">
      <${Card} title="Business context">
        <div class="col gap-3">
          <div><div class="small muted">Domain</div>${mayCurate ? html`<${Select} label="" aria-label="Domain" value=${dom ? dom.id : ''} onChange=${setDomain} placeholder="None" options=${(domains.data || []).map((d) => ({ value: d.id, label: d.name }))} />${g.domain && g.domain.source !== 'direct' ? html`<div class="small muted">Comes from ${g.domain.source === 'product' ? 'a data product it belongs to' : 'a parent asset'}.</div>` : null}` : html`<div>${dom ? dom.name : html`<span class="muted">None</span>`}</div>`}</div>
          <div><div class="small muted">Data products</div>${(g.products || []).length ? html`<div class="chips">${g.products.map((p) => html`<${Badge} key=${p.id} kind="outline">${p.name}<//>`)}</div>` : html`<span class="muted">Not part of any</span>`}</div>
          <div><div class="small muted">Glossary terms</div>${(g.terms || []).length ? html`<ul class="change-list">${g.terms.map((t) => html`<li key=${t.id}><a href=${governanceHref('glossary', { term: t.id })}>${t.name}</a>${mayCurate ? html`<${Btn} size="sm" kind="ghost" iconOnly icon="x" label=${`Unlink ${t.name}`} onClick=${() => removeTerm(t)} />` : null}</li>`)}</ul>` : html`<span class="muted">None linked</span>`}
            ${(g.pending_term_links || []).length ? html`<div class="small muted mt-1">${plural(g.pending_term_links.length, 'suggested link')} <a href=${governanceHref('review')}>${g.pending_term_links.length === 1 ? 'waits' : 'wait'} for review</a></div>` : null}
            ${mayCurate ? html`<div class="field mt-2"><label for="term-find">Link a term</label><input id="term-find" class="input" type="search" value=${pickTerm} onInput=${(e) => setPickTerm(e.target.value)} placeholder="Search the glossary" autocomplete="off" />${termSearch.trim() && terms.data ? (terms.data.length ? html`<ul class="pick-list">${terms.data.map((r) => html`<li key=${r.term.id}><button type="button" onClick=${() => addTerm(r.term)}>${r.term.name}</button></li>`)}</ul>` : html`<div class="hint">No term found.</div>`) : null}</div>` : null}</div></div></${Card}>
      <${Card} title="Quality" sub="Out of 100, from what is recorded">${quality ? html`<div class="col gap-2" data-testid="gov-quality"><div class="row"><${QualityScore} score=${quality.score} /></div>
        ${(quality.components || []).filter((c) => c.applicable).map((c) => html`<div key=${c.name} class="cov-row"><div>${qualityName(c.name)}${(c.reasons || []).length ? html`<div class="small muted">${c.reasons.map((r) => r.reason).join('; ')}</div>` : null}</div><div class=${'cov-bar ' + (c.fraction >= 0.8 ? '' : c.fraction >= 0.5 ? 'mid' : 'low')} role="presentation"><i style=${`width:${Math.round((c.fraction || 0) * 100)}%`}></i></div><div class="num small">${Math.round(c.points * 10) / 10} of ${Math.round(c.max_points * 10) / 10}</div></div>`)}
        ${(quality.components || []).some((c) => !c.applicable) ? html`<div class="small muted">Not counted for this kind of asset: ${quality.components.filter((c) => !c.applicable).map((c) => qualityName(c.name).toLowerCase()).join(', ')}.</div>` : null}</div>` : html`<span class="muted">No score available.</span>`}</${Card}></div>
    <div class="grid c2 top">
      <${Card} title="Owners and stewards"><${OwnersEditor} prefix="assets" id=${id} canEdit=${mayCurate} onChange=${() => invalidateGovernance()} /></${Card}>
      <${Card} title="Certification"><${CertPanel} prefix="assets" subjectType="asset" id=${id} workspaceId=${ws} onChange=${reload} /></${Card}></div>
    <${Card} title="Labels for sensitive data" sub="Guesses from names are suggestions until a person approves them">
      <div class="col gap-3" data-testid="asset-classifications">
        <div><div class="small muted mb-1">On this asset</div>${(cl.direct || []).length ? html`<ul class="change-list">${cl.direct.map((a) => html`<li key=${a.id}><${ClassBadge} c=${a.classification} status=${a.status === 'suggested' ? 'suggested' : null} />${a.status === 'suggested' ? html`<span class="small muted">suggested${a.confidence != null ? `, ${Math.round(a.confidence * 100)}% sure` : ''}. <a href=${governanceHref('review')}>Review</a></span>` : null}${mayCurate && a.status !== 'rejected' ? html`<${Btn} size="sm" kind="ghost" iconOnly icon="x" label=${`Remove ${a.classification.name}`} onClick=${() => removeClass(a)} />` : null}</li>`)}</ul>` : html`<span class="muted">None</span>`}</div>
        ${(cl.suggestions || []).length ? html`<div><div class="small muted mb-1">Suggested</div><ul class="change-list">${cl.suggestions.map((a, i) => html`<li key=${a.id || i}><${ClassBadge} c=${a.classification} status="suggested" /><span class="small muted">${a.confidence != null ? `${Math.round(a.confidence * 100)}% sure` : ''}</span></li>`)}</ul></div>` : null}
        ${(cl.inherited || []).length ? html`<div><div class="small muted mb-1">Passed on by lineage</div><ul class="change-list">${cl.inherited.map((a, i) => html`<li key=${i}><${ClassBadge} c=${a.classification} /><span class="small muted">from ${a.evidence && a.evidence.from_asset ? html`<${AssetLink} asset=${a.evidence.from_asset} />` : 'upstream'}${a.precision === 'column' ? ' (column level)' : a.precision === 'contains' ? ' (a column inside it)' : ' (table level)'}, ${confLabel(a.confidence_floor || 'derived').toLowerCase()} lineage</span></li>`)}</ul></div>` : null}
        ${(cl.contained || []).length ? html`<div><div class="small muted mb-1">Columns inside it carry</div><div class="chips">${cl.contained.map((c, i) => html`<span key=${i} class="small"><${ClassBadge} c=${c.classification} /> ${c.columns} column${c.columns === 1 ? '' : 's'}</span>`)}</div></div>` : null}
        ${mayCurate ? html`<div class="filter-row"><div style="min-width:220px"><${Select} label="Add a label" value=${pickClass} onChange=${setPickClass} placeholder="Choose" options=${(classes.data || []).map((c) => ({ value: c.id, label: c.name }))} /></div><${Btn} disabled=${!pickClass} onClick=${addClass}>Add<//></div>` : null}</div></${Card}>
    <${Card} title=${`Policy violations (${(g.violations || []).length})`}>${(g.violations || []).length ? html`<ul class="change-list">${g.violations.map((v) => html`<li key=${v.id}><${Badge} kind=${(SEVERITY[v.severity] || ['', ''])[0]}>${(SEVERITY[v.severity] || [0, v.severity])[1]}<//><span class="small wrap-any">${v.message}</span></li>`)}</ul><div class="small muted mt-2">These are reports only. They do not block anything.</div>` : html`<span class="muted">None reported.</span>`}</${Card}>
    ${(g.open_changes || []).length ? html`<${Card} title="Requests waiting"><ul class="change-list">${g.open_changes.map((c) => html`<li key=${c.id}>${CHANGE_LABEL[c.type] || c.type} <a href=${governanceHref('requests')}>see requests</a></li>`)}</ul></${Card}>` : null}</div>`;
}

// ---------------------------------------------------------------------------------------------------------------- the page
export function GovernancePage({ params }) {
  const gov = useGovernance();
  const section = (params && params.section) || '';
  const tabs = GOV_SECTIONS.map((s) => ({ id: s.id || 'overview', label: s.label }));
  const cur = section || 'overview';
  const known = tabs.some((t) => t.id === cur);
  const q = gov.summary && gov.summary.review_queue;
  if (gov.loading) return html`<div class="page"><${PageHead} icon="shield-check" title="Governance" /><${Loading} /></div>`;
  if (!gov.available) return html`<div class="page"><${PageHead} icon="shield-check" title="Governance" sub="Who owns what, what it means, how sensitive it is, and whether it can be trusted." />
    <${Empty} icon="lock" title="Governance is not available">Either this installation does not include governance, or your account is not allowed to read it. Intelligence itself keeps working: <a href="#/intelligence">go back to the overview</a>.</${Empty}></div>`;
  const body = !known ? html`<${Empty} icon="search" title="No such section">Choose one of the tabs above.</${Empty}>` : { overview: OverviewSection, glossary: GlossarySection, domains: DomainsSection, classifications: ClassificationsSection, review: ReviewSection, requests: RequestsSection, policies: PoliciesSection, exposure: ExposureSection, owners: OwnersSection }[cur];
  return html`<div class="page wide"><${PageHead} icon="shield-check" title="Governance" sub="Who owns what, what it means, how sensitive it is, and whether it can be trusted. It observes and reports; it never blocks work." />
    <${Tabs} label="Governance sections" tabs=${tabs.map((t) => (t.id === 'review' && q && q.total ? { ...t, count: q.total } : t))} value=${cur} onChange=${(id) => navigate(governanceHref(id === 'overview' ? '' : id))} />
    <div class="tab-body"><${TabPanel} id=${cur} value=${cur}>${typeof body === 'function' ? html`<${body} />` : body}</${TabPanel}></div></div>`;
}
