// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Notebook side panels and dialogs: run-as-job parameters, session/notebook settings, version history, shortcuts.
import { html, useState, useAsync, useMemo, useEffect } from '../core/h.js';
import { api, can } from '../api/client.js';
import { Btn, Input, Select, Field, Check, Banner, ErrorBox, Loading, Badge, Kv, Tabs, TabPanel, Textarea, Empty, Switch } from '../ui/basic.js';
import { Dialog, Drawer, act, toast, confirmDialog } from '../ui/overlay.js';
import { enc, fmtTime, ago, jsonPretty, titleCase } from '../core/util.js';
import { DiffView } from '../ui/diffview.js';
import { CodeView } from './cm.js';
import { jobHref } from '../core/links.js';

// ---- run as a job with parameters ------------------------------------------------------------------------------------------------------------
function ParamInput({ p, value, onChange }) {
  const t = String(p.type || 'string').toLowerCase();
  const label = html`<span class="pname">${p.name}</span>`;
  const desc = html`<div class="pdesc">${p.description || ''} <span class="muted">(${t}${p.has_default ? `, default ${JSON.stringify(p.default)}` : ''})</span></div>`;
  let input;
  if (t === 'boolean' || t === 'bool') input = html`<${Switch} label=${value ? 'true' : 'false'} checked=${!!value} onChange=${onChange} aria-label=${p.name} />`;
  else if (t === 'number' || t === 'integer' || t === 'int' || t === 'float') input = html`<input class="input mono" type="number" step=${t === 'integer' || t === 'int' ? '1' : 'any'} value=${value ?? ''} aria-label=${p.name} onInput=${(e) => onChange(e.target.value === '' ? null : Number(e.target.value))} />`;
  else if (t === 'list' || t === 'dict' || t === 'object' || t === 'array') input = html`<textarea class="textarea mono" rows="3" aria-label=${p.name} value=${typeof value === 'string' ? value : jsonPretty(value)} onInput=${(e) => { try { onChange(JSON.parse(e.target.value)); } catch { onChange(e.target.value); } }}></textarea>`;
  else input = html`<input class="input mono" value=${value ?? ''} aria-label=${p.name} onInput=${(e) => onChange(e.target.value)} />`;
  return html`<div class="param-row"><div>${label}${desc}</div><div>${input}</div></div>`;
}

export function RunDialog({ nb, close }) {
  const pr = useAsync(() => api.get(`/notebooks/${enc(nb.id)}/parameters`), [nb.id]);
  const [vals, setVals] = useState(null);
  const [preview, setPreview] = useState(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState(null);
  const [name, setName] = useState('');
  const params = pr.data ? pr.data.parameters || [] : [];
  useEffect(() => { if (pr.data && !vals) setVals(Object.fromEntries((pr.data.parameters || []).map((p) => [p.name, p.has_default ? p.default : null]))); }, [pr.data]);
  const eff = () => Object.fromEntries(Object.entries(vals || {}).filter(([, v]) => v !== null && v !== undefined && v !== ''));
  async function doPreview() {
    setErr(null);
    try { setPreview(await api.post(`/notebooks/${enc(nb.id)}/parameters/preview`, { parameters: eff() })); } catch (e) { setErr(e); }
  }
  async function go() {
    setBusy(true); setErr(null);
    try {
      const body = { parameters: eff(), name: name || undefined };
      if (nb.settings && nb.settings.engine) body.engine = nb.settings.engine;
      const job = await api.post(`/notebooks/${enc(nb.id)}/run`, body);
      toast.ok('Notebook submitted as a job');
      close(job);
    } catch (e) { setErr(e); setBusy(false); }
  }
  return html`<${Dialog} title="Run as a job" size="lg" onClose=${() => close(null)}
    footer=${html`<${Btn} onClick=${() => close(null)}>Cancel<//><${Btn} onClick=${doPreview} disabled=${!params.length}>Preview injected cell<//><${Btn} kind="primary" icon="play" busy=${busy} onClick=${go}>Submit job<//>`}>
    <p class="muted mt-0">The saved notebook runs top to bottom on the cluster, independent of this editor and its session. Unsaved edits are saved first.</p>
    <div class="field mb-3"><label for="job-name">Job name (optional)</label><input id="job-name" class="input" value=${name} onInput=${(e) => setName(e.target.value)} placeholder=${nb.name} /></div>
    ${pr.loading ? html`<${Loading} label="Reading parameters" />` : pr.error ? html`<${ErrorBox} error=${pr.error} />` : params.length ? html`<h3>Parameters</h3>
      <p class="small muted mt-0">Values replace the defaults in the cell tagged <span class="mono">parameters</span> (cell ${pr.data.cell_index != null ? pr.data.cell_index + 1 : '–'}). Empty values keep the default.</p>
      ${params.map((p) => html`<${ParamInput} key=${p.name} p=${p} value=${vals ? vals[p.name] : null} onChange=${(v) => setVals((s) => ({ ...s, [p.name]: v }))} />`)}`
      : html`<${Banner} kind="info">This notebook has no parameters. Tag a code cell as <b>parameters</b> to declare some.<//>`}
    ${(pr.data && pr.data.warnings || []).map((w, i) => html`<${Banner} key=${i} kind="warn">${typeof w === 'string' ? w : jsonPretty(w)}<//>`)}
    ${preview ? html`<h3 class="mt-4">Injected cell</h3>${preview.cell ? html`<${CodeView} value=${typeof preview.cell === 'string' ? preview.cell : preview.cell.source || jsonPretty(preview.cell)} />` : html`<div class="muted small">Nothing is injected: the values equal the defaults.</div>`}` : null}
    ${err ? html`<div class="mt-3"><${ErrorBox} error=${err} /></div>` : null}</${Dialog}>`;
}

// ---- settings ---------------------------------------------------------------------------------------------------------------------------------
export function SettingsDrawer({ nb, settings, onChange, onClose, kernel, onRestart }) {
  const matrix = useAsync(() => api.get('/runtimes/matrix').catch(() => ({ families: [] })), []);
  const lakes = useAsync(() => api.get(`/workspaces/${enc(nb.workspace_id)}/items`, { type: 'lakehouse', limit: 200 }).then((r) => r.items || []).catch(() => []), [nb.workspace_id]);
  const envs = useAsync(() => api.get(`/workspaces/${enc(nb.workspace_id)}/items`, { type: 'environment', limit: 200 }).then((r) => r.items || []).catch(() => []), [nb.workspace_id]);
  const pip = useAsync(() => api.get(`/notebooks/${enc(nb.id)}/pip`).catch(() => null), [nb.id]);
  const s = settings || {};
  const set = (patch) => onChange({ ...s, ...patch });
  const fams = (matrix.data && matrix.data.families) || [];
  const fam = (s.runtime && s.runtime.family) || '';
  const famObj = fams.find((f) => f.family === fam);
  const res = s.resources || {};
  const setRes = (k, v) => set({ resources: { ...res, [k]: v === '' || v == null ? undefined : Number(v) } });
  const dl = s.default_lakehouse || {};
  return html`<${Drawer} title="Notebook settings" onClose=${onClose}>
    <div class="nb-settings stack gap-3">
      <${Banner} kind="info">Settings are saved with the notebook. A running session keeps its old settings until you restart it.<//>
      <${Select} label="Engine" value=${s.engine || 'python'} onChange=${(v) => set({ engine: v })} options=${[{ value: 'python', label: 'Python (single process)' }, { value: 'spark', label: 'Spark (PySpark session)' }]} />
      <${Select} label="Runtime family" value=${fam} onChange=${(v) => set({ runtime: v ? { family: v } : undefined })} placeholder="Platform default"
        options=${fams.map((f) => ({ value: f.family, label: `${f.family}  (${f.runtimes.reduce((n, r) => n + (r.ready_nodes || 0), 0)} ready node-runtimes)` }))} />
      ${famObj ? html`<${Select} label="Specific runtime" value=${(s.runtime && s.runtime.runtime_id) || ''} placeholder="Choose automatically" onChange=${(v) => set({ runtime: { family: fam, ...(v ? { runtime_id: v } : {}) } })}
        options=${famObj.runtimes.map((r) => ({ value: r.id, label: `${r.id}  (${r.ready_nodes} node${r.ready_nodes === 1 ? '' : 's'} ready)` }))} hint="Pin one runtime to get identical behaviour on every run." />` : null}
      <${Select} label="Default lakehouse" value=${dl.lakehouse_id || dl.name || ''} placeholder="None" onChange=${(v) => { const l = (lakes.data || []).find((x) => x.id === v); set({ default_lakehouse: v ? { name: l ? l.name : v, ...(l ? { lakehouse_id: l.id } : {}) } : undefined }); }}
        options=${(lakes.data || []).map((l) => ({ value: l.id, label: l.name }))} hint="Relative paths such as Tables/fact_sales resolve in this lakehouse." />
      <${Select} label="Environment" value=${s.environment_id || ''} placeholder="None" onChange=${(v) => set({ environment_id: v || undefined })} options=${(envs.data || []).map((e) => ({ value: e.id, label: e.name }))} hint="Python packages and Spark properties from the environment item." />
      ${(s.engine || 'python') === 'spark' ? html`<fieldset class="fieldset"><legend>Spark resources</legend><div class="form-grid">
        <${Input} label="Driver cores" type="number" min="1" value=${res.driver_cores ?? ''} onInput=${(v) => setRes('driver_cores', v)} />
        <${Input} label="Driver memory (MB)" type="number" min="256" step="256" value=${res.driver_memory_mb ?? ''} onInput=${(v) => setRes('driver_memory_mb', v)} />
        <${Input} label="Executor cores" type="number" min="1" value=${res.executor_cores ?? ''} onInput=${(v) => setRes('executor_cores', v)} />
        <${Input} label="Executor memory (MB)" type="number" min="256" step="256" value=${res.executor_memory_mb ?? ''} onInput=${(v) => setRes('executor_memory_mb', v)} />
        <${Input} label="Max executors" type="number" min="1" value=${res.max_executors ?? ''} onInput=${(v) => setRes('max_executors', v)} /></div></fieldset>` : null}
      <${Input} label="Idle timeout (seconds)" type="number" min="60" value=${s.idle_timeout_seconds ?? ''} onInput=${(v) => set({ idle_timeout_seconds: v === '' ? undefined : Number(v) })} hint="The session is stopped after this long without activity." />
      <div><h3>Package installs</h3>${pip.data ? html`<${Kv} items=${[['Policy', html`<${Badge}>${pip.data.policy}<//>`], ['Installs allowed', pip.data.allowed ? 'Yes' : 'No'], ['Declared requirements', (pip.data.declared_requirements || []).length ? html`<span class="mono">${pip.data.declared_requirements.join(', ')}</span>` : 'None'], ['Notebook commands', (pip.data.commands || []).length ? html`<span class="mono">${pip.data.commands.join('; ')}</span>` : 'No %pip commands']]} />` : html`<span class="muted small">Not available.</span>`}</div>
      ${kernel && kernel.session ? html`<div class="row gap-2"><${Btn} icon="refresh" onClick=${onRestart}>Restart session to apply<//></div>` : null}
    </div></${Drawer}>`;
}

// ---- history ----------------------------------------------------------------------------------------------------------------------------------
export function HistoryDrawer({ nb, onClose, onRestored, canEdit }) {
  const vers = useAsync(() => api.get(`/notebooks/${enc(nb.id)}/versions`).then((r) => r.items || []), [nb.id]);
  const [pair, setPair] = useState(null); // {a, b}
  const [diff, setDiff] = useState({ loading: false });
  const items = vers.data || [];
  useEffect(() => { if (items.length > 1 && !pair) setPair({ a: items[1].version, b: items[0].version }); }, [vers.data]);
  useEffect(() => {
    if (!pair) return undefined;
    let live = true;
    setDiff({ loading: true });
    api.get(`/notebooks/${enc(nb.id)}/diff`, { a: pair.a, b: pair.b, context: 3 }).then((d) => live && setDiff({ data: d })).catch((e) => live && setDiff({ error: e }));
    return () => { live = false; };
  }, [pair && pair.a, pair && pair.b]);
  async function restore(v) {
    if (!(await confirmDialog({ title: `Restore version ${v.version}?`, confirmLabel: 'Restore', body: 'The current content becomes a new version, so nothing is lost. Unsaved edits in this editor are discarded.' }))) return;
    const r = await act(() => api.post(`/notebooks/${enc(nb.id)}/versions/${v.version}/restore`, {}), { ok: `Restored version ${v.version}` });
    if (r) onRestored();
  }
  const d = diff.data;
  return html`<${Drawer} title="Version history" wide onClose=${onClose}>
    ${vers.loading ? html`<${Loading} />` : vers.error ? html`<${ErrorBox} error=${vers.error} />` : html`
    <div class="split-v">
      <ol class="ver-list" aria-label="Versions">${items.map((v) => html`<li key=${v.version} class=${pair && (pair.a === v.version || pair.b === v.version) ? 'sel' : ''}>
        <div class="grow"><b>Version ${v.version}</b> ${v.current ? html`<${Badge} kind="ok">current<//>` : null}<div class="small muted">${v.comment || 'saved'} · <time title=${v.created_at}>${fmtTime(v.created_at)}</time></div></div>
        <div class="row gap-1"><${Btn} size="sm" onClick=${() => setPair({ a: v.version, b: items[0].version })} disabled=${v.current}>Compare to current<//>
        ${canEdit && !v.current ? html`<${Btn} size="sm" onClick=${() => restore(v)}>Restore<//>` : null}</div></li>`)}</ol>
      <div>${!pair ? html`<${Empty} icon="history" title="Only one version">Edit and save the notebook to build a history.<//>` : html`<h3>Version ${pair.a} to version ${pair.b}</h3>
        ${diff.loading ? html`<${Loading} />` : diff.error ? html`<${ErrorBox} error=${diff.error} />` : d ? html`
          <div class="chips mb-3"><${Badge} kind="ok">${d.summary.added} added<//><${Badge} kind="bad">${d.summary.removed} removed<//><${Badge} kind="warn">${d.summary.modified} modified<//><${Badge}>${d.summary.moved} moved<//><${Badge}>${d.summary.unchanged} unchanged<//></div>
          ${d.identical ? html`<div class="muted">The two versions have identical cells.</div>` : html`<${UnifiedText} text=${d.text} />`}
          <ul class="plain mt-3">${(d.cells || []).filter((c) => c.status !== 'unchanged').map((c) => html`<li key=${c.id}><${Badge} kind=${c.status === 'added' ? 'ok' : c.status === 'removed' ? 'bad' : 'warn'}>${c.status}<//> <span class="mono">${c.id}</span> <span class="muted small">${c.cell_type}</span></li>`)}</ul>` : null}`}</div></div>`}</${Drawer}>`;
}
function UnifiedText({ text }) {
  const lines = String(text || '').split('\n');
  return html`<div class="diff" role="group" aria-label="Differences">${lines.map((l, i) => { const c = l[0]; const k = l.startsWith('+++') || l.startsWith('---') ? 'fold' : c === '+' ? 'add' : c === '-' ? 'del' : c === '@' ? 'fold' : ''; return html`<div key=${i} class=${k}><span class="t" aria-hidden="true">${k === 'add' || k === 'del' ? c : ''}</span><span class="sr-only">${k === 'add' ? 'added: ' : k === 'del' ? 'removed: ' : ''}</span><span>${k === 'add' || k === 'del' ? l.slice(1) || ' ' : l || ' '}</span></div>`; })}</div>`;
}

// ---- keyboard shortcuts -----------------------------------------------------------------------------------------------------------------------
export const SHORTCUTS = [
  ['Editing a cell', [['Shift+Enter', 'Run the cell and select the next one'], ['Ctrl+Enter', 'Run the cell and stay'], ['Alt+Enter', 'Run the cell and insert one below'], ['Ctrl+/', 'Comment or uncomment lines'], ['Ctrl+Space', 'Complete from the session'], ['Esc', 'Leave the editor (command mode)']]],
  ['Command mode (cell selected, not typing)', [['Enter', 'Edit the selected cell'], ['Up|Down|K|J', 'Select the previous or next cell'], ['A|B', 'Insert a cell above or below'], ['M|Y', 'Make the cell markdown or code'], ['D D', 'Delete the cell'], ['Alt+Up|Alt+Down', 'Move the cell'], ['Z|Shift+Z', 'Undo or redo a cell change'], ['I I', 'Interrupt the session'], ['Shift+Enter', 'Run and advance']]],
  ['Anywhere', [['Ctrl+S', 'Save now'], ['?', 'Show this list (command mode)']]],
];
export function ShortcutsDialog({ close }) {
  return html`<${Dialog} title="Keyboard shortcuts" onClose=${close} footer=${html`<${Btn} kind="primary" onClick=${close}>Close<//>`}>
    ${SHORTCUTS.map(([h2, rows]) => html`<h3 key=${h2}>${h2}</h3><dl class="sc-grid">${rows.map(([k, d]) => html`<dt key=${k}>${k.split('|').map((x, i) => html`${i ? ' or ' : ''}<kbd>${x}</kbd>`)}</dt><dd>${d}</dd>`)}</dl>`)}</${Dialog}>`;
}
