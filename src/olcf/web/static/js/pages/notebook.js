// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Notebook editor: cells (CodeMirror), live session over WebSocket, outputs, autosave with ETag, undo, drag reorder, params, settings, history, export.
import { html, useState, useEffect, useRef, useCallback, Component, useLayoutEffect } from '../core/h.js';
import { api, can, ApiError, describeError } from '../api/client.js';
import { Btn, Banner, ErrorBox, Loading, Light, Badge, Empty } from '../ui/basic.js';
import { Icon } from '../ui/icons.js';
import { MenuButton, act, toast, confirmDialog, openOverlay, promptDialog } from '../ui/overlay.js';
import { enc, hex, fmtClock, downloadBlob, ago, titleCase, clone, copyText } from '../core/util.js';
import { jobHref, wsHref } from '../core/links.js';
import { CodeEditor } from '../notebook/cm.js';
import { Outputs } from '../notebook/outputs.js';
import { Markdown } from '../notebook/markdown.js';
import { Kernel } from '../notebook/kernel.js';
import { RunDialog, SettingsDrawer, HistoryDrawer, ShortcutsDialog } from '../notebook/panels.js';

const newId = () => 'c-' + hex(8);
const mkCell = (type = 'code', source = '') => ({ id: newId(), cell_type: type, source, metadata: {}, ...(type === 'code' ? { outputs: [], execution_count: null } : {}) });
const normCell = (c) => ({ id: c.id || newId(), cell_type: c.cell_type || 'code', source: Array.isArray(c.source) ? c.source.join('') : c.source || '', metadata: c.metadata || {}, ...(c.cell_type === 'code' || !c.cell_type ? { outputs: c.outputs || [], execution_count: c.execution_count ?? null } : {}) });
const toWire = (c) => (c.cell_type === 'code' ? { id: c.id, cell_type: 'code', source: c.source, metadata: c.metadata || {}, outputs: c.outputs || [], execution_count: c.execution_count ?? null } : { id: c.id, cell_type: c.cell_type, source: c.source, metadata: c.metadata || {} });
const hasTag = (c, t) => ((c.metadata && c.metadata.tags) || []).includes(t);
const jup = (c, k) => !!(c.metadata && c.metadata.jupyter && c.metadata.jupyter[k]);

const KSTATE = {
  none: { light: 'unknown', text: 'No session' }, connecting: { light: 'busy', text: 'Connecting' }, starting: { light: 'degraded', text: 'Starting' }, idle: { light: 'ok', text: 'Idle' },
  busy: { light: 'busy', text: 'Busy' }, dead: { light: 'down', text: 'Ended' }, reconnecting: { light: 'degraded', text: 'Reconnecting' },
};

// ---------------------------------------------------------------------------------------------------------------------------------------------
class CellView extends Component {
  shouldComponentUpdate(n) {
    const o = this.props;
    return n.cell !== o.cell || n.sel !== o.sel || n.editing !== o.editing || n.state !== o.state || n.index !== o.index || n.total !== o.total || n.drop !== o.drop || n.dragging !== o.dragging || n.canEdit !== o.canEdit || n.hint !== o.hint;
  }
  render(p) { return html`<${CellInner} ...${p} />`; }
}

function CellInner({ cell, index, total, sel, editing, state, drop, dragging, canEdit, ctl, hint }) {
  const root = useRef();
  const isCode = cell.cell_type === 'code';
  const isMd = cell.cell_type === 'markdown';
  const srcHidden = jup(cell, 'source_hidden');
  const outHidden = jup(cell, 'outputs_hidden');
  useEffect(() => { ctl.roots.set(cell.id, root.current); return () => ctl.roots.delete(cell.id); }, [cell.id]);
  useEffect(() => {
    const el = root.current;
    const f = () => { requestAnimationFrame(() => root.current && root.current.focus()); };
    el.addEventListener('cm-escape', f);
    return () => el.removeEventListener('cm-escape', f);
  }, []);
  const onKey = (e) => { if (e.target === e.currentTarget) ctl.commandKey(e, cell); };
  const run = (how) => ctl.runCell(cell.id, how);
  const countLabel = state === 'running' ? '[*]' : state === 'queued' ? '[…]' : isCode ? `[${cell.execution_count ?? ' '}]` : '';
  const hasErr = isCode && (cell.outputs || []).some((o) => o.output_type === 'error');
  const label = `${isCode ? 'Code' : isMd ? 'Markdown' : 'Raw'} cell ${index + 1} of ${total}${state ? ', ' + state : ''}`;
  return html`<div class=${`cell ${cell.cell_type}${sel ? ' sel' : ''}${editing ? ' editing' : ' cmd'}${state === 'running' ? ' running' : ''}${dragging ? ' dragging' : ''}${drop === 'before' ? ' drop-before' : ''}${drop === 'after' ? ' drop-after' : ''}`}
      ref=${root} tabindex="0" role="group" aria-label=${label} data-cell=${cell.id} data-state=${state || ''} onFocus=${(e) => { if (e.target === e.currentTarget) ctl.select(cell.id, false); }} onMouseDown=${(e) => { if (!e.target.closest('.cm-host,button,a,input,select,textarea')) ctl.select(cell.id, false); }} onKeyDown=${onKey}
      onDragOver=${(e) => ctl.dragOver(e, cell.id)} onDrop=${(e) => ctl.drop(e, cell.id)} onDragLeave=${() => ctl.dragLeave(cell.id)}>
    <div class="cell-gutter">
      ${canEdit ? html`<button class="handle" draggable="true" aria-label=${`Move cell ${index + 1}. Drag, or use Alt+Up and Alt+Down.`} title="Drag to reorder" onDragStart=${(e) => ctl.dragStart(e, cell.id, root.current)} onDragEnd=${() => ctl.dragEnd()}><${Icon} name="grip" size="sm" /></button>` : null}
      ${isCode ? html`<button class="runbtn" aria-label=${state === 'running' ? 'Interrupt' : `Run cell ${index + 1}`} title=${state === 'running' ? 'Interrupt' : 'Run cell (Ctrl+Enter)'} disabled=${!canEdit && !can('session.use')} onClick=${() => (state === 'running' ? ctl.interrupt() : run('stay'))}><${Icon} name=${state === 'running' ? 'stop' : 'play'} size="sm" /></button>` : null}
      <span class=${'count' + (state === 'running' ? ' run' : state === 'queued' ? ' queued' : hasErr ? ' err' : '')} aria-hidden="true">${countLabel}</span>
    </div>
    <div class="cell-main">
      ${hasTag(cell, 'parameters') || hasTag(cell, 'injected-parameters') ? html`<div class="cell-tags"><${Badge} kind="info" title="Run-as-job parameters replace the values in this cell">parameters<//></div>` : null}
      ${isCode && srcHidden ? html`<div class="cell-hidden-input"><${Btn} size="sm" kind="ghost" icon="chevron-right" onClick=${() => ctl.toggleMeta(cell.id, 'source_hidden')}>Show code<//><span class="muted">${(cell.source.split('\n')[0] || '').slice(0, 80)}</span></div>`
        : isMd && !editing ? html`<div class=${'md-cell' + (!cell.source.trim() ? ' empty' : '')} onDblClick=${() => canEdit && ctl.edit(cell.id)} title=${canEdit ? 'Double-click to edit' : undefined}>${cell.source.trim() ? html`<${Markdown} source=${cell.source} />` : 'Empty markdown cell. Double-click to edit.'}</div>`
        : html`<${CodeEditor} value=${cell.source} mode=${isCode ? 'python' : cell.cell_type === 'raw' ? 'text' : 'markdown'} readOnly=${!canEdit} onChange=${(v) => ctl.edited(cell.id, v)} onRun=${run} autofocus=${editing && !isCode}
          hint=${isCode ? hint : undefined} onFocus=${() => ctl.focusEditor(cell.id)} onBlur=${() => ctl.blurEditor(cell.id)} editorRef=${ctl.editorRef(cell.id)} placeholder=${isCode ? 'Write code. Shift+Enter runs it.' : 'Write markdown.'} label=${`${label} source`} />`}
      ${state === 'running' ? html`<div class="cell-run-line" aria-hidden="true"></div>` : null}
      ${isCode ? html`<${Outputs} outputs=${cell.outputs} collapsed=${outHidden} onToggle=${() => ctl.toggleMeta(cell.id, 'outputs_hidden')} />` : null}
    </div>
    <div class="cell-ctl" role="toolbar" aria-label=${`Cell ${index + 1} actions`}>
      ${isCode ? html`<${Btn} size="sm" iconOnly icon=${state === 'running' ? 'stop' : 'play'} label=${state === 'running' ? 'Interrupt' : 'Run cell'} onClick=${() => (state === 'running' ? ctl.interrupt() : run('stay'))} />` : null}
      ${canEdit ? html`<${Btn} size="sm" iconOnly icon="arrow-up" label="Move cell up" disabled=${index === 0} onClick=${() => ctl.move(cell.id, -1)} />
      <${Btn} size="sm" iconOnly icon="arrow-down" label="Move cell down" disabled=${index === total - 1} onClick=${() => ctl.move(cell.id, 1)} />
      <${Btn} size="sm" iconOnly icon="plus" label="Add a code cell below" onClick=${() => ctl.insert(cell.id, 'below', 'code')} />
      <${Btn} size="sm" iconOnly icon="trash" label="Delete cell" onClick=${() => ctl.remove(cell.id)} />` : null}
      <${MenuButton} label="More cell actions" items=${[
        isCode && { label: 'Run cells above', icon: 'play', onClick: () => ctl.runRange(cell.id, 'above') },
        isCode && { label: 'Run this and cells below', icon: 'play-all', onClick: () => ctl.runRange(cell.id, 'below') },
        { sep: true },
        canEdit && { label: isCode ? 'Convert to markdown' : 'Convert to code', icon: 'code', onClick: () => ctl.convert(cell.id, isCode ? 'markdown' : 'code') },
        canEdit && { label: 'Insert code cell above', icon: 'plus', onClick: () => ctl.insert(cell.id, 'above', 'code') },
        canEdit && { label: 'Insert markdown below', icon: 'plus', onClick: () => ctl.insert(cell.id, 'below', 'markdown') },
        canEdit && { label: 'Duplicate', icon: 'copy', onClick: () => ctl.duplicate(cell.id) },
        isCode && canEdit && { label: hasTag(cell, 'parameters') ? 'Remove parameters tag' : 'Tag as parameters cell', icon: 'tag', onClick: () => ctl.toggleParams(cell.id) },
        isCode && { label: srcHidden ? 'Show code' : 'Hide code', icon: 'eye-off', onClick: () => ctl.toggleMeta(cell.id, 'source_hidden') },
        isCode && (cell.outputs || []).length > 0 && { label: outHidden ? 'Show output' : 'Hide output', icon: 'eye', onClick: () => ctl.toggleMeta(cell.id, 'outputs_hidden') },
        isCode && canEdit && (cell.outputs || []).length > 0 && { label: 'Clear output', icon: 'recycle', onClick: () => ctl.clearOutputs(cell.id) },
      ]} />
    </div></div>`;
}

// ---------------------------------------------------------------------------------------------------------------------------------------------
export function NotebookPage({ params }) {
  const id = params.id;
  const [load, setLoad] = useState({ loading: true });
  const [doc, setDoc] = useState(null);
  const [cells, setCells] = useState([]);
  const [sel, setSel] = useState(null);
  const [editing, setEditing] = useState(null);
  const [exec, setExec] = useState({});
  const [kst, setKst] = useState({ state: 'none' });
  const [save, setSave] = useState({ state: 'saved' });
  const [panel, setPanel] = useState(null); // 'settings' | 'history'
  const [drag, setDrag] = useState(null); // {id, over, pos}
  const [undoN, setUndoN] = useState({ u: 0, r: 0 });
  const [conflict, setConflict] = useState(null);
  const [wsName, setWsName] = useState('');
  const [title, setTitle] = useState('');
  const [runErr, setRunErr] = useState(null);
  const [workers, setWorkers] = useState({});

  const R = useRef({ cells: [], doc: null, etag: null, dirty: false, saving: false, again: false, timer: null, undo: [], redo: [], queue: [], inflight: null, lastKey: '', lastKeyAt: 0, editors: new Map(), roots: new Map(), exec: {}, sel: null, dragId: null, openRun: false, stopOnError: true, seenAccepted: new Set() });
  const r = R.current;
  const canEdit = doc ? can('item.update', doc.workspace_id) : false;
  const canRun = doc ? can('session.use', doc.workspace_id) || canEdit : false;
  r.canEdit = canEdit; r.canRun = canRun;

  const setCellsBoth = useCallback((fn) => { const next = typeof fn === 'function' ? fn(r.cells) : fn; r.cells = next; setCells(next); }, []);
  const patchCell = useCallback((cid, patch) => setCellsBoth((cs) => cs.map((c) => (c.id === cid ? (typeof patch === 'function' ? patch(c) : { ...c, ...patch }) : c))), []);
  const setExecFor = useCallback((cid, v) => { const e = { ...r.exec }; if (v) e[cid] = v; else delete e[cid]; r.exec = e; setExec(e); }, []);

  // ---- loading --------------------------------------------------------------------------------------------------------------------------
  const loadDoc = useCallback(async () => {
    setLoad({ loading: true });
    try {
      const { data, etag } = await api.full('GET', `/notebooks/${enc(id)}/content`, { query: { outputs: true } });
      r.etag = data.etag || (etag || '').replace(/"/g, '');
      const cs = (data.cells || []).map(normCell);
      r.doc = data; R.current.settings = data.settings || {}; r.dirty = false; r.undo = []; r.redo = [];
      setUndoN({ u: 0, r: 0 });
      setDoc({ id: data.id, workspace_id: data.workspace_id, name: data.name, version: data.version, settings: data.settings || {}, metadata: data.metadata || {}, nbformat: data.nbformat, minor: data.nbformat_minor });
      setTitle(data.name);
      setCellsBoth(cs);
      setSel(cs[0] ? cs[0].id : null);
      setSave({ state: 'saved', at: new Date() });
      setConflict(null);
      setLoad({});
      api.get(`/workspaces/${enc(data.workspace_id)}`).then((w) => setWsName(w.display_name || w.name)).catch(() => {});
      return data;
    } catch (e) { setLoad({ error: e }); return null; }
  }, [id]);

  // ---- kernel ---------------------------------------------------------------------------------------------------------------------------
  const kernel = useRef(null);
  if (!kernel.current) {
    kernel.current = new Kernel({
      onState: (state, info) => {
        setKst({ state, session: info.session, detail: info.detail, spark: info.spark });
        if (state === 'idle') {
          // a cell that was running when the connection dropped never receives its reply: release it once the kernel is idle again
          if (r.inflight && r.seenAccepted.has(r.inflight) && r.reconnected) { setExecFor(r.inflight, null); r.inflight = null; }
          r.reconnected = false;
          pump();
        }
        if (state === 'reconnecting') r.reconnected = true;
        if (state === 'dead' || state === 'none') { if (r.inflight) { setExecFor(r.inflight, null); r.inflight = null; } flushQueue(); }
      },
      onOutput: (cid, output, meta) => {
        if (!r.cells.some((c) => c.id === cid)) return;
        if (output.output_type === 'clear_output') { patchCell(cid, { outputs: [] }); return; }
        patchCell(cid, (c) => ({ ...c, outputs: meta.replay && meta.firstReplay ? [output] : [...(c.outputs || []), output] }));
        if (!meta.replay) markDirty(8000);
      },
      onAccepted: (cid) => { r.seenAccepted.add(cid); },
      onReply: (cid, m) => {
        if (r.cells.some((c) => c.id === cid)) patchCell(cid, (c) => ({ ...c, execution_count: m.execution_count ?? c.execution_count }));
        if (r.inflight === cid) r.inflight = null;
        r.seenAccepted.delete(cid);
        setExecFor(cid, null);
        if (m.status !== 'ok' && r.stopOnError) flushQueue();
        if (!m.replay) markDirty(3000);
        pump();
      },
      onReplayDone: () => { /* outputs of the last run are on screen */ },
    });
  }
  const K = kernel.current;

  function flushQueue() { const q = r.queue; r.queue = []; q.forEach((cid) => setExecFor(cid, null)); }
  function pump() {
    if (r.inflight || !r.queue.length || K.state !== 'idle') return;
    const cid = r.queue.shift();
    const cell = r.cells.find((c) => c.id === cid);
    if (!cell) { pump(); return; }
    r.inflight = cid;
    setExecFor(cid, 'running');
    patchCell(cid, { outputs: [] });
    K.execute(cid, cell.source);
  }
  async function startKernel() {
    const s = (r.doc && r.doc.settings) || {};
    const cur = (R.current.settings) || s;
    const body = {};
    for (const k of ['engine', 'runtime', 'resources', 'default_lakehouse', 'environment_id', 'idle_timeout_seconds']) if (cur[k] !== undefined && cur[k] !== null) body[k] = cur[k];
    if (r.dirty) await saveNow();
    setRunErr(null);
    return K.start(id, body);
  }
  async function ensureAndPump() {
    if (K.state === 'none' || K.state === 'dead') {
      try { await startKernel(); } catch (e) { flushQueue(); setRunErr(e); return; }
    }
    pump();
  }
  const attach = useCallback(async (d) => { try { await K.attach(id, d.workspace_id); } catch { /* no session */ } }, [id]);

  // ---- autosave -------------------------------------------------------------------------------------------------------------------------
  function markDirty(delay = 2200) {
    if (!r.canEdit) return;
    r.dirty = true;
    setSave((s) => (s.state === 'conflict' || s.state === 'saving' ? s : { state: 'dirty' }));
    clearTimeout(r.timer);
    r.timer = setTimeout(() => saveNow(), delay);
  }
  async function saveNow({ overwrite, comment } = {}) {
    clearTimeout(r.timer);
    if (!r.doc || (!r.dirty && !overwrite)) return true;
    if (r.saving) { r.again = true; return false; }
    if (R.current.conflict && !overwrite) return false;
    r.saving = true;
    setSave((s) => ({ ...s, state: 'saving' }));
    const settings = R.current.settings || r.doc.settings;
    try {
      const body = { cells: r.cells.map(toWire), metadata: r.doc.metadata, settings, comment: comment || 'saved from the editor' };
      const { data } = await api.full('PUT', `/notebooks/${enc(id)}/content`, { body, ifMatch: overwrite ? undefined : r.etag });
      r.etag = data.etag || r.etag; r.doc = { ...r.doc, ...data, cells: undefined }; r.dirty = false; R.current.conflict = false;
      setSave({ state: 'saved', at: new Date() }); setConflict(null);
      setDoc((d) => ({ ...d, version: data.version, settings: data.settings || d.settings }));
      return true;
    } catch (e) {
      if (e instanceof ApiError && e.status === 412) { R.current.conflict = true; setConflict(e); setSave({ state: 'conflict' }); } else setSave({ state: 'error', error: e });
      return false;
    } finally {
      r.saving = false;
      if (r.again) { r.again = false; if (r.dirty) setTimeout(() => saveNow(), 50); }
    }
  }

  useEffect(() => { if (kst.session && kst.session.worker_id && !workers[kst.session.worker_id]) api.get('/workers').then((w) => setWorkers(Object.fromEntries((w.items || []).map((x) => [x.id, x.name])))).catch(() => {}); }, [kst.session && kst.session.worker_id]);

  // ---- lifecycle ------------------------------------------------------------------------------------------------------------------------
  useEffect(() => {
    let alive = true;
    loadDoc().then((d) => { if (alive && d) attach(d); });
    const beforeUnload = (e) => { if (r.dirty) { e.preventDefault(); e.returnValue = ''; } };
    const keys = (e) => { if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 's') { e.preventDefault(); saveNow({ comment: 'saved' }).then((ok) => ok && toast.ok('Notebook saved')); } };
    window.addEventListener('beforeunload', beforeUnload);
    document.addEventListener('keydown', keys);
    return () => {
      alive = false;
      window.removeEventListener('beforeunload', beforeUnload); document.removeEventListener('keydown', keys);
      if (r.dirty && !R.current.conflict) saveNow();
      clearTimeout(r.timer);
      K.dispose();
    };
  }, [id]);

  // ---- cell operations ------------------------------------------------------------------------------------------------------------------
  const pushUndo = () => { r.undo.push(r.cells); if (r.undo.length > 60) r.undo.shift(); r.redo = []; setUndoN({ u: r.undo.length, r: 0 }); };
  const structural = (fn) => { if (!r.canEdit) return; pushUndo(); setCellsBoth(fn); markDirty(); };
  const idx = (cid) => r.cells.findIndex((c) => c.id === cid);
  const focusCell = (cid) => requestAnimationFrame(() => { const el = r.roots.get(cid); if (el) { el.focus(); el.scrollIntoView({ block: 'nearest' }); } });

  const ctl = useRef(null);
  if (!ctl.current) ctl.current = {
    roots: r.roots,
    editorRef: (cid) => ({ get current() { return r.editors.get(cid) || null; }, set current(v) { if (v) r.editors.set(cid, v); else r.editors.delete(cid); } }),
    select(cid, edit) { r.sel = cid; setSel(cid); if (!edit) setEditing((e) => (e === cid ? e : null)); },
    focusEditor(cid) { r.sel = cid; setSel(cid); setEditing(cid); },
    blurEditor(cid) { setEditing((e) => (e === cid ? null : e)); },
    edit(cid) { r.sel = cid; setSel(cid); setEditing(cid); const cm = r.editors.get(cid); if (cm) cm.focus(); },
    edited(cid, v) { patchCell(cid, { source: v }); markDirty(); },
    insert(cid, where, type) {
      const c = mkCell(type);
      structural((cs) => { const i = cs.findIndex((x) => x.id === cid); const at = i < 0 ? cs.length : where === 'above' ? i : i + 1; return [...cs.slice(0, at), c, ...cs.slice(at)]; });
      r.sel = c.id; setSel(c.id);
      if (type === 'markdown') { setEditing(c.id); } else requestAnimationFrame(() => { const cm = r.editors.get(c.id); if (cm) cm.focus(); else setTimeout(() => { const m = r.editors.get(c.id); if (m) m.focus(); }, 300); });
      return c.id;
    },
    async remove(cid) {
      const c = r.cells.find((x) => x.id === cid);
      if (c && c.source.trim().split('\n').length > 6 && !(await confirmDialog({ title: 'Delete this cell?', danger: true, confirmLabel: 'Delete', body: 'You can undo this with the Undo button or Z.' }))) return;
      const i = idx(cid);
      structural((cs) => cs.filter((x) => x.id !== cid));
      const nxt = r.cells[Math.min(i, r.cells.length - 1)];
      if (nxt) { r.sel = nxt.id; setSel(nxt.id); focusCell(nxt.id); } else { r.sel = null; setSel(null); }
    },
    move(cid, d) {
      const i = idx(cid); const j = i + d;
      if (i < 0 || j < 0 || j >= r.cells.length) return;
      structural((cs) => { const a = [...cs]; [a[i], a[j]] = [a[j], a[i]]; return a; });
      focusCell(cid);
    },
    duplicate(cid) { const c = r.cells.find((x) => x.id === cid); if (!c) return; const d = { ...clone(c), id: newId(), outputs: c.cell_type === 'code' ? [] : undefined, execution_count: null }; structural((cs) => { const i = cs.findIndex((x) => x.id === cid); return [...cs.slice(0, i + 1), d, ...cs.slice(i + 1)]; }); },
    convert(cid, type) {
      structural((cs) => cs.map((c) => (c.id !== cid ? c : type === 'code' ? { ...c, cell_type: 'code', outputs: [], execution_count: null } : { id: c.id, cell_type: type, source: c.source, metadata: c.metadata })));
      setEditing(null);
    },
    toggleParams(cid) { structural((cs) => cs.map((c) => { if (c.id !== cid) return c; const tags = new Set((c.metadata && c.metadata.tags) || []); if (tags.has('parameters')) tags.delete('parameters'); else tags.add('parameters'); return { ...c, metadata: { ...c.metadata, tags: [...tags] } }; })); },
    toggleMeta(cid, key) { patchCell(cid, (c) => ({ ...c, metadata: { ...c.metadata, jupyter: { ...(c.metadata && c.metadata.jupyter), [key]: !jup(c, key) } } })); markDirty(); },
    clearOutputs(cid) { patchCell(cid, { outputs: [] }); markDirty(); },
    runCell(cid, how) {
      const c = r.cells.find((x) => x.id === cid);
      if (!c) return;
      if (c.cell_type !== 'code') { setEditing(null); focusCell(cid); if (how === 'advance') { const n = r.cells[idx(cid) + 1]; if (n) { r.sel = n.id; setSel(n.id); focusCell(n.id); } } return; }
      enqueue([cid]);
      if (how === 'advance') { const i = idx(cid); const n = r.cells[i + 1]; if (n) { r.sel = n.id; setSel(n.id); focusCell(n.id); } else { const nid = ctl.current.insert(cid, 'below', 'code'); r.sel = nid; } }
      if (how === 'insert') ctl.current.insert(cid, 'below', 'code');
    },
    runRange(cid, which) { const i = idx(cid); enqueue(which === 'above' ? r.cells.slice(0, i).map((c) => c.id) : r.cells.slice(i).map((c) => c.id)); },
    interrupt() { r.queue.forEach((c) => setExecFor(c, null)); r.queue = []; K.interrupt().catch((e) => toast.bad('Could not interrupt', describeError(e).title)); },
    dragStart(e, cid, el) { r.dragId = cid; e.dataTransfer.effectAllowed = 'move'; e.dataTransfer.setData('text/plain', cid); try { e.dataTransfer.setDragImage(el, 20, 20); } catch { /* optional */ } setDrag({ id: cid }); },
    dragOver(e, cid) {
      if (!r.dragId || r.dragId === cid) return;
      e.preventDefault(); e.dataTransfer.dropEffect = 'move';
      const b = e.currentTarget.getBoundingClientRect();
      const pos = e.clientY < b.top + b.height / 2 ? 'before' : 'after';
      setDrag((d) => (d && d.over === cid && d.pos === pos ? d : { id: r.dragId, over: cid, pos }));
    },
    dragLeave() { /* the next dragover decides */ },
    drop(e, cid) {
      e.preventDefault();
      const from = r.dragId; r.dragId = null;
      const b = e.currentTarget.getBoundingClientRect();
      const pos = e.clientY < b.top + b.height / 2 ? 'before' : 'after';
      setDrag(null);
      if (!from || from === cid) return;
      structural((cs) => { const m = cs.find((x) => x.id === from); const rest = cs.filter((x) => x.id !== from); const i = rest.findIndex((x) => x.id === cid); const at = pos === 'before' ? i : i + 1; return [...rest.slice(0, at), m, ...rest.slice(at)]; });
      focusCell(from);
    },
    dragEnd() { r.dragId = null; setDrag(null); },
    commandKey(e, cell) {
      const k = e.key; const cid = cell.id; const i = idx(cid);
      const plain = !e.ctrlKey && !e.metaKey && !e.altKey;
      const now = Date.now();
      const twice = (ch) => { const ok = r.lastKey === ch && now - r.lastKeyAt < 900; r.lastKey = ok ? '' : ch; r.lastKeyAt = now; return ok; };
      if (k === 'Enter' && e.shiftKey) { e.preventDefault(); ctl.current.runCell(cid, 'advance'); } else if (k === 'Enter' && (e.ctrlKey || e.metaKey)) { e.preventDefault(); ctl.current.runCell(cid, 'stay'); } else if (k === 'Enter' && e.altKey) { e.preventDefault(); ctl.current.runCell(cid, 'insert'); } else if (k === 'Enter' && plain) { e.preventDefault(); if (r.canEdit) ctl.current.edit(cid); } else if ((k === 'ArrowUp' || k === 'k') && plain) { e.preventDefault(); const n = r.cells[i - 1]; if (n) { r.sel = n.id; setSel(n.id); focusCell(n.id); } } else if ((k === 'ArrowDown' || k === 'j') && plain) { e.preventDefault(); const n = r.cells[i + 1]; if (n) { r.sel = n.id; setSel(n.id); focusCell(n.id); } } else if (k === 'ArrowUp' && e.altKey) { e.preventDefault(); ctl.current.move(cid, -1); } else if (k === 'ArrowDown' && e.altKey) { e.preventDefault(); ctl.current.move(cid, 1); } else if (!r.canEdit) {
        if (k === '?') openOverlay(({ close }) => html`<${ShortcutsDialog} close=${close} />`);
      } else if (k === 'a' && plain) { e.preventDefault(); ctl.current.insert(cid, 'above', 'code'); } else if (k === 'b' && plain) { e.preventDefault(); ctl.current.insert(cid, 'below', 'code'); } else if (k === 'm' && plain) { ctl.current.convert(cid, 'markdown'); } else if (k === 'y' && plain) { ctl.current.convert(cid, 'code'); } else if (k === 'd' && plain) { if (twice('d')) ctl.current.remove(cid); } else if (k === 'i' && plain) { if (twice('i')) ctl.current.interrupt(); } else if (k === 'z' && plain) { undo(); } else if (k === 'Z' && !e.ctrlKey) { redo(); } else if (k === '?') { openOverlay(({ close }) => html`<${ShortcutsDialog} close=${close} />`); }
    },
  };
  const C = ctl.current;
  const enqueue = (ids) => {
    const runnable = ids.filter((cid) => { const c = r.cells.find((x) => x.id === cid); return c && c.cell_type === 'code' && c.source.trim() && !r.exec[cid]; });
    if (!runnable.length) { if (ids.length) toast.info('Nothing to run', 'The selected cells are empty or already queued.'); return; }
    if (!r.canRun) { toast.bad('Not permitted', 'Your role cannot run code in this workspace.'); return; }
    r.queue.push(...runnable);
    runnable.forEach((cid) => setExecFor(cid, 'queued'));
    ensureAndPump();
  };
  function undo() { const prev = r.undo.pop(); if (!prev) return; r.redo.push(r.cells); setCellsBoth(prev); setUndoN({ u: r.undo.length, r: r.redo.length }); markDirty(); }
  function redo() { const nxt = r.redo.pop(); if (!nxt) return; r.undo.push(r.cells); setCellsBoth(nxt); setUndoN({ u: r.undo.length, r: r.redo.length }); markDirty(); }

  // ---- completion from the session -------------------------------------------------------------------------------------------------------
  const hintFn = useRef((cm, cb) => {
    if (!K.ready) { cb(null); return; }
    const cur = cm.getCursor();
    const code = cm.getValue();
    const off = cm.indexFromPos(cur);
    K.complete(code, off).then((m) => {
      if (!m) { cb(null); return; }
      const raw = m.matches || [];
      const start = m.cursor_start != null ? m.cursor_start : null;
      const list = raw.map((x) => (typeof x === 'string' ? x : x.text || x.label || String(x)));
      if (!list.length) { cb(null); return; }
      const word = cm.findWordAt(cur);
      const from = start != null ? cm.posFromIndex(start) : { line: cur.line, ch: Math.max(0, cur.ch - (/[\w]*$/.exec(cm.getLine(cur.line).slice(0, cur.ch)) || [''])[0].length) };
      cb({ list, from, to: m.cursor_end != null ? cm.posFromIndex(m.cursor_end) : cur });
    });
  });
  useEffect(() => { hintFn.current.async = true; }, []);

  // ---- toolbar actions ------------------------------------------------------------------------------------------------------------------
  const runAll = () => enqueue(r.cells.map((c) => c.id));
  const restart = async () => { if (!K.session) { toast.info('There is no session to restart'); return; } r.queue = []; Object.keys(r.exec).forEach((c) => setExecFor(c, null)); r.inflight = null; await act(() => K.restart(), { ok: 'Session restarting' }); };
  const stopSession = async () => { r.queue = []; r.inflight = null; Object.keys(r.exec).forEach((c) => setExecFor(c, null)); await act(() => K.stop(), { ok: 'Session stopped' }); };
  const exportAs = async (fmt) => {
    if (r.dirty) await saveNow();
    const blob = await act(() => api.blob(`/notebooks/${enc(id)}/export`, { format: fmt, outputs: fmt === 'ipynb' || fmt === 'html' ? true : undefined }));
    if (blob) downloadBlob(blob, `${doc.name}.${{ ipynb: 'ipynb', py: 'py', fabric: 'py', fabric_git_source: 'json', html: 'html' }[fmt] || fmt}`);
  };
  const openRun = async () => {
    if (r.dirty) await saveNow();
    const job = await openOverlay(({ close }) => html`<${RunDialog} nb=${{ id, name: doc.name, settings: R.current.settings || doc.settings }} close=${close} />`);
    if (job && job.id) toast.ok('Job submitted', `Open it from Jobs: ${job.id}`);
    if (job && job.id) window.location.hash = jobHref(job.id);
  };
  const renameTitle = async (name) => {
    const n = name.trim();
    if (!n || n === doc.name) { setTitle(doc.name); return; }
    try {
      const { data: item, etag } = await api.full('GET', `/items/${enc(id)}`);
      await api.put(`/items/${enc(id)}`, { name: n, comment: 'renamed' }, { ifMatch: item.etag || etag });
      const { data } = await api.full('GET', `/notebooks/${enc(id)}/content`, { query: { outputs: false } });
      r.etag = data.etag; setDoc((d) => ({ ...d, name: n })); toast.ok('Renamed');
    } catch (e) { const d = describeError(e); toast.bad(d.title, d.remediation); setTitle(doc.name); }
  };
  const changeSettings = (s) => { R.current.settings = s; setDoc((d) => ({ ...d, settings: s })); markDirty(800); };

  // ---- render ---------------------------------------------------------------------------------------------------------------------------
  if (load.loading && !doc) return html`<${Loading} label="Opening notebook" />`;
  if (load.error) return html`<div class="page"><${ErrorBox} error=${load.error} onRetry=${loadDoc} /><div class="mt-3"><${Btn} href="#/workspaces">Back to workspaces<//></div></div>`;
  const ks = KSTATE[kst.state] || KSTATE.none;
  const engine = (doc.settings && doc.settings.engine) || 'python';
  const runningNow = Object.values(exec).includes('running');
  const kernelItems = [
    !kst.session && { label: 'Start session', icon: 'play', onClick: () => { ensureStart(); } },
    kst.session && { label: 'Interrupt', icon: 'stop', onClick: () => C.interrupt() },
    kst.session && { label: 'Restart', icon: 'refresh', onClick: restart },
    kst.session && { label: 'Stop session', icon: 'power', danger: true, onClick: stopSession },
    kst.spark && kst.spark.ui_url && { sep: true },
    kst.spark && kst.spark.ui_url && { label: 'Open Spark UI', icon: 'external', onClick: () => window.open(kst.spark.ui_url, '_blank', 'noopener') },
    kst.session && { sep: true },
    kst.session && { label: 'Copy session id', icon: 'copy', onClick: () => copyText(kst.session.id) },
  ];
  async function ensureStart() { try { await startKernel(); } catch (e) { setRunErr(e); } }
  const sessionMeta = kst.session ? [kst.session.runtime_id, workers[kst.session.worker_id]].filter(Boolean).join(' on ') : '';
  const saveText = save.state === 'saving' ? 'Saving' : save.state === 'dirty' ? 'Unsaved changes' : save.state === 'conflict' ? 'Conflict: changed elsewhere' : save.state === 'error' ? 'Could not save' : save.at ? `Saved ${fmtClock(save.at)}` : 'Saved';

  return html`<div class="nb">
    <div class="nb-bar">
      <div class="nb-title">
        <div class="crumb"><a href=${wsHref(doc.workspace_id)}>${wsName || 'Workspace'}</a><${Icon} name="chevron-right" size="sm" /></div>
        <h1><${Icon} name="notebook" /><input class="name-input" value=${title} aria-label="Notebook name" disabled=${!canEdit} onInput=${(e) => setTitle(e.target.value)} onBlur=${(e) => renameTitle(e.target.value)} onKeyDown=${(e) => { if (e.key === 'Enter') e.target.blur(); if (e.key === 'Escape') { setTitle(doc.name); e.target.blur(); } }} /></h1>
        <span class=${'nb-save ' + (save.state === 'dirty' ? 'dirty' : save.state === 'conflict' || save.state === 'error' ? 'bad' : '')} role="status" aria-live="polite">${save.state === 'saving' ? html`<span class="spinner"></span>` : null}${saveText}</span>
        <${Badge} title="Version">v${doc.version}<//>
        <span class="grow"></span>
        <${MenuButton} label="Session menu" icon="" iconOnly=${false} kind="" size="" items=${kernelItems} align="right">
          <${Light} state=${ks.light} label=${ks.text} /> <span data-testid="kernel-state">${ks.text}</span><span class="k-meta">${titleCase(engine)}${sessionMeta ? ' · ' + sessionMeta : ''}</span><${Icon} name="chevron-down" size="sm" /><//>
      </div>
      <div class="nb-tools" role="toolbar" aria-label="Notebook actions">
        <${Btn} kind="primary" icon="play-all" onClick=${runAll} disabled=${!canRun} data-testid="run-all">Run all<//>
        ${runningNow || kst.state === 'busy' ? html`<${Btn} icon="stop" onClick=${() => C.interrupt()}>Interrupt<//>` : null}
        <${Btn} icon="refresh" onClick=${restart} disabled=${!kst.session}>Restart<//>
        <span class="sep"></span>
        ${canEdit ? html`<${Btn} icon="plus" onClick=${() => C.insert(sel || (r.cells[r.cells.length - 1] || {}).id, 'below', 'code')}>Code<//>
        <${Btn} icon="plus" onClick=${() => C.insert(sel || (r.cells[r.cells.length - 1] || {}).id, 'below', 'markdown')}>Markdown<//>
        <${Btn} iconOnly icon="undo" label="Undo cell change (Z)" disabled=${!undoN.u} onClick=${undo} />
        <${Btn} iconOnly icon="redo" label="Redo cell change (Shift+Z)" disabled=${!undoN.r} onClick=${redo} />
        <${Btn} iconOnly icon="recycle" label="Clear all outputs" onClick=${() => { structural((cs) => cs.map((c) => (c.cell_type === 'code' ? { ...c, outputs: [] } : c))); }} />
        <span class="sep"></span>` : null}
        <${Btn} icon="rocket" onClick=${openRun} disabled=${!canRun} data-testid="run-job">Run as job<//>
        <${Btn} icon="sliders" onClick=${() => setPanel('settings')} data-testid="nb-settings">Settings<//>
        <${Btn} icon="history" onClick=${async () => { if (r.dirty) await saveNow(); setPanel('history'); }}>History<//>
        <${MenuButton} label="Export notebook" icon="download" iconOnly=${false} kind="" size="" items=${[{ label: 'Jupyter notebook (.ipynb)', onClick: () => exportAs('ipynb') }, { label: 'Python script (.py)', onClick: () => exportAs('py') }, { label: 'Fabric notebook source (.py)', onClick: () => exportAs('fabric') }, { label: 'Fabric Git format (.json)', onClick: () => exportAs('fabric_git_source') }, { label: 'HTML with outputs', onClick: () => exportAs('html') }]}>Export<//>
        <${Btn} iconOnly icon="info" label="Keyboard shortcuts" onClick=${() => openOverlay(({ close }) => html`<${ShortcutsDialog} close=${close} />`)} />
      </div>
    </div>
    <div class="nb-banners">
      ${conflict ? html`<div class="nb-banner"><${Banner} kind="warn" title="This notebook was changed somewhere else" remediation="Reload to take the saved version (your edits here are dropped), or overwrite it with what you see now."
        actions=${html`<${Btn} size="sm" onClick=${() => { R.current.conflict = false; loadDoc(); }}>Reload saved version<//><${Btn} size="sm" kind="danger" onClick=${() => saveNow({ overwrite: true, comment: 'overwrite after conflict' })}>Overwrite<//>`}>Version ${(conflict.details && conflict.details.current_etag) ? 'on the server differs from the one you opened.' : 'differs.'}<//></div>` : null}
      ${save.state === 'error' && !conflict ? html`<div class="nb-banner"><${ErrorBox} error=${save.error} onRetry=${() => saveNow()} /></div>` : null}
      ${runErr ? html`<div class="nb-banner"><${ErrorBox} error=${runErr} onRetry=${() => { setRunErr(null); ensureAndPump(); }} /></div>` : null}
      ${kst.state === 'reconnecting' ? html`<div class="nb-banner"><${Banner} kind="warn" title="Connection to the session was lost">Reconnecting. Output produced meanwhile is replayed when the connection returns.<//></div>` : null}
      ${kst.state === 'dead' ? html`<div class="nb-banner"><${Banner} kind="info" title="The session ended">${kst.detail || ''} Running a cell starts a new session.<//></div>` : null}
      ${!canEdit ? html`<div class="nb-banner"><${Banner} kind="info">You can read this notebook${canRun ? ' and run it' : ''}, but not edit it.<//></div>` : null}
    </div>
    <div class="nb-body">
      ${cells.length === 0 ? html`<${Empty} icon="notebook" title="This notebook is empty" action=${canEdit ? html`<div class="row gap-2"><${Btn} kind="primary" icon="plus" onClick=${() => C.insert(null, 'below', 'code')}>Add a code cell<//><${Btn} icon="plus" onClick=${() => C.insert(null, 'below', 'markdown')}>Add a text cell<//></div>` : null}>Add a cell to start.<//>` : html`
      <div class="cells" role="list" aria-label="Notebook cells">${cells.map((c, i) => html`<${CellView} key=${c.id} cell=${c} index=${i} total=${cells.length} sel=${sel === c.id} editing=${editing === c.id} state=${exec[c.id]} canEdit=${canEdit} ctl=${C} hint=${hintFn.current}
        drop=${drag && drag.over === c.id ? drag.pos : null} dragging=${!!drag && drag.id === c.id} />`)}</div>
      ${canEdit ? html`<div class="nb-end"><${Btn} icon="plus" onClick=${() => C.insert((cells[cells.length - 1] || {}).id, 'below', 'code')}>Add code cell<//><${Btn} icon="plus" onClick=${() => C.insert((cells[cells.length - 1] || {}).id, 'below', 'markdown')}>Add markdown cell<//></div>` : null}`}
    </div>
    ${panel === 'settings' ? html`<${SettingsDrawer} nb=${{ id, workspace_id: doc.workspace_id }} settings=${doc.settings} onChange=${changeSettings} kernel=${{ session: kst.session }} onRestart=${async () => { await saveNow(); await stopSession(); ensureStart(); }} onClose=${() => setPanel(null)} />` : null}
    ${panel === 'history' ? html`<${HistoryDrawer} nb=${{ id }} canEdit=${canEdit} onClose=${() => setPanel(null)} onRestored=${() => { setPanel(null); loadDoc(); }} />` : null}
  </div>`;
}
