// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// CodeMirror 5 (vendored under /vendor/codemirror) loaded on demand with plain script tags (CSP: script-src 'self'), and a Preact wrapper component.
import { html, useRef, useEffect, useLayoutEffect } from '../core/h.js';

const BASE = new URL('../../vendor/codemirror/', import.meta.url).href;
const loaded = new Map();
function script(rel) {
  if (!loaded.has(rel)) {
    loaded.set(rel, new Promise((resolve, reject) => {
      const s = document.createElement('script');
      s.src = BASE + rel; s.async = false;
      s.onload = () => resolve();
      s.onerror = () => reject(new Error('Could not load ' + rel));
      document.head.appendChild(s);
    }));
  }
  return loaded.get(rel);
}
function css(rel) {
  if (!loaded.has(rel)) {
    loaded.set(rel, new Promise((resolve) => {
      const l = document.createElement('link');
      l.rel = 'stylesheet'; l.href = BASE + rel; l.onload = resolve; l.onerror = resolve;
      document.head.appendChild(l);
    }));
  }
  return loaded.get(rel);
}

const MODES = { python: 'mode/python/python.js', sql: 'mode/sql/sql.js', markdown: 'mode/markdown/markdown.js', javascript: 'mode/javascript/javascript.js', yaml: 'mode/yaml/yaml.js', shell: 'mode/shell/shell.js', xml: 'mode/xml/xml.js', clike: 'mode/clike/clike.js' };
const ADDONS = ['addon/edit/matchbrackets.js', 'addon/edit/closebrackets.js', 'addon/comment/comment.js', 'addon/selection/active-line.js', 'addon/display/placeholder.js', 'addon/hint/show-hint.js', 'addon/hint/sql-hint.js'];
let corePromise = null;
export function loadCodeMirror(modes = ['python', 'sql', 'markdown']) {
  corePromise ||= Promise.all([css('codemirror.css'), css('addon/hint/show-hint.css'), script('codemirror.js')]);
  return corePromise.then(() => Promise.all([...ADDONS.map(script), script(MODES.xml), ...modes.map((m) => MODES[m] && script(MODES[m]))])).then(() => window.CodeMirror);
}

/** Map our language names to CodeMirror modes. */
const modeFor = (lang) => ({ python: { name: 'python', version: 3 }, sql: 'text/x-sql', markdown: 'markdown', json: { name: 'javascript', json: true }, yaml: 'yaml', shell: 'shell', text: null }[lang] ?? lang);

/**
 * <CodeEditor value mode onChange onRun onCursor readOnly placeholder hint editorRef autoHeight/>
 * The CodeMirror instance is created once; `value` changes from outside are applied without echoing back through onChange.
 */
export function CodeEditor({ value, mode = 'python', onChange, onRun, onFocus, onBlur, readOnly, placeholder, hint, hintOptions, editorRef, autoHeight = true, minLines, extraKeys, lineNumbers = false, class: cls = '', label = 'Code editor', height, autofocus }) {
  const host = useRef();
  const cmRef = useRef(null);
  const props = useRef();
  props.current = { onChange, onRun, onFocus, onBlur, hint, hintOptions };
  useLayoutEffect(() => {
    let dead = false;
    loadCodeMirror(['python', 'sql', 'markdown', 'javascript', 'yaml', 'shell']).then((CM) => {
      if (dead || !host.current) return;
      const keys = {
        'Ctrl-Enter': () => props.current.onRun && props.current.onRun('stay'),
        'Cmd-Enter': () => props.current.onRun && props.current.onRun('stay'),
        'Shift-Enter': () => props.current.onRun && props.current.onRun('advance'),
        'Alt-Enter': () => props.current.onRun && props.current.onRun('insert'),
        'Ctrl-/': 'toggleComment', 'Cmd-/': 'toggleComment',
        'Ctrl-Space': (cm) => cm.showHint({ completeSingle: false, hint: props.current.hint, ...(props.current.hintOptions || {}) }),
        Tab: (cm) => { if (cm.somethingSelected()) cm.indentSelection('add'); else cm.replaceSelection('    ', 'end'); },
        'Shift-Tab': (cm) => cm.indentSelection('subtract'),
        Esc: () => { if (document.activeElement) document.activeElement.blur(); host.current && host.current.dispatchEvent(new CustomEvent('cm-escape', { bubbles: true })); },
        ...(extraKeys || {}),
      };
      const cm = CM(host.current, {
        theme: 'olcf',
        value: value ?? '', mode: modeFor(mode), lineNumbers, lineWrapping: false, matchBrackets: true, autoCloseBrackets: true, styleActiveLine: true, indentUnit: 4, tabSize: 4, indentWithTabs: false,
        readOnly: readOnly ? 'nocursor' : false, placeholder, viewportMargin: autoHeight ? Infinity : 10, extraKeys: keys, inputStyle: 'contenteditable', spellcheck: false,
      });
      if (autoHeight) cm.setSize(null, 'auto');
      else if (height) cm.setSize(null, height);
      cm.getInputField().setAttribute('aria-label', label);
      cm.on('change', (inst, ch) => { if (ch.origin !== 'setValue' && props.current.onChange) props.current.onChange(inst.getValue()); });
      cm.on('focus', () => props.current.onFocus && props.current.onFocus(cm));
      cm.on('blur', () => props.current.onBlur && props.current.onBlur(cm));
      cm.on('inputRead', (inst, ch) => {
        if (!props.current.hint || inst.state.completionActive || ch.origin !== '+input') return;
        const cur = inst.getCursor(); const line = inst.getLine(cur.line).slice(0, cur.ch);
        if (/[\w.]$/.test(line) && /(\.|\w{2,})$/.test(line)) inst.showHint({ completeSingle: false, hint: props.current.hint, ...(props.current.hintOptions || {}) });
      });
      cmRef.current = cm;
      if (editorRef) editorRef.current = cm;
      if (minLines) host.current.style.minHeight = `${minLines * 20 + 12}px`;
      cm.refresh();
      if (autofocus) { cm.focus(); cm.setCursor(cm.lineCount(), 0); }
    }).catch((e) => { if (host.current) host.current.textContent = 'The editor failed to load: ' + e.message; });
    return () => { dead = true; if (editorRef) editorRef.current = null; cmRef.current = null; if (host.current) host.current.textContent = ''; };
  }, []);
  useEffect(() => {
    const cm = cmRef.current;
    if (cm && value != null && value !== cm.getValue()) {
      const cur = cm.getCursor();
      cm.setValue(value);
      try { cm.setCursor(cur); } catch { /* cursor beyond the new text */ }
    }
  }, [value]);
  useEffect(() => { const cm = cmRef.current; if (cm) { cm.setOption('mode', modeFor(mode)); cm.setOption('readOnly', readOnly ? 'nocursor' : false); } }, [mode, readOnly]);
  return html`<div class=${'cm-host ' + cls} ref=${host} data-mode=${mode}></div>`;
}

/** A read-only highlighted code block (history diffs, API snippets). */
export function CodeView({ value, mode = 'python', class: cls = '' }) {
  return html`<${CodeEditor} value=${value} mode=${mode} readOnly=${true} class=${'cm-view ' + cls} label="Code (read only)" />`;
}
