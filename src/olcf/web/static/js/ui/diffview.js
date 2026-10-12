// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
import { html } from '../core/h.js';
import { diffLines, foldDiff } from '../core/diff.js';

/** Unified text diff of two strings. */
export function DiffView({ a, b, ctx = 3 }) {
  const lines = foldDiff(diffLines(a, b), ctx);
  const changed = lines.some((l) => l.t === '+' || l.t === '-');
  if (!changed) return html`<div class="muted small">No differences.</div>`;
  return html`<div class="diff" role="group" aria-label="Differences">${lines.map((l, i) => html`<div key=${i} class=${l.t === '+' ? 'add' : l.t === '-' ? 'del' : l.t === '…' ? 'fold' : ''}><span class="t" aria-hidden="true">${l.t === '…' ? '' : l.t}</span><span class="sr-only">${l.t === '+' ? 'added: ' : l.t === '-' ? 'removed: ' : ''}</span><span>${l.s || ' '}</span></div>`)}</div>`;
}
