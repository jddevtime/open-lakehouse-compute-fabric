// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Small line diff (LCS) used by version history views. Returns [{t: ' '|'+'|'-', s}] with unchanged runs collapsed by the caller.

export function diffLines(a, b) {
  const x = String(a ?? '').split('\n'); const y = String(b ?? '').split('\n');
  const n = x.length; const m = y.length;
  if (n * m > 4_000_000) return [...x.map((s) => ({ t: '-', s })), ...y.map((s) => ({ t: '+', s }))];
  const dp = Array.from({ length: n + 1 }, () => new Uint32Array(m + 1));
  for (let i = n - 1; i >= 0; i -= 1) for (let j = m - 1; j >= 0; j -= 1) dp[i][j] = x[i] === y[j] ? dp[i + 1][j + 1] + 1 : Math.max(dp[i + 1][j], dp[i][j + 1]);
  const out = []; let i = 0; let j = 0;
  while (i < n && j < m) {
    if (x[i] === y[j]) { out.push({ t: ' ', s: x[i] }); i += 1; j += 1; } else if (dp[i + 1][j] >= dp[i][j + 1]) { out.push({ t: '-', s: x[i] }); i += 1; } else { out.push({ t: '+', s: y[j] }); j += 1; }
  }
  while (i < n) { out.push({ t: '-', s: x[i] }); i += 1; }
  while (j < m) { out.push({ t: '+', s: y[j] }); j += 1; }
  return out;
}

/** Collapse long unchanged runs to a marker, keeping `ctx` lines around changes. */
export function foldDiff(lines, ctx = 3) {
  const keep = new Array(lines.length).fill(false);
  lines.forEach((l, i) => { if (l.t !== ' ') for (let k = Math.max(0, i - ctx); k <= Math.min(lines.length - 1, i + ctx); k += 1) keep[k] = true; });
  const out = []; let skipped = 0;
  lines.forEach((l, i) => { if (keep[i]) { if (skipped) { out.push({ t: '…', s: `${skipped} unchanged line${skipped === 1 ? '' : 's'}` }); skipped = 0; } out.push(l); } else skipped += 1; });
  if (skipped) out.push({ t: '…', s: `${skipped} unchanged line${skipped === 1 ? '' : 's'}` });
  return out;
}
