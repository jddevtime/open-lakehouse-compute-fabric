// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Intelligence vocabulary in plain words: asset types, confidence levels, gap kinds, scan states and the links between Intelligence pages. Pure (no DOM, no network).

/** Where each kind of asset belongs in a picture: `category` drives colour and the column heading in the lineage graph. */
export const TYPE_META = {
  tenant: { label: 'Tenant', icon: 'cloud', category: 'container' },
  workspace: { label: 'Workspace', icon: 'folder', category: 'container' },
  domain_scope: { label: 'Domain scope', icon: 'grid', category: 'container' },
  lakehouse: { label: 'Lakehouse', icon: 'database', category: 'container' },
  warehouse: { label: 'Warehouse', icon: 'database', category: 'container' },
  schema: { label: 'Schema', icon: 'folder-open', category: 'container' },
  table: { label: 'Table', icon: 'table', category: 'dataset' },
  view: { label: 'View', icon: 'eye', category: 'dataset' },
  column: { label: 'Column', icon: 'list', category: 'column' },
  file: { label: 'File', icon: 'file', category: 'source' },
  source_system: { label: 'External system', icon: 'plug', category: 'source' },
  external: { label: 'External data', icon: 'globe', category: 'source' },
  notebook: { label: 'Notebook', icon: 'notebook', category: 'process' },
  pipeline: { label: 'Pipeline', icon: 'workflow', category: 'process' },
  activity: { label: 'Pipeline activity', icon: 'workflow', category: 'process' },
  sql_query: { label: 'SQL query', icon: 'terminal', category: 'process' },
  spark_job: { label: 'Spark job', icon: 'zap', category: 'process' },
  script: { label: 'Script', icon: 'file-code', category: 'process' },
  dataflow: { label: 'Dataflow', icon: 'route', category: 'process' },
  semantic_model: { label: 'Semantic model', icon: 'layers', category: 'model' },
  model_table: { label: 'Model table', icon: 'table', category: 'dataset' },
  measure: { label: 'Measure', icon: 'gauge', category: 'model' },
  report: { label: 'Report', icon: 'chart', category: 'report' },
  page: { label: 'Report page', icon: 'file-text', category: 'report' },
  visual: { label: 'Visual', icon: 'pie', category: 'report' },
  dashboard: { label: 'Dashboard', icon: 'chart-line', category: 'report' },
};
export const typeMeta = (t) => TYPE_META[t] || { label: String(t || 'Unknown').replace(/_/g, ' '), icon: 'file', category: 'dataset' };
export const typeLabel = (t) => typeMeta(t).label;
export const typeIcon = (t) => typeMeta(t).icon;
export const categoryOf = (t) => typeMeta(t).category;
export const isProcess = (t) => categoryOf(t) === 'process';
export const CATEGORY_HEADING = { source: 'Sources', dataset: 'Tables', process: 'Notebooks, queries and pipelines', model: 'Models', report: 'Reports and visuals', column: 'Columns', container: 'Containers' };
/** Plural, lower-case noun phrase for counts ("3 tables"). */
export const typeNoun = (t) => typeLabel(t).replace(/^\p{Lu}(?=\p{Ll})/u, (c) => c.toLowerCase());
export function typePlural(t, n) {
  const l = typeNoun(t);
  if (n === 1) return l;
  if (/y$/.test(l)) return l.slice(0, -1) + 'ies';
  if (/(s|x)$/.test(l)) return l + 'es';
  return l + 's';
}

/** The asset types the catalog filter offers, in the order people think about them. */
export const CATALOG_TYPES = ['table', 'view', 'column', 'lakehouse', 'notebook', 'pipeline', 'sql_query', 'spark_job', 'semantic_model', 'measure', 'report', 'visual', 'dashboard', 'file', 'workspace'];

// ---- confidence -----------------------------------------------------------------------------------------------------------------
export const CONFIDENCE = ['verified', 'derived', 'inferred', 'unknown'];
const RANK = { verified: 0, derived: 1, inferred: 2, unknown: 3 };
export const confRank = (c) => (RANK[c] ?? 3);
export const CONF_LABEL = { verified: 'Verified', derived: 'Derived', inferred: 'Inferred', unknown: 'Unknown' };
export const CONF_HELP = {
  verified: 'Observed at run time by the platform: a job, query or pipeline run really read or wrote the data.',
  derived: 'Worked out by reading code, SQL or a definition (including what a pipeline or report states about its own inputs). Not yet seen running.',
  inferred: 'A good guess from names or other indirect hints. Check it before relying on it.',
  unknown: 'A dependency exists, but how the data moves is not known.',
};
export const confLabel = (c) => CONF_LABEL[c] || 'Unknown';
export const confBadgeKind = (c) => ({ verified: 'ok', derived: 'info', inferred: 'warn', unknown: 'off' }[c] || 'off');
/** True when `level` is at least as strong as `floor` (the "min confidence" filter). */
export const atLeast = (level, floor) => !floor || confRank(level) <= confRank(floor);
/** The weakest of several confidence levels (a path is only as sure as its weakest edge). */
export function weakest(levels) {
  let w = 'verified';
  for (const l of levels) if (confRank(l) > confRank(w)) w = l;
  return w;
}

// ---- gaps and scans -------------------------------------------------------------------------------------------------------------
export const GAP_KINDS = {
  permission_denied: { label: 'Permission denied', icon: 'lock', help: 'The scanning identity is not allowed to read this. What sits behind it is invisible here.' },
  api_unavailable: { label: 'Not available from the source', icon: 'cloud', help: 'The source did not offer this information (switched off, not licensed, or failing).' },
  unsupported_construct: { label: 'Not understood', icon: 'code', help: 'Code or a definition uses something the analysers cannot follow, so connections through it are missing.' },
  dynamic_reference: { label: 'Name built at run time', icon: 'zap', help: 'A table or file name is computed while the code runs, so it cannot be read from the code.' },
  parse_error: { label: 'Could not be read', icon: 'alert-triangle', help: 'A definition could not be parsed, so nothing was learned from it.' },
  not_inspected: { label: 'Not inspected', icon: 'eye-off', help: 'The scan skipped this or could only partly inspect it.' },
  unresolved_reference: { label: 'Points at something unknown', icon: 'link', help: 'Code mentions an object no scan has reported yet, so a placeholder stands in for it.' },
  identity_conflict: { label: 'Same name, different object', icon: 'link', help: 'Two different objects share a qualified name (for example two Fabric workspaces mapped to one OLCF workspace). They are kept apart; a reference that only gives the name cannot be resolved.' },
  sampling_disabled: { label: 'Sampling is off', icon: 'shield', help: 'Looking at data values is switched off. That is the default and it is on purpose.' },
};
export const gapMeta = (k) => GAP_KINDS[k] || { label: String(k || 'Other').replace(/_/g, ' '), icon: 'info', help: '' };

/** Scan or source state -> the light state used by the control panel (green, amber, red). */
export function scanLight(status) {
  const s = String(status || '').toLowerCase();
  if (s === 'succeeded') return 'ok';
  if (s === 'partial') return 'degraded';
  if (s === 'failed') return 'down';
  if (s === 'running') return 'busy';
  if (s === 'cancelled') return 'unknown';
  return 'unknown';
}
export const SCAN_LABEL = { succeeded: 'Scanned', partial: 'Scanned with problems', failed: 'Failed', running: 'Scanning', cancelled: 'Cancelled' };
export const scanLabel = (s) => SCAN_LABEL[s] || 'Never scanned';
export const SOURCE_KIND = { local: 'This installation', catalog_mirror: 'Catalog mirror', fabric: 'Microsoft Fabric and Power BI', scanner_import: 'Imported scanner file' };
export const sourceKindLabel = (k) => SOURCE_KIND[k] || k;

// ---- links ----------------------------------------------------------------------------------------------------------------------
const enc = encodeURIComponent;
export const assetHref = (id, tab) => `#/intelligence/assets/${enc(id)}${tab ? '?tab=' + enc(tab) : ''}`;
export const lineageHref = (id, params = {}) => {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) if (v != null && v !== '') q.set(k, String(v));
  const s = q.toString();
  return `#/intelligence/lineage${id ? '/' + enc(id) : ''}${s ? '?' + s : ''}`;
};
export const impactHref = (id) => `#/intelligence/impact/${enc(id)}`;
export const catalogHref = (params = {}) => {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) if (v != null && v !== '') q.set(k, String(v));
  const s = q.toString();
  return '#/intelligence/catalog' + (s ? '?' + s : '');
};
export const discoveryHref = (tab, params = {}) => {
  const q = new URLSearchParams({ ...(tab ? { tab } : {}), ...params });
  const s = q.toString();
  return '#/intelligence/discovery' + (s ? '?' + s : '');
};
export const governanceHref = (section, params = {}) => {
  const q = new URLSearchParams(params).toString();
  return '#/intelligence/governance' + (section ? '/' + section : '') + (q ? '?' + q : '');
};

// ---- small formatting helpers ---------------------------------------------------------------------------------------------------
/** "Bronze", "Silver" or "Gold" when the NAME says so (a naming convention, not a fact the platform checked); else null. */
export function layerOf(node) {
  const text = `${node && node.name ? node.name : ''} ${node && node.path ? node.path : ''}`.toLowerCase();
  const m = /(^|[^a-z0-9])(bronze|silver|gold)([^a-z0-9]|$)/.exec(text);
  return m ? m[2][0].toUpperCase() + m[2].slice(1) : null;
}
export function pct(n, d, digits = 0) {
  if (!d) return null;
  return Math.round((n / d) * 100 * 10 ** digits) / 10 ** digits;
}
/** A one-line summary of counts by key: "10 visuals, 1 report". */
export function countsPhrase(by) {
  return Object.entries(by || {}).filter(([, n]) => n > 0).sort((a, b) => b[1] - a[1]).map(([t, n]) => `${n} ${typePlural(t, n)}`).join(', ');
}
export function escapeRegExp(s) { return String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); }
/** Short path tail for tight spaces: "Sample Retail / retail_lakehouse / fact_sales" -> "retail_lakehouse / fact_sales". */
export function pathTail(path, keep = 2) {
  if (!path) return '';
  const parts = String(path).split(' / ');
  return parts.slice(-keep).join(' / ');
}

// ---- connections in words -------------------------------------------------------------------------------------------------------
/** Edges are stored the way they read in English: "<src> <verb> <dst>". */
export const EDGE_VERB = { READS_FROM: 'reads from', WRITES_TO: 'writes to', TRANSFORMS: 'feeds', DEPENDS_ON: 'depends on', USES_MEASURE: 'uses', DISPLAYS: 'displays', CONTAINS: 'contains', EXECUTES: 'runs' };
export const edgeVerb = (type) => EDGE_VERB[type] || String(type || 'is linked to').toLowerCase().replace(/_/g, ' ');
