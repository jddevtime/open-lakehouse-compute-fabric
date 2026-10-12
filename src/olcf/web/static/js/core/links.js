// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Where each kind of object opens in the UI, and item-type presentation metadata.
import { enc } from './util.js';

export const ITEM_TYPES = {
  notebook: { label: 'Notebook', icon: 'notebook', plural: 'Notebooks' },
  lakehouse: { label: 'Lakehouse', icon: 'database', plural: 'Lakehouses' },
  sql_query: { label: 'SQL query', icon: 'terminal', plural: 'SQL queries' },
  report: { label: 'Dashboard', icon: 'chart', plural: 'Dashboards' },
  pipeline: { label: 'Pipeline', icon: 'workflow', plural: 'Pipelines' },
  schedule: { label: 'Schedule', icon: 'calendar', plural: 'Schedules' },
  environment: { label: 'Environment', icon: 'box', plural: 'Environments' },
  spark_job_definition: { label: 'Spark job definition', icon: 'zap', plural: 'Spark job definitions' },
  variable_set: { label: 'Variable set', icon: 'sliders', plural: 'Variable sets' },
  extension: { label: 'Extension', icon: 'package', plural: 'Extensions' },
};
export const itemMeta = (type) => ITEM_TYPES[type] || { label: type, icon: 'file', plural: type };

export function itemHref(it) {
  const id = enc(it.id);
  switch (it.type) {
    case 'notebook': return `#/notebook/${id}`;
    case 'lakehouse': return `#/lakehouse/${id}`;
    case 'sql_query': return `#/sql/${id}`;
    case 'report': return `#/dashboards/${id}`;
    case 'pipeline': return `#/pipelines/${id}`;
    case 'environment': return `#/environments/${id}`;
    case 'spark_job_definition': return `#/sparkjobs/${id}`;
    case 'variable_set': return `#/variables/${id}`;
    default: return `#/workspaces/${enc(it.workspace_id || '')}?item=${id}`;
  }
}
export const jobHref = (id) => `#/jobs/${enc(id)}`;
export const runHref = (id) => `#/runs/${enc(id)}`;
export const wsHref = (id) => `#/workspaces/${enc(id)}`;
