// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// i18n-ready string table: every user-facing string that is shared (navigation, common actions, standard messages) lives here.
// Pages use t('key') for shared text; adding a locale means adding another table and calling setLocale().

const tables = {
  en: {
    'app.name': 'Open Lakehouse Compute Fabric',
    'nav.home': 'Home', 'nav.workspaces': 'Workspaces', 'nav.lakehouse': 'Lakehouse', 'nav.sql': 'SQL editor', 'nav.dashboards': 'Dashboards',
    'nav.pipelines': 'Pipelines', 'nav.environments': 'Environments', 'nav.sparkjobs': 'Spark job definitions', 'nav.variables': 'Variable sets',
    'nav.compute': 'Compute', 'nav.jobs': 'Jobs', 'nav.runtimes': 'Runtimes', 'nav.alerts': 'Alerts & webhooks',
    'nav.fabric': 'Microsoft Fabric', 'nav.agents': 'AI agents (MCP)', 'nav.connections': 'Connections & secrets',
    'nav.security': 'Security', 'nav.admin': 'Administration', 'nav.docs': 'API reference', 'nav.about': 'About',
    'nav.intel': 'Intelligence', 'nav.intelCatalog': 'Data catalog', 'nav.intelLineage': 'Lineage', 'nav.intelDiscovery': 'Discovery', 'nav.intelGovernance': 'Governance',
    'group.overview': 'Overview', 'group.intelligence': 'Intelligence', 'group.data': 'Data', 'group.develop': 'Develop', 'group.operate': 'Operate', 'group.integrate': 'Integrate', 'group.admin': 'Administer',
    'common.cancel': 'Cancel', 'common.save': 'Save changes', 'common.create': 'Create', 'common.delete': 'Delete', 'common.close': 'Close', 'common.refresh': 'Refresh',
    'common.copy': 'Copy', 'common.copied': 'Copied', 'common.loading': 'Loading', 'common.search': 'Search', 'common.confirm': 'Confirm', 'common.retry': 'Try again',
    'common.none': 'None', 'common.yes': 'Yes', 'common.no': 'No', 'common.name': 'Name', 'common.state': 'State', 'common.actions': 'Actions', 'common.details': 'Details',
    'common.test': 'Test connectivity', 'common.run': 'Run', 'common.stop': 'Stop', 'common.open': 'Open', 'common.back': 'Back', 'common.next': 'Next', 'common.done': 'Done',
    'error.network': 'The control plane could not be reached. Check that the server is running and your network allows the connection.',
    'error.denied': 'Your account does not have permission for this action.',
    'error.generic': 'Something went wrong.',
    'session.expired': 'Your session expired. Sign in again to continue.',
    'login.title': 'Sign in', 'login.user': 'Username', 'login.password': 'Password', 'login.remember': 'Keep me signed in on this device',
  },
};
let locale = 'en';
export function setLocale(l) { if (tables[l]) locale = l; }
export function registerLocale(l, table) { tables[l] = { ...(tables[l] || {}), ...table }; }
export function t(key, vars) {
  let s = (tables[locale] && tables[locale][key]) ?? tables.en[key] ?? key;
  if (vars) s = s.replace(/\{(\w+)\}/g, (_, k) => (vars[k] != null ? String(vars[k]) : ''));
  return s;
}
