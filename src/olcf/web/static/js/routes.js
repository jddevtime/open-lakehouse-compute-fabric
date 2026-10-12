// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Justin Dodrill
// Route table and navigation structure. `module` paths are relative to this file's URL.
const p = (f) => new URL('./pages/' + f, import.meta.url).href;

export const routes = [
  { path: '/', module: p('home.js'), export: 'HomePage', title: 'Home', nav: 'home' },
  { path: '/compute', module: p('compute.js'), export: 'ComputePage', title: 'Compute', nav: 'compute' },
  { path: '/fabric', module: p('fabric.js'), export: 'FabricPage', title: 'Microsoft Fabric', nav: 'fabric' },
  { path: '/workspaces', module: p('workspaces.js'), export: 'WorkspacesPage', title: 'Workspaces', nav: 'workspaces' },
  { path: '/workspaces/:ws', module: p('workspaces.js'), export: 'WorkspaceDetailPage', title: 'Workspace', nav: 'workspaces' },
  { path: '/notebook/:id', module: p('notebook.js'), export: 'NotebookPage', title: 'Notebook', nav: 'workspaces' },
  { path: '/lakehouse', module: p('lakehouse.js'), export: 'LakehousePage', title: 'Lakehouse', nav: 'lakehouse' },
  { path: '/lakehouse/:id', module: p('lakehouse.js'), export: 'LakehousePage', title: 'Lakehouse', nav: 'lakehouse' },
  { path: '/sql', module: p('sql.js'), export: 'SqlPage', title: 'SQL editor', nav: 'sql' },
  { path: '/sql/:id', module: p('sql.js'), export: 'SqlPage', title: 'SQL editor', nav: 'sql' },
  { path: '/dashboards', module: p('reports.js'), export: 'ReportsPage', title: 'Dashboards', nav: 'dashboards' },
  { path: '/dashboards/:id', module: p('reports.js'), export: 'ReportViewPage', title: 'Dashboard', nav: 'dashboards' },
  { path: '/dashboards/:id/edit', module: p('reports.js'), export: 'ReportDesignerPage', title: 'Dashboard designer', nav: 'dashboards' },
  { path: '/jobs', module: p('jobs.js'), export: 'JobsPage', title: 'Jobs', nav: 'jobs' },
  { path: '/jobs/:id', module: p('jobs.js'), export: 'JobDetailPage', title: 'Job', nav: 'jobs' },
  { path: '/pipelines', module: p('pipelines.js'), export: 'PipelinesPage', title: 'Pipelines', nav: 'pipelines' },
  { path: '/pipelines/:id', module: p('pipelines.js'), export: 'PipelineDetailPage', title: 'Pipeline', nav: 'pipelines' },
  { path: '/runs/:id', module: p('pipelines.js'), export: 'RunDetailPage', title: 'Pipeline run', nav: 'pipelines' },
  { path: '/alerts', module: p('alerts.js'), export: 'AlertsPage', title: 'Alerts & webhooks', nav: 'alerts' },
  { path: '/connections', module: p('connections.js'), export: 'ConnectionsPage', title: 'Connections & secrets', nav: 'connections' },
  { path: '/runtimes', module: p('runtimes.js'), export: 'RuntimesPage', title: 'Runtimes', nav: 'runtimes' },
  { path: '/environments', module: p('envs.js'), export: 'EnvironmentsPage', title: 'Environments', nav: 'environments' },
  { path: '/environments/:id', module: p('envs.js'), export: 'EnvironmentDetailPage', title: 'Environment', nav: 'environments' },
  { path: '/variables', module: p('envs.js'), export: 'VariableSetsPage', title: 'Variable sets', nav: 'variables' },
  { path: '/variables/:id', module: p('envs.js'), export: 'VariableSetDetailPage', title: 'Variable set', nav: 'variables' },
  { path: '/sparkjobs', module: p('envs.js'), export: 'SparkJobsPage', title: 'Spark job definitions', nav: 'sparkjobs' },
  { path: '/sparkjobs/:id', module: p('envs.js'), export: 'SparkJobDetailPage', title: 'Spark job definition', nav: 'sparkjobs' },
  { path: '/agents', module: p('agents.js'), export: 'AgentsPage', title: 'AI agents', nav: 'agents' },
  { path: '/security', module: p('security.js'), export: 'SecurityPage', title: 'Security', nav: 'security' },
  { path: '/admin', module: p('ops.js'), export: 'AdminPage', title: 'Administration', nav: 'admin' },
  { path: '/intelligence', module: p('intelligence.js'), export: 'IntelOverviewPage', title: 'Intelligence', nav: 'intel' },
  { path: '/intelligence/catalog', module: p('intelligence.js'), export: 'IntelCatalogPage', title: 'Data catalog', nav: 'intel-catalog' },
  { path: '/intelligence/assets/:id', module: p('intel_asset.js'), export: 'AssetPage', title: 'Asset', nav: 'intel-catalog' },
  { path: '/intelligence/lineage', module: p('intel_lineage.js'), export: 'LineagePage', title: 'Lineage', nav: 'intel-lineage' },
  { path: '/intelligence/lineage/:id', module: p('intel_lineage.js'), export: 'LineagePage', title: 'Lineage', nav: 'intel-lineage' },
  { path: '/intelligence/impact/:id', module: p('intel_asset.js'), export: 'ImpactPage', title: 'Impact analysis', nav: 'intel-lineage' },
  { path: '/intelligence/discovery', module: p('intel_discovery.js'), export: 'DiscoveryPage', title: 'Discovery', nav: 'intel-discovery' },
  { path: '/intelligence/governance', module: p('intel_governance.js'), export: 'GovernancePage', title: 'Governance', nav: 'intel-governance' },
  { path: '/intelligence/governance/:section', module: p('intel_governance.js'), export: 'GovernancePage', title: 'Governance', nav: 'intel-governance' },
  { path: '/about', module: p('about.js'), export: 'AboutPage', title: 'About', nav: 'about' },
];

/** Navigation rail structure: [group, [[navKey, label key, icon, path, permission?]]] */
export const navGroups = [
  ['group.overview', [['home', 'nav.home', 'home', '/']]],
  ['group.data', [
    ['lakehouse', 'nav.lakehouse', 'database', '/lakehouse'],
    ['sql', 'nav.sql', 'terminal', '/sql'],
    ['dashboards', 'nav.dashboards', 'chart', '/dashboards'],
  ]],
  ['group.develop', [
    ['workspaces', 'nav.workspaces', 'folder', '/workspaces'],
    ['pipelines', 'nav.pipelines', 'workflow', '/pipelines'],
    ['environments', 'nav.environments', 'box', '/environments'],
    ['sparkjobs', 'nav.sparkjobs', 'zap', '/sparkjobs'],
    ['variables', 'nav.variables', 'sliders', '/variables'],
  ]],
  ['group.intelligence', [
    ['intel', 'nav.intel', 'sparkles', '/intelligence'],
    ['intel-catalog', 'nav.intelCatalog', 'search', '/intelligence/catalog'],
    ['intel-lineage', 'nav.intelLineage', 'route', '/intelligence/lineage'],
    ['intel-discovery', 'nav.intelDiscovery', 'radio', '/intelligence/discovery'],
    ['intel-governance', 'nav.intelGovernance', 'shield-check', '/intelligence/governance'],
  ]],
  ['group.operate', [
    ['compute', 'nav.compute', 'server', '/compute'],
    ['jobs', 'nav.jobs', 'play-all', '/jobs'],
    ['runtimes', 'nav.runtimes', 'layers', '/runtimes'],
    ['alerts', 'nav.alerts', 'bell', '/alerts'],
  ]],
  ['group.integrate', [
    ['fabric', 'nav.fabric', 'cloud', '/fabric'],
    ['agents', 'nav.agents', 'bot', '/agents'],
    ['connections', 'nav.connections', 'plug', '/connections'],
  ]],
  ['group.admin', [
    ['security', 'nav.security', 'shield', '/security'],
    ['admin', 'nav.admin', 'settings', '/admin'],
    ['docs', 'nav.docs', 'book', '/v1/docs', true],
    ['about', 'nav.about', 'info', '/about'],
  ]],
];
