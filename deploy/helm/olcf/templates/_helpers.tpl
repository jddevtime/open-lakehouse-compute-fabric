{{/* SPDX-License-Identifier: Apache-2.0 */}}
{{/* Copyright (c) 2026 Justin Dodrill */}}
{{- define "olcf.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "olcf.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "olcf.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "olcf.selectorLabels" -}}
app.kubernetes.io/name: {{ include "olcf.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "olcf.labels" -}}
helm.sh/chart: {{ include "olcf.chart" . }}
{{ include "olcf.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: olcf
{{- end -}}

{{- define "olcf.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "olcf.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "olcf.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}

{{- define "olcf.workerImage" -}}
{{- printf "%s:%s" .Values.workerImage.repository (default .Chart.AppVersion .Values.workerImage.tag) -}}
{{- end -}}

{{- define "olcf.secretName" -}}
{{- default (printf "%s-secrets" (include "olcf.fullname" .)) .Values.secrets.existingSecret -}}
{{- end -}}

{{- define "olcf.dbSecretName" -}}
{{- if .Values.database.external.existingSecret -}}
{{- .Values.database.external.existingSecret -}}
{{- else -}}
{{- printf "%s-db" (include "olcf.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "olcf.computeNamespace" -}}
{{- default .Release.Namespace .Values.kubernetesProvider.namespace -}}
{{- end -}}

{{- define "olcf.workerURL" -}}
{{- if .Values.workers.url -}}
{{- .Values.workers.url -}}
{{- else if .Values.controlPlane.publicUrl -}}
{{- .Values.controlPlane.publicUrl -}}
{{- else if .Values.workers.insecureInCluster -}}
{{- printf "http://%s:%v" (include "olcf.fullname" .) .Values.controlPlane.service.port -}}
{{- else -}}
{{- fail "workers need a controller URL: set controlPlane.publicUrl (https) or workers.url, or workers.insecureInCluster=true for a lab setup" -}}
{{- end -}}
{{- end -}}

{{- define "olcf.pullSecrets" -}}
{{- if .Values.image.pullSecrets }}
imagePullSecrets:
{{- range .Values.image.pullSecrets }}
  - name: {{ . }}
{{- end }}
{{- end }}
{{- end -}}

{{/* one NetworkPolicy port entry for an inclusive range {start, end} */}}
{{- define "olcf.portRule" -}}
- {port: {{ .start }}{{ if gt .end .start }}, endPort: {{ .end }}{{ end }}, protocol: TCP}
{{- end -}}

{{/* the NetworkPolicy peers that select this release's pods by label */}}
{{- define "olcf.podPeer" -}}
- podSelector:
    matchLabels:
      {{- include "olcf.selectorLabels" .ctx | nindent 6 }}
      app.kubernetes.io/component: {{ .component }}
{{- end -}}


{{/* "true" when an operator-supplied runtime trust directory (runtimeTrust.existingSecret or .existingConfigMap) is mounted into the control plane and the workers */}}
{{- define "olcf.runtimeTrustEnabled" -}}
{{- if and .Values.runtimeTrust.existingSecret .Values.runtimeTrust.existingConfigMap -}}
{{- fail "runtimeTrust.existingSecret and runtimeTrust.existingConfigMap are mutually exclusive: set one of them" -}}
{{- end -}}
{{- if or .Values.runtimeTrust.existingSecret .Values.runtimeTrust.existingConfigMap -}}true{{- end -}}
{{- end -}}

{{- define "olcf.runtimeTrustVolume" -}}
- name: runtime-trust
{{- if .Values.runtimeTrust.existingSecret }}
  secret:
    secretName: {{ .Values.runtimeTrust.existingSecret }}
    defaultMode: 0444
{{- else }}
  configMap:
    name: {{ .Values.runtimeTrust.existingConfigMap }}
    defaultMode: 0444
{{- end }}
{{- end -}}
