{{/*
Expand the name of the chart.
*/}}
{{- define "kitaru.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "kitaru.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "kitaru.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "kitaru.labels" -}}
helm.sh/chart: {{ include "kitaru.chart" . }}
{{ include "kitaru.selectorLabels" . }}
{{- if .Chart.Version }}
app.kubernetes.io/version: {{ .Chart.Version | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "kitaru.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kitaru.name" . }}
{{- if .Values.server.instanceLabel }}
app.kubernetes.io/instance: {{ .Values.server.instanceLabel | quote }}
{{- else }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
{{- end }}

{{/*
Selector labels for server-only resources
*/}}
{{- define "kitaru.serverSelectorLabels" -}}
{{ include "kitaru.selectorLabels" . }}
app.kubernetes.io/component: server
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "kitaru.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "kitaru.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Build the complete NO_PROXY list
*/}}
{{- define "kitaru.noProxyList" -}}
{{- $noProxy := .Values.server.proxy.noProxy -}}
{{- /* Add the server URL hostname */ -}}
{{- if .Values.server.serverURL -}}
{{- $serverURL := urlParse .Values.server.serverURL -}}
{{- if not (contains $serverURL.host $noProxy) -}}
{{- $noProxy = printf "%s,%s" $noProxy $serverURL.host -}}
{{- end -}}
{{- end -}}
{{- /* Add the ingress hostname if specified */ -}}
{{- if .Values.server.ingress.host -}}
{{- if not (contains .Values.server.ingress.host $noProxy) -}}
{{- $noProxy = printf "%s,%s" $noProxy .Values.server.ingress.host -}}
{{- end -}}
{{- end -}}
{{- /* Add the gateway hostname if specified */ -}}
{{- if .Values.server.gateway.host -}}
{{- if not (contains .Values.server.gateway.host $noProxy) -}}
{{- $noProxy = printf "%s,%s" $noProxy .Values.server.gateway.host -}}
{{- end -}}
{{- end -}}
{{- range .Values.server.proxy.additionalNoProxy -}}
{{- $noProxy = printf "%s,%s" $noProxy . -}}
{{- end -}}
{{- /* Add service hostnames if they're not already included */ -}}
{{- if not (contains ".svc" $noProxy) -}}
{{- $noProxy = printf "%s,%s" $noProxy (include "kitaru.fullname" .) -}}
{{- $noProxy = printf "%s,%s-dashboard" $noProxy (include "kitaru.fullname" .) -}}
{{- end -}}
{{- $noProxy -}}
{{- end -}}
