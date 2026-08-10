{{/*
Expand the name of the chart.
*/}}
{{- define "kitaru-worker.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "kitaru-worker.fullname" -}}
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
{{- define "kitaru-worker.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "kitaru-worker.labels" -}}
helm.sh/chart: {{ include "kitaru-worker.chart" . }}
{{ include "kitaru-worker.selectorLabels" . }}
{{- if .Chart.Version }}
app.kubernetes.io/version: {{ .Chart.Version | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "kitaru-worker.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kitaru-worker.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Whether an existing secret was given for the Kitaru API key.
*/}}
{{- define "kitaru-worker.hasApiKeySecretRef" -}}
{{- if .Values.kitaru.apiKeySecret }}
{{- if .Values.kitaru.apiKeySecret.name }}
{{- true }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Name of the Kubernetes secret containing the Kitaru API key.
*/}}
{{- define "kitaru-worker.apiKeySecretName" -}}
{{- if include "kitaru-worker.hasApiKeySecretRef" . }}
{{- .Values.kitaru.apiKeySecret.name }}
{{- else if .Values.kitaru.apiKey }}
{{- include "kitaru-worker.fullname" . }}
{{- else }}
{{- fail "kitaru.apiKey or kitaru.apiKeySecret.name must be set" }}
{{- end }}
{{- end }}

{{/*
Key within the API key secret that holds the Kitaru API key.
*/}}
{{- define "kitaru-worker.apiKeySecretKey" -}}
{{- if include "kitaru-worker.hasApiKeySecretRef" . }}
{{- required "kitaru.apiKeySecret.key is required when kitaru.apiKeySecret.name is set" .Values.kitaru.apiKeySecret.key }}
{{- else }}
{{- print "KITARU_API_KEY" }}
{{- end }}
{{- end }}
