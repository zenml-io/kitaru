{{- define "kitaru.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

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

{{- define "kitaru.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "kitaru.labels" -}}
helm.sh/chart: {{ include "kitaru.chart" . }}
{{ include "kitaru.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "kitaru.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kitaru.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "kitaru.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "kitaru.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- required "serviceAccount.name is required when serviceAccount.create is false" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "kitaru.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) }}
{{- end }}

{{- define "kitaru.environment" -}}
{{- $environment := deepCopy (.Values.server.environment | default dict) }}
{{- if .Values.migration.enabled }}
{{- $_ := set $environment "KITARU_SERVER_SKIP_DB_MIGRATION" "true" }}
{{- end }}
{{- range $name, $value := $environment }}
- name: {{ $name }}
  value: {{ $value | toString | quote }}
{{- end }}
{{- range $name, $_ := .Values.server.secretEnvironment }}
- name: {{ $name }}
  valueFrom:
    secretKeyRef:
      name: {{ include "kitaru.fullname" $ }}
      key: {{ $name }}
{{- end }}
{{- end }}

{{- define "kitaru.hookEnvironment" -}}
{{- range $name, $value := .Values.server.environment }}
- name: {{ $name }}
  value: {{ $value | toString | quote }}
{{- end }}
{{- range $name, $_ := .Values.server.secretEnvironment }}
- name: {{ $name }}
  valueFrom:
    secretKeyRef:
      name: {{ include "kitaru.fullname" $ }}-migration
      key: {{ $name }}
{{- end }}
{{- end }}
