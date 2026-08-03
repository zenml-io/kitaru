{{/*
Helpers for environment variables configured in Kitaru deployments
*/}}


{{/*
Kitaru  configuration options (non-secret values).

This template constructs a dictionary that is similar to the environment
variables that can be configured for the Kitaru server. Only non-secret values
are included in this dictionary.

The dictionary is then converted into deployment environment variables by other
templates and inserted where it is needed.

The input is taken from a .Kitaru dict that is passed to the template and
contains the values configured in the values.yaml file for the Kitaru server.

Args:
  .Kitaru: A dictionary with the Kitaru configuration values configured for the
  Kitaru server.
Returns:
  A dictionary with the non-secret values configured for the Kitaru server.
*/}}
{{- define "kitaru.serverConfigurationAttrs" -}}
{{- if .Kitaru.database.host }}
db_host: {{ .Kitaru.database.host | quote }}
{{- end }}
{{- if .Kitaru.database.port }}
db_port: {{ .Kitaru.database.port | quote }}
{{- end }}
{{- if .Kitaru.database.username }}
db_user: {{ .Kitaru.database.username | quote }}
{{- end }}
{{- if .Kitaru.database.database }}
db_name: {{ .Kitaru.database.database | quote }}
{{- end }}
db_ssl_mode: {{ .Kitaru.database.sslMode | default "disable" | quote }}
{{- if .Kitaru.database.sslCa }}
db_ssl_ca: /dbcerts/ca.pem
{{- end }}
{{- if .Kitaru.database.sslCert }}
db_ssl_cert: /dbcerts/client-cert.pem
{{- end }}
{{- if .Kitaru.database.sslKey }}
db_ssl_key: /dbcerts/client-key.pem
{{- end }}
{{- if .Kitaru.database.poolSize }}
db_pool_size: {{ .Kitaru.database.poolSize | quote }}
{{- end }}
{{- if .Kitaru.database.maxOverflow }}
db_max_overflow: {{ .Kitaru.database.maxOverflow | quote }}
{{- end }}
{{- if .Kitaru.database.poolTimeoutSeconds }}
db_pool_timeout_seconds: {{ .Kitaru.database.poolTimeoutSeconds | quote }}
{{- end }}

{{- if .Kitaru.pro.enabled }}
auth_scheme: control_plane
control_plane_api_url: "{{ .Kitaru.pro.apiURL }}"
jwt_audience: "{{ .Kitaru.pro.apiURL }}"
jwt_issuer: "{{ .Kitaru.pro.apiURL }}"
auth_cookie_name: kitaru-server-{{ .Kitaru.pro.workspaceID }}
server_id: {{ .Kitaru.pro.workspaceID | quote }}
dashboard_url: {{ .Kitaru.pro.dashboardURL }}/workspaces/{{ .Kitaru.pro.workspaceID }}

{{- else }}

auth_scheme: {{ .Kitaru.authType | default .Kitaru.auth.authType | quote }}
{{- if .Kitaru.auth.authCookieName }}
auth_cookie_name: {{ .Kitaru.auth.authCookieName | quote }}
{{- end }}
{{- if .Kitaru.auth.corsAllowOrigins }}
cors_allow_origins: {{ join "," .Kitaru.auth.corsAllowOrigins | quote }}
{{- end }}
{{- if .Kitaru.auth.jwtIssuer }}
jwt_issuer: {{ .Kitaru.auth.jwtIssuer | quote }}
{{- end }}
{{- if .Kitaru.auth.jwtAudience }}
jwt_audience: {{ .Kitaru.auth.jwtAudience | quote }}
{{- end }}
{{- if .Kitaru.dashboardURL }}
dashboard_url: {{ .Kitaru.dashboardURL | quote }}
{{- end }}

{{- end }}

{{- if .Kitaru.auth.jwtLifetimeSeconds }}
jwt_lifetime_seconds: {{ .Kitaru.auth.jwtLifetimeSeconds | quote }}
{{- end }}
{{- if .Kitaru.auth.authCookieDomain }}
auth_cookie_domain: {{ .Kitaru.auth.authCookieDomain | quote }}
{{- end }}
{{- if .Kitaru.auth.maxFailedDeviceAuthAttempts }}
max_failed_device_auth_attempts: {{ .Kitaru.auth.maxFailedDeviceAuthAttempts | quote }}
{{- end }}
{{- if .Kitaru.auth.deviceAuthTimeoutSeconds }}
device_auth_timeout_seconds: {{ .Kitaru.auth.deviceAuthTimeoutSeconds | quote }}
{{- end }}
{{- if .Kitaru.auth.deviceAuthPollingIntervalSeconds }}
device_auth_polling_interval_seconds: {{ .Kitaru.auth.deviceAuthPollingIntervalSeconds | quote }}
{{- end }}
{{- if .Kitaru.auth.deviceExpirationMinutes }}
device_expiration_minutes: {{ .Kitaru.auth.deviceExpirationMinutes | quote }}
{{- end }}
{{- if .Kitaru.auth.trustedDeviceExpirationMinutes }}
trusted_device_expiration_minutes: {{ .Kitaru.auth.trustedDeviceExpirationMinutes | quote }}
{{- end }}
{{- if .Kitaru.rootUrlPath }}
root_url_path: {{ .Kitaru.rootUrlPath | quote }}
{{- end }}
{{- if .Kitaru.serverURL }}
server_url: {{ .Kitaru.serverURL | quote }}
{{- end }}
{{- with .Kitaru.openTelemetry }}
{{- if or .endpoint .tracesEndpoint .metricsEndpoint .logsEndpoint }}
{{- if .endpoint }}
otel_exporter_otlp_endpoint: {{ .endpoint | quote }}
{{- end }}
{{- if .serviceName }}
otel_service_name: {{ .serviceName | quote }}
{{- end }}
{{- if .tracesEndpoint }}
otel_exporter_otlp_traces_endpoint: {{ .tracesEndpoint | quote }}
{{- end }}
{{- if .metricsEndpoint }}
otel_exporter_otlp_metrics_endpoint: {{ .metricsEndpoint | quote }}
{{- end }}
{{- if .logsEndpoint }}
otel_exporter_otlp_logs_endpoint: {{ .logsEndpoint | quote }}
{{- end }}
{{- if hasKey . "tracesEnabled" }}
otel_traces_enabled: {{ .tracesEnabled | quote }}
{{- end }}
{{- if hasKey . "metricsEnabled" }}
otel_metrics_enabled: {{ .metricsEnabled | quote }}
{{- end }}
{{- if hasKey . "logsEnabled" }}
otel_logs_enabled: {{ .logsEnabled | quote }}
{{- end }}
{{- end }}
{{- end }}
{{- if .Kitaru.analyticsOptIn }}
analytics_opt_in: "True"
{{- else }}
analytics_opt_in: "False"
{{- end }}
log_level: {{ default "info" .Kitaru.logging.verbosity | upper | quote }}
{{- end }}


{{/*
Kitaru server configuration options (secret values).

This template constructs a dictionary that is similar to the environment
variables that can be configured for the Kitaru server. Only secret values are
included in this dictionary.

The dictionary is then converted into deployment environment variables by other
templates and inserted where it is needed.

The input is taken from a .Kitaru dict that is passed to the template and
contains the values configured in the values.yaml file for the Kitaru server.

Args:
  .Kitaru: A dictionary with the Kitaru configuration values configured for the
  Kitaru server.
Returns:
  A dictionary with the secret values configured for the Kitaru server.
*/}}
{{- define "kitaru.serverSecretConfigurationAttrs" -}}
{{- if .Kitaru.database.password }}
db_pwd: {{ .Kitaru.database.password | quote }}
{{- end }}
{{- end }}


{{/*
Server configuration environment variables (non-secret values).

Resolves server values and passes them as input to the
`kitaru.serverConfigurationAttrs` template, converting the output into a
dictionary of environment variables that need to be configured for the server.

Args:
  .: The root context containing .Values
Returns:
  A dictionary with the non-secret environment variables that are configured for
  the server (i.e. keys starting with `KITARU_SERVER_`).
*/}}
{{- define "kitaru.serverEnvVariables" -}}
{{ $kitaru := dict "Kitaru" .Values.server }}
{{- range $k, $v := include "kitaru.serverConfigurationAttrs" $kitaru | fromYaml }}
KITARU_SERVER_{{ $k | upper }}: {{ $v | quote }}
{{- end }}
{{- end }}

{{/*
Store configuration environment variables (secret values).

Resolves server values and passes them as input to the
`kitaru.serverSecretConfigurationAttrs` template, converting the output into a
dictionary of environment variables that need to be configured for the server.

Args:
  .: The root context containing .Values
Returns:
  A dictionary with the secret environment variables that are configured for
  the server (i.e. keys starting with `KITARU_SERVER_`).
*/}}
{{- define "kitaru.storeSecretEnvVariables" -}}
{{ $kitaru := dict "Kitaru" .Values.server }}
{{- range $k, $v := include "kitaru.serverSecretConfigurationAttrs" $kitaru | fromYaml }}
KITARU_SERVER_{{ $k | upper }}: {{ $v | quote }}
{{- end }}
{{- end }}

{{/*
Base environment variables for Kitaru deployments.

Returns a dictionary with common configuration env vars.
*/}}
{{- define "kitaru.baseEnvVariables" -}}
NODE_OPTIONS: "--use-openssl-ca"
{{- if or .Values.server.certificates.customCAs .Values.server.certificates.secretRefs }}
REQUESTS_CA_BUNDLE: "/updated-certs/ca-certificates.crt"
SSL_CERT_FILE: "/updated-certs/ca-certificates.crt"
{{- end }}

{{- if .Values.server.proxy.enabled }}
HTTP_PROXY: {{ .Values.server.proxy.httpProxy | quote }}
HTTPS_PROXY: {{ .Values.server.proxy.httpsProxy | quote }}
NO_PROXY: {{ include "kitaru.noProxyList" . | quote }}
http_proxy: {{ .Values.server.proxy.httpProxy | quote }}
https_proxy: {{ .Values.server.proxy.httpsProxy | quote }}
no_proxy: {{ include "kitaru.noProxyList" . | quote }}
{{- end }}
{{- end }}


{{/*
Complete environment variables for Kitaru server.

This template constructs a dictionary of all non-secret environment variables
needed for Kitaru deployments. It merges (in order of increasing precedence):
1. Base configuration (kitaru.baseEnvVariables)
2. Server configuration (kitaru.serverEnvVariables)
3. User-provided environment variables (server.environment)

Args:
  .: The root context containing .Values
Returns:
  A dictionary of environment variables ready to be converted to name/value pairs.
*/}}
{{- define "kitaru.envVariables" -}}
{{- $envVars := include "kitaru.baseEnvVariables" . | fromYaml | default dict }}
{{- $envVars = merge (include "kitaru.serverEnvVariables" . | fromYaml | default dict) $envVars }}
{{- $envVars = merge (.Values.server.environment | default dict) $envVars }}
{{ $envVars | toYaml }}
{{- end }}
