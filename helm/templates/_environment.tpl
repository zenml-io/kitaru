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
{{- if .Kitaru.database.user }}
db_user: {{ .Kitaru.database.user | quote }}
{{- end }}
{{- if .Kitaru.database.database }}
db_name: {{ .Kitaru.database.database | quote }}
{{- end }}
{{- if .Kitaru.database.ssl }}
db_ssl: {{ .Kitaru.database.ssl | quote }}
{{- end }}
{{- if and .Kitaru.database.sslCa .Kitaru.database.sslCa.secretRef }}
db_ssl_ca: /dbcerts/{{ .Kitaru.database.sslCa.secretRef.key }}
{{- end }}
{{- if and .Kitaru.database.sslCert .Kitaru.database.sslCert.secretRef }}
db_ssl_cert: /dbcerts/{{ .Kitaru.database.sslCert.secretRef.key }}
{{- end }}
{{- if and .Kitaru.database.sslKey .Kitaru.database.sslKey.secretRef }}
db_ssl_key: /dbcerts/{{ .Kitaru.database.sslKey.secretRef.key }}
{{- end }}
db_ssl_verify_server_cert: {{ .Kitaru.database.sslVerifyServerCert | quote }}
{{- if .Kitaru.database.poolSize }}
db_pool_size: {{ .Kitaru.database.poolSize | quote }}
{{- end }}
{{- if .Kitaru.database.maxOverflow }}
db_max_overflow: {{ .Kitaru.database.maxOverflow | quote }}
{{- end }}

{{- if .Kitaru.pro.enabled }}
deployment_type: cloud
pro_api_url: "{{ .Kitaru.pro.apiURL }}"
pro_dashboard_url: "{{ .Kitaru.pro.dashboardURL }}"
pro_oauth2_audience: "{{ .Kitaru.pro.apiURL }}"
pro_organization_id: "{{ .Kitaru.pro.organizationID }}"
pro_workspace_id: "{{ .Kitaru.pro.workspaceID }}"
{{- if .Kitaru.pro.workspaceName }}
pro_workspace_name: "{{ .Kitaru.pro.workspaceName }}"
{{- end }}
{{- if .Kitaru.pro.organizationName }}
pro_organization_name: "{{ .Kitaru.pro.organizationName }}"
{{- end }}
{{- if .Kitaru.pro.extraCorsOrigins }}
cors_allow_origins: "{{ join "," .Kitaru.pro.extraCorsOrigins }}"
{{- end }}
{{- if .Kitaru.auth.jwtTokenExpireMinutes }}
jwt_token_expire_minutes: {{ .Kitaru.auth.jwtTokenExpireMinutes | quote }}
{{- end }}

{{- else }}

auth_scheme: {{ .Kitaru.authType | default .Kitaru.auth.authType | quote }}
deployment_type: {{ .Kitaru.deploymentType | default "kubernetes" }}
{{- if .Kitaru.auth.corsAllowOrigins }}
cors_allow_origins: {{ join "," .Kitaru.auth.corsAllowOrigins | quote }}
{{- end }}
{{- if .Kitaru.auth.externalLoginURL }}
external_login_url: {{ .Kitaru.auth.externalLoginURL | quote }}
{{- end }}
{{- if .Kitaru.auth.externalUserInfoURL }}
external_user_info_url: {{ .Kitaru.auth.externalUserInfoURL | quote }}
{{- end }}
{{- if .Kitaru.auth.externalServerID }}
external_server_id: {{ .Kitaru.auth.externalServerID | quote }}
{{- end }}
{{- if .Kitaru.auth.jwtTokenExpireMinutes }}
jwt_token_expire_minutes: {{ .Kitaru.auth.jwtTokenExpireMinutes | quote }}
{{- end }}
{{- if .Kitaru.dashboardURL }}
dashboard_url: {{ .Kitaru.dashboardURL | quote }}
{{- end }}

{{- end }}

{{- if .Kitaru.auth.jwtTokenAlgorithm }}
jwt_token_algorithm: {{ .Kitaru.auth.jwtTokenAlgorithm | quote }}
{{- end }}
{{- if .Kitaru.auth.jwtTokenIssuer }}
jwt_token_issuer: {{ .Kitaru.auth.jwtTokenIssuer | quote }}
{{- end }}
{{- if .Kitaru.auth.jwtTokenAudience }}
jwt_token_audience: {{ .Kitaru.auth.jwtTokenAudience | quote }}
{{- end }}
{{- if .Kitaru.auth.jwtTokenLeewaySeconds }}
jwt_token_leeway_seconds: {{ .Kitaru.auth.jwtTokenLeewaySeconds | quote }}
{{- end }}
{{- if .Kitaru.auth.authCookieName }}
auth_cookie_name: {{ .Kitaru.auth.authCookieName | quote }}
{{- end }}
{{- if .Kitaru.auth.authCookieDomain }}
auth_cookie_domain: {{ .Kitaru.auth.authCookieDomain | quote }}
{{- end }}
{{- if .Kitaru.rootUrlPath }}
root_url_path: {{ .Kitaru.rootUrlPath | quote }}
{{- end }}
{{- if .Kitaru.serverURL }}
server_url: {{ .Kitaru.serverURL | quote }}
{{- end }}

{{- range $key, $value := .Kitaru.secure_headers }}
secure_headers_{{ $key }}: {{ $value | quote }}
{{- end }}
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
{{- if and .Kitaru.database.sslCa .Kitaru.database.sslCa.value }}
ssl_ca: {{ .Kitaru.database.sslCa.value | quote }}
{{- end }}
{{- if and .Kitaru.database.sslCert .Kitaru.database.sslCert.value }}
ssl_cert: {{ .Kitaru.database.sslCert.value | quote }}
{{- end }}
{{- if and .Kitaru.database.sslKey .Kitaru.database.sslKey.value }}
ssl_key: {{ .Kitaru.database.sslKey.value | quote }}
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
{{- $server := include "kitaru.serverValues" . | fromYaml -}}
{{ $kitaru := dict "Kitaru" $server }}
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
{{- $server := include "kitaru.serverValues" . | fromYaml -}}
{{ $kitaru := dict "Kitaru" $server }}
{{- range $k, $v := include "kitaru.storeSecretConfigurationAttrs" $kitaru | fromYaml }}
KITARU_SERVER_{{ $k | upper }}: {{ $v | quote }}
{{- end }}
{{- end }}

{{/*
Base environment variables for Kitaru deployments.

Returns a dictionary with common configuration env vars.
*/}}
{{- define "kitaru.baseEnvVariables" -}}
{{- $server := include "kitaru.serverValues" . | fromYaml -}}
{{- $logging := $server.logging | default dict -}}
ZENML_SERVER: "True"
NODE_OPTIONS: "--use-openssl-ca"
{{- if or $server.certificates.customCAs $server.certificates.secretRefs }}
REQUESTS_CA_BUNDLE: "/updated-certs/ca-certificates.crt"
SSL_CERT_FILE: "/updated-certs/ca-certificates.crt"
{{- end }}
ZENML_LOGGING_VERBOSITY: {{ default "info" $logging.verbosity | upper | quote }}
{{- with $logging }}
{{- if .format }}
ZENML_CONSOLE_LOGGING_FORMAT: {{ .format | quote }}
{{- end }}
{{- if hasKey . "colorsDisabled" }}
ZENML_LOGGING_COLORS_DISABLED: {{ .colorsDisabled | quote }}
{{- end }}
{{- end }}
{{- if $server.analyticsOptIn }}
ZENML_ANALYTICS_OPT_IN: "True"
{{- else }}
ZENML_ANALYTICS_OPT_IN: "False"
{{- end }}

{{- if $server.proxy.enabled }}
HTTP_PROXY: {{ $server.proxy.httpProxy | quote }}
HTTPS_PROXY: {{ $server.proxy.httpsProxy | quote }}
NO_PROXY: {{ include "kitaru.noProxyList" . | quote }}
http_proxy: {{ $server.proxy.httpProxy | quote }}
https_proxy: {{ $server.proxy.httpsProxy | quote }}
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
{{- $server := include "kitaru.serverValues" . | fromYaml -}}
{{- $envVars := include "kitaru.baseEnvVariables" . | fromYaml | default dict }}
{{- $envVars = merge (include "kitaru.serverEnvVariables" . | fromYaml | default dict) $envVars }}
{{- $envVars = merge ($server.environment | default dict) $envVars }}
{{ $envVars | toYaml }}
{{- end }}
