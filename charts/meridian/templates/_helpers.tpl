{{- define "meridian.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "meridian.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "meridian.watchNamespace" -}}
{{- default .Release.Namespace .Values.watchNamespace -}}
{{- end -}}

{{- define "meridian.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "meridian.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "meridian.operator.fullname" -}}
{{- printf "%s-operator" (include "meridian.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "meridian.operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "meridian.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: operator
{{- end -}}

{{- define "meridian.operator.restName" -}}
{{- printf "%s-operator-rest" (include "meridian.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "meridian.mcp.fullname" -}}
{{- printf "%s-mcp" (include "meridian.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "meridian.mcp.selectorLabels" -}}
app.kubernetes.io/name: {{ include "meridian.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: mcp
{{- end -}}

{{/* Restricted pod security settings shared by both workloads. */}}
{{- define "meridian.podSecurityContext" -}}
runAsNonRoot: true
runAsUser: 65532
runAsGroup: 65532
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{- define "meridian.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: ["ALL"]
{{- end -}}

{{/* MCP held tools enabled via values, as a list. "all" expands to every held tool. */}}
{{- define "meridian.mcp.tools" -}}
{{- $held := list "create_cluster" "delete_cluster" "scale_pool" "add_catalog" "remove_catalog" "rotate_credentials" "promote_environment" -}}
{{- $tools := .Values.mcp.enableTools | default list -}}
{{- if kindIs "string" $tools -}}
{{- $tools = splitList "," $tools -}}
{{- end -}}
{{- range $tools -}}
{{- if and (ne . "all") (not (has . $held)) -}}
{{- fail (printf "mcp.enableTools: unknown tool %q (held tools: %s, or all)" . (join ", " $held)) -}}
{{- end -}}
{{- end -}}
{{- if has "all" $tools -}}
{{- $tools = $held -}}
{{- end -}}
{{- toJson $tools -}}
{{- end -}}
