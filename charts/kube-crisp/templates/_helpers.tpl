{{/* Chart name, overridable. */}}
{{- define "kube-crisp.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Fully qualified release name. */}}
{{- define "kube-crisp.fullname" -}}
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

{{- define "kube-crisp.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "kube-crisp.labels" -}}
helm.sh/chart: {{ include "kube-crisp.chart" . }}
{{ include "kube-crisp.selectorLabels" . }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "kube-crisp.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kube-crisp.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "kube-crisp.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "kube-crisp.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Namespaces a data source Secret may be read from. Defaults to the release
namespace, which is also the only namespace the Role below grants.
*/}}
{{- define "kube-crisp.dataSourceNamespaces" -}}
{{- if .Values.crisp.dataSourceNamespaces -}}
{{- .Values.crisp.dataSourceNamespaces | join "," -}}
{{- else -}}
{{- .Release.Namespace -}}
{{- end -}}
{{- end -}}
