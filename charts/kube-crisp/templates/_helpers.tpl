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

{{/*
Admission plugins to disable because the cluster predates the API they watch.
Each policy plugin watches the v1 API for its policies and refuses every write
until that watch has synced; a cluster without the API answers the watch with
not found, so the plugin never becomes ready and every projected write fails
with "not yet ready to handle request". Disabling the plugin there is what
keeps the rest of the chain usable. v1 ValidatingAdmissionPolicy arrived in
1.30 and v1 MutatingAdmissionPolicy in 1.36. Outside a cluster, helm template
uses Helm's own idea of the version; pass --kube-version to match the target.
*/}}
{{- define "kube-crisp.disabledAdmissionPlugins" -}}
{{- $plugins := list -}}
{{- if semverCompare "<1.30.0-0" .Capabilities.KubeVersion.Version -}}
{{- $plugins = append $plugins "ValidatingAdmissionPolicy" -}}
{{- end -}}
{{- if semverCompare "<1.36.0-0" .Capabilities.KubeVersion.Version -}}
{{- $plugins = append $plugins "MutatingAdmissionPolicy" -}}
{{- end -}}
{{- join "," $plugins -}}
{{- end -}}
