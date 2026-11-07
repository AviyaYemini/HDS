{{/* The release's name: hds-postgres, which the hds release's values name as the database host. */}}
{{- define "hds-postgres.name" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "hds-postgres.labels" -}}
app.kubernetes.io/name: hds-postgres
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: database
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "hds-postgres.selector" -}}
app.kubernetes.io/name: hds-postgres
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
