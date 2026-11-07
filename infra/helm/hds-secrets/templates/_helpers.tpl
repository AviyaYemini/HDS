{{- define "hds-secrets.labels" -}}
app.kubernetes.io/name: hds-secrets
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/part-of: hds
app.kubernetes.io/component: secrets
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}
