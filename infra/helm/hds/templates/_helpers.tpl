{{/* The release's name, hds: every object is hds-<part>. */}}
{{- define "hds.name" -}}
{{- .Release.Name | trunc 50 | trimSuffix "-" -}}
{{- end -}}

{{/* Labels. part-of: hds groups the release; it grants nothing. */}}
{{- define "hds.labels" -}}
app.kubernetes.io/name: hds
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
app.kubernetes.io/part-of: hds
app.kubernetes.io/version: {{ .root.Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
helm.sh/chart: {{ .root.Chart.Name }}-{{ .root.Chart.Version }}
{{- end -}}

{{/*
The label the hds-postgres NetworkPolicy lets in (research R3, amended): only on the pods that use
the database, the API, the worker, the migration Job and (Group H) the backup jobs; never the web.
*/}}
{{- define "hds.databaseClient" -}}
hds/database-client: "true"
{{- end -}}

{{- define "hds.selector" -}}
app.kubernetes.io/name: hds
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/* An image: by digest when one is set, else by tag (the chart's appVersion by default). */}}
{{- define "hds.image" -}}
{{- $image := index .root.Values.images .part -}}
{{- if $image.digest -}}
{{ $image.repository }}@{{ $image.digest }}
{{- else -}}
{{ $image.repository }}:{{ $image.tag | default .root.Chart.AppVersion }}
{{- end -}}
{{- end -}}

{{/* Non-root, nothing writable but the mounted folders, no privilege to gain. */}}
{{- define "hds.containerSecurity" -}}
runAsNonRoot: true
readOnlyRootFilesystem: true
allowPrivilegeEscalation: false
capabilities:
  drop: ["ALL"]
{{- end -}}

{{- define "hds.podSecurity" -}}
runAsNonRoot: true
runAsUser: {{ .uid }}
runAsGroup: {{ .uid }}
fsGroup: {{ .uid }}
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{/*
The API image's settings, shared by the API, the worker and the migration Job. Inline rather than a
ConfigMap: the migration Job is a pre-install hook, which runs before the chart's own objects exist.
DATABASE_PASSWORD comes first, as DATABASE_URL refers to it; it must be URL-safe (hex, as made by
`openssl rand -hex`). The restore test passes databaseUrl, its own throwaway database, instead.
*/}}
{{- define "hds.apiEnv" -}}
{{- $v := .root.Values -}}
- name: ENVIRONMENT
  value: {{ $v.settings.environment | quote }}
- name: TRUSTED_PROXY_COUNT
  value: {{ $v.settings.trustedProxyCount | quote }}
- name: METRICS_PORT
  value: "9100"
- name: SCHEDULE_TIME_LIMIT_SECONDS
  value: {{ $v.settings.scheduleTimeLimitSeconds | quote }}
- name: CORS_ORIGINS
  value: {{ $v.settings.corsOrigins | toJson | quote }}
- name: MAIL_BACKEND
  value: {{ $v.mail.backend | quote }}
- name: OTEL_SERVICE_NAME
  value: {{ .service | quote }}
{{- with $v.settings.otlpEndpoint }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ . | quote }}
{{- end }}
- name: JWT_SECRET_KEY
  valueFrom:
    secretKeyRef: {name: hds-api, key: JWT_SECRET_KEY}
- name: THROTTLE_SECRET_KEY
  valueFrom:
    secretKeyRef: {name: hds-api, key: THROTTLE_SECRET_KEY, optional: true}
{{- if .databaseUrl }}
- name: DATABASE_URL
  value: {{ .databaseUrl | quote }}
{{- else }}
- name: DATABASE_PASSWORD
  valueFrom:
    secretKeyRef: {name: hds-api, key: DATABASE_PASSWORD}
- name: DATABASE_URL
  value: "postgresql+asyncpg://{{ $v.database.user }}:$(DATABASE_PASSWORD)@{{ $v.database.host }}:{{ $v.database.port }}/{{ $v.database.name }}"
{{- end }}
{{- end -}}

{{/* The relay's settings, from the hds-smtp Secret, when mail goes over SMTP. */}}
{{- define "hds.apiEnvFrom" -}}
{{- if eq .root.Values.mail.backend "smtp" }}
envFrom:
  - secretRef:
      name: hds-smtp
{{- end }}
{{- end -}}

{{/* The writable folders of a read-only root: /tmp, and the outbox in a test environment. */}}
{{- define "hds.apiMounts" -}}
- name: tmp
  mountPath: /tmp
{{- if eq .root.Values.mail.backend "outbox" }}
- name: outbox
  mountPath: /app/.dev-mail
{{- end }}
{{- end -}}

{{- define "hds.apiVolumes" -}}
- name: tmp
  emptyDir: {}
{{- if eq .root.Values.mail.backend "outbox" }}
- name: outbox
  emptyDir: {}
{{- end }}
{{- end -}}


{{/* The backup scripts' database and remote settings; the remotes' secrets are in hds-backup. */}}
{{- define "hds.backupEnv" -}}
{{- $v := .root.Values -}}
- name: HDS_BACKUP_DIR
  value: /backups
- name: HDS_OFFSITE_PATH
  value: {{ $v.backup.offsitePath | quote }}
- name: HDS_RETENTION_DAYS
  value: {{ $v.backup.retentionDays | quote }}
{{- end -}}

{{/* A backup-image container: non-root (the image's uid 999), /tmp writable, nothing else. */}}
{{- define "hds.backupContainer" -}}
image: {{ include "hds.image" (dict "root" .root "part" "backup") }}
imagePullPolicy: {{ .root.Values.images.pullPolicy }}
envFrom:
  - secretRef:
      name: hds-backup
securityContext:
  {{- include "hds.containerSecurity" . | nindent 2 }}
resources:
  {{- toYaml .root.Values.backup.resources | nindent 2 }}
{{- end -}}
