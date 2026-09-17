{{- define "heos-control.name" -}}
{{- printf "%s-heos-control" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- define "heos-control.selector" -}}
app.kubernetes.io/name: heos-control
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
{{- define "heos-control.image" -}}
{{- if .Values.image.digest -}}
{{ .Values.image.repository }}@{{ .Values.image.digest }}
{{- else -}}
{{ .Values.image.repository }}:{{ .Values.image.tag }}
{{- end -}}
{{- end -}}

{{- define "heos-control.migrationServiceAccount" -}}
{{- default (printf "%s-migration" .Release.Name) .Values.migration.serviceAccountName -}}
{{- end -}}

{{- define "heos-control.migrationJobAnnotations" -}}
{{- $annotations := dict -}}
{{- if eq .Values.migration.hooks "helm" -}}
{{- $_ := set $annotations "helm.sh/hook" "pre-install,pre-upgrade" -}}
{{- $_ := set $annotations "helm.sh/hook-weight" "-1" -}}
{{- $_ := set $annotations "helm.sh/hook-delete-policy" "before-hook-creation,hook-succeeded" -}}
{{- end -}}
{{- toYaml (mergeOverwrite $annotations .Values.migration.jobAnnotations) -}}
{{- end -}}

{{- define "heos-control.migrationResourceAnnotations" -}}
{{- $annotations := dict -}}
{{- if eq .Values.migration.hooks "helm" -}}
{{- $_ := set $annotations "helm.sh/hook" "pre-install,pre-upgrade" -}}
{{- $_ := set $annotations "helm.sh/hook-weight" "-2" -}}
{{- $_ := set $annotations "helm.sh/hook-delete-policy" "before-hook-creation" -}}
{{- end -}}
{{- toYaml (mergeOverwrite $annotations .Values.migration.resourceAnnotations) -}}
{{- end -}}
