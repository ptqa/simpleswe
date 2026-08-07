{{- define "simpleswe.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "simpleswe.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := include "simpleswe.name" . -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "simpleswe.namespace" -}}
{{- default .Release.Namespace .Values.namespace.name -}}
{{- end -}}

{{- define "simpleswe.serviceAccountName" -}}
{{- default (include "simpleswe.fullname" .) .Values.serviceAccount.name -}}
{{- end -}}

{{- define "simpleswe.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}
{{- end -}}

{{- define "simpleswe.hermesImage" -}}
{{- if .Values.hermes.image.digest -}}
{{- printf "%s@%s" .Values.hermes.image.repository .Values.hermes.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.hermes.image.repository .Values.hermes.image.tag -}}
{{- end -}}
{{- end -}}

{{- define "simpleswe.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" | quote }}
app.kubernetes.io/name: {{ include "simpleswe.name" . | quote }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
{{- end -}}

{{- define "simpleswe.selectorLabels" -}}
app.kubernetes.io/name: {{ include "simpleswe.name" . | quote }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
app.kubernetes.io/component: controller
{{- end -}}

{{- define "simpleswe.workerSelectorLabels" -}}
app.kubernetes.io/name: simpleswe
app.kubernetes.io/component: worker
app.kubernetes.io/managed-by: simpleswe
{{- end -}}
