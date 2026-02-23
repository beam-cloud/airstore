{{- define "airstore.init" -}}
  {{/* Hack to disable main controller */}}
  {{- $_ := include "airstore.disable-main-controller" . | fromYaml | merge .Values -}}

  {{/* Make sure all variables are set properly */}}
  {{- include "bjw-s.common.loader.init" . }}

  {{/* Enforce default values */}}
  {{- $_ := include "airstore" . | fromYaml | merge .Values -}}
{{- end -}}


{{/* Disable main controller */}}
{{- define "airstore.disable-main-controller" -}}
controllers:
  main:
    enabled: false
service:
  main:
    enabled: false
ingress:
  main:
    enabled: false
route:
  main:
    enabled: false
serviceMonitor:
  main:
    enabled: false
networkpolicies:
  main:
    enabled: false
{{- end -}}


{{/* Define hard coded defaults */}}
{{- define "airstore" -}}
controllers:
  gateway:
    type: deployment
    annotations:
      secret-hash: {{ include "sha256sum" (toYaml .Values.config) }}
    containers:
      main:
        command:
        - /usr/local/bin/gateway
        image:
          repository: {{ .Values.images.gateway.repository }}
          tag: "{{ .Values.images.gateway.tag | default .Chart.AppVersion }}"
          pullPolicy: {{ .Values.images.gateway.pullPolicy | default "IfNotPresent" }}
        probes:
          startup:
            enabled: true
            custom: true
            spec:
              failureThreshold: 45
              periodSeconds: 2
              timeoutSeconds: 1
              httpGet:
                path: /api/v1/health/live
                port: 1994
          readiness:
            enabled: true
            custom: true
            spec:
              initialDelaySeconds: 2
              successThreshold: 1
              failureThreshold: 3
              periodSeconds: 3
              timeoutSeconds: 1
              httpGet:
                path: /api/v1/health/ready
                port: 1994
          liveness:
            enabled: true
            custom: true
            spec:
              initialDelaySeconds: 10
              successThreshold: 1
              failureThreshold: 10
              periodSeconds: 5
              timeoutSeconds: 1
              httpGet:
                path: /api/v1/health/live
                port: 1994
        securityContext:
          privileged: true
    pod:
      labels:
        airstore.io/component: gateway
    hostNetwork: true
service:
  gateway:
    controller: gateway
ingress:
  gateway:
    controller: gateway
serviceAccount:
  create: true
persistence:
  config-helm:
    enabled: {{ if .Values.config }}true{{ else }}false{{ end }}
    type: secret
    name: airstore-config-helm
    globalMounts:
    - path: /etc/airstore.d/config.yaml
      subPath: config.yaml
      readOnly: true
{{- end -}}


{{/*Adds labels to custom manifests.*/}}
{{- define "manifests.metadata" -}}
metadata:
  labels:
    {{ include "bjw-s.common.lib.metadata.allLabels" . | nindent 4 }}
{{- end -}}


{{- define "sha256sum" -}}
{{- printf "%s" (. | sha256sum) | quote -}}
{{- end -}}
