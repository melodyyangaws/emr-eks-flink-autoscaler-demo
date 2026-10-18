#!/usr/bin/env bash
# Run a query-benchmark step from INSIDE the cluster, so closing the laptop
# does not stop it. starrocks.sh already runs each arm inside the FE pod, but the
# harness itself (polling, reading the audit log, the warm DuckDB half through
# port-forwards) ran on the laptop and died with it.
#
#   ./qb-runner.sh start <cmd...>   ship querybench/ + .bench-env(.local) and run
#                                   <cmd> in a Job (qb-runner) on driver-nodepool
#   ./qb-runner.sh logs             follow the Job's output
#   ./qb-runner.sh fetch [dir]      copy its bench-results/ out (default ../bench-results)
#   ./qb-runner.sh delete           delete the Job, its ConfigMap and Secret
#
# <cmd> runs in throughput-benchmark/ with .bench-env sourced, e.g.
#   ./qb-runner.sh start 'WAIT_FOR_SR_RUN=20261017T140929Z_71523 STAMP=... ./querybench/duckdb-warm.sh run'
# WAIT_FOR_SR_RUN=<RUN_ID> first waits until that starrocks.sh run has nothing
# left in the FE pod (no running arm file, no cid_events query), so the two
# engines never overlap.
#
# The pod has kubectl (in-cluster ServiceAccount qb-runner, a namespace Role:
# pods exec/port-forward, deployments restart, flinkdeployments read/patch),
# bash, python3, curl. It has NO AWS credentials: give it pinned windows
# (SNAPSHOT_DT, SNAPSHOT_S4_FROM/TO; HOT_TO) so lib.sh never calls aws. The DuckDB
# servers and StarRocks read S3 with their own roles. After <cmd> the pod stays
# up 7 days so `fetch` can copy the results; `delete` ends it.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TB="$(cd "${HERE}/.." && pwd)"
NAMESPACE="${NAMESPACE:-emr-flink}"
NAME=qb-runner

rbac() {
    kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: ServiceAccount
metadata: {name: ${NAME}, namespace: ${NAMESPACE}}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: ${NAME}, namespace: ${NAMESPACE}}
rules:
- {apiGroups: [""], resources: [pods, pods/log], verbs: [get, list, watch]}
- {apiGroups: [""], resources: [pods/exec, pods/portforward], verbs: [create, get]}
- {apiGroups: [apps], resources: [deployments, replicasets], verbs: [get, list, watch, patch]}
- {apiGroups: [flink.apache.org], resources: [flinkdeployments], verbs: [get, list, patch]}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {name: ${NAME}, namespace: ${NAMESPACE}}
subjects: [{kind: ServiceAccount, name: ${NAME}, namespace: ${NAMESPACE}}]
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: Role, name: ${NAME}}
EOF
}

cmd_start() {
    [[ $# -gt 0 ]] || { echo "usage: $0 start '<cmd>'" >&2; exit 1; }
    local d; d=$(mktemp -d)
    # The command and the wait go in a file in the bundle: no quoting through
    # the Job's YAML.
    cat > "${d}/cmd.sh" <<'EOS'
#!/bin/bash
set -uo pipefail
cd /work
source .bench-env
FE_POD="${FE_POD:-kube-starrocks-fe-0}"
fe() { kubectl exec -n "$NAMESPACE" "$FE_POD" -c fe -- "$@" 2>/dev/null; }
# Wait until the given starrocks.sh run has nothing left in the FE pod: no arm
# script without its exit file, and no cid_events query running.
# grep -c exits 1 when it counts 0, hence the `; true`: without it the idle
# answer "0" also tripped `|| echo unreachable` and the wait never ended (2026-10-17).
wait_for_sr_run() {   # <RUN_ID>
    local busy
    while :; do
        busy=$(fe sh -c "for s in /tmp/qb-$1-*.sql; do [ -f \"\$s\" ] && [ ! -f \"\${s%.sql}.rc\" ] && echo \"\$s\"; done; mysql -h127.0.0.1 -P9030 -uroot -N -B -e 'SHOW PROCESSLIST' | grep -c cid_events; true" || echo unreachable)
        [[ "$busy" == 0 ]] && { echo "== StarRocks run $1 finished $(date -u)"; return 0; }
        echo "   waiting for StarRocks run $1: $(echo $busy | tr '\n' ' ') $(date -u +%H:%M:%SZ)"
        sleep 60
    done
}
[[ -n "${WAIT_FOR_SR_RUN:-}" ]] && wait_for_sr_run "$WAIT_FOR_SR_RUN"
EOS
    printf '%s\n' "$*" >> "${d}/cmd.sh"
    cp "${d}/cmd.sh" "${TB}/.qb-runner-cmd.sh"
    tar -C "$TB" -czf "${d}/bundle.tgz" querybench .bench-env .qb-runner-cmd.sh
    rm -f "${TB}/.qb-runner-cmd.sh"
    kubectl delete job "$NAME" -n "$NAMESPACE" --ignore-not-found --wait=true >/dev/null
    kubectl delete configmap "$NAME" -n "$NAMESPACE" --ignore-not-found >/dev/null
    kubectl delete secret "$NAME" -n "$NAMESPACE" --ignore-not-found >/dev/null
    kubectl create configmap "$NAME" -n "$NAMESPACE" --from-file="${d}/bundle.tgz" >/dev/null
    kubectl create secret generic "$NAME" -n "$NAMESPACE" --from-file=bench-env-local="${TB}/.bench-env.local" >/dev/null
    rm -rf "$d"
    rbac
    kubectl apply -f - >/dev/null <<EOF
apiVersion: batch/v1
kind: Job
metadata: {name: ${NAME}, namespace: ${NAMESPACE}, labels: {app: ${NAME}}}
spec:
  backoffLimit: 0                       # a re-run would requery and double-record
  activeDeadlineSeconds: 691200         # 8 days: <cmd> + the 7-day fetch window
  template:
    metadata:
      labels: {app: ${NAME}}
      annotations:
        karpenter.sh/do-not-disrupt: "true"
        instrumentation.opentelemetry.io/inject-java: "false"
        instrumentation.opentelemetry.io/inject-python: "false"
        instrumentation.opentelemetry.io/inject-nodejs: "false"
        instrumentation.opentelemetry.io/inject-dotnet: "false"
    spec:
      restartPolicy: Never
      serviceAccountName: ${NAME}
      nodeSelector:
        karpenter.sh/nodepool: driver-nodepool
        karpenter.sh/capacity-type: on-demand
        kubernetes.io/arch: amd64
      containers:
      - name: runner
        image: python:3.12-slim
        resources: {requests: {cpu: "500m", memory: 1Gi}, limits: {cpu: "1", memory: 2Gi}}
        env: [{name: NAMESPACE, value: "${NAMESPACE}"}]
        volumeMounts:
        - {name: bundle, mountPath: /bundle}
        - {name: env, mountPath: /env}
        command: ["/bin/bash", "-c"]
        args:
        - |
          set -uo pipefail
          apt-get update -qq >/dev/null && apt-get install -y -qq curl procps >/dev/null
          curl -fsSLo /usr/local/bin/kubectl "https://dl.k8s.io/release/\$(curl -fsSL https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
          chmod +x /usr/local/bin/kubectl
          mkdir -p /work && tar -C /work -xzf /bundle/bundle.tgz && cp /env/bench-env-local /work/.bench-env.local
          mkdir -p /work/bench-results && cd /work
          echo "== runner \$(date -u)"; tail -1 .qb-runner-cmd.sh
          bash .qb-runner-cmd.sh; rc=\$?
          echo "== runner-exit=\${rc} \$(date -u)"
          for f in bench-results/*; do echo "===FILE \$f"; cat "\$f"; done
          sleep 604800
      volumes:
      - {name: bundle, configMap: {name: ${NAME}}}
      - {name: env, secret: {secretName: ${NAME}}}
EOF
    echo "job/${NAME} started: $*"
}

case "${1:-}" in
    start)  shift; cmd_start "$@" ;;
    logs)   kubectl logs -f -n "$NAMESPACE" "job/${NAME}" ;;
    fetch)  p=$(kubectl get pods -n "$NAMESPACE" -l job-name="$NAME" -o jsonpath='{.items[0].metadata.name}')
            kubectl cp -n "$NAMESPACE" "${p}:/work/bench-results" "${2:-${TB}/bench-results}" ;;
    delete) kubectl delete job "$NAME" -n "$NAMESPACE" --ignore-not-found
            kubectl delete configmap "$NAME" -n "$NAMESPACE" --ignore-not-found
            kubectl delete secret "$NAME" -n "$NAMESPACE" --ignore-not-found ;;
    *)      echo "usage: $0 start '<cmd>' | logs | fetch [dir] | delete" >&2; exit 1 ;;
esac
