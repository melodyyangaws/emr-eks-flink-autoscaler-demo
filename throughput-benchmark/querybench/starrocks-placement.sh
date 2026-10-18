#!/usr/bin/env bash
# Pin the StarRocks cluster to Karpenter's driver-nodepool, next to the warm
# DuckDB servers (duckdb-warm.yaml), so both engines run on the same pool, the
# same CPU family and nodes that are never consolidated while busy.
#
# Why: the CR had no nodeSelector at all. On 2026-10-17 the FE and two BEs sat on
# managed-nodegroup m5.4xlarge nodes, BE-1 and BE-2 SHARING one, while a
# restarted BE-0 had landed on an executor-memorynodepool m5.8xlarge: three BEs,
# three different neighbourhoods. driver-nodepool is on-demand only and
# consolidates only EMPTY nodes, so a node running a BE is never taken away.
#
# This patches the live StarRocksCluster. cdc-pipeline/helm/starrocks-eks-values.yaml
# (cdc-pipeline is read-only for this benchmark) does not carry it, so a
# `helm upgrade` from there drops it: rerun this script afterwards.
#
#   ./starrocks-placement.sh          patch, then wait for FE + BEs to be Ready
#   ./starrocks-placement.sh show     where each pod runs now
#
# No auto-instrumentation: the cluster's CloudWatch Observability add-on
# auto-annotated both StatefulSets after the first placement patch (2026-10-17),
# injecting the OpenTelemetry Java agent (JAVA_TOOL_OPTIONS=-javaagent, X-Ray
# sampling) into the FE's JVM and into each BE, whose embedded JVM runs the
# Java-based readers (native Paimon among them). The warm DuckDB pods were not
# annotated, so that was a tracer on one engine only. The pod annotations below
# opt both components out; `show` reports any agent that comes back.
#
# The operator rolls the pods one at a time. The PVCs are zonal EBS volumes, so
# Karpenter launches each node in its volume's zone. BE datacache lives on those
# PVCs and survives the move: clear it before a scoring run.
set -euo pipefail
NAMESPACE="${NAMESPACE:-emr-flink}"
CR="${CR:-kube-starrocks}"
POOL="${POOL:-driver-nodepool}"
FAMILY="${FAMILY:-m5}"              # = the warm DuckDB servers
BE_SIZE="${BE_SIZE:-2xlarge}"       # 8 vcpu / 32Gi per BE: its 6 cpu / 24Gi limit + daemonsets
# Each BE has a node to itself: BEs repel each other, and the FE repels BEs (it
# first landed on BE-0's node, which then shared its CPU with the FE).
FE_SIZE="${FE_SIZE:-2xlarge}"       # FE limit 4 cpu / 16Gi

show() {
    local p n
    for p in $(kubectl get pods -n "$NAMESPACE" -l app.starrocks.ownerreference/name -o name | sort); do
        n=$(kubectl get -n "$NAMESPACE" "$p" -o jsonpath='{.spec.nodeName}')
        printf '%-28s %s  otel=%s\n' "${p#pod/}" "$(kubectl get node "$n" --no-headers \
            -L karpenter.sh/nodepool,node.kubernetes.io/instance-type,topology.kubernetes.io/zone | awk '{print $1, $6, $7, $8}')" \
            "$(kubectl get -n "$NAMESPACE" "$p" -o jsonpath='{.spec.containers[*].env[?(@.name=="JAVA_TOOL_OPTIONS")].value}' | grep -q javaagent && echo INJECTED || echo none)"
    done
}

patch() {
    kubectl patch starrockscluster "$CR" -n "$NAMESPACE" --type=merge -p "$(cat <<EOF
{"spec": {
  "starRocksFeSpec": {
    "annotations": {"instrumentation.opentelemetry.io/inject-java": "false", "instrumentation.opentelemetry.io/inject-python": "false",
                    "instrumentation.opentelemetry.io/inject-nodejs": "false", "instrumentation.opentelemetry.io/inject-dotnet": "false"},
    "nodeSelector": {"karpenter.sh/nodepool": "${POOL}", "kubernetes.io/arch": "amd64",
                     "karpenter.k8s.aws/instance-family": "${FAMILY}", "karpenter.k8s.aws/instance-size": "${FE_SIZE}",
                     "karpenter.sh/capacity-type": "on-demand"},
    "affinity": {"podAntiAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [
      {"labelSelector": {"matchLabels": {"app.kubernetes.io/component": "be"}},
       "topologyKey": "kubernetes.io/hostname"}]}}
  },
  "starRocksBeSpec": {
    "annotations": {"instrumentation.opentelemetry.io/inject-java": "false", "instrumentation.opentelemetry.io/inject-python": "false",
                    "instrumentation.opentelemetry.io/inject-nodejs": "false", "instrumentation.opentelemetry.io/inject-dotnet": "false"},
    "nodeSelector": {"karpenter.sh/nodepool": "${POOL}", "kubernetes.io/arch": "amd64",
                     "karpenter.k8s.aws/instance-family": "${FAMILY}", "karpenter.k8s.aws/instance-size": "${BE_SIZE}",
                     "karpenter.sh/capacity-type": "on-demand"},
    "affinity": {"podAntiAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [
      {"labelSelector": {"matchLabels": {"app.kubernetes.io/component": "be"}},
       "topologyKey": "kubernetes.io/hostname"}]}}
  }
}}
EOF
)"
    echo "patched ${CR}: FE ${FAMILY}.${FE_SIZE} off the BE nodes, BEs ${FAMILY}.${BE_SIZE} one per node, on ${POOL}"
    sleep 20
    kubectl rollout status -n "$NAMESPACE" statefulset/kube-starrocks-fe --timeout=1200s
    kubectl rollout status -n "$NAMESPACE" statefulset/kube-starrocks-be --timeout=2400s
    show
}

case "${1:-patch}" in
    patch) patch ;;
    show)  show ;;
    *)     echo "usage: $0 [patch|show]" >&2; exit 1 ;;
esac
