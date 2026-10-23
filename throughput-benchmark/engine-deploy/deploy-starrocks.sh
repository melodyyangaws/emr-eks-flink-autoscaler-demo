#!/usr/bin/env bash
###############################################################################
# StarRocks for the query tests: operator + FE/BE cluster as one Helm release,
# with the BEs' volumes on a gp3 class sized to match DuckDB's cache disk.
#
#   ./engine-deploy/deploy-starrocks.sh install            helm upgrade --install; a NEW cluster gets its
#                                                      BE volumes on the class
#   ./engine-deploy/deploy-starrocks.sh migrate-be-disks   an EXISTING cluster: move its BE volumes to the
#                                                      class, one BE at a time (below)
#   ./engine-deploy/deploy-starrocks.sh status             pods, and each BE volume's class and EBS speed
#   ./engine-deploy/deploy-starrocks.sh connection         how to reach the FE
#   ./engine-deploy/deploy-starrocks.sh uninstall          release + FE/BE PVCs (asks first)
#   ./engine-deploy/deploy-starrocks.sh down               the FE/BE pods and PVCs, no prompt, kubectl only
#                                                      (the test's watcher runs it in the cluster);
#                                                      the release and operator stay, `install` brings it back
#
# Copied from ../cdc-pipeline/starrocks/deploy-starrocks.sh, with
# its chart notes (below). What differs:
#   - The BEs' volumes use StorageClass gp3-starrocks-be (gp3-fast-sc.yaml,
#     applied here): 283 MB/s / 5333 IOPS each, so the 3 BEs together get the
#     ~850 MB/s / 16,000 IOPS that gp3-fast gives DuckDB behind its m5.8xlarge.
#     The chart has one storageClassName for a BE's data, log and spill
#     volumes, so all get it.
#   - BE 9 cpu / 38 Gi. mem_limit = 90% of the pod = 34.2 GiB (set in bytes:
#     the default "90%" came out as 81% in SHOW BACKENDS); datacache memory =
#     20% of mem_limit = 6.84 GiB (7.34 GB; 22.03 GB for the 3, DuckDB's
#     CACHE_MEM_BYTES), so ~27.4 GiB is left for queries. The BE is
#     the 1:4 cpu:memory of the original 6 cpu / 24 Gi. requests = limits.
#     Nodes: driver-nodepool (the values file's on-demand / amd64 / m5 stay);
#     Karpenter picks the size from the requests.
#   - No catalog setup: querybench/starrocks.sh creates the session's catalogs.
#   - No install-iam: the FE/BE pods use starrocks-sa, annotated with the EMR
#     execution role (option 2 of the CDC script's create_iam_service_account).
#   - Logging and .bench-env from lib/common.sh, like every script here.
#
# Chart layout notes (kube-starrocks 1.11.x), from the CDC script — they drive
# the shape of this script, so do not "simplify" them back:
#   * The operator and the cluster are one chart, with one release name. 
#
# Knobs: RELEASE_NAME (starrocks), CHART_VERSION (1.11.7), SR_IMAGE_TAG (4.1.4;
# empty = the chart's floating tag), VALUES_FILE (the CDC pipeline's
# helm/starrocks-eks-values.yaml), BE_DATACACHE_MEM_BYTES (8350000000),
# BE_CPU (9), BE_MEMORY (38Gi),
# HEALTHY_TIMEOUT_S (1800, per BE in migrate-be-disks).
###############################################################################
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# .bench-env, NAMESPACE (emr-flink: the only namespace the execution role's
# trust policy covers, so the FE/BE pods' IRSA works), log/info/warn/die.
# shellcheck source=../lib/common.sh
source "${HERE}/../lib/common.sh"

RELEASE_NAME=${RELEASE_NAME:-starrocks}
# Pin the chart. Without --version, helm takes whatever the repo's latest is.
CHART_VERSION=${CHART_VERSION:-1.11.7}
# Pin the FE/BE image tags; `-` (no colon): SR_IMAGE_TAG="" opts back into the
# chart's floating appVersion tag. Not IMAGE_TAG: that one is the Flink image's.
SR_IMAGE_TAG=${SR_IMAGE_TAG-4.1.4}
# The CDC pipeline's values file: the same cluster shape (3 BEs, 1 FE),
# overridden here for the BEs' storage class, memory and datacache memory
# (be_overrides).
VALUES_FILE=${VALUES_FILE:-${BENCH_DIR}/../cdc-pipeline/helm/starrocks-eks-values.yaml}
BE_STORAGE_CLASS=gp3-starrocks-be                   # defined in gp3-fast-sc.yaml
BE_CPU=${BE_CPU:-9}
BE_MEMORY=${BE_MEMORY:-38Gi}
# mem_limit = 90% of BE_MEMORY = 34.2 GiB, in bytes (be.conf): its default "90%" gave 81% of the pod.
BE_MEM_LIMIT_BYTES=${BE_MEM_LIMIT_BYTES:-36721970380}
# datacache memory = 20% of mem_limit = 6.84 GiB; x 3 = 22.03 GB = DuckDB's CACHE_MEM_BYTES.
BE_DATACACHE_MEM_BYTES=${BE_DATACACHE_MEM_BYTES:-7344394076}
HEALTHY_TIMEOUT_S=${HEALTHY_TIMEOUT_S:-1800}
BE_STS=kube-starrocks-be
FE_POD=kube-starrocks-fe-0

check_prerequisites() {
    command -v kubectl &>/dev/null || die "kubectl not found"
    command -v helm &>/dev/null || die "helm not found (Helm 3)"
    kubectl cluster-info &>/dev/null || die "cannot reach the Kubernetes cluster (kubectl config)"
    [[ -f "$VALUES_FILE" ]] || die "values file not found: ${VALUES_FILE}"
}

# The BEs' StorageClass, from gp3-fast-sc.yaml
apply_be_storage_class() {
    kubectl apply -f "${HERE}/gp3-fast-sc.yaml" >/dev/null \
        || die "could not apply gp3-fast-sc.yaml (a StorageClass that differs from it must be deleted first)"
    log "✓ StorageClass ${BE_STORAGE_CLASS}: $(kubectl get storageclass "$BE_STORAGE_CLASS" \
          -o jsonpath='{.parameters.iops} IOPS / {.parameters.throughput} MB/s') per volume"
}

add_helm_repo() {
    helm repo add starrocks https://starrocks.github.io/starrocks-kubernetes-operator >/dev/null 2>&1 || true
    helm repo update starrocks >/dev/null
}

# The BE values over the CDC pipeline's file (helm merges the maps). be.conf
# comes from configyaml, which replaces the chart's default config string,
# so that string's six lines are repeated here.
be_overrides() {
    cat <<EOF
starrocks:
  starrocksBeSpec:
    nodeSelector:
      karpenter.sh/nodepool: driver-nodepool
      # No size: Karpenter sizes the node from the BE's cpu/memory requests.
      # null removes the values file's 2xlarge pin (helm deletes a key set to
      # null); without it the BEs would stay Pending. Karpenter may then mix
      # sizes (one m5.8xlarge + two m5.4xlarge on 2026-10-23).
      karpenter.k8s.aws/instance-size: null
    resources:
      # cpu in millicores (7500m): the StarRocksCluster CRD takes an integer or a
      # string, and the chart writes limits out unquoted, so 7.5 arrives as a number.
      requests: {cpu: "${BE_CPU}", memory: ${BE_MEMORY}}
      limits: {cpu: "${BE_CPU}", memory: ${BE_MEMORY}}
    storageSpec:
      storageClassName: ${BE_STORAGE_CLASS}
    configyaml:
      be_port: "9060"
      webserver_port: "8040"
      heartbeat_service_port: "9050"
      brpc_port: "8060"
      sys_log_level: "INFO"
      default_rowset_type: "beta"
      datacache_mem_size: "${BE_DATACACHE_MEM_BYTES}"
      mem_limit: "${BE_MEM_LIMIT_BYTES}"
EOF
}

# Operator + FE/BE cluster as a single Helm release (see the chart notes above).
deploy_starrocks() {
    local ov; ov=$(mktemp); be_overrides > "$ov"
    local -a set_args=(--values "$ov")
    if [[ -n "$SR_IMAGE_TAG" ]]; then
        set_args+=(--set "starrocks.starrocksFESpec.image.tag=${SR_IMAGE_TAG}")
        set_args+=(--set "starrocks.starrocksBeSpec.image.tag=${SR_IMAGE_TAG}")
    else
        warn "SR_IMAGE_TAG is empty: the chart appVersion, on chart ${CHART_VERSION} the MUTABLE tag"
        warn "'4.1-latest'. Numbers taken this way are not reproducible across pod restarts."
    fi
    info "chart starrocks/kube-starrocks ${CHART_VERSION}, images ${SR_IMAGE_TAG:-<appVersion>}, BE class ${BE_STORAGE_CLASS}"
    info "BE: ${BE_CPU} cpu / ${BE_MEMORY} (driver-nodepool), datacache memory ${BE_DATACACHE_MEM_BYTES} B"
    info "values ${VALUES_FILE}"
    helm upgrade --install "$RELEASE_NAME" starrocks/kube-starrocks \
        --version "$CHART_VERSION" --namespace "$NAMESPACE" --values "$VALUES_FILE" \
        "${set_args[@]}" --wait --timeout 15m || { rm -f "$ov"; die "helm upgrade --install failed"; }
    rm -f "$ov"
    log "✓ StarRocks operator + cluster deployed (release ${RELEASE_NAME})"
}

wait_for_starrocks() {
    local waited=0 fe be
    while (( waited < 600 )); do
        fe=$(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/component=fe \
               -o jsonpath='{.items[*].status.conditions[?(@.type=="Ready")].status}' | { grep -o True || true; } | wc -l | tr -d ' ')
        be=$(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/component=be \
               -o jsonpath='{.items[*].status.conditions[?(@.type=="Ready")].status}' | { grep -o True || true; } | wc -l | tr -d ' ')
        # All of them: the values file's FE and BE replica counts.
        (( fe >= 1 && be >= 3 )) && { log "✓ StarRocks ready (FE ${fe}, BE ${be})"; return 0; }
        sleep 10; waited=$(( waited + 10 ))
    done
    die "StarRocks not ready after 10 min: kubectl get pods -n ${NAMESPACE} | grep starrocks"
}

sr() { kubectl exec -n "$NAMESPACE" "$FE_POD" -- mysql -h127.0.0.1 -P9030 -uroot -N -e "$1" 2>/dev/null; }

# The FE's unhealthy-tablet total (SHOW PROC '/statistic', its Total row), or
# empty if the FE does not answer.
unhealthy_tablets() { sr "SHOW PROC '/statistic'" | awk '$1 == "Total" {print $8}'; }

be_count() { kubectl get sts "$BE_STS" -n "$NAMESPACE" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo 0; }

migrate_be_disks() {
    local n i p cls deadline u
    [[ "$(kubectl get job qb-runner -n "$NAMESPACE" -o jsonpath='{.status.active}' 2>/dev/null)" =~ ^[1-9] ]] \
        && die "a query test is running (qb-runner): migrate after it ends"
    n=$(be_count); (( n > 0 )) || die "no ${BE_STS} StatefulSet in ${NAMESPACE}: use install"
    (( n >= 2 )) || warn "only ${n} BE: its tablets have no other replica to clone from"
    apply_be_storage_class

    # 2. A StatefulSet's volumeClaimTemplates are immutable: orphan-delete it,
    # and the operator re-creates it from the upgraded CR, adopting the pods.
    if [[ "$(kubectl get sts "$BE_STS" -n "$NAMESPACE" -o jsonpath='{.spec.volumeClaimTemplates[0].spec.storageClassName}')" != "$BE_STORAGE_CLASS" ]]; then
        log "re-creating StatefulSet ${BE_STS} with ${BE_STORAGE_CLASS} (BE pods keep running)"
        kubectl delete sts "$BE_STS" -n "$NAMESPACE" --cascade=orphan
        deploy_starrocks
        deadline=$(( $(date +%s) + 600 ))
        until [[ "$(kubectl get sts "$BE_STS" -n "$NAMESPACE" -o jsonpath='{.spec.volumeClaimTemplates[0].spec.storageClassName}' 2>/dev/null)" == "$BE_STORAGE_CLASS" ]]; do
            (( $(date +%s) < deadline )) || die "the operator did not re-create ${BE_STS} with ${BE_STORAGE_CLASS} in 10 min"
            sleep 10
        done
        kubectl rollout status sts "$BE_STS" -n "$NAMESPACE" --timeout=30m
    fi

    # 3. One BE at a time, so the other replicas of its tablets stay readable.
    for (( i = 0; i < n; i++ )); do
        p="${BE_STS}-${i}"
        cls=$(kubectl get pvc "be-data-${p}" -n "$NAMESPACE" -o jsonpath='{.spec.storageClassName}' 2>/dev/null || true)
        [[ "$cls" == "$BE_STORAGE_CLASS" ]] && { log "${p}: already on ${BE_STORAGE_CLASS}"; continue; }
        u=$(unhealthy_tablets); [[ "$u" == 0 ]] || die "the FE reports ${u:-no answer} unhealthy tablets before ${p}: wait, or check SHOW PROC '/statistic'"
        log "${p}: replacing its volumes (${cls:-?} -> ${BE_STORAGE_CLASS})"
        kubectl delete pvc -n "$NAMESPACE" "be-data-${p}" "be-log-${p}" --ignore-not-found --wait=false
        kubectl delete pod -n "$NAMESPACE" "$p" --wait=true
        kubectl wait pod -n "$NAMESPACE" "$p" --for=condition=Ready --timeout=20m
        deadline=$(( $(date +%s) + HEALTHY_TIMEOUT_S ))
        while u=$(unhealthy_tablets); [[ "$u" != 0 ]]; do
            (( $(date +%s) < deadline )) || die "${p}: ${u:-?} unhealthy tablets after ${HEALTHY_TIMEOUT_S}s"
            log "${p}: waiting for the FE to re-replicate (${u:-?} unhealthy tablets)"
            sleep 30
        done
        log "✓ ${p} on ${BE_STORAGE_CLASS}, all tablets healthy"
    done
    status
}

status() {
    local p vol
    kubectl get pods -n "$NAMESPACE" | grep -E 'NAME|starrocks' || true
    echo ""
    for p in $(kubectl get pvc -n "$NAMESPACE" -o name | grep -E '/be-(data|log)-kube-starrocks-be-[0-9]+$' | sed 's|.*/||' | sort); do
        vol=$(kubectl get pv "$(kubectl get pvc "$p" -n "$NAMESPACE" -o jsonpath='{.spec.volumeName}')" -o jsonpath='{.spec.csi.volumeHandle}')
        printf '%-28s %-18s %s\n' "$p" "$(kubectl get pvc "$p" -n "$NAMESPACE" -o jsonpath='{.spec.storageClassName}')" \
            "$(aws ec2 describe-volumes --region "$AWS_REGION" --volume-ids "$vol" \
                 --query 'Volumes[0].[VolumeId,Iops,Throughput]' --output text | awk '{print $1, $2 " IOPS", $3 " MB/s"}')"
    done
}

# No external endpoint: the values file makes the FE service ClusterIP.
connection() {
    local svc
    svc=$(kubectl get svc -n "$NAMESPACE" -l app.kubernetes.io/component=fe \
            -o jsonpath='{.items[?(@.spec.clusterIP!="None")].metadata.name}' | awk '{print $1}')
    [[ -n "$svc" ]] || die "no StarRocks FE service in ${NAMESPACE}"
    echo "  in-cluster:     mysql -h ${svc}.${NAMESPACE}.svc.cluster.local -P 9030 -u root"
    echo "  via the FE pod: kubectl exec -it -n ${NAMESPACE} ${FE_POD} -- mysql -h127.0.0.1 -P9030 -uroot"
    echo "  port-forward:   kubectl port-forward -n ${NAMESPACE} svc/${svc} 9030:9030 8030:8030"
}

# Not the namespace: emr-flink also holds the Flink operator and the writers.
# The PVCs go explicitly: helm uninstall leaves StatefulSet volumes billing.
uninstall() {
    warn "This deletes release '${RELEASE_NAME}' in '${NAMESPACE}' and its FE/BE PVCs (all StarRocks data)."
    read -p "Are you sure? (yes/no): " -r
    [[ $REPLY =~ ^[Yy][Ee][Ss]$ ]] || { log "cancelled"; return 0; }
    helm uninstall "$RELEASE_NAME" -n "$NAMESPACE" || true
    kubectl delete pvc -n "$NAMESPACE" -l app.kubernetes.io/component=fe || true
    kubectl delete pvc -n "$NAMESPACE" -l app.kubernetes.io/component=be || true
    info "Namespace ${NAMESPACE} and StorageClass ${BE_STORAGE_CLASS} were kept."
}

# The cluster without Helm and without asking: what qb-watch.sh runs in its pod
# (kubectl only) once a test ends. Deleting the StarRocksCluster makes the
# operator remove the FE/BE StatefulSets; their nodes empty and Karpenter
# removes them. The FE/BE PVCs go explicitly (StatefulSets leave them). The
# Helm release and the operator stay: `install` (helm upgrade --install)
# re-creates the StarRocksCluster from the release.
down() {
    local i n
    kubectl delete starrocksclusters.starrocks.com kube-starrocks -n "$NAMESPACE" --ignore-not-found --wait=true --timeout=300s >/dev/null \
        || die "could not delete StarRocksCluster kube-starrocks"
    for (( i = 0; i < 60; i++ )); do
        n=$(kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/component=fe -o name 2>/dev/null; \
            kubectl get pods -n "$NAMESPACE" -l app.kubernetes.io/component=be -o name 2>/dev/null)
        [[ -z "$n" ]] && break
        sleep 5
    done
    [[ -z "$n" ]] || warn "FE/BE pods still terminating after 5 min"
    kubectl delete pvc -n "$NAMESPACE" -l app.kubernetes.io/component=fe --ignore-not-found >/dev/null || true
    kubectl delete pvc -n "$NAMESPACE" -l app.kubernetes.io/component=be --ignore-not-found >/dev/null || true
    log "StarRocks FE/BE and their volumes deleted (release ${RELEASE_NAME} and the operator kept; install re-creates them)"
}

case "${1:-}" in
    install)          check_prerequisites; add_helm_repo; apply_be_storage_class; deploy_starrocks; wait_for_starrocks; status; connection ;;
    migrate-be-disks) check_prerequisites; add_helm_repo; migrate_be_disks ;;
    status)           status ;;
    connection)       connection ;;
    uninstall)        uninstall ;;
    down)             down ;;
    *)                sed -n '3,16p' "${HERE}/deploy-starrocks.sh" | sed 's/^# \{0,1\}//'; exit 1 ;;
esac
