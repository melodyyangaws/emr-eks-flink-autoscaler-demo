# DuckDB v2.0.0 from source: image for the CDC warm server

`duckdb-cdc-server:2.0.0` runs `engine-deploy/duckdb_cdc_server.py`
(Deployment `duckdb-warm-cdc`). It holds DuckDB **v2.0.0 compiled from
source** with **iceberg, httpfs, aws and avro statically linked** into the
Python module (no CLI), plus awscli 1.46.2 and the `cache_httpfs` community
extension preinstalled. One tag, a manifest list for **linux/amd64 and
linux/arm64**.

| File | What |
|---|---|
| `Dockerfile` | the whole build (a single file: it is the entire build context) |
| `build-duckdb-image.sh` | builds it in the cluster for both architectures and pushes the one tag to ECR |

## Requirements

1. **`cdc-pipeline/.bench-env.local`** with `AWS_ACCOUNT_ID` and `AWS_REGION`;
   `NAMESPACE` (`emr-flink`) comes from `.bench-env`. Laptop: `kubectl`, AWS
   CLI, `python3`, `shasum`.
2. **ServiceAccount `image-builder-sa`** in the namespace (the one
   `build-deploy-generic.sh` uses), on an IRSA role that can push to ECR
   repository `duckdb-cdc-server`. On this account that is the execution
   role's inline policy **`FlinkCDCImagePush`**, which had to be extended with
   the `duckdb-cdc-server` repository (2026-11-02); without it Kaniko fails at
   push time, after the whole compile.
3. **Laptop credentials** for `ecr:DescribeRepositories`, `CreateRepository`,
   `BatchGetImage`, `PutImage` (the script creates the repository and writes
   the manifest list from the laptop).
4. **Build nodes**: on-demand nodes of both architectures that fit a pod of
   `COMPILE_JOBS` cpu and `2 × COMPILE_JOBS + 8` GiB (default 30 cpu / 68 GiB:
   an 8xlarge, 32 vCPU / 128 GiB), with a **200 Gi root disk**: Kaniko
   compiles inside its own root filesystem, i.e. on the node's root volume
   (the pod requests 80Gi ephemeral storage). The Karpenter EC2NodeClass
   `memoryec2` was raised from 50Gi to 200Gi for this. The pod selects only
   `kubernetes.io/arch` and `capacity-type: on-demand`, so check that the
   nodepool it lands on uses a 200Gi node class.

## Build

```bash
cd cdc-pipeline/engine-deploy/duckdb-image
./build-duckdb-image.sh                    # COMPILE_JOBS=30 by default
kubectl logs -f duckdb-cdc-image-build-amd64 -n emr-flink    # and -arm64, in another terminal
```

What it does:

1. Creates ECR repository `duckdb-cdc-server` if it is missing.
2. Puts the Dockerfile into ConfigMap `duckdb-cdc-image-context` (the build
   context, mounted at `/workspace`).
3. Starts two Kaniko pods (`gcr.io/kaniko-project/executor:v1.23.2`) **in
   parallel**, `duckdb-cdc-image-build-amd64` and `-arm64`, each on a node of
   its own architecture (Kaniko cannot cross-build), with
   `--build-arg=COMPILE_JOBS=<n>`, `--cache=false`. Each pushes
   `duckdb-cdc-server:2.0.0-<arch>`. The script polls every 30 s, up to 4 h per
   build; a failed pod's last 40 log lines are printed and the script stops.
4. Joins the two into the manifest list **`duckdb-cdc-server:2.0.0`**
   (`aws ecr put-image`, media type
   `application/vnd.docker.distribution.manifest.list.v2+json`). The per-arch
   tags stay in the repository.

Done: `pushed <account>.dkr.ecr.<region>.amazonaws.com/duckdb-cdc-server:2.0.0 (linux/amd64, linux/arm64)`.
A rerun deletes and recreates both build pods; finished pods stay until then
(`kubectl delete pod duckdb-cdc-image-build-amd64 duckdb-cdc-image-build-arm64 -n emr-flink`).

**`COMPILE_JOBS`** (default 30): parallel C++ compile jobs per build (vcpkg's
`VCPKG_MAX_CONCURRENCY` and CMake's build parallelism). DuckDB's unity build
needs about 1.5–2 GiB per job. Lower it when 8xlarge nodes are not available,
e.g. `COMPILE_JOBS=8 ./build-duckdb-image.sh` (8 cpu / 24 GiB, a 4xlarge).

## Time

| Build | arm64 | amd64 |
|---|---|---|
| `COMPILE_JOBS=8`, CLI still built (2026-11-02, measured) | 34 min | 43 min |
| `COMPILE_JOBS=30`, no CLI (the current default) | not measured yet | not measured yet |

The build is CPU-bound (vcpkg dependencies, aws-sdk-cpp the largest, then the
wheel); the measured builds kept their 8 jobs 100 % busy. Both architectures
run at the same time, so the wall time is the slower one plus node start-up.

## Verify the pushed image

```bash
REPO=duckdb-cdc-server
aws ecr describe-images --repository-name $REPO --region "$AWS_REGION" \
  --query 'imageDetails[].{tags:imageTags,type:imageManifestMediaType,pushed:imagePushedAt}' --output table
# the manifest list: two entries, linux/amd64 and linux/arm64
aws ecr batch-get-image --repository-name $REPO --image-ids imageTag=2.0.0 --region "$AWS_REGION" \
  --query 'images[0].imageManifest' --output text | python3 -m json.tool
```

`2.0.0` must be `application/vnd.docker.distribution.manifest.list.v2+json`
with one `manifests[]` entry per platform, and `2.0.0-amd64` / `2.0.0-arm64`
must exist. With docker on the laptop (after `aws ecr get-login-password |
docker login ...`), `docker manifest inspect <uri>:2.0.0` shows the same.

Then deploy and check the engine (`cdc-pipeline/`):

```bash
./engine-deploy/deploy-duckdb-cdc.sh up
./engine-deploy/deploy-duckdb-cdc.sh status    # "image": "<uri>:2.0.0", "duckdb": "2.0.0", "engine": "v2.0.0"
```

The image's last build step already ran `SELECT version()` and `LOAD iceberg;
LOAD httpfs;`, so an image that cannot load them is never pushed.

## What the Dockerfile builds

Stages: `deps` (toolchain, vcpkg, DuckDB and extension sources, every vcpkg
dependency) → `py-build` (the Python wheel, scikit-build-core, Release) →
`runtime` (`python:3.12-slim-trixie` + the wheel + awscli + `cache_httpfs`).
No CLI build: the server only imports the Python module. It uses no
BuildKit-only syntax (Kaniko ignores it), so `docker buildx` builds it too.

| Pin | Value |
|---|---|
| DuckDB | `v2.0.0` = `d54cbbe3df17e8e5ce68312f483b4155836112aa` |
| duckdb-python | `83c719961098611e769dc4bab0bda2bf149a2d1e` (its `external/duckdb` is the commit above) |
| iceberg | `4c0c0874c48ec82a3e29910554891d04bcc0e988` |
| httpfs | `77392a72077a85897db2519fae11e2c0da972b8a` |
| aws | `16ae9c135300f750478d648c2496f2952a6f2827` |
| avro | `758fb6063cbb6da49b2e1c16c4044cfcafefc52f` |
| vcpkg | `2026.06.24` = `cd61e1e26a038e82d6550a3ebbe0fbbfe7da78e3` |
| toolchain | Debian trixie, GCC 14, CMake 3.31.6, Ninja; Python 3.12 |
| awscli | 1.46.2 |

The extension commits are the ones the v2.0.0 tag pins (synced with DuckDB's
own `scripts/sync_out_of_tree_extensions.py`, patches included); the build
checks each one. The wheel lists them in `BUILD_EXTENSIONS` (with
`core_functions;json;parquet;icu`, which that list replaces) and links all of
them statically.

**GCC 14, not GCC 12.** The first build (Debian bookworm, GCC 12) stopped in
httpfs: `httpfs.cpp:215: could not convert 'result' from
'unique_ptr<HTTPFSParams>' to 'unique_ptr<HTTPParams>'`. That is the commit
DuckDB's own release was built from, so the cause was the compiler, and the
build moved to trixie (`DEBIAN_SUITE`). If a build fails again, the first
compiler error in the pod log is the place to look; a manylinux_2_28 base
(DuckDB's CI image) is the next thing to try.

**A separate timing baseline.** This build is GCC 14 without LTO; the PyPI
wheels are clang with ThinLTO. Don't compare DuckDB timings from this image
with runs on the PyPI engine (`DUCKDB_IMAGE=`).

## What `duckdb_cdc_server.py` changed for 2.x

1. **Scan profiling uses `EXPLAIN (ANALYZE, FORMAT JSON)`**: `io.total_bytes_read`
   and, per `TABLE_SCAN` node, the rows after pushed filters (2.0's text plans
   broke the old regex). Still open: compare `total_bytes_read` once with
   1.5's HTTPFS byte count.
2. **No `INSTALL` for built-in extensions**: `INSTALL` downloads the official
   binary even for a statically linked extension, so the server only `LOAD`s
   extensions that are `STATICALLY_LINKED` or already installed
   (`cache_httpfs`). On this image the pod downloads nothing at start.
3. **Results stream**: in duckdb-python 2.0 an error can surface in
   `fetchall()`; execute and fetch share one `try` and one timer, and each
   query uses its own cursor.
4. 2.0 needs Python ≥ 3.11 (the image has 3.12).

### `iceberg_unsafe_skip_puffin_verification`

New in the 2.0 iceberg extension (global, default `false`): skips the check
that a v3 deletion-vector file is a real Puffin container.

- **The Paimon arm on v3 needs it**: Paimon 2.1's exported deletion vectors
  point into its own index files, bare blobs, not Puffin. The server sets it
  per query: `true` for the arms in `SKIP_PUFFIN_VERIFICATION_ARMS` (default
  `paimon` when `ICEBERG_FORMAT_VERSION=3`), `false` otherwise, so native
  Iceberg's Puffin files are still verified. `/health` and the results record it.
- The v2 set doesn't need it (a v2 export has no deletion vectors).
- "Unsafe": a wrong offset gives wrong deletes silently, so keep the
  zero-scan and cross-arm row checks on.
