# DuckDB v2.0.0 from source: image for the CDC warm server

The image `duckdb-cdc-server:2.0.0` runs `engine-deploy/duckdb_cdc_server.py` (Deployment
`duckdb-warm-cdc`). It holds DuckDB **v2.0.0 compiled from source**, with **iceberg, httpfs,
aws and avro statically linked** into the Python module (no CLI), plus awscli and the
`cache_httpfs` community extension already installed. One tag serves **linux/amd64 and
linux/arm64**.

| file | what |
|---|---|
| `Dockerfile` | the whole build; nothing else is copied in |
| `build-duckdb-image.sh` | builds the Dockerfile for amd64 and arm64 in the cluster and pushes the one tag to ECR |

## Build

```bash
cd cdc-pipeline/engine-deploy/duckdb-image
./build-duckdb-image.sh
```

What it does:

1. Creates the ECR repository `duckdb-cdc-server` if it is missing.
2. Puts the Dockerfile into the ConfigMap `duckdb-cdc-image-context`.
3. Starts one Kaniko pod per architecture (`duckdb-cdc-image-build-amd64` / `-arm64`), in
   parallel, each on a node of that architecture. Kaniko cannot cross-build, so each builds
   natively and pushes `2.0.0-amd64` / `2.0.0-arm64`.
4. Joins the two into the manifest list `2.0.0`. The per-arch tags stay in the repository.

Follow a build with `kubectl logs -f duckdb-cdc-image-build-amd64 -n emr-flink` (or `-arm64`).

**Settings.** `AWS_ACCOUNT_ID`, `AWS_REGION` and `NAMESPACE` come from `cdc-pipeline/.bench-env`
(+ `.bench-env.local`). The only optional knob is **`COMPILE_JOBS`** (default 30): the number of
parallel C++ compile jobs in each build, like `make -j`. Each pod gets `COMPILE_JOBS` cpu and
`2 × COMPILE_JOBS + 8` GiB of memory, so 30 cpu / 68 GiB by default, which Karpenter places on an
8xlarge node (32 vCPU, 128 GiB). Lower it if 8xlarge nodes are not available, e.g.
`COMPILE_JOBS=8 ./build-duckdb-image.sh` (8 cpu / 24 GiB, fits a 4xlarge).

**Requirements.**

- ServiceAccount `image-builder-sa` in the namespace, with an IRSA role that can push to the
  repository. The execution role's inline policy `FlinkCDCImagePush` was extended to
  `duckdb-cdc-server`; ClaudeCode helpped to grant the IAM role.
- 80 GiB of node disk per pod: Kaniko compiles inside its own root filesystem, i.e. on the
  node's root volume. The Karpenter's EC2NodeClass has 200 Gi root disks.

**Time.** The build is CPU-bound (the vcpkg dependencies, aws-sdk-cpp the largest, then the
Python wheel). At `COMPILE_JOBS=8` with the CLI still built as well, the first trixie build ran
at 8 of 16 vCPUs, 100% busy; 30 jobs and no CLI build should cut that several-fold (not yet
measured). Both architectures run at the same time; the script waits up to 4 hours per build.

## What the Dockerfile builds

Stages: `deps` (toolchain, vcpkg, DuckDB and extension sources, every vcpkg dependency) →
`py-build` (the Python wheel) → `runtime` (`python:3.12-slim-trixie` + the wheel + awscli +
`cache_httpfs`). There is no CLI build: the server only imports the Python module, and dropping
it removes a second full DuckDB compile. The last build step runs `SELECT version(); LOAD iceberg;
LOAD httpfs;` through the Python module, so an image that cannot load them is never pushed.

| pin | value |
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

The extension commits are the ones the DuckDB v2.0.0 tag itself pins. The wheel build names
them in `BUILD_EXTENSIONS` (with `core_functions;json;parquet;icu`, which that list replaces) and
links all of them statically.

**Toolchain: GCC 14, not GCC 12.** The first build used Debian bookworm (GCC 12) and stopped in
httpfs: `httpfs.cpp:215: could not convert 'result' from 'unique_ptr<HTTPFSParams>' to
'unique_ptr<HTTPParams>'`. That is the commit DuckDB's own release was built from, so the cause
was the compiler, and the build moved to trixie (GCC 14). If a build fails again, the first
compiler error in the pod log is the place to look; a manylinux_2_28 base (DuckDB's CI image) is
the next thing to try.

**Timings are a new baseline.** This build is GCC 14 without LTO; the PyPI wheels are clang with
ThinLTO. Don't compare DuckDB timings from this image with runs on the PyPI engine (1.5.6 or the
2.0.0 wheel).

## Deploy

`engine-deploy/deploy-duckdb-cdc.sh` takes its image from `DUCKDB_IMAGE`, which
`cdc-pipeline/.bench-env` sets to `<account>.dkr.ecr.<region>.amazonaws.com/duckdb-cdc-server:2.0.0`.
With an image set, the pod runs it as is: no pip step, so the source-built engine and its static
extensions are what runs. `DUCKDB_IMAGE=` (empty) falls back to `python:3.12-slim` plus
`pip install duckdb==${DUCKDB_VERSION}` (default 2.0.0) at every start, and the server downloads
the extensions.

```bash
cd cdc-pipeline
./engine-deploy/deploy-duckdb-cdc.sh render     # writes duckdb-cdc-deployed.yaml only
./engine-deploy/deploy-duckdb-cdc.sh up         # Deployment on DUCKDB_IMAGE (.bench-env)
./engine-deploy/deploy-duckdb-cdc.sh status     # /health reports the image and "duckdb": "2.0.0"
DUCKDB_IMAGE= ./engine-deploy/deploy-duckdb-cdc.sh up     # the PyPI fallback
```

Don't swap the image with `kubectl set image`: the next `up` re-renders the Deployment from
`DUCKDB_IMAGE` and undoes it. `querybench/run.sh` passes `DUCKDB_IMAGE` (even empty) to the
runner, and redeploys the server if `/health` reports a different image.

## What `duckdb_cdc_server.py` changed for 2.x

All of these are implemented in `engine-deploy/duckdb_cdc_server.py`:

1. **Scan profiling uses `EXPLAIN (ANALYZE, FORMAT JSON)`.** 2.0 draws text plans in boxes
   titled `Table Scan`, so the old `TABLE_SCAN … rows` regex matched nothing, and a text regex
   can pick up a neighbouring operator's count. The JSON gives `io.total_bytes_read` and, per
   `TABLE_SCAN` node, the rows after pushed filters. Still open: compare `total_bytes_read` once
   against 1.5's HTTPFS `in:` byte count over S3 (with `cache_httpfs_type='noop'`).
2. **No `INSTALL` for built-in extensions.** `INSTALL` still downloads the official binary even
   when the extension is statically linked, so the server only `LOAD`s an extension whose
   `install_mode` is `STATICALLY_LINKED` or that is already installed (`cache_httpfs`). On this
   image the pod downloads nothing at start.
3. **Results stream.** In duckdb-python 2.0 an error can surface in `fetchall()`, not
   `execute()`; both sit in one `try` and one timer, so measured times are unchanged, and
   `interrupt()` from the timer still raises `InterruptException`. Each query uses its own
   cursor (2.0 refuses other calls on a connection with an open result).
4. **Nothing else changed** for the calls and settings the server uses. 2.0 needs Python ≥ 3.11
   (the image has 3.12).

### `iceberg_unsafe_skip_puffin_verification`

New in the 2.0 iceberg extension (boolean, global, default `false`). Before applying an Iceberg
v3 deletion vector, DuckDB checks that the delete file is a real Puffin container; the setting
skips that check.

- **The Paimon arm on the v3 default set needs it.** Paimon 2.1 exports 64-bit deletion vectors
  to Iceberg readers, but they point into Paimon's own index files, which are bare blobs, not
  Puffin. The server sets it per query: `true` for the arms in `SKIP_PUFFIN_VERIFICATION_ARMS`
  (default `paimon` when `ICEBERG_FORMAT_VERSION=3`), `false` for the rest, so native Iceberg's
  real Puffin files are still verified. `/health` and the run's results record it.
- **The v2 set doesn't need it**: a v2 export carries no deletion vectors.
- **Why "unsafe":** a wrong offset or length gives wrong deletes silently instead of an error,
  so keep the zero-scan and cross-arm row checks on.
