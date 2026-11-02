# DuckDB v2.0.0 built FROM SOURCE for the CDC benchmark's warm server
# (duckdb_cdc_server.py), with iceberg, httpfs, aws and avro STATICALLY linked
# into the Python module. No CLI: the server only imports duckdb. See README.md.
#
# Stages
#   deps       toolchain, vcpkg at DuckDB's pinned release, the DuckDB sources
#              (duckdb-python v2.0.0, whose external/duckdb submodule IS duckdb
#              v2.0.0), the extension sources at the commits the v2.0.0 tag pins,
#              and every vcpkg dependency.
#   py-build   the duckdb Python wheel (scikit-build-core, Release).
#   runtime    python slim + the wheel + awscli; LOADs iceberg and httpfs, so
#              an image that cannot never gets pushed.
#
# Builds natively on linux/amd64 and linux/arm64 (the triplet follows
# `uname -m`), so it works under Kaniko (one pod per arch, like
# cdc-pipeline/build-deploy-generic.sh) and under `docker buildx` alike. It uses
# no BuildKit-only syntax (no RUN --mount), because Kaniko ignores it.
#
# Parallelism: COMPILE_JOBS parallel C++ compile jobs (make -j; default 30, an
# 8xlarge node). DuckDB's unity build needs ~1.5-2 GiB per job.

ARG PYTHON_VERSION=3.12
# trixie (GCC 14), not bookworm (GCC 12): GCC 12 rejects httpfs 77392a72's
# `return result;` of a unique_ptr<HTTPFSParams> as unique_ptr<HTTPParams>
# (httpfs.cpp:215, first Kaniko build 2026-11-02). Build and runtime stages share
# the suite, so the binaries' glibc matches.
ARG DEBIAN_SUITE=trixie

###############################################################################
FROM python:${PYTHON_VERSION}-${DEBIAN_SUITE} AS deps
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

# --- pins (all from the duckdb v2.0.0 tag, checked 2026-11-02) ---------------
ARG DUCKDB_TAG=v2.0.0
ARG DUCKDB_COMMIT=d54cbbe3df17e8e5ce68312f483b4155836112aa
ARG DUCKDB_PYTHON_COMMIT=83c719961098611e769dc4bab0bda2bf149a2d1e
# Makefile `setup-vcpkg` and .github/workflows/Main.yml at v2.0.0: release 2026.06.24
ARG VCPKG_TAG=2026.06.24
ARG VCPKG_COMMIT=cd61e1e26a038e82d6550a3ebbe0fbbfe7da78e3
# .github/config/extensions/<name>.cmake GIT_TAG at v2.0.0. Only verified here;
# the source of truth is the tag (scripts/sync_out_of_tree_extensions.py reads it).
ARG ICEBERG_COMMIT=4c0c0874c48ec82a3e29910554891d04bcc0e988
ARG HTTPFS_COMMIT=77392a72077a85897db2519fae11e2c0da972b8a
ARG AWS_COMMIT=16ae9c135300f750478d648c2496f2952a6f2827
ARG AVRO_COMMIT=758fb6063cbb6da49b2e1c16c4044cfcafefc52f
ARG EXTENSIONS="iceberg;httpfs;aws;avro"
# duckdb-python needs CMake >= 3.29; 3.x rather than 4.x because some vcpkg ports
# still declare cmake_minimum_required < 3.5, which CMake 4 rejects.
ARG CMAKE_VERSION=3.31.6
ARG COMPILE_JOBS=30

# ARGs end with their stage; py-build (FROM deps) reads these ENVs.
# VCPKG_FORCE_SYSTEM_BINARIES: required on arm64 (vcpkg ships no aarch64
# cmake/ninja), harmless on amd64.
ENV DDB_TAG=${DUCKDB_TAG} \
    DDB_COMMIT=${DUCKDB_COMMIT} \
    DDB_EXTENSIONS=${EXTENSIONS} \
    COMPILE_JOBS=${COMPILE_JOBS} \
    DEBIAN_FRONTEND=noninteractive \
    VCPKG_ROOT=/opt/vcpkg \
    VCPKG_DISABLE_METRICS=1 \
    VCPKG_FORCE_SYSTEM_BINARIES=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ROOT_USER_ACTION=ignore

# hadolint ignore=DL3008
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        build-essential git ca-certificates curl zip unzip tar pkg-config \
        ninja-build bison flex perl autoconf automake libtool linux-libc-dev \
 && rm -rf /var/lib/apt/lists/* \
 && pip install --no-cache-dir "cmake==${CMAKE_VERSION}" \
 && cmake --version | head -1 && ninja --version && gcc --version | head -1

# vcpkg: a FULL clone, because manifest versioning (builtin-baseline, the openssl
# 3.6.0 override) reads port versions out of the registry's git history.
RUN git clone --quiet --branch "${VCPKG_TAG}" https://github.com/microsoft/vcpkg.git "${VCPKG_ROOT}" \
 && test "$(git -C "${VCPKG_ROOT}" rev-parse HEAD)" = "${VCPKG_COMMIT}" \
 && "${VCPKG_ROOT}/bootstrap-vcpkg.sh" -disableMetrics \
 && case "$(uname -m)" in \
        x86_64)  echo x64-linux-release ;; \
        aarch64) echo arm64-linux-release ;; \
        *) echo "unsupported arch $(uname -m)" >&2; exit 1 ;; \
    esac > /opt/vcpkg-triplet \
 && echo "vcpkg triplet: $(cat /opt/vcpkg-triplet)"

# DuckDB sources. duckdb-python v2.0.0 vendors duckdb as a submodule at exactly
# the v2.0.0 commit, so the wheel is built from exactly that tree.
WORKDIR /src
RUN git clone --quiet --branch "${DUCKDB_TAG}" https://github.com/duckdb/duckdb-python.git duckdb-python \
 && test "$(git -C duckdb-python rev-parse HEAD)" = "${DUCKDB_PYTHON_COMMIT}" \
 && git -C duckdb-python submodule update --init --recursive --depth 1 \
 && test "$(git -C duckdb-python/external/duckdb rev-parse HEAD)" = "${DUCKDB_COMMIT}" \
 && echo "duckdb ${DUCKDB_TAG} = $(git -C duckdb-python/external/duckdb rev-parse HEAD)"

# Out-of-tree extensions at the commits the tag pins, synced into
# external/duckdb/extension/external/<name> with DuckDB's own script (it applies
# .github/patches/extensions/<name> too: avro gets one patch commit on top of its
# pin), plus the merged vcpkg manifest of all four, then every vcpkg dependency
# built into /opt/vcpkg_installed (aws-sdk-cpp, curl, openssl 3.6.0, roaring,
# jansson, liblzma, snappy, zlib).
WORKDIR /src/duckdb-python/external/duckdb
# `set -- ${pin}` splits "name commit" on purpose.
# hadolint ignore=SC2086
RUN VCPKG_TOOLCHAIN_PATH="${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake" \
        python3 scripts/sync_out_of_tree_extensions.py \
        --build-extensions "${EXTENSIONS}" --output-dir /src/vcpkg-manifest \
 && for pin in "iceberg ${ICEBERG_COMMIT}" "httpfs ${HTTPFS_COMMIT}" "aws ${AWS_COMMIT}" "avro ${AVRO_COMMIT}"; do \
        set -- ${pin}; d="extension/external/$1"; \
        git -C "$d" merge-base --is-ancestor "$2" HEAD \
          || { echo "extension $1 is not at its v2.0.0 pin $2" >&2; exit 1; }; \
        echo "extension $1 @ $2 (+$(git -C "$d" rev-list --count "$2"..HEAD) patch commit(s))"; \
    done \
 && cat /src/vcpkg-manifest/vcpkg.json \
 && triplet="$(cat /opt/vcpkg-triplet)" \
 && VCPKG_MAX_CONCURRENCY="${COMPILE_JOBS}" "${VCPKG_ROOT}/vcpkg" install \
        --x-manifest-root=/src/vcpkg-manifest --x-install-root=/opt/vcpkg_installed \
        --triplet="${triplet}" --host-triplet="${triplet}" --clean-after-build \
 && rm -rf "${VCPKG_ROOT}/buildtrees" "${VCPKG_ROOT}/downloads" "${VCPKG_ROOT}/packages" \
 && find /opt/vcpkg_installed/"${triplet}"/lib -maxdepth 1 -name "*.a" | sort

###############################################################################
FROM deps AS py-build
# The wheel's BUILD_EXTENSIONS replaces pyproject's "core_functions;json;parquet;icu",
# so those four are listed again; LINK_EXTENSIONS stays "" = link all of them.
# DUCKDB_NEW_EXTENSION_BUILD=1 makes DuckDB's CMake take the extension sources
# synced in `deps` (extension/external/<name>) instead of fetching them again.
# OVERRIDE_GIT_DESCRIBE=v2.0.0 makes it the 2.0.0 release build (package 2.0.0,
# engine v2.0.0), not a 2.0.0.devN. No LTO: PyPI's Linux wheels are clang+ThinLTO.
WORKDIR /src/duckdb-python
RUN triplet="$(cat /opt/vcpkg-triplet)" \
 && DUCKDB_NEW_EXTENSION_BUILD=1 \
    OVERRIDE_GIT_DESCRIBE="${DDB_TAG}" \
    CMAKE_BUILD_PARALLEL_LEVEL="${COMPILE_JOBS}" \
    CMAKE_GENERATOR=Ninja \
    pip wheel --no-deps --wheel-dir /out/wheels -v . \
        -C build-dir=/tmp/pybuild \
        -C cmake.define.BUILD_EXTENSIONS="core_functions;json;parquet;icu;${DDB_EXTENSIONS}" \
        -C cmake.define.CMAKE_TOOLCHAIN_FILE="${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake" \
        -C cmake.define.VCPKG_TARGET_TRIPLET="${triplet}" \
        -C cmake.define.VCPKG_HOST_TRIPLET="${triplet}" \
        -C cmake.define.VCPKG_MANIFEST_DIR=/src/vcpkg-manifest \
        -C cmake.define.VCPKG_INSTALLED_DIR=/opt/vcpkg_installed \
        -C cmake.define.VCPKG_MANIFEST_INSTALL=OFF \
        -C cmake.define.VCPKG_BUILD=1 \
 && rm -rf /tmp/pybuild \
 && ls -l /out/wheels

###############################################################################
FROM python:${PYTHON_VERSION}-slim-${DEBIAN_SUITE} AS runtime
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]
ARG DUCKDB_TAG=v2.0.0
# The server shells out to `aws sts assume-role-with-web-identity` (refresh_secret).
# awscli 1.x as the pod's `pip install awscli` today (latest on PyPI 2026-11-02).
ARG AWSCLI_SPEC=awscli==1.46.2
# Installed into the image (~/.duckdb/extensions/v2.0.0/<platform>) so the pod
# needs no download at start; the server's INSTALL ... FROM community then finds
# it. Empty to skip (no network at build time).
ARG PREFETCH_COMMUNITY_EXTENSIONS=cache_httpfs
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ROOT_USER_ACTION=ignore \
    PYTHONUNBUFFERED=1

COPY --from=py-build /out/wheels/ /tmp/wheels/

# AWSCLI_SPEC is pinned (awscli==...); hadolint cannot see through the ARG.
# hadolint ignore=DL3013
RUN pip install --no-cache-dir /tmp/wheels/duckdb-*.whl "${AWSCLI_SPEC}" \
 && rm -rf /tmp/wheels \
 && for ext in ${PREFETCH_COMMUNITY_EXTENSIONS}; do \
        python3 -c "import duckdb; duckdb.sql(\"INSTALL ${ext} FROM community; LOAD ${ext};\")"; \
    done \
 && python3 -c "import duckdb; print(duckdb.sql('SELECT version()').fetchone()); duckdb.sql('LOAD iceberg; LOAD httpfs;')"

LABEL org.opencontainers.image.title="duckdb-cdc-server" \
      org.opencontainers.image.description="DuckDB ${DUCKDB_TAG} from source, iceberg/httpfs/aws/avro statically linked (Python)" \
      org.opencontainers.image.source="https://github.com/duckdb/duckdb/tree/${DUCKDB_TAG}"
WORKDIR /opt/bench
CMD ["python3", "-u", "/opt/bench/duckdb_cdc_server.py"]
